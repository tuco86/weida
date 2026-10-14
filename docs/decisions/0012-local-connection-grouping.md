# 0012 — Grouping local connections into a peer

- **Status:** accepted
- **Date:** 2026-09-11
- **Related:** [0010](0010-local-transport.md) §4.2, §4.4; [0008](0008-session-identity.md) §4.1,
  §4.2, §4.5; [0002](0002-control-and-bulk-separation.md) §6.2; B-038, B-039

## 1. The question

[0010](0010-local-transport.md) §4.2 decided that on a local transport **the OS connection is
the stream**: one connection per transfer, no multiplexer, because a multiplexer is
reimplementing QUIC's stream layer in the one place the project set out not to (0010 Option C,
rejected). Implementing it exposed what that sentence does not say.

A QUIC connection is symmetric: either side opens streams whenever it needs one. An accepted
`AF_UNIX` connection is not. The dialling side can connect again; the accepting side has
nothing to connect *to*. Three things need a stream from the accepting side:

- **Its own HELLO.** `docs/PROTOCOL.md` §2.1 keeps the HELLO exchange on local transports and
  fences the version with the `versions` intersection instead of ALPN. Both sides must send one.
- **Pub/Sub fan-out.** A publisher writes one DATA frame per matching subscriber, each on its
  own stream (`docs/PROTOCOL.md` §9.5), toward a subscriber that dialled *it*.
- **Everything per connection that is really per peer.** Sequence scopes, the dedup window, the
  parked drain receipts and `Agreed` are per `ConnCtx` today; with one connection per transfer
  each of them would be born and die with a single transfer, which is not what
  [0001](0001-sequence-field.md) §7.1 or [0009](0009-drain.md) §4.2 mean.

The in-process transport has none of these problems, because a channel pair is symmetric. So
this note is about the socket transports only, and the answer has to fit named pipes too:
whatever it is, B-039 inherits it.

## 2. The evidence, condensed

**Descriptor passing is not available shape-wide.** Passing one end of a `socketpair()` over a
control connection — the "role A" shape `ipc.md` §11 calls strictly better, and Cap'n Proto's
and Mojo's design — needs `SCM_RIGHTS`. Windows `AF_UNIX` has "no ancillary data and therefore
no handle passing" [ipc §3.6], the trade-off table records handle passing as `DuplicateHandle`
only and "not on `AF_UNIX`" [ipc §11], and Wayland is named as "a protocol that structurally
cannot run on a transport without `SCM_RIGHTS`" [ipc §7] — which is exactly the property weida
must not acquire if B-039 is to exist. `SCM_RIGHTS` is also still unstable in Rust `std`
[ipc §11], and `crates/weida` forbids `unsafe_code`, so taking it means a new dependency for a
capability one of the three target platforms cannot offer at all.

**A token is the shape the sheet records for exactly this problem.** Where the kernel cannot
authenticate the second connection, the surveyed systems use a nonce or token: D-Bus's
`nonce-tcp` "is TCP plus a simple authentication step intended to ensure that only clients with
read access to a filesystem [nonce file] can connect" [ipc §7, §11], and Tailscale's sandboxed
Darwin fallback is "loopback TCP on `127.0.0.1:0` with a random token and a same-user proof
file" [ipc §11]. Both pair a token with a kernel-checkable fact rather than relying on the token
alone.

**The kernel-checkable fact exists on both socket platforms, with a known asymmetry.**
`SO_PEERCRED` yields `{pid, uid, gid}` captured at `connect()`/`listen()` rather than at send
time [ipc §1.5]; `LOCAL_PEERCRED` returns effective UID and groups and **no PID**, and Apple's
advice is to bind authorization to the audit token rather than a PID [ipc §2.2, §7]. Windows has
the client's token through `ImpersonateNamedPipeClient`, and PID and computer name are
observations rather than authentication [ipc §3.3, §7].

**Named pipes remove the reverse-connection problem only by half.** A pipe instance also runs
client-to-server; instances "vanish when the last handle closes" so there is no stale endpoint
[ipc §3.1], and the instance count is capped at 1-255 [ipc §3.1]. Nothing in the sheet gives a
server a way to initiate toward a pipe client either.

**Precedent for parked reverse connections.** The sheet does not record one: D-Bus, Wayland,
PostgreSQL, Redis and the SSH agent are all request/response or client-driven-stream protocols
over one long-lived connection [ipc §7], and the one system that needs server-initiated bulk
transfer — Wayland — solves it with `SCM_RIGHTS`, which is the route closed above. The absence
is itself evidence: a reverse pool is weida's own construction and must be justified as such,
not by precedent.

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — socketpair over a control connection | either side mints a pair per transfer and passes an end | needs `SCM_RIGHTS`: unavailable on Windows `AF_UNIX` [ipc §3.6] and on named pipes, unstable in `std` [ipc §11], and `unsafe`-or-dependency here. B-039 could not inherit it |
| B — control connection + token-bound transfer connections | the first connection is the peer; each further one proves membership with a token *and* matching kernel credentials | a 16-byte token per peer is new L0 state, and server-initiated streams still need an answer |
| C — one long-lived connection, framed transfers | what D-Bus, Wayland and PostgreSQL do [ipc §7] | this is 0010's rejected Option C: a multiplexer with stream ids, flow control and resets |
| D — the client also binds a socket and tells the server | symmetric addressing | doubles the socket hygiene (path budget, permissions, stale files) on the side least able to own a directory [ipc §1.2, §7], and named pipes would need the client to be a server |
| E — B plus **parked reverse connections** | B, and the client parks spare connections the server may take when it needs a stream | a parked connection is a file descriptor held open for a stream that may never come; running out is a drop |

## 4. Decision

Option E: B for the direction that works, plus a bounded pool of parked connections for the
direction that does not.

1. **The first connection a client opens to a local endpoint is the peer's control
   connection.** HELLO travels both ways on it — the dialling side writes its HELLO frame, the
   accepting side writes its own back on the same connection — and it lives as long as the peer
   does. Everything the runtime holds per connection today (`Agreed`, sequence scopes, the dedup
   window, the parked drain receipts, the subscription registry entry) belongs to it, which is
   how a per-peer guarantee survives a transport whose connections are per transfer. This is
   [0002](0002-control-and-bulk-separation.md) §6.2's control connection with its local
   consequence made explicit; 0010 §4.2 already permits it ("one more local connection,
   distinguished by nothing but its use").

2. **Every further connection carries a group token in a local preamble, and is accepted only
   if the kernel agrees.** The server issues **16 random bytes** in its HELLO reply on the
   control connection. A transfer connection opens with that token, and the server accepts it
   only when the token matches a live control connection **and** the kernel credentials of the
   new connection match that control connection's: the uid always, and the pid where the
   platform reports one. A mismatch is refused and the connection closed with `LIMIT_EXCEEDED`'s
   sibling `PROTOCOL_VIOLATION`; nothing is dispatched from an unbound connection. This is the
   local analogue of the fingerprint binding that groups a peer's QUIC connections
   ([0002](0002-control-and-bulk-separation.md) §6.3, [0008](0008-session-identity.md) §4.2),
   with the kernel in the prover's role that 0010 §4.4 gave it.

   The token is **not** a credential and MUST NOT be treated as one: it names a group, the
   kernel says who is asking, and the two together are what admit a connection. A token that
   leaked to another uid still fails the credential check; a peer with the right uid and no
   token is a *different* peer, not an impostor.

3. **A transfer connection is dispatched by path alone**, which `docs/PROTOCOL.md` §2.1 already
   specifies for local transports: the accepting side reads the preamble and the DATA header,
   looks the path up, and the pattern registered there says whether a reply is expected. A
   replier answers on the same connection; a puller or publisher path never writes back. The
   stream kinds of §4.1 are a QUIC vocabulary and have no place on the wire here.

4. **Server-initiated streams ride on parked reverse connections.** A client that subscribes
   parks spare connections carrying a `reverse` preamble with the same token; the server takes
   one when it needs to open a stream toward that peer, and the client replenishes what is
   consumed. The pool is bounded by a named `Limits` field, `max_parked_reverse`, on both sides,
   and counts against `max_local_streams` like any other live local connection.

   **A server that needs a stream and finds no parked connection drops that copy**, exactly as a
   publisher drops a copy for a subscriber whose byte budget is exhausted, and counts it in the
   same place (`Publisher::dropped`, [GUARANTEES.md](../GUARANTEES.md) §6 `Drop`). This is the
   honest mapping: fan-out is already the one place weida answers overload by discarding, and a
   reverse pool that blocks the publisher instead would make a local subscriber able to stall a
   publisher that serves remote ones.

5. **What the token is not.** Sixteen bytes per peer is new L0 state, and
   [0008](0008-session-identity.md) §4.5 forbids session state at L0. The rule survives because
   the token **binds connections and resumes nothing**: it carries no subscriptions, no sequence
   position and no dedup window; it is meaningless once the control connection closes; and a
   reconnecting peer gets a new one and starts over. That is precisely the line 0008 §4.2 draws
   for the fingerprint — "the identity that spans connections" — with the difference that a
   fingerprint is proved cryptographically and a token is merely unguessable, which is why §4.2
   does not let it stand alone.

6. **B-039 inherits this unchanged.** A named pipe instance is opened by the client exactly as a
   socket connection is; the first instance is the control connection, further instances carry
   the same preamble, and the credential check reads the client's token through
   `ImpersonateNamedPipeClient` [ipc §3.3] instead of `SO_PEERCRED`. No descriptor passing is
   required anywhere, which was the constraint that killed Option A.

7. **Named losses.** (a) A local peer costs at least two connections — the control connection
   plus one per live transfer — against `max_local_streams` (255, Windows' instance cap
   [ipc §3.1]). (b) The token is unguessable, not proved; the kernel check is what makes it
   safe, so a platform that reported no credentials at all could not use this shape. (c) The
   reverse pool trades file descriptors for the ability to receive fan-out, and an exhausted
   pool is a silent-to-the-publisher, counted-at-the-publisher drop. (d) A client that never
   parks a reverse connection cannot receive Pub/Sub at all — which is a *configuration* a
   subscriber chooses, and must be reported rather than inferred.
   (e) **On `AF_UNIX` a reset was indistinguishable from a FIN, so a cancelled transfer read
   as a complete one — the one named loss of these five that is now closed.**
   `crates/weida/src/unix.rs` implemented this transport's `finish` and `reset` identically —
   both `drop(writer)`, the code discarded — so `OutgoingTransfer::cancel`, a transfer dropped
   without `finish()` and a producer that died mid-payload all reached the reader as
   `Ok(truncated_bytes)` from `collect` and as EOF on the `AsyncRead`. The cost was the one
   guarantee a stream-shaped system cannot do without: "half a frame is not a frame"
   ([PATTERNS.md](../PATTERNS.md) §1.5, §4.1) silently stopped holding, and the application
   took a truncated payload for a whole one. It was also the only one of these five losses
   **not** inherent to the shape decided in §4, which is why it was filed to be closed rather
   than accepted (B-245).

   **The constraint is the kernel's, and it was established experimentally.** Linux offers no
   abort for an `AF_UNIX` stream socket: `SO_LINGER` with a zero timeout followed by `close` is
   byte-for-byte what a plain close produces at the reader — the data, then a clean EOF. The
   distinction can therefore only be carried **in the payload**, and there were exactly two
   shapes for that:

   - **Per-write framing, as [`pipe.rs`](../../crates/weida/src/pipe.rs) already had it.** Five
     bytes per write — a kind byte plus a `u32` length — a FIN chunk, and a RESET chunk
     carrying the code; no lookahead anywhere in the reader. Nothing per byte, and it makes the
     two socket transports one implementation of one framing rather than two conventions, which
     is the argument for it: §4.6's whole point is that a named pipe inherits this note
     unchanged.
   - **One end-of-stream marker per transfer.** One byte on a clean end and nine on a reset, so
     the per-write cost disappears — but the reader must hold back the last nine bytes of the
     payload until it sees EOF to know whether they were a marker, and that costs the zero-copy
     read `AF_UNIX` had, where `grouped::LocalRecv::read` hands the caller's own buffer
     straight to the socket. Cheaper on the wire, more expensive in the reader, and a second
     convention beside the pipe's.

   **B-245 took the first**, for the reason the loss existed at all — a convention per transport
   is how a distinction gets lost. The framing moved out of `pipe.rs` into
   `crates/weida/src/chunked.rs` and both socket transports use it, so there is one framing and
   one reader; the pipe's behaviour is unchanged byte for byte and the socket gained the RESET
   chunk it had no kernel equivalent for. What the framing still cannot carry on either socket
   transport is the reader's refusal, which is loss (b) above and unchanged.

## 5. Consequences and follow-ups

- **B-038 implements §4.1-§4.3 and §4.7(a)**: the control connection with the HELLO exchange,
  the group token with the credential check, path dispatch on transfer connections, the socket
  hygiene of 0010 §4.5 (explicit mode, unlink-then-bind, the decoded path budget), and
  `IncomingMeta::peer` carrying the local principal. Req/Rep and Push/Pull work over
  `weida+unix://`; Pub/Sub does not.
- **Follow-up item, filed with this note** (as B-048: B-047 is the id this note itself was
  written under, and ids are never reused):

  > ### B-048 — AF_UNIX: parked reverse connections for fan-out
  > kind: code | size: 90 | status: ready | needs: [B-038]
  > acceptance: a subscriber over `weida+unix://` parks spare connections carrying the `reverse`
  > preamble and its group token, bounded by the new `Limits::max_parked_reverse` and counted
  > against `max_local_streams`; the publisher takes one per fan-out copy and the subscriber
  > replenishes; a publisher that finds no parked connection drops that copy and counts it in
  > `Publisher::dropped`, with a test that exhausts the pool and observes the drop; a subscriber
  > that parks nothing is reported at subscribe time rather than silently receiving nothing;
  > GUARANTEES §6 gains the local fan-out row and PROTOCOL §2.1 the `reverse` preamble
  > ([decisions/0012](0012-local-connection-grouping.md) §4.4).

- **B-039 inherits** §4.1-§4.4 with `ImpersonateNamedPipeClient` in place of `SO_PEERCRED`, and
  should need no new decision.
- **[PROTOCOL.md](../PROTOCOL.md) §2.1** gains the local preamble: what a transfer connection
  sends before its first frame (the token, and later the `reverse` marker), and the rule that an
  unbound connection is refused. This is a *local* framing detail, not a wire-version change:
  nothing about it appears on a QUIC connection.
- **[INVARIANTS.md](../INVARIANTS.md)** gains `max_parked_reverse` as a named bound when B-048
  lands; `max_local_streams` already covers the connections themselves.
- **[GUARANTEES.md](../GUARANTEES.md) §6** records that Pub/Sub over a socket transport depends
  on the reverse pool, and that an exhausted pool is a counted drop.
- **Status was provisional** because the reverse pool of §4.4 had no precedent in the sheets
  and no measurement behind its bound. B-048 has run: the shape holds unchanged, the shared
  Pub/Sub test body passes over `AF_UNIX` exactly as over QUIC and inproc
  (`pub_sub_over_unix`), an exhausted pool is a counted drop that leaves the subscription
  alive (`an_exhausted_reverse_pool_drops_the_copy_and_counts_it`), and a subscriber that
  parks nothing is refused at subscribe time
  (`a_subscriber_that_parks_nothing_is_refused_at_connect`). The one thing the
  implementation sharpened is *when* a replacement is parked: on the first byte the peer
  writes, not at the end of the copy, so a long transfer does not shrink the pool for its
  duration.

## 6. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §2.1, §9.5; [GUARANTEES.md](../GUARANTEES.md) §6;
[INVARIANTS.md](../INVARIANTS.md); [0001](0001-sequence-field.md) §7.1;
[0002](0002-control-and-bulk-separation.md) §6.2, §6.3; [0008](0008-session-identity.md) §4.1,
§4.2, §4.5; [0009](0009-drain.md) §4.2; [0010](0010-local-transport.md) §4.2, §4.4, §4.5.

Research sheets: [ipc.md](../research/ipc.md) §1.2, §1.5, §2.2, §3.1, §3.3, §3.6, §7, §8.4, §11.
