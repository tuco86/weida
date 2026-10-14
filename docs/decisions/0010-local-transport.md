# 0010 — The local transport, per platform

- **Status:** accepted
- **Date:** 2026-09-11
- **Related:** SYNTHESIS §8.8; P18, P14; LOOP §9 A9; decisions 0002 §6.2, 0006 §4.6, 0008 §4.1

## 1. The question

SYNTHESIS §8.8 asks whether weida gets a local transport and which, and lists what `ipc.md`
§11 leaves open: the Linux default, the macOS default, the Windows default, the fallback
chains, and whether bulk payloads travel by handle rather than by bytes [SYNTHESIS §8.8]. The
roadmap already fixes the order — "inproc binding first; then `AF_UNIX` (Linux, macOS) and
named pipes (Windows) per `docs/research/ipc.md` §11, peer credentials as the identity"
[LOOP §9 A9] — so what this note decides is everything that sentence does not say, and two
things it quietly assumes: how a local address is written, and how "peer credentials as the
identity" survives contact with [0008](0008-session-identity.md), whose §4.1 says the proved
fingerprint is the peer identity "and it is the only one".

Three of weida's own contracts are in the way, and each has to be either kept or amended with
the reasoning recorded:

- "The transport is QUIC" and ALPN MUST be exactly `weida/0`, a mismatch failing the TLS
  handshake [PROTOCOL §2.1].
- An address is `weida://[sha256:<hex>@]host:port/path` with the port required
  [ARCHITECTURE §3].
- Identity is the SHA-256 of the leaf's `SubjectPublicKeyInfo`, proved by the handshake
  [ARCHITECTURE §2], [0008 §4.1].

## 2. The evidence, condensed

**A local transport is worth it, and the sheet's numbers say by how much.** `AF_UNIX` against
loopback TCP on the same box: **130,372 against 70,221 messages per second** at 100 bytes,
i.e. **7.67 µs against 14.24 µs** round trip, and 127,582 against 67,901 at 1 KiB [ipc §9]. A
Go ping-pong measurement puts one-way latency at **2.3 µs for a Unix socket against 3.6 µs for
loopback TCP** at 128 bytes, with the two converging around 13 GB/s at 16-32 MiB [ipc §9]. So
the win is latency and syscall overhead at small sizes, and it disappears for bulk.

**Shared memory is a different order, and that is the argument for deferring it rather than
skipping it.** A 2026 single-producer measurement: at 32 bytes, **270 ns** round trip for
shared memory against **5,910 ns** for a Unix stream; at 8 KiB, 14,360 ns against 23,280 ns
[ipc §9]. On Windows, a named pipe costs **0.025022 ms** against **0.004145 ms** for a
shared-memory section, over a 0.002099 ms baseline [ipc §9]. And there is no kernel zero-copy
path for `AF_UNIX` at all: passing a sealed memfd, a Mach memory entry or a file-mapping handle
is "the only route to zero-copy" [ipc §11], [ipc §9].

**In-process needs almost nothing.** libzmq's `inproc` passes messages in memory between
threads of one context with no I/O threads, its endpoint name being at most **256 characters**
[ipc §8.1]; NNG's inproc "tries hard to avoid copying data" and ignores `NNG_OPT_RECVMAXSZ`
because peers are "implicitly trusted" [ipc §8.2]. The sheet's summary of what an in-process
transport requires: "No credentials, and none needed", no independent socket endpoint, and "no
serialisation is required in principle"; what it gives up is fault containment — a peer's
death is a dropped channel half, and an abort takes the whole process [ipc §8.4].

**Linux: the default everyone uses, with three costs.** `AF_UNIX` `SOCK_STREAM` is a byte
stream with **no message boundaries**, so it needs framing [ipc §1.1]. A pathname must fit
`char sun_path[108]` including its NUL — **107 bytes** usable [ipc §1.1]. A new socket file
gets every permission bit not masked by `umask`, so the default is whatever the process
inherited and must be overridden [ipc §1.2]. Closing does not remove the node, so a crash
leaves it and `bind()` returns `EADDRINUSE`; unlink-then-bind is the usual answer and creates a
substitution race "unless directory ownership and permissions prevent endpoint substitution"
[ipc §1.2], [ipc §7]. `SO_PEERCRED` yields `struct ucred { pid, uid, gid }`, captured at
`connect()`/`listen()`/`socketpair()` rather than at send time [ipc §1.5].

**The two Linux alternatives each fail on something specific.** `SOCK_SEQPACKET` removes the
framing layer and preserves boundaries and order, but is unavailable on macOS and Windows and
Tokio has no seqpacket type at all — `UnixStream` is stream-only, `UnixDatagram` datagram-only
— so it would need a lower-level reactor-integrated socket crate [ipc §10], [ipc §11]. The
abstract namespace removes stale state entirely, since it has no filesystem node and vanishes
at last close, but "Abstract sockets have no permissions at all" and it is reachable by every
process in the same network namespace and none outside, which breaks containers [ipc §1.2],
[ipc §11].

**macOS: the same shape, tighter.** `AF_LOCAL` `SOCK_STREAM`, with `SOCK_SEQPACKET`
unsupported so framing is mandatory; `sun_path` is exactly **104 characters**, and an App Group
container path plus a team-ID-prefixed group name consumes most of that, so the expanded path
must be checked before bind [ipc §2.1], [ipc §6.1]. `LOCAL_PEERCRED` returns effective UID and
groups captured at connect/listen and **no PID**; Apple's own advice is to bind authorization
to the audit token rather than a PID [ipc §2.2], [ipc §7]. XPC is the only surveyed mechanism
with launchd-supervised on-demand start, crash restart and documented code-signing
requirements on the peer, at the cost of being macOS-only and opaque, with bundled services
private to their app [ipc §2.4], [ipc §11]. Measured: Unix against XPC round trip **11 against
12 µs** at 10 bytes, 11 against 15 µs at 1 KiB, and connect-plus-10-bytes **32 against 95 µs**
[ipc §9].

**Windows: kernel framing, and a security default that must be overridden.** A message-mode
pipe makes every `WriteFile` a message unit, so framing is the kernel's; an undersized read
buffer returns `ERROR_MORE_DATA` and the remainder is read again, and there is no documented
maximum message size [ipc §3.1]. Instances vanish when the last handle closes, including on
owner death: no stale endpoint [ipc §3.1]. With NULL security attributes the default descriptor
grants "read access to Everyone and the anonymous account", so an explicit DACL is required
[ipc §3.2]. SMB remote access is the default and `PIPE_REJECT_REMOTE_CLIENTS` is what makes a
pipe local-only [ipc §3.1]. Two accept-loop races have to be handled: a client connecting
between `CreateNamedPipe` and `ConnectNamedPipe`, which then returns `ERROR_PIPE_CONNECTED`
although the connection is good, and `ERROR_PIPE_BUSY` when all instances are busy, requiring
`WaitNamedPipe` and a retry [ipc §3.1], [ipc §11]. Identity is the client's token through
`ImpersonateNamedPipeClient`, which only the server may do and must follow with
`RevertToSelf`; PID and computer name are observations, not authentication [ipc §3.3],
[ipc §7]. Instances are limited to **1-255** [ipc §3.1].

**Windows `AF_UNIX` is not the single code path it looks like.** Stream-only; no ancillary data
and therefore no handle passing; no `socketpair`; no documented peer credentials; a
reparse-point node that needs `DeleteFile` before rebind, so stale state returns; abstract
support reported nonfunctional; AppContainer support undocumented; and **neither Rust `std`
nor Tokio supports it** [ipc §3.6], [ipc §10], [ipc §11].

**What the kernel proves, and what weida proves.** "Without TLS, a local server learns who
connected from the kernel" [ipc §7]: a PID/UID/GID snapshot on Linux, an effective UID without
a PID on macOS, an impersonation token on Windows. weida instead accepts a peer for a key it
proved it holds, and "authentication is not authorization" — the application decides on
`IncomingMeta::peer` [ARCHITECTURE §2].

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| A — no local transport | loopback TCP/QUIC only | status quo [PROTOCOL §2.1] | 2× the round-trip latency of a Unix socket at small sizes [ipc §9], a port and a certificate for two processes on one machine, and no kernel-proved peer |
| B — QUIC over a local datagram transport | keep the whole stack, swap the socket | none in the sheet | named pipes cannot carry it at all, and it keeps TLS where the kernel already authenticates |
| C — a local byte transport with a multiplexing frame layer | one connection, weida frames streams over it | ZeroMQ/NNG on `ipc://` [ipc §8.1] | reimplements QUIC's stream layer — flow control, stream ids, resets — in the one place the project set out not to |
| D — a local byte transport with **one connection per transfer** | the OS connection *is* the stream | `socketpair` role-A topology [ipc §11]; the invariant's own wording | a connection per transfer, bounded by named-pipe instances (1-255) [ipc §3.1] and by file descriptors; no multiplexing means no per-connection window either |
| E — shared memory first | memfd / Mach entry / file-mapping handle | the numbers: 270 ns against 5,910 ns at 32 B [ipc §9] | it is a data path, not a transport: it still needs a control channel, and handle passing is absent on Windows `AF_UNIX` [ipc §3.6] |

## 4. Decision

Option D, in the roadmap's order, with shared memory deferred as a data path rather than
rejected.

1. **weida gets local transports, in three slices: inproc, then `AF_UNIX`, then named pipes.**
   Inproc comes first because it needs no platform decision at all — no credentials, no socket
   node, no framing choice [ipc §8.4] — and because it is the cheapest place to prove that the
   transport boundary in the runtime is real.

2. **The OS connection is the stream: one local connection per transfer.** This keeps the
   invariant literally rather than approximately — "one data flow maps naturally to one
   transport stream where the transport supports it" [INVARIANTS] — and it is what lets a local
   transport exist without reimplementing QUIC's stream layer (Option C). Consequences taken
   deliberately: a transfer costs a `connect()` (7.67 µs round trip at 100 bytes on Linux
   [ipc §9], 32 µs connect-plus-10-bytes on macOS [ipc §9]); there is no per-connection window,
   so the head-of-line coupling [0002](0002-control-and-bulk-separation.md) exists to remove
   cannot arise locally; and the count of live local connections becomes a named bound,
   `max_local_streams`, because Windows caps pipe instances at 1-255 [ipc §3.1] and every
   platform caps descriptors. A control connection per peer (0002 §6.2) stays exactly as it is:
   one more local connection, distinguished by nothing but its use.

3. **No TLS on a local transport, and the kernel is the prover.** Running TLS over an inproc
   channel would require a certificate for a function call; running it over `AF_UNIX` would
   encrypt against an attacker who by the sheet's own account is already the same user or root
   [ipc §7]. So a local binding has no `Identity` and no `Trust`, and ALPN — which fences the
   wire version at the TLS layer [PROTOCOL §2.1] — is replaced by the mechanism that was always
   doing the real work: HELLO's `versions` intersection [PROTOCOL §2.3]. The named loss is
   explicit: **no confidentiality and no integrity against anything that can read the socket or
   the address space**, which is why §4.5 makes the socket's permissions part of the decision
   rather than an implementation detail.

4. **`IncomingMeta::peer` gains a second kind of proved identity, and 0008 §4.1 is amended by
   this note.** 0008's rule was that identity is proved and never claimed, and its text made
   the fingerprint the only one; a local peer is proved by the kernel, which is a stronger
   statement than a certificate makes, so the rule survives and the text does not. The identity
   becomes a sum: a key (`Fingerprint`) or a local principal — Linux `uid`/`gid`/`pid` from
   `SO_PEERCRED`, macOS effective `uid`/groups from `LOCAL_PEERCRED` with **no PID**, Windows
   the client's token SID through `ImpersonateNamedPipeClient` [ipc §1.5], [ipc §2.2],
   [ipc §3.3]. Three rules come with it: a PID is an observation and MUST NOT be the thing
   authorized on [ipc §7]; the credential is the one captured at connect time, not at send time
   [ipc §1.5]; and an inproc peer has **no** identity at all, because there is nothing to prove
   [ipc §8.4] — `peer` is `None` there, exactly as it is for an anonymous TLS client.

5. **Per platform, with the rejected alternatives named.**

   - **Linux: `AF_UNIX` `SOCK_STREAM` on a filesystem path**, created in a directory whose
     ownership and permissions prevent endpoint substitution, with the mode set explicitly
     rather than inherited from `umask`, and unlink-then-bind at startup [ipc §1.2]. The path
     budget is **107 bytes** and is validated before bind [ipc §1.1]. `SOCK_SEQPACKET` is
     rejected: it buys framing weida does not need under §4.2, and Tokio has no type for it
     [ipc §10]. The abstract namespace is rejected as a default because it has "no permissions
     at all" and cannot cross a network namespace [ipc §1.2].
   - **macOS: `AF_LOCAL` `SOCK_STREAM`**, the same shape with a **104-character** budget that
     is checked against the *expanded* path, because an App Group container path consumes most
     of it [ipc §2.1], [ipc §6.1]. XPC is **not** the library's transport: it is macOS-only and
     opaque, and its value — launchd-supervised start and restart [ipc §2.4] — is a deployment
     property an application can have without weida speaking XPC.
   - **Windows: named pipes in message mode**, with an explicit DACL (the default grants
     "read access to Everyone and the anonymous account" [ipc §3.2]),
     `PIPE_REJECT_REMOTE_CLIENTS` always set [ipc §3.1], and both accept-loop races handled —
     `ERROR_PIPE_CONNECTED` treated as success and `ERROR_PIPE_BUSY` retried after
     `WaitNamedPipe` [ipc §3.1]. Windows `AF_UNIX` is rejected: neither `std` nor Tokio
     supports it, and it reintroduces stale state while dropping handle passing [ipc §3.6].

6. **No automatic fallback between transports.** The sheet's per-platform chains end in
   "loopback TCP with a token file" [ipc §11]; weida will not walk them by itself. Falling back
   from a kernel-authenticated local socket to a loopback port silently changes who can connect
   and what proves them, which is the same class of act as an adapter inventing a guarantee
   [INVARIANTS], [0006 §4.7]. An address names one transport; an operator who wants a fallback
   configures the second address and accepts the different identity model in the open.

7. **Bulk payloads stay bytes for now, and the zero-copy route is named rather than taken.**
   There is no kernel zero-copy path for `AF_UNIX`, and a sealed memfd, Mach memory entry or
   file-mapping handle is "the only route" [ipc §11]; the numbers make the prize concrete — 270
   ns against 5,910 ns at 32 bytes, and 0.004 against 0.025 ms on Windows [ipc §9]. It is not in
   these slices because it needs a capability Windows `AF_UNIX` lacks entirely [ipc §3.6], and
   because it changes the payload model rather than the transport: it is a decision of its own,
   taken when a measured need exists.

8. **A local address is written with an explicit scheme, and the fingerprint form is
   forbidden.** `weida://host:port/path` is unchanged [ARCHITECTURE §3]; local transports get
   their own schemes, each naming its endpoint in the authority and keeping the opaque endpoint
   path exactly as it is:

   - `weida+inproc://<bus-name>/<path>` — a name unique to the process, at most 256 bytes,
     which is libzmq's own budget for the same thing [ipc §8.1];
   - `weida+unix://<percent-encoded-socket-path>/<path>` — percent-encoded because a socket
     path contains the separator the endpoint path also uses, and validated against 107 bytes
     on Linux and 104 on macOS *after* decoding [ipc §1.1], [ipc §2.1];
   - `weida+pipe://<pipe-name>/<path>` — mapped to `\\.\pipe\<pipe-name>`, never a UNC path,
     which is the address-level half of `PIPE_REJECT_REMOTE_CLIENTS` [ipc §3.1].

   The `sha256:…@` userinfo form MUST be rejected on all three: there is no key to pin, and an
   address that looks like it authenticates but does not is worse than one that plainly does
   not. Who may connect is stated in the binding's configuration as accepted local principals.

## 5. Consequences and follow-ups

- **[ARCHITECTURE.md](../ARCHITECTURE.md) §2, §3.** Identity becomes the sum of §4.4 and the
  address section gains the three schemes of §4.8 with their validation rules. §2's transport
  paragraph stops implying that QUIC is the only transport.
- **[PROTOCOL.md](../PROTOCOL.md) §2.1.** "The transport is QUIC" becomes the statement for the
  network transport, beside a local-transport paragraph: same frames, same HELLO, same
  negotiation, no TLS and therefore no ALPN, with the version fenced by `versions` in HELLO
  instead. Nothing in §3-§9 changes, which is the point of §4.2.
- **[INVARIANTS.md](../INVARIANTS.md).** `max_local_streams` joins the named bounds before the
  code exists (§4.2). The identity row records that a local peer is proved by the kernel and an
  inproc peer is not proved at all because there is nobody else to prove.
- **[0008](0008-session-identity.md) §4.1 is amended by this note**, in the way INVARIANTS
  requires of an invariant: the reasoning is here, the rule ("proved, never claimed") is
  unchanged, and only the claim that the fingerprint is the *only* identity gives way.
- **[GUARANTEES.md](../GUARANTEES.md) §6.** The peer-identity row gains the local kinds; the
  backpressure row notes that a local transfer has no connection window, so `Block` is the
  socket's own buffer rather than a shared budget.
- **Roadmap A9 becomes three items**, in the order of §4.1, each with the platform rules of
  §4.5 as its acceptance. These transports remain weida transports; similarly named ZeroMQ
  transports keep their own protocol semantics.
- **[SYNTHESIS.md](../research/SYNTHESIS.md) §8.8** is closed by this note. Every question in
  §8 is now decided.

## 6. Sources

weida documents: [ARCHITECTURE.md](../ARCHITECTURE.md) §2, §3; [PROTOCOL.md](../PROTOCOL.md)
§2.1, §2.3; [INVARIANTS.md](../INVARIANTS.md); [GUARANTEES.md](../GUARANTEES.md) §6;
[LOOP.md](../LOOP.md) §9 A8, A9; [0002](0002-control-and-bulk-separation.md) §6.2;
[0006](0006-guarantee-sets.md) §4.6, §4.7; [0008](0008-session-identity.md) §4.1.

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §8.8;
[ipc.md](../research/ipc.md) §1.1, §1.2, §1.5, §2.1, §2.2, §2.4, §3.1, §3.2, §3.3, §3.6, §6.1,
§7, §8.1, §8.2, §8.4, §9, §10, §11.
