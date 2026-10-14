# 0031 — Transparent redial, and the outbox that makes a `send` a message

- **Status:** accepted
- **Date:** 2026-09-18
- **Accepted:** 2026-10-13
- **Items:** B-270, B-271, B-272, B-273; amends B-259
- **Supersedes:** [ARCHITECTURE.md](../ARCHITECTURE.md) §6c point 1 ("no mute state"),
  [PATTERNS.md](../PATTERNS.md) §1.8 ("Nothing reconnects")
- **Related:** [0008](0008-session-identity.md) §4.5, [0023](0023-completion-is-a-cursor.md) §6,
  [0001](0001-sequence-field.md) §7, [PATTERNS.md](../PATTERNS.md) §1.11,
  [GUARANTEES.md](../GUARANTEES.md) §1, §3, [INVARIANTS.md](../INVARIANTS.md),
  `crates/zmq/weida-zmq` (B-073, B-078, B-104)

## 1. The question

weida is ZeroMQ's idea on QUIC, and one of ZeroMQ's ideas is that **a dialled endpoint outlives
its connection**: `zmq_connect` registers an address, the socket redials it with
`ZMQ_RECONNECT_IVL` backoff for as long as the socket lives, a message handed to the socket sits
in the endpoint's pipe until a connection can carry it, and the application never sees the
connection at all. That is not a convenience on top of the pattern family; it is what makes
`PUSH` fire-and-forget and what lets a `SUB` be started before its `PUB`.

weida's core has none of it, on purpose and in writing: "Nothing reconnects. The application
calls `connect` again" ([PATTERNS §1.8]); "a program that never connected should be told so, not
hang… worth revisiting together with reconnect logic: blocking-until-peer would live in
`PeerSet::pick` behind an awaitable peer-list change, and is deliberately not built now"
([ARCHITECTURE §6c] point 1). Meanwhile the repository's own ZeroMQ implementation has the
whole mechanism, merged and tested: an `Engine` whose pipe "belongs to the endpoint, not to the
connection", `ZMQ_RECONNECT_IVL`/`_IVL_MAX` backoff, `ZMQ_IMMEDIATE`, re-subscription on redial
that "needed no reconnect logic" of its own, and a monitor stream (B-073, B-078, B-104).

The owner's framing, which this note adopts, is a rule about **responsibility**: a message is a
complete unit and can therefore be handed to the runtime; a stream is unfinished until its FIN
and stays the application's. So the question is where exactly the hand-over happens, what the
runtime owes for what it holds, and what a redial must and must not restore.

## 2. What the code actually does

Five facts, each read rather than assumed.

**The unit of `connect` is a connection.** `PeerSet` holds `PeerEntry { conn, path }`
(`crates/weida/src/stream.rs`); `Peer::dial` dials once, pushes the entry and returns it. A
closed connection stays in the list until the next `add` reaps it, and `pick` returns
`Error::NotConnected` for an empty list or the `LossCause` of the last closed peer for a list
with no live one. Nothing owns the URL after the dial.

**The API already separates a message from a stream.** `Pusher::send(body)` "returns once the
FIN is queued" with the receipt discarded; `Pusher::open(meta)` returns an `OutgoingTransfer`
whose `finish()` yields a `Delivery` (`crates/weida/src/endpoint.rs`). `Requester::request`
versus `Requester::open`, and `Pair` likewise. The caller of `send` has already surrendered a
complete, owned buffer; the caller of `open` writes at its own pace and holds the source.

**Loss during a reply is `Indeterminate`, and must stay so.** `indeterminate_on_loss` in
`crates/weida/src/transfer.rs` relabels a `ConnectionLost` while awaiting a reply, because the
request may have been applied; [GUARANTEES §5] forbids collapsing that into either outcome.

**A subscriber already re-sends on `connect`.** `Subscriber::connect` registers the path as a
transfer route in the connection's namespace, fills the reverse pool where the transport needs
one, and sends every filter in `SubState.filters` (`crates/weida/src/endpoint.rs`). A redial
needs exactly that sequence and nothing else — the filters are already sender-side state.

**A peer is bound by its fingerprint.** `ClientPool` refuses a second connection to an authority
that answers with a different key than the first ([0008 §4.2], `crates/weida/src/pool.rs`). A
redial is a second connection.

## 3. Options

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| A — keep "nothing reconnects" | today | none among the surveyed systems; every ZeroMQ, NNG, NATS, MQTT and AMQP client redials | every application writes the same backoff loop; `Pusher::send` cannot be fire-and-forget because it needs a live peer at the call; B-259 has to teach a loop instead of a property |
| B — redial in the runtime, hold `send` bodies until written | ZeroMQ: endpoint owns the queue, connection comes and goes; at-most-once | `weida-zmq` `Engine`, libzmq | a body written to a connection that then dies is gone, as in ZeroMQ; a bounded outbox is new sender-side memory |
| C — B, and hold `send` bodies until the transport receipt, resending on loss | at-least-once at L0 | MQTT QoS 1 | duplicates when the connection dies after receipt and before the ack; needs the [0001] counter to survive the reconnect and a dedup window per (peer, endpoint) at every receiver; `Indeterminate` for Req collapses unless the request is declared idempotent |
| D — B, and hold completed streams too, up to a size threshold | "a message is a stream that reached FIN", so retain any finished stream the runtime can afford | none | behaviour changes at an arbitrary byte count: the same program gets at-most-once below the threshold and today's semantics above it, and nobody chose the number |

C and D were the owner's first two framings and both were withdrawn by the owner. D because the
threshold is arbitrary and makes behaviour depend on payload size rather than on what the caller
asked for. C because a stream's remedy is already exact and already the application's: it
notices the connection is unrecoverable, abandons the stream, and opens a new one — the
[PATTERNS §1.11] table, unchanged — and a message resent by the runtime after a possible receipt
is a second delivery nobody asked for.

## 4. The decision

**Option B.** Responsibility moves from the caller to the runtime at the API boundary, not at a
size, and the runtime redials on the caller's behalf.

**4.1 A dialled address outlives its connection.** `connect(url)` records a *slot* — the URL,
the trust configuration it was dialled with, and the current state: live with a connection, or
down with the `LossCause`, the attempt count and the next redial instant. The slot exists until
the endpoint is dropped or `disconnect(url)` is called. A background task per slot redials with
the policy of §4.5. `peer_count` counts live slots, as it counts live connections today.

**4.2 `send` is a message, and the runtime owns it from the call.** For `Pusher` and a
dialling `Pair`, `send(body)` returns once the body is either written to a live connection
(the fast path, no copy, exactly today's code) or copied into the endpoint's **outbox** because
no connection is live or the outbox is not yet empty (order is preserved). The outbox is drained
in order onto the next live connection. It is bounded in messages and in bytes
(`RuntimeConfig::outbox_messages`, `RuntimeConfig::outbox_bytes`; defaults 1000, ZeroMQ's HWM,
and 8 MiB — on `RuntimeConfig` rather than `Limits`, because `Limits` is a per-connection
profile and an outbox is per endpoint), and a full outbox does what
`RuntimeConfig::outbox_full` says in the vocabulary of [GUARANTEES §3]: `Block` by default,
`Drop` as a counted discard, `Reject` as `LimitExceeded`. It is a **local** setting rather than
the negotiated backpressure dimension, because a puller has no say in how its pusher waits. A
body larger than `outbox_bytes` on its own is refused at the call, by name, rather than
silently blocking forever. `Requester::request` needs no outbox: its caller awaits the reply
anyway, so waiting for a peer in `open` (§4.4) is the same wait with nothing to copy.

**4.3 The runtime's responsibility for a message ends when it is written.** A body handed to a
connection that then dies before the peer's transport received it is gone, and nothing resends
it. This is ZeroMQ's contract exactly, it is at-most-once, and it keeps [0001 §7]'s
per-connection counter, [0008 §4.5]'s absence of session state and [PATTERNS §1.11]'s "never a
resend" true without qualification. A caller who needs to know that a message arrived uses
`open` and awaits the `Delivery`, as today.

**4.4 `open` is a stream, and stays the application's.** `open` with no live peer **waits** for
one — the awaitable peer-list change [ARCHITECTURE §6c] point 1 sketched — bounded by
`RuntimeConfig::send_timeout` (ZeroMQ's `ZMQ_SNDTIMEO`; default unbounded, as ZeroMQ's). An
`OutgoingTransfer` on a connection that dies fails with `ConnectionLost(cause)` before FIN and
the reply half with `Indeterminate` after it, unchanged. The application abandons it and opens
a new stream, which lands on the next live slot. The runtime holds no stream and no part of
one. `Error::NotConnected` remains the answer of an endpoint that was **never** connected —
no slot exists — so a program that forgot to `connect` is still told so rather than hanging.

**4.5 The redial policy is configuration with ZeroMQ's shape and better defaults.**

```text
ReconnectPolicy {
    initial:  100 ms      // ZMQ_RECONNECT_IVL
    max:      30 s        // ZMQ_RECONNECT_IVL_MAX; libzmq's 0 means "constant", the worse default
    jitter:   true
    stop:     []          // ZMQ_RECONNECT_STOP: ConnectionRefused | HandshakeFailed | AfterDisconnect
    give_up:  never       // or after N attempts / a duration
}
```

`ReconnectPolicy::never()` is today's behaviour: the slot is dropped on the first loss and the
next operation reports it. The policy lives on `RuntimeConfig` and may be overridden per
endpoint; the exact surface is B-270's.

**4.6 A redial restores a transport, not a registration — except the one the client owns.**
[0008 §4.5] stands: no session, no sequence position, no server-side subscription survives. What
the redial task does after a successful dial is precisely what `Subscriber::connect` does today
on the new connection: register the route, refill the reverse pool, re-send the filters held in
`SubState.filters`. A subscriber therefore sees a gap, not a resumption, and B-259's sentence "a
re-sent subscription is a new subscription" stays the rule.

**4.7 A redial that reaches a different peer is not a reconnect.** The pool compares a new
connection against a peer's *live* ones only, so after a total loss it would accept a
replacement server with a new key as a new peer. The slot therefore pins the key its first
connection proved into the redialled address, and the TLS verifier refuses a different one in
the handshake — before any connection exists for anyone to use. The slot reports
`PeerChanged` through §4.8 and stops. A server whose identity is generated afresh per process
therefore cannot be transparently reconnected across a restart — by construction, and the
event says so rather than the outbox silently draining into a stranger. A local peer proves a
principal or nothing, and neither is pinned (§5).

**4.8 Every transition is observable.** The ZeroMQ complaint this note is not allowed to inherit
is that a socket reconnects and the application cannot react. Each dialling endpoint exposes a
bounded event stream:

```text
PeerEvent::Connected  { url, fingerprint }
PeerEvent::Lost       { url, cause: LossCause }
PeerEvent::Retrying   { url, attempt, delay }
PeerEvent::GaveUp     { url, cause }          // policy exhausted, or PeerChanged
```

The shape is `weida-zmq`'s `Monitor` and ZeroMQ's `zmq_socket_monitor`, without the side socket:
a typed stream on the endpoint whose lag is counted, so an application that does not read it
costs nothing and one that reads late knows how much it missed. This is the hook an application
uses to invalidate whatever it cached about the peer — the thing 0008 §4.5 says is not resumed.

**4.9 What an outbox is not.** It is sender-local state with a sender-local owner and a
sender-configured bound, which is why it does not contradict [0008 §4.5]'s refusal of L0
session state: "a retained session is remote-controlled state that the stream core has no owner
for", and no remote input can grow an outbox. It is not a queue in [0018]'s sense either — it
issues no `Accepted`, reports no cursor, and is gone with the process.

**4.10 Every scheme redials; only the waiting differs.** A slot is a slot whether the address
is `weida://`, `weida+unix://`, `weida+pipe://` or `weida+inproc://`, and the owner's rule is
that the application sees no difference: a ZeroMQ program talking over `ipc://` or `inproc://`
to a peer that restarts is not asked to do anything either. What differs is how the runtime
learns that the peer is back. Over `AF_UNIX` and named pipes the other process restarts and
re-binds, so the policy of §4.5 applies unchanged — a dial fails until the socket file or pipe
name answers again. In process there is no network to fail; a bus disappears only when its
binding is dropped, and it reappears exactly when a name is registered again. So the inproc
redial is not a timer but a wait on the name registry, the shape `weida-zmq` already built for
connect-before-bind ("parks rather than polls", `wait_until_bound` registering its `Notified`
before checking `is_bound`, B-080). `ReconnectPolicy` still governs `stop` and `give_up`
there; `initial` and `max` have nothing to wait for. The events of §4.8 are emitted identically.

## 5. What this does not decide

**At-least-once at L0.** Option C stays open as a guarantee-set dimension (`Delivery`,
[GUARANTEES §3]) rather than a behaviour of `send`. Should it be wanted, the pieces are named:
retain until receipt, the [0001] counter continuing across the reconnect under the fingerprint
0008 already made stable, and a bounded dedup window per (peer, endpoint). Nothing in §4 makes
that harder; §4.3 simply does not do it.

**Request replay.** `request` queues while no peer is live (§4.2) and never replays after the
write (§4.3). A built-in Lazy Pirate for requests declared idempotent would be a separate
decision with [FAILURE_MODEL.md](../FAILURE_MODEL.md) as its referee.

**Bind-side behaviour.** A `Replier`, `Puller` or `Publisher` has no address to redial. An
accepted peer's queue dies with its connection, exactly as `weida-zmq`'s does and as the RFCs
say.

**Whether a local peer's identity can change.** §4.7's `PeerChanged` has no meaning where the
peer is proved by the kernel or not at all ([0010](0010-local-transport.md) §4.4): a re-bound
`AF_UNIX` socket or inproc bus is accepted as the same peer, because nothing named it before
either. Whether a local principal change on redial (a different uid re-binding the socket path)
should be a refusal is left to the item that finds a reason for it.

## 6. Consequences and follow-ups

- **[PATTERNS.md](../PATTERNS.md) §1.8** loses "Nothing reconnects" and gains §4.1, §4.4 and
  §4.6; **§1.11** keeps "never a resend" for streams and qualifies "does not remember a payload"
  with the outbox of §4.2.
- **[ARCHITECTURE.md](../ARCHITECTURE.md) §6c** point 1 is rewritten: the mute state exists
  for a *connected* endpoint whose peers are down, and `NotConnected` is kept for the endpoint
  that never connected.
- **[0008](0008-session-identity.md) §4.5** gains one amending sentence: "no reconnect logic
  enters the runtime" is superseded for the transport half by this note; the session half is
  unchanged.
- **[INVARIANTS.md](../INVARIANTS.md)** gains the two bounds of §4.2 and the event-stream lag
  bound of §4.8 when B-270 and B-273 land.
- **B-259** is re-scoped: the chapter teaches what a redial restores and what it does not, on a
  program that watches `PeerEvent` rather than one that loops on `connect`.

### B-270 — A dialled address outlives its connection: slots, redial policy, events
kind: code | size: 90 | status: done 76cbb28 | needs: []
acceptance: §4.1, §4.5, §4.7, §4.8 and §4.10 in `crates/weida`: `PeerSet` holds slots keyed by URL and trust, a per-slot task redials with `ReconnectPolicy` (backoff with jitter, `stop` causes, `give_up`), `ReconnectPolicy::never()` reproduces today's tests unchanged, a redial answering with a different fingerprint does not go live and reports `PeerChanged`, and every transition is delivered on a bounded, lag-counting `PeerEvent` stream per endpoint. On `weida+inproc://` the redial is a wait on the name registry rather than a timer; on `AF_UNIX` and named pipes it is the same policy as QUIC. Tests: a pusher whose QUIC server restarts delivers again with no second `connect`; the same over `AF_UNIX` with the listener re-bound on the same path, and over inproc with the bus dropped and re-registered under the same name, each asserting the same `Lost`, `Retrying`, `Connected` event order; a policy of `never` reports `ConnectionLost` as before; a QUIC server that comes back with a new key yields `GaveUp { PeerChanged }`.

### B-271 — `open` waits for a live peer; `NotConnected` means never connected
kind: code | size: 45 | status: done 76cbb28 | needs: [B-270]
acceptance: §4.4: `PeerSet::pick` awaits a peer-list change when every slot is down, bounded by `RuntimeConfig::send_timeout`, and returns `NotConnected` only for an endpoint with no slot. The `after_the_server_restarts_the_pusher_must_reconnect` test is replaced by one that opens a transfer during the outage and sees it complete on the redialled connection; a `send_timeout` test sees the timeout error with the last `LossCause` in it.

### B-272 — Re-subscribe on the redialled connection
kind: code | size: 30 | status: done 76cbb28 | needs: [B-270]
acceptance: §4.6: the redial task performs the route registration, reverse-pool fill and filter re-send that `Subscriber::connect` performs today, factored so there is one implementation of that sequence. Test: a subscriber whose publisher restarts receives the next publish after the restart with no application call; a filter subscribed *during* the outage is present after it.

### B-273 — The outbox: `send` returns when the runtime owns the body
kind: code | size: 60 | status: done 76cbb28 | needs: [B-270, B-271]
acceptance: §4.2 and §4.3 for `Pusher` and a dialling `Pair`: the fast path is byte-identical to today's `open` + `write_all` + `finish`; with no live peer the body is copied into a per-endpoint outbox bounded by `RuntimeConfig::outbox_messages` and `RuntimeConfig::outbox_bytes`, drained in order after the redial, with `Block` at the bound by default and a counted drop under `OutboxFull::Drop`; a body over `outbox_bytes` is refused at the call. Tests: 100 sends during an outage arrive in order after it; the fourth send at a bound of three blocks until the drain begins; a `Drop` endpoint counts what it discarded; a body written before the restart is delivered once (asserted by the puller's sequence after the redial).

## 7. Sources

weida documents: [PATTERNS.md](../PATTERNS.md) §1.8, §1.11; [ARCHITECTURE.md](../ARCHITECTURE.md)
§6c; [GUARANTEES.md](../GUARANTEES.md) §1, §3, §5; [INVARIANTS.md](../INVARIANTS.md);
[0001](0001-sequence-field.md) §7; [0008](0008-session-identity.md) §4.2, §4.5;
[0018](0018-minimal-broker.md); [0023](0023-completion-is-a-cursor.md) §6.

weida code: `crates/weida/src/stream.rs` (`PeerSet`, `Peer::dial`), `crates/weida/src/endpoint.rs`
(`Pusher::{open, send}`, `Subscriber::connect`), `crates/weida/src/transfer.rs`
(`indeterminate_on_loss`), `crates/weida/src/pool.rs` (fingerprint binding),
`crates/core/src/error.rs` (`LossCause`); `crates/zmq/weida-zmq` (`Engine`, `Monitor`; BACKLOG
B-073, B-078, B-104).

ZeroMQ: `zmq_socket(3)`, `zmq_setsockopt(3)` (`ZMQ_RECONNECT_IVL`, `ZMQ_RECONNECT_IVL_MAX`,
`ZMQ_RECONNECT_STOP`, `ZMQ_IMMEDIATE`, `ZMQ_SNDHWM`, `ZMQ_SNDTIMEO`), `zmq_socket_monitor(3)`.
