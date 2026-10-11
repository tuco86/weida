# Failure model

Failures are modelled explicitly. This document defines the failure vocabulary
(master doc §61-63) and the normative v0 outcome rules that map observable transport events
to the results the application sees.

Related: [GUARANTEES.md](GUARANTEES.md), [PROTOCOL.md](PROTOCOL.md),
[INVARIANTS.md](INVARIANTS.md), [IMPLEMENTATION.md](IMPLEMENTATION.md).

---

## 1. Failure scope

Primary scope, verbatim from master doc §61:

- process crash;
- host crash;
- network partition;
- packet loss/delay;
- peer disappearance;
- disk failure/full disk;
- corrupted persisted data;
- leader/owner loss;
- reconnect;
- retries;
- ACK loss.

Byzantine fault tolerance is **out of scope**. Authenticated transport protects
communication but does not make the cluster a BFT consensus system. A peer that completes
the TLS handshake has proved that it holds the key its identity names, and nothing more:
what it is allowed to do is the application's decision on `IncomingMeta::peer`, and its wire
input remains hostile ([INVARIANTS.md](INVARIANTS.md)).

---

## 2. Failure analysis method

For every reliability mode, ask (verbatim from master doc §62):

```text
Who currently owns responsibility?

What state is durable?

What has the next hop acknowledged?

May the current hop safely discard local state?

May the operation be retried?

Can a retry create a duplicate?

Can a duplicate create a second effect?

Is the final outcome known or indeterminate?
```

Any new reliability mode MUST answer all eight questions in writing before it is
implemented.

---

## 3. Required failure scenarios

Verbatim from master doc §63, each annotated with its status in the current increment.
Scenarios are covered now only where they are reachable without persistence and without a
broker; the rest are deferred to the phase that introduces the subsystem they exercise
(see [IMPLEMENTATION.md](IMPLEMENTATION.md)).

```text
producer crashes before local acceptance                                [out of scope until Phase 4]
producer crashes after local persistence before sending                 [out of scope until Phase 5]
network disappears during stream                                        [covered in v0]
receiver crashes during stream                                          [covered in v0]
receiver gets entire transfer but crashes before ACK                    [covered in v0]
receiver persists transfer but ACK is lost                              [out of scope until Phase 5]
broker persists but crashes before replication                          [out of scope until Phase 7]
replicated quorum completes but broker dies before upstream ACK         [out of scope until Phase 7]
consumer receives but crashes before processing                         [out of scope until Phase 4]
consumer processes successfully then crashes before processed ACK        [out of scope until Phase 4]
processed ACK reaches broker but broker crashes before storing state    [out of scope until Phase 6]
network partition splits broker cluster                                 [decided: 0022 §4.7, unbuilt]
old shard owner returns after epoch changed                             [out of scope until Phase 7]
disk becomes full during stream persistence                             [out of scope until Phase 5]
persisted state becomes corrupt                                         [out of scope until Phase 5]
client reconnects through another broker                                [out of scope until Phase 6]
retry reaches another replica                                           [out of scope until Phase 7]
request side effect succeeds but reply disappears                       [partially covered in v0]
```

**The partition row is decided even though it is unbuilt**, and the decision is what makes the
topology worth its overhead: consensus is one group for the cluster plus one per replicated queue
([decisions/0022](decisions/0022-consensus-topology.md) §4.1-§4.2), so a partition has two
separable effects. With the **control group** below quorum, administration stops — nothing is
created, deleted, reconfigured or moved — and **every existing queue keeps serving under its own
group's quorum**. With a **queue's group** below quorum, that queue refuses admission and pauses
delivery and nothing else on the cluster notices. A confirmed message is not lost while a majority
of its own group survives; a permanently lost majority makes that one queue unavailable and needs
operator action (§4.7).


Notes on the covered ones:

- **network disappears during stream** — exercised by
  `hostile::a_server_that_disappears_mid_stream_yields_connection_lost`, which closes the
  connection after reading part of the request; the sender observes `ConnectionLost` while
  still pre-FIN, and `is_definite_failure()` is true. Two tests in
  `crates/weida/tests/streams.rs` cover the variants that carry no close frame at all:
  `idle_timeout_reports_loss_within_the_window`, where a silent connection simply expires and
  the endpoint reports `PeerEvent::Lost { cause: IdleTimeout }`, and
  `a_server_that_goes_away_is_reported_as_connection_lost`, where under
  `ReconnectPolicy::never()` the first send after the peer went away fails with the cause.
  Under the default policy the runtime redials the address and a `send` during the outage is
  held in the outbox; what is **not** repaired by that is the stream that was in flight, which
  stays lost exactly as above
  ([decisions/0031](decisions/0031-transparent-redial-and-the-sender-outbox.md) §4.3, §4.4;
  `crates/weida/tests/reconnect.rs`).
- **receiver crashes during stream** — indistinguishable at the wire from the previous case;
  same rule, same outcome, and the same two `streams.rs` tests apply unchanged.
- **receiver gets the entire transfer but crashes before answering** — exercised by
  `hostile::a_server_that_never_answers_yields_indeterminate`, where a raw server reads the
  request to FIN, sends nothing, then closes with `NO_ERROR`. The transport receipt resolves
  `Ok(())` — the peer's stack really did take every byte — or `Indeterminate` if the close
  races the acknowledgement, while `ReplyStream::recv()` resolves `Indeterminate` either
  way. That split is the canonical §22 case, and the sharpest demonstration that a transport
  receipt is not an application acknowledgement ([GUARANTEES.md](GUARANTEES.md) §3).
- **request side effect succeeds but reply disappears** — *partially* covered: the
  disappearing-reply half is covered (the requester resolves `Indeterminate`), but there is
  no side-effect durability to reason about in v0, so the scenario cannot be closed until
  persistence exists.

Every guarantee mode must have explicitly documented outcomes. The v0 core has exactly one
delivery signal — the transport receipt — and §4 documents its outcomes exhaustively,
together with the reply-side, cancellation and receiver-side rules that surround it.

---

## 4. v0 sender outcome rules

Normative. There is no separate outcome enum and no pending-transfer table: the stream *is*
the transfer, so a sender's outcome is whatever `write_all`, `finish()` and the resulting
`Delivery` report.

**The table below is QUIC's.** Three of its rows turn on the gap between "the FIN went out"
and "the peer acknowledged it", and on a local transport there is no such gap; the local rules
are stated in the same shape in their own subsection below.

| Event | Local state | Result | Rationale |
| --- | --- | --- | --- |
| Connection lost | before local FIN | `write_all` fails with `Error::ConnectionLost(cause)`; `is_definite_failure()` is true | The receiver discards partial transfers on reset or connection loss ([PROTOCOL.md](PROTOCOL.md) §9), so the payload was definitely not delivered. Definite failure, not indeterminate. The `cause` says why the connection went, and never changes whether the failure is definite. |
| Local FIN completes | `Delivery` dropped | nothing is observed at all | `finish()` is synchronous and dropping the receipt is free. Fire-and-forget by choice: the only assertion is that the local side finished writing, which is explicitly not a delivery claim. |
| Connection lost | after local FIN, before the receipt | `Delivery::delivered()` yields `Error::Indeterminate` | The FIN went out. The peer may hold every byte and may already have acted on them; no local observation distinguishes that from a loss. `is_definite_failure()` is false. |
| Receipt resolves | after local FIN | `Delivery::delivered()` yields `Ok(())` | The peer's **transport** acknowledged every byte and the FIN. It is not a claim that the peer's application read the payload ([GUARANTEES.md](GUARANTEES.md) §3). |
| `STOP_SENDING` observed | any | definite typed refusal, selected by the QUIC application error code: `REJECTED` → `Error::Rejected`, `CANCELED` → `Error::Canceled`, `UNKNOWN_ENDPOINT` → `Error::UnknownEndpoint`, `UNSUPPORTED` → `Error::Unsupported`, `SHUTDOWN` → `Error::Rejected` | The receiver refused explicitly, so the outcome is definite. The refusal surfaces from `write_all` when it lands mid-payload and from `delivered()` otherwise; a sender must be prepared to see it at either point. `SHUTDOWN` is the drain's answer to a stream that arrived after admission stopped ([decisions/0009](decisions/0009-drain.md) §4.5): a refusal like any other — nothing was taken and the peer will not take it later — so it shares `Rejected` rather than adding a second word for one outcome. `StopReason::ShuttingDown` names it for a caller that wants the distinction, and `a_draining_runtime_refuses_a_late_stream_on_an_open_connection` pins it. |
| 0-RTT rejected | any | `Error::Transport` | The data was never delivered under the accepted keys. |
| Handshake refused: the peer is not the one trusted | nothing was sent | `connect` fails with `Error::Untrusted(fp)`; `is_definite_failure()` is true | The dial ended in the verifier, before any transfer existed. The error carries the fingerprint that actually answered rather than the one that was expected, so an operator can check it out of band and pin it. |
| Handshake fails for any other reason | nothing was sent | `Error::Tls` | Covers a binding that requires a client identity the dialling endpoint does not present or does not trust, and a dial under an empty `Trust` to an address that names no fingerprint — the latter fails before a packet leaves. |
| Idle timeout or stateless reset | any | `Error::ConnectionLost(LossCause::IdleTimeout)` or `(LossCause::Reset)`; `is_definite_failure()` is true | Both mean the connection is gone, and `conn_error` maps quinn's `TimedOut` and `Reset` there rather than to a bare transport error. **The reason survives.** `ConnectionLost` carries a `LossCause` — `IdleTimeout`, `PeerClosed`, `PeerRestarting`, `LocallyClosed`, `Reset` or `TransportError` — and peer selection reports the cause of the peer it rejected instead of flattening every dead connection into one error. `PeerRestarting` is the peer's close with `RESTARTING` ([PROTOCOL.md](PROTOCOL.md) §7): its server is being replaced and a successor serves, or is about to serve, the same address, so the dialler's first redial waits for nothing, whatever `stop_on_peer_closed` says ([decisions/0038](decisions/0038-restart-handoff.md) §4.4). The outcome is unchanged and so is `is_definite_failure()`: the cause is not a second outcome vocabulary, it is what an application deciding whether to redial needs. Demonstrated by `idle_timeout_reports_loss_within_the_window` in `crates/weida/tests/streams.rs`, which asserts `LossCause::IdleTimeout`, where the server's 500 ms idle timeout is shorter than the dialling side's 10 s keep-alive interval, so silence ends the connection. |

`Error::is_definite_failure()` is the machine-readable form of the word "definite": true for
`ConnectionLost`, `Rejected`, `UnknownEndpoint`, `Unsupported`, `Canceled`, `NotConnected`,
`LimitExceeded` and `Untrusted`. A typed refusal — by stop code or by ERROR frame — proves
the payload never reached an application, and a refused handshake proves nothing was sent at
all; that is what makes them definite. `Indeterminate` is outside the set by construction,
because keeping it apart from failure is the whole point of master doc §22, and so is
`NoReply`: a replier that declines to answer has still read the request and may well have
acted on it, so only the answer is missing. The exclusions, and every member except
`Untrusted`, are pinned by `definite_failures_exclude_the_unknowable_ones`.

### Local transports: the receipt is the peer's buffer

The rules above are QUIC's. On the three local transports of
[decisions/0010](decisions/0010-local-transport.md) — in process, `AF_UNIX` and Windows named
pipes — one transfer is one channel pair or one OS connection [0010 §4.2], nothing
acknowledges anything, and the receipt therefore says less than its QUIC counterpart.

| Event | Local state | Result | Rationale |
| --- | --- | --- | --- |
| Connection lost | before local FIN | the write fails definitely: `Error::Canceled` where the peer closed its read side under the write, `Error::ConnectionLost(PeerClosed)` where the connection was already known dead; `is_definite_failure()` is true either way | A closed channel or socket takes no bytes, so the payload was definitely not delivered — the same reason QUIC's row is definite. Which of the two words appears is only which operation noticed first, and neither is a second outcome. |
| Connection lost | after local FIN, before the receipt | **cannot occur**, because there is no interval to lose the connection in | The receipt does not wait for anything, so nothing can happen between the FIN and it. |
| Receipt resolves | after local FIN | `Delivery::delivered()` yields `Ok(())` as soon as the writer's FIN is set, with no involvement from the peer | The bounded channel (in process) or the kernel's socket buffer (`AF_UNIX`, named pipes) **is** the peer's transport, so a write that completed already put the bytes in it. `inproc::LocalSend::stopped` resolves on the writer's own finished flag; `grouped::LocalSend::stopped` is `Ok(None)` unconditionally. The receipt adds nothing to what `write_all` and `finish()` already returned. |
| `STOP_SENDING` observed | any | **there is none.** In process a refusal still reaches the receipt, because the reader sets a flag the receipt reads and `stop_reason` maps it to the same typed error QUIC's stop code does. Over `AF_UNIX` it reaches the **write** instead, as `Error::Canceled`, and never `delivered()`. Over a named pipe it reaches neither | A local stream carries no application error code [0012 §4.7]. Closing an `AF_UNIX` reader's half makes the writer's next write `EPIPE`, which is a refusal without a code and arrives from `write_all`. A named-pipe reader cannot even do that — the writer's direction is the only one it could signal on and the reply owns it — so it **drains** the rest of the payload in the background and the writer finishes normally (`crates/weida/src/pipe.rs`). |

Two consequences, both load-bearing for an application that moves between transports:

- **On a local transport `Delivery::delivered()` is a statement about the peer's buffer, not
  about an acknowledgement.** It resolves without the peer's participation, so it is exactly as
  strong as the write that preceded it and not one bit stronger. The weaker reading that holds
  on every transport — a transport receipt is never an application read
  ([GUARANTEES.md](GUARANTEES.md) §3) — is the one to write applications against.
- **`Indeterminate` is a QUIC-only sender outcome.** It exists because a QUIC FIN travels and
  its acknowledgement may not come back; a local FIN does not travel at all, so a local
  sender's receipt is always definite. What is *not* local-only is the reply-side rule below: a
  replier that read the request and died leaves the exchange indeterminate on every transport,
  because that uncertainty is about the peer's **application** rather than about its transport.

### Reply-side rules

An exchange's reply half carries the other half of the sender's outcome.

| Event | Result of `ReplyStream::recv()` |
| --- | --- |
| DATA header | `Ok(IncomingTransfer)` — the reply payload follows on that half until FIN |
| ERROR header | `Err`, by code: `UNKNOWN_ENDPOINT`, `REJECTED`, `UNSUPPORTED` and `NO_REPLY` map to the matching `Error`; `INTERNAL` maps to `Error::Transport` |
| Connection lost | `Error::Indeterminate`, **never** `ConnectionLost` |
| Any other frame kind | `Error::Protocol` — nothing else is legal on a reply half |

The connection-lost row is a rule, not an accident of implementation. The replier may
already have read the request, dispatched it and produced an answer that died with the
connection; claiming a definite failure there would be a lie. `ReplyStream::recv` therefore
re-labels `ConnectionLost` as `Indeterminate` before it reaches the caller. The request
half's own receipt is unaffected and may still read `Ok(())`.

### Precedence

An ERROR frame on the reply half wins over the request half's transport receipt: the
exchange resolves as a failure carrying the ERROR code, even though the request bytes
demonstrably arrived. An implementation MUST NOT report success and then report the error
afterwards. The two halves never contradict each other about *delivery*, only about
*outcome*, and the outcome is the answer.

### Cancellation

No frame cancels anything; cancellation is entirely QUIC stream state.

| Situation | Mechanism | What the peer observes |
| --- | --- | --- |
| Sender abandons its own outgoing payload | `RESET_STREAM(CANCELED)` — `OutgoingTransfer::cancel`, or a drop without `finish()` | the transfer is never observable as complete: the read in progress fails — `Error::Canceled` through `read_capped`, `io::ErrorKind::ConnectionReset` on the `AsyncRead` — and never returns EOF. On **every** transport: QUIC resets the stream, the in-process reader checks the writer's reset flag, and both socket transports carry a RESET chunk with the code, which is what a stream socket cannot express by closing (B-245, [decisions/0012](decisions/0012-local-connection-grouping.md) §4.7(e)) |
| Receiver refuses an inbound payload | `STOP_SENDING(REJECTED)` — `IncomingTransfer` dropped mid-payload, or a payload past `read_capped`'s cap | `Error::Rejected` from `write_all` or from `delivered()` |
| Requester abandons the reply | drop `ReplyStream` before `recv()`, which stops the reply half with `CANCELED` | `IncomingRequest::canceled()` resolves; subsequent reply writes fail with `Error::Canceled` |
| Replier will not answer | ERROR `{NO_REPLY}` + FIN on the reply half — `IncomingRequest` dropped without `reply()` | `ReplyStream::recv()` yields `Error::NoReply` |
| Replier declines on purpose | the same ERROR frame, said deliberately: `IncomingRequest::refuse(code)` | `ReplyStream::recv()` yields the code's `Error` — `Rejected` where this side declined, `NoReply` where the request was taken and no reply will exist ([decisions/0005](decisions/0005-refusal-race.md) §4.3) |

`IncomingRequest::canceled()` is the reply half's `stopped()` future, and it is `'static`:
a handler takes it before `reply()` consumes the request, then selects on it beside its own
reply writes. A long reply nobody wants otherwise burns the peer's flow-control window, and
this is how the handler learns to stop.

`cancel()` guarantees an outcome, not a retraction — **on every transport**. What holds is
that the receiver can never mistake the transfer for a complete one: it observes a reset error
and never EOF. Two of the four transports get that from the kernel and two had to be given it:
QUIC has `RESET_STREAM`, the in-process reader checks the writer's reset flag, and both socket
transports frame their payload and carry a RESET chunk with the code
(`crates/weida/src/chunked.rs`). That framing was the named pipe's — a pipe has no half-close
at all — and `AF_UNIX` needed it for the opposite reason: a stream socket has a half-close and
**no abort**, so `SO_LINGER` with a zero timeout plus `close` is byte-for-byte a plain close at
the reader and the transport's `finish` and `reset` were the same call. Until B-245 a cancelled
transfer, a transfer dropped without `finish()` and a producer that died mid-payload therefore
all arrived over `weida+unix://` as a clean end of payload, and an application took half a
message for a whole one with nothing on the wire to warn it
([decisions/0012](decisions/0012-local-connection-grouping.md) §4.7(e)). What does
**not** hold is that the peer saw fewer bytes. The receiver's assembler is cleared when the
reset is *processed*, so a reader that gets there first is still served whatever was already
buffered: `cancel_discards_unread_bytes_and_keeps_read_ones` in
`crates/weida/tests/streams.rs` reads 4 KiB of an 8 KiB transfer, cancels, and then still
receives the entire 4 KiB remainder before the read fails. Bytes the application had already
taken are unaffected either way. An application that must be able to withdraw a payload needs
its own retraction; the transport offers none.

### Receiver-side rules

| Event | Receiver action |
| --- | --- |
| Peer `RESET_STREAM` before FIN | Discard all partial state for that stream and surface `Error::Canceled` to any application read in progress. Send nothing back. |
| Unregistered path, one-way transfer | `STOP_SENDING(UNKNOWN_ENDPOINT)`. A unidirectional stream has no return path, so the stop code carries the whole answer. |
| Unregistered path, exchange | ERROR `{UNKNOWN_ENDPOINT}` + FIN on the reply half, plus `STOP_SENDING(UNKNOWN_ENDPOINT)` on the initiating half. |
| Stream addressed to an endpoint whose pattern cannot serve it | One-way transfer to a replier or publisher: `STOP_SENDING(UNSUPPORTED)`. Exchange to a puller or publisher: ERROR `{UNSUPPORTED}` + FIN on the reply half, plus `STOP_SENDING(UNSUPPORTED)`. The connection survives in both cases. |
| Application drops the body before FIN | `STOP_SENDING(REJECTED)`. |
| Application drops a request without replying | ERROR `{NO_REPLY}` + FIN on the reply half, so the requester does not hang until the idle timeout. |
| DATA without `endpoint` on an initiating stream, ERROR on a unidirectional stream, or any non-DATA frame opening an exchange | `CONNECTION_CLOSE(PROTOCOL_VIOLATION)`. These are framing violations, not refusals: a refusal is per-stream, a violation ends the connection ([PROTOCOL.md](PROTOCOL.md) §3). |

**A refusal can lose the race with the transport.** The refusals above are raised by the
receiving *application's* dispatch, while the peer's transport acknowledges bytes on its own.
A one-way transfer small enough to fit in flight may therefore be acknowledged before the
application refuses it, and `Delivery::delivered()` then resolves `Ok(())` for a transfer that
was discarded a moment later — truthfully, because a transport receipt says nothing about the
application, including that it said no ([GUARANTEES.md](GUARANTEES.md) §3). This is decided
behaviour rather than a gap in this document:
[decisions/0005](decisions/0005-refusal-race.md) closes the race as documented, and no
application-level signal is added to the L0 wire to order a refusal ahead of the receipt.

**Two constructions make a refusal deterministic, and there is no third** [0005 §4.3]:

- a payload beyond the peer's stream receive window, so that flow control makes the writer
  wait for a reader that never comes; or
- an exchange, whose ERROR frame is written by the receiving application on the reply half
  and takes precedence over the request half's receipt (Precedence, above).

An application that must observe a refusal therefore uses Req/Rep.

**No sender outcome exists for a refusal observed after the receipt resolved, and none is
added** [0005 §4.4]. The outcome rules above are complete as written: once `delivered()` has
resolved `Ok(())` the `Delivery` is consumed, so a later `STOP_SENDING` reaches no observer,
and no counter, metric or late error is invented for it. Nothing in the vocabulary contradicts
a receipt after the fact, because the receipt was true when it resolved — it asserted what the
peer's transport held, and never what its application did.

That is why `push_to_rep_path_is_unsupported` and `push_to_an_unknown_path_is_reported` in
`crates/weida/tests/pushpull.rs` and `a_publisher_path_refuses_inbound_transfers` in
`crates/weida/tests/pubsub.rs` push 2 MiB instead of a few bytes: at that size the write
cannot finish unless the peer acts, so the refusal is deterministic rather than racy. Their
payload size is load-bearing and must not be reduced — a smaller one would make the tests
racy rather than make the code wrong [0005 §5].

### Connection teardown

When a connection fails there is no pending table to resolve — every in-flight operation is
a stream, and QUIC fails its halves directly. Each failure maps by the rules above: pre-FIN
writes to `ConnectionLost`, receipts and awaited replies to `Indeterminate`, reads in
progress to the corresponding read error. No operation may be left to time out silently.

### Publisher fan-out loss

One loss mode in v0 is deliberately outside the outcome vocabulary above. When a publisher
fans a message out, a subscriber whose byte budget is exhausted does not receive that
message, and **no** stream is ever opened for it — so there is nothing to reset, no receipt
to await and no outcome to resolve. The loss is therefore:

- invisible to the subscriber, which cannot distinguish "nothing was published" from
  "a message was dropped for me" (making it visible needs a sequence field, which v0 does
  not have);
- visible to the publisher only in aggregate, as `Publisher::publish` returning a smaller
  count than `Publisher::subscriber_count` and as the `Publisher::dropped` counter.

This is the intended behaviour for fan-out, not a gap in the failure model
([GUARANTEES.md](GUARANTEES.md) §6): a publisher that blocked on its slowest subscriber
would let one consumer degrade every other. Applications that cannot tolerate silent loss
must not use Pub/Sub for that data in v0.

---

## 5. What `Indeterminate` means for the application

`Indeterminate` is not a synonym for failure and MUST NOT be treated as one.

It means: **the operation may or may not have taken effect.** The peer may have delivered
the payload to its application and acted on it, or may have died before doing so. No
observation available to the local side distinguishes these.

Consequences for the application:

- Retry only if the operation is **idempotent**. v0 provides no deduplication
  ([GUARANTEES.md](GUARANTEES.md) §6), so a retry of a non-idempotent operation can produce
  a second effect.
- Do not report success to a user. Do not report definite failure either.
- If the operation is not idempotent and the outcome matters, reconcile out of band — query
  the peer for the effect, or defer to an application-level idempotency key. The framework
  will grow durable idempotency helpers in Phase 4/5; until then reconciliation is the
  application's responsibility.
- Distinguish it in logs and metrics from both success and failure. Folding indeterminate
  outcomes into an error counter destroys exactly the information §22 exists to preserve.
