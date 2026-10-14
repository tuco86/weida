# 0009 — A bounded drain at shutdown

- **Status:** accepted
- **Date:** 2026-09-11
- **Related:** SYNTHESIS §8.6; P17; decisions 0005 §4.2, 0006 §4.2

## 1. The question

SYNTHESIS §8.6, verbatim:

> **8.6 Is a quiescence signal needed at shutdown?** `Runtime::shutdown` is "the one thing that
> cuts a finished transfer short" and there is no linger and no drain [PATTERNS §1.1]. The
> evidence spans the full range: ZeroMQ's `ZMQ_LINGER` with an infinite default that can hang
> forever [zeromq §12/P17]; RabbitMQ's requeue-on-channel-close [rabbitmq-amqp091 §12/P17];
> AMQP 1.0's `drain`/`echo` quiescence point [amqp10 §9]; NATS Lame Duck Mode
> [nats §12/P17]; and D12's finding that none of them is a drain *acknowledgement*. The
> decision is whether weida needs a bounded drain, and whether it belongs to the runtime or to
> L2.

Two questions, and the second is the one that decides the first: a drain is only worth having
if something can be drained *honestly*. What weida can prove about a transfer is exactly one
thing, the transport receipt [GUARANTEES §3]; a drain that claimed more would be the
application acknowledgement decision [0005](0005-refusal-race.md) closed.

## 2. The evidence, condensed

**Nobody has a drain acknowledgement.** D12, verbatim: "ZeroMQ's linger certifies transfer to
the network, not receipt [zeromq §12/P17]; RabbitMQ has 'no linger, no drain' and requeues on
channel close [rabbitmq-amqp091 §12/P17]; MQTT abandons in-flight messages on the wire
[mqtt5 §12/P17]; NNG specifies no drain at all [nanomsg-nng §12/P17]. The two mechanisms that
come closest are AMQP 1.0's `flow(drain=true)`/`flow(link-credit=0, echo=true)` quiescence
point [amqp10 §9] and NATS Lame Duck Mode's server-side client drain [nats §12/P17]"
[SYNTHESIS §2 D12]. So the design space contains no example of the strong thing; every
mechanism in it is local.

**ZeroMQ is the anti-pattern, in its own words.** `ZMQ_LINGER` defaults to `-1`, infinite, and
`zmq_ctx_term()` "shall block until all pending messages have been sent to a peer"; `0`
discards immediately and a positive value is a millisecond bound [zeromq §1], [zeromq §12/P17].
The consequence is documented rather than theoretical: "if you leave any sockets open, the
`zmq_ctx_destroy()` function will hang forever", and even closed sockets make it "by default
wait forever" for pending connects or sends unless linger was set to zero first
[zeromq §12/P17]. An unbounded drain is not a stronger drain; it is a hang with a rationale.

**AMQP 1.0's mechanism is a receiver stopping, not a sender finishing.** `flow(drain=true)`
tells the sender to exhaust its remaining credit and report, advancing `delivery-count` until
credit is zero even when nothing is available; `flow(link-credit=0, echo=true)` asks the
partner to report state, and the echoed flow marks the point after which no further transfer
will come [amqp10 §5]. The sheet calls this a "definite quiescence point", and notes that
in-flight transfers can still arrive until the sender processes the changed state [amqp10 §9].
It answers "stop sending to me", which is a credit question already decided by
[0003](0003-credit-unit.md), not "my process is leaving".

**NATS answers the operational half and nothing else.** Lame Duck Mode sends a later `INFO`
with `ldm: true`, stops admitting new clients, drains existing ones and shuts down; a client
must reconnect elsewhere, and in-flight core delivery stays non-durable [nats §1],
[nats §12/P17]. No drain timeout is stated in the sheet [nats §12/P17]. The shape worth taking
is the ordering — stop admitting, then let what exists finish — not a wire notification.

**QUIC gives the primitive and refuses the courtesy.** `CONNECTION_CLOSE` makes every stream
immediately closed and open streams "can be assumed to be implicitly reset"; QUIC "does not
provide any mechanism for graceful connection termination", and unacknowledged stream data is
gone at close [quic-standards §5.5]. What it *does* give is the completion fact this decision
needs: "Once all stream data has been successfully acknowledged" the sending side enters
`Data Recvd`, while `Data Read` — the peer's application — is not observable by the sender
[quic-standards §2.5]. The closing and draining periods last "at least three times the current
PTO interval" [quic-standards §5.5], which is the floor on any close, drain or not.

**What weida does today, exactly.** `Runtime::shutdown(self)` takes the owned endpoints and the
pooled client endpoint, calls `endpoint.close(SHUTDOWN, b"runtime shutting down")` on each and
then awaits `wait_idle()` on each; there is no timeout anywhere in it
(`crates/weida/src/runtime.rs`). So the current behaviour is the worst pairing of the two
options in the catalogue: the close is **abortive** for application data — it is "the one thing
that cuts a finished transfer short" [PATTERNS §1.1] — while the wait afterwards is
**unbounded**, which is ZeroMQ's failure mode moved to a place where it buys nothing.

**And one transfer is one stream.** A transfer that has been `finish()`ed is a queued FIN on a
QUIC stream; what "drained" can mean for it is precisely what `Delivery::delivered()` already
means, and no more: the peer's transport holds every byte, "although not necessarily the
processing of it" [GUARANTEES §3].

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| A — no drain; keep the abortive close | `shutdown()` closes and resets | status quo [PATTERNS §1.1]; MQTT's DISCONNECT [mqtt5 §12/P17] | every caller that finished a transfer reinvents a sleep before exiting, and picks the interval by superstition |
| B — a bounded drain at L0, deadline supplied by the caller, outcome counted locally | stop accepting, let queued FINs reach the peer's transport, close at the deadline | ZeroMQ's *positive* linger [zeromq §12/P17]; NATS LDM's ordering [nats §12/P17] | a drain can expire with transfers unfinished, and the caller is told a number rather than given a guarantee |
| C — an unbounded drain | wait until every queued FIN is acknowledged | ZeroMQ's default [zeromq §12/P17] | the documented hang: a peer that stops reading holds the process open forever |
| D — no L0 drain; leave it to L2 | the broker drains queues; L0 keeps the abortive close | RabbitMQ's requeue [rabbitmq-amqp091 §12/P17] | every brokerless deployment — which is every deployment today — has no answer, and what needs draining is transport state the broker does not own |
| E — a wire quiescence signal | a frame the peer answers when it has taken everything | AMQP 1.0 `flow(echo)` [amqp10 §5] | it is an application acknowledgement, which [0005](0005-refusal-race.md) closed; and D12 records that no protocol actually offers one |

## 4. Decision

Option B.

1. **weida gets a drain, it lives in the runtime, and it is separate from `shutdown`.**
   `Runtime::shutdown` keeps its present meaning and its documentation gains the word for it:
   **abortive**. Draining is a second, explicit operation — `Runtime::drain(Duration)` — that
   returns an outcome and then performs the same close. A caller that wants today's behaviour
   changes nothing; a caller that wants its finished transfers to land asks for it.

2. **What is drained is defined by the only completion signal L0 has.** A drain waits for
   every transfer that has already been `finish()`ed to reach the peer's **transport** — the
   condition `Delivery::delivered()` reports and QUIC's `Data Recvd` state marks
   [GUARANTEES §3], [quic-standards §2.5]. It does not wait for a peer application to read
   anything, and the API must not suggest it: there is no `Processed`, no read confirmation and
   no new frame. A transfer that has *not* been finished is not part of the drain — the
   application still owns it, and cutting it off is what `cancel` already means.

3. **The deadline is mandatory, finite and supplied by the caller.** `drain` takes a
   `Duration`, not an `Option<Duration>`; there is no infinite variant and no configuration
   default that could become one. This is the single most load-bearing line in the note:
   ZeroMQ's `-1` default is the one thing the catalogue unanimously warns about
   [zeromq §12/P17]. The deadline is wall-clock and starts when `drain` is called.

4. **The current unbounded `wait_idle()` is a defect, not a drain.** After the close, waiting
   for sockets to go idle must be bounded by the same deadline; QUIC's own closing and draining
   periods are "at least three times the current PTO interval" [quic-standards §5.5], so a
   bound in the hundreds of milliseconds is generous rather than tight. `shutdown` — the
   abortive path — gets a bound too, and it is the same argument: nothing may make process exit
   depend on a peer's behaviour.

5. **A drain stops admission first, which is what makes it terminate.** While draining: every
   binding stops accepting new connections, and a new inbound stream on an existing connection
   is refused with the existing `SHUTDOWN` application code. This is NATS Lame Duck Mode's
   ordering [nats §12/P17] without its wire notification: no `ldm`-style frame is added,
   because a peer that must be told is a peer that must answer, and that is Option E.

6. **The outcome is local and it is a count, not an acknowledgement.** `drain` reports how many
   transfers reached the peer's transport and how many were still outstanding when the deadline
   expired. That is a true local statement; it is not a claim about the peer, and D12 is the
   reason the stronger thing is not offered [SYNTHESIS §2 D12]. An expired drain is not an
   error: it is a number the caller may log, retry against, or ignore.

7. **L2 owes a different drain, and may not present it as this one.** A broker hop drains
   queues: unacknowledged deliveries are requeued or redelivered, consumer acknowledgements
   settle, and durability decides what survives — RabbitMQ's requeue-on-channel-close is the
   shape [rabbitmq-amqp091 §12/P17]. That is responsibility transfer, and it belongs with the
   vocabulary reserved for it [GUARANTEES §1]. The L0 drain of §4.2 is about bytes in flight
   and says nothing about queues.

8. **No wire change, at wire version 0 or any other.** A drain is entirely a local sequencing
   of operations the protocol already has: stop accepting, wait on existing receipts, close
   with `SHUTDOWN`. Nothing is added to PROTOCOL, and a peer observes exactly what it observes
   today.

## 5. Consequences and follow-ups

- **`crates/weida/src/runtime.rs`.** `Runtime::drain(Duration)` per §4.1-§4.6, and a bound on
  the `wait_idle()` await in both paths per §4.4. Filed as a backlog item; the unbounded wait
  is a defect in shipped code, so it does not wait for the drain feature.
- **[PATTERNS.md](../PATTERNS.md) §1.1.** "`Runtime::shutdown` is the one thing that cuts a
  finished transfer short" stays exactly true and gains its counterpart: `drain` is how a
  finished transfer is given its chance, bounded, with a count rather than a promise. §7
  (Choosing) gains no row: this is not a pattern choice.
- **[GUARANTEES.md](../GUARANTEES.md) §3.** The transport receipt section notes that the drain
  of 0009 waits on exactly this signal and therefore inherits its meaning — including that a
  drained transfer may still have been discarded by the peer's application [0005 §4.2].
- **[PROTOCOL.md](../PROTOCOL.md) §11.** "Drain" joins the not-specified list as a *local*
  operation with no wire representation, with this note as the reason, so that no
  implementation invents a quiescence frame for it.
- **Foreign-library composition.** ZeroMQ `ZMQ_LINGER` and weida `drain(timeout)` remain
  protocol-native shutdown operations. An application or managed Connector that composes both
  MUST NOT present either side's shutdown as a drain acknowledgement.
- **[INVARIANTS.md](../INVARIANTS.md).** No new bound: a drain waits on receipts that already
  exist and holds nothing new. Worth stating, because "wait until things finish" is exactly the
  shape that usually grows a queue.
- **[SYNTHESIS.md](../research/SYNTHESIS.md) §8.6** is closed by this note. Only §8.8, the local
  transport, remains open.

## 6. Sources

weida documents: [PATTERNS.md](../PATTERNS.md) §1.1, §1.8;
[GUARANTEES.md](../GUARANTEES.md) §1, §3; [PROTOCOL.md](../PROTOCOL.md) §11;
[INVARIANTS.md](../INVARIANTS.md); `crates/weida/src/runtime.rs` (`Runtime::shutdown`,
`OwnedRuntime::drop`); [0003](0003-credit-unit.md) §4.2; [0005](0005-refusal-race.md) §4.2;
[0006](0006-guarantee-sets.md) §4.2.

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §1 (P17), §2 (D12), §8.6;
[zeromq.md](../research/zeromq.md) §1, §5, §12/P17;
[rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §12/P17;
[amqp10.md](../research/amqp10.md) §5, §9, §12/P17; [nats.md](../research/nats.md) §1, §12/P17;
[mqtt5.md](../research/mqtt5.md) §12/P17; [kafka.md](../research/kafka.md) §5, §11, §12/P17;
[nanomsg-nng.md](../research/nanomsg-nng.md) §12/P17;
[quic-standards.md](../research/quic-standards.md) §2.5, §5.5.
