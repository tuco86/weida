# 0023 — A completion is a cursor, and wire state is not one of them

- **Status:** provisional
- **Date:** 2026-09-13
- **Related:** the owner's direction on acknowledgements and on wire state;
  [0003](0003-credit-unit.md) §4.2-§4.4; [0004](0004-durability-levels.md) §4.1-§4.5;
  [0005](0005-refusal-race.md) §4.2; [0006](0006-guarantee-sets.md) §4.4, §4.7;
  [0009](0009-drain.md) §4.2; [0016](0016-conflation.md) §4.10;
  [0018](0018-minimal-broker.md) §4.3, §4.6; [0020](0020-cluster-and-discovery.md) §4.5;
  [STORE.md](../STORE.md) §2.4, §5; [PATTERNS.md](../PATTERNS.md) §1.1, §1.2;
  [PROTOCOL.md](../PROTOCOL.md) §4, §4.1, §6.2, §6.5, §6.6, §9.2;
  [GUARANTEES.md](../GUARANTEES.md) §1, §2, §6; [INVARIANTS.md](../INVARIANTS.md).

## 1. The question

Everything in weida is a stream, and every completion level is a verdict about a whole message.
The owner named the mismatch: "die ack … beziehen sich noch immer auf die ganze nachricht, sagen
wir haben ein ack fuer received, synced, processed usw. — das sind keine antworten auf die
gesamte nachricht sondern **cursor die den stream stueck fuer stueck wandern**", and then the
second half: "processed ist dabei eh fraglich, normal will man da schon ein decoupling zwischen
den komponenten. also processed wuerde man eher mit einer request reply architektur abbilden".

And after the first draft of this note proposed reading a byte cursor out of QUIC, the owner drew
the line that decides its shape: "wieso sollte ein lib user zugriff auf den wire zustand
brauchen, den hat zmq aus guten gruenden ja auch eingeschraenkt, da bekommt man nicht mal den
connect mit."

So: which completions are cursors, whose cursors are they, and which of them may an application
see at all.

## 2. The evidence, condensed

**The information is already cursor-shaped, and we already collapse it.**
[PATTERNS.md](../PATTERNS.md) §1.2 states what `Delivery::delivered()` means, and the second half
is a statement about a *quantity of bytes*: "Inside the peer's stream receive window that is all
it means… **Beyond the window it means more**, because QUIC cannot accept more bytes than the
window until the application has consumed some. For a payload of `p` bytes against a window of
`w`, the receipt cannot resolve until the reader has consumed at least `p - w` bytes, rounded up
to the next eighth of a window." Three tests pin it, including one that measures the crossing at
131072 consumed bytes for a 160 KiB transfer against a 64 KiB window.

**QUIC tracks the cursor and quinn deliberately does not publish it.**
`quinn::SendStream::stopped()` "yields `None` if the local side `finish()`es the stream and then
the peer acknowledges receipt of all stream data (although **not necessarily the processing of
it**)", with a warning attached: "For a variety of reasons, the peer may not send
acknowledgements immediately upon receiving data. As such, relying on `stopped` to know when the
peer has read a stream to completion may introduce more latency than **using an application-level
response of some sort**." One layer down, `quinn-proto`'s `Send::ack` does
`self.pending.ack(frame.offsets)` and asks `is_fully_acked()`, so the acknowledged byte ranges
exist per stream — as internal state, with no public accessor.

**ZeroMQ restricts the same thing, and its reason is the one that applies here.** Sockets
"SHALL establish connections opportunistically, that is: they connect to an endpoint
**asynchronously**", and reconnect on their own with randomized backoff [zeromq §1]; an
application does not observe a connect at all. What it can have is an **opt-in side channel**:
`zmq_socket_monitor()` creates a `ZMQ_PAIR` on an `inproc://` endpoint that the application
connects to in order to collect events, and it "supports only connection-oriented transports"
[zeromq §11]. So the precedent is not "hide everything" but "keep transport state out of the
data API and offer it as a separate, typed, opt-in stream" — which is exactly what
`weida-zmq`'s own monitor does in this repository (B-086).

**What the levels are, and which of them a transport can possibly know.**
[GUARANTEES.md](../GUARANTEES.md) §1 defines four: `Accepted` (responsibility taken in memory),
`Stored(Written|Flushed)` (survives the process, survives power loss), `Replicated(n, flushed)`,
and `Processed` ("the next hop's application-level consumer has explicitly reported successful
processing"). A transport can know none of them: they are statements about a **store** or about
an **application**, which is why [0004 §4.1] reserved them for the L2 hop in the first place.
What the transport knows is bytes acknowledged, which is not a level at all.

**And one frame shape for a cursor already exists.** The credit of [0003 §4.2-§4.3] is "an
absolute delivery limit, not a delta and not an ack-driven window… a lost or duplicated credit
frame changes nothing", counting messages only, and [0022 §4.x]'s implementation keeps the
highest value seen so that a reordered grant is harmless. That is precisely a cursor: absolute,
monotone, idempotent, receiver-granted.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — cursors only where the state is the application's: a store's and a consumer's. Transport progress stays invisible** | `Stored(*)` and settlement become byte cursors carried by weida frames; `Delivery` stays the one whole-stream collapse of transport state; progress observability, if ever wanted, is an opt-in event channel and not a field | ZeroMQ's asynchronous connect plus its separate monitor socket [zeromq §1, §11]; quinn's own advice to use "an application-level response of some sort" | A producer streaming into a broker learns nothing until the store reports. Acceptable, because the store's cursor is the one resumption needs |
| B — publish a transport byte cursor (a quinn accessor, or a fork) | `Delivery` gains `acked_offset()` | none: quinn keeps it internal on purpose | Turns an inference into an interface: [PATTERNS §1.2]'s "a receipt beyond the window is evidence the reader consumed" becomes an API an application writes logic against, and that logic breaks when a window size changes. It also invites applications to rebuild the reliability QUIC already provides, which is the mistake [ARCHITECTURE §1] records for brokerless application ACKs |
| C — keep whole-message verdicts, add nothing | today's model | AMQP 1.0, where settlement is per delivery and a fragmented transfer settles as one [amqp10 §6] | A 1 GiB transfer is all-or-nothing: a broker restart at 900 MiB has stored nothing it may report, and a consumer crash redelivers the whole gigabyte. For a system whose first invariant is "all user payloads may remain streams end-to-end", that is the model contradicting the invariant |
| D — a new transfer identifier so cursors can travel on their own stream | a `transfer_id` in the header, cursors on a separate uni stream | Kafka's offsets; MQTT's packet identifier | Reintroduces exactly what [PROTOCOL §6.2] removed — "There is no `transfer_id`, no `role` and no `correlation_id`. The stream carries all three" — to solve a problem the reply half already solves |

## 4. Decision

**Option A.** A completion is a cursor where the completing state belongs to an application, and
transport progress is not published at all.

**4.1 The line: a cursor about the transport is wire state and stays hidden; a cursor about a
store or a consumer is application state and belongs on the wire.** This is the owner's line and
it is ZeroMQ's: an application does not observe a connect, does not observe a reconnect, and does
not observe an acknowledgement offset, because every one of those invites logic that is wrong as
soon as the transport changes. What an application observes is what it can act on.

So: **no quinn change is asked for and none is needed.** `Delivery` keeps its exact current
meaning — the peer's transport holds every byte and the FIN — and [PATTERNS §1.2]'s window
reasoning stays what it is: an explanation of what a receipt *implies*, not an interface. The
fact that `quinn-proto` tracks the ranges internally is recorded here so that nobody rediscovers
it and mistakes it for an opportunity.

**4.2 Three cursors exist, and each is reported by whoever knows it exactly.**

| Cursor | Whose state | How it travels | Who may report it |
| --- | --- | --- | --- |
| `Stored(Written)` up to byte *N* | the store | weida frame (§4.3) | the hop that owns the store |
| `Stored(Flushed)` up to byte *N* | the store | the same frame, another level | the same hop |
| consumed up to byte *N* — the settlement `Processed` becomes | the consuming application | the same frame, on the delivery's reply half | the consumer |

`Accepted` stays a **verdict**, not a cursor, and that is not an inconsistency: it says
"responsibility taken, in memory, for this message", which is a statement about a whole message
by construction — a queue either took the message or refused it [0018 §4.6]. `Replicated(n,
flushed)` is a cursor over the same bytes as `Stored`, with the replica count attached, and it is
the one [0020 §4.5]'s streamed payload replication needs: "0..N replicated to a quorum" is what
lets a 33 MB frame be replicated without any hop holding it whole.

**4.3 Cursors travel on their own unidirectional stream, as frame kind `6`.** The rule is one
sentence — **a cursor never shares a stream with payload** — and
[0024](0024-three-families-one-back-channel.md) §4.4 records why the first draft's answer (the
exchange's reply half) fails: a DATA frame is "header followed by opaque payload bytes until FIN",
so a responder that streams a reply while still reporting on the request would have to interleave
control into payload, and a cursor about the *reply* direction has no carrier at all once the
initiating half is finished.

So kind `6` — the number [0016 §4.10] recorded as the first free one — is a **uni** stream, as
kind `5` is: a head frame naming the payload stream it reports about, then `(level, offset)`
records until FIN. The level is §4.3a's `uint`; the offset is **absolute**, monotone per level at
the receiver, idempotent under loss and reordering. One encoding, two directions: credit is a
cursor the receiver *grants*, a completion is a cursor the receiver *reports*.

[PROTOCOL §4.1] is therefore **unchanged** — a reply half still carries "either DATA or ERROR,
exactly one frame, followed by FIN" — and this note is one stream kind, not a new rule about
exchanges.

**4.3a The level space is open, and weida interprets only its own half.** A fixed ladder cannot
express what a consumer actually does — "written, then synced, then validated, then indexed" is
several stages, and only the first two are weida's. So the level is a `uint` split once:

| Range | Meaning | Who may interpret it |
| --- | --- | --- |
| `0..=15` | weida's own levels, the `acknowledgement` values of §6.5 | weida, and it is negotiated |
| `16..` | **application-defined stages** | the application at each end; weida carries them |

The rule that makes this safe is the one this protocol already applies twice — to `topic`
("opaque bytes: weida never parses it") and to `tracestate` ("opaque to weida and MUST be
forwarded unmodified"): **weida never interprets an application level.** It checks exactly two
things — that the offset is monotone for that level, and that the level is a `uint` — and it
neither refuses an unknown level nor assigns it a meaning. A hop that does not know a level
forwards it or ignores it; it never fails on it, which is the same skip-the-unknown discipline
§5 applies to header keys.

The consequence for negotiation is a line worth writing down before someone tries the opposite:
the `acknowledgement` dimension of §6.5 stays the **negotiated floor of what weida itself
guarantees**, and an application stage is outside negotiation by construction — weida cannot
promise a level whose meaning it does not know. A peer asks its counterpart for application
stages the way it asks for anything application-level: in the application's own terms.

**4.3b Which cursors are reported is the sender's order, and batching is free.** A sender states
in its configuration which levels it wants back and how finely — "every *N* bytes or every *T*
milliseconds", per level (§4.5). Batching needs no protocol support and loses nothing, because
the cursors are **absolute**: a later cursor for a level supersedes every earlier one, so a
reporter may coalesce freely and a lost intermediate cursor costs nothing. That is Nagle's
argument without Nagle's hazard — there is no delayed-ack interaction to get wrong, because
nothing waits on a cursor to make progress.

Two consequences, both deliberate:

- **A one-way transfer can be reported on**, because the cursor stream is separate: a Push
  producer keeps its unidirectional payload stream and still receives a reliable verdict. This is
  what the separate stream buys over the first draft's reply half, and it means no pattern changes
  shape to gain cursors. A producer that orders nothing still gets the transport receipt and
  nothing more [0018 §4.6].
- **A cursor is never a refusal.** A hop that cannot continue sends the ERROR that terminates the
  reply half; cursors only ever move forward, which is what makes "keep the maximum" a complete
  rule.
- **Several levels advance independently on one reply half.** A consumer that writes, then syncs,
  then runs two application stages reports four cursors over the same bytes, each monotone in its
  own level, and the terminal frame closes the exchange when the consumer is done with the
  message. That is the multi-stage case, and it needs nothing beyond §4.3's shape.

**4.4 `Processed` is not an end-to-end claim, and the note says so where the level is defined.**
The owner is right that a producer wanting to know its message was processed is describing
request/reply, and [0018 §4.3] already refused the relay: guarantees are hop-local
[GUARANTEES §2], so the producer's certificate stays what its own hop gave it however the
consumer later answers. What `Processed` remains is the **settlement cursor toward the queue** —
"you may forget up to *N*" — which is the thing that defines redelivery and, for a stream, is the
difference between resuming at *N* and redelivering a gigabyte. The level keeps its code point.
**Amended by [0029](0029-a-report-is-relayed-a-certificate-is-not.md) §4.1:** it also keeps its
definition, and a queue hop may **re-emit** a consumer's `Processed` toward the producer as the
consumer's report — never as the broker's certificate, which stays `Accepted`, never fabricated,
and never relayed a second time. What this paragraph got right is the half that survives: a
report is not a guarantee, and no hop certifies a hop it does not own. What it got wrong is
treating "not a guarantee" as "not transportable", which would make the mechanism this note
built unavailable through exactly the topology a queue is.

**4.5 Whether cursors are reported is negotiated; how often is local.** The `acknowledgement`
dimension of [PROTOCOL §6.5] already expresses whether: a peer that negotiates `Stored` promises
store-level reporting, and a peer that cannot reach the requested level fails the negotiation
rather than degrading quietly [0006 §4.4], [GUARANTEES §4]. How often is **not** negotiated,
because it is nobody else's business and because a wrong value is a performance problem rather
than a correctness one: a local number, "report every *N* bytes or every *T* milliseconds,
whichever comes first", with the rule that a reporter may always report **more** often than its
configuration and never less. Every cursor system has this number — Kafka's commit interval,
AMQP's credit top-up threshold — and naming it here is what keeps it from being invented per
slice.

**4.6 A cursor reports responsibility; it is not a recovery mechanism.** This replaces what the
first draft of this sub-section claimed — "resumption" — and the correction is the owner's: within
a connection QUIC retransmits, and **when the connection ends there is no resend above it**. A
cursor therefore buys exactly one thing: it tells the layer above **how far responsibility got**,
so that the application's own transaction logic can act on a number instead of a guess. Whether it
then cancels, re-derives the work, or opens a new stream carrying the remainder is the
application's decision, and which of the three is right follows from the pattern rather than from
the protocol ([PATTERNS.md](../PATTERNS.md) §1.11).

What that decision needs, weida deliberately does not supply: an application-level identity for
"the same payload" and a receiver that persisted a prefix. A library that supplied them would be
rebuilding retries, which are Phase 4 and, in every system surveyed here, application-level.

So the honest statement of the value is narrower than the first draft's and survives contact with
the failure model: a whole-message verdict tells an interrupted sender **nothing**, and a cursor
tells it a number. For a 1 GiB transfer that is the difference between "unknown" and "900 MiB of
this is durable at the far end" — and what to do with that is the sender's business.

**4.7 What does not change.** L0 gains nothing and loses nothing: two stream kinds, `Delivery`,
refusal by stop code, cancellation. The ZeroMQ analogies are untouched, because none of them has
an acknowledgement to begin with. And no adapter's guarantee changes: a foreign protocol that
settles per message keeps settling per message, and a cursor at a weida edge is reported as the
whole-message verdict the foreign side understands — the adapter honesty rule [INVARIANTS]
applies unchanged.

**4.8 Status is `provisional`, and what would change it.** Two things. If a measurement shows
that cursor frames cost more than resumption saves at realistic payload sizes — the number to
measure is frames per megabyte against the cost of re-sending a prefix — then §4.5's granularity
is the knob and §4.3 is the shape to revisit. And if a store turns out to be unable to report a
meaningful byte offset (a chunked or content-addressed store whose durability is per object
rather than per prefix), then `Stored` stays a verdict and only settlement is a cursor.

## 5. Consequences and follow-ups

- **[PROTOCOL.md](../PROTOCOL.md) §4, §6** gain kind `6` as a uni stream with a head frame and
  records until FIN; **§4.1 is unchanged**; §6.5's `acknowledgement` dimension gains the sentence
  that the level decides *whether* cursors are reported; §9.2 keeps the transport receipt exactly
  as it is.
- **[GUARANTEES.md](../GUARANTEES.md) §1** gains the split of §4.2: which levels are cursors,
  which are verdicts, and that `Processed` is a settlement rather than an end-to-end claim.
- **[PATTERNS.md](../PATTERNS.md) §1.2** gains one sentence: the window reasoning is an
  explanation and not an interface, and there is deliberately no byte-cursor API.
- **[STORE.md](../STORE.md) §2.4** gains the consequence that the flush callback's offset is what
  a `Stored` cursor reports, so the store's own durability granularity *is* the cursor's.
- **[0004](0004-durability-levels.md)** is amended rather than replaced: its four states keep
  their definitions and gain an offset where the state is about bytes.
- **Backlog.** Three items, links relative to `docs/BACKLOG.md`.

### B-233 — Frame kind `6`: the cursor stream
kind: code | size: 90 | status: ready | needs: []
acceptance: `FrameKind::Cursor` = 6 on a **unidirectional** stream, as CREDIT is: a head frame naming the payload stream it reports about — QUIC's own `StreamId`, which `quinn` exposes as `SendStream::id()`/`RecvStream::id()` and which is a varint both ends already know — followed by `(level, offset)` records until FIN, where the level is the `uint` of [0023](0023-completion-is-a-cursor.md) §4.3a and the offset is absolute. Encoded and decoded in `weida-protocol` with golden vectors in §8's shape and a fuzz target beside the other headers. The receiving side keeps the **maximum** offset per level, so a duplicated or reordered record changes nothing — mutation-checked, as B-202's credit rule was. Hostile tests: a cursor record whose offset goes backwards is ignored rather than fatal, a cursor stream naming a stream id that does not exist on the connection is a protocol violation, and **[PROTOCOL.md](../PROTOCOL.md) §4.1 is untouched** — a test asserts that a reply half still carries exactly one DATA or ERROR.
note: a cursor never shares a stream with payload, which is what makes the frame independent of the pattern: the same stream kind reports on a one-way transfer and on either direction of an exchange.

### B-234 — Reporting a cursor, and the number that says how often
kind: code | size: 60 | status: blocked | needs: [B-233, the Phase 5 store]
acceptance: a hop with a store reports `Stored(Written)` and, where it flushes, `Stored(Flushed)` up to a byte offset on the reply half, at a granularity from its own configuration — "every *N* bytes or every *T* milliseconds, whichever comes first", with a default and the rule that more often is always allowed and less often never ([0023](0023-completion-is-a-cursor.md) §4.5). A peer that negotiated `TransportReceipt` receives **no** cursor frames at all; a peer that negotiated `Stored` and talks to a hop with no store fails the negotiation rather than being sent nothing ([0006](0006-guarantee-sets.md) §4.4).
note: the granularity number is the whole of the configuration surface, and it is named here so that no slice invents its own.

### B-235 — What an interrupted stream means, per pattern
kind: spec | size: 45 | status: done | needs: []
acceptance: [PATTERNS.md](../PATTERNS.md) gains the section a stream-shaped system needs and did not have: a stream is unfinished by definition, so the question is not "was it delivered" but what a half-sent stream means — **within a connection QUIC retransmits, and when the connection ends weida does not resend**. The three answers an application has (cancel, reschedule, reconnect-and-continue) with when each is right and what it costs, and which one each pattern usually needs: Req/Rep reschedules because it has a reply half, Push/Pull reschedules and only the sender can, Pub/Sub cancels because a copy is best effort by definition, and a queue cancels on the way in and redelivers on the way out.
note: done, docs-only, and it replaces an item that was wrong: the first draft filed "resumption" as a protocol feature, which presumes a resend layer this system deliberately does not have. The correction came from the owner, and the sentence that makes the whole bridge is now in the document: **a message is a stream that reached FIN** — so a queue's unit is a completed stream, an interrupted admission needs no vocabulary of its own because nothing was a message yet, and a redelivery is an ordinary new stream rather than a continuation.

## 6. What this note does not decide

- **Progress observability.** Whether an application may ever watch transfer progress, and
  through what. If it may, the shape is `weida-zmq`'s monitor and ZeroMQ's: a separate, typed,
  **opt-in** event channel, never a field on the hot path [zeromq §11]. This note only refuses to
  put it in the data API.
- **Whether a store can always report a prefix offset.** §4.8's second reopening condition; a
  content-addressed store whose unit is an object may only be able to report whole objects.
- **Anything that looks like a resend.** There is no protocol-level continuation, and this note
  does not add one: an application that wants to carry a remainder opens an ordinary new stream
  and needs its own identity for "the same payload" ([PATTERNS.md](../PATTERNS.md) §1.11). The
  first draft of this note filed that as an item and it was wrong — the mechanism it presumed
  does not exist and is not wanted.
- **Cursors between brokers.** Whether replication reports offsets to the leader or the leader
  infers them, which is [0020 §4.5]'s ordering rule seen from the other side.
- **Anything about L0.** No transport cursor, no quinn change, no new field on `Delivery`.

## 7. Sources

weida documents: [PATTERNS.md](../PATTERNS.md) §1.1, §1.2;
[PROTOCOL.md](../PROTOCOL.md) §4, §4.1, §6.2, §6.5, §6.6, §8, §9.2;
[GUARANTEES.md](../GUARANTEES.md) §1, §2, §4, §6; [STORE.md](../STORE.md) §2.4, §5;
[INVARIANTS.md](../INVARIANTS.md); [ARCHITECTURE.md](../ARCHITECTURE.md) §1;
[0003](0003-credit-unit.md) §4.2-§4.4; [0004](0004-durability-levels.md) §4.1-§4.5;
[0006](0006-guarantee-sets.md) §4.4; [0016](0016-conflation.md) §4.10;
[0018](0018-minimal-broker.md) §4.3, §4.6; [0020](0020-cluster-and-discovery.md) §4.5;
[0022](0022-consensus-topology.md) §4.2.

Research sheets: [zeromq.md](../research/zeromq.md) §1 (opportunistic asynchronous connect,
reconnection rules), §11 (the monitor socket as a separate `inproc://` `ZMQ_PAIR`);
[amqp10.md](../research/amqp10.md) §6 (settlement per delivery, not per fragment);
[kafka.md](../research/kafka.md) §5, §6 (offsets as cursors, the committed boundary).

External, read 2026-09-13: quinn 0.11.11 `src/send_stream.rs` — `stopped()`'s documented meaning
and its warning to prefer "an application-level response of some sort"; quinn-proto 0.11
`src/connection/streams/send.rs` — `Send::ack` calling `pending.ack(frame.offsets)` and
`is_fully_acked()`, i.e. the per-stream acknowledged ranges exist as internal state with no
public accessor. **No upstream change is requested.**
