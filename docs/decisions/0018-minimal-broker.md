# 0018 — The minimal L2 broker — a queue at a path, `Accepted` in, `Processed` out

- **Status:** provisional
- **Date:** 2026-09-14
- **Related:** B-198; [LOOP.md](../LOOP.md) §5, §9 (Phase D, D2);
  [0003](0003-credit-unit.md) §4.1-§4.4, §5; [0004](0004-durability-levels.md) §4.1-§4.6, §5;
  [0006](0006-guarantee-sets.md) §4.4, §4.7-§4.9; [0007](0007-topic-namespace.md) §4.1, §4.2, §5;
  [0008](0008-session-identity.md) §4.5, §4.6; [0009](0009-drain.md) §4.2-§4.8;
  [0011](0011-answered-where-it-arrived.md) §4.2, §4.3; [0016](0016-conflation.md) §4.1, §4.7,
  §4.10; [GUARANTEES.md](../GUARANTEES.md) §1, §2, §3, §4, §6; [PROTOCOL.md](../PROTOCOL.md) §4,
  §6.4, §6.5, §9.4, §9.5, §10, §11; [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §2, §4;
  [FAILURE_MODEL.md](../FAILURE_MODEL.md) §3; [IMPLEMENTATION.md](../IMPLEMENTATION.md) §1, §6;
  [INVARIANTS.md](../INVARIANTS.md); P3, P7 and P12 of
  [SYNTHESIS.md](../research/SYNTHESIS.md).

## 1. The question

B-198 asks which reserved acknowledgement levels the first broker slice implements
(`Accepted` alone, or `Accepted` plus `Stored(Written)`), whether the L2 credit frame of
[0003](0003-credit-unit.md) §4.2 lands with that slice, where a queue's identity lives while
endpoint paths remain opaque ([0007](0007-topic-namespace.md) §4), what the queue-level drain
means, and where the persistence boundary lies so that Phase 5 is not pre-empted. It names the
first three code slices with their acceptance lines [BACKLOG B-198].

The layer is not in question. [ARCHITECTURE.md](../ARCHITECTURE.md) §1 names it — "L2 — broker
semantics. The RabbitMQ-analog layer: queues, publisher confirms, consumer acknowledgements with
redelivery. Deferred to Phase 6 as its own crate; none of it exists today" — and §4 reserves the
crate: "`weida-broker`… The broker is a separate crate because it is a separate layer: it
depends on the patterns, nothing in the core may depend on it, and a brokerless deployment must
not link it" [ARCHITECTURE §4]. What is in question is the **first slice**: which certificates
it may issue, what it puts on the wire, what names a queue, and which neighbouring phase it must
not quietly consume. The local `BrokerConfig` built here is bootstrap input, not the eventual
cluster authority: [0022](0022-consensus-topology.md) later places managed Queue and Connector
resources in the control Raft. This note must therefore define queue behavior without baking
deployment ownership into the data path.

## 2. The evidence, condensed

**Most of the vocabulary is already decided.** Four completion states with exact conditions,
reserved for this layer: `Accepted` — "the next hop has accepted responsibility in memory
according to the selected policy"; `Stored(Written)` — survives "the **broker process** dying:
crash, restart or orderly stop", possibly lost to OS or power failure; `Stored(Flushed)` —
flushed to durable media; `Replicated(n, flushed)` — `n` replicas, leader included, *achieved*
rather than configured, `Replicated(1, …)` not reportable; `Processed` — "the next hop's
application-level consumer has explicitly reported successful processing" [GUARANTEES §1],
[0004 §4.1-§4.3]. Two rules bind the first slice hard: "`Stored` MUST NOT be reported for an
in-memory buffer" [GUARANTEES §1], and a requested guarantee is never silently weakened
[GUARANTEES §4], [0004 §4.5]. The states above `TransportReceipt` "are **not a ladder**":
persistence level and replica count are independent axes [GUARANTEES §3], [0004 §4.4].

**The credit is decided down to its arithmetic and its frame number.** L0 carries no application
credit — "QUIC's byte windows are the byte credit and the concurrent-stream budget is the
message credit" [0003 §4.1], [GUARANTEES §6] — while "**L2 has an explicit message credit per
subscription**", granted by the consumer per subscription or queue, so that "credit can be
withdrawn without closing streams and can be re-stated after a connection change" [0003 §4.2],
amended so that it rides the connection of the path it names [0011 §4.3]. It is "an absolute
delivery limit, not a delta and not an ack-driven window… a lost or duplicated credit frame
changes nothing" [0003 §4.3], counting messages only [0003 §4.4]. The wire slot exists: "Kind
`5` additionally carries a *name* already — the L2 credit frame of
[decisions/0003](0003-credit-unit.md) §4.2… so that nothing else claims the number"
[PROTOCOL §4], with "its fields are fixed with the Phase 6 broker design" [PROTOCOL §11].

**The namespaces are decided, and one is deliberately structureless.** "Endpoint paths stay
opaque. The invariant is not amended" — a path is "an exact key into a flat map, validated for
bytes and length only", and dispatch keeps "exactly one answer per (stream kind, path), which is
what makes `UNKNOWN_ENDPOINT` and `UNSUPPORTED` decidable" [0007 §4.1], [PROTOCOL §9.4].
Hierarchy lives only in weida's topic namespace: a segmented filter with `*` and a final `#`,
matched allocation-free in one walk [0007 §4.2], [PROTOCOL §6.4]. Similar foreign constructs,
such as MQTT shared subscriptions, imply no mapping; a future Connector must name any
conversion explicitly.

**The drain is decided for L0 and explicitly not for L2.** `Runtime::drain(Duration)` waits for
already-`finish()`ed transfers to reach the peer's transport, with a mandatory finite deadline
and a local count rather than an acknowledgement [0009 §4.2, §4.3, §4.6], and no wire
representation [0009 §4.8]. Item 7 hands this note the rest: "**L2 owes a different drain, and
may not present it as this one.** A broker hop drains queues: unacknowledged deliveries are
requeued or redelivered, consumer acknowledgements settle, and durability decides what
survives… The L0 drain of §4.2 is about bytes in flight and says nothing about queues"
[0009 §4.7].

**What is not built, and must stay that way.** The tracker: 4 Reliability, 5 Persistence
subsystem, 6 Standalone broker, 7 Broker clustering — all "not started" [IMPLEMENTATION §1]. The
debt list is blunter: "**No retries.**… Retry policy arrives in Phase 4"; "**No persistence.**
No payload store, no WAL, no recovery. Phase 5"; "**Application acknowledgements are absent, not
partial.**… no wire representation, no code point" [IMPLEMENTATION §6].
[FAILURE_MODEL.md](../FAILURE_MODEL.md) §3 sorts the broker's failure scenarios by phase, and
that sort is the boundary: "broker persists but crashes before replication", "replicated quorum
completes but broker dies before upstream ACK", "network partition splits broker cluster" and
"old shard owner returns after epoch changed" are "[out of scope until Phase 7]"; "disk becomes
full during stream persistence" and "persisted state becomes corrupt" belong to Phase 5; and
exactly two rows name *this* phase — "processed ACK reaches broker but broker crashes before
storing state" and "client reconnects through another broker" [FAILURE_MODEL §3].

**What a broker of each family actually is, in the five columns that decide the first slice.**

| Family | What holds messages, and who creates it | Admission certificate | Consumer credit | Delivery outcome |
| --- | --- | --- | --- | --- |
| RabbitMQ / AMQP 0-9-1 | a **queue** that "stores messages for consumption"; declared on the wire, "create-if-absent and idempotent only when every attribute matches", and the empty name yields a server-named `amq.gen-*` queue [rabbitmq-amqp091 §2] | `basic.ack` in confirm mode, meaning "accepted by all the queues" generally, accepted **and persisted** for a persistent message in a durable classic queue, a majority "wrote *and flushed*" for a quorum queue, quorum replication without an explicit `fsync` for a stream [rabbitmq-amqp091 §6d] | `basic.qos` prefetch, in messages, granted by the consumer; at the bound "RabbitMQ stops delivering on that channel until an acknowledgement frees a slot — no error, no drop, no disconnect" [rabbitmq-amqp091 §5] | consumer `basic.ack`/`reject`/`nack`; unacknowledged deliveries are requeued when their channel or connection closes, and `redelivered` is a hint a client MUST NOT rely on [rabbitmq-amqp091 §6k-l] |
| AMQP 1.0 | a **node**, "a named entity inside a container responsible for safe storage and/or delivery"; creation is out of scope — "AMQP 1.0 defines no operation to create, configure, enumerate or delete a node" [amqp10 §2] | `disposition(accepted)`, which certifies the receiver processed the message and "says nothing about disk"; durability is the separate per-message `header.durable` contract a target that cannot honour it MUST refuse [amqp10 §6.4], [amqp10 §6.5] | link credit, one unit per message, receiver-granted, communicated as an absolute limit — "`delivery-count + link-credit`, not an increment, so `flow` frames are idempotent"; initial credit `0`, and at zero "the sender pauses: nothing is dropped, nothing disconnects" [amqp10 §5.1]. Brokers turn it into prefetch: "**Prefetch is link credit**" [amqp10 §5.4] | settlement in up to four steps, and settling "is forgetting: idempotent, irreversible, one-way" [SYNTHESIS §4] |
| NATS JetStream | a **stream**, "a server-side JetStream message store bound to one or more subject patterns", plus a **consumer**, "a stateful server-side view of one stream, with delivery position and acknowledgement state"; both server-configured, storage memory or file [nats §2], [nats §11] | the publish acknowledgement, which "certifies JetStream accepted and committed that stream message according to the configured replication quorum" and "does not certify that any consumer received or processed the message" [nats §6]; memory storage gives no persistence at all [nats §12/P7], which is why [0004 §5] maps it to `Accepted` | `max_ack_pending`, in messages, default 1000: at the count "further delivery pauses until acknowledgements advance it" [nats §5] | `+ACK`/`-NAK`/`+TERM`, with redelivery on a timer — "If a required acknowledgement does not arrive before `ack_wait`, JetStream schedules redelivery", bounded by `max_deliver` [nats §6] |
| Kafka | a **partition**, "an ordered commit log and the unit of replication, leadership, offset ordering, and consumer-group assignment"; partition count "fixed at creation until changed administratively" [kafka §2], [kafka §11] | the Produce response under `acks=0`/`1`/`all`; `acks` "does not make a broker fsync each record before acknowledgement" [kafka §6] | none: "no broker-to-consumer per-message credit grant" — consumers pull byte-bounded fetches [kafka §5] | no per-record acknowledgement: "A committed group offset is the durable restart position for a group and partition, **not an acknowledgement on each record**" [kafka §2] |
| MQTT 5 | a **session** at the server, which binds and accepts while the client connects [mqtt5 §4]; topics "may be predefined by an administrator or created dynamically on first subscribe or first publish" [mqtt5 §4.7] | PUBACK/PUBCOMP per hop, and nothing about disk: "**No acknowledgement certifies durability.** The spec puts the decision on the solution developer" [SYNTHESIS §1 P7], [mqtt5 §12/P7] | the send quota, one QoS > 0 PUBLISH per unit, receiver-granted, per connection and "explicitly not session state" [mqtt5 §5] | per-hop QoS handshake; shared subscriptions distribute with **no capacity signal** — "the Server implementation is free to choose, on a message by message basis, which Session to use and what criteria it uses" [mqtt5 §4.2] |

**The shape all five share, and the two things they do not.** Every broker in the set is (1) a
named thing that holds messages, whose lifecycle the messaging protocol leaves out entirely
(AMQP 1.0, Kafka, JetStream), declares on the wire (AMQP 0-9-1) or creates implicitly (MQTT);
(2) *two* transfer points rather than one, in and out of that store, unaware of each other
[SYNTHESIS §4]; and (3) a per-consumer message credit, receiver-granted, in messages — four of
five, Kafka inverting it into a pull [SYNTHESIS §1 P12], [SYNTHESIS §2 D1, D2]. What they do not
share is a durability certificate — two of five certify nothing about disk — nor an ordering
promise wider than their store's own unit [SYNTHESIS §2 D3, D4]. Two further facts the table
understates: RabbitMQ names its credit-free, acknowledgement-free mode for what it is —
*at-most-once*, "fire-and-forget", explicitly "should be considered unsafe", a delivery counted
as successful "immediately after it is sent out (written to a TCP socket)"
[rabbitmq-amqp091 §6], [rabbitmq-amqp091 §6g-j]; and without prefetch its dispatch is blind,
"every n-th message to the n-th consumer" [SYNTHESIS §1 P3]. weida's own row in the
transfer-point table is the one to move: "One: sender to the peer's **transport**. There is no
application-level transfer point in v0" [SYNTHESIS §4].

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — a queue at an endpoint path: `Accepted` in, per-subscription credit out, `Processed` back, everything in memory** | the broker registers a queue on a path; a producer's exchange reply half carries the confirm; a consumer grants credit and acknowledges each delivery; nothing survives the process | RabbitMQ's two orthogonal transfer points [rabbitmq-amqp091 §6]; AMQP 1.0 link credit and `accepted` [amqp10 §5.1], [amqp10 §6.4]; JetStream `max_ack_pending` plus `ack_wait` [nats §5], [nats §6] | Loses everything to a restart, so it may never say `Stored` [GUARANTEES §1] and closes no Phase 5 or Phase 7 failure row [FAILURE_MODEL §3]. A non-durable queue is a decoupler, not a safety net, and the note has to say so in those words |
| B — A plus `Stored(Written)` in the same slice | the queue writes before confirming | RabbitMQ's persistent message in a durable classic queue [rabbitmq-amqp091 §6d] | Needs a payload store, a WAL and recovery — Phase 5 in full [IMPLEMENTATION §6] — plus the batching latency RabbitMQ documents, confirms reaching "hundreds of milliseconds" [rabbitmq-amqp091 §6d], and the two Phase 5 failure rows [FAILURE_MODEL §3]. Pre-empting a phase to reach one word is what "Work MUST NOT be spread chaotically across phases" forbids [IMPLEMENTATION §1] |
| C — A without a consumer acknowledgement (auto-ack) | the broker deletes on write-out | RabbitMQ's `no-ack` mode [rabbitmq-amqp091 §6g-j]; JetStream `AckNone` [nats §6] | At-most-once delivery, which RabbitMQ's own documentation calls unsafe and which buys nothing over the Push/Pull this tree already has: a hop added, a guarantee removed. It also leaves the queue unable to decide when a message is done, so redelivery — half of what [ARCHITECTURE §1] calls L2 — has no state to hang on |
| D — a log with offsets instead of a queue | append-only, consumers hold positions, an acknowledgement deletes nothing | Kafka partitions and committed offsets [kafka §2] | Needs a position identifier weida does not have, the recorded weakest link of the Kafka chain [SYNTHESIS §7.3], plus retention policy and a partition concept. It is a second programming model, which [ARCHITECTURE §1] forbids: "L2 is a layer, not a fork" |
| E — A without the credit frame; deliver against the consumer's QUIC stream budget | the broker pushes, bounded only by `max_concurrent_uni_streams` | 0003's own option A [0003 §3]; MOQT's refusal of per-object credit [prior-art §4] | Its loss is already written down: "L2 credit is per connection, not per queue or subscription; cannot be withdrawn without closing streams" [0003 §3]. It leaves `basic.qos`, link credit, `Receive Maximum` and `max_ack_pending` with nothing to map onto, which is what [0003 §5] promised them, and it ignores the roadmap's own gate for this phase [LOOP §9] |
| F — a queue declared on the wire, as AMQP 0-9-1 does | a client creates and configures queues with a frame | `queue.declare`, create-if-absent, server-named `amq.gen-*` [rabbitmq-amqp091 §2] | Remote-controlled state with a remote-chosen name and lifetime — the allocation shape [INVARIANTS] wants bounded before it exists — plus a management vocabulary (declare, delete, purge, precondition failure) larger than the whole first slice. AMQP 1.0 shows the alternative is respectable: node lifecycle is "entirely out of scope of the core standard" [amqp10 §2] |

## 4. Decision

**Option A, with the credit frame option E would have deferred, and options B, C, D and F
refused for the first slice.** The minimal L2 broker is one process holding named queues at
endpoint paths, taking responsibility for a message in memory and saying so, handing each
message to exactly one consumer under credit that consumer granted, and taking the consumer's
word for the outcome. It issues two of the four reserved certificates and refuses the other two
by name.

**4.1 The shape, in one paragraph.** A queue is a registered endpoint path served by the
`weida-broker` crate [ARCHITECTURE §4]. A producer sends a message to that path **as an
exchange** — a bidi stream — and the reply half carries the broker's certificate: `Accepted`, or
an ERROR when the queue refuses. A consumer registers with SUBSCRIBE on the queue's path
[PROTOCOL §6.4] and grants credit with the frame of [0003](0003-credit-unit.md) §4.2; the broker
delivers each message to exactly one consumer, as one transfer inside the credit limit; the
consumer answers `Processed` or refuses, and the broker forgets or redelivers. Everything the
broker holds is in memory, bounded, and lost when the process ends — which is why it never says
`Stored`.

**4.2 The first slice implements `Accepted` and nothing above it.** `Stored(Written)` is not
available, because it is defined as surviving "the **broker process** dying: crash, restart or
orderly stop" [GUARANTEES §1], [0004 §4.1], and the first slice has no store to survive in: "No
persistence. No payload store, no WAL, no recovery. Phase 5" [IMPLEMENTATION §6]. Reporting it
anyway is not a shortcut but a prohibited claim — "`Stored` MUST NOT be reported for an
in-memory buffer" [GUARANTEES §1] — and that prohibition is what makes the level worth anything.
`Stored(Flushed)` and `Replicated(n, flushed)` stay reserved one phase further out: replication
is Phase 7 [IMPLEMENTATION §1], and `n` must be *achieved at the commit* [0004 §4.3], which
presupposes a commit. A configuration asking for a level this broker cannot reach is refused
when it is configured, never degraded [GUARANTEES §4], [0004 §4.5], [0006 §4.7] — which is also
how the slice stays honest at an adapter edge: RabbitMQ classic persistent confirms map to
`Stored(Written)` [0004 §5], so a D2 bridge that needs them is a refused configuration rather
than an approximation [0006 §4.8].

**4.3 On the consumer hop it implements `Processed`, and that is not a second claim about the
producer's message.** A queue that cannot learn when a delivery is done cannot delete it, cannot
redeliver it, and is option C. The level that fits is already defined: "the next hop's
application-level consumer has explicitly reported successful processing" [GUARANTEES §1] — the
consumer's statement about its own hop, the same thing AMQP 1.0's `disposition(accepted)`
certifies and which [0004 §5] maps to `Processed` rather than `Stored` because it "says nothing
about disk" [amqp10 §6.5]. **Amended by [0029](0029-a-report-is-relayed-a-certificate-is-not.md)
§4.1:** this paragraph said "the broker MUST NOT relay it", and the rule it meant is that a hop
MUST NOT present another hop's report as **its own guarantee**. A cursor is a report and the
negotiated `acknowledgement` dimension is the guarantee [0023 §4.5], so the broker's certificate
to the producer stays `Accepted` however the consumer later answers — unchanged, and the part
this paragraph was protecting — while the consumer's report may be re-emitted toward the
producer **as the consumer's**, bounded to one queue and never fabricated [0029 §4.3, §4.4].
What remains true as written: the two transfer points are "entirely orthogonal and unaware of
each other… Neither is end-to-end" [rabbitmq-amqp091 §6] in the sense that neither *certifies*
the other. A producer that needs a consumer's own answer rather than its report still uses an
application reply, which is what every sheet says too [SYNTHESIS §2 D3].

**4.4 The credit frame is *in* the first slice, and it is frame kind `5`.** Kind `5` is the
number PROTOCOL reserves [PROTOCOL §4], [PROTOCOL §11], and the slice spends it rather than
deferring it, for four reasons in descending order of force:

1. **The first Queue consumer flow needs it.** A broker that accepts responsibility must also
   bound outstanding deliveries per subscription; deferring credit would make the first slice
   runnable only under an unbounded consumer assumption.
2. **Without it there is no per-queue bound on deliveries.** The alternative bound is the
   consumer's stream budget: per connection, not per subscription, and it "cannot be withdrawn
   without closing streams" [0003 §3]. A broker that cannot pause a consumer without resetting
   streams has no prefetch [rabbitmq-amqp091 §5].
3. **It makes distribution capacity-based rather than turn-based.** P3's answer in four of five
   families is a per-consumer message credit; MQTT, the family without one, is the family whose
   shared subscriptions have "no capacity signal" [SYNTHESIS §1 P3], [mqtt5 §4.2], and
   [0007 §5] already routed that construct here.
4. **Four adapters were promised this mapping.** "MQTT Receive Maximum, RabbitMQ
   `prefetch_count`, AMQP 1.0 link credit and JetStream `max_ack_pending` all map onto the L2
   credit of §4.2 in the same unit" [0003 §5], and D2 names `basic.qos` explicitly [LOOP §9].

Its fields are the ones 0003 fixed and this note does not reopen: a subscription — which is
`(endpoint, filter)` on the connection it arrived on [PROTOCOL §6.4], [0011 §4.2] — and an
absolute delivery limit, idempotent under loss and duplication [0003 §4.3], counting messages
only [0003 §4.4]. Initial credit is **zero**, following AMQP 1.0 [amqp10 §5.1]: a consumer that
registers and grants nothing receives nothing, the only default that cannot surprise a consumer
with a flood. The frame rides that path's connection [0011 §4.3]. What it does not carry in the
first slice is a drain bit: AMQP 1.0's `flow(drain=true)` answers "stop sending to me", which
[0009 §2] identifies as a credit question rather than a shutdown one, and credit `0` answers it.

**4.5 A queue's identity is an endpoint path, and the broker creates it.** A queue is **an
endpoint path** — opaque bytes, capped at 512 B, validated "for bytes and length only", an exact
key into a flat map with "exactly one answer per (stream kind, path)" [0007 §4.1],
[PROTOCOL §9.4]. It is **not** a topic: a topic is a selector *inside* one publisher's path
[0007 §4.1], so a queue-as-topic would either have no address or put several queues on one path,
breaking the single answer dispatch depends on. It is **not** a third namespace: [0007 §4.1]
refused that shape once and [0016 §4.1] refused it again for the conflation key. Three
consequences make the choice cheap:

- **The distribution policy belongs to the object at the path, never to the frame.** A path
  registered as a publisher fans out — every matching subscriber gets a copy [PROTOCOL §9.5] —
  and a path registered as a queue distributes: exactly one consumer per message. Both are
  already expressible, because "dispatch is a function of the stream kind and the addressed
  path" [PROTOCOL §9.4]. No frame, flag or filter syntax encodes the difference, which is the
  mistake `$share/{name}/{filter}` makes by putting the policy in the filter string
  [mqtt5 §4.2] and which [0007 §5] declined to copy.
- **Consumers register with SUBSCRIBE, unchanged.** It names an endpoint and a filter, both
  required, idempotent per connection and path [PROTOCOL §6.4]. A consumer of a whole queue
  sends the empty filter, which "matches every topic"; a consumer of part of one sends a
  segmented filter. A foreign filter reaches this surface only through an explicit Connector
  conversion. Subscriptions stay bounded by `max_subscriptions` and are dropped wholesale when
  the connection closes [PROTOCOL §6.4], [PROTOCOL §10].
- **Who creates a Queue: the administrative resource API, not the messaging wire.** Before the
  cluster cutover, process-local `BrokerConfig` is bootstrap input for the same runtime object.
  No declare frame — option F — following AMQP 1.0, where node lifecycle is "entirely out of
  scope of the core standard" [amqp10 §2], Kafka, where partition count is administrative
  [kafka §11], and JetStream, where a stream's configuration is server-side [nats §2]. A
  producer addressing a path with no Queue gets the refusal that already exists, ERROR
  `{UNKNOWN_ENDPOINT}` on the reply half [PROTOCOL §9.4]. A Connector serving AMQP 0-9-1 must
  explicitly refuse or implement `queue.declare`; it MUST NOT present a preconfigured Queue as
  one dynamically declared by the client [rabbitmq-amqp091 §2], [INVARIANTS].

**4.6 The confirm needs no new frame kind, because an exchange already has a reply half.** A
producer's message to a queue is an exchange: DATA on the initiating half, and the reply half
carries "either DATA or ERROR, exactly one frame, followed by FIN" [PROTOCOL §4.1]. That reply
*is* the publisher confirm, carrying the achieved level [0004 §5]. Here weida's one structural
advantage over AMQP 0-9-1 shows: `basic.publish` "has no reply method", which is why confirm
mode needs a channel flag and a publisher sequence counter to correlate asynchronous acks
[rabbitmq-amqp091 §6a-b], whereas here "the stream *is* the correlation, and nothing on the wire
names an exchange" [ARCHITECTURE §1]. A refusal uses §9.4's existing vocabulary — `{REJECTED}`
for a queue at its bound, `{UNKNOWN_ENDPOINT}` for a path with no queue — so a nack is no
invention either [PROTOCOL §9.4]. A producer that will not wait sends a one-way transfer to the
same path and gets the transport receipt and nothing more, the honest spelling of `acks=0`
[kafka §6]. **The whole first slice's frame budget is therefore one number: kind `5`.** Kind `6`
was the first free kind when this note was written, as [0016 §4.10] recorded; it has since been
**spent on the cursor stream** [0023], [0024 §4.4], and `7` is the first free kind now. That
spending also answers §4.3's open half: the consumer acknowledgement does **not** travel on the
reply half of a delivery exchange — a delivery stays a one-way transfer and settlement is a
cursor on a CURSOR stream, which is why §6's "a kind `6` carrying batched acks in RabbitMQ's
`multiple` shape" alternative is not merely unchosen but unavailable, and needs no `multiple`
flag: a cursor is cumulative because it is an absolute offset. A producer's confirm is
untouched by all of it — still DATA key `8` on the reply half of *its* exchange — and a producer
that wants a verdict without an exchange now orders `Accepted` as a cursor instead.

**4.7 The drain: a queue drain is a different operation, and here is what it means.** Plainly,
in 0009's own words: "**L2 owes a different drain, and may not present it as this one**"
[0009 §4.7]. The L0 drain is about bytes in flight — finished transfers reaching the peer's
transport, counted locally, no wire representation [0009 §4.2], [0009 §4.8]. A **queue drain** is
about responsibility: what the broker still owes for messages it has accepted. In the first
slice it means four things and no more. (1) **Stop admitting**: new messages are refused with
ERROR `{REJECTED}` on the reply half and new connections are refused as during an L0 drain —
that ordering is what makes a drain terminate, "stop admitting, then let what exists finish"
[0009 §4.5], NATS Lame Duck Mode's shape without its wire notification [nats §12/P17].
(2) **Keep delivering under credit already granted**, until the queue is empty or the deadline
expires; no new credit is solicited and a consumer that grants none is not waited for.
(3) **Settle what is outstanding**: unacknowledged deliveries are requeued at the deadline, the
shape RabbitMQ takes on channel close [rabbitmq-amqp091 §6k-l] — and with no store a requeue
into a queue that will not survive the process is honest only as a count. (4) **Report a count,
never an acknowledgement**: acknowledged, requeued, never delivered. "That is a true local
statement; it is not a claim about the peer" [0009 §4.6]. The deadline is mandatory, finite and
supplied by the caller — no infinite variant and no configuration default that could become one,
the most load-bearing line of 0009 [0009 §4.3].

Two further separations, because the catalogue calls three things "drain": AMQP 1.0's
`flow(drain=true)` is a **credit** drain, "a receiver stopping, not a sender finishing"
[0009 §2], [amqp10 §5.1], answered here by credit `0` (§4.4); and the L0 drain stays available
underneath, because a broker is an ordinary weida process whose own finished transfers must
still land. `Broker::drain(Duration)` and `Runtime::drain(Duration)` are two calls with two
counts, performed in that order.

**4.8 The persistence boundary, as a list, so that Phase 5 is not pre-empted.** *What the first
slice keeps, all in memory, every item bounded before it is allocated* [INVARIANTS]: the queue
registry, each queue's message bodies up to a per-queue byte budget, each subscription's granted
delivery limit and delivery count, and each subscription's unsettled deliveries up to a cap.
Those bounds are new numbers and they belong on the broker's own configuration rather than on
`weida-core::Limits`, because a queue is not a per-connection object and "Numbers that belong to
a runtime rather than to a connection live on `RuntimeConfig`" [PROTOCOL §10]: `queue_bytes` per
queue, `max_queues` per broker, `max_unsettled` per subscription as the ceiling on deliveries a
subscription may hold without reporting — named `max_unacked` here until
[0029](0029-a-report-is-relayed-a-certificate-is-not.md) §4.7 observed that nothing in it is an
ack. A queue at `queue_bytes` refuses admission — `Reject`, "a cap refused before
buffering" [GUARANTEES §6] — and never discards a message it has already confirmed, because a
confirmed message is one whose responsibility it took [GUARANTEES §1].

*What it refuses to promise, each with its owner:* `Stored(Written)`, `Stored(Flushed)` and
`Replicated(n, flushed)` (§4.2; Phases 5 and 7); survival of a broker restart in any form — the
queue is gone and every message in it with it; `Durable` deduplication, which "needs a store and
belongs to the broker" [PROTOCOL §11], [0001 §7.6], but not to this slice; subscription and
sequence resumption, assigned to L2 by [0008 §4.6] and unbuilt here, so a reconnecting consumer
is a new subscription with zero credit and `Stable` producer naming, which "needs a supplier that
outlives a connection" [GUARANTEES §6], stays undeclarable [0008 §4.5]; `Spill`, which "needs a
store, so it is in the same position as `Durable` deduplication" [0016 §6]; and retries, which
are Phase 4 [IMPLEMENTATION §6] — redelivery of an *unacknowledged* delivery is not a retry of a
failed transfer and must not be described as one.

*Which failure rows this closes.* Of the two rows naming Phase 6, "processed ACK reaches broker
but broker crashes before storing state" is not closed but made **vacuous**: the broker holds no
state a crash could leave inconsistent, because queue, delivery and acknowledgement vanish
together. That is a degenerate answer and this note says so rather than claiming coverage.
"Client reconnects through another broker" stays open: one broker process, no session resumption
[FAILURE_MODEL §3].

**4.9 What the broker is not — the list a reader will otherwise assume.**

| A reader may assume | The first slice | Who owns it |
| --- | --- | --- |
| clustering, failover, partition tolerance | absent; one process | Phase 7, "Broker clustering" [IMPLEMENTATION §1], and the four Phase 7 rows of [FAILURE_MODEL §3] |
| replication, `Replicated(n, flushed)` | absent and undeclarable | Phase 7; [0004 §4.2-§4.3] already fixed its meaning |
| durability, a WAL, recovery, `Stored(*)` | absent, refused by name (§4.2) | Phase 5, "Persistence subsystem" [IMPLEMENTATION §1], [IMPLEMENTATION §6] |
| exchanges, bindings, routing tables | absent as objects; routing is the segmented filter a consumer already sends (§4.5) | [0007 §4.2] for the grammar, [0007 §5] for the AMQP 0-9-1 mapping, D2 for the 0-9-1 server [LOOP §9] |
| consumer groups, partitions, offsets | absent; a queue with several consumers under credit answers the same problem (§4.4) | option D above; Kafka's shape needs a position identifier weida does not have [SYNTHESIS §7.3] |
| retries of a failed transfer | absent; redelivery of an unacknowledged delivery is a different thing (§4.8) | Phase 4 [IMPLEMENTATION §6] |
| `PerKey` ordering | absent; delivery order inside a queue is the queue's own order and no negotiated level promises it | [0001 §7.4] assigns `PerKey` to L2; it has no wire representation [PROTOCOL §11] and is not in this slice |
| `Coalesce` backpressure, a conflating queue | absent — but three of [0016 §4.7]'s four preconditions now exist (a queue that outlives the transfer, an owner for the key, the credit frame that defines "pending"); the fourth, a bound on the keyed table, does not | [0016 §4.7]; still a later decision |
| dead-letter queues, TTL, priorities, transactions | absent and unowned: no weida document specifies any of them, and this note adds none | nobody yet; §6 |
| a second programming model | refused: same patterns, same frames, one frame kind added (§4.6) | [ARCHITECTURE §1], "L2 is a layer, not a fork" |

**4.10 What reaches the wire.** Two changes, both of which the first slice writes into
[PROTOCOL.md](../PROTOCOL.md) rather than leaves implied: kind `5` becomes a known frame on a
connection that negotiated the broker's acknowledgement level, and `acknowledgement` value `2`
(`Accepted`) becomes honourable rather than merely declarable — §6.5's rule that levels "`2` to
`5` — are legal to *declare* and impossible to honour in v0" becomes a statement about v0
specifically [PROTOCOL §6.5]. Values `3` and `4` stay declare-only-and-fail, with their
`durability` and `replicas` axes unchanged [PROTOCOL §6.5], [0004 §4.4]. Negotiation needs
nothing new: offers intersect per dimension and a shortfall fails the connection with
`NEGOTIATION_FAILED` [0006 §4.4], which is how a client requiring `Stored(Flushed)` learns at
handshake time that it is talking to an in-memory broker instead of discovering it after a crash.

**4.11 Status is `provisional`, and what would change it.** Two choices are the kind a first
slice either vindicates or embarrasses: the confirm as an exchange reply half (§4.6) and initial
credit zero (§4.4). Both are cheap to reverse while the broker is one crate nobody depends on,
and expensive afterwards. The note becomes `accepted` when B-201 to B-203 are green and their
acceptance lines held without amendment.

## 5. Consequences and follow-ups

- **[ARCHITECTURE.md](../ARCHITECTURE.md) §1, §4.** The L2 paragraph and the `weida-broker` line
  gain the first slice's boundary in one sentence — queues at endpoint paths, `Accepted` in,
  `Processed` out, nothing durable — so that "queues, publisher confirms, consumer
  acknowledgements with redelivery" reads as three slices rather than one heading.
- **[GUARANTEES.md](../GUARANTEES.md) §1.** The four states gain a line naming which two the
  first broker slice issues, on which hop, and that the broker never relays the consumer's
  `Processed` to the producer (§4.3). §6 stays a v0 table; the broker's status belongs beside it.
- **[PROTOCOL.md](../PROTOCOL.md) §4, §6.5, §9.4, §11.** Kind `5`'s reservation gains the fields
  of §4.4 when B-202 lands; §6.5 gains the qualification of §4.10; §9.4's dispatch tables gain a
  queue row per stream kind (bidi = admission with confirm, uni = admission without); §11's
  application-acknowledgement entry gains the exchange-reply-half encoding of §4.6.
- **[FAILURE_MODEL.md](../FAILURE_MODEL.md) §3 and
  [IMPLEMENTATION.md](../IMPLEMENTATION.md) §6.** The two Phase 6 rows gain §4.8's answers — one
  vacuous, one still open — and the debt entry "Application acknowledgements are absent, not
  partial" gains this note as the shape the first slice takes, keeping every word about `Stored`
  and `Replicated`.
- **[0009](0009-drain.md) §4.7** is answered by §4.7 here: the different drain now has a
  definition, a deadline rule and a count. **[0016](0016-conflation.md) §4.7** loses three of its
  four open preconditions (§4.9) and keeps the fourth.
- **Backlog.** B-198 is answered by this note, and `docs/decisions/README.md` gains its row per
  [LOOP.md](../LOOP.md) §5. The three items below replenish Phase D's head. Their links are
  written relative to `docs/BACKLOG.md`, because that is the file they are pasted into.

### B-201 — `weida-broker`: a queue at an endpoint path, and `Accepted`
kind: code | size: 90 | status: ready | needs: [B-198]
acceptance: the admission half of [0018](0018-minimal-broker.md) §4.1-§4.6 in a new crate `crates/broker/` → `weida-broker` ([ARCHITECTURE.md](../ARCHITECTURE.md) §4), depending on `weida` and depended on by nothing. A queue is registered on an endpoint path from the broker's configuration — no declare frame, §4.5 — and a producer's **exchange** to that path is answered on the reply half with the achieved level `Accepted` (§4.6), or with ERROR `{REJECTED}` when the queue is at `queue_bytes` and ERROR `{UNKNOWN_ENDPOINT}` when no queue is registered, both from [PROTOCOL.md](../PROTOCOL.md) §9.4's existing vocabulary; a one-way transfer to the same path is admitted with no confirm. The three bounds of §4.8 — `queue_bytes`, `max_queues`, `max_unacked` — exist with defaults and are documented beside the per-runtime numbers of [PROTOCOL.md](../PROTOCOL.md) §10. Tests assert what a producer observes: a confirm that arrives with **no consumer registered at all**, which is the temporal decoupling a queue exists for; a refusal at the byte bound that leaves the connection alive; and that no code path can report `Stored`, asserted by its absence from the type rather than by a comment.
note: the first slice deliberately delivers nothing. A queue that admits, confirms and holds is verifiable on its own, and splitting admission from delivery keeps each slice's acceptance observable.

### B-202 — Frame kind `5`: the L2 credit frame, and delivery under it
kind: code | size: 90 | status: ready | needs: [B-201]
acceptance: frame kind `5` decoded and encoded in `weida-protocol` with the fields [0003](0003-credit-unit.md) §4.2-§4.3 fixed and [0018](0018-minimal-broker.md) §4.4 confirms — a subscription and an **absolute** delivery limit — with golden vectors in [PROTOCOL.md](../PROTOCOL.md) §8's shape and a fuzz target beside the other header targets; the frame rides the connection of the path it names ([0011](0011-answered-where-it-arrived.md) §4.3). A consumer registers with SUBSCRIBE on the queue's path and receives **nothing** until it grants credit (initial credit zero, §4.4); a message is delivered to exactly one consumer as a one-way transfer; the broker stops at the limit and resumes when the limit is raised. Tests assert the three properties a credit scheme is bought for: a duplicated or reordered credit frame changes nothing; a consumer at its limit receives no further delivery while another consumer with credit does; and credit lowered to `0` pauses delivery without resetting a stream or closing the connection.
note: the roadmap gated Phase D on this frame having "a consumer on both ends" ([LOOP.md](../LOOP.md) §9). This is the end inside weida; the adapters named in §5 of [0003](0003-credit-unit.md) are the other.

### B-203 — Consumer acknowledgement, redelivery, and the queue drain
kind: code | size: 90 | status: ready | needs: [B-202]
acceptance: the outcome half of [0018](0018-minimal-broker.md) §4.3 and §4.7. A delivery is answered by the consumer with `Processed` or a refusal on the reply half of the delivery exchange; an acknowledged message leaves the queue and frees one unit of `max_unacked`; a refused or unanswered delivery is requeued and redelivered to any consumer with credit, and a redelivery is observable as one by the consumer. Unacknowledged deliveries of a connection that closes are requeued, the shape [rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §6 documents. `Broker::drain(Duration)` implements the four steps of §4.7 — stop admitting, keep delivering under granted credit, requeue what is outstanding at the deadline, return a count of acknowledged, requeued and undelivered — with a mandatory finite deadline and no infinite variant ([0009](0009-drain.md) §4.3), and its documentation states in one sentence that it is **not** the L0 drain and certifies nothing about a peer. Tests assert: a consumer that drops a delivery without answering sees it again; a queue drain with a silent consumer terminates at its deadline and reports the requeue; and the producer's certificate stays `Accepted` however the consumer answers, which is hop-locality ([GUARANTEES.md](../GUARANTEES.md) §2).
note: this is the slice that makes the broker worth its hop. Without a consumer acknowledgement the queue is RabbitMQ's `no-ack` mode, which its own documentation calls unsafe.
note: **this snapshot is what the note proposed, not what was built.** B-203 was executed under
[0029](0029-a-report-is-relayed-a-certificate-is-not.md): settlement is a **cursor** the delivery
orders, not an answer on a reply half — a delivery stayed one-way — the bound is `max_unsettled`,
and the drain is `retire(deadline)` returning `Retired { settled, unsettled, requeued }`. The
wording above is kept as written so that what the note asked for stays readable.

## 6. What this note does not decide

- **The protocol version the broker ships under.** Kind `5` becoming known and `acknowledgement`
  `2` becoming honourable are wire changes (§4.10); `0.x` permits them — "breaking changes are
  permitted. Implementations MUST NOT assume any compatibility guarantee across `0.x` releases"
  [PROTOCOL Status and scope] — but the number is chosen when B-202 lands, not here.
- **Anything about durability, and anything about clustering.** `Stored(Written)` is refused for
  the first slice (§4.2), not argued against: its definition stands [0004 §4.1] and Phase 5 owns
  the store. Replication and failover are Phase 7, with the four failure rows naming it
  [FAILURE_MODEL §3].
- **A wire vocabulary for managing queues.** Option F is refused for the first slice only; a
  later slice wanting `queue.declare` semantics for D2's 0-9-1 server needs a bounded name space,
  a lifetime policy and a refusal vocabulary, and that is its own decision
  [rabbitmq-amqp091 §2], [INVARIANTS].
- **Dead-letter queues, message TTL, priorities and transactions.** No weida document specifies
  any of them and this note adds none. RabbitMQ has TTL expiry and length limits that remove a
  queued message with no signal to the publisher [rabbitmq-amqp091 §6]; AMQP 1.0 has transactions
  as a coordinator link [amqp10 §6.6]. Later questions, both.
- **Whether a subscription ever outlives its connection.** [0008 §4.6] assigns subscription and
  sequence resumption to L2; the first slice does not build them, so `Stable` producer naming
  stays undeclarable [GUARANTEES §6].
- **`PerKey` ordering and `Coalesce` backpressure** — both L2-owned, neither in the first slice
  [0001 §7.4], [0016 §4.7] — and **whether the broker is ever the durable hop of a bridge**:
  "Store before forwarding is not the default, and a chain that needs it is refused"
  [0006 §4.8], and an in-memory broker cannot be that hop at all.

**What would reopen this.** Two things, both checkable. First, a D2 client whose `basic.qos`
prefetch cannot be expressed as an absolute delivery limit per subscription — the mapping
[0003 §5] promised and §4.4 relies on; if the channel-wide `global` flag
[rabbitmq-amqp091 §5] turns out to need a second scope, the credit frame's fields are wrong
rather than incomplete. Second, a measured cost: one exchange per published message spends a bidi
stream where a one-way transfer spends a uni stream, and the per-transfer numbers of
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §4 do not cover an exchange at a broker's message rate.
If a confirm costs more than a queue is worth, §4.6 is the choice to revisit, and the alternative
is a kind `6` carrying batched confirms in RabbitMQ's `multiple` shape [rabbitmq-amqp091 §6d].

## 7. Sources

weida documents: [LOOP.md](../LOOP.md) §5, §9; [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §2, §4;
[GUARANTEES.md](../GUARANTEES.md) §1, §2, §3, §4, §6; [PROTOCOL.md](../PROTOCOL.md) Status and
scope, §4, §4.1, §6.4, §6.5, §8, §9.4, §9.5, §10, §11;
[FAILURE_MODEL.md](../FAILURE_MODEL.md) §3; [IMPLEMENTATION.md](../IMPLEMENTATION.md) §1, §4, §6;
[INVARIANTS.md](../INVARIANTS.md); [PATTERNS.md](../PATTERNS.md) §1.4, §4;
[BACKLOG.md](../BACKLOG.md) B-198; [0001](0001-sequence-field.md) §7.4, §7.6;
[0003](0003-credit-unit.md) §3, §4.1-§4.4, §5; [0004](0004-durability-levels.md) §4.1-§4.6, §5;
[0006](0006-guarantee-sets.md) §4.4, §4.7-§4.9; [0007](0007-topic-namespace.md) §4.1, §4.2, §5;
[0008](0008-session-identity.md) §4.5, §4.6; [0009](0009-drain.md) §2, §4.2-§4.8;
[0011](0011-answered-where-it-arrived.md) §4.2, §4.3; [0016](0016-conflation.md) §4.1, §4.7,
§4.10, §6.

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §1 (P3, P7, P12), §2 (D1, D2, D3, D4),
§4, §7.3; [rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §2, §5, §6 (including its
labelled items §6a-b, §6d, §6g-j, §6k-l), §12/P3, §12/P7;
[amqp10.md](../research/amqp10.md) §2, §5.1, §5.4, §6.4, §6.5, §6.6;
[nats.md](../research/nats.md) §2, §5, §6, §11, §12/P7, §12/P17;
[kafka.md](../research/kafka.md) §2, §5, §6, §11; [mqtt5.md](../research/mqtt5.md) §4, §4.2,
§4.7, §5, §12/P7; [prior-art.md](../research/prior-art.md) §4 (flow control).

Code read: none, and deliberately — this note decides a layer that does not exist yet. What it
read instead is where a new bound may live (`crates/core/src/limits.rs` against
[PROTOCOL.md](../PROTOCOL.md) §10's split between per-connection and per-runtime numbers) and
which frame kinds are already spent (`crates/protocol/src/header.rs`).
