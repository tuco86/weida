# 0006 — Guarantee sets and explicit managed-resource boundaries

- **Status:** accepted
- **Date:** 2026-09-11
- **Related:** SYNTHESIS §8.7; P1, P7, P9, P14; decisions 0001 §7.3, 0003 §4.2, 0004 §4.4

## 1. The question

Two things have to be decided together. The first is what a guarantee is as a configurable
object: the dimensions exist as independent vocabularies
([GUARANTEES.md](../GUARANTEES.md) §3), while a connection needs one set it can negotiate and
validate. The second is what happens when a future managed Connector joins a foreign-protocol
source or sink to a Queue whose responsibility transfer sits in a different place.

The constraints are configuration-time refusal and hop-local honesty: invalid combinations are
rejected before traffic, standalone foreign libraries keep their own completion vocabulary,
and one Connector's explicit conversion policy never becomes a global protocol mapping.

## 2. The evidence, condensed

**The dimensions exist; a unit of configuration does not.** Guarantees "MUST NOT be collapsed
into a single enum such as `Reliable`", and the five dimensions are Delivery (`BestEffort`,
`AtMostOnce`, `AtLeastOnce`), Acknowledgement/completion (`None`, `TransportReceipt`, and the
reserved `Accepted`, `Stored`, `Replicated(...)`, `Processed`), Ordering (`None`, `PerProducer`,
`PerKey`, `Total`), Deduplication (`None`, `Bounded`, `Durable`) and Backpressure (`Block`,
`Reject`, `Drop`, `Spill`, `Coalesce`) [GUARANTEES §3]. What v0 actually offers is one point in
that space: transport receipt only, delivery `BestEffort`, ordering `None`, dedup `None`,
backpressure `Block`/`Reject`/`Drop` [GUARANTEES §6]. The illustrative configurations of §4
("very fast", "reliable job processing", "strong broker persistence") are prose, not objects
[GUARANTEES §4].

**The two configuration rules are already normative**, verbatim [GUARANTEES §4]:

> - Invalid combinations MUST be rejected. Validation is explicit and happens at configuration
>   time, not silently at runtime.
> - A requested guarantee MUST NEVER be silently weakened (master doc §81 rule 6). If a peer or
>   a build cannot honour a requested guarantee, the operation MUST fail visibly.

**The handshake already has the right shape for a declaration, and none of the content.** Each
side sends exactly one HELLO and FINs it; before the peer HELLO is processed, DATA is parked
[PROTOCOL §2.2]. HELLO carries `versions`, `max_header_bytes`, `max_transfers`, `capabilities`
(v0 `[]`) and `required_capabilities` (v0 `[]`), and `negotiate(ours, theirs)` is pure: it
intersects versions, selects the maximum common one, and **fails if any peer-required capability
is unsupported**, closing with `NEGOTIATION_FAILED` [PROTOCOL §6.1], [PROTOCOL §2.3]. So the
mechanism for "declare what I offer, require what I need, fail rather than proceed" exists and
is empty.

**Transfer points do not line up, and one protocol has none.** §4 of the synthesis records the
moment responsibility moves for each protocol: ZeroMQ transfers at `zmq_send` returning into the
local queue and never again on the wire [zeromq §6]; core NATS has **no transfer point** at all,
because `+OK` acknowledges only a well-formed operation [nats §6]; MQTT has one per hop with
independently selected QoS [mqtt5 §6]; RabbitMQ has two, publisher-to-leader and queue-to-consumer,
"entirely orthogonal and unaware of each other" [rabbitmq-amqp091 §6], [rabbitmq-amqp091 §6d];
JetStream two, the first after quorum commit [nats §6]; Kafka producer-to-ISR plus an offset
commit that "merely moves restart position and deletes nothing" [kafka §6], [kafka §2]; AMQP 1.0
up to four settlement steps, settlement being "idempotent, irreversible, one-way"
[amqp10 §6.1], [amqp10 §6.2]. weida v0 has exactly one: sender to peer **transport**, marked by
`Delivery::delivered()` [GUARANTEES §3], [GUARANTEES §6]. The table states the bridging rule
itself: protocols can only be joined where their transfer points coincide, "or where the bridge
itself becomes a transfer point" [SYNTHESIS §4].

**Each of the three shapes is already forced somewhere by the chains.** Refusal: a ZeroMQ PUB
source into a RabbitMQ quorum queue with confirms, because PUB drops silently at the high-water
mark and the publisher is never told [SYNTHESIS §7.1]; MQTT QoS 2 into AMQP 1.0
`rcv-settle-mode=second` "must be refused as an end-to-end exactly-once claim" [SYNTHESIS §7.2];
Kafka into several weida streams when downstream order matters [SYNTHESIS §7.3]. Store before
forwarding: RabbitMQ to ZeroMQ preserves at-least-once only if the bridge "store[s] the message
before acking", becoming the durable hop [SYNTHESIS §7.1]. Degrade explicitly: acknowledge on
the weaker transport receipt, or commit Kafka offsets early and accept at-most-once
[SYNTHESIS §7.1], [SYNTHESIS §7.3].

**MQTT shows what an honest degradation looks like.** Publisher-to-broker and
broker-to-subscriber are "two separate hops, each with its own QoS"; the outbound QoS "could
differ", and the rule is that a subscription's delivery QoS "MUST be the minimum of the QoS of
the originally published message and the Maximum QoS granted by the Server" — downgraded, never
upgraded [mqtt5 §6]. The granted maximum is stated in the subscription acknowledgement, before
any message flows; the consequence is stated too, that QoS 2 is exactly-once "strictly per hop"
and a QoS 2 publish delivered at QoS 1 yields duplicates [mqtt5 §12/P9]. The degradation is
legal because it is declared and bounded, not because it is small.

**The two invariants that fence this in**, verbatim [INVARIANTS]:

> - All guarantees are defined against the immediate next hop.
> - A managed Connector may claim only what its concrete source, queue and sink can prove.

The first is mechanically checked today through the definition of `delivered()`. The second is
deferred with the broker resource subsystem. Standalone foreign-protocol libraries terminate no
weida guarantee chain and need no cross-protocol guarantee mapping.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| A — per-message degradation | each message carries the guarantee it got; the sender inspects the outcome | MQTT's per-hop QoS minimum [mqtt5 §6] | every sender must check every message to learn what it received; a sender that does not check is silently weakened |
| B — a default set plus configured supersets, validated at configuration time | guarantees are a tuple over the dimensions of §3; HELLO declares offered and required sets; the intersection decides; a managed Connector declares the concrete level its configured path achieves | `required_capabilities` failing the handshake [PROTOCOL §2.3]; AMQP 1.0 refusing `header.durable` it cannot honour [amqp10 §6.5] | a validation step and a declaration on the wire; a Connector needing a durable queue is refused when none is configured |
| C — exact equality only, no supersets | both sides must be configured identically or the connection fails | none | a weida-only network could not opt into stronger behaviour |
| D — store before forwarding as every Connector's default | every Connector becomes a durable transfer point | RabbitMQ→ZeroMQ chain [SYNTHESIS §7.1] | turns protocol integration into mandatory persistence even where the resource asks for none |

## 4. Decision

Option B.

1. **A guarantee set is the unit of configuration.** A set is a tuple with one level per
   dimension of [GUARANTEES.md](../GUARANTEES.md) §3 — delivery, acknowledgement/completion,
   ordering, deduplication, backpressure — where an acknowledgement level of `Stored` or
   `Replicated` carries the durability axes of 0004 §4.1-§4.2. A set is never a single enum, and
   no set may name a level outside that vocabulary: a guarantee set introduces no new words, only
   a way to carry the existing ones as one object [GUARANTEES §3].

2. **`core` is the default set, and it is what v0 does.** Delivery `BestEffort`,
   acknowledgement `TransportReceipt`, ordering `None`, deduplication `None`, backpressure
   `Block` (with `Reject` at an endpoint's queue bound and `Drop` for fan-out only)
   [GUARANTEES §6]. An endpoint configured with nothing gets `core`; nothing in the hot path
   is allocated for a dimension `core` leaves at `None`, which is the existing invariant that
   disabled guarantees do not participate [INVARIANTS].

3. **Inside the weida network, a configured set may only be a superset of `core`.** "Superset"
   is defined per dimension: set `B` is at least `A` iff for every dimension `B`'s level is
   greater than or equal to `A`'s in that dimension's own order. Where a dimension's levels form
   a partial order rather than a ladder — the persistence-versus-replica-count case of 0004 §4.4
   — the comparison is per axis, and incomparable levels do not satisfy each other. Backpressure
   is not ordered at all: its levels are behaviours, not strengths, so a configured set states
   the behaviour and two peers must state the same one or fail.

4. **Declaration and intersection happen in HELLO, and a shortfall fails the connection.** Each
   side declares the set it *offers* and the set it *requires*. `negotiate()` computes the
   intersection per dimension — the weaker level of the two offers — and fails with
   `NEGOTIATION_FAILED` when the result does not reach what the peer requires, which is exactly
   how `required_capabilities` already behaves [PROTOCOL §2.3], [PROTOCOL §6.1]. There is no
   downgrade path: a requested level the peer does not offer is a failed handshake, never a
   quieter success. The wire encoding of the declaration is the work of decisions already
   scheduled (PROTOCOL §2.3/§6.1 sync and the `Hello` fields); what is decided here is the rule.

5. **Validation is at configuration time.** A guarantee set is validated when an endpoint,
   connection or managed resource is configured, against what the build and the local
   configuration can honour, before traffic starts — the first rule of
   [GUARANTEES.md](../GUARANTEES.md) §4. Per-message refusal exists only where the protocol
   itself makes the property per-message: AMQP 1.0's `header.durable`, where a target that
   cannot honour it MUST NOT accept the message and answers `amqp:precondition-failed`
   [amqp10 §6.5], [0004 §4.5]. weida itself adds no per-message guarantee flag.

6. **Standalone foreign-protocol libraries are outside this negotiation.** A ZeroMQ, NNG,
   MQTT, AMQP or NATS socket speaks its own protocol and reports that protocol's own completion
   states. Similar names do not create a weida guarantee, and no global mapping is inferred.

7. **A managed Connector declares one concrete boundary.** Its resource specification names a
   source or sink, the Queue it attaches to, its conversion policy and the guarantee level the
   configured path requires. Apply is refused when that specific composition cannot honour the
   declaration. This is resource validation, not a protocol-wide equivalence table.

8. **Durability comes from the Queue, never from translation.** A Connector may report a durable
   level only after the attached queue's store or replicated group has achieved it. Holding a
   message in a task or socket queue is `Accepted` at most and must not be presented as
   `Stored`.

9. **Protocol-specific facts stay protocol-specific.** Transfer points, acknowledgements,
   security and overload behavior remain documented by the foreign library and its research
   sheet. A Connector implementation documents only the resource schema and conversion it
   actually implements; it must not publish a socket-to-pattern mapping as a framework rule.

## 5. Consequences and follow-ups

- **[GUARANTEES.md](../GUARANTEES.md) §3/§4.** A new subsection defines the guarantee set, names
  `core` with the levels of §4.2, states the per-dimension superset relation of §4.3 including
  the unordered backpressure dimension, and folds the intersection-and-fail rule of §4.4 into the
  two existing configuration rules.
- **[GUARANTEES.md](../GUARANTEES.md) §6.** The v0 status table is the normative source for
  `core`; it says so, so that the default set cannot drift away from what the code does.
- **[PROTOCOL.md](../PROTOCOL.md) §2.3/§6.1.** HELLO carries the offered and required guarantee
  declarations and `negotiate()` computes the per-dimension intersection, failing with
  `NEGOTIATION_FAILED` on a shortfall. Wire keys, encoding and golden vectors are the scheduled
  wire work, not this note.
- **[INVARIANTS.md](../INVARIANTS.md).** The future Connector subsystem is checked by
  configuration-time validation against the concrete source, Queue and sink it names.
- **[ARCHITECTURE.md](../ARCHITECTURE.md) §1.** The configuration surface distinguishes
  endpoint/connection guarantee sets from a managed Connector resource's explicit policy.
- **Phase D.** Connector implementations follow the control-plane resource model of
  [0022](0022-consensus-topology.md); no protocol-wide mapping document or cross-adapter test
  is required.
- **[SYNTHESIS.md](../research/SYNTHESIS.md) §8.7** is closed by this note.
- **Open, deliberately.** Whether a guarantee set may vary per endpoint on one connection, or
  only per connection, is left to the wire work: 0001 §7 already records the same question for
  ordering, and the answer must be one answer for both.
- **Extended by the wire work (B-014), recorded here rather than silently.** §4.1 says a set
  carries one level per dimension of [GUARANTEES.md](../GUARANTEES.md) §3 and "introduces no
  new words". The HELLO fields needed two dimensions §3 did not have — producer naming
  ([0001](0001-sequence-field.md) §7.3, [0008](0008-session-identity.md) §4.3) and control
  isolation ([0002](0002-control-and-bulk-separation.md) §6.1) — and they arrived as
  [PROTOCOL.md](../PROTOCOL.md) §6.5 keys `8` and `9`. Both were then added to GUARANTEES §3,
  which is what keeps §4.1 true: the rule is not "these five dimensions forever" but **no wire
  key may name a dimension the vocabulary does not**. A future dimension follows the same
  order: vocabulary first, then a key.

## 6. Sources

weida documents: [GUARANTEES.md](../GUARANTEES.md) §1, §3, §4, §6;
[PROTOCOL.md](../PROTOCOL.md) §2.2, §2.3, §5, §6.1; [INVARIANTS.md](../INVARIANTS.md);
[0001](0001-sequence-field.md) §7.3; [0003](0003-credit-unit.md) §4.2;
[0004](0004-durability-levels.md) §4.1, §4.2, §4.4, §4.5.

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §2 (D3, D7), §4, §7.1, §7.2, §7.3,
§8.7; [mqtt5.md](../research/mqtt5.md) §6, §12/P9;
[rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §6, §6d;
[amqp10.md](../research/amqp10.md) §6.1, §6.2, §6.5; [nats.md](../research/nats.md) §6;
[kafka.md](../research/kafka.md) §2, §6; [zeromq.md](../research/zeromq.md) §6;
[nanomsg-nng.md](../research/nanomsg-nng.md) §6.
