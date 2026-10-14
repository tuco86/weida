# 0003 — Flow-control credit — unit and layer

- **Status:** accepted
- **Date:** 2026-09-10
- **Related:** SYNTHESIS §8.1; P3, P4, P12; decisions 0001 §7.8, 0002 §6

## 1. The question

weida's credit is mixed and entirely transport-granted: bytes for the QUIC windows
(`stream_receive_window`, `connection_receive_window`), streams for the concurrent-stream
budgets (`max_concurrent_uni_streams`, `max_concurrent_bidi_streams`), and messages only for
the local `endpoint_queue` ([GUARANTEES.md](../GUARANTEES.md) §6, [PATTERNS.md](../PATTERNS.md)
§1.3-§1.4). SYNTHESIS §8.1 asks whether an L2 hop needs a message-unit credit signal of its
own, or whether QUIC's byte windows plus the stream budget are the whole answer for a
stream-native protocol, and notes the sub-decision that if credit ever goes on the wire, the
absolute-limit form makes it idempotent [SYNTHESIS §8.1], [amqp10 §5.1].

This note was decided directly from the catalogue evidence; the options are recorded in §2
for the same reason the other notes record theirs.

## 2. The evidence, condensed

**Messages are the near-universal credit unit outside QUIC.** RabbitMQ `prefetch_count` in
messages with `prefetch_size` in bytes specified and never implemented
[rabbitmq-amqp091 §5]; AMQP 1.0 link credit in messages plus a session window in frames, the
two tied together "nowhere", with "no guidance on relative sizing, which is where brokers
differ most" [amqp10 §5.2], [amqp10 §5.3]; MQTT Receive Maximum in QoS > 0 packets, with no
byte credit and none at all for QoS 0 [mqtt5 §5]; JetStream `max_ack_pending` in messages and
pull batches in messages [nats §5]; Kafka the outlier, bytes only (`fetch.max.bytes`,
`max.partition.fetch.bytes`) with no per-record credit [kafka §5]. ZeroMQ and NNG have no
credit at all, only high-water marks that block or drop [zeromq §5], [nanomsg-nng §5]. The
synthesis records this as denominators D1 (unit) and D2 (receiver-granted is the majority)
[SYNTHESIS §2].

**On QUIC, the projects that added an application credit layer regret it or fence it.** MOQT
deliberately has no per-object credit and instead documents the deadlock rule — "endpoints
MUST allocate connection flow control to the control streams before allocating it to any
data streams" — and its one remaining credit, `MAX_REQUEST_UPDATES`, replaced a request-id
credit scheme that was removed [prior-art §4 flow control], [prior-art §8 lesson 8]. Cap'n
Proto's `setFlowLimit` warns that "the flow limit may prevent those calls from completing,
leading to deadlock" [prior-art §8 lesson 8]. WebTransport excludes stream headers and
capsules from `WT_MAX_DATA` so that associating a stream with its session can never block
[prior-art §8 lesson 8].

**QUIC already provides a receiver-granted, absolute, idempotent message credit for a
stream-per-message design.** MAX_STREAMS is cumulative and monotonic, raised only by the
receiver [quic-standards §2.8]; a transfer holds its stream until read to EOF, dropped or
refused, and at the bound "the next `open` waits. It does not fail" [PATTERNS §1.4]. The
absolute-offset form of MAX_DATA and MAX_STREAM_DATA is the same idempotent shape AMQP 1.0
chose for `flow` [quic-standards §3.1], [amqp10 §5.1].

**What message credit alone cannot express.** Size: ten messages may be ten kilobytes or ten
gigabytes. AMQP 1.0 answers with its second level and inherits the sizing problem
[amqp10 §5.3]; Kafka answers with bytes only [kafka §5]. On weida every transfer is a stream,
so the consumer's QUIC windows bound the bytes of each message and of the connection
regardless of what an application credit says [PATTERNS §1.3].

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| A — no application credit at any layer | MAX_STREAMS is the message credit, MAX_DATA the byte credit, at L0 and L2 alike | QUIC itself [quic-standards §2.8]; MOQT [prior-art §4] | L2 credit is per connection, not per queue or subscription; cannot be withdrawn without closing streams |
| B — message credit on the L0 wire | a credit-grant control frame beside HELLO/SUBSCRIBE | AMQP 1.0 link credit [amqp10 §5.1]; MQTT Receive Maximum [mqtt5 §5] | second credit level next to QUIC, with the relative-sizing problem [amqp10 §5.3] and the deadlock shape [prior-art §8 lesson 8] |
| C — no L0 credit; explicit L2 credit per subscription | control-connection frame carrying an absolute delivery limit per subscription/queue; bytes left to QUIC | AMQP 1.0's absolute form [amqp10 §5.1]; JetStream `max_ack_pending` per consumer [nats §5] | a broker cannot choose the next message by size; a per-queue credit is L2 state that must survive what L2 defines as a session |
| D — C plus a byte dimension in the L2 credit | credit frame carries messages and bytes | AMQP 1.0's two levels [amqp10 §5.2] | the sizing problem the sheet records, for a capability (size-aware selection) no use case has asked for |

## 4. Decision

1. **L0 carries no application credit.** QUIC's byte windows are the byte credit and the
   concurrent-stream budget is the message credit, both receiver-granted through transport
   parameters and both absolute and idempotent [quic-standards §2.8], [quic-standards §3.1].
   Nothing is added to the L0 wire for flow control. The deadlock shape that MOQT and Cap'n
   Proto document [prior-art §8 lesson 8] is answered structurally by decision 0002: control
   traffic has its own connection, so no data-window exhaustion can withhold a control frame
   [0002 §6.3].
2. **L2 has an explicit message credit per subscription.** The broker hop of Phase 6 needs
   "distribute by capacity, not by turn" (P3), which every brokered protocol answers with a
   per-consumer message credit [SYNTHESIS §1 P3]. weida's is granted by the consumer per
   subscription (or queue, in L2's vocabulary) on the control connection, independent of which
   bulk connection carries the deliveries, so that credit can be withdrawn without closing
   streams and can be re-stated after a connection change [0002 §6.3].

   **Amended by [0011](0011-answered-where-it-arrived.md) §4.3:** the credit frame names a
   subscription, and a subscription names a path, so it rides **that path's connection** rather
   than a control connection. The reason given above — that credit must be withdrawable
   independent of which connection carries the deliveries — is answered better that way, since
   the connection carrying the deliveries is the one whose credit is being withdrawn.
3. **The credit is an absolute delivery limit, not a delta and not an ack-driven window.** The
   frame carries the highest delivery number the sender may reach; the receiver maintains a
   delivery count, and a lost or duplicated credit frame changes nothing — AMQP 1.0's
   `delivery-count + link-credit` construction, which SYNTHESIS §8.1 named as the choice to
   make once [amqp10 §5.1]. The delivery number is the per-producer sequence of decision 0001,
   scoped to the subscription [0001 §7.1].
4. **Bytes stay with the transport.** The L2 credit counts messages only; the size of each
   delivery and of the whole connection is bounded by the consumer's QUIC windows on the bulk
   connection [PATTERNS §1.3]. Size-aware selection by the broker is not a goal; if one
   appears, it is a new decision, not a second dimension added quietly.

## 5. Consequences and follow-ups

- **[GUARANTEES.md](../GUARANTEES.md) §3 and §6.** The backpressure row states the two credit
  units explicitly — bytes (windows) and streams (budgets) — and that no application credit
  exists at L0; the L2 credit is recorded under the reserved broker vocabulary beside
  `Accepted`/`Stored`/`Processed`, with no v0 wire representation.
- **[PROTOCOL.md](../PROTOCOL.md) §11 (open questions).** Reserve the credit frame kind for
  the control connection with the absolute-limit form of §4.3; its fields are fixed with the
  Phase 6 broker design, not before.
- **[docs/PATTERNS.md](../PATTERNS.md) §1.4.** Add the statement that the stream budget *is*
  weida's message credit at L0 and that a consumer sizes its prefetch at L0 by granting
  `max_concurrent_uni_streams` on its bulk connection profile [0002 §6.4].
- **[SYNTHESIS.md](../research/SYNTHESIS.md) §8.1** is closed by this note.
- **Adapters.** MQTT Receive Maximum, RabbitMQ `prefetch_count`, AMQP 1.0 link credit and
  JetStream `max_ack_pending` all map onto the L2 credit of §4.2 in the same unit; Kafka's
  byte-only fetch maps onto the bulk connection's windows and has no message credit to carry
  [SYNTHESIS §7.3]. No adapter translates between units.

## 6. Sources

weida documents: [GUARANTEES.md](../GUARANTEES.md) §3, §6; [PATTERNS.md](../PATTERNS.md)
§1.3, §1.4; [0001](0001-sequence-field.md) §7.1, §7.8; [0002](0002-control-and-bulk-separation.md)
§6.3, §6.4.

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §1 (P3, P4, P12), §2 (D1, D2),
§7.3, §8.1; [quic-standards.md](../research/quic-standards.md) §2.8, §3.1;
[prior-art.md](../research/prior-art.md) §4 (flow control), §8 lesson 8;
[amqp10.md](../research/amqp10.md) §5.1, §5.2, §5.3; [rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md)
§5; [mqtt5.md](../research/mqtt5.md) §5; [nats.md](../research/nats.md) §5;
[kafka.md](../research/kafka.md) §5; [zeromq.md](../research/zeromq.md) §5;
[nanomsg-nng.md](../research/nanomsg-nng.md) §5.
