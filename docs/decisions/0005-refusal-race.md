# 0005 — The refusal race — closed as documented behaviour

- **Status:** accepted
- **Date:** 2026-09-11
- **Related:** SYNTHESIS §8.5; P1, P16; decisions 0001 §7.2, 0002 §6.5, 0003 §4.1

## 1. The question

A peer refuses a one-way transfer with `STOP_SENDING` and a code (`UNKNOWN_ENDPOINT`,
`UNSUPPORTED`, `REJECTED`). "That is an application act, and it races the transport
acknowledgement: a payload that fits in flight can be acknowledged by the peer's transport
before its application refuses it, and `delivered()` then resolves `Ok` — truthfully, since a
receipt says nothing about the application, including that it said no"
([PATTERNS.md](../PATTERNS.md) §1.6).

SYNTHESIS §8.5 states the open decision: "whether to leave the race documented (the current
position) or to make refusal deterministic for small payloads, which requires an
application-level signal the v0 wire deliberately lacks [GUARANTEES §3]" [SYNTHESIS §8.5]. It
also states why every foreign protocol is free of the race: each of them has an application
acknowledgement, "because the acknowledgement *is* the application's" [SYNTHESIS §8.5].

This note is decided directly from the catalogue evidence; §2 condenses it, §3 records the
options, §4 the decision.

## 2. The evidence, condensed

**QUIC cannot tell a sender what the peer's application did, by construction.** The sheet's
list of what QUIC does not give a messaging system has it twice. Item 8: "No way to learn the
peer's application read progress" — §2.4's list of stream operations has no such query and
"RFC 9000 §3.2 states the sender cannot observe it"; arriving MAX_STREAM_DATA credit "is the
only proxy, and it is only a proxy", since a receiver is free to advertise its flow-control
offset on any basis it likes (RFC 9000 §4.1) [quic-standards §12 item 8]. Item 1: a QUIC ACK
certifies that packet protection was removed and the frames were processed *by the transport*
(RFC 9000 §13.1), while the receiving side's terminal state "Data Read" — the state that means
the application consumed the bytes — is one "the sender cannot observe" (RFC 9000 §3.1, §3.2)
[quic-standards §12 item 1]. No wire mechanism can therefore order an application refusal
before a transport acknowledgement; only the application layer can.

**`STOP_SENDING` is a request, and the standard explicitly denies it the property the race
would need.** It "requests that the receiving endpoint send a RESET_STREAM frame"; it
"typically indicates that the receiving application is no longer reading data it receives from
the stream, but it is not a guarantee that incoming data will be ignored", and "STREAM frames
received after sending a STOP_SENDING frame are still counted toward connection and stream
flow control" (RFC 9000 §3.5) [quic-standards §2.6]. Delayed delivery means "a receiver could
receive any of these three frames in any state" (RFC 9000 §3.3) [quic-standards §2.6]. A
refusal is thus intrinsically late relative to the bytes it refuses.

**The race is already specified — in three weida documents — together with the exact condition
under which the refusal *is* deterministic.** "The refusal is guaranteed to be observed only
when the transfer cannot complete without the application acting, which is any payload beyond
the stream receive window. Req/Rep has no such race: its refusal is an ERROR frame written by
the application on the reply half" [PATTERNS §1.6]. GUARANTEES states the same for the receipt
side and calls it definitional: `delivered()` "may resolve `Ok(())` for a transfer the
application then discarded. That is not a defect of the receipt but its definition"
([GUARANTEES.md](../GUARANTEES.md) §3). [FAILURE_MODEL.md](../FAILURE_MODEL.md) §4 repeats it
normatively under the receiver-side rules and names the mechanism the tests use: three tests
push 2 MiB rather than a few bytes, because "at that size the write cannot finish unless the
peer acts, so the refusal is deterministic rather than racy"
(`push_to_rep_path_is_unsupported`, `push_to_an_unknown_path_is_reported` in
`crates/weida/tests/pushpull.rs`; `a_publisher_path_refuses_inbound_transfers` in
`crates/weida/tests/pubsub.rs`) [FAILURE_MODEL §4], [PATTERNS §1.6].

**When the refusal is observed it is already definite and typed.** `STOP_SENDING` maps by
application error code to `Error::Rejected`, `Error::Canceled`, `Error::UnknownEndpoint` and
`Error::Unsupported`, all inside `is_definite_failure()`; "the refusal surfaces from
`write_all` when it lands mid-payload and from `delivered()` otherwise; a sender must be
prepared to see it at either point" [FAILURE_MODEL §4]. The codes themselves are specified
([PROTOCOL.md](../PROTOCOL.md) §7) and dispatch is specified to refuse rather than to close
the connection [PROTOCOL §9.4]. In an exchange the refusal wins outright: "An ERROR frame on
the reply half wins over the request half's transport receipt" [FAILURE_MODEL §4 Precedence].

**Making refusal deterministic means reintroducing the application ACK that was deliberately
removed.** The v0 vocabulary has exactly one delivery signal, `TransportReceipt`;
`Accepted`, `Stored`, `Replicated` and `Processed` are "reserved for the L2 broker layer and
are deliberately absent from the v0 wire" [GUARANTEES §3], [PROTOCOL §11]. The reason is
recorded: "Earlier drafts carried application ACK frames in the core and they were removed:
without a broker in the topology, an application ACK means 'arrived in RAM at the other end',
which is precisely what QUIC already guarantees by retransmitting until the peer
acknowledges." [GUARANTEES §3]. SYNTHESIS §4 lines weida up against the others on exactly this
axis: weida v0 has "One: sender to the peer's **transport**. There is no application-level
transfer point in v0" [SYNTHESIS §4].

**The foreign acknowledgements that avoid the race pay for it, and the sheets say what with.**
MQTT's PUBACK certifies "having accepted ownership" [SYNTHESIS §4], [mqtt5 §6], but the
"delivered to nobody" case is an *optional success* code — the server "MAY use this Reason Code
instead of 0x00 (Success)" for 0x10 No matching subscribers, "the only in-protocol 'delivered
to nobody' signal" [mqtt5 §4.1], [mqtt5 §12/P16] — and a PUBACK code ≥ 0x80 means the PUBLISH
"counts as acknowledged and MUST NOT be retransmitted ([MQTT-4.4.0-2])… the message is dead
with no protocol recourse" [mqtt5 §6]. AMQP 1.0 buys determinism with settlement in up to four
steps and a `rejected` outcome [amqp10 §6.4], [SYNTHESIS §4]; RabbitMQ with `basic.nack`, by
which "the broker indicates that it was unable to process the messages and refuses
responsibility for them" — while a message already queued can still be dropped by TTL, a length
limit, deletion or purge, "and none of this is signalled to the publisher, whose ack was sent
long before" [rabbitmq-amqp091 §6]. A per-message application acknowledgement is therefore not
a free upgrade in honesty: it moves the ambiguity, it does not remove it.

## 3. Options considered

| Option | Shape | What it costs | Precedent | Named loss |
| --- | --- | --- | --- | --- |
| A — leave the race documented, sharpen the statement | no wire change; PATTERNS §1.6, GUARANTEES §3 and FAILURE_MODEL §4 keep the rule and state the deterministic condition normatively | zero | QUIC itself: "Data Read" is unobservable to the sender [quic-standards §12 items 1, 8] | a small one-way transfer can be refused after its receipt resolved `Ok`, and the sender is never told |
| B — application ACK frame on the L0 wire, receipt withheld until dispatch decided | a new frame kind and a per-transfer wait on the receiving application | reintroduces exactly what was removed as meaningless without a broker hop [GUARANTEES §3]; adds a round trip to every one-way transfer and a pending-transfer table where "the stream *is* the transfer" [FAILURE_MODEL §4] | MQTT PUBACK [mqtt5 §6]; AMQP 1.0 `disposition` [amqp10 §6.4] | Push/Pull stops being fire-and-forget; the ACK still means "arrived in RAM" at an L0 hop [GUARANTEES §3] |
| C — pre-flight dispatch: a header-only bidi round trip before the payload | every one-way send becomes an exchange | one RTT per transfer; the one-way pattern disappears into Req/Rep, which already has no race [PATTERNS §1.6] | Req/Rep as it stands [PROTOCOL §9.1] | Push/Pull and Pub/Sub lose their reason to exist as separate patterns |
| D — make the deterministic condition universal by shrinking the receive window | receiver advertises a `stream_receive_window` smaller than any payload, so no transfer can complete without the application acting | throughput: flow control binds below the bandwidth-delay product [quic-standards §3.2]; the sender cannot control the peer's window anyway, so it is not a guarantee the sender can rely on | none in the sheets | trades every transfer's throughput for a refusal signal nobody asked for |
| E — defer determinism to L2 | the refusal that is guaranteed to be observed is the broker's `Accepted`, when a hop exists that can own the message | nothing now; the L2 broker design owes it [GUARANTEES §3], [PROTOCOL §11] | AMQP 1.0's settlement as the explicit transfer point [SYNTHESIS §4] | brokerless deployments — every deployment v0 has — keep the race |

## 4. Decision

1. **The race stays, as documented behaviour.** Option A is taken, with Option E for the
   future: v0 adds no application-level signal to the L0 wire, and the determinism the
   question asks for arrives, if ever, as the reserved `Accepted` state at a broker hop that
   can own the message [GUARANTEES §3], [PROTOCOL §11]. Options B, C and D are closed, B and C
   because they make Push/Pull an exchange for a signal that would still only mean "arrived in
   RAM" [GUARANTEES §3], D because a receiver-side window is not a property a sender can rely
   on [quic-standards §3.2].
2. **`delivered()` keeps its definition and is not weakened.** A transport receipt asserts that
   the peer's transport holds every byte and the FIN, and nothing about the peer's application
   — including that it said no [GUARANTEES §3], [PATTERNS §1.6]. An `Ok(())` for a transfer
   the peer discarded is a true statement about what was asserted, and no API distinguishes the
   two cases [GUARANTEES §3].
3. **The condition under which a refusal *is* guaranteed becomes normative rather than
   incidental.** Two constructions give a deterministic refusal, and the specification must
   state both as the supported answer: a payload larger than the peer's stream receive window,
   where flow control forces the application to act before the write can finish
   [PATTERNS §1.6], [quic-standards §3.1]; and an exchange, whose ERROR frame is written by the
   receiving application and takes precedence over the request half's receipt
   [FAILURE_MODEL §4 Precedence], [PROTOCOL §9.4]. An application that must observe a refusal
   uses Req/Rep. This is guidance the pattern-choice table owes its reader, not a new
   mechanism.
4. **A refusal that loses the race is not reported anywhere, and that is accepted, not
   hidden.** Once `delivered()` has resolved, a later `STOP_SENDING` has no observer: the
   `Delivery` is consumed and dropping it "is free and observes no outcome at all"
   [GUARANTEES §3]. No counter, no metric and no late error is added for it; inventing one
   would claim an observability weida's own vocabulary does not have [PATTERNS §1.6]. What the
   documents owe is the plain statement that the refusal is lost to the sender in that window.
5. **No adapter may present a foreign application acknowledgement as backed by weida's
   receipt.** A bridge that answers PUBACK, `basic.ack` or an AMQP 1.0 `accepted` on the
   strength of a weida transport receipt claims ownership transfer the receipt does not carry
   — the acknowledgement in each of those protocols is the *application's*
   [SYNTHESIS §8.5], [SYNTHESIS §4], [mqtt5 §6], [rabbitmq-amqp091 §6], [amqp10 §6.4] — and
   that is precisely what "protocol adapters may not silently invent guarantees their source
   protocol cannot provide" forbids ([INVARIANTS.md](../INVARIANTS.md)). Such a bridge must own
   the acknowledgement itself or refuse the configuration, which is the shape SYNTHESIS §7.2
   already records for the MQTT chain and which [0006](0006-guarantee-sets.md) §4.6-§4.7
   makes the general rule: the chain ends at the foreign protocol's transfer point, and
   refusal at configuration time is the default.

## 5. Consequences and follow-ups

- **[PATTERNS.md](../PATTERNS.md) §1.6.** Keep the section and add decision 4.3's two
  deterministic constructions as an explicit "how to get a guaranteed refusal" statement, and
  decision 4.4's sentence that a refusal arriving after the receipt has resolved reaches no
  observer. §7 (Choosing) gains a row: *you must know whether the receiver accepted it* → use
  Req/Rep, because a one-way refusal can lose the race.
- **[GUARANTEES.md](../GUARANTEES.md) §3.** The paragraph that already states the asymmetry
  gains the closing sentence that this is a decided position, with the note number, rather than
  an open question — and the statement that the deterministic counterpart is the reserved
  `Accepted` of the L2 broker, not a future L0 ACK.
- **[FAILURE_MODEL.md](../FAILURE_MODEL.md) §4.** "A refusal can lose the race with the
  transport" stays normative and gains the explicit consequence for the sender's outcome
  vocabulary: no outcome exists for a refusal observed after the receipt resolved, and none is
  added.
- **[PROTOCOL.md](../PROTOCOL.md) §9.2 and §11.** §9.2 states that the transport receipt is
  the only delivery signal; add that a refusal is not ordered against it and that no frame
  will be added at wire version 0 to order it. §11 keeps the application acknowledgement in the
  "not specified in v0" list and now names decision 0005 as the reason.
- **Managed Connectors.** A Connector specification must state that a weida transport receipt
  does not certify the foreign protocol's ownership transfer, name the concrete transfer point
  it relies on — for example MQTT PUBACK/PUBREC [mqtt5 §6], RabbitMQ
  `basic.ack`/`basic.nack` [rabbitmq-amqp091 §6], or AMQP 1.0
  `disposition`/`rejected` [amqp10 §6.4] — and refuse a conversion whose claim it cannot prove
  [INVARIANTS], [SYNTHESIS §7.2]. This is per resource, not a protocol-wide mapping.
- **Code.** No change is required in `crates/weida` or `crates/weida-core`: the behaviour is
  what the implementation already does, and the three 2 MiB tests named in §2 are the
  regression guard for the deterministic half of the rule. They MUST keep their payload sizes;
  shrinking any of them would make the test racy rather than make the code wrong, and the doc
  comment on each should say so.
- **SYNTHESIS §8.5** is closed by this note.

## 6. Sources

weida documents: [PATTERNS.md](../PATTERNS.md) §1.2, §1.6, §7; [GUARANTEES.md](../GUARANTEES.md)
§3, §6; [FAILURE_MODEL.md](../FAILURE_MODEL.md) §4 (sender outcome rules, receiver-side rules,
precedence); [PROTOCOL.md](../PROTOCOL.md) §7, §9.1, §9.2, §9.4, §11;
[INVARIANTS.md](../INVARIANTS.md); [0001](0001-sequence-field.md) §7.2;
[0002](0002-control-and-bulk-separation.md) §6.5; [0003](0003-credit-unit.md) §4.1;
[0006](0006-guarantee-sets.md) §4.6, §4.7.

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §1 (P1, P16), §4, §7.2, §8.5;
[quic-standards.md](../research/quic-standards.md) §2.4, §2.6, §3.1, §3.2, §12 items 1 and 8;
[mqtt5.md](../research/mqtt5.md) §4.1, §6, §12/P16; [amqp10.md](../research/amqp10.md) §6.1,
§6.4; [rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §6.
