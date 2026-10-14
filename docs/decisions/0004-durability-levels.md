# 0004 — Durability levels for `Stored` and `Replicated`

- **Status:** accepted
- **Date:** 2026-09-11
- **Related:** SYNTHESIS §8.3; P7, P9; decisions 0001 §7.1, 0003 §4.2

## 1. The question

Four completion states are reserved for the L2 broker hop with precise wording and no wire
representation: `Accepted` — "the next hop has accepted responsibility in memory according to
the selected policy"; `Stored` — "the next hop has persisted sufficient state to survive the
documented failure domain"; `Replicated(n)` — "the next hop guarantees that configured
replication criteria have been met"; `Processed`
([GUARANTEES.md](../GUARANTEES.md) §1). Two of those four do not yet say enough to be
implementable.

`Stored` names a failure domain it never fixes, and `Replicated(n)` names criteria it never
states. SYNTHESIS §8.3 puts it exactly: the sheets show three distinct certification points in
real use — "after a disk write (RabbitMQ classic), after a majority wrote *and flushed*
(RabbitMQ quorum), after quorum replication without an explicit `fsync` (RabbitMQ streams)"
[rabbitmq-amqp091 §6d] — plus Kafka's warning that `acks` "does not make a broker fsync each
record before acknowledgement" [kafka §6]. "The decision is which of these `Stored` means, and
whether `Replicated(n)` counts acceptance or durable flush" [SYNTHESIS §8.3].

The question is load-bearing before any adapter exists, not after: an adapter that carries a
RabbitMQ publisher confirm or a Kafka produce response into weida's vocabulary has to pick one
of the three points, and "Protocol adapters may not silently invent guarantees their source
protocol cannot provide" [INVARIANTS].

## 2. The evidence, condensed

**One broker, three different certificates, all called an ack.** RabbitMQ's confirm-mode
`basic.ack` generally certifies that "the message has been accepted by all the queues" it
routed to; for a persistent message in a durable classic queue it certifies acceptance *and*
persistence to disk, with the classic store persisting in batches "after a few hundred
milliseconds" or when the queue goes idle, so persistent-confirm latency under constant load
can reach "hundreds of milliseconds"; for a quorum queue it certifies that a **majority wrote
and flushed to disk**; for a stream it certifies replication to a quorum, but streams do not
explicitly `fsync` and rely on the OS page cache, so an uncontrolled shutdown can lose
confirmed data [rabbitmq-amqp091 §6d], [rabbitmq-amqp091 §12/P7]. The same verb — acknowledged
— spans a page-cache write and a flushed majority.

**Kafka separates replication from flushing on purpose.** `acks=1` certifies leader append
only, and an acknowledged record is lost if the leader fails before followers catch up;
`acks=all` waits for the current in-sync replicas and is subject to `min.insync.replicas`, and
is rejected with an insufficient-replica error when the ISR is smaller than that, rather than
being acknowledged more weakly. `acks` does not imply an `fsync` per record; durability rests
on the replica-failure model and the page cache [kafka §6], [kafka §11]. With
`unclean.leader.election.enable` on and no in-sync replica left, an out-of-sync replica may
take leadership and discard committed records — availability bought with loss [kafka §8],
[kafka §9].

**JetStream certifies a quorum commit, not a flush.** A JetStream publish acknowledgement
reports stream name, stream sequence and duplicate status, and with replication it follows the
stream leader's quorum commit rather than mere reception by the connected server. Storage is
configured per stream as **memory or file**, so restart behaviour depends on that choice, and
below quorum the RAFT group cannot commit at all [nats §6], [nats §11], [nats §8]. The sheet
makes no `fsync` claim anywhere; core NATS has no transfer point to certify — `+OK` acknowledges
only a well-formed protocol operation [nats §6].

**AMQP 1.0 keeps durability out of the outcome entirely.** `disposition(state=accepted)` from a
receiver certifies that the receiver processed the message and "says nothing about disk".
Durability is a separate, per-message assertion: `header.durable=true` demands the message "MUST
NOT be lost even if an intermediary is unexpectedly terminated and restarted", and a target that
cannot honour it MUST NOT accept it — it rejects or detaches with `amqp:precondition-failed`.
Terminus durability (`none` / `configuration` / `unsettled-state`, default `none`) is independent
of the message header again [amqp10 §6.4], [amqp10 §6.5]. So the outcome vocabulary answers
"processed", the header answers "survives a restart", and the refusal is the enforcement.

**The synthesis already sorts these into classes.** D3 identifies hop-scoped ownership transfer,
`Stored` (RabbitMQ persistent message in a durable classic queue), `Replicated` (RabbitMQ quorum
majority wrote-and-flushed, JetStream quorum commit, Kafka `acks=all` at the ISR boundary) and
`Processed` (an application reply, JetStream `+ACK`) as four distinct certification classes
[SYNTHESIS §2/D3], [SYNTHESIS §4].

**What weida's own rule already forbids.** An implementation MUST NOT report one of the four
states unless its exact condition holds; `Stored` specifically MUST NOT be reported for an
in-memory buffer [GUARANTEES §1]. And a requested guarantee MUST NEVER be silently weakened:
invalid combinations are rejected explicitly at configuration time, not at runtime
[GUARANTEES §4].

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| A — leave `Stored` as one level | one word for all three certification points | status quo [GUARANTEES §1] | the three points collapse into one claim; an adapter carrying a stream confirm and one carrying a quorum confirm report the same thing, which is the invention the adapter invariant forbids [INVARIANTS] |
| B — `Stored(Written \| Flushed)` and `Replicated(n, flushed: bool)` | persistence strength and replica count as two orthogonal axes, each naming its failure domain | RabbitMQ's three points [rabbitmq-amqp091 §6d]; Kafka's `acks` × `min.insync.replicas` split [kafka §6]; JetStream storage × replicas [nats §11] | two axes to validate instead of one word; `n` must be defined as achieved rather than configured |
| C — a single ordered durability scale (0..3) | one integer from "memory" to "flushed majority" | none in the sheets | a number names no failure domain, and it forces an order between "flushed on one node" and "in page cache on three", which the evidence does not support |
| D — `Stored` means flushed, strictly | only an `fsync`-backed write may be called stored | the strict reading of "persisted sufficient state" [GUARANTEES §1] | Kafka `acks=all`, RabbitMQ streams and JetStream could then never report `Stored` and would fall back to `Accepted` — "accepted responsibility in memory" — which understates a quorum commit as badly as A overstates a page-cache write |

## 4. Decision

Option B.

1. **`Stored` gains a durability level naming the failure domain it survives.**

   - `Stored(Written)` — the broker's storage layer holds the message and it survives the
     **broker process** dying (crash, restart, orderly stop). It may be lost to OS or power
     failure, because it may still be in the page cache. This is the level a system certifies
     when it acknowledges after a disk write without an `fsync`: Kafka's non-flushing append
     [kafka §6], [kafka §11] and RabbitMQ streams' page-cache reliance [rabbitmq-amqp091 §6d].
   - `Stored(Flushed)` — the write has been flushed to durable media and returned, so the
     message survives **loss of power on that node**. This is RabbitMQ's quorum-queue
     certificate, minus the replication [rabbitmq-amqp091 §6d].

   `Stored` without a level is not a reportable state. `Stored(Written)` remains subject to the
   existing prohibition: an in-memory buffer is `Accepted`, never `Stored`, however the broker
   spells its buffer [GUARANTEES §1].

2. **`Replicated(n, flushed: bool)` counts replicas that reached a stated persistence level,
   and says which.** `n` is the number of replicas that hold the message, **the leader
   included** — the counting Kafka's `min.insync.replicas` uses [kafka §6] and the one a
   RabbitMQ or RAFT majority uses [rabbitmq-amqp091 §6d], [nats §11]. Every counted replica has
   reached at least `Stored(Written)`; `flushed: true` additionally certifies that every counted
   replica reached `Stored(Flushed)`. `Replicated(1, …)` is not reportable: a single replica is
   `Stored`, and calling it replicated invents a redundancy that does not exist.

   This makes the concrete certificates expressible without translation:
   RabbitMQ quorum queue → `Replicated(majority, flushed: true)`; RabbitMQ stream →
   `Replicated(quorum, flushed: false)`; Kafka `acks=all` → `Replicated(isr_at_commit,
   flushed: false)`; JetStream file storage with quorum commit → `Replicated(n, flushed: false)`
   [rabbitmq-amqp091 §6d], [kafka §6], [nats §6].

3. **`n` is achieved, not requested.** The acknowledgement carries the number of replicas that
   actually met the level at the moment of the commit, not the configured replication factor.
   Kafka is the precedent that matters here: a successful `acks=all` requires enough in-sync
   replicas *at the commit boundary*, and a shrunken ISR is an error rather than a quieter
   success [kafka §6], [kafka §11]. A broker that cannot reach the configured level fails the
   operation visibly; it never acknowledges a smaller `n` in place of the requested one, because
   that is the silent weakening §4 of GUARANTEES forbids.

4. **The two axes are independent, and the state set is a partial order, not a ladder.**
   `Stored(Flushed)` on one node and `Replicated(3, flushed: false)` are not comparable: one
   survives power loss on a single machine, the other survives losing two machines that were
   all running. Validation therefore compares a request to an offer **per axis** — persistence
   level and replica count — in the shape §3 of GUARANTEES already requires for guarantee
   dimensions, and rejects a request no offer covers on both axes at configuration time
   [GUARANTEES §3], [GUARANTEES §4]. Only the pairs within one axis are ordered:
   `Accepted` < `Stored(Written)` < `Stored(Flushed)`, and, at equal `n`,
   `flushed: false` < `flushed: true`.

5. **Durability is asserted by the producer and enforced by refusal, following AMQP 1.0.** A
   message that requires `Stored(Flushed)` and reaches a destination that cannot flush is
   refused at configuration time where the destination is known then, and refused per message
   otherwise — AMQP 1.0's `header.durable` rule, whose target "MUST NOT accept" what it cannot
   honour and answers `amqp:precondition-failed` [amqp10 §6.5]. Degrading the message to a
   weaker level is not an available behaviour.

6. **Nothing of this reaches the v0 wire.** The levels sharpen reserved vocabulary; they remain
   without wire representation until the broker resource subsystem exists, exactly as
   `Accepted` and `Processed` do [GUARANTEES §1]. What changes today is the contract a replicated
   Queue must satisfy before reporting a level.

## 5. Consequences and follow-ups

- **[GUARANTEES.md](../GUARANTEES.md) §1.** The `Stored` and `Replicated(n)` headings carry the
  levels of §4.1 and §4.2 with their failure domains, the "leader included" counting rule, and
  the statement that `Stored` without a level and `Replicated(1, …)` are not reportable states.
- **[GUARANTEES.md](../GUARANTEES.md) §3.** The acknowledgement/completion ladder lists
  `Stored(Written|Flushed)` and `Replicated(n, flushed)` instead of `Stored` and
  `Replicated(...)`, with the note that the two axes are a partial order per §4.4.
- **[GUARANTEES.md](../GUARANTEES.md) §4.** The validation rule gains the per-axis comparison:
  a request is honoured only when an offer covers it on the persistence axis *and* the replica
  count; `n` in an acknowledgement is achieved, not configured.
- **[PROTOCOL.md](../PROTOCOL.md) §11.** Beside the reserved L2 credit frame of 0003 §4.3, note
  that the L2 acknowledgement carries the achieved level, not the requested one; its encoding is
  fixed with the Phase 6 broker design, not before.
- **Foreign-protocol states are not weida states by analogy.** A managed Connector that converts
  between a Queue and a foreign system must name the evidence and loss in its own spec. There is
  no protocol-wide guarantee table. For example, a RabbitMQ classic persistent confirm says that
  a disk write occurred but does not certify `fsync` [rabbitmq-amqp091 §6d], while a JetStream
  memory-backed stream certifies no disk state [nats §11]. Those are inputs to a concrete
  Connector policy, not global mappings.
- **Kafka's unclean leader election is a named Connector loss, not a level.** With it enabled, a
  committed record can be discarded by an out-of-sync replica taking leadership [kafka §8],
  [kafka §9]. A Connector configured for that system must expose the loss rather than inventing a
  fifth weaker `Replicated` variant.
- **[SYNTHESIS.md](../research/SYNTHESIS.md) §8.3** is closed by this note.
- **`Processed` and `Accepted` are untouched.** A foreign outcome such as AMQP 1.0 `accepted`
  does not automatically become either weida state; a concrete Connector must state what it can
  prove at its configured boundary [amqp10 §6.5].

## 6. Sources

weida documents: [GUARANTEES.md](../GUARANTEES.md) §1, §3, §4; [INVARIANTS.md](../INVARIANTS.md)
(adapter honesty); [0001](0001-sequence-field.md) §7.1; [0003](0003-credit-unit.md) §4.2, §4.3.

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §2 (D3), §4, §8.3;
[rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §6, §6d, §12/P7;
[kafka.md](../research/kafka.md) §6, §8, §9, §11; [nats.md](../research/nats.md) §2, §6, §8,
§11, §12/P7; [amqp10.md](../research/amqp10.md) §6.1, §6.4, §6.5.
