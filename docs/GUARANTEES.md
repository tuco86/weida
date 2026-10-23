# Guarantees

This document defines the guarantee vocabulary. It is normative for what a weida
acknowledgement is permitted to mean. It derives from master doc §18-22.

Related: [PROTOCOL.md](PROTOCOL.md), [FAILURE_MODEL.md](FAILURE_MODEL.md),
[INVARIANTS.md](INVARIANTS.md), [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. Reliability as transfer of responsibility

Reliability is formulated around **responsibility transfer**, not around the word
"reliable".

An ACK means:

> The next hop has reached a precisely defined state at which the previous hop is allowed to
> release some or all responsibility.

There is deliberately no vague universal `Committed` state. Each completion state has exact
semantics:

### Accepted

The next hop has accepted responsibility in memory according to the selected policy.

### Stored

The next hop has persisted sufficient state to survive the documented failure domain. The
domain is named by a level, and `Stored` without a level is not a reportable state
([decisions/0004](decisions/0004-durability-levels.md) §4.1):

- **`Stored(Written)`** — the storage layer holds the message and it survives the **broker
  process** dying: crash, restart or orderly stop. It may still be lost to an operating-system
  or power failure, because it may still sit in the page cache. This is what a system
  certifies when it acknowledges after a disk write without an `fsync`.
- **`Stored(Flushed)`** — the write has been flushed to durable media and returned, so the
  message survives **loss of power on that node**.

### Replicated(n, flushed)

The next hop guarantees that configured replication criteria have been met, stated as a count
and a persistence level ([decisions/0004](decisions/0004-durability-levels.md) §4.2):

- `n` is the number of replicas that hold the message, **the leader included**, and it is the
  number *achieved* at the commit, never the configured replication factor (§4).
- Every counted replica has reached at least `Stored(Written)`; `flushed: true` additionally
  certifies that every counted replica reached `Stored(Flushed)`.
- `Replicated(1, …)` is not a reportable state: one replica is `Stored`, and calling it
  replicated invents a redundancy that does not exist.

### Processed

The next hop's application-level consumer has explicitly reported successful processing.

An implementation MUST NOT report one of these states unless the exact condition above
holds. In particular, `Accepted` MUST NOT be reported before the payload has actually been
handed to the application, and `Stored` MUST NOT be reported for an in-memory buffer.

These four states describe a responsibility transfer **to a broker hop**, and they belong to
the **L2 broker layer**. `weida-broker` uses two of them, and the split is worth stating
exactly:

- **`Accepted`** is what `weida-broker` reports for a message it has admitted into a queue,
  on the reply half of the producer's own exchange, encoded as DATA key `8`
  ([PROTOCOL.md](PROTOCOL.md) §6.2,
  [decisions/0018](decisions/0018-minimal-broker.md) §4.6).
- **`Processed`** is the consumer's own statement about its own hop. The broker orders it on
  each delivery and the consumer reports it on a cursor stream, which settles the delivery:
  the queue drops the message, or requeues it when the report ends without it
  ([decisions/0029](decisions/0029-a-report-is-relayed-a-certificate-is-not.md) §4.6, §4.7).
  The producer's certificate stays `Accepted` however the consumer answers, because
  guarantees are hop-local (§2). The broker MAY re-emit the consumer's report toward the
  producer as [decisions/0029](decisions/0029-a-report-is-relayed-a-certificate-is-not.md)
  §4.1 and §4.3 define, and MUST NOT present it as its own guarantee. Re-emitting is not built
  yet; a producer that needs to know a consumer succeeded uses an application reply.
- **A queue with one replica can never report `Replicated`**, whatever it is configured with:
  `Replicated(1, …)` is not a reportable state (§4), and a single-replica queue has no consensus
  group to commit in ([decisions/0022](decisions/0022-consensus-topology.md) §4.3). It reports
  `Accepted`, or `Stored(*)` once a store is under it.
- **`Stored(*)`** and **`Replicated(n, flushed)`** are unreachable, and not by
  oversight: the broker holds every queue in memory, so reporting them would be the
  prohibited claim above rather than an optimistic one. They become reachable with a durable
  store and with replication. A configuration asking for a level a hop
  cannot reach is refused when it is configured, never silently degraded (§4).

**Two of these are cursors and two are verdicts**, which matters for a system whose payloads are
streams ([decisions/0023](decisions/0023-completion-is-a-cursor.md) §4.2). `Accepted` is a verdict
by construction: a hop either took the message or refused it. `Stored(*)` and
`Replicated(n, flushed)` are statements about **bytes**, reported as an absolute offset that
advances — "durable up to *N*" — which is what lets a payload larger than memory be certified at
all, and what lets an interrupted transfer continue at *N* instead of starting over. `Processed`
is a cursor too, and it is a **settlement toward the queue** — "you may forget up to *N*" — never
an end-to-end claim: a producer that needs to know a consumer succeeded uses an application reply
until the queue re-emits its consumer's report (§2,
[decisions/0029](decisions/0029-a-report-is-relayed-a-certificate-is-not.md) §4.3).

In the **v0 core**, with no broker in the topology, all four remain absent from the wire:
there is nobody to transfer responsibility *to*, and an acknowledgement that only says "the
bytes are in the peer's RAM" is a claim QUIC already makes for free (§3).

---

## 2. Guarantees are hop-local

This is a hard invariant.

> All delivery and completion guarantees refer to the immediate next hop.

Consider:

```text
Producer → Broker A → Broker B → Consumer
```

If Broker A returns `Stored`, this means Broker A has met the documented storage guarantee.
It does **not** inherently mean:

```text
Consumer eventually processed the message.
```

Long end-to-end guarantees arise only through explicit composition of hop-local guarantees.
An implementation MUST NOT make hidden claims about downstream systems, and MUST NOT
present a hop-local acknowledgement as an end-to-end result (master doc §81 rule 7).

In the brokerless core there is exactly one hop — a direct QUIC connection between requester and
responder — so hop-local and end-to-end coincide. This coincidence is an accident of the
brokerless topology and MUST NOT be relied on by API design or documentation wording.

**A guarantee is hop-local; a report may be carried.** The two are different objects and only
the first is bound by the rule above. A *guarantee* is what a peer promised at negotiation —
the `acknowledgement` dimension of the HELLO set — and it is always about the immediate next
hop. A *report* is a cursor: an observation somebody made, on a stream of its own, never
load-bearing
([0023](decisions/0023-completion-is-a-cursor.md) §4.5). A hop MAY re-emit a report it received
toward the peer that ordered it, and MUST NOT present such a report as **its own** guarantee.
A queue is the case this exists for: a producer orders `Processed`, the queue's consumer
reports it, and the queue re-emits that report while its own certificate to the producer stays
`Accepted`
([0029](decisions/0029-a-report-is-relayed-a-certificate-is-not.md) §4.1). Two prohibitions
keep it honest and are normative: a hop MUST NOT emit a level it did not receive for that
message, and MUST NOT re-emit a report that reached it as a re-emission — so a report travels
at most one hop past its observer, and no claim can drift away from what somebody saw.

---

## 3. Independent guarantee dimensions

Guarantees MUST NOT be collapsed into a single enum such as `Reliable`. The dimensions are
independent.

### Delivery

```text
BestEffort
AtMostOnce
AtLeastOnce
```

Exactly-once delivery MUST NOT be casually promised. Exactly-once **effects** may be
achievable with additional mechanisms such as durable idempotency/inbox/outbox cooperation;
that is a property of a composed system, not of a transport.

### Acknowledgement/completion

```text
None
TransportReceipt          (v0 core)
Accepted                  (L2 broker)
Stored(Written|Flushed)   (reserved, L2 broker)
Replicated(n, flushed)    (reserved, L2 broker)
Processed                 (L2 broker)
```

The completion states above `TransportReceipt` are **not a ladder**. Persistence level and
replica count are independent axes, so `Stored(Flushed)` on one node and
`Replicated(3, flushed: false)` are incomparable: one survives power loss on a single machine,
the other survives losing two machines that were all running. Only pairs within one axis are
ordered — `Accepted` < `Stored(Written)` < `Stored(Flushed)`, and, at equal `n`,
`flushed: false` < `flushed: true`
([decisions/0004](decisions/0004-durability-levels.md) §4.4).

The v0 stream core offers exactly one delivery signal, the **transport receipt**.
`OutgoingTransfer::finish` hands back a `Delivery`, and `Delivery::delivered().await`
resolves `Ok(())` when the peer's transport holds every byte of the payload and the FIN.
quinn documents the underlying condition as the local side finishing the stream and the peer
then acknowledging receipt of all stream data *"(although not necessarily the processing of
it)"*. That parenthesis is the whole distinction: a transport receipt says the bytes
arrived, never that an application read them, still less that it acted on them. It is not an
implementation limit but the transport's: the receiving side's terminal state "Data Read" —
the state that means the application consumed the bytes — is one the sender cannot observe
(RFC 9000 §3.1, §3.2), and QUIC provides no way at all to learn the peer's application read
progress, MAX_STREAM_DATA credit being only a proxy a receiver may advertise on any basis it
likes (RFC 9000 §4.1).

`Accepted`, `Stored`, `Replicated` and `Processed` belong to a hop that owns the message, and
the brokerless core produces none of them on its own. What they have is a carrier: they ride
a CURSOR stream as absolute byte offsets, ordered per transfer in DATA keys `9`-`11`
([PROTOCOL.md](PROTOCOL.md) §6.7, [decisions/0023](decisions/0023-completion-is-a-cursor.md)).
The distinction is worth keeping sharp: an application ACK **frame in the core** would mean
"arrived in RAM at the other end", which is precisely what QUIC already guarantees by
retransmitting until the peer acknowledges. It would buy RabbitMQ's vocabulary without
RabbitMQ's responsibility transfer — the one thing that makes the vocabulary worth having
(§1). The cursor stream does not reopen that: a cursor is
**reported by a hop that took responsibility**, never synthesized by the transport, nothing
waits on one, and a level a peer cannot reach is simply absent rather than invented. So the
words are on the wire while the rule that makes them meaningless in the core still holds
— which is why `weida-broker` reports `Accepted`, its consumers report `Processed`, and nothing
in this repository reports `Stored`.

The receipt is a correctness signal, not a latency-sensitive one. On an idle loopback
connection `delivered()` resolves in ~26 ms, because the peer delays its acknowledgement up
to QUIC's max ack delay; the same 1 KiB push without the receipt costs ~7.9 µs. This is why
`Pusher::send` finishes the transfer and drops the `Delivery` — zmq pipeline semantics —
while callers who want the receipt use `Pusher::open` plus `finish()` and `delivered()`
themselves. Dropping a `Delivery` is free and observes no outcome at all.

**`Runtime::drain` waits on exactly this signal**, and therefore inherits exactly this
meaning ([decisions/0009](decisions/0009-drain.md) §4.2). A drain counts a transfer as
delivered when the peer's transport acknowledged it — not when a peer application read it,
and not when one acted on it. A drained transfer may still be discarded by the peer's
application after the fact: the refusal race is documented behaviour and no receipt closes
it ([decisions/0005](decisions/0005-refusal-race.md) §4.2). What a drain adds is a bound
and a count; what it cannot add is a stronger signal, because L0 has none. Dropping a
`Delivery` remains free for the caller — the runtime parks the receipt so the drain has
something to wait on, and a receipt the caller awaits itself is never parked.

What the receipt implies about the peer's application depends on the payload size, and the
dependency is worth stating precisely. Inside the peer's stream receive window the receipt
resolves before the application has called `recv`, and even while the transfer is still
parked in the peer's accept queue. Beyond the window it cannot resolve until the application
has consumed at least `payload - window` bytes, because QUIC will not accept more than the
window without that. A receipt for a large transfer is therefore evidence of application
progress; a receipt for a small one is not, and no API distinguishes the two cases
([PATTERNS.md](PATTERNS.md) §1.2, `a_receipt_beyond_the_window_implies_the_reader_consumed`).

The same asymmetry cuts the other way for refusals: a one-way transfer that fits in flight may
be acknowledged by the peer's transport before the peer's application refuses it, so
`delivered()` may resolve `Ok(())` for a transfer the application then discarded. That is not
a defect of the receipt but its definition ([PATTERNS.md](PATTERNS.md) §1.6), and it is a
decided position rather than an open question:
[decisions/0005](decisions/0005-refusal-race.md) closes the race as documented behaviour. No
application-level signal is added to the L0 wire to order a refusal ahead of the receipt; a
refusal is guaranteed to be observed only where the payload exceeds the peer's stream receive
window or where the pattern is Req/Rep, whose ERROR frame the receiving application writes
itself [0005 §4.3]. A refusal that arrives after the receipt has resolved reaches no observer,
and none is invented for it [0005 §4.4]. The deterministic counterpart is the `Accepted` of
an L2 broker hop, not an L0 acknowledgement.

### Ordering

```text
None
PerProducer(detect|reassemble)
PerKey                          (L2 only)
Total
```

`PerProducer` is scoped to (producer, endpoint or topic), where the producer is by default the
sending peer's proved fingerprint and the counter therefore restarts with the connection
([decisions/0008](decisions/0008-session-identity.md) §4.3). It has two modes
([decisions/0001](decisions/0001-sequence-field.md) §7.5): **detect** reports the gap and
delivers messages as they arrive, and is the default; **reassemble** holds a message back
until its predecessors have arrived, accepting head-of-line blocking above a transport chosen
to avoid it. `PerKey` is unreachable at L0 and reserved for the L2 broker, because key order
exists only under a stable key-to-partition binding weida does not have [0001 §7.4].

**What reassemble does at its bound, normatively.** The hold is bounded by
`Limits::max_reorder_hold`, counted in transfers over all scopes of one connection. When an
arrival does not fit, the receiver **releases the oldest held transfer out of order and
reports the numbers it skipped as a gap** on that transfer; it never refuses the arrival and
never grows the hold. Degrading to detect-mode behaviour at the bound keeps the rule that a
requested guarantee is never *silently* weakened (§4): the application is told exactly what
it would have been told under detect. A hole that is never filled therefore costs
`max_reorder_hold` transfers of delay before it becomes visible, and a hole at the very end
of a producer's traffic holds its successors until the connection closes — v0 bounds the hold
in count only, not in time.

**A repeated number is delivered, not suppressed.** If a number that is already held arrives
a second time — a producer retransmit, or any repeat on a connection without deduplication —
the held transfer keeps its place in the run and the repeat is passed to the application
immediately, out of order. Reassembly never discards a transfer: dropping one would reset a
stream its sender believes is in flight, and suppressing a repeat is the `Deduplication`
dimension's job, which is independent of this one [0001 §7.1].

A held transfer is an **unread stream**, not a copy of its payload: reassembly materializes
nothing ([INVARIANTS.md](INVARIANTS.md)). What it pins is transport memory — one stream of
the peer's `max_concurrent_uni_streams` budget and up to `stream_receive_window` bytes each,
the whole hold bounded by `connection_receive_window` — which is where the peer is held
accountable for it ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.6 asks
for eager reads into an application-owned buffer instead; that would require materializing
the payload, which the core forbids, so the bytes stay in the transport and the cost is paid
as backpressure).

The performance implications of each level MUST be documented. Measured: the two DATA
keys that carry the sequence and the producer identity cost **81 B on a 76 B frame** — a
64-byte message more than doubles — and 12 % of the message rate at a 64-byte payload in their
worst case, while the default omits the producer key entirely and pays the sequence's 6 B
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.4 and §4.5;
[decisions/0008](decisions/0008-session-identity.md)
§4.4). Reassembly adds no per-message allocation beyond one map entry per held transfer; its
cost under cross-stream reordering in bytes is the pinned receive windows above.

### Deduplication

```text
None
Bounded(window)
Durable                         (L2 only)
```

`Bounded(window)` suppresses a repeated identity within a time window; an identifier reused
after its window is not suppressed, and the window length is configuration rather than a
constant of the protocol ([decisions/0001](decisions/0001-sequence-field.md) §7.6). Ordering
and deduplication are separate dimensions: neither implies the other, and one may be enabled
without the other [0001 §7.1]. `Durable` needs a store and belongs to the L2 broker.

### Backpressure behavior

```text
Block
Reject
Drop
Spill
Coalesce
```

These names are provisional; semantics matter more than naming. Backpressure is one of the two
dimensions that are **not** ordered — producer naming below is the other: its levels are
behaviours, not strengths, so two peers state the same one or fail to agree
([decisions/0006](decisions/0006-guarantee-sets.md) §4.3).

**`Coalesce` has a definition, and it is not v0's**
([decisions/0016](decisions/0016-conflation.md)): it is a **keyed queue at a hop** — depth one
per key, the arriving value replacing the queued one, the key being the DATA `topic` of
[PROTOCOL.md](PROTOCOL.md) §6.2 rather than a value a peer declares. Only a hop that *holds* a
queue can honour it, so it belongs to the L2 broker and a v0 peer declares it neither offered
nor required; §6's row omits it for exactly that reason. What v0 offers instead is not a weaker
conflation but a different shape: the producer keeps the single slot and publishes with
`Publisher::open` + `write_now`, so the discard is the transport's per subscriber and the
newest value is the one in flight ([PATTERNS.md](PATTERNS.md) §4.1). `Spill` is the same kind
of reservation — a queue that overflows to storage needs a durable store ([STORE.md](STORE.md)) — and neither name
may be declared until the hop that could honour it exists.

### Producer naming

```text
Fingerprint
Stable
```

Which name a sequenced transfer's producer carries. `Fingerprint` is the default and needs no
bytes on the wire at all: the receiver already knows the sending peer's proved fingerprint from
the handshake, so the producer key is absent and the counter restarts with the connection
([decisions/0008](decisions/0008-session-identity.md) §4.3, §4.4). `Stable` is the variant that
survives a reconnect, whose supplier is an L2 subscription rather than an L0 session
([decisions/0001](decisions/0001-sequence-field.md) §7.3), so a v0 peer cannot offer it.

Like backpressure, this dimension is **not** ordered: the two values are names, not strengths,
so two peers state the same one or fail to agree.

### Control isolation

```text
No
Yes
```

Whether control traffic can be stalled by bulk traffic on the way to this peer. `Yes` means the
control connection of [decisions/0002](decisions/0002-control-and-bulk-separation.md) exists, so
no amount of unread payload can withhold a control frame; `No` is one connection carrying both,
which is what v0 does. This dimension **is** ordered — isolation is strictly stronger than none
— so the intersection of two declarations is the logical AND: a peer gets isolation only if
both sides run it. It is declarable because it is requirable: a peer that multiplexes many
independent sessions onto one weida connection is the case SYNTHESIS §8.2 named, and it needs
to *require* isolation rather than hope for it.

### Guarantee sets

A **guarantee set** is the unit of configuration: a tuple with one level per dimension above —
delivery, acknowledgement/completion, ordering, deduplication, backpressure, producer naming
and control isolation — where an acknowledgement level of `Stored` or `Replicated` carries the
durability axes of §1. A set is never a single enum, and it introduces no new words: it is a way
to carry the existing ones as one object
([decisions/0006](decisions/0006-guarantee-sets.md) §4.1). The dimension list is closed in the
same sense as the invariant list — a wire key may not name a dimension this section does not,
and the two that only the HELLO declarations use, producer naming and control isolation, are
listed here rather than left implicit in the codec ([PROTOCOL.md](PROTOCOL.md) §6.5 keys `8` and `9`).

**`core` is the default set, and it is what v0 does**: delivery `BestEffort`, acknowledgement
`TransportReceipt`, ordering `None`, deduplication `None`, backpressure `Block` (with `Reject`
at an endpoint's queue bound and `Drop` for fan-out only), producer naming `Fingerprint`,
control isolation `No`. §6 is the normative source for `core`, so that the default cannot drift
away from what the code does. An endpoint configured with nothing gets `core`, and every
endpoint configured with no stronger set uses `core`. On the wire a dimension left at its
`core` level is **not written at all**, so a `core` declaration and no declaration are the
same bytes ([PROTOCOL.md](PROTOCOL.md) §6.5).

**Inside the weida network a configured set may only be a superset of `core`.** Set `B` is at
least `A` when, for every dimension, `B`'s level is greater than or equal to `A`'s in that
dimension's own order. Where a dimension is a partial order rather than a ladder — persistence
level against replica count, §1 — the comparison is per axis and incomparable levels do not
satisfy each other [0006 §4.3], [0004 §4.4].

**Standalone foreign protocols are outside this guarantee negotiation.** Their libraries expose
their own transfer points and completion states without translating them into weida levels. A
future managed Connector validates the guarantee level of its one configured source–Queue or
Queue–sink path and records that policy in the resource specification. That local declaration
does not define a protocol-wide mapping.

---

## 4. Configuration of guarantees

The framework must support the full performance/reliability spectrum. Illustrative
configurations — only the first is reachable in the v0 core, the other two describe the L2
broker layer:

**Very fast**

```text
persistence = none
ACK = none
ordering = none
dedup = none
backpressure = drop/coalesce
```

**Reliable job processing**

```text
outgoing responsibility retained
remote persistence
at-least-once
deduplication
consumer ACK
```

**Strong broker persistence**

```text
replication factor = 3
ACK after N persisted replicas
durable deduplication
consumer processed ACK
```

Four rules govern configuration:

- Invalid combinations MUST be rejected. Validation is explicit and happens at
  configuration time, not silently at runtime. A guarantee set is validated when an endpoint,
  connection or managed resource is configured, against what the build and configuration can
  honour, before traffic starts
  ([decisions/0006](decisions/0006-guarantee-sets.md) §4.5).
- A requested guarantee MUST NEVER be silently weakened (master doc §81 rule 6). If a peer
  or a build cannot honour a requested guarantee, the operation MUST fail visibly. The v0
  core has no acknowledgement knob to weaken — it offers the transport receipt or nothing —
  so the rule shows up in routing instead: a stream addressed to an endpoint whose pattern
  cannot serve it is refused with `UNSUPPORTED` rather than quietly treated as something
  the endpoint does understand (see [PROTOCOL.md](PROTOCOL.md) §9). A Connector follows the
  same rule at resource application: its explicit conversion policy either validates or the
  resource is refused.
- **Validation compares per axis, not per word.** A request for a completion state is
  honoured only when an offer covers it on the persistence axis *and* on the replica count;
  incomparable levels do not satisfy each other, and an acknowledgement reports the `n` that
  was achieved, never the one that was configured
  ([decisions/0004](decisions/0004-durability-levels.md) §4.3, §4.4).
- **Both sides declare, and the intersection decides.** Each side declares in HELLO the set
  it offers and the set it requires; negotiation computes the per-dimension intersection —
  the weaker level of the two offers — and fails the connection with `NEGOTIATION_FAILED`
  when the result does not reach what the peer requires. There is no downgrade path
  [0006 §4.4], [PROTOCOL.md](PROTOCOL.md) §2.3. Both declarations are **on the wire**, as
  HELLO keys `5` and `6`, and a `core` set is omitted so that a v0 HELLO stays byte-identical
  ([PROTOCOL.md](PROTOCOL.md) §6.1); a header that requires more than it offers is rejected by
  the decoder rather than negotiated.
- **A live connection therefore carries the set its runtime configured, and nothing reports
  it.** `RuntimeConfig::guarantees` is offered *and* required — one setting by
  [0006](decisions/0006-guarantee-sets.md) §4.4 — so for any connection that came up the
  agreed set is the minimum of two offers that each reached the requirement, which is that
  configured set exactly. An accessor for it would hand a caller its own configuration back,
  which is why there is none ([0027](decisions/0027-the-negotiated-set-is-the-configured-set.md)).
  A peer that requires more than its peer offers is refused at connect time and receives no
  weaker connection.

---

## 5. Unknown outcomes

Distributed systems contain states where the caller cannot know what happened. This MUST be
represented explicitly, as a first-class result rather than as an error or a success.

Example:

```text
replier read the request to FIN and acted on it
reply written
connection disappeared before the requester saw it
```

The truthful result is not necessarily `Failed`. It may be `Indeterminate` /
`UnknownOutcome`. The public API MUST preserve this distinction. Collapsing
`Indeterminate` into either `Ok` or `Failed` is a correctness defect, because it invites the
application to retry a possibly-applied operation or to discard a possibly-applied one.

The v0 rules that produce `Indeterminate`, and what an application may do with it, are
normative in [FAILURE_MODEL.md](FAILURE_MODEL.md).

---

## 6. Implementation status in v0

Everything below describes the **L0 stream core** and the **L1 patterns** built on it. The
L2 broker layer, where the completion states of §1 acquire meaning, is `weida-broker`.

This table is also the normative source for the default guarantee set `core` of §3: `core` is
what this table says, so the default cannot drift away from what the code does
([decisions/0006](decisions/0006-guarantee-sets.md) §4.2). Rows marked *spec ahead of code*
name a level that is decided and specified but not yet implemented.

| Dimension | v0 support | Notes |
| --- | --- | --- |
| Acknowledgement | transport receipt at L0; **`Accepted` reported as a cursor** by `weida-broker` | `Delivery::delivered()` resolves `Ok(())` when the peer's **transport** holds every byte and the FIN — explicitly not "the application read it" (§3). That is the *only* signal the brokerless core produces on its own. The higher levels have a **carrier**: a sender orders levels per transfer in DATA keys `9`-`11` and a receiver reports them as **absolute byte offsets** on a CURSOR stream, frame kind `6`, which never shares a stream with payload ([PROTOCOL.md](PROTOCOL.md) §6.7, [decisions/0023](decisions/0023-completion-is-a-cursor.md)). Ordering a level is a **request, not a guarantee**: a hop that cannot reach it simply does not report it, and the transfer does not fail for that — a level a peer MUST reach is this dimension, negotiated in HELLO. `weida-broker` reports `Accepted` at the admitted body length and orders `Processed` on each delivery, which its consumer reports; `Stored` and `Replicated` are reported by nothing in this repository, because no hop here owns a store. |
| Delivery | `BestEffort` only | v0 performs no retries. A failed or indeterminate transfer is reported to the application, which decides. `AtMostOnce` and `AtLeastOnce` require retry and dedup machinery that does not exist yet. |
| Ordering | `None` by default; `PerProducer` **implemented in both modes**, opt-in | QUIC guarantees byte order **within** one stream. A one-way transfer is one stream, and each half of an exchange is one stream, so a single payload is ordered end to end. Across streams there is no ordering guarantee of any kind, which is what `PerProducer` addresses: a runtime configured with it (`RuntimeConfig::guarantees`) numbers its one-way transfers per (producer, path or topic) in DATA key `6`. In **detect** mode the receiver reports what is missing through `IncomingMeta::gap` and delivers every message as it arrives; in **reassemble** mode it holds an arrival whose predecessors are missing and releases the run in sequence order, bounded by `Limits::max_reorder_hold` and releasing out of order with a reported gap at the bound (§3). The level is declared in HELLO and negotiated, so both ends agree or the handshake fails ([PROTOCOL.md](PROTOCOL.md) §2.3). *Spec ahead of code:* `PerKey` is L2-only by decision [0001 §7.4]; `Total` is not specified. Exchanges are not numbered: a reply carries no endpoint, and the stream is the correlation. |
| Deduplication | `None` by default; `Bounded(window)` **implemented**, opt-in | Under `core` there are no idempotency ids and no dedup window, and nothing on the wire names a transfer, so a receiver could not deduplicate even if it wanted to. A runtime configured `Bounded` with a window (`RuntimeConfig::guarantees`) remembers the identity of what it received — `(producer, scope, sequence)`, the producer being the connection's proved fingerprint unless DATA key `7` names another ([decisions/0008](decisions/0008-session-identity.md) §4.4) — and suppresses a repeat inside the window, counting it in `Runtime::suppressed_duplicates`. A suppressed transfer is read to EOF and discarded, so the sender sees an ordinary receipt: the window saves the application, not the bandwidth. Bounded means bounded twice over — in time by the window and in count by `Limits::max_dedup_entries` — so an identity reused after its window, or evicted at the cap, is **not** suppressed [0001 §7.6]. `Durable` needs a store and belongs to the L2 broker. |
| Backpressure | `Block`, `Reject`, `Drop` | **The two credit units at L0 are bytes and streams, and there is no application credit *at L0*** — the L2 credit frame (kind `5`) is per subscription and exists only where a queue serves the path ([PROTOCOL.md](PROTOCOL.md) §6.6). Bytes: `stream_receive_window` and `connection_receive_window`. Streams: `max_concurrent_uni_streams` and `max_concurrent_bidi_streams`, which *are* weida's message credit — a consumer sizes its prefetch by granting them ([decisions/0003](decisions/0003-credit-unit.md) §4.1, §5). Both are receiver-granted through QUIC transport parameters and both are absolute and idempotent; nothing on the L0 wire grants credit at the application level, and the per-subscription message credit of [0003 §4.2] is the L2 credit frame above, which only a queue honours. `Block`: those two windows, those two budgets and bounded internal channels (`endpoint_queue`, the actor control channel) make senders await capacity; this is what Req/Rep and Push/Pull use. The budget lands on `open`, and a transfer parked in an accept queue still holds its stream, so a deeper queue does not raise it ([PATTERNS.md](PATTERNS.md) §1.4). `Reject`: `IncomingTransfer::read_capped` refuses a payload past its cap with `STOP_SENDING(REJECTED)` and `LimitExceeded` before buffering it, and `Publisher::publish` rejects a payload larger than `subscriber_buffer_bytes` locally. `Drop`: publisher fan-out only, and there are **two causes** — a subscriber past `subscriber_buffer_bytes` loses the message rather than stalling the publisher, and on a socket transport a publisher that finds no parked reverse connection drops that copy ([decisions/0012](decisions/0012-local-connection-grouping.md) §4.4); both are counted in `Publisher::dropped` and neither ends the subscription. **Which end of the queue is dropped is the arriving copy, never a queued one** — the fact that separates a fan-out drop from conflation ([decisions/0016](decisions/0016-conflation.md) §2): a fan-out drop is drop-*new*, so a subscriber that falls behind keeps the oldest copies it has not read and loses the newest, which is the opposite of what "keep the newest, discard the rest" means. A **streamed** publish (`Publisher::open`, [PATTERNS.md](PATTERNS.md) §4.1) drops the same way with one difference worth stating: the unit is the transfer, not the message, because half a payload is not a payload, so a subscriber that misses one chunk has its stream reset with `CANCELED` and loses that payload alone. A producer that wants the newest value instead keeps one slot and publishes with `write_now`, which is v0's conflation and is the application's ([0016](decisions/0016-conflation.md) §4.3). **Over a local transport the units are different but the answer is the same:** there is no connection window to share and no stream budget, so `Block` on the byte windows becomes the kernel's socket buffer or a channel's capacity, and the stream budget becomes a descriptor count — an `open` with all `max_local_streams` (255) live **waits** for one to end, exactly as a QUIC `open` waits on the peer's stream budget, so `Block` is `Block` on every transport ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.13; [decisions/0010](decisions/0010-local-transport.md) §4.2, [PATTERNS.md](PATTERNS.md) §1.10). **A sender with no live peer has one more bound, and it is its own:** the outbox a `Pusher::send` or a dialling `Paired::send` lands in while the runtime redials ([decisions/0031](decisions/0031-transparent-redial-and-the-sender-outbox.md) §4.2), `RuntimeConfig::outbox_messages` (1000) and `outbox_bytes` (8 MiB), at which `RuntimeConfig::outbox_full` — not the negotiated dimension, because a puller has no say in how its pusher waits — is `Block` (the default, ZeroMQ's `PUSH` at its high-water mark), `Drop` counted in `Pusher::dropped`, or `Reject` as `LimitExceeded`. `Spill` and `Coalesce` are not implemented. |
| Producer naming | `Fingerprint` only | The proved fingerprint names the producer and is therefore **not written**: the receiver has it from the handshake ([decisions/0008](decisions/0008-session-identity.md) §4.4). `Stable` needs a supplier that outlives a connection, which is an L2 subscription, so a v0 peer declares it neither offered nor required. |
| Control isolation | `No`, **partly provided** | One connection per dialled endpoint path, so payload on one path does not stall another ([PATTERNS.md](PATTERNS.md) §1.3, [PROTOCOL.md](PROTOCOL.md) §2.5, proved by `a_stalled_path_does_not_stall_another_path`). What is coupled is control traffic on the *same* path: a SUBSCRIBE rides the connection of the path it names, because that is the only route back to the subscriber ([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.1-§4.2), so an endpoint that publishes and subscribes on one path can queue its own subscription behind its own payload. The declaration therefore stays `No` — a peer that declared `Yes` would be promising a tier that carries control frames on a connection of their own, and that control tier is parked by [0011 §4.3] for want of cargo — and it remains declarable, which is what lets a peer that needs it fail the handshake rather than discover the coupling under load. A peer that must keep one session's stall away from another's gets it from the per-path rule: one weida connection per session, which is one connection per path ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.7). **The local transports have neither the coupling nor a way to declare it away:** one transfer is one channel pair or one OS connection, so nothing shares a window and the stall this row is about cannot arise ([decisions/0010](decisions/0010-local-transport.md) §4.2) — what a local peer pays instead is a descriptor per live transfer and, for fan-out, a parked reverse connection. |
| Indeterminate outcomes | implemented | First-class: `Error::Indeterminate` is deliberately excluded from `Error::is_definite_failure()`. See [FAILURE_MODEL.md](FAILURE_MODEL.md). |
| Peer identity | implemented | Every inbound transfer carries the sending peer's identity in `IncomingMeta::peer`: the proved public-key fingerprint over QUIC, `None` for an anonymous client, a `Local { uid, gid, pid }` principal over `AF_UNIX` and a `Windows { sid, pid }` principal over a named pipe, where the kernel proves it instead of TLS and the PID is an observation that MUST NOT be authorized on ([decisions/0010](decisions/0010-local-transport.md) §4.4); in process there is no identity, because there is no boundary to prove one across. Whichever it is, it comes from the transport and never from a header, so it can be authorized on but not claimed (master doc §47). Over QUIC, `IncomingMeta::peer_chain` carries the certificate chain behind the key as presented, leaf first, at most 8 certificates and 32 KiB, parsed by weida no further than the leaf's public key ([decisions/0035](decisions/0035-keys-proved-not-judged.md) §4.2). Trust is stated per dialling endpoint (`Trust`: pins, anchors, or only what the address names) and optionally required of clients per binding (`ServerTls::require_client`), either against a trust or as `ClientTrust::AnyKey`, which judges no key but still requires the client to prove possession of it in the handshake signature ([decisions/0035](decisions/0035-keys-proved-not-judged.md) §4.1). Authorization beyond "is this key trusted at all" is the application's decision on the fingerprint. The fingerprint is also the peer's name **across** connections — two connections that proved the same key are one peer, which is what binds a peer's connections together — and it carries no session: recognizing a peer restores nothing from a previous connection ([decisions/0008](decisions/0008-session-identity.md) §4.1, §4.2, §4.4). Locally that role falls to the **group token plus the kernel credentials**, never the token alone ([decisions/0012](decisions/0012-local-connection-grouping.md) §4.2), and it resumes nothing either. **An identity is not a grant**, and that is a decision rather than an omission ([decisions/0015](decisions/0015-peer-authorization.md)): the handshake proves who answered, and whether that peer may use a given endpoint path is the acceptor's own decision, taken on `IncomingMeta::peer` and expressed with the refusals [PROTOCOL.md](PROTOCOL.md) §9.4 defines. No credential rides the wire, and a token flow is an application exchange plus `ServerTls::require_client` ([PATTERNS.md](PATTERNS.md) §1.9). **Where an identity comes from is a source** ([decisions/0032](decisions/0032-identity-sources-and-the-handoff.md)): ephemeral by default, a reloaded directory, or a PKI role in OpenBao signing over the local key, so a certificate rotates under a live binding while the fingerprint — and everything above that pins it — stays. **Revocation is not checked**: no CRL, no OCSP, on any trust path; a certificate is accepted until its `notAfter`, and a short TTL under a renewing source is the only mitigation. |
| Hop-locality | implemented, trivially | Exactly one hop exists in the brokerless core (a direct connection); `weida-broker` adds the second, and its certificates are hop-local (§2). |
| Transport | QUIC, in-process, `AF_UNIX` and named pipes | The local transports of [decisions/0010](decisions/0010-local-transport.md): in-process, `AF_UNIX` and Windows named pipes are all **implemented**, the last two sharing one implementation of the 0012 grouping. Three of their properties belong in this table because they change what a row above means: a local peer is proved by the **kernel**, so `IncomingMeta::peer` is a `PeerIdentity` — a key on QUIC, a `Local { uid, gid, pid }` principal on `AF_UNIX`, a `Windows { sid, pid }` principal on a named pipe, and nothing in process [0010 §4.4]; a local transfer is its own OS connection, so there is no shared connection window and `Block` is that connection's own buffer rather than a budget shared with siblings [0010 §4.2]; and **a reset is never mistaken for a FIN on any of them**: QUIC has `RESET_STREAM`, the in-process reader checks the writer’s reset flag, and both socket transports frame their payload and carry a RESET chunk with its code (`crates/weida/src/chunked.rs`). A named pipe has no half-close and a stream socket has a half-close and no abort — `SO_LINGER` with a zero timeout plus `close` is a plain close at the reader — so one framing answers both, at five bytes per write and the zero-copy read ([0012](decisions/0012-local-connection-grouping.md) §4.7(e) prices it). What a local peer *is* — the connections the kernel credentials and a group token bind together — is [decisions/0012](decisions/0012-local-connection-grouping.md) §4.2, and fan-out toward such a peer rides the connections it parked, bounded by `Limits::max_parked_reverse` [0012 §4.4]. |

### Per pattern

| Pattern | Delivery | Acknowledgement | Ordering |
| --- | --- | --- | --- |
| Req/Rep | `BestEffort` | the reply itself | `None` across exchanges |
| Push/Pull | `BestEffort` | optional transport receipt | `None` |
| Pub/Sub | `BestEffort`, with per-subscriber drop | none | `None` |
| Pub/Sub over a local **socket** transport | `BestEffort`, with a second drop cause: the subscriber's reverse pool | none | `None` |
| RADIO/DISH | `BestEffort`, with supersession, expiry at the smaller of the sender's and the dish's `max_age`, and layer cuts from the top ([decisions/0034](decisions/0034-late-is-lost.md) §4.6, [decisions/0037](decisions/0037-layered-segments.md) §4.3) | none | per topic and layer, segments never go backwards at a dish |
| `Peer::segment` | `BestEffort`, with supersession and sender expiry ([decisions/0037](decisions/0037-layered-segments.md) §4.2) | none | per topic, numbered |
| flow | `BestEffort` per datagram ([decisions/0034](decisions/0034-late-is-lost.md) §4.2) | none | none |
| SWARM (feature `swarm`) | `BestEffort`, per order, with **content integrity against the BLAKE3 root**: a `Fetch` yields a `Piece` only after it verified, so what it hands over is the blob's own bytes or nothing, from whichever seeder sent them ([decisions/0040](decisions/0040-swarm-pattern.md) §4.4, [decisions/0041](decisions/0041-swarm-sessions.md) §4.3). That is a property of the bytes, not a hop receipt | none beyond the verified answer; a seeder's `answer_*` returns once its bytes are written into the session, with no receipt per answer | none: pieces arrive in any order |

Four points deserve emphasis, because each is easy to assume otherwise:

- **Req/Rep needs no receipt.** A reply is written by the peer's application after it read
  the request to FIN and dispatched it, so it proves strictly more than a transport receipt
  ever could. `Requester::request` therefore drops the `Delivery` of the request half and
  waits on the reply; an ERROR frame on the reply half is equally conclusive. The receipt
  remains available to callers who drive the halves themselves with `Requester::open`.
- **Pub/Sub drops are silent to the subscriber under `core`, and visible under
  `PerProducer` in either mode.** A subscriber whose byte budget at the publisher is
  exhausted simply does not receive that message; the publisher counts the drop locally
  (`Publisher::dropped`, and per topic and cause in `Publisher::dropped_on`, so "which
  signal is starving" is a lookup and not a guess). This is the one place where weida
  answers overload by discarding,
  and it is confined to fan-out (master doc §17). The copy is numbered before fan-out, so the
  number a dropped copy would have carried is missing from that subscriber's sequence. Under
  **detect** the next copy it receives carries `IncomingMeta::gap` naming exactly what it
  lost (`a_dropped_fan_out_copy_shows_up_as_a_gap`). Under **reassemble** the copies behind
  the hole are held — the missing number is never coming — until the hold is full, and the
  release forced by the bound carries the gap
  (`a_full_hold_reports_the_pub_sub_drop_it_was_waiting_for`). Either way the loss is
  observable, which is the capability [decisions/0001](decisions/0001-sequence-field.md)
  §7.2 required.
- **Over a local socket transport a copy has a second way to be dropped.** A publisher
  cannot dial a peer that dialled it, so every copy needs one of the connections the
  subscriber parked; a publisher that finds the pool empty drops that copy and counts it
  in the same `Publisher::dropped`
  ([decisions/0012](decisions/0012-local-connection-grouping.md) §4.4,
  `an_exhausted_reverse_pool_drops_the_copy_and_counts_it`). Two consequences are worth
  stating plainly: the publisher is never stalled by a subscriber that is slow to
  replenish, and the subscription survives its drops — the pool refills and the next copy
  goes out. A subscriber that parks nothing cannot receive fan-out at all and is refused
  when it subscribes, because silence is not an answer
  (`a_subscriber_that_parks_nothing_is_refused_at_connect`).
- **No wire signal confirms a subscription either.** The converse of the drops above: a
  SUBSCRIBE is never answered, and the connection staying up means only that it was received
  and not refused ([decisions/0017](decisions/0017-subscription-verdict.md) §4.1). So
  "silence is not an answer" holds on the wire in the other direction too: a subscriber that
  needs a verdict asks for one over Req/Rep.
- **Ordering across messages is `None` unless it is configured.** Each message is its
  own stream and QUIC does not order streams relative to each other. A publisher's
  per-subscriber writer enqueues copies in publication order, but that is an implementation
  property of one hop, not a guarantee an application may rely on. `PerProducer(detect)`
  does not change delivery order either: it reports what is missing and holds nothing back.
  `PerProducer(reassemble)` does change it — that is its entire point — and pays for it with
  head-of-line blocking bounded by `Limits::max_reorder_hold`.
