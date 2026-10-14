# 0016 — Conflation — keep the newest, discard the rest

- **Status:** provisional
- **Date:** 2026-09-13
- **Related:** B-065; [requirements/zeughaus-video.md](../requirements/zeughaus-video.md) request
  2; [0001](0001-sequence-field.md) §7.1, §7.2; [0003](0003-credit-unit.md) §4.2;
  [0006](0006-guarantee-sets.md) §4.1-§4.5; [0007](0007-topic-namespace.md) §4.1;
  [0009](0009-drain.md) §4.4; [0012](0012-local-connection-grouping.md) §4.4;
  [GUARANTEES.md](../GUARANTEES.md) §3, §4, §6; [PROTOCOL.md](../PROTOCOL.md) §4, §5, §6.2,
  §6.4, §6.5, §7, §9.5, §11; [PATTERNS.md](../PATTERNS.md) §4, §4.1;
  [INVARIANTS.md](../INVARIANTS.md); P4 and P5 of [SYNTHESIS.md](../research/SYNTHESIS.md).

## 1. The question

B-065, verbatim in its acceptance line: where "keep the newest, discard the rest" lives — "the
reserved broker-level `Coalescing(key)` of [GUARANTEES.md](../GUARANTEES.md) §6, a fan-out drop
policy beside `Drop`, or nowhere in L0/L1 at all" — "with the evidence from the sheets… and the
consequence for the guarantee vocabulary spelled out; the note says plainly which layer owns the
key and what a subscriber observes when a value is skipped" [BACKLOG B-065].

The source is request 2 of
[requirements/zeughaus-video.md](../requirements/zeughaus-video.md), which states both the
present absence and the reason the question is not urgent for its own design: "**Nothing
conflates.** Backpressure is `Block`, `Reject` or (fan-out only) `Drop`; 'keep the newest,
discard the rest' is not implemented and is planned as broker-level `Coalescing(key)`… The
design below does not need it: a private stream per viewer conflates by construction, because
the runtime holds only the newest frame" [zeughaus-video, "What weida provides today"]. Its
priority list qualifies that: conflation is "**A conflating queue** — the planned
`Coalescing(key)`. Not needed for a private stream per viewer… but the moment fan-out exists it
does: one publisher and N consumers at different speeds is exactly where 'keep the newest,
discard the rest' has to live in the transport rather than in the producer" [zeughaus-video,
"What weida would need", item 2].

B-064 is what makes the question answerable rather than hypothetical: streaming fan-out now
exists ([PATTERNS.md](../PATTERNS.md) §4.1, [PROTOCOL.md](../PROTOCOL.md) §11,
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §1's Phase 3 deferral list, "**Delivered later as
B-064**"), so "one publisher and N consumers at different speeds" is a shape the code has, and
the discard is already the transport's rather than the producer's. The question is therefore no
longer "should something discard" but "does the discard need a **key**, and if so, who holds
the structure the key indexes".

## 2. The evidence, condensed

**What v0 discards today, stated precisely, because the difference is the whole question.**
Fan-out `Drop` is per subscriber, counted in bytes, and **drops the arriving copy**: "a copy
that does not fit in `subscriber_buffer_bytes` for that subscriber is dropped and counted in
`dropped()`, and per topic and cause in `dropped_on(topic)` / `drops()` — budget, full queue, or
no parked connection on a socket transport… the publisher never blocks"
[PATTERNS §4], [GUARANTEES §6], [0012 §4.4]. That is *drop-new*. Conflation is *drop-old*: the
value already queued is the one that goes. The two differ only in which end of the queue is
sacrificed, and they converge exactly when the queue holds one value — **conflation is drop-new
at a queue depth of one, per key**. weida's queue depth is not one and is not per key: it is
`Limits::subscriber_buffer_bytes`, 8 MiB by default (`crates/core/src/limits.rs`,
[PROTOCOL §10]), shared by every topic that subscriber matches.

**What B-064 added, and what it already conflates by construction.** `Publisher::open(topic)`
returns a `FanOut`, one stream per matched subscriber written chunk by chunk;
`write_now(chunk)` "never waits: a subscriber without room right now loses the transfer.
Fan-out's `Drop` in its purest form, and the right call where a later chunk supersedes an
earlier one", while `write_within(chunk, limit)` waits a mandatory finite bound first
[PATTERNS §4.1], [0009 §4.4]. The drop is "per subscriber and per *transfer* rather than per
message — a subscriber that misses one chunk loses the whole payload, because half a frame is
not a frame" [PATTERNS §4.1]. A producer that holds a single-slot latest value and calls
`open`/`write_now` per value therefore already keeps the newest and discards the rest, per
subscriber, with no key and nothing retained: exactly the requester's own construction, which it
describes for its private-stream design as "Conflation falls out of holding only the newest
frame, which the capture path already does" [zeughaus-video, "A video signal is one standing
request"].

**ZeroMQ: conflation with no key at all, and this repository already refused it.**
`ZMQ_CONFLATE` "keeps only the last message per queue and 'Ignores ZMQ_RCVHWM and ZMQ_SNDHWM
options. Does not support multi-part messages'; for PULL, PUSH, SUB, PUB, DEALER; default off",
with the caveat "If recv is not called on the inbound socket, the queue and memory will grow
with each message received" [zeromq §5]. Key: **none** — the unit is the per-peer pipe, so a SUB
socket conflates across every topic it subscribed to. Owner: the local socket, set by whichever
side sets the option; nothing about it is negotiated or visible on the wire, since ZMTP has no
acknowledgement and no sequence [zeromq §6, §7]. When: as the queue is written, replacing
whatever stands there. What a subscriber observes: nothing whatsoever — no receipt, no count, no
gap; "no confirms, receipts, counters or tracing headers in the protocol" [zeromq §12/P16], and
the drop is not even attributable to conflation rather than to an ordinary PUB-side HWM drop
[zeromq §12/P4]. And `weida-zmq` refuses the option outright, in its own words: "`ZMQ_CONFLATE`
— **refused** — absent: keep-only-the-last is a queue this library does not have, and §11 names
it as one of the four unbounded-resource shapes: 'the queue and memory will grow with each
message received' on an inbound socket nobody reads" [zmq.md §5]. That refusal is the
same argument this note has to answer for weida itself: a conflating queue is still a queue,
and a queue nobody drains is unbounded memory [INVARIANTS].

**MQTT 5: a keyed last-value store, and not a queue policy.** "RETAIN = 1 replaces any existing
retained message for that exact topic ([MQTT-3.3.1-5]); a zero-byte payload deletes it and is
itself not stored ([MQTT-3.3.1-6], [MQTT-3.3.1-7]); RETAIN = 0 neither stores nor removes
anything ([MQTT-3.3.1-8])" [mqtt5 §4.4]. Key: the exact Topic Name, "at most one per exact Topic
Name, held outside any session" [mqtt5 §2]. Owner: the server, and the store survives session
end — "Retained messages are explicitly not session state and survive session end (4.1)"
[mqtt5 §2]. When: at publish time, by replacement. What a subscriber observes: a delivery **at
subscribe time**, governed by Retain Handling 0/1/2 and by Retain As Published, where "0 the
server clears RETAIN when forwarding ([MQTT-3.3.1-12])" — so with Retain As Published 0 a
subscriber cannot even tell the message was retained [mqtt5 §4.4]. The decisive point for B-065:
this answers the *late joiner* (P5), not the *slow consumer* (P4). MQTT's actual slow-consumer
behaviour has no protocol bound at all — "the queue behind the window has **no protocol bound**",
and implementations diverge: "Mosquitto silently drops subsequent QoS 1/2 messages at
`max_queued_messages`; EMQX evicts the oldest QoS 0 message; HiveMQ defaults to `discard` (drop
new)" [SYNTHESIS §1 P4], [mqtt5 §12/P4]. Retention and conflation are two mechanisms, and only
the second is what B-065 asks about.

**NATS: a stream-level limit, owned by the stream.** Core NATS conflates nothing and answers a
slow consumer by hanging up: the server queues pending outbound bytes per client up to
`max_pending` (64 MiB default) and "beyond it the server reports a slow consumer and
**disconnects that client**" [SYNTHESIS §1 P4], [nats §11]. The last-value shape is JetStream's:
"Stream limits include maximum messages, bytes, age, **messages per subject**, consumers, and
storage" [nats §11], with a discard policy — "`DiscardNew` rejects a write at full limits while
`DiscardOld` removes older retained messages to admit a new one" [nats §11] — and a consumer
deliver policy that can start at "last per subject" [nats §4]. A KV bucket is a view over that,
not a separate engine: "a **key-value bucket** is a JetStream-based materialized key/value view,
not an independent storage engine" [nats §2], exposing "the latest revision for a key and a
watchable update history" [nats §12/P5]. Key: the subject. Owner: the **stream**, i.e. server
configuration, chosen by neither the producer nor the consumer [nats §2, §11]. When: at append
time, when a limit is exceeded. What a consumer observes: the sheet states that "a stream
assigns a sequence number when it appends a matching published message" [nats §2] and that
limits evict; it does **not** state what a live consumer or watcher observes at the moment of
eviction, so this note does not claim one.

**Kafka: keyed compaction as a background process over durable storage.** "Compaction retains
the latest record for each key in a compacted topic while preserving an ordered log and allows
tombstones to remove keys after their retention" [kafka §3]. Key: the record key, and it is
mandatory — "a compacted topic requires keys for meaningful table state; a null key is invalid
for compaction and can produce `CORRUPT_MESSAGE`" [kafka §3]. Owner: the topic's cleanup policy,
run by the broker's cleaner. When: **eventually, never at produce time** — "compaction is not an
immediate snapshot guarantee: consumers must tolerate older versions and tombstones until
cleaner progress and retention make them unavailable" [kafka §3]. What a consumer observes:
offsets, unchanged — "compaction retains offsets and order even when obsolete keyed records are
removed" [kafka §7], so the consumer sees offset gaps and never reordering. Deletion needs an
explicit record: "a tombstone is a keyed record with a null value used by compaction to remove a
key eventually" [kafka §7]. And it does not compose with everything: "KIP-405 does not support
compacted topics with tiered storage enabled" [kafka §3].

**The shape all four share.** Conflation is a property of something that **holds values**: a
socket's local pipe (ZeroMQ, unkeyed, per peer), a server's retained-message table (MQTT, keyed
by topic, consulted at subscribe), a stream's per-subject limit (NATS, keyed by subject, applied
at append), a topic's cleaner (Kafka, keyed by record key, applied in the background). In no
sheet is it a property of a transport that holds nothing. weida L0 holds nothing between
producer and subscriber except a per-subscriber byte budget and QUIC's own windows — "the two
credit units at L0 are bytes and streams, and there is no application credit" [GUARANTEES §6],
[0003 §4.1] — and weida's own P5 answer is the null one: "Nothing is retained… No last-value
cache, snapshot or replay mechanism is stated in any weida document" [SYNTHESIS §5 P5].

**What the vocabulary actually reserves, which is less than B-065's phrasing suggests.** The
backpressure dimension lists five levels — `Block`, `Reject`, `Drop`, `Spill`, `Coalesce` — with
the caveat "these names are provisional; semantics matter more than naming", and the dimension
is "**not** ordered… its levels are behaviours, not strengths, so two peers state the same one
or fail to agree" [GUARANTEES §3], [0006 §4.3]. §6's backpressure row lists only "`Block`,
`Reject`, `Drop`" and does not mention coalescing at all [GUARANTEES §6]; §4's illustrative
"very fast" configuration writes `backpressure = drop/coalesce` [GUARANTEES §4]. The wire value
exists and is bare: `backpressure` is [PROTOCOL.md](../PROTOCOL.md) §6.5 key `7`, "`0` Block,
`1` Reject, `2` Drop, `3` Spill, `4` Coalesce", a `uint` with **no key parameter**; the code
matches, `Backpressure::Coalesce = 4` marked "Reserved" in `crates/protocol/src/header.rs`. So
the spelling `Coalescing(key)` appears only in B-065 and in the requirement, never in
GUARANTEES — and the requirement's citation for it, `docs/GUARANTEES.md:181-190`, now points at
the refusal-race paragraph of §1 instead. There is a reserved *level*; there has never been a
reserved *key*.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — keep it reserved as the broker-level `Coalesce` of the backpressure dimension** | no v0 behaviour; the level stays declarable and unhonourable, and the L2 broker hop of Phase 6 defines the keyed queue | Kafka's compacted topic, a broker-side cleaner over durable storage [kafka §3]; NATS' `max_msgs_per_subject` on a JetStream stream [nats §11] | Reserves a word without a definition, which is what let `Coalescing(key)` be quoted as a plan while no document defined the key, the owner or the observation. Costs nothing on the wire and answers the requester with "later" |
| **B — a fan-out drop policy beside `Drop` at L1** | the publisher keeps, per subscriber and per key, the newest value not yet written, and replaces it instead of dropping the arrival | ZeroMQ's `ZMQ_CONFLATE`, the only brokerless precedent [zeromq §5]; NNG's SUB, which "drops its oldest queued message by default (`PREFNEW=true`)" [SYNTHESIS §1 P4] | Requires the publisher to **hold a value per key per subscriber** — the materialization [INVARIANTS] forbids and B-064 exists to remove [PATTERNS §4.1]. Undefined for `Publisher::open`, where there is no value to hold, only a chunk in flight. And since backpressure is not ordered, it is a *different* declared level, not a refinement of `Drop`: two peers state the same one or fail [GUARANTEES §3] |
| **C — nowhere in L0/L1 at all** | the transport keeps drop-new per subscriber, in bytes, unkeyed; nothing indexes a key | weida's own P4 answer: "There is no high-water-mark setting; the windows plus `endpoint_queue` do that job 'by exerting backpressure rather than by discarding'" [SYNTHESIS §5 P4] | Leaves the multi-topic case unanswered: one subscription matching a fast and a slow topic has one byte budget, so the fast topic's copies can consume it and the slow topic's copy is the one refused (§4.4) |
| **D — the application conflates, over the API B-064 delivered** | a single-slot latest value in the producer plus `Publisher::open` + `write_now`, with `subscriber_buffer_bytes` sized to about one value | the requester's own design [zeughaus-video, "A video signal is one standing request"]; ZeroMQ's Last Value Caching, which is an application-side XSUB/XPUB proxy rather than a protocol feature [SYNTHESIS §1 P5] | Only the producer can do it, so a relay or an adapter edge that receives values it did not produce has no conflation point [0006 §4.6]. The bound is a byte budget, not a value count, so "at most one behind" is a sizing convention rather than a guarantee |
| E — a retained last value per topic | the publisher keeps the last value per topic and sends it to a subscriber on SUBSCRIBE | MQTT retained messages [mqtt5 §4.4]; NATS deliver policy "last per subject" [nats §4] | Answers a different problem — P5, the late joiner, not P4, the slow consumer [SYNTHESIS §1 P4-P5] — and contradicts the stated Pub/Sub semantics that "nothing is queued for a subscriber that does not exist yet" [PATTERNS §4]. A separate question, not this one |

## 4. Decision

**Option C for the transport, option D for the application, option A left reserved with its
preconditions now written down. Option B is refused.** "Keep the newest, discard the rest" is a
property of a queue that holds values under a key; weida L0/L1 holds no such queue, so the
behaviour has no home there and does not get one. What the requester asked for exists today as a
producer-side construction over B-064's API, and the reserved `Coalesce` level of the
backpressure dimension stays what it is: a level for the L2 broker hop, honourable only once
something at that hop owns a keyed queue.

**4.1 Which layer owns the key — plainly.** *Today: no layer, because there is no key.* The
producer's application code owns the choice of topic and the single-slot latest value; the
transport owns a per-subscriber byte budget and reads no key when it discards. `Publisher::open`
takes a topic and fans out, and the topic is a *routing selector* matched against filters
[PROTOCOL §6.4], never an index into a slot. *If the L2 hop is ever built: the queue owns the
key, and the queue belongs to the broker* — the same placement every sheet shows, and the same
placement [0003 §4.2] already gives the per-subscription credit. **The key is the DATA `topic`
key**, [PROTOCOL.md](../PROTOCOL.md) §6.2 key `5`: opaque bytes capped at 256 B, already carried
on every fan-out copy, "opaque bytes: weida never parses it, and no character in it is special"
[PROTOCOL §6.2]. It is **not** a new caller-supplied byte string, because a third key namespace
beside endpoint paths and topics is precisely what [0007](0007-topic-namespace.md) §4.1 refused
when it put hierarchy in the topic namespace to keep "exactly one answer per (stream kind,
path)"; and it is not the endpoint path, because a path is opaque and identifies a registration,
not a value [INVARIANTS].

**4.2 Why option B is refused, and not merely deferred.** A fan-out coalescer has to hold the
value it will send instead of the one it replaced. For `publish` that is a `Bytes` per key per
subscriber inside the publisher — the per-subscriber copy B-064 was built to eliminate, "which
is exactly the materialization [INVARIANTS.md](../INVARIANTS.md) forbids" [PATTERNS §4.1]. For
`Publisher::open` it is not even definable: the payload is written chunk by chunk and never
materialized, the drop is per transfer, and "half a frame is not a frame" [PATTERNS §4.1], so
there is no object a later value could replace. A policy that works for the small API and is
undefined for the large one is not a transport behaviour; it is an application pattern, which is
option D.

**4.3 What the v0 answer is, concretely, and its one sizing rule.** A producer that wants
"keep the newest" does three things, all of which exist: hold one slot for the latest value;
publish with `Publisher::open(topic)` and `write_now` so that no subscriber can slow the
producer down [PATTERNS §4.1]; and set `Limits::subscriber_buffer_bytes` to roughly one value
rather than the 8 MiB default, so a subscriber can be at most about one value behind. The last
point is the honest form of the convergence in §2: drop-new at a queue depth of one *is*
conflation, and the budget is what sets the depth. What this does not give is a per-key depth —
see §4.4 — and it is bounded in bytes rather than values, so the convention has to be stated by
the application, not read off a guarantee.

**4.4 What a key would actually buy, so that the reservation has a purpose.** Exactly one thing:
**several keys multiplexed over one subscription**. A subscriber whose filter matches
`sensors.#` shares one byte budget across every topic under it, so a fast topic's copies can
occupy the budget and the copy refused is whichever arrives with no room — including a slow
topic's only value of the minute. Per-key conflation is what makes each key's newest value
independent of every other key's rate. B-067's per-topic drop table makes that failure
*visible* — `Publisher::dropped_on(topic)` names "which signal is starving" rather than leaving
it to be inferred [PATTERNS §4] — and deliberately does not fix it. A subscriber that wants
independence today gets it the way weida gets everything else: one subscription per key, or one
private exchange per consumer, which is the requester's design.

**4.5 Conflation and sequence numbers claim two things at once; here is the resolution.** A hop
that both numbers messages and deliberately discards them is asserting "this is my per-producer
sequence" and "I threw some of it away". They are reconcilable in exactly one order, and it is
the order the code already has: **the copy is numbered before fan-out**, so "the number a
dropped copy would have carried is missing from that subscriber's sequence", and under
`PerProducer(detect)` the next copy "carries `IncomingMeta::gap` naming exactly what it lost"
[GUARANTEES §6], [0001 §7.2]. Therefore:

- A conflating hop **MUST NOT renumber**. Renumbering after the discard would make the sequence
  a per-*subscriber* counter, and the dimension is per producer, scoped to (producer, endpoint or
  topic) [PROTOCOL §6.2], [0001 §7.1]. It would also make `Bounded` deduplication meaningless,
  since the identity it remembers is `(producer, scope, sequence)` [GUARANTEES §6].
- A deliberate discard is therefore indistinguishable, at the subscriber, from an accidental
  one: both are a `Gap`. That is correct rather than regrettable — from the subscriber's side
  they are the same event — and the distinction lives where the intent lives, in the publisher's
  per-cause counters [PATTERNS §4].
- `Coalesce` and `PerProducer(reassemble)` are **incompatible** and must be refused together at
  configuration time [0006 §4.5]. Reassemble "holds an arrival whose predecessors are missing"
  up to `Limits::max_reorder_hold` [GUARANTEES §6]; under conflation those predecessors are
  never coming by design, so the hold converts a latency-*reducing* policy into a
  latency-*increasing* one and releases only at the bound. Kafka is the precedent for the
  opposite choice done properly: compaction "retains offsets and order even when obsolete keyed
  records are removed" [kafka §7] — gaps, never renumbering.

**4.6 What a subscriber observes when a value is skipped — the whole table, today.**

| Case | Wire | Subscriber observes | Publisher observes |
| --- | --- | --- | --- |
| copy dropped at the budget, `core` | nothing is sent | silence; nothing on the wire says so [PATTERNS §4] | `dropped_on(topic)`, cause `SubscriberBudget` |
| copy dropped, `PerProducer(detect)` | the next copy carries a higher `sequence` | `IncomingMeta::gap`, expected against seen [GUARANTEES §6] | as above |
| copy dropped, `PerProducer(reassemble)` | as above | nothing until the hold fills, then the release carries the gap [GUARANTEES §6] | as above |
| streamed transfer dropped mid-payload | `RESET_STREAM(CANCELED)` on that subscriber's stream and nothing on anybody else's [PROTOCOL §11], [PROTOCOL §7] | "a partial payload, ended by a reset rather than a FIN, so it is never mistaken for a whole one" [PATTERNS §4] | `dropped_on(topic)`, per transfer |
| no parked reverse connection, local socket transport | nothing is sent | silence, and the subscription survives [0012 §4.4] | cause `NoParkedConnection` |

Never a reset *subscription* and never a renumbered stream. A conflating L2 hop, if it is ever
built, adds no row to this table: it produces the second row, by the mechanism of §4.5.

**4.7 What the guarantee vocabulary gains: nothing, and what would have to exist first.** No
dimension is added — the list is closed, "a wire key may not name a dimension this section does
not" [GUARANTEES §3] — and no level is added, because `Coalesce` is already there. What this
note adds is a definition for the reservation, so that it stops being a word:

1. **A queue that outlives the transfer.** L0 has none. `Block` is QUIC's windows and bounded
   internal channels, `Reject` is a cap refused before buffering, `Drop` is a byte budget per
   subscriber [GUARANTEES §6]. A keyed slot is state a hop holds on behalf of a consumer, which
   is the L2 broker hop of Phase 6.
2. **An owner for the key that is not the transport.** By §4.1 that is the broker's queue, and
   the key is the DATA `topic`.
3. **A moment at which the hop may replace a pending value**, which needs the per-subscription
   message credit of [0003](0003-credit-unit.md) §4.2 — reserved as frame kind `5`
   [PROTOCOL §11]. Without credit the hop never knows a value is *pending* rather than *sent*,
   and replacement has no defined instant. This is why conflation is a broker property in every
   sheet that has it and an unbounded socket buffer in the one that does not [zeromq §5].
4. **A bound on the keyed table**, named before the allocation exists [INVARIANTS]. The
   publisher's per-topic drop table already needed one and took `max_sequence_scopes`.

Until all four exist, a peer may declare `backpressure = Coalesce` and no peer may honour it:
declaring a level the other side does not offer fails negotiation with
`IncomparableGuarantee { dimension: "backpressure" }` rather than degrading quietly, because the
dimension is not ordered [GUARANTEES §3], [0006 §4.4], `crates/protocol/src/negotiate.rs`.

**4.8 The `Coalescing(key)` spelling is not adopted.** The level stays `Coalesce`, an unadorned
value of the backpressure dimension, and the key is fixed to the topic by §4.1 rather than
carried as a parameter. A parameterized level would need a second guarantee-set key for a value
that a peer can already read off the DATA header, and guarantee sets are deliberately
allocation-free: "every value is a `uint`, so a guarantee set introduces no new allocation a peer
can influence" [PROTOCOL §6.5].

**4.9 What a Connector may not do with this.** A managed Connector whose foreign side
conflates — for example `ZMQ_CONFLATE` or a JetStream stream with
`max_msgs_per_subject` — MUST NOT present the Queue side as conflating unless its concrete
conversion policy can prove that property. Protocol similarity is not a guarantee. The
Connector specification names the evidence and loss or the configuration is refused
[INVARIANTS], [0006 §4.7].

**4.10 If this is reopened, here is the bill.** Naming the numbers now is what makes the
reservation checkable rather than decorative. Nothing below is taken by this note.

| Piece | What it would take | Who allocates it |
| --- | --- | --- |
| honouring `Coalesce` | no new wire value: §6.5 key `7` value `4` exists and `Backpressure::Coalesce` is coded | nobody — the number is already spent |
| a forbidden combination | a row in §6.5's rules: `backpressure` `4` with `ordering` `2` is a framing violation, by §4.5 | [PROTOCOL.md](../PROTOCOL.md) §6.5, in the version that honours the level |
| a key parameter, if §4.8 were reversed | **guarantee-set key `10`**, the first free key in the §6.5 table (`0`-`9` are taken) | [PROTOCOL.md](../PROTOCOL.md). Keys `0..=63` are "reserved for this specification" and an out-of-tree extension "MUST use keys `64` and above" [PROTOCOL §5] |
| a subscriber asking for conflation, the way the requester's own runtime lets "the consumer declare… conflating or every-sample" [zeughaus-video, "The split"] | **SUBSCRIBE key `2`**, the first free key in the §6.4 table (`0` `endpoint` and `1` `filter` are taken, both required) | the same |
| a frame, if one were ever needed | **frame kind `6`**: kinds `0`-`4` are in use and kind `5` is reserved for the L2 credit frame [PROTOCOL §4], [PROTOCOL §11] | the same — and §4.7 argues it is a queue policy, so no frame is the right count |

**4.11 Status is `provisional` and why.** The decision is docs-only and reversible: no frame, no
key, no type, no config field and no default changed. It stays provisional because item 2 of a
named requirement is answered with "the API you already have, plus a limit you set yourself",
and whether that is enough is measurable rather than arguable — see §6.

## 5. Consequences and follow-ups

- **No field anywhere.** `RuntimeConfig` gains nothing: it already carries `guarantees:
  GuaranteeSet` (`crates/weida/src/config.rs`), and `GuaranteeSet` already carries
  `backpressure: Backpressure` with `Coalesce` among its values
  (`crates/protocol/src/header.rs`). `Limits` gains nothing: the knob that makes v0 conflate well
  is `subscriber_buffer_bytes`, which exists (`crates/core/src/limits.rs`). No code change falls
  out of this note.
- **[GUARANTEES.md](../GUARANTEES.md) §3.** The backpressure paragraph should carry the
  reservation's definition from §4.7 — a keyed queue at a hop, the key being the topic — so that
  `Coalesce` stops being a name with no referent, and should say that §6's row omits it because
  v0 does not implement it.
- **[GUARANTEES.md](../GUARANTEES.md) §6.** The Pub/Sub drop bullet should say *which end* of
  the queue is dropped: the arriving copy, not the queued one (§2). That is the single fact that
  made this question look answered when it was not.
- **[PROTOCOL.md](../PROTOCOL.md) §6.5.** When the level is honoured, its rules gain the
  forbidden combination of §4.5 (`backpressure` `4` with `ordering` `2`) and the statement that
  the coalescing key is the DATA `topic` of §6.2 rather than a declared value.
- **[PATTERNS.md](../PATTERNS.md) §4.1.** Gains the recipe of §4.3 in one sentence — single-slot
  latest value, `open` plus `write_now`, `subscriber_buffer_bytes` sized to one value — which is
  the pattern `write_now`'s row already gestures at.
- **[IMPLEMENTATION.md](../IMPLEMENTATION.md) §1.** The Phase 3 deferral line "Coalescing
  backpressure (master doc §27); only `Block`, `Reject` and fan-out `Drop` exist" gains this note
  as its reason, in the shape its streaming-fan-out neighbour already has.
- **[SYNTHESIS.md](../research/SYNTHESIS.md) §5 P4.** weida's P4 answer gains the drop-new
  qualification and the pointer here; §5 P5 stays as it is, because retention is a different
  question (§3 option E).
- **Backlog.** B-065 is answered by this note. No code item falls out of it. The measurement of
  §6 is worth an item only if the multi-topic starvation of §4.4 is actually observed.

## 6. What this note does not decide

- **Whether L2 ever implements `Coalesce`.** This note fixes what it would mean and what must
  exist first (§4.7); Phase 6 decides whether the broker wants it.
- **Anything about late joiners.** A retained last value per topic — MQTT's mechanism
  [mqtt5 §4.4], NATS' "last per subject" [nats §4] — answers P5, not P4, and stays open (§3
  option E): today "nothing is queued for a subscriber that does not exist yet" [PATTERNS §4].
- **Whether `subscriber_buffer_bytes` should ever be per topic.** That is the small version of
  §4.4 and would be a `Limits` change with its own bound; it is not proposed here.
- **Rate limiting or sampling.** "Keep the newest" is neither "one in N" nor "at most X per
  second"; neither exists and neither is decided here.
- **`Spill`,** the other reserved backpressure level [GUARANTEES §3]. It needs a store, so it is
  in the same position as `Durable` deduplication and belongs to the same later decision.

**What would reopen this.** One measurement, and it is checkable: a deployment where one
subscription matches several topics at very different rates, and `dropped_on(topic)` shows the
drops concentrated on a topic whose *own* publication rate is low — the fast topic eating the
shared byte budget of §4.4. That is the failure a per-key slot fixes and a per-subscriber byte
budget structurally cannot, and it is the only argument that would justify paying §4.7's bill.
A weaker second trigger: a relay or adapter edge that must conflate values it did not produce,
where option D has no producer to hold the slot [0006 §4.6].

## 7. Sources

weida documents: [GUARANTEES.md](../GUARANTEES.md) §1, §3, §4, §6;
[PROTOCOL.md](../PROTOCOL.md) §4, §5, §6.2, §6.4, §6.5, §7, §9.5, §10, §11;
[PATTERNS.md](../PATTERNS.md) §4, §4.1; [INVARIANTS.md](../INVARIANTS.md);
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §1 (the Phase 3 pattern increment's deferral list);
[BACKLOG.md](../BACKLOG.md) B-064, B-065, B-067;
[requirements/zeughaus-video.md](../requirements/zeughaus-video.md) ("What weida provides
today", "A video signal is one standing request", "The split", "What weida would need" item 2);
[libraries/zmq.md](../libraries/zmq.md) §5 (the `ZMQ_CONFLATE` row);
[0001](0001-sequence-field.md) §7.1, §7.2; [0003](0003-credit-unit.md) §4.1, §4.2;
[0006](0006-guarantee-sets.md) §4.3-§4.7; [0007](0007-topic-namespace.md) §4.1;
[0009](0009-drain.md) §4.4; [0012](0012-local-connection-grouping.md) §4.4.

Code read: `crates/protocol/src/header.rs` (the `Backpressure` wire enum, `Coalesce = 4` marked
reserved, and `GuaranteeSet`); `crates/protocol/src/negotiate.rs` (the per-dimension
intersection and `IncomparableGuarantee { dimension }`); `crates/weida/src/config.rs`
(`RuntimeConfig::guarantees`); `crates/core/src/limits.rs` (`subscriber_buffer_bytes`, 8 MiB;
`max_sequence_scopes`).

Research sheets: [zeromq.md](../research/zeromq.md) §5 (`ZMQ_CONFLATE`, the high-water marks and
what happens at the bound), §6 (no acknowledgement), §7 (ordering scope), §11 and §12/P4, §12/P16
(no counters in the protocol); [mqtt5.md](../research/mqtt5.md) §2 (retained message, one per
exact Topic Name, outside any session), §4.4 (RETAIN semantics, Retain Handling, Retain As
Published, QoS 0 retained), §12/P4 (no protocol bound behind the receive window; divergent broker
policies), §12/P5 (a last-value cache, not a replay log); [nats.md](../research/nats.md) §2 (KV
as a materialized JetStream view; stream sequence assignment), §4 (deliver policy, last per
subject; `DiscardNew`/`DiscardOld`), §11 (`max_pending`; stream limits including messages per
subject), §12/P4, §12/P5; [kafka.md](../research/kafka.md) §3 (compaction, tombstones, null keys,
"not an immediate snapshot guarantee", KIP-405), §7 (offsets and order retained through
compaction), §12/P5; [SYNTHESIS.md](../research/SYNTHESIS.md) §1 P4, §1 P5, §5 P4, §5 P5.
