# 0037 — Layered segments: codec-agnostic video, sent from either side

- **Status:** accepted
- **Date:** 2026-10-04
- **Amended:** 2026-10-04, §4.11 (numbers and freshness per connection, a copy that follows
  upstream); 2026-10-22, §4.5 (the finish grace learns each connection's settle time)
- **Items:** B-304 to B-312, B-314
- **Answers:** [requirements/griasdi-video.md](../requirements/griasdi-video.md) (all five asks
  of its "Proposal"; its open questions stay open)
- **Amends:** [0034](0034-late-is-lost.md) §4.6 (the supersession and expiry rows of the
  RADIO/DISH table), [0034](0034-late-is-lost.md) §6 ("A connecting radio": the upstream
  direction of a stream segment is now an L0 segment from a dialling `Peer`; a connecting radio
  stays deferred), [PROTOCOL.md](../PROTOCOL.md) §6.2 (DATA key `13`, "written by a radio only")
- **Related:** [0002](0002-control-and-bulk-separation.md) §6.2,
  [0011](0011-answered-where-it-arrived.md), [0015](0015-peer-authorization.md),
  [0016](0016-conflation.md) §4.2, [0024](0024-three-families-one-back-channel.md),
  [0031](0031-transparent-redial-and-the-sender-outbox.md), [0034](0034-late-is-lost.md),
  [0035](0035-keys-proved-not-judged.md) §4.3, [0036](0036-connection-statistics.md),
  [PATTERNS.md](../PATTERNS.md) §1.12, §5, §6.4

## 1. The question

griasdi shares screens, and later cameras, inside voice sessions: AV1, one stream segment per
2 s GOP, SFrame ciphertext the relaying server cannot read
[griasdi-video, Context]. Its screen-sharing branch runs on a draft weida change, tuco86/weida#2,
and its own proposal names what main lacks. Four things, in the order they block:

- **a superseding uplink from a dialling client** — the sharer dials its server, and only a
  radio can send a segment today;
- **priority among segment copies** — a large enhancement GOP must not delay the base layer of
  the same viewer, and nothing reaches the copies' streams;
- **quality that adapts per viewer without re-encoding** — one encoding for the slowest viewer
  costs everybody else, one rebuild per change costs everybody a keyframe;
- **signals for adaptation** — the sender's encoder target and a viewer's layer choice.

The owner's constraint, in his words: weida "bekommt nicht die av1 dependency und stellt eher
bereit was man braucht um so etwas zu bauen". The draft PR is not merged as it stands, because
its uplink is a one-off method rather than a pattern. So the question is not "how does weida
carry AV1" but: **which codec-free mechanisms let an encoder's layer structure be honoured by
the transport and by every relay on the way, from whichever side of a connection the video
starts.**

## 2. The evidence, condensed

**2.1 What main delivers** (read at `05a6b8e`). `Radio::segment(&self, topic)`
(`crates/weida/src/radio.rs:448`) opens one uni DATA stream per joined dish; `Segment`
(`radio.rs:639-705`) has `number`, `topic`, a `write` that never waits and drops a dish without
budget or queue room, and `finish`; each copy queues at most `COPY_QUEUE = 64` chunks
(`radio.rs:50`). A dish joins with `Dish::join(filter, max_age)` (`radio.rs:1116`), and its
freshness is one number per topic: `DishShared::fresh(topic, segment)` (`radio.rs:961`), called
from `deliver_segment` (`radio.rs:986-1006`), discards any arrival not newer than the newest
delivered. A segment reaches the dish as `Received::Segment(IncomingTransfer)`
(`radio.rs:933-936`) with `IncomingMeta::segment` (`crates/weida/src/transfer.rs:214`). Drops are
counted per topic in `TopicDrops` (`crates/weida/src/pubsub.rs:121-138`), which is not
`#[non_exhaustive]`. DATA key `13` is "written by a radio only" (`docs/PROTOCOL.md:533`); the next
free DATA key is `14`, SUBSCRIBE uses `0..=2` (`PROTOCOL.md:709-713`), and unknown keys are skipped
(§5, `PROTOCOL.md:470`). From 0035 a radio sees every join (`Radio::with_admission`,
`radio.rs:603`, with `Join { peer, peer_chain, filter }` at `radio.rs:62`) and can withdraw one
(`Radio::evict`, `radio.rs:617`); from 0036 every dialling handle reads its own link
(`Peer::connection_stats`, `crates/weida/src/stream.rs:474`; `Dish::connection_stats`,
`radio.rs:1103`).

**2.2 What the draft PR found.** tuco86/weida#2 (draft, head `5e6ee45`, based on `6dcb64b`) adds
`Peer::segment(topic, max_age) -> Segment` and `Peer::segment_drops(topic)`, moves the per-topic
numbering out of `RadioHub` into a shared `SegmentTopics`, and fixes supersession. Its finding is
the important part: **a healthy dish lost whole GOPs**, because opening segment *n+1* reset every
copy of *n* that was still unacknowledged, and behind a slow path a finished copy is always
unacknowledged for at least a round trip — longer while a keyframe burst drains at the congestion
window's pace. GOPs are back to back by construction, since a GOP ends where the next keyframe
begins. The fix gives a finished copy a grace of `rtt + 50 ms + rtt * bytes / cwnd`
(`SUPERSEDE_SLACK`, `finish_grace`, `finish_grace_over`) and biases the copy's write select so "a
finished copy outlives its successor's open"; a write that flow control holds back still means a
stalled dish and is reset at once. Its probe (15 GOPs of 10 frames, 15 and 30 fps, 25/50/100 ms
one way): "150/150 frames, nothing superseded" in every configuration, against "13/15 GOPs at
100 ms and 30 fps" before. The PR **does not compile on main**: `finish_grace_over` calls
`conn.conn.path_stats()`, which 0036 replaced with `Link::transport_stats()`
(`crates/weida/src/transport.rs:389`, read as `.map(|t| t.path)` at
`crates/weida/src/flow.rs:928`). It also carries an unrelated Windows change
(`current_account_sid`).

**2.3 Priority exists, is per connection, and the radio does not use it.**
`OutgoingTransfer::set_priority` (`transfer.rs:434`) is the only caller of `quinn`'s
`SendStream::set_priority`; every radio copy runs at the default 0. `quinn` orders the streams of
one connection by it — "locally buffered data from streams with higher priority will be
transmitted before data from streams with lower priority" — and warns that "changing the priority
of a stream with pending data may only take effect after that data has been transmitted" and that
"using many different priority levels per connection may have a negative impact on performance"
(`quinn` 0.11.12 `src/send_stream.rs:213-220`). Datagrams outrank every stream on their
connection by `quinn`'s packing ([0034](0034-late-is-lost.md) §2.3), and a priority never reaches
across connections, so across paths (0034 §2.4).

**2.4 MOQT draws the stream boundary at the dependency.** A subgroup is one stream, and its
objects "have a dependency and priority relationship consistent with sharing a stream"; a group is
delivered on "at least as many streams as there are Subgroups", and is the join point
([research/prior-art.md](../research/prior-art.md) lines 553-562). Priorities are per
subscription and per track, overridable per subgroup, and a delivery timeout set by both sides
takes "the smaller non-zero value" (lines 630-652).

**2.5 A layered encoding has a dependency order.** W3C's SVC extension for WebRTC (Working Draft
14 September 2026, §5) defines scalability modes such as `L1T3` (one spatial layer, three temporal
layers) and `L3T3` (three spatial layers at 2:1, three temporal layers, inter-layer dependency
"Yes"), with the identifiers AV1 assigns in its §6.7.5, and dependency diagrams in its §9. A
layer depends on layers below it in its own structure and never above, so every encoding of this
kind has a topological order of its layers, and **cutting any suffix of that order leaves a
decodable prefix**. That is the one fact about codecs this note uses.

**2.6 A plaintext layer index next to an encrypted payload has precedent.** The IETF draft for a
codec-agnostic RTP payload format observes that with SFrame "the RTP Payload is made completely
opaque to the SFUs, some extra mechanism must also be added for them to be able to route the
packets", and "complements the SFrame (media encryption), and Dependency Descriptor (AV1 payload
annex) documents": the forwarder routes on a header it can read, beside a payload it cannot.

## 3. Options considered

For the uplink:

| Option | Shape | Named loss |
| --- | --- | --- |
| U-A — `Peer::segment` as the draft PR has it | a one-off L0 method beside the radio | No layers and no priority, and a second entry into the copy machinery that every later radio change has to remember; the next feature (layers, priority) would land twice |
| U-B — a connecting RADIO and a binding DISH | ZeroMQ's sockets in both directions | Joins would run against the dial direction ([0011](0011-answered-where-it-arrived.md)), a bound dish accepts many radios and needs a merge rule per topic, and it is a protocol change for one user; [PROTOCOL.md](../PROTOCOL.md) §11 keeps connecting publishers deferred for the same reason |
| **U-C — the segment becomes an L0 unit** | the numbering, supersession, expiry and never-blocking write live with the segment; RADIO is its fan-out and `Peer::segment` one copy toward the path it dialled | Chosen. `Radio::segment` changes signature |

For adapting quality:

| Option | Shape | Named loss |
| --- | --- | --- |
| Q-A — one topic per layer, priority only | no wire change; griasdi's own proposal | weida cannot know that topic `.1` depends on topic `.0`, so it delivers layer 1 of a GOP whose layer 0 it dropped, and supersession on one topic does not reach the others; numbering is per topic, so a viewer cannot tell which layers belong to one GOP without its own index |
| **Q-B — layers inside a segment** | MOQT's subgroup: one stream per layer of one segment, the index in the header, cut from the top | Chosen. Up to sixteen streams per copy instead of one, each counted against the connection's stream budget like any copy |
| Q-C — a codec-aware radio that parses OBUs | read `temporal_id`/`spatial_id` from AV1's extension headers | Refused: a segment's payload stays opaque, as every DATA payload is ([0034](0034-late-is-lost.md) §6), and SFrame makes it unreadable anyway |
| Q-D — MOQT as the media layer | subgroups and priorities wholesale | Refused already by [0034](0034-late-is-lost.md) §3 option C, and nothing here changes that argument |

## 4. Decision

**U-C and Q-B.** A segment is an L0 unit any sender can open, a segment may carry ordered layers,
and weida cuts them from the top. Everything about which bytes are a layer stays the
application's.

### 4.1 The principle: an order and a direction, never a codec

weida knows that layers are numbered `0..=15`, that layer *k* may depend on layers below *k* and
never above, and nothing else. The application maps its encoding onto that order — a linear
quality ladder, lowest first — and every cut weida makes, by budget, queue, cap or supersession,
removes a suffix of it. So whatever weida delivers is a prefix the decoder can use, without weida
ever knowing what decodes it.

### 4.2 The segment is an L0 unit; RADIO is its fan-out

One mechanism serves two senders:

- **The machinery is shared.** Numbering per `(sender, path, topic)` in DATA key `13` (the
  sender of a `Peer::segment` is its connection, §4.11),
  supersession with §4.5's finish grace, a sender `max_age`, the `write` that never waits, §4.4's
  priority, §4.3's layers and the drop counters live with the segment, not with the radio. The
  draft PR's `SegmentTopics` and its shared `segment_copy` are kept as the seam.
- **`Radio::segment`** makes one copy per matching dish, as today. **`Peer::segment`** makes one
  copy toward the bound path the peer dialled, on the connection `Peer::open`
  (`stream.rs:501`) would pick, and waits for a peer exactly as `Peer::open` does.
- **The bound side receives `Incoming::Stream`** with `meta.topic`, `meta.segment` and
  `meta.layer` set, and authorizes it on `(proved peer, path)` as any stream
  ([0015](0015-peer-authorization.md)).
- **`Peer::segment`'s byte budget is `Limits::subscriber_buffer_bytes` per `Peer`**, over all
  its segments, as in the draft PR; a radio's stays per dish.
- **Expiry.** A radio copy expires at the smaller of the sender's `max_age` and the dish's —
  MOQT's "smaller non-zero value" (§2.4); a `Peer::segment` copy at the sender's.
- **Dropping an unfinished `Segment`** resets its unfinished layers, as dropping one resets its
  copies today, so no receiver mistakes a partial layer for a whole one.

The surface, as a sketch rather than a signature:

```rust
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct SegmentTerms {
    pub max_age: Option<Duration>,
    pub priority: i16,
}
impl SegmentTerms {
    pub fn with_max_age(self, max_age: Duration) -> Self;
    pub fn with_priority(self, priority: i16) -> Self;
}
impl Radio {
    pub fn segment(&self, topic: &str, terms: SegmentTerms) -> Result<Segment, Error>;
}
impl Peer {
    pub async fn segment(&self, topic: &str, terms: SegmentTerms) -> Result<Segment, Error>;
    pub fn segment_drops(&self, topic: &str) -> Option<TopicDrops>;
}
impl Segment {
    pub fn write(&mut self, chunk: impl Into<Bytes>) -> Result<usize, Error>; // layer 0
    pub fn write_layer(&mut self, layer: u8, chunk: impl Into<Bytes>) -> Result<usize, Error>;
    pub fn finish_layer(&mut self, layer: u8);
    pub fn finish(self) -> usize; // every layer
}
```

### 4.3 Layers

- **A stream segment has layers `0..=15`** (`MAX_SEGMENT_LAYERS = 16`, enough for `L3T3`'s nine
  linearised layers with room). Layer *k* may depend on layers below *k* and on its own earlier
  bytes, never on a higher layer. A segment that only ever writes layer 0 is today's segment.
- **Each `(copy, layer)` is its own uni DATA stream**, opened lazily on that layer's first chunk,
  with its own writer task and its own `COPY_QUEUE` of 64 chunks, so a flow-control-blocked upper
  layer never delays a lower one. The byte budget stays per copy — per dish at a radio, per
  `Peer` for `Peer::segment`.
- **The layer travels in new DATA key `14` `layer`**: `uint`, cap 15, absent means `0`, written
  only together with key `13`. A value above 15, or key `14` without key `13`, is a
  `PROTOCOL_VIOLATION`.
- **The cut rule.** A copy without room for a chunk of layer *k* first cuts its layers above
  *k*, whose queued bytes return to the copy's budget at once; if there is still no room — in the
  budget, or in layer *k*'s own queue — it cuts layers *k* and above. Cutting a layer resets its
  stream with `CANCELED`, including a layer already finished and not yet acknowledged, and later
  chunks of a cut layer for that copy are not sent. If *k* is 0 the copy loses the whole segment,
  exactly as today. A cut above layer 0 counts once per copy and segment in the new
  `TopicDrops::layers_cut`; a whole-segment loss keeps its existing cause. The first step is what
  keeps a base layer from being starved by the enhancement layers queued ahead of it, and it
  needs the segment side to know each layer's queued bytes, which is B-308's to build.
- **The dish's cap.** A dish states `max_layer` when it joins, in new **SUBSCRIBE key `3`
  `max_layer`** (`uint`, cap 15, absent means no cap, meaningful on a RADIO path only, a value
  above 15 a `PROTOCOL_VIOLATION`). Layers above it are never opened for that dish, and that is
  not a drop. When several of the dish's matching filters carry caps, the **largest** applies,
  and a matching filter without a cap means no cap — the opposite of `max_age`'s smallest-wins,
  because a cap limits what a dish asked for and a second filter asking for more is a request
  for more. Joining a filter again updates its cap, as it updates `max_age` today.
- **`Dish::join(filter, max_age)` becomes `Dish::join(filter, JoinTerms)`**, a clean cutover of
  every caller:

  ```rust
  #[non_exhaustive]
  #[derive(Clone, Debug, Default)]
  pub struct JoinTerms {
      pub max_age: Option<Duration>,
      pub max_layer: Option<u8>,
  }
  impl JoinTerms {
      pub fn with_max_age(self, max_age: Duration) -> Self;
      pub fn with_max_layer(self, max_layer: u8) -> Self;
  }
  ```

- **Dish freshness becomes per `(topic, segment, layer)`** (kept per connection and checked by
  every receiver, §4.11). An arrival is fresh if its segment is
  newer than the newest delivered on its topic, or equal to it with that layer not yet delivered;
  anything else is stale and counted in `stale()`. The memory per topic grows from one number to
  a number and a 16-bit mask, and stays bounded by the same `max_topics`.
- **`IncomingMeta::layer: Option<u8>`** is `Some` exactly when `segment` is `Some`, and `Some(0)`
  when key `14` is absent. Each layer reaches the dish as its own `Received::Segment`; merging
  the layers of one segment into frames is the application's.
- **Datagram segments have no layers.** A one-packet unit has nothing to cut.
- **`TopicDrops` and `IncomingMeta` gain a field**, which breaks exhaustive patterns over them in
  `0.x`; `TopicDrops` becomes `#[non_exhaustive]` in the same change so the next cause does not
  break it again.

### 4.4 Priority: layer first, then the application's

- **The `quinn` priority of a copy's layer stream is `(15 - layer) as i32 * 65_536 +
  terms.priority as i32`**, set when the stream opens, which is the only time `quinn` guarantees
  it takes effect (§2.3). That is layer-major: every topic's base layer on a connection goes
  before any topic's enhancement layer, and the application's `i16` orders topics within a layer.
  At most sixteen levels per connection times the application's few values, which keeps
  `quinn`'s "many different priority levels" warning in view.
- **Datagrams outrank all of it**, by `quinn`'s packing ([0034](0034-late-is-lost.md) §2.3), so
  voice and a share's sound need no priority of their own.
- **The rule to state, because nothing can enforce it: put a session's voice flows and video
  segments on one path** — the uplink `Peer` that carries the voice flow also carries
  `Peer::segment`, and the downlink radio carries both the voice datagrams and the video
  segments. Across paths no priority exists ([0002](0002-control-and-bulk-separation.md) §6.2),
  and bulk that shares the bottleneck runs BBR (B-289, 0034 §6).
- **A superseded copy keeps its priority** while its grace runs: the older segment's tail has the
  earlier playout deadline.
- **Locally, priority is a no-op**, as `set_priority` already documents.

### 4.5 Supersession, amended

Opening segment *n+1* on a topic, for each copy of segment *n*:

- a layer stream whose writer is behind — not yet finished by the sender, or holding queued
  chunks the transport does not take at once — is reset at once, **and so is every layer above
  it** (§4.3's cut rule);
- a layer stream that was finished and handed to the transport gets a grace of `rtt + 50 ms +
  rtt * bytes_written / cwnd`, with `bytes_written` the copy's total over its layers and the path
  from `Link::transport_stats()`, and is reset only if it is still unacknowledged when the grace
  ends (`stopped()` resolving first means it was acknowledged);
- on a local transport, which reports no path, the grace is zero.

This is the draft PR's rule, credited to it with its probe numbers (§2.2), extended to layers.
It amends [0034](0034-late-is-lost.md) §4.6's supersession row, "resets every copy of segment *n*
on that topic that is still unacknowledged", which is exactly what lost the healthy dish its
GOPs. Rule 1 of 0034 §4.6 stands: supersession still holds nothing, it only waits for bytes that
are already the transport's.

**Amended 2026-10-22, by the owner's choice.** The grace is the larger of the rule above and
what the copy's connection has taught: every finished copy acknowledged whole is one sample of
its time from FIN to acknowledgement, and the connection keeps a smoothed mean and mean
deviation of those samples as RFC 6298 does for a retransmission timeout (gains 1/8 and 1/4); a
finished copy is reset no earlier than its FIN plus the mean plus four deviations (at least
1 ms, RFC 9002's `kGranularity`). The fixed 50 ms lost a healthy copy on a loaded Windows host
whose acknowledgements scattered more than that around the smoothed round trip (nightly #21,
`a_healthy_dish_behind_a_slow_path_gets_back_to_back_segments_whole`: grace 346 ms at an `rtt`
of 212 ms, no loss). `quinn` 0.11 exposes no RTT variance, so the samples are weida's own; they
measure exactly the wait the grace covers, queueing, carry, round trip and ack delay together.
A copy reset at its grace is not a sample, so a connection that stops acknowledging cannot
stretch the grace (`SettleTime` in `crates/weida/src/segment.rs`).

### 4.6 Signals for adaptation; weida decides nothing about bitrate

- **The sender** reads `Peer::connection_stats()` (0036) for its uplink and
  `Peer::segment_drops(topic)`, including `layers_cut`, for what its own segments lost.
- **A viewer** reads `Dish::connection_stats()`, `ConnectionStats::remote` once B-302 ships,
  gaps in the segment numbers, reset layer streams, and `stale()`/`overflow()`.
- **A radio** gains `Radio::dish_drops() -> Vec<DishDrops>`, one record per joined dish
  connection, summed over topics and removed when that connection closes:

  ```rust
  #[non_exhaustive]
  #[derive(Clone, Debug)]
  pub struct DishDrops {
      pub peer: Option<PeerIdentity>,
      pub subscriber_budget: u64,
      pub subscriber_queue: u64,
      pub superseded: u64,
      pub expired: u64,
      pub too_large: u64,
      pub no_datagrams: u64,
      pub layers_cut: u64,
  }
  ```

  It is bounded by `max_connections`, and answers griasdi's gap 4: the server tells a viewer on
  its own control channel that it is losing enhancement layers, or stops relaying a layer no
  viewer can take.
- **The effect on griasdi's encoder**: it no longer has to step down for its slowest viewer, and
  so no longer rebuilds for that reason. The radio sheds upper layers per viewer, a viewer lowers
  its `max_layer`, and the encoder's target follows the uplink only.

### 4.7 What stays the application's

- **Keyframe and join-point requests.** A joining viewer waits for the next segment
  ([0034](0034-late-is-lost.md) §4.6 rule 2) or asks the sender for one over an exchange;
  `Radio::with_admission` sees every join and is the trigger point on a relay.
- **The codec-to-layer mapping, chunk framing and timestamps, merging the layer streams of one
  segment back into frames, SFrame, lip sync, and bitrate.**
- **Simulcast** — independent encodings per resolution — is one topic per resolution, expressible
  today without anything in this note.

### 4.8 The architectures, expressed

**griasdi sharer.** One `/voice/up` `Peer` carries the voice flow, the share's sound flow and the
video. Each keyframe opens `peer.segment(&topic, SegmentTerms::default().with_max_age(..))`, and
each encoded frame goes to `write_layer(layer_of(frame), frame)`.

**The server relay.** The per-layer upstream streams of one segment arrive as separate
`Incoming::Stream`s with the same topic and number, and map onto one radio `Segment`: a number
newer than the last one relayed on that topic opens a new radio segment that follows upstream
(§4.11) and keeps the old one until the old one's upstream layers have ended; each upstream
chunk of layer *k* goes to
`write_layer(k, chunk)`; upstream EOF on layer *k* becomes `finish_layer(k)`, called only once
every lower layer is finished, so a relay never finishes a layer whose base it is about to lose.
The relay never calls `finish()` on a segment with an upstream layer that was reset; it drops it.

**Viewer.** `dish.join(&filter, JoinTerms::default().with_max_age(..).with_max_layer(1))`, and a
join again with a lower cap when its link degrades. It decodes the longest whole prefix of layers
it received for a segment.

**Mappings.** For `L1T3` the three temporal layers T0, T1, T2 are layers 0, 1 and 2 — what W3C
states is the three temporal layers and their dependency diagram (§9.3). That cutting layer 2
halves the frame rate, and cutting 1 and 2 quarters it, assumes the usual dyadic structure
[INFERENCE: not read from the diagram]. For `L3T3` a spatial-major linearisation — S0T0, S0T1,
S0T2, S1T0, … S2T2 as layers 0 to 8 — is a topological order of W3C's stated structure (three
spatial layers with inter-layer dependency, three temporal layers); a temporal-major order is one
too, and which ladder serves a viewer better is the application's choice. **Whether `nvav1enc`
or `svtav1enc` in the GStreamer version griasdi uses emits SVC with usable temporal or spatial
ids is unverified**; `svtav1enc`'s `hierarchical-levels=2` is a hierarchical mini-GOP of four
frames, which is not the same claim.

**zeughaus.** Its preview tiers become layers of one segment once they are compressed, so a
viewer's tier is a cap rather than a topic.

### 4.9 The local transports carry the same contract

Priority is a no-op, the finish grace is zero, and each layer stream is its own OS connection
([0010](0010-local-transport.md)), as every copy is today. The cut rule and the cap are
unchanged.

### 4.10 Compatibility

- DATA key `14` and SUBSCRIBE key `3` are additive and skipped by an older decoder
  ([PROTOCOL.md](../PROTOCOL.md) §5). No capability code is needed.
- An older radio ignores `max_layer` and sends every layer the sender wrote.
- An older dish does not know key `14`: it delivers whichever stream of a segment arrives first
  and discards the others as stale. So **layered topics need both sides at this version**;
  single-layer topics are unchanged on the wire, because key `14` is never written for layer 0.
- An older acceptor receiving `Peer::segment` sees each layer as a separate stream with the same
  topic and segment number, which is what it would see from a relay forwarding them.

### 4.11 Amendment: numbers and freshness per connection, and a copy that follows upstream

Accepted by the owner on 2026-10-04, after tuco86/weida#2 grew three commits during the build
(5cbba1a, 2ebcbd9, a1be629). The pull request is closed and nothing of it is merged; each commit
named a loss the sections above did not see, and the shapes below are this note's.

- **`Peer::segment` numbers per connection.** §4.2 made the sender of a `Peer::segment` the
  `Peer`. Two `Peer`s the pool hands one connection, or a `Peer` rebuilt on a connection that
  stayed open, restart a sequence the receiver still remembers, and every segment below the old
  newest is discarded. The number is taken per `(path, topic)` from the dialling connection
  (`ConnCtx::segments_out`), shared by every `Peer` on it; each `Peer` still supersedes only its
  own copies. The table holds at most `max_sequence_scopes` entries; at the cap it evicts one,
  and a key that returns resumes above every number an evicted key reached, so a receiver never
  reads it as stale. A radio keeps numbering per `(radio path, topic)`.
- **Freshness is per connection, and every receiver checks it.** §4.3's table lived as long as
  the dish. A dish that redials a restarted radio, which numbers from 0 again, discarded every
  segment until the new numbers overtook the old newest. The newest segment and §4.3's 16-bit
  layer mask are kept per `(path, topic)` on the receiving connection (`ConnCtx::segments_in`), so
  a redial starts over and no incarnation id is needed: every radio restart is a new connection.
  The same table guards every other route: the dispatcher refuses a stale stream segment with
  `CANCELED` before it reaches an acceptor, a transfer endpoint or a pair, which keeps a relay's
  upstream as clean as a dish. A dish still counts its own in `stale()`. Datagram segments are
  checked against the same table at layer 0. At most `max_sequence_scopes` entries per
  connection; at the cap an untracked key is fresh, as before.
- **A copy can follow upstream.** §4.8's relay opens segment *n+1* the moment upstream does,
  while the tail of *n* is still arriving, so under §4.5 the successor reset every copy of *n*
  still being written: every viewer behind a relay lost each segment's tail. `SegmentTerms` gains
  `follows_upstream: bool` (`with_follows_upstream`). Supersession takes a layer of such a copy
  only where a write has to wait, which is §4.5's stalled-dish rule; otherwise the layer keeps
  taking chunks, a layer may still open after the successor did, and a finished layer gets §4.5's
  grace. Dropping the `Segment` unfinished still resets every unfinished layer, which is what a
  relay does when an upstream layer is reset. It applies to `Radio::segment` and `Peer::segment`
  alike; §4.8's relay sets it.
- **The items.** B-307 takes the numbering and `follows_upstream`; a new B-314 takes freshness
  per connection; B-308 puts the layer mask into that table and replaces its two-radio freshness
  test, because two paths are two keys; B-310's relay follows upstream; B-311 exposes the term in
  both Python surfaces; B-312 documents all three. The rest of the pull request (8d35ed8,
  9bcb0e0, a0bcf4f) is not taken.

## 5. Consequences and follow-ups

Documents, once the owner accepts the note (B-312): [PATTERNS.md](../PATTERNS.md) §1.12 (priority
of segment copies), §5 (`Peer::segment`), §6.4 (layers, the cap, the amended supersession, the
signals) and "Choosing"; [GUARANTEES.md](../GUARANTEES.md) §6 (a row for `Peer::segment`,
`BestEffort`); [PROTOCOL.md](../PROTOCOL.md) DATA keys `13` and `14`, SUBSCRIBE key `3`, and
golden vectors; [INVARIANTS.md](../INVARIANTS.md) (at most sixteen layer streams per copy, and
`dish_drops` bounded by `max_connections`); 0034 §4.6 and §6 gain "**Amended by 0037**" lines; the
requirement document points at what shipped.

The draft PR is taken apart rather than merged: its two radio fixes and their tests become B-304,
its `Peer::segment` becomes B-307, and its Windows `current_account_sid` change is outside this
note. Backlog:

### B-304 — Supersession that keeps finished copies whole
kind: code | size: 60 | status: ready | needs: []
acceptance: [0037](0037-layered-segments.md) §4.5 in `crates/weida/src/radio.rs`, taken from tuco86/weida#2's two radio commits (`7ce78c2`, `402afd3`) and rebased on main with the grace read from `Link::transport_stats().map(|t| t.path)`. The PR's tests pass in `crates/weida/tests/radio.rs` with `common::delay_proxy`: `a_healthy_dish_behind_a_slow_path_gets_back_to_back_segments_whole` and `a_segment_finished_right_before_its_successor_still_arrives_whole`; `a_stalled_dish_loses_old_segments_while_a_fast_one_gets_every_one` still passes.

### B-305 — PROTOCOL: DATA key 14 `layer`, SUBSCRIBE key 3 `max_layer`
kind: spec | size: 45 | status: ready | needs: []
acceptance: [PROTOCOL.md](../PROTOCOL.md) §6.2 and §6.4 rows per [0037](0037-layered-segments.md) §4.3; key `13`'s row says "written by a radio or by `Peer::segment`"; golden vectors for a segment DATA header with `layer = 2` and a SUBSCRIBE with `max_layer = 1`; the violations for a value above 15 and for key `14` without key `13`.

### B-306 — The codec for `layer` and `max_layer`
kind: code | size: 45 | status: ready | needs: [B-305]
acceptance: `weida-protocol` encodes and decodes both keys against B-305's vectors; a value above 15, and key `14` without key `13`, is a `PROTOCOL_VIOLATION`; the `roundtrip` fuzz target is extended.

### B-307 — The segment as an L0 unit: `SegmentTerms`, `Peer::segment`, priority
kind: code | size: 90 | status: ready | needs: [B-304]
acceptance: [0037](0037-layered-segments.md) §4.2, §4.4 and §4.11: `Radio::segment(topic, SegmentTerms)` and `Peer::segment(topic, SegmentTerms)` share one copy machinery, and `Peer::segment_drops` exists; copy streams get the §4.4 priority at open; a sender `max_age` applies, at a radio together with the dish's, the smaller winning; a `Peer::segment` takes its number per `(path, topic)` from its connection (`ConnCtx::segments_out`, at most `max_sequence_scopes` entries); `SegmentTerms::follows_upstream` keeps a copy against its successor unless a write has to wait; every `Radio::segment` caller is migrated. Tests: tuco86/weida#2's `a_peer_segment_reaches_an_acceptor_with_its_number` and `a_peer_segment_supersedes_the_previous_one_on_its_topic`, `two_peers_on_one_connection_number_one_sequence`, `a_segment_following_upstream_keeps_its_copy_while_its_successor_opens` and `a_segment_following_upstream_still_resets_a_stalled_dish`. No priority test, because it would test `quinn`'s scheduler, as B-284 argued.

### B-314 — Segment freshness per connection
kind: code | size: 45 | status: ready | needs: [B-307]
acceptance: [0037](0037-layered-segments.md) §4.11: `ConnCtx::segments_in` keeps the newest segment per `(path, topic)`, at most `max_sequence_scopes` entries; a dish checks stream and datagram segments against it and keeps only its counters; the dispatcher refuses a stale stream segment with `CANCELED` before it reaches an acceptor, a transfer endpoint or a pair. Tests in `crates/weida/tests/radio.rs`: `a_dish_redialled_to_a_restarted_radio_takes_its_numbers_from_zero` (with `common::Restartable`, moved from `tests/stats.rs`) and `an_acceptor_refuses_a_segment_older_than_one_it_delivered`.

### B-308 — Layers inside a segment
kind: code | size: 90 | status: ready | needs: [B-306, B-307, B-314]
acceptance: [0037](0037-layered-segments.md) §4.3 and §4.11: `write_layer`, `finish_layer`, lazy per-`(copy, layer)` streams with their own queues, the cut rule including the release of upper layers' queued bytes, `TopicDrops::layers_cut` with `TopicDrops` made `#[non_exhaustive]`, `JoinTerms` replacing `join`'s `max_age` parameter at every caller, the largest-cap rule, per-layer freshness in B-314's per-connection table, and `IncomingMeta::layer`. Tests in `crates/weida/tests/radio.rs`: `a_dish_short_of_budget_keeps_layer_zero_whole_while_upper_layers_are_cut`, `a_dish_capped_at_layer_zero_is_never_sent_layer_one`, `a_layer_cut_also_cuts_every_higher_layer_of_that_segment`, `a_receiver_delivers_each_layer_of_a_segment_once`, `a_peer_segment_carries_its_layers_to_the_acceptor`.

### B-309 — Per-dish drops at a radio
kind: code | size: 45 | status: ready | needs: [B-308]
acceptance: [0037](0037-layered-segments.md) §4.6: `Radio::dish_drops() -> Vec<DishDrops>`, one record per joined dish connection, removed when it closes. Test: two dishes, one short of budget; only its record counts `subscriber_budget` or `layers_cut`, and the record is gone after it disconnects.

### B-310 — A layered relay as a program, and a guide section
kind: code | size: 60 | status: ready | needs: [B-308]
acceptance: an example in B-290's shape — a `Peer::segment` uplink writing three layers, an acceptor, a `Radio::segment` that follows upstream per [0037](0037-layered-segments.md) §4.8 and §4.11 — asserted by a test: a dish with `max_layer = 0` receives layer 0 of every segment and nothing else, and an uncapped dish receives all three; a [GUIDE.md](../GUIDE.md) section "quality without re-encoding".

### B-311 — Layered segments in `weida::blocking` and `weida-py`
kind: code | size: 60 | status: ready | needs: [B-308, B-309]
acceptance: the `weida::blocking` twins of `SegmentTerms` (with `follows_upstream`), `JoinTerms`, `write_layer`, `finish_layer` and `dish_drops`, and both Python surfaces; one layered round trip per Python surface.

### B-312 — Documents for 0037
kind: spec | size: 45 | status: ready | needs: [B-304, B-307, B-308, B-309, B-314]
acceptance: §5's edits, and §4.11's three rules in PATTERNS §6.4, GUARANTEES §6 and INVARIANTS (`segments_out` and `segments_in` per connection, each bounded by `max_sequence_scopes`); no passage outside `decisions/`, `research/`, BACKLOG and NIGHTLOG still says key `13` is written by a radio only, or that a dialling side cannot send a segment; [requirements/griasdi-video.md](../requirements/griasdi-video.md) points at what shipped.

## 6. What this note does not decide

- **Anything codec-aware.** No OBU, NAL unit or dependency descriptor is parsed anywhere in
  weida, and no codec crate is a dependency.
- **Forward error correction.** A layer is delivered whole or reset; repair inside it is the
  application's, as 0034 §6 left it.
- **Partial reliability inside a layer.** A layer stream is reliable until cut; dropping single
  frames inside it would need a framing weida does not have.
- **Whether media wants its own congestion controller.** B-289's answer stands: bulk that shares
  a bottleneck with media runs `Congestion::Bbr`; media itself keeps the default.
- **DSCP marking**, for 0034 §6's reasons.
- **A connecting radio.** Still deferred: the upstream direction is `Peer::segment`.
- **Interoperating with MOQT.** 0034 §6 stands; a layer maps onto a subgroup, a segment onto a
  group.
- **RTT variation.** [0036](0036-connection-statistics.md) §4.7 item 1 stands.

## 7. Sources

weida documents: [PATTERNS.md](../PATTERNS.md) §1.12, §5, §6.4; [GUARANTEES.md](../GUARANTEES.md)
§6; [PROTOCOL.md](../PROTOCOL.md) §5, §6.2, §6.4, §11 (lines 470, 533, 709-713);
[INVARIANTS.md](../INVARIANTS.md); decisions 0002, 0010, 0011, 0015, 0016, 0024, 0031, 0034
(§2.3, §2.4, §3, §4.6, §6), 0035 (§4.3), 0036 (§4.5, §4.7);
[requirements/griasdi-video.md](../requirements/griasdi-video.md);
[research/prior-art.md](../research/prior-art.md) lines 553-562 and 630-652.

Code read at `05a6b8e`: `crates/weida/src/radio.rs:50, 62, 441-488, 603-619, 634-705, 933-936,
957-1006, 1103, 1112-1130`; `crates/weida/src/transfer.rs:146-214, 434`;
`crates/weida/src/pubsub.rs:121-151`; `crates/weida/src/stream.rs:474, 501`;
`crates/weida/src/transport.rs:389, 411-415`; `crates/weida/src/flow.rs:928`; `quinn` 0.11.12
`src/send_stream.rs:213-220`.

Pull request: tuco86/weida#2 (draft, head `5e6ee45`; commits `7ce78c2`, `1e7eb9e`, `402afd3`,
`5e6ee45`), its description and diff, read 2026-10-04.

griasdi: `docs/proposals/weida-media-transport.md` at `65bb85f`; branch `screen-sharing` at
`75e7edd`: `crates/griasdi-screen/src/encoder.rs` (`GOP_SECONDS`, `software_properties`),
`crates/griasdi-client/src/share/video.rs`, `crates/griasdi-server/src/screen.rs`,
`SCREEN-SHARING-OFFEN.md`.

External, read 2026-10-04: W3C, "Scalable Video Coding (SVC) Extension for WebRTC", Working Draft
14 September 2026, §5 and §9, <https://www.w3.org/TR/webrtc-svc/>; Garcia Murillo and
Gouaillard, "Codec agnostic RTP payload format for video",
draft-codec-agnostic-rtp-payload-format-00, February 2021,
<https://datatracker.ietf.org/doc/html/draft-codec-agnostic-rtp-payload-format-00>.
