# 0002 — Control and bulk traffic — separation against head-of-line coupling

- **Status:** accepted
- **Date:** 2026-09-10 (drafted), 2026-09-10 (accepted)
- **Related:** SYNTHESIS §8.2; P4, P12; decision 0001 §7.8

## 1. The question

weida's connection window is shared, so "one slow reader stalls every writer on the connection"
([PATTERNS.md](../PATTERNS.md) §1.3). SYNTHESIS §8.2 states the open decision: MQTT forbids
exactly this for its own control traffic — acknowledgements, subscriptions and pings "never
stall behind a blocked publish path" [mqtt5 §5] — and asks "whether control and bulk traffic
must be separated (separate connections, or a reserved window)", noting that it "becomes
load-bearing for any adapter that multiplexes many foreign sessions onto one weida connection"
[SYNTHESIS §8.2].

Decision [0001](0001-sequence-field.md) sharpens it twice. HELLO now carries guarantee
declarations validated against the intersection [0001 §7.8], so HELLO is control traffic whose
delay is a connection failure: a side "MUST NOT process any frame other than HELLO before it
has processed the peer's HELLO frame", and a peer HELLO missing `hello_timeout` (10 s) closes
the connection with `NEGOTIATION_FAILED` ([PROTOCOL.md](../PROTOCOL.md) §2.2). And
detect/reassemble ordering plus the bounded dedup window add receiver state whose lifetime is
coupled to when the application reads [0001 §7.5], [0001 §7.6] — withheld reads being exactly
what consumes the shared connection window [quic-standards §3.2]. Sections 2-5 lay out the
options with their sourced costs as they were drafted; §6 records the decision taken on them.

## 2. What couples today

**The two windows behave differently, and it is measured.** Per stream, "a stream nobody reads
does not delay its siblings"; per connection, "the connection window is shared by every stream
on the connection. Unread streams consume it; when it is spent, every writer on the connection
blocks until the slow reader consumes at least an eighth of the window" [PATTERNS §1.3]. The
named test pins it: at a 64 KiB stream window, 256 KiB connection window and 32 KiB per
stream, "the seventh unread stream stalls the connection at 229376 bytes; reading the first
one releases it" (`a_stalled_stream_does_not_block_its_siblings`) [PATTERNS §1.3].

**At shipped defaults the ratio is 16.** `stream_receive_window` is 1 MiB and
`connection_receive_window` is 16 MiB [PROTOCOL §10], so sixteen unread full-window transfers
exhaust the connection's credit for everything else, control frames included; the header
spends the same window, and "the last eighth only clears once the application reads"
[PATTERNS §1.3].

**The stream budget is a second shared resource, and it binds at `open`.** A peer grants
`max_concurrent_uni_streams` and `max_concurrent_bidi_streams`, 1024 each by default
[PROTOCOL §10]; a transfer holds its stream "until it has been read to EOF, dropped or
refused — including while it sits in a bounded accept queue", and at the bound "the next
`open` waits. It does not fail" [PATTERNS §1.4]. QUIC's limit is cumulative and monotonic,
raised only by MAX_STREAMS [quic-standards §2.8].

**QUIC puts the release of credit on the receiver, and warns what happens if it is late.**
The limits are absolute byte offsets: MAX_STREAM_DATA per stream, MAX_DATA as "the maximum of
the sum of the absolute byte offsets of all streams" [quic-standards §3.1]. "A receiver MUST
NOT wait for a STREAM_DATA_BLOCKED or DATA_BLOCKED frame before sending a MAX_STREAM_DATA or
MAX_DATA frame; doing so could result in the sender being blocked for the rest of the
connection" [quic-standards §3.2]. Two of RFC 9308's three deadlock shapes are this coupling:
a message larger than available credit, "possible even where stream flow control limits are
not reached because connection flow control limits can be consumed by other streams", and
interdependent streams, where A is unread pending B while B is blocked by A's withheld credit
[quic-standards §3.3].

**Control frames ride the same two resources as bulk.** Every weida control message is an
ordinary stream on the ordinary connection: HELLO is "exactly one uni stream carrying a single
HELLO frame" opened right after the handshake [PROTOCOL §2.2]; SUBSCRIBE and UNSUBSCRIBE are
"short header-only unidirectional streams", ERROR rides the reply half of its exchange, and
"there is no multiplexed control stream anywhere in the implementation"
([INVARIANTS.md](../INVARIANTS.md)); a withdrawn subscription first crosses the connection
driver's bounded `mpsc` of depth 1024 ([ARCHITECTURE.md](../ARCHITECTURE.md) §5). An exhausted
window or stream budget therefore delays negotiation, interest, and the release of filters at
the publisher alike.

**A parked stream holds resources while it waits for control.** "Receiving DATA before HELLO
is **not** a protocol violation. The receiver MUST park such a stream (retain it, unread
beyond its preamble …) until the peer HELLO has been processed" [PROTOCOL §2.2] — so it holds
stream budget and connection credit while awaiting the very control frame that would release
it, under a 10 s deadline [PROTOCOL §2.4].

**What 0001 adds to the coupling.** HELLO gains guarantee-declaration fields and an
intersection check [0001 §7.8], making the HELLO stream's timeliness a correctness property.
Reassembly holds messages back "up to a bounded buffer" [0001 §7.5]; whether that buffer is
filled by reading eagerly into application memory (releasing window, paying memory — RFC
9308's mitigation, whose caveat is that "the receiver then needs another way to hold the peer
accountable for that memory" [quic-standards §3.3]) or by leaving streams unread (paying
window) is unstated by 0001 and is a consequence of this decision. The dedup window is
receiver state with a time bound [0001 §7.6] and holds no transport credit.

**Pub/Sub already answers overload by dropping, but not at connection scope.**
`subscriber_buffer_bytes` (8 MiB) bounds payload queued *at the publisher* per subscriber, and
a copy that does not fit is dropped for that subscriber alone so that "a slow consumer
therefore cannot stall a publisher or its other subscribers" [PROTOCOL §9.5], [PATTERNS §4].
The copies that are sent still travel the one shared connection window [PATTERNS §1.3].

**QUIC offers no wire-level help.** "Stream prioritization is not exposed to either the
network or the receiver" [quic-standards §12 item 3], and stream identity is inside the
encryption boundary, so differentiated network treatment "needs separate connections and
separate DSCPs" [quic-standards §12 item 13]. Priority is a *local scheduling* input that
RFC 9000 only SHOULD-level requires an API for [quic-standards §2.3].

## 3. Options

### Option A — accept the coupling; document it; adapters use one connection per foreign session

Change nothing on the wire. The shared window stays, the coupling stays documented
[PATTERNS §1.3], and an adapter that must honour a foreign no-stall rule opens one weida
connection per foreign session — the MQTT chain's own conclusion: "the bridge therefore needs
one weida connection per MQTT session, or separate connections for control and data"
[SYNTHESIS §7.2].

- **What it fixes:** nothing in the core; the fix moves into adapter deployment.
- **Cost:** zero protocol change, zero new state. Adapters pay a handshake, a pool entry and a
  fingerprint check per foreign session, since the pool keys on
  `(host, port, ClientTls, Option<Fingerprint>)` and sessions sharing terms would otherwise
  share one connection [ARCHITECTURE §5].
- **Invariants:** untouched; no new remote-influenced allocation appears [INVARIANTS].
- **Precedent:** Kafka accepts the analogous property at protocol level — the broker "allows
  only a single in-flight request per connection in order to guarantee this ordering"
  [prior-art §8 lesson 2]; NATS core answers a slow consumer by disconnecting it [SYNTHESIS P4].
- **Named loss:** weida can never promise MQTT's rule that acknowledgements, subscriptions and
  pings "never stall behind a blocked publish path" [mqtt5 §5], so no adapter may present it
  on a shared connection [INVARIANTS]. HELLO — now load-bearing for guarantee negotiation
  [0001 §7.8] — stays behind bulk, with `hello_timeout` as the only backstop [PROTOCOL §2.2].

### Option B — two QUIC connections per peer pair: control and bulk (Zenoh-style ALPN split)

Every peer pair holds two connections with separate windows and separate stream budgets: one
carrying HELLO, SUBSCRIBE/UNSUBSCRIBE and future control frames, one carrying DATA. Zenoh does
the same split inside one connection with ALPN-selected stream classes (`zenoh`, `zenoh-mr`,
`zenoh-ms`, `zenoh-ms-mr`), multi-stream meaning "one bidirectional stream for
`Priority::Control` plus seven unidirectional streams opened in strict order"
[prior-art §1 message-to-transport mapping]; this option is that split with hard isolation.

- **What it fixes:** control starvation entirely — a bulk-saturated window cannot consume the
  control connection's MAX_DATA, since flow control is per connection [quic-standards §3.1].
- **Cost:** two handshakes, two pool entries and two fingerprint checks per peer pair, plus
  two idle timers and two keep-alive schedules, which only the dialling side sends
  [ARCHITECTURE §5], [PATTERNS §1.8]. Both connections must be proved to be the *same* peer,
  which the pool key expresses per connection but does not tie together [ARCHITECTURE §5].
  EMQX reports the direction of the cost: QUIC "reconnection consumes more bandwidth than
  TLS" [prior-art §3 reported problems].
- **Invariants:** "one data flow maps naturally to one transport stream" is unaffected;
  "transfer-related control messages do not require a permanent control stream" [INVARIANTS]
  is strained if the control connection becomes a permanent channel by another name.
- **Precedent:** Zenoh's priority-to-stream mapping with class 0 reserved for `Control`
  [prior-art §1]; EMQX multi-stream, where "the initial stream … is referred to as the control
  stream" [prior-art §3 message-to-transport mapping]; MOQT's "pair of unidirectional streams
  for creating the session and exchanging control messages" [prior-art §4].
- **Named loss:** no bulk-to-bulk isolation — a slow DATA reader still stalls every other DATA
  writer [PATTERNS §1.3] — and per-peer state doubles, which is the resource the stream
  commitment attack targets from the other side [quic-standards §2.8].

### Option C — one connection, stream classes with local priority and a reserved control budget

Keep one connection; classify streams (control, bulk), give control streams a local scheduling
priority — Zenoh's call is `set_priority(-(prio as i32))`, inverted because "QUIC stream
priority semantics (P0 < P1 < P2) are the opposite of Zenoh's" [prior-art §1] — and reserve a
slice of the connection window and the stream budget that bulk may not consume.

- **What it fixes:** control frames are scheduled first locally and cannot be starved of
  credit, which is MOQT's rule made explicit: "endpoints MUST allocate connection flow control
  to the control streams before allocating it to any data streams. Otherwise, a receiver might
  wait for a control message … while the sender waits for flow control to send the message"
  [prior-art §4 flow control], [prior-art §8 lesson 8].
- **Cost:** the reservation splits `connection_receive_window` and a share of
  `max_concurrent_uni_streams` [PROTOCOL §10] into accounts bulk cannot use even when control
  is idle. Priority is local and unenforceable on the wire [quic-standards §12 item 3], and
  MOQT concedes it "is implementation dependent and is constrained by the prioritization
  interface of the underlying transport" [prior-art §4 delivery guarantees], [prior-art §8
  lesson 7]; quinn's windows are fixed with no auto-tuning, so the split is static
  [quic-standards §13].
- **Invariants:** the reserved control window is a new named allocation for "no remote input
  can cause unbounded memory allocation", and "disabled guarantees should not participate in
  the hot path" argues the classification must add no branch to the payload path [INVARIANTS].
- **Precedent:** MOQT's control-first allocation rule [prior-art §4]; EMQX, which "gives the
  control stream the highest priority so `PINGREQ` survives a saturated data stream"
  [prior-art §8 lesson 7]; Zenoh's eight priority classes with class 0 `Control`
  [prior-art §1 flow control]; AMQP 1.0's two simultaneous levels — link credit in messages,
  session window in frames — tied together "nowhere", with "no guidance on relative sizing,
  which is where brokers differ most" [amqp10 §5.2], [amqp10 §5.3].
- **Named loss:** no different *network* treatment, since stream identity is encrypted and
  differentiation "needs separate connections and separate DSCPs" [quic-standards §12 item
  13]; and one congestion and loss-recovery domain remains, so bulk-induced tail loss delays
  control packets whatever the scheduler does [quic-standards §3.5].

### Option D — one connection, connection window sized so control never binds

Keep one connection and one scheduling class, but size the credit so the connection limit is
never the binding constraint: raise `connection_receive_window` well beyond realistic
concurrent unread payload and treat the per-stream window as the only real bound. Throughput
is flow-control-limited only while credit is below the bandwidth-delay product
[quic-standards §3.2], and quinn's own default is this extreme: connection window
`VarInt::MAX` against a 1.25 MB stream window [quic-standards §13].

- **What it fixes:** the connection window stops being the scarce resource, so control frames
  find credit and per-stream isolation [PATTERNS §1.3] becomes the operative mechanism.
- **Cost:** memory. The connection window bounds unread payload buffered per connection
  [PROTOCOL §10]; raising it raises what a hostile or merely slow peer can pin, multiplied by
  `max_connections` (1024) [PROTOCOL §10]. The stream budget stays a separate scarce resource
  this option does not touch [PATTERNS §1.4].
- **Invariants:** it directly loads "no remote input can cause unbounded memory allocation",
  whose named bounds currently include the connection window [INVARIANTS].
- **Precedent:** quinn's fixed defaults (connection `VarInt::MAX`); quiche and Google QUICHE
  auto-tune with caps of 16 MB stream / 24 MB connection; msquic ships 16 MiB connection
  against a 64 KiB stream window [quic-standards §13]. The divergence is the sheet's own
  point: receive-window policy "is an implementation property, not a QUIC property"
  [quic-standards §13.3].
- **Named loss:** isolation bought with memory and no *guarantee* — enough unread streams
  exhaust any finite window, so the coupling is postponed, and nothing stops bulk from
  consuming the stream budget control needs to open its stream at all [PATTERNS §1.4].

### Option E — per-endpoint connections (one QUIC connection per dialled path)

Make the connection granularity the endpoint path rather than the peer: each dialled
`weida://host/path` gets its own connection, so isolation follows the application's own
addressing. Today the pool keys on `(host, port, ClientTls, Option<Fingerprint>)`, so several
endpoints to one authority under the same terms deliberately share a connection
[ARCHITECTURE §5].

- **What it fixes:** cross-endpoint isolation, which no other option gives: a slow reader on
  one path cannot stall writers on another, since they share no window [PATTERNS §1.3], and
  control traffic sits only behind its own endpoint's bulk.
- **Cost:** one handshake, pool entry, fingerprint check and idle/keep-alive pair per path
  [ARCHITECTURE §5], [PATTERNS §1.8], against `max_connections` = 1024 per binding
  [PROTOCOL §10]; HELLO multiplies too, since each connection negotiates the 0001 guarantee
  declarations again [0001 §7.8], [PROTOCOL §2.3].
- **Invariants:** "endpoint paths are opaque identifiers" survives — the path is a key, not
  parsed [INVARIANTS] — but the change contradicts a pool key that deliberately excludes the
  path [ARCHITECTURE §5].
- **Precedent:** iroh, where a QUIC connection is "always for *exactly one* ALPN", so peers
  speaking several protocols hold several connections with shared internal state so "hole
  punching doesn't have to start from zero" [prior-art §2 message-to-transport mapping].
- **Named loss:** no control/bulk split *within* an endpoint — a saturated path still delays
  its own SUBSCRIBE and HELLO — and the pool's amortisation is given up, per-peer cost
  becoming per-path cost.

## 4. Consequences per option

| Option | Control starvation possible? | Cross-endpoint isolation | Pool/identity cost | Adapter fit | Precedent | Named loss |
| --- | --- | --- | --- | --- | --- | --- |
| A — accept and document | yes: HELLO, SUBSCRIBE and UNSUBSCRIBE queue behind bulk [PATTERNS §1.3], [PROTOCOL §2.2] | none beyond per-stream isolation [PATTERNS §1.3] | unchanged: one pooled connection per (authority, terms) [ARCHITECTURE §5] | MQTT no-stall rule unmet; adapter must open one connection per foreign session [SYNTHESIS §7.2], [mqtt5 §5]; AMQP's two levels unmatched [amqp10 §5.3] | Kafka's one in-flight request per connection [prior-art §8 lesson 2] | weida can never state MQTT's rule; HELLO stays behind bulk under a 10 s deadline |
| B — two connections (control/bulk) | no for control; yes among bulk flows | none: all bulk shares one window | 2 handshakes, 2 pool entries, 2 fingerprint checks, 2 idle/keep-alive pairs per peer pair [ARCHITECTURE §5], [PATTERNS §1.8] | matches MQTT's rule directly [mqtt5 §5]; maps onto AMQP session-vs-link only by analogy [amqp10 §5.3] | Zenoh ALPN classes with `Control` class 0 [prior-art §1]; EMQX control stream [prior-art §3]; MOQT control stream pair [prior-art §4] | no bulk-to-bulk isolation; double per-peer state |
| C — classes + priority + reserved budget | no by construction if the reservation holds [prior-art §4 flow control] | none: bulk still shares one window | unchanged [ARCHITECTURE §5] | matches MQTT's rule within one connection [mqtt5 §5]; closest analogue to AMQP's two credit levels, which have "no guidance on relative sizing" [amqp10 §5.3] | MOQT control-first allocation [prior-art §8 lesson 8]; EMQX priority for `PINGREQ` [prior-art §8 lesson 7]; Zenoh `set_priority` inversion [prior-art §1] | priority is local and unenforceable on the wire [quic-standards §12 item 3]; one congestion/loss domain [quic-standards §3.5] |
| D — oversized connection window | reduced, not excluded: any finite window can be exhausted, and the stream budget still binds [PATTERNS §1.4] | none | unchanged [ARCHITECTURE §5] | no guarantee to offer an adapter; MQTT's rule stays a best effort [mqtt5 §5] | quinn connection `VarInt::MAX` [quic-standards §13]; quiche/QUICHE auto-tuning caps [quic-standards §13] | relaxes a named memory bound [INVARIANTS], [PROTOCOL §10] |
| E — per-endpoint connections | only within one endpoint's own traffic | yes, by construction [PATTERNS §1.3] | 1 handshake, pool entry, fingerprint check and timer pair **per path**, against `max_connections` 1024 [ARCHITECTURE §5], [PROTOCOL §10] | one foreign session per endpoint maps cleanly [SYNTHESIS §7.2]; still no control/bulk split inside a path | iroh's one connection per ALPN [prior-art §2] | no control/bulk split within an endpoint; per-path cost replaces per-peer amortisation, and HELLO negotiation repeats per path [0001 §7.8] |

## 5. What the evidence does not settle

- **What the coupling costs on weida's own defaults.** The named test measures a
  64 KiB/256 KiB configuration [PATTERNS §1.3]; nothing measures the shipped 1 MiB/16 MiB pair
  [PROTOCOL §10], and for long-lived bidirectional messaging "there is no published
  measurement, academic or vendor" [quic-standards §11.9]. The closest datapoint is
  MQTT-shaped: single-stream QUIC stopped publishing after 6 s of head-of-line blocking, and
  multi-stream kept delivering ~10 messages per second for the whole run [prior-art §3].
- **How much reserving costs when control is idle.** Unmeasured; AMQP 1.0 runs two levels at
  once and the spec "ties the two together nowhere and gives no guidance on relative sizing"
  [amqp10 §5.3].
- **Whether local priority does anything measurable on quinn.** RFC 9000 only SHOULD-level
  requires the API [quic-standards §2.3]; Zenoh documents the sign inversion it needs
  [prior-art §1] and moq-lite reports that "it may not be possible to get fine-grained
  control" [prior-art §8 lesson 7]. No number exists for weida's stack.
- **The cost of a second handshake per peer pair.** EMQX reports only the direction — QUIC
  "reconnection consumes more bandwidth than TLS" [prior-art §3 reported problems] — and
  nothing covers the extra pool entry and fingerprint check [ARCHITECTURE §5].
- **Whether 0001's reassembly buffer should read eagerly or withhold credit.** RFC 9308 offers
  eager reads as a mitigation, warning that the receiver "then needs another way to hold the
  peer accountable for that memory" [quic-standards §3.3]; 0001 fixes the bound, not the
  strategy [0001 §7.5].
- **Whether two connections can be proved to be one peer.** Identity is per connection and
  comes from the handshake [ARCHITECTURE §5], [PATTERNS §1.9]; nothing states how a control
  and a bulk connection bind into one logical session, and weida has no session concept
  [PATTERNS §1.8], [0001 §7.3].

## 6. Decision

The decider answered the seven questions of the draft as follows; the result is Option B
combined with Option E, plus three properties no single option carried.

1. **"Control never stalls behind bulk" is a guarantee weida offers**, not an operational
   property. It is declarable and validatable, so it joins the HELLO declaration set of
   [0001 §7.8]; a peer that cannot provide it is refused at negotiation, per the rule that a
   requested guarantee "MUST NEVER be silently weakened" ([GUARANTEES.md](../GUARANTEES.md)
   §4). Options A and D are closed: neither can promise it [§4].
2. **Isolation is in scope at both granularities**: control against bulk, and endpoint
   against endpoint. Only Option E gives the latter [PATTERNS §1.3], so the connection
   granularity becomes the dialled path: one bulk QUIC connection per `(host, port, terms,
   path)`, replacing the pool key that deliberately excluded the path [ARCHITECTURE §5]. The
   precedent is iroh's one connection per ALPN [prior-art §2].
3. **Control gets its own connection per peer**, Option B's shape rather than Option C's
   stream classes: one control connection per `(host, port, terms)` carrying HELLO,
   SUBSCRIBE/UNSUBSCRIBE and future control frames, beside the per-path bulk connections of
   decision 2. The reasons are the ones §3 records: flow control and the stream budget are per
   connection, so the isolation is QUIC's own [quic-standards §3.1], [quic-standards §2.8],
   whereas stream priority is local and unenforceable on the wire [quic-standards §12 item 3]
   and the projects that rely on it report the interface as the weak link [prior-art §8
   lesson 7]. The control and bulk connections of one peer are bound together by the proved
   fingerprint, which is already the identity the pool keys on [PATTERNS §1.9],
   [ARCHITECTURE §5]; a bulk connection whose fingerprint differs from the control
   connection's is not the same peer. The named cost is accepted: one extra handshake, pool
   entry and timer pair per peer, and the invariant "transfer-related control messages do not
   require a permanent control stream" ([INVARIANTS.md](../INVARIANTS.md)) must be restated so
   that a control *connection* is permitted while a multiplexed control *stream* remains
   forbidden.

   **Amended by [0011](0011-answered-where-it-arrived.md) §4.3: this connection is parked.**
   0011 decides that server-initiated traffic is written on the connection the registration
   arrived on, so a frame naming a path rides that path's connection — which takes
   SUBSCRIBE/UNSUBSCRIBE and the reserved credit frame out of this tier's cargo, and leaves
   HELLO, which every connection performs for itself anyway. A control connection in v0 would
   therefore pay a handshake and a timer pair per peer to carry nothing. It revives on a
   genuinely peer-scoped latency-sensitive frame, or on a measurement showing the residual
   coupling of 0011 §4.4 hurting a real workload. Decision 2 — one connection per dialled
   path — is implemented and is what carries the isolation this note was written for.
4. **Limits are configuration per runtime**, not protocol constants and not negotiated:
   `RuntimeConfig` carries two `Limits` profiles, `control` and `bulk`, with their own
   receive windows, stream budgets, idle timeout and keep-alive. QUIC's windows are the
   receiver's own transport parameters in any case [quic-standards §3.1], so the peer sees
   only what it is granted; AMQP 1.0's experience that relative sizing is "where brokers
   differ most" [amqp10 §5.3] argues against fixing it in the protocol.
5. **Overload is a named, reported condition**, separate from its prevention. A bulk reader
   that withholds credit still stalls the other writers on its own connection [PATTERNS §1.3];
   weida reports that state to the application as an error and a metric rather than leaving
   it to idle timeouts and `NEGOTIATION_FAILED` [PROTOCOL §2.2]. The shape follows MOQT's
   `PUBLISH_DONE` with `TOO_FAR_BEHIND` [prior-art §4 flow control]; whether it also gets a
   stop code on the wire is left to the protocol change that introduces it.
6. **0001's reassembly reads eagerly** into an application-owned buffer with its own named
   cap in bytes and count — RFC 9308's mitigation, accepting its caveat that "the receiver
   then needs another way to hold the peer accountable for that memory"
   [quic-standards §3.3]. Transport credit is released as soon as the transfer is read, so
   reassembly never contributes to the coupling this note is about [0001 §7.5].
7. **Adapters may multiplex foreign sessions onto shared weida connections** and still
   promise MQTT's rule that acknowledgements, subscriptions and pings "never stall behind a
   blocked publish path" [mqtt5 §5], because decisions 1-3 make the rule true of the
   transport. One connection per foreign session is no longer required of an adapter
   [SYNTHESIS §7.2].

## 7. Consequences and follow-ups

- **[ARCHITECTURE.md](../ARCHITECTURE.md) §5.** The client pool gains two tiers: a control
  connection per `(host, port, ClientTls, Option<Fingerprint>)` and bulk connections per that
  key plus path. `Peer::connect` dials the control connection first, negotiates HELLO there,
  then opens the path's bulk connection; a bulk connection with a fingerprint different from
  the control connection's is refused as a different peer. Server side, `Binding` accepts both
  kinds; how a bulk connection names its control connection (a HELLO field, or the fingerprint
  alone) is a follow-up for [PROTOCOL.md](../PROTOCOL.md) §2.
- **[PROTOCOL.md](../PROTOCOL.md) §2 and §10.** HELLO moves to the control connection and
  gains, beside 0001's guarantee declarations, a `control_isolated` declaration; the limits
  table splits into control and bulk profiles with their defaults. Whether ALPN distinguishes
  the two connection kinds (Zenoh's `zenoh-ms` shape [prior-art §1]) or a HELLO field does is
  part of the same follow-up.
- **The follow-up above is closed by [0011](0011-answered-where-it-arrived.md).** The question
  "how a bulk connection names its control connection" turned out to be two: which connections
  are one peer — answered by [0008](0008-session-identity.md) §4.2, the proved fingerprint and
  nothing else — and which connection a side writes *its own* traffic on, which 0008 does not
  answer and 0011 §4.1 now does: the one the registration arrived on. No HELLO field and no
  ALPN distinction is added, so nothing on the wire distinguishes the tiers.
- **[GUARANTEES.md](../GUARANTEES.md) §6 and [PATTERNS.md](../PATTERNS.md) §1.3.** The
  sentence "one slow reader stalls every writer on the connection" is narrowed to "every bulk
  writer on the same path's connection"; control traffic and other paths are isolated by
  construction. The new overload condition of decision 5 joins the failure tables.
- **§6.6 is deviated from, and the deviation is accepted.** Reassembly as built (B-022) holds
  an out-of-order transfer as an **unread stream** instead of reading it eagerly into an
  application-owned buffer: the eager read §6.6 asks for would materialize the payload, which
  the core forbids ([INVARIANTS.md](../INVARIANTS.md)), so the bytes stay in the transport and
  the hold is paid as backpressure. The bound is stated in
  [GUARANTEES.md](../GUARANTEES.md) §3 — `Limits::max_reorder_hold` transfers, each up to
  `stream_receive_window` bytes, the whole hold inside `connection_receive_window`, which is
  also the "another way to hold the peer accountable" that §6.6's own caveat demands.
- **[INVARIANTS.md](../INVARIANTS.md).** Restate the control-stream invariant to permit a
  control connection; add the reassembly buffer cap and the per-peer connection count (control
  plus one per dialled path, against `max_connections`) to the named bounds.
- **`weida-core::Limits` and `RuntimeConfig`.** Two profiles, `control` and `bulk`; the
  defaults for `control` are small (header-sized windows, a modest stream budget) so that a
  hostile peer's control connection pins little memory.
- **[SYNTHESIS.md](../research/SYNTHESIS.md) §8.2** is closed by this note. Decision 3's
  binding of two connections to one peer is the first concrete use of the fingerprint as a
  session-like identity and feeds the open session decision recorded by 0001.
- **Measurement before the change lands.** The cost of the second handshake and of per-path
  connections at the shipped defaults is unmeasured [§5]; both are tasks on weida itself,
  alongside the two from 0001.

## 8. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §2.1, §2.2, §2.3, §2.4, §9.5, §10;
[PATTERNS.md](../PATTERNS.md) §1.3, §1.4, §1.8, §1.9, §4; [ARCHITECTURE.md](../ARCHITECTURE.md)
§5 (connection driver, client connection pool, TLS); [INVARIANTS.md](../INVARIANTS.md);
[0001](0001-sequence-field.md) §7.3, §7.5, §7.6, §7.8.

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §1 (P4, P12), §7.2, §8.2;
[quic-standards.md](../research/quic-standards.md) §2.3, §2.8, §3.1, §3.2, §3.3, §3.5, §11.9,
§12 items 3 and 13, §13, §13.3; [prior-art.md](../research/prior-art.md) §1 (Zenoh:
message-to-transport mapping, flow control), §2 (iroh), §3 (EMQX: mapping, flow control,
reported problems), §4 (MoQ: mapping, flow control, delivery guarantees), §8 lessons 2, 7, 8;
[mqtt5.md](../research/mqtt5.md) §5; [amqp10.md](../research/amqp10.md) §5.2, §5.3.
