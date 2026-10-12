# Implementation

Implementation status, process rules and the acceptance criteria of the current increment.

Related: [ARCHITECTURE.md](ARCHITECTURE.md), [PROTOCOL.md](PROTOCOL.md),
[GUARANTEES.md](GUARANTEES.md), [FAILURE_MODEL.md](FAILURE_MODEL.md),
[INVARIANTS.md](INVARIANTS.md).

---

## 1. Phase tracker

Phases are executed in dependency order (master doc §79). Work MUST NOT be spread
chaotically across phases.

| Phase | Name | Status |
| --- | --- | --- |
| 0 | Architecture/specification | done |
| 1 | Core model | done |
| 2 | Native QUIC transport | done |
| 3 | Brokerless messaging patterns | in progress |
| 4 | Reliability | not started |
| 5 | Persistence subsystem | not started |
| 6 | Standalone broker | first slice done |
| 7 | Broker clustering | not started |
| 8 | Web adapter | not started |
| 9 | Legacy adapters | in progress |
| 10 | Language bindings | in progress |
| 11 | CLI and administration | first slice done |
| 12 | Documentation/site/stabilization | not started |

Phases 0, 1 and 2 are complete. Phase 3 is under way: its first increment (Push/Pull and
Pub/Sub) landed, its second re-founded the stack on the layer model — L0 stream core,
L1 patterns, L2 broker — see [ARCHITECTURE.md](ARCHITECTURE.md), and its third gave peers
identities and pinned the stream semantics with measured probes. Router/Dealer are answered
as emergent rather than implemented, see [ARCHITECTURE.md](ARCHITECTURE.md) §6a. Phases 4
and later remain out of scope; Phase 6 gained the acknowledgement vocabulary that used to
sit in the core.

Phase 9 is open out of order for standalone foreign-protocol libraries. Their dependency-free
codecs and `weida-runtime`-based implementations need none of the later weida phases and keep
their own protocol semantics. They do not introduce general bridges or mappings onto weida
patterns ([0013](decisions/0013-competitor-libraries.md)).

One increment belongs to no phase: **`weida-runtime` is extracted** (B-070,
`crates/runtime`). It holds `Exec` — `spawn`, `sleep`, `within`, `enter` and the capped
resolver with its IP-literal fast path — the three reactor-ownership constructors with the
background-shutdown discipline, the `AF_UNIX` bind hygiene and `peer_credentials` of
[0010](decisions/0010-local-transport.md) §4.5, a generic named-endpoint registry with a
byte budget, and the bounded close budget; `weida` uses it and its public API is unchanged
([0013](decisions/0013-competitor-libraries.md) §4.2, §5.3). It is the prerequisite of
0013's standalone libraries, which need a reactor, a resolver and socket hygiene and none of
weida's frames — so it is neither a phase nor a slice of one, but the crate boundary the two
product lines share. `weida-runtime` depends on `weida-core` and `tokio` and on nothing
else.

**Phase 11's first slice is the `weida` binary** (B-060, `crates/weida/src/bin/weida.rs`),
out of order for the same narrow reason Phase 9 is: it needs nothing from phases 4-10 and it
is what a reader of this tree hits first, because until it existed the fastest way to find
out whether an endpoint was reachable was to write and compile a program. Four verbs —
`serve` (`--echo`, `--sink`, `--pub`), `request`, `send`, `sub` — over the same library the
examples use, with the payload on stdin and stdout and nothing else on them, addresses and
receipts on stderr, and exit codes that name the error vocabulary a script can branch on
(`3` refused, `4` unknown endpoint, `5` no reply, `6` untrusted, `7` connection lost; `1`
for everything else, deliberately including `Indeterminate`, which must not become a
matchable number). Two things it does *not* do are decisions rather than omissions: a
**bind** address is parsed by the binary rather than by `Address::parse`, because a bind
address may say port `0` and carries no fingerprint — the one it prints is its output; and
`weida+inproc://` is refused with a sentence naming why, since an in-process bus cannot be
reached from another process and a CLI that pretended otherwise would only ever talk to
itself. Standard input and output go through `std`, not tokio's `io-std` and `fs`: those
features are not in the library's dependency set and a binary is not a reason to widen what
every downstream crate compiles. What remains of the phase — administration, introspection
of a running runtime, anything that needs a broker — is untouched.

### Phase 0 — Architecture/specification

Delivers this documentation set, and only it: no code.

- Terminology formalized ([ARCHITECTURE.md](ARCHITECTURE.md) §2).
- Core invariants finalized ([INVARIANTS.md](INVARIANTS.md)).
- Architecture documents produced (this set).
- Failure vocabulary defined ([FAILURE_MODEL.md](FAILURE_MODEL.md)).
- Guarantee vocabulary defined ([GUARANTEES.md](GUARANTEES.md)).
- Protocol envelope designed ([PROTOCOL.md](PROTOCOL.md) §3).
- Serialization selected (CBOR, see §5 below).
- Version/capability negotiation designed ([PROTOCOL.md](PROTOCOL.md) §2.3).
- Initial Rust public API designed ([ARCHITECTURE.md](ARCHITECTURE.md) §7).
- Deliberately unresolved questions identified (§6 below and
  [PROTOCOL.md](PROTOCOL.md) §11).

No broker implementation begins before these foundations are coherent.

### Phase 1 — Core model

Delivers `weida-core`: an I/O-free crate, unit-tested without any networking.

- `EndpointAddr` endpoint identifiers with the `weida://` parser and the `SCHEME` constant.
- `Error`, `ErrorCode` and `StopReason` with hand-written `Display`/`Error` impls.
- `Limits`.
- `TraceContext` with the W3C `traceparent` parser and formatter.
- Cancellation representation.

As first delivered the crate also carried `TransferId` and id allocation (`id.rs`), the
`AckMode`/`AckState`/`Outcome` policy types (`policy.rs`), and the send, receive and
correlation state machines as pure transition functions (`state/`). All three were deleted
by the Phase 3 re-architecture below; the surviving modules are `addr`, `error`, `limits`
and `trace`.

The Runtime, Listener, Binding and typed Endpoint abstractions named in master doc §79's
Phase 1 list are realized in crate `weida` in this increment, because their shape is
determined by the transport they front; their model-level types (`Limits`, `EndpointAddr`,
`Error`) live in `weida-core`.

### Phase 2 — Native QUIC transport

Delivers `weida-protocol` and crate `weida`.

- Quinn integration and connection establishment.
- Negotiation (HELLO exchange, version and capability intersection).
- The protocol envelope: varints, preamble, the frame headers, CBOR codec with
  cap-before-allocation and depth-limited skip.
- Endpoint routing through a flat namespace shared by all bindings of a Listener.
- Request/reply correlation over paired unidirectional streams, with ACK, ERROR and CANCEL
  as short header-only control streams. Superseded by the Phase 3 re-architecture: an
  exchange is now one bidirectional stream, ACK and CANCEL are gone from the wire, and
  ERROR rides the reply half.
- Cancellation and stream reset in both directions.
- Resource limits, all remote-input-bounded.
- Client connection pooling keyed by `(host, port)`. The key has since grown the dialling
  terms and the fingerprint the address names (third increment below).

Large streaming transfers are tested immediately, not deferred.

---

### Phase 3 — Brokerless messaging patterns

Master doc §79 asks at minimum for Push/Pull, Pub/Sub, Req/Rep and Router/Dealer
equivalents. §82 asks whether a smaller internal primitive set implements them cleanly.

**Delivered in the first increment:**

- The §82 answer: four primitives (P1 one-way transfer, P2 correlation, P3 peer set plus
  selection policy, P4 bounded inbound queue behind an opaque path), recorded with the
  Router/Dealer-is-emergent rationale in [ARCHITECTURE.md](ARCHITECTURE.md) §6a.
- Wire: the `topic` DATA key, the SUBSCRIBE and UNSUBSCRIBE frame kinds, and the limits
  `max_subscriptions` and `subscriber_buffer_bytes` — all normative in
  [PROTOCOL.md](PROTOCOL.md). (The numbers this increment assigned were compacted by the
  re-architecture below; current numbering is `topic = 5`, SUBSCRIBE `= 3`,
  UNSUBSCRIBE `= 4`.)
- Push/Pull: `Pusher` connects and round-robins over its peers, `Puller` binds. Both ride
  P1 and P3 unchanged.
- Pub/Sub: `Publisher` binds, `Subscriber` connects; byte-prefix topic filters; fan-out
  with a per-subscriber byte budget and explicit drops, so a slow subscriber never stalls
  the publisher.
- Refusal rather than reinterpretation when a pattern meets an endpoint that does not serve
  it: the connection stays intact and the sender learns the reason. (As first delivered
  that was an ERROR `UNSUPPORTED` on its own stream plus `STOP_SENDING(REJECTED)`; the
  re-architecture gave misrouting its own stop code, see below.)

**Delivered in the second increment — the layered re-architecture:**

The stack was re-founded on three layers ([ARCHITECTURE.md](ARCHITECTURE.md)): **L0 stream
core**, ZeroMQ's idea rebuilt on QUIC, whose primitives are exactly the two QUIC stream
kinds with exactly QUIC's guarantees; **L1 patterns**, the zmq/nanomsg family as thin
wrappers over L0; and **L2 broker**, the RabbitMQ analog, deferred to Phase 6.

Deleted from the core and from the wire: `FrameKind::Ack`, `FrameKind::Cancel`,
`AckHeader`, `CancelHeader`, the `ack_mode`, `role`, `transfer_id` and `correlation_id`
DATA keys, `AckMode`, `AckState`, `Outcome`, `Role`, `TransferId`, `Correlator`,
`SendMachine`, `RecvMachine`, `ReplyDisposition`, `PendingReply` and `Limits::max_pending`
— and with them the files `crates/core/src/id.rs`, `crates/core/src/policy.rs` and the
whole `crates/core/src/state/` directory.

Two reasons, both load-bearing:

1. **A brokerless application ACK means "arrived in RAM".** QUIC already retransmits, and
   already reports that the peer's transport holds every byte. An application ACK with no
   broker behind it therefore carried no information the transport did not already have:
   it bought RabbitMQ's vocabulary — Accepted / Stored / Replicated / Processed — without
   RabbitMQ's responsibility transfer. Nothing had taken responsibility for the message, so
   the words were unearned. That vocabulary is now reserved for the L2 broker hop, where
   responsibility genuinely changes hands, and has no v0 wire representation
   ([GUARANTEES.md](GUARANTEES.md)).
2. **The correlation machinery existed only because replies rode separate unidirectional
   streams.** Correlation ids, the `Correlator`, `ReplyArrived`, the CANCEL frame and the
   per-connection pending-reply table were all bookkeeping for one question: which reply
   stream answers which request stream. One bidirectional stream per exchange answers it
   structurally — the stream *is* the correlation. Nothing on the wire names an exchange,
   the requester abandoning a reply is `STOP_SENDING(CANCELED)` on the reply half rather
   than a CANCEL frame, and the actor that held the table is gone. P2 in the §82 taxonomy
   stopped being "correlation" and became "exchange": a bidirectional stream, with nothing
   behind it ([ARCHITECTURE.md](ARCHITECTURE.md) §6a).

What took their place:

- Req/Rep is one client-opened bidirectional stream: DATA (with `endpoint`) + payload + FIN
  on the initiating half; DATA (no `endpoint`) + payload + FIN, **or** ERROR + FIN, on the
  reply half. The halves are independent, so request and reply still stream simultaneously.
- `OutgoingTransfer::finish` returns a `Delivery` transport receipt instead of an ack
  outcome; `Delivery::delivered().await` resolves when the peer's *transport* holds every
  byte, explicitly not when the application read it
  ([FAILURE_MODEL.md](FAILURE_MODEL.md)).
- The L0 API is public: `Peer` (`connect`, `open`, `open_bi`) and `Acceptor`
  (`Listener::acceptor`, `accept() -> Incoming`) in `crates/weida/src/stream.rs`, proven by
  `crates/weida/tests/raw_streams.rs`. The patterns in `endpoint.rs` are now wrappers over
  it.
- Misrouting gained its own QUIC application error code, `UNSUPPORTED = 9`, because a
  unidirectional stream can no longer be refused with an ERROR frame of its own.
- Frame kinds and DATA keys were renumbered densely and every golden vector recomputed
  ([PROTOCOL.md](PROTOCOL.md) §4, §6, §8).

**Delivered in the third increment — identity, trust and stream semantics:**

Peers gained identities. `weida_core::Fingerprint` (`crates/core/src/identity.rs`) is the
SHA-256 digest of a leaf certificate's DER `SubjectPublicKeyInfo`, with text form
`sha256:<64 lowercase hex>`; the key is hashed rather than the certificate, so a pin survives
a renewal that reuses the key. `EndpointAddr` gained `peer: Option<Fingerprint>` and the
grammar `weida://[sha256:<hex>@]host:port/path`, so one string says where to dial and whom to
accept. The TLS configuration was split along the two questions it answers: `Identity`
(`generate`, `generate_for`, `from_pem`, `from_pem_files`, `from_pem_file`, `fingerprint`,
`certificate_pem`, `to_pem`) for *who am I*, and `Trust` (`by_address`, `pin`, `anchor`,
`anchor_file` plus the `and_*` builders) for *whom do I accept* — pinned fingerprints or CA
anchors, with an address-named fingerprint overriding both. `ClientTls { trust, identity }`
and `ServerTls { identity, client_trust }` combine them; `impl Into` at every call site lets
a bare `Trust` or `Identity` stand in, and the previous root-list client shape and
certificate-and-key server shape are both gone. A binding may now require client identity
(`ServerTls::require_client`; since [0035](decisions/0035-keys-proved-not-judged.md) it takes a
`ClientTrust`, a trust or `AnyKey`), and whatever a peer proved in the handshake is surfaced as
`IncomingMeta::peer` on inbound transfers, requests and the requester's reply half — from the
handshake, never from a header, so it can only be proved and never claimed (master doc §47).
New errors: `Error::InvalidFingerprint` and the definite `Error::Untrusted(Fingerprint)`,
which names the identity that answered instead of the one that was expected. The connection
pool key became `(host, port, ClientTls, Option<Fingerprint> from the address)`.

Three defects the new tests exposed were fixed. `ConnCtx::negotiated()` now fails immediately
with the connection's close reason instead of waiting out the 10 s HELLO deadline, so a client
whose identity a binding refuses learns it at once rather than hanging for `hello_timeout_ms`.
`conn_error` maps `TimedOut` (idle timeout) and `Reset` (stateless reset) to
`Error::ConnectionLost` — definite, where it used to be `Error::Transport("timed out")` — and
a peer close carrying a TLS alert code to `Error::Tls`. Dead peers used to be reaped by
`PeerSet::add` on the next `connect`; since 0031 a dead peer is a slot the runtime redials,
and nothing accumulates either way.

The increment is pinned by 10 tests in `crates/weida/tests/identity.rs` (an address pin under
an empty trust; a wrong pin refused with `Untrusted(what answered)` and no peer added; an
address pin overriding an anchor; `Trust::pin` accepting only the key; pins and anchors
composing; an anchor checking the name where a pin does not; a binding refusing anonymous and
untrusted clients, accepting a pinned one and seeing its fingerprint in `IncomingMeta::peer`;
an anonymous client reported as `None`; reply metadata naming the server; connections not
shared across different terms), by unit tests in `crates/weida/src/tls.rs`,
`crates/core/src/identity.rs` and `crates/core/src/addr.rs`, and by 10 stream probes in
`crates/weida/tests/streams.rs` that measure what the transport actually promises about
receipts, flow control, stream budgets, cancellation, connection loss and idle timeout (§4).
The examples lost their PEM handling: `push_pull` and `pub_sub` use `Identity::generate()`
plus a pinned URL and touch no files at all, `transform_server` prints its
`weida://sha256:…@host:port/…` addresses and takes `--identity PATH` for a fingerprint that
survives a restart and `--cert-out PATH` for publishing its certificate, while
`transform_client` and `large_stream` take an optional `--ca PATH` and otherwise trust what
the address names.

**Delivered in the fourth increment — DATA keys 6 and 7 in the codec (B-013):**

`DataHeader` gained `sequence: Option<u64>` (key `6`) and
`producer: Option<[u8; 32]>` (key `7`), encoded per
[decision 0008](decisions/0008-session-identity.md) §4.4: a minimal CBOR `uint` and a raw
32-byte `bstr`, both written only when set and both skipped by a peer that does not know
them. `limits::PRODUCER_BYTES` is an exact length rather than a cap — a `bstr` of any other
length is a framing violation, because a truncated digest names nobody
([PROTOCOL.md](PROTOCOL.md) §6.2) — and `MapReader::byte_array` is the strict reader behind
it. Two golden vectors pin the encoding in `docs/PROTOCOL.md` §8, in
`crates/protocol/tests/golden_vectors.rs` and in the header unit tests; the `roundtrip` and
`data_header` fuzz targets and the deterministic `fuzz_smoke` mirrors generate both fields.

**Nothing in `crates/weida` reads or writes either key.** The runtime's `data_header` sets
both to `None` and says why, so the wire is byte-identical to the previous increment and the
B-009 header-cost numbers below still describe what the tree sends. The keys exist so that
the ordering work of [0001](decisions/0001-sequence-field.md) has a wire to land on, not
because anything uses them yet.

**Delivered in the fifth increment — HELLO guarantee declarations (B-014):**

`Hello` gained `guarantees_offered` and `guarantees_required`, both
`Option<GuaranteeSet>` and both encoded as the nested map of
[PROTOCOL.md](PROTOCOL.md) §6.5. `GuaranteeSet` is ten `Copy` fields — delivery,
acknowledgement, the durability and replica axes, ordering, deduplication and its window,
backpressure, producer naming, control isolation — with `GuaranteeSet::CORE` as the default
and `is_core()` as the reason nothing is written for it. `negotiate()` now computes the
effective set as the weaker of the two offers (`GuaranteeSet::intersect`) and fails with
`NEGOTIATION_FAILED` when it does not reach either side's requirement
(`GuaranteeSet::reaches`); `Agreed` carries the result.

Three properties are pinned rather than asserted in prose. **A v0 peer that declares nothing
still negotiates `core`** (`two_v0_peers_negotiate_core_unchanged`), and a HELLO whose
declarations are explicitly `core` encodes to the same bytes as one with none
(`golden_hello_with_guarantees_frame`). **A required level the peer does not offer fails the
handshake**, in both directions, and over a real connection in
`crates/weida/tests/hostile.rs`. **A malformed declaration is a framing violation, not a
negotiation failure**: the hostile suite builds six illegal guarantee maps byte by byte —
the library's encoder cannot produce one — and each closes the connection with
`PROTOCOL_VIOLATION`.

No new bound was needed for [INVARIANTS.md](INVARIANTS.md): a guarantee set is fixed-size
`Copy` data whose enclosing map is already bounded by `max_header_bytes`, so it adds no
remote-influenced allocation. Nothing in `crates/weida` declares anything yet; the runtime
still sends `Hello::v0`, which is why the wire is unchanged.

**Delivered in the sixth increment — detector-mode ordering (B-015):**

`RuntimeConfig::guarantees` is the configuration surface: one set, offered *and* required,
so a peer that cannot match it fails the handshake and the local set is therefore the
effective one. Configured with `OrderingMode::PerProducerDetect`, a runtime numbers its
one-way transfers in DATA key `6` — per dialled path for `Peer::open` and Push, per **topic**
for a published message, assigned once before fan-out — and the receiving connection reports
what is missing through `IncomingMeta::gap` (`Gap { expected, seen }`, `missed()`), delivering
every message as it arrives. Exchanges are not numbered: a reply carries no endpoint and the
stream is the correlation.

Both halves live in `crates/weida/src/ordering.rs` (`Sequencer`, `GapDetector`) and both are
inert under `core`. The hot-path invariant is checked rather than claimed: two unit tests
drive a thousand calls through a disabled sequencer and a disabled detector and assert the
backing tables' `capacity() == 0`, so an endpoint that negotiated nothing allocates nothing.
The detector's scope table is remote-influenced — the peer names the paths and topics — so it
is capped by the new `Limits::max_sequence_scopes` (1024); at the cap a new scope is left
untracked rather than inserted, which a unit test pins.

The end-to-end proof is `a_dropped_fan_out_copy_shows_up_as_a_gap` in
`crates/weida/tests/pubsub.rs`: a subscriber whose byte budget is exhausted loses copies, and
the next copy it receives carries a gap whose `missed()` equals `Publisher::dropped()`. That
is the capability [0001](decisions/0001-sequence-field.md) §7.2 required and the wire could
not express before B-013.

**Deduplication is not part of this increment** — it landed in the next one, below.

**Delivered in the seventh increment — bounded deduplication (B-025):**

A runtime configured `Deduplication::Bounded` with a `dedup_window_ms` suppresses a
repeated `(producer, scope, sequence)` inside that window. The producer is the connection's
proved fingerprint unless DATA key `7` names another, per
[0008](decisions/0008-session-identity.md) §4.4; the scope is the topic for a published copy
and the endpoint path otherwise, the same scope the sequencer numbers. The window lives in
`crates/weida/src/dedup.rs` (`DedupWindow`) and sits between the wire and the application in
`handle_data`: a suppressed transfer is read to EOF and discarded rather than dropped, so the
sender sees an ordinary transport receipt and never reads the suppression as a refusal.
Suppressions are counted per runtime and exposed as `Runtime::suppressed_duplicates`, which
is where drops are already visible on the publishing side.

Bounded is bounded twice. In time by the negotiated window, and in count by the new
`Limits::max_dedup_entries` (4096), because a time window bounds how long an identity is kept
and not how many arrive within it — the reason [INVARIANTS.md](INVARIANTS.md) named this bound
before the code existed. At the cap the oldest entry is evicted, so a peer that floods buys
itself missed suppression rather than memory. Both bounds are pinned by unit tests, as is the
hot-path invariant: a thousand calls through a disabled window leave the table's
`capacity() == 0`.

The end-to-end proof is `crates/weida/tests/dedup.rs`, which replays from the raw wire —
the library's own sequencer is monotone and cannot produce a repeat. Three tests: the same
numbered transfer twice inside the window arrives once and increments the counter, with an
unsuppressed sentinel behind it proving suppression rather than delay; the same identity
after the window arrives again; and under `core` the same replay arrives twice, because
nothing was negotiated.

**One bug in B-015, found by thinking about replays.** `GapDetector::observe` advanced its
position to `max(expected, seen) + 1` on every arrival, including one *below* the expected
number. A late or repeated number therefore consumed a number that had not arrived and
silenced its gap: with `0, 2, 1` the detector reported the hole at 1 and then skipped 3
without a word. A below-expected arrival now moves the position nowhere, which is what the
method's own doc comment already claimed, and `an_out_of_order_arrival_is_not_a_gap` pins it
by asserting that the hole after the late arrival is still reported. The suppression check
is also placed ahead of the detector, so a duplicate the receiver never accepted cannot
shift the receiver's idea of where the producer is.

**Delivered in the eighth increment — reassemble-mode ordering (B-022):**

`OrderingMode::PerProducerReassemble` holds an arrival whose predecessors have not come yet
and releases the run in sequence order. `Reassembler<T>` in `crates/weida/src/ordering.rs`
is the whole mechanism: a `BTreeMap` per scope plus the position, generic in what is held so
that the ordering logic is unit-testable without a connection, and the connection instantiates
it as `Reassembler<Held>`. The gap detector is now enabled in detect mode *only* — under
reassemble the reassembler owns the position, and two components tracking it would report the
same hole twice.

**A held transfer is an unread stream.** `Held` carries the `RecvStream`, the metadata and
the path; no payload is read, so reassembly stays inside "core transport does not require
payload materialization" ([INVARIANTS.md](INVARIANTS.md)). What the hold pins is transport
memory: one stream of the peer's `max_concurrent_uni_streams` budget and up to
`stream_receive_window` bytes each, the total bounded by `connection_receive_window`. This is
a **documented deviation from [0002](decisions/0002-control-and-bulk-separation.md) §6.6**,
which proposed reading eagerly into an application-owned buffer so that transport credit is
released early: v0 cannot do that without materializing the payload, so the bytes stay in the
transport, where the receiver's own window already bounds them and the sender feels the hold
as backpressure. The pubsub test above had to be given a receive window large enough to park
two copies *and* accept the arrival that evicts them — the deviation is not theoretical.

**The bound is enforced by releasing, never by growing.** `Limits::max_reorder_hold` (256 —
B-010 measured the peak at N − 1 of the transfers in flight and 84 of 256 with no adversarial
pattern, so the cap is configuration, not an assumption about arrival order) counts held
transfers over all scopes. When an arrival does not fit, the oldest held transfer is released
out of order carrying `IncomingMeta::gap` — exactly what detect mode would have reported —
and whatever now follows it in order goes out behind it. The victim scope is the one holding
the most, so one stalled producer cannot spend the whole budget and push every other scope
into out-of-order release. Seven unit tests cover it: in-order release, gapless runs, the
forced release and its gap, the hold never exceeding the cap over 500 arrivals, the scope
table at its own cap, pass-through for unnumbered, untracked-scope and late arrivals, and
the repeat rule below.

The end-to-end proofs are `crates/weida/tests/reorder.rs` — sequences scrambled on the raw
wire arrive in sequence order; a hold of two releases out of order with the gap when the
third arrival does not fit; and the reverse-FIN probe of
`reverse_order_completion_measures_the_reorder_buffer` rerun at 16 and 256 transfers with
both `Batched` and `Sequenced` finishing, now asserting that the *runtime* delivers in
sequence order what the probe's application had to reorder for itself — and
`a_full_hold_reports_the_pub_sub_drop_it_was_waiting_for` in `crates/weida/tests/pubsub.rs`,
which is the acceptance criterion that a subscriber in either mode reports a Pub/Sub drop
through `IncomingMeta`. Note what that test shows about reassemble mode: a hole a publisher
drop leaves is *never* filled, so the loss becomes visible only when the hold is full. Under
reassemble, `max_reorder_hold` is the delay a permanent loss costs.

**Two things review caught in the first cut of `admit`.** A repeated number at or above the
position went through `BTreeMap::insert`, which replaced the held transfer silently while
`held` was incremented again: the counter drifted upward for good, forced releases began
firing below the real bound, and the displaced `RecvStream` was dropped — resetting a stream
its sender still believed was in flight. The slot now belongs to whoever claimed it first
and the repeat is passed straight through (`a_repeated_number_is_delivered_and_does_not_move_the_hold`
asserts both the unchanged hold and that every item offered came back exactly once; it fails
on the old code with `held` at 2). Suppressing a repeat is deduplication's job and stays
there. Second, the in-order arrival — the common case, and the only one on a network that is
not reordering — allocated a map node just to take it out again; when the number is the
expected one and nothing is waiting, it is now released without touching the `BTreeMap`.

**Not bounded in time.** v0 bounds the hold in count only. A hole at the very end of a
producer's traffic holds its successors until the connection closes, because nothing arrives
to push the hold over its cap. A time bound on a held transfer is the obvious next knob and
is deliberately not invented here.

**Delivered in the ninth increment — the bounded drain (B-032):**

`Runtime::drain(Duration) -> Drained` is the counterpart of the abortive `shutdown`
([decisions/0009](decisions/0009-drain.md) §4.1). In order: admission stops, the finished
transfers are awaited to their transport receipt, and then the same close runs, bounded by
whatever is left of the same deadline (§4.4). The deadline is a plain `Duration` — no
`Option`, no configuration default that could grow into ZeroMQ's infinite linger (§4.3) —
and `Drained { delivered, outstanding }` is a pair of local counts. An expired drain is not
an error and returns no error (§4.6). Nothing was added to the wire (§4.8).

**Admission** is one flag in the per-runtime `Shared` state that every connection already
needs: a binding refuses an incoming handshake outright, and an inbound stream on an
existing connection is answered with `STOP_SENDING(SHUTDOWN)` — a bidirectional one also
has its reply half reset with the same code (§4.5). No frame announces it: a peer that had
to be told would have to answer, which is the application acknowledgement
[0005](decisions/0005-refusal-race.md) closed.

**What the drain waits on, and why it had to be parked.** The only completion signal L0 has
is `SendStream::stopped()`, and a fire-and-forget sender drops the `Delivery` that holds
it, taking the last handle to the acknowledgement with it. `Delivery::drop` now hands that
receipt to **its own connection's** parked set (`ConnDrain` in
`crates/weida/src/drain.rs`, beside the sequencer and the gap detector in `ConnCtx`)
instead of dropping it; a receipt the application awaits itself is never parked. Published
fan-out copies, which never had a `Delivery` at all, are parked the same way at `finish()`
in `pubsub::write_one`. Parking is a lock and a push and nothing else — the set is walked
only when it is full, where settled receipts are reaped with one `Waker::noop()` poll
before anything is thrown away, and once more at drain time, when `DrainState::take` walks
the connections the runtime registered (once per connection, never per message). The cap
is that connection's stream budgets from `Limits`, which is why
[INVARIANTS.md](INVARIANTS.md) needs no new number for it. A receipt evicted at that cap
is counted as outstanding by the next drain rather than assumed delivered. What the first
cut got wrong, and what the numbers for it are, is §4 (B-032).

**What a slow reader cannot show, and the test that says so.** QUIC acknowledges bytes into
the receive window whether or not the application reads them, so a slow *reader* never
leaves a finished transfer outstanding — what does is an acknowledgement that never comes.
The proof pair in `crates/weida/tests/drain.rs` is therefore built around a window smaller
than the payload: a 512 KiB transfer whose receiver starts reading late arrives whole under
`drain` and is cut short by `shutdown`, asserted in both directions so the pair cannot
drift. The expiry test gives the peer its own single-threaded reactor and blocks that
thread outright — socket open, connection up, nothing read or acknowledged — and a
200 ms drain returns at its deadline with `outstanding = 1`, `delivered = 0` and no error.

**The admission refusals, and what testing them needed (B-046).** Both are three lines in an
accept loop, and neither has an observable unless a drain is *in flight* — a drain with
nothing outstanding returns at once, so there is no window to race a connect against. The
`Draining` harness in `crates/weida/tests/drain.rs` makes one: a runtime that is a server
**and** a stalled sender, holding a binding plus one finished transfer to a frozen peer whose
receipt can never settle, so its drain runs for its whole deadline. Inside that window a new
dial is refused (the control is in the same test: the same client dials one path before the
drain and another during it, which one connection per dialled path makes a real handshake
rather than a reuse), and a new stream on the connection that already exists is refused with
`SHUTDOWN`. Both fail under mutation: disabling either `is_draining()` check makes exactly its
own test fail.

Writing them exposed a vocabulary gap. `STOP_SENDING(SHUTDOWN)` fell through
`codes::stop_reason` to `StopReason::Other(6)` and reached the sender as
`Error::Transport("peer stopped receiving with code 6")` — a refusal the application could only
recognize by parsing a string. `StopReason::ShuttingDown` now names it and maps to
`Error::Rejected`: the *outcome* is the same as any other refusal — nothing of the transfer was
taken and the peer will not take it later — and a second word for one outcome is what B-028's
`LossCause` exists to avoid.

**Delivered in the tenth increment — the transport boundary and inproc (B-037):**

The first non-QUIC transport, and therefore the first time the runtime had to say what a
transport *is*. `crates/weida/src/transport.rs` defines three types — `Link`,
`SendHalf`, `RecvHalf` — and everything above them is transport-blind: the frame readers,
the patterns, the guarantees and the drain are written once and run over either transport,
which is what makes [PROTOCOL.md](PROTOCOL.md) §2.1's "same frames, same HELLO, same
negotiation" checkable rather than merely stated. An enum, not a trait object: the set of
transports is closed and small, and the payload path must stay a direct call rather than a
vtable hop. `AF_UNIX` (B-038) and named pipes (B-039) are one variant each.

`crates/weida/src/inproc.rs` is the first of them. A process-global registry maps a bus
name — at most 256 bytes, libzmq's budget for the same thing — to a bound listener, and
`Listener::bind_inproc` returns a `LocalBinding` that unregisters the name when it drops.
A connection is a pair of queues, one per stream kind, and **a stream is one channel
pair**: opening mints a `tokio::io::duplex` pair whose capacity is `stream_receive_window`,
so the backpressure is the buffer's, as [0010](decisions/0010-local-transport.md) §4.2
expects where there is no connection window. That is also why the two accept queues are
separate: the uni and bidi accept loops are separate tasks, and one queue would let each
consume the other's stream — which is exactly the bug the first cut had, and the
`req_rep_over_inproc` failure that found it.

**No TLS, no credentials, nobody proved.** A local binding takes no identity and a local
dial ignores the trust it is handed, because there is no key to check [0010 §4.3]; the
address form rejects the `sha256:…@` userinfo so that it cannot look otherwise [0010 §4.8].
In process there is nothing to prove at all, so `IncomingMeta::peer` is `None` [0010 §4.4].
`weida_core::Address` parses both schemes and the scheme alone picks the transport: nothing
falls back from one to the other [0010 §4.6].

**Ordering, deduplication and the drain are not excluded**, because they are defined on
headers and receipts rather than on sockets: `guarantees_and_the_drain_work_over_inproc`
negotiates `PerProducer(detect)` and `Bounded` over a bus, asserts the sequence numbers on
the arriving transfers and then drains the client to zero outstanding. Two transport-level
details make that work: `LocalSend::stopped` resolves once the payload and its FIN are in
the peer's buffer — in process, that *is* "the peer's transport holds every byte" — and
`Runtime::shutdown` now closes the registered links as well as the endpoints, because a
local connection has no endpoint to close.

**`max_local_streams` (255) is enforced, not just named.** Live transfers on one local
connection are counted, and an `open` at the cap waits for one to end rather than
refusing (B-059, below); the number is Windows' named-pipe instance limit, the tightest
of the three platforms [0010 §4.2]. INVARIANTS' named-bounds table is down to one entry,
`max_connections_per_peer`.

**The tests are parametrized rather than copied.** `crates/weida/tests/transports.rs`
writes one Req/Rep body, one Push/Pull body and one Pub/Sub body and runs each over both
transports through a `Harness`; the address rules, local peer loss and the guarantee/drain
case are inproc-specific tests beside them. Nine tests in that file, and the pre-existing
QUIC suites are untouched — they cover what is QUIC-specific (trust, pooling, hostile
peers), which is the split that keeps the shared bodies honest.

**Not in this slice**, and named rather than discovered: no pooling for local connections
(each dial is a fresh pair, and the pool exists for the expensive case), no control/bulk
split locally (there is no shared window to separate), and the `AF_UNIX` and named-pipe
variants with their platform rules and their local-principal identity, which are B-038 and
B-039 with the acceptance already written in [0010](decisions/0010-local-transport.md) §4.5.

**Delivered in the eleventh increment — `AF_UNIX` (B-038), and the decision it needed
(0012):**

Implementing 0010's "the OS connection is the stream" on a socket exposed what that
sentence does not say: an accepted socket cannot be dialled back, and three things need a
stream from the accepting side — its own HELLO, Pub/Sub fan-out, and everything the runtime
holds per connection that is really per peer. Descriptor passing, the shape `ipc.md` §11
calls strictly better, is closed to us: Windows `AF_UNIX` has no ancillary data and named
pipes have none either, so B-039 could not inherit it, and `SCM_RIGHTS` is unstable in
`std` while this crate forbids `unsafe`. [0012](decisions/0012-local-connection-grouping.md)
decided the alternative and this increment implements §4.1-§4.3 of it.

**Connections become a peer by grouping.** The first connection a client opens is the
control connection: it carries the HELLO exchange both ways and owns everything per peer
(`Agreed`, the sequence scopes, the dedup window, the parked drain receipts). Every further
connection opens with `0x02` and the 16-byte group token the server issued on the control
connection, and is admitted only if the token names a live peer **and** the kernel
credentials match that peer's — uid always, pid where the platform reports one. An unbound
connection is dispatched nowhere, which
`a_transfer_connection_with_an_unknown_token_is_refused` pins by injecting a raw connection
with a zero token and observing that only the legitimate transfer arrives.

**Dispatch is by path**, exactly as [PROTOCOL.md](PROTOCOL.md) §2.1 already specified for
local transports: the accepted connection's frame is read, the path is looked up, and a
replier answers on that same connection while a puller path never writes back. That is one
new concept in the boundary — `Link::dispatch_by_path` and a `handle_local` beside
`handle_bi` — and it is what a transport with no stream kinds needs.

**Identity is now a sum.** `IncomingMeta::peer` is `Option<PeerIdentity>`: a `Key` proved by
TLS, or a `Local` principal proved by the kernel — `SO_PEERCRED` on Linux through tokio's
safe `peer_cred()`, `LOCAL_PEERCRED` on macOS with no PID. A PID is reported where it
exists and is documented as an observation that must not be authorized on [0010 §4.4].
`a_unix_peer_presents_the_principal_the_kernel_proved` asserts the shape: a principal, no
key, the uid of the process that owns the socket directory, and a PID on Linux.

**Socket hygiene is the caller's directory plus our two rules.** `bind_unix` removes a
stale socket node before binding and sets mode `0600` explicitly rather than inheriting
`umask`, and its doc comment states the obligation the code cannot discharge: unlink-then-bind
races unless the directory's ownership and permissions prevent substitution
(`docs/research/ipc.md` §1.2, §7). `a_stale_socket_file_does_not_stop_the_next_bind` leaves
a node behind, binds over it and checks the mode. The path budget is checked after
percent-decoding, against 107 bytes on Linux and 104 on macOS.

**What was not here, and landed next.** Pub/Sub over `AF_UNIX`: a publisher has no stream
to a peer that dialled it, so this increment refused it with `Unsupported` and filed the
reverse pool of [0012 §4.4] as **B-048**, which the increment below implements. No
pooling, and no control/bulk split, for the same reasons as inproc. The suite is
`#[cfg(unix)]`, so Windows builds compile the transport out entirely; B-039 adds the pipe
variant to the same shape.

**Delivered in the twelfth increment — the reverse pool (B-048), which closes 0012 §4.4:**

Fan-out over `AF_UNIX` was the one pattern B-038 could not serve, because a publisher has
no way to dial a peer that dialled it. It now rides connections the subscriber parks in
advance, which is the shape [0012 §4.4] proposed without precedent; this increment is the
evidence that decides it.

**The pool.** `Subscriber::connect` over a socket transport opens
`Limits::max_parked_reverse` connections (default 8), each carrying preamble `0x03` plus
the group token, and each admitted by exactly the rule a transfer connection is: the token
names a live peer and the kernel credentials match. The server keeps their *write* halves
in a `ReversePool` shared with that peer's link; the subscriber keeps their read halves,
and hands them to its own connection driver, where they wait like any other inbound
stream. Nothing new is on the wire: what the publisher writes is a DATA frame, and the
client dispatches it by the path in its header — `dispatch_by_path` of B-038, in the other
direction.

**Replenishment is driven by the first byte, not by the end of the copy.** A parked
connection is spent the moment the peer writes on it, so `LocalRecv` signals a deficit on
its first successful read (and on drop, for one that dies unused); a maintainer task per
subscribing connection parks a replacement per deficit. Refilling at the *end* of a copy
would have shrunk the pool for the whole duration of a long transfer.

**Both bounds are real.** A parked connection is capped twice: by `max_parked_reverse`,
the pool's own ceiling, and by `max_local_streams`, which counts it like any other live
local connection — on this transport a stream is a file descriptor, so the pool is a
descriptor budget. A permit of the connection's slot semaphore is that accounting, and
the pool takes its permits with `StreamSlot::try_acquire`: filling a pool is a refusal
when the budget is spent, where a transfer waits (B-059).

**An empty pool is a drop, not a stall and not a teardown.** `Link::open_uni` on the
accepting side returns the new `Error::NoParkedConnection`, and the per-subscriber writer
counts that copy in `Publisher::dropped` and carries on — the same answer an exhausted
subscriber byte budget already gets, and the reason 0012 gave for choosing a drop: a local
subscriber that replenishes slowly must not be able to stall a publisher serving remote
ones. `an_exhausted_reverse_pool_drops_the_copy_and_counts_it` runs a pool of one against
a subscriber that never reads, and asserts both halves: the drop is counted within a
handful of publishes — the other two drop causes need 1024 queued copies or 8 MiB of them,
so the cause is unambiguous — and a later copy still arrives, so the subscription survived.

**A subscriber that parks nothing is refused when it subscribes.** With
`max_parked_reverse = 0` there is no route for a copy at all, so `Subscriber::connect`
returns `Unsupported` rather than registering a subscription that would be silent
(`a_subscriber_that_parks_nothing_is_refused_at_connect`). This replaces B-038's blanket
refusal of Pub/Sub on this transport.

**The shared Pub/Sub body now runs over all three transports.** `pub_sub_over_unix` uses
the same fan-out body as QUIC and inproc, which is the strongest statement available that
the pool is not a special case: nothing above the transport knows it is there. 17 tests in
`transports.rs`.

**Deliberately deferred** (recorded now, not discovered later):

- Connecting publishers and binding pushers; v0 fixes Pub/Pull as binders and Sub/Push as
  connectors.
- Streaming fan-out — tee-ing one long stream to many subscribers needs its own drop and
  ordering design. **Delivered later as B-064** (`Publisher::open`, §4.1 of
  [PATTERNS.md](PATTERNS.md)): the drop design turned out to be the per-subscriber drop this
  increment already had, applied per transfer instead of per message.
- Per-producer ordering. Ordering is `None` for both new patterns; a sequence field would
  be a protocol addition, not an implementation detail.
- Coalescing backpressure (master doc §27); only `Block`, `Reject` and fan-out `Drop` exist.
  **Answered later as B-065** ([decisions/0016](decisions/0016-conflation.md)): the level stays
  reserved for the L2 broker with its definition written down — a keyed queue at a hop, the key
  being the DATA `topic` — and a fan-out coalescer at L1 is *refused*, because holding the
  superseded value per key per subscriber is the materialization B-064 exists to remove. v0's
  conflation is the producer's single slot over `Publisher::open` + `write_now`.
- PAIR, BUS and SURVEYOR/RESPONDENT: mapped onto L0 in
  [ARCHITECTURE.md](ARCHITECTURE.md), deliberately not implemented until a use case asks.
  **Delivered later as B-236, B-237 and B-238**, when the owner asked for the full family:
  `weida::Paired`, `weida::Surveyor`/`Respondent` and `weida::BusMember`, and the mapping held
  — not one byte of wire vocabulary was added for any of the three ([PATTERNS.md](PATTERNS.md)
  §6).
- Router/Dealer as first-class types, and the broker work they would actually require
  (master doc §47, §85, Phase 6).

**Delivered in the thirteenth increment — the SP codec (B-050):**

`crates/nng/weida-sp`, the Phase B2 counterpart of `weida-zmtp` and under the same
rule: an **empty `[dependencies]`**, not even `weida-core`, so the codec is checkable
against a foreign specification instead of against our reading of it
([ARCHITECTURE.md](ARCHITECTURE.md) §4). Four modules, one per decode entry point: the
8-octet TCP protocol header (`header`), the 64-bit message framing (`message`), the
REQ/REP and survey tag stack (`backtrace`), and PAIR v1's hop count (`pair`). Every
decode takes its bound as an argument — a byte cap for the message, a hop cap for the two
header stacks — and rejects from the declared value alone, before anything is allocated.
42 tests: 21 unit, 13 golden vectors, 7 hostile-input smoke tests, 1 doctest, plus five
`cargo fuzz` targets (`header`, `message`, `message_stream`, `backtrace`, `pair`).

**Four places where the specification, the research sheet and NNG disagree.** Each is a
decision here rather than a comment in the code, because each one changes what a real peer
will accept and the interop suite must settle it:

1. **Endpoint type numbers are the implementation's.** The SP RFCs assign the 12-bit
   protocol IDs and delegate the 4-bit endpoint role to the per-protocol RFCs, which never
   published them (sp-protocol-ids-01 §1-§2). **Decision:** take the role halves from NNG's
   registry (`NNI_PROTO(major, minor) = major * 16 + minor`, `src/core/protocol.h`), which
   is what both NNG and mangos put on the wire, and publish all eleven as golden vectors so
   the assumption is visible rather than buried in a match arm.
2. **PAIR v1's initial hop count differs by one.** The research sheet says the counter is
   "initialized to one and incremented at each node"
   ([research/nanomsg-nng.md](research/nanomsg-nng.md) §4); NNG's current source appends
   `0` on a cooked send and increments on receipt (`src/sp/protocol/pair1/pair.c`).
   **Decision:** encode NNG's `0`, decode both without complaint — the difference is a
   count, not a format — and name the disagreement in the module documentation, in
   `pair::INITIAL_HOPS` and in the crate documentation. A test against a real NNG peer closes
   the question; until then the codec follows the implementation rather than the prose.
3. **The hop ceiling has two values.** The sheet documents `MAXTTL` as 1-255 with 8 the
   common default (§11); NNG's `NNI_MAX_MAX_TTL` is **15** (`src/core/defs.h`), and its own
   comment says the buffer sizing is why. **Decision:** ship all three as named constants
   (`DEFAULT_MAX_HOPS = 8`, `NNG_MAX_HOPS = 15`, `SPEC_MAX_HOPS = 255`) and make the bound a
   parameter of `backtrace::decode`. A codec that picked 255 would accept stacks a real peer
   drops; one that picked 15 would silently narrow the specification. The caller chooses and
   the unit test asserts that the two ceilings are in fact different.
4. **An oversized message is fatal here and recoverable in NNG.** NNG discards a message
   beyond `RECVMAXSZ` and keeps the pipe (§8), which it can do because it owns the socket
   and can drain the declared octets. **Decision:** `MessageError::BodyTooLarge` reports
   `is_violation() == true`, because a sans-I/O decoder cannot drain anything — the next
   message begins after a body it refused to read — and draining is left to a caller that
   has the socket. The same reasoning already applies to `weida-zmtp`'s `BodyTooLarge`, for
   a different upstream reason (libzmq disconnects).

**What the golden vectors are for.** `crates/nng/weida-sp/tests/golden_vectors.rs` asserts
every accepted vector in both directions and every rejected vector on the decoder. Encoding
alone would not catch a decoder wrong in the same way, which is the failure mode that matters
when the peer is NNG and not us.

**Not in this slice:** no I/O and no weida types. SP socket state, topic conventions and
transport policy belong to the standalone `weida-nng` library above the codec.


**Delivered in the seventeenth increment — named pipes on Windows (B-039), which closes
Phase A:**

The third local transport of [0010](decisions/0010-local-transport.md) §4.1, and the one
that waited for a runner rather than for a design. It was built and gated on a Windows 11
VM (`win11-geselle`, MSVC, stable 1.98.1) beside the Linux gate: the same 0012 grouping over
`\\.\pipe\<name>`, and the same three pattern bodies of `tests/transports.rs` run over
it unchanged, plus the principal, the unknown-token refusal, the slot tests and two claims
only a pipe can make.

**The grouping was written once, and this is the increment that proved it.** `unix.rs`
held 0012's whole mechanism bound to `tokio::net::UnixStream`; it is now
`grouped.rs`, generic over a `Stream` trait with the eight things a transport contributes
— dial, principal, split, same-peer, identity, `finish`, `reset`, `stop` — and `unix.rs`
is the eighty-line socket implementation. `Link`, `SendHalf` and `RecvHalf` gained a `Pipe`
variant over the same generic types, boxed so the enum stays the size of its QUIC
variant. The accept path is one generic `LocalAccept::serve`; the two loops around it —
`accept()` on a listener, `connect()` on an instance that is replaced before it is served
— are the only per-transport code in `listener.rs`.

**A pipe has no half-close, so the pipe carries its streams in chunks.** A socket ends a
stream by shutting down one direction; a pipe handle closes whole and would take the reply
with it. Everything after the local preamble on a pipe connection is therefore chunked —
`0x00` + length + payload, `0x01` FIN, `0x02` + code RESET ([PROTOCOL.md](PROTOCOL.md)
§2.1) — at five bytes per write and nothing per byte, written through the same
`AsyncWrite` polls as the payload. It carries what the socket could not, a reset code:
`an_abandoned_request_over_a_pipe_is_a_named_cancellation` cancels a request mid-payload
and the replier reads `Error::Canceled` rather than a truncated body. What it cannot carry
is the reader's refusal, because the only direction a reader could write on is the one the
reply owns; a stopped pipe reader **drains** the rest on the runtime instead, so the writer
never blocks on a pipe nobody reads and learns of the refusal from the reply. That is
0012 §4.7's named loss moved from the write to the receipt, and it is stated in the
protocol document rather than discovered. **Byte mode, not message mode**, against 0010
§4.5's letter and for its reason: the kernel's framing would go unused under 0012, and
`mio` reads a message-mode pipe into a fixed buffer where a longer message is
`ERROR_MORE_DATA` and an error.

**Identity is a SID.** `PeerIdentity::Windows(WindowsPrincipal { sid, pid })`: the client's
token SID through `ImpersonateNamedPipeClient` on the accepting side, read only after the
first byte because a client that has not written cannot be impersonated, and reverted on
every path by a guard; the pipe object's owner SID on the dialling side, which the
creator's token set. The SID is an `Arc<str>` so that `PeerIdentity` — copied into every
incoming transfer's metadata — stays a refcount bump to clone; `PeerIdentity` lost `Copy`
for it, and six call sites gained a `.clone()`. Clients open with
`SECURITY_IDENTIFICATION`, so a server may learn who a client is and may not act as it.
`a_pipe_peer_presents_the_principal_the_kernel_proved` asserts the shape and that the pid
is this process's.

**The hygiene is `weida-runtime`'s, as on unix.** `BoundPipe::bind` creates the first
instance with `FILE_FLAG_FIRST_PIPE_INSTANCE` — a name that exists is refused as
`InvalidAddress`, which is the answer to pipe squatting — an owner-only DACL built from
the process token's SID (`O:<sid>D:P(A;;GA;;;SY)(A;;GA;;;<sid>)`, the pipe's `0600`), and
`PIPE_REJECT_REMOTE_CLIENTS`; `next_instance` recreates the same after every accept, so a
client between accepts finds the pipe busy rather than absent; `connect_pipe` waits out
`ERROR_PIPE_BUSY` on the runtime's timer for five seconds instead of blocking a thread in
`WaitNamedPipe`, and reports a missing pipe as `ConnectionLost(PeerClosed)` so a caller
redials. Three runtime tests pin the SID agreement, the taken-name refusal and the absent
pipe. `PIPE_UNLIMITED_INSTANCES` turned out to be what "255" means, so the per-peer
`max_local_streams` is the only ceiling on every platform and INVARIANTS closes its
pending entry.

**One crate may use `unsafe`, and it is thirteen kilobytes.** No dependency exposes a
security descriptor on `CreateNamedPipe`, `ImpersonateNamedPipeClient` with a token read,
`GetSecurityInfo` or the pipe pids safely — `interprocess` comes closest and stops before
the SID — so `crates/winpipe` (`weida-winpipe`) wraps those fifteen Win32 calls from
`windows-sys` behind `OwnerOnlyDacl`, `create_instance`, `open_client`, `client_peer` and
`server_peer`, each `unsafe` block one call with its precondition stated,
`unsafe_op_in_unsafe_fn` forbidden, and the crate empty off Windows. Every other crate,
`weida-runtime` included, keeps `unsafe_code = "forbid"`.

**Verified.** Linux: fmt, clippy in both configurations, **1680 tests** with 37 ignored,
rustdoc with `-D warnings`. Windows, same commit: build, fmt, clippy in both
configurations, **1658 tests** across 119 binaries with 37 ignored, rustdoc. The address
form is checked on both: `weida+pipe://<name>/<path>`, name 1..=256 bytes, no backslash,
no `sha256:…@`, mapped to `\\.\pipe\<name>` and never a UNC path; on a platform without
pipes the dial refuses it by name, as `weida+unix://` is refused on Windows.

---

## 2. Mandatory development loop per phase

For every phase (master doc §80):

```text
design
    ↓
implement
    ↓
unit test
    ↓
failure test
    ↓
fuzz relevant boundaries
    ↓
benchmark relevant hot paths
    ↓
check invariants
    ↓
refactor as if architecture had always been designed this way
    ↓
continue
```

Architectural debt MUST NOT be deferred to a later phase. "A later phase will clean this up"
is not an accepted justification for accumulating it.

---

## 3. Agent implementation rules

Verbatim from master doc §81. The implementation agent must follow these rules:

1. Read the architectural documents before changing foundational components.
2. Do not duplicate messaging semantics between brokered and brokerless paths.
3. Do not introduce complete-payload materialization into core APIs.
4. Do not add a permanent control stream merely for convenience.
5. Do not interpret endpoint paths beyond opaque lookup.
6. Do not silently weaken requested guarantees.
7. Do not advertise end-to-end guarantees when only next-hop state is known.
8. Do not mix user replies with protocol ACK semantics.
9. Do not put bulk payload replication through Raft without an exceptional demonstrated reason.
10. Do not expose broker topology to normal clients as a requirement.
11. Do not make Web a second-class feature subset.
12. Do not make Rust typing the only representation of protocol roles.
13. Do not add large dependencies without justification.
14. Do not optimize by disabling correctness for configurations that request stronger guarantees.
15. Conversely, do not impose strong-guarantee overhead on weak-guarantee configurations.
16. Prefer clear explicit state machines over distributed implicit state.
17. Prefer bounded resources everywhere.
18. Treat all network input as hostile.
19. Keep observability available from the beginning.
20. Preserve the ability to refactor the `0.x` protocol when real-world findings justify it.

---

## 4. Current increment acceptance

The reference prototype of master doc §83 is the acceptance criterion for Phases 0-2:

```text
Process A:
    Runtime
    Endpoint<Req>

Process B:
    Runtime
    Listener
        QUIC binding
        /transform Endpoint<Rep>
```

Each numbered requirement below is verbatim from §83, mapped to the artifact that
demonstrates it. Requirement 9 is the one the project has since decided against; the row
says so rather than quietly claiming it.

| # | Requirement | Demonstrated by |
| --- | --- | --- |
| 1 | Establish QUIC connection. | `echo_roundtrip` in `crates/weida/tests/reqrep.rs`; examples `transform_server` + `transform_client` |
| 2 | Negotiate protocol. | `echo_roundtrip`; `hello_with_an_unsupported_version_fails_negotiation` in `crates/weida/tests/hostile.rs` asserting close code `NEGOTIATION_FAILED` |
| 3 | Open uni request stream. | `echo_roundtrip` — but on a **bidirectional** stream: the re-architecture replaced §83's paired unidirectional streams with one exchange ([PROTOCOL.md](PROTOCOL.md) §4). A unidirectional request stream is still the Push/Pull shape (`push_best_effort` in `crates/weida/tests/pushpull.rs`) and the raw L0 `Peer::open` |
| 4 | Send metadata header. | golden-vector tests in `crates/protocol/tests/golden_vectors.rs`; `echo_roundtrip` |
| 5 | Stream arbitrary-sized input. | `streaming_overlap` in `crates/weida/tests/reqrep.rs`; example `large_stream` |
| 6 | Server begins processing before FIN where possible. | `streaming_overlap` (handler consumes the first chunk before the request FIN) |
| 7 | Server opens correlated reply stream before request necessarily completes. | `streaming_overlap` (the handler replies on the exchange's reply half after the first chunk; the client withholds the remaining payload until at least one reply byte has arrived, making the overlap deterministic). The reply half exists from the moment the exchange is opened, so "opens" is now free — and the correlation is the stream, not an id |
| 8 | Stream reply simultaneously. | `streaming_overlap`; the two halves are independent QUIC streams |
| 9 | Send protocol ACK on a separate control stream. | **Deliberately not met.** The v0 core carries no application ACK: brokerless, it would only mean "arrived in RAM", which QUIC already guarantees (§1, Phase 3). What remains is the transport receipt, `push_delivery_receipt` in `crates/weida/tests/pushpull.rs` asserting `Delivery::delivered()`. Accepted / Stored / Replicated / Processed are reserved for the Phase 6 broker ([GUARANTEES.md](GUARANTEES.md)) |
| 10 | Propagate OpenTelemetry trace context. | `trace_propagation` in `crates/weida/tests/reqrep.rs`; `traceparent` fuzz target under `crates/protocol/fuzz`; the trace id printed by `transform_client` and logged by `transform_server` |
| 11 | Cancel mid-transfer. | `cancel_mid_transfer` and `reply_abort_when_reply_stream_dropped` in `crates/weida/tests/reqrep.rs`; `canceled_resolves_when_the_requester_walks_away` for the handler-side signal |
| 12 | Test connection loss. | `crates/weida/tests/hostile.rs`: `a_server_that_never_answers_yields_indeterminate` (closed after FIN) and `a_server_that_disappears_mid_stream_yields_connection_lost` |
| 13 | Test malformed headers. | `crates/weida/tests/hostile.rs`: garbage preamble and `header_len = 1 MiB` both assert close code `PROTOCOL_VIOLATION`; `weida-protocol` unit tests for duplicate keys, missing required keys (`missing_required_keys_are_rejected`) and oversized lengths |
| 14 | Fuzz parser. | fuzz targets `preamble`, `data_header`, `hello`, `traceparent`, `roundtrip`, `subscribe`, `cursor`, `credit` and `error` under `crates/protocol/fuzz`, plus the deterministic `fuzz_smoke_*` tests in `crates/protocol/tests/fuzz_smoke.rs`. The fuzz crate declares its own `[workspace]`, so no workspace-wide build ever compiles it: `roundtrip` was broken by the DATA fields that arrived with keys 8-11 and did not compile again until its generator learned them. The recorded runs below therefore cover the targets that existed at the time of each run, not this list |
| 15 | Benchmark small and large transfers. | `crates/protocol/benches/codec.rs` (header encode/decode); `crates/weida/benches/echo.rs` (`echo_1kib_rtt`, `echo_1kib_rtt_explicit`, `stream_throughput_64mib`) |
| 16 | Demonstrate bounded memory with a multi-GB generated stream. | `large_stream_bounded_memory` in `crates/weida/tests/large.rs` (ignored by default; run with `--ignored`), asserting checksum equality and peak RSS below 512 MiB for a 1 GiB echo; example `large_stream` for ad-hoc runs |

This prototype proves the central architecture before the feature surface expands.

### Verified results — Phases 0-2

Recorded from the run that closed that increment, on an AMD Ryzen 7 5800X, Linux, stable
Rust 1.97.1 (nightly 1.100.0 for the fuzz targets only). Left exactly as recorded: it
measures a wire that no longer exists, so its ACK rows are history, not documentation.

| Check | Command | Result |
| --- | --- | --- |
| Whole workspace | `cargo test --workspace` | 182 tests pass, 1 ignored (the 1 GiB memory test); `weida-core` 58, `weida-protocol` 69 unit + 8 fuzz-smoke, `weida` 23 unit + 11 Req/Rep + 12 hostile + 1 doc |
| Two-process prototype | `transform_server` + `printf 'hello weida' \| transform_client --ack` | stdout `HELLO WEIDA`; stderr `outcome=Acked(Accepted)`; the printed trace id appears in the server's request log line |
| Bounded memory, in process | `cargo test --release -p weida --test large -- --ignored` | 1 GiB echoed each way, checksums equal, 905 MiB/s, peak RSS **24.0 MiB** against a 512 MiB ceiling |
| Bounded memory, two process | `large_stream --gib 4` against the release server | 4 GiB echoed byte for byte, 908.7 MiB/s both directions, peak RSS **14.4 MiB** |
| Fuzzing | `cargo +nightly fuzz run <target> -- -runs=200000 -max_len=20000` | the five targets that existed then — `preamble`, `data_header`, `hello`, `traceparent`, `roundtrip` — zero crashes, zero OOMs |
| Header codec | `cargo bench -p weida-protocol` | DataHeader encode 100.2 ns / decode 60.8 ns minimal, 218.5 ns / 146.8 ns fully populated; ACK 32.8 ns / 20.2 ns; preamble parse 6.9 ns |
| End to end | `cargo bench -p weida` | 1 KiB echo round trip 82.9 us best effort, 113.5 us with an `Accepted` ACK; 64 MiB streaming echo at 1.04 GiB/s counting both directions |

Peak RSS two orders of magnitude below the payload size is the substantive result: no stage
of the path buffers a whole transfer, which is what requirement 16 exists to establish.

No thresholds are asserted on the benchmark numbers this increment; they are the baseline
for later comparison.

### Verified results — Phase 3, first increment

Same machine and toolchains. The Phase 0-2 numbers above are left as recorded; these are
the checks that closed the Push/Pull and Pub/Sub increment, and they too predate the
re-architecture — the `Accepted` ACK figures below no longer have a wire to run on.

| Check | Command | Result |
| --- | --- | --- |
| Whole workspace | `cargo test --workspace` | 215 tests pass, 1 ignored (the 1 GiB memory test); `weida-core` 58, `weida-protocol` 76 unit + 9 fuzz-smoke, `weida` 26 unit + 11 Req/Rep + 7 Push/Pull + 11 Pub/Sub + 16 hostile + 1 doc |
| Golden vectors | `cargo test -p weida-protocol` | the four new vectors byte-exact both directions: oneshot DATA, fan-out DATA with `topic`, SUBSCRIBE, UNSUBSCRIBE |
| Drop policy | `slow_subscriber_drops_not_blocks` | 100 x 32 KiB published against a 64 KiB per-subscriber budget: the whole publish loop completes inside the deadline, the draining subscriber receives all 100, `Publisher::dropped() > 0` |
| Filtering | `subscribe_filters_topics_by_segment` | a non-matching topic published between two matching ones never arrives; proven by a FIFO sentinel rather than a sleep |
| Selection policy reuse | `push_round_robins_two_peers` | 8 pushes over 2 peers split 4/4 by destination path, each message delivered exactly once |
| Hostile subscriptions | `cargo test -p weida --test hostile` | 257-byte filter → `PROTOCOL_VIOLATION`; `max_subscriptions + 1` distinct filters → `LIMIT_EXCEEDED`; oneshot DATA ahead of HELLO is parked and then delivered |
| Fuzzing | `cargo +nightly fuzz run <target> -- -runs=200000 -max_len=20000` | all six targets including the new `subscribe`, zero crashes, zero OOMs |
| Push | `cargo bench -p weida --bench patterns` | 1 KiB push 27.6 us best effort (35.4 MiB/s), 83.0 us with an `Accepted` ACK (11.8 MiB/s) |
| Fan-out | `cargo bench -p weida --bench patterns` | 1 KiB to 8 subscribers, published and fully drained by all 8: 65.2 us (119.8 MiB/s aggregate) |

The fan-out number is the one worth reading carefully: it measures publish **plus** eight
receives, so it is a whole-cycle figure, not the cost of `publish` itself, which is a
non-blocking enqueue.

No thresholds are asserted on these numbers either.

### Verified results — Phase 3, layered re-architecture

Same machine and toolchains. `cargo test --workspace` green and
`cargo fmt --all --check` clean. `cargo clippy --workspace --all-targets` emits no
diagnostic in any file this increment touched; the workspace's one remaining warning,
`chunks_exact` with a constant chunk size in the untouched `crates/core/src/trace.rs`,
predates it and is the only thing standing between the tree and a green
`clippy -D warnings`.

| Check | Command | Result |
| --- | --- | --- |
| Whole workspace | `cargo test --workspace` | 188 tests pass, 1 ignored (the 1 GiB memory test); `weida-core` 20, `weida-protocol` 70 unit + 6 golden-vector + 9 fuzz-smoke, `weida` 28 unit + 13 Req/Rep + 7 Push/Pull + 11 Pub/Sub + 21 hostile + 2 raw-stream + 1 doc |
| Golden vectors | `cargo test -p weida-protocol --test golden_vectors` | all six vectors recomputed and byte-exact both directions: HELLO, DATA request, DATA reply (empty map `A0`), DATA fan-out with `topic`, ERROR `{code:5}`, SUBSCRIBE/UNSUBSCRIBE |
| Transport receipt is not an application ack | `push_delivery_receipt` | `Delivery::delivered()` resolves **before** the puller calls `recv()` — a 1 KiB payload fits the flow-control window, so the receipt demonstrably reports the peer's transport, not its application |
| Cancellation without a CANCEL frame | `reply_abort_when_reply_stream_dropped`, `canceled_resolves_when_the_requester_walks_away` | dropping the `ReplyStream` stops the reply half; the handler's `canceled()` future fires and its subsequent writes fail `Canceled`. The second test is verified by mutation: stubbing the future to `pending` makes it time out |
| Typed refusal on the reply half | `bidi_request_to_a_pull_path_is_refused_with_unsupported` | a raw bidirectional DATA to a puller path is answered with a real ERROR `{UNSUPPORTED}` frame and `STOP_SENDING(UNSUPPORTED)`; the connection survives |
| Public L0 API | `acceptor_receives_both_stream_kinds` in `crates/weida/tests/raw_streams.rs` | one `Acceptor` on `/raw` receives `Incoming::Stream` from `Peer::open` and `Incoming::Exchange` from `Peer::open_bi`, and the exchange reply round-trips |
| Two-process prototype | `transform_server` + `printf 'hello weida' \| transform_client` | stdout `HELLO WEIDA`; stderr `delivered sent=… received=…`; the printed trace id appears in the server's request log line |
| Fuzzing | `cargo +nightly fuzz run <target> -- -runs=200000 -max_len=20000` | all six targets against the new field set, zero crashes, zero OOMs |
| End to end | `cargo bench -p weida` | see the table below |

| Benchmark | Now | Previously recorded |
| --- | --- | --- |
| `echo/echo_1kib_rtt` | 58.2 us | 82.9 us over two uni streams plus control — **−30 %** |
| `echo/echo_1kib_rtt_explicit` | 59.3 us | new: `open_bi`/`finish`/`recv` on the raw handles |
| `stream/stream_throughput_64mib` | 130 ms | +7 % against the Phase 0-2 run |
| `push/push_1kib_best_effort` | 7.9 us | 27.6 us — **−72 %**, no registration round trip |
| `push/push_1kib_delivered` | 26.2 ms | replaces `push_1kib_acked` |
| `fanout/pub_1kib_8_subscribers` | 62.1 us | 65.2 us — unchanged within noise |

The echo number is the substantive one: collapsing an exchange from two unidirectional
streams plus correlation bookkeeping into one bidirectional stream took 30 % off the round
trip, which is what removing work rather than tuning it looks like.

`push_1kib_delivered` at ~26 ms is not a weida cost. On an idle loopback connection the
peer delays its acknowledgement up to QUIC's max ack delay, and `delivered()` waits for
exactly that acknowledgement. The receipt is a correctness signal, not a latency-sensitive
one, which is why `Pusher::send` discards it and callers who want it reach for
`Pusher::open` + `finish()` + `delivered()`.

No thresholds are asserted on these numbers either.

### Verified results — Phase 3, identity and stream semantics

Same machine as the runs above; every stream number was measured on loopback in a debug
build against `quinn 0.11.11` / `quinn-proto 0.11.17`. `cargo test --workspace` green four
times in a row, `cargo fmt --all --check` clean, `cargo clippy --workspace --all-targets
-D warnings` clean with and without the `generate` feature, `cargo doc` without warnings. The
byte counts below are observations; each test asserts the race-free bound around them, which
is noted where the two differ.

| Check | Command | Result |
| --- | --- | --- |
| Whole workspace | `cargo test --workspace` | green: 217 tests pass, 1 ignored (the 1 GiB memory test). Counted from the test files: `weida-core` 24, `weida-protocol` 70 unit + 6 golden-vector + 9 fuzz-smoke, `weida` 33 unit + 13 Req/Rep + 7 Push/Pull + 11 Pub/Sub + 21 hostile + 2 raw-stream + 10 identity + 10 stream + 1 doc |
| Identity and trust | `cargo test -p weida --test identity` | the 10 tests listed in §1: pinned addresses, a refused pin reported as `Untrusted` with the fingerprint that answered, an address pin overriding an anchor, a pinned trust accepting the key and only the key, pins and anchors composing, name checking on anchors only, a binding requiring client identity, an anonymous client as `None`, the server's fingerprint on the reply half, and no connection sharing across different terms |
| Two-process prototype | `transform_server --bind … [--identity PATH] [--cert-out PATH]` + `transform_client [--ca PATH] URL` | a pinned address round trip prints `HELLO WEIDA`; a wrong pin exits 1 with "presented sha256:…, which is not trusted"; a plain address without `--ca` fails with "nothing to trust" before any packet; restarting with `--identity` keeps the fingerprint, so the printed URL is unchanged; `--cert-out` plus `--ca` against a pinned address works |
| A receipt beyond the window implies the reader consumed | `a_receipt_beyond_the_window_implies_the_reader_consumed` | 160 KiB pushed into a 64 KiB stream receive window: no marker at all within 300 ms while nothing is read; with an 8 KiB-chunk reader, `write_all` returns at **131072 bytes consumed** and the receipt resolves at **163840** (the whole payload). The assertion is the flow-control bound, `payload - window = 98304` |
| The stream window is not a payload budget | `a_payload_the_size_of_the_stream_window_waits_for_the_reader` | a payload of exactly 65536 bytes against a 64 KiB window produces no `Wrote` within 300 ms although the header was already parsed; it completes as soon as the reader consumes one eighth of the window (8192 bytes). The DATA header spends the same window as the payload |
| The connection window is the shared resource | `a_stalled_stream_does_not_block_its_siblings` | 32 KiB payloads, 64 KiB stream window, 256 KiB connection window: siblings of an unread stream arrive and read back correctly, and the stall lands at **7 unread streams = 229376 of 262144 bytes**; reading one stream's 32 KiB releases the stalled writer. The test asserts the bound `1 + blocked_at <= connection_window / payload` and the unblocking, not the index |
| The stream budget is backpressure, and `open` is where it lands | `the_stream_budget_is_backpressure_not_an_error`, `a_deeper_endpoint_queue_does_not_raise_the_stream_budget` | with `max_concurrent_uni_streams = 2` the third transfer blocks in `Pusher::open` — not in `write_all`, not in the receipt — and never errors; reading one transfer to EOF releases it. `endpoint_queue = 8` changes nothing, because a transfer parked in the queue still owns its stream |
| Cancellation | `cancel_discards_unread_bytes_and_keeps_read_ones` | the 4 KiB already read still compares equal; after `cancel()` the reader is served the whole 4 KiB buffered remainder and only then fails, as `io::ErrorKind::ConnectionReset` on the `AsyncRead` and as `Error::Canceled` through `read_capped` — never EOF |
| Idle timeout is connection loss | `idle_timeout_reports_loss_within_the_window` | a 500 ms server idle timeout against the client's default 10 s keep-alive (asserted to be the longer of the two): after 1.5 s of silence the endpoint reports `PeerEvent::Lost { cause: IdleTimeout }` — the cause, not only the outcome, since B-028 — and, since 0031, the next request rides the redialled connection |
| A lost peer is reported, and the redial is the runtime's | `a_server_that_goes_away_is_reported_as_connection_lost`; `crates/weida/tests/reconnect.rs` | under `ReconnectPolicy::never()` the **first** send after the server closed fails with `ConnectionLost(PeerClosed)` and `peer_count()` drops to 0. Under the default policy (B-270 to B-273): a server restarted on the same address is redialled with `Lost`, `Retrying`, `Connected` on the event stream, over QUIC, `AF_UNIX` and inproc alike; a body sent during the outage arrives after it, 100 of them in order; `open` waits through the outage and `send_timeout` bounds the wait with the loss cause; a subscriber's filters — including one added during the outage — are on the new connection; a replacement server with a new key is refused as `GaveUp { PeerChanged }`; the outbox bound blocks or drops as `outbox_full` says and a body over `outbox_bytes` is `LimitExceeded` |
| Hot paths unchanged | `cargo bench -p weida` (3 s measurement, both trees on the same idle machine) | `echo_1kib_rtt` 63.5-67.3 µs over four runs of this tree against 62.9 µs for the tree before it; `push_1kib_best_effort` 7.9 µs against 7.9; `pub_1kib_8_subscribers` 71.7 µs against 70.3. All within the run-to-run spread of this machine: the verifier runs once per handshake and the per-stream path gained one `Option<[u8; 32]>` copy |

The two probes that resisted determinism are recorded as observations only: the buffered tail
delivered after `cancel()` is a genuine race between the reset frame and the reader's polls,
and the exact stall index in the connection-window probe depends on header sizes. Both tests
assert the race-free part instead, and no stream probe is `#[ignore]`d.

### Verified results — DATA header cost at a high message rate (B-009)

What the two DATA keys of [decision 0001](decisions/0001-sequence-field.md) cost per message.
No sender writes either key, so each was simulated by the existing key with its exact wire
shape: `content_len` for the `uint` sequence, and a `sha256:<64 hex>` `content_type` for the
producer identity. Both endpoint paths are six bytes, so the two keys are the only difference.
Same machine as the runs above, release profile, loopback, debug assertions off.

| Check | Command | Result |
| --- | --- | --- |
| Bytes per message | `cargo bench -p weida --bench patterns -- header` (printed by the bench) | minimal DATA frame **135 B** for a 64-byte payload; with the two keys **215 B**, i.e. **+80 B**, +59.3 % of the whole frame. The split is `content_len` 6 B (1 B key + 5 B `uint`) and the 71-byte producer string 74 B (1 B key + 2 B `tstr` prefix + 71 B) |
| Messages per second | `cargo bench -p weida --bench patterns -- header --warm-up-time 1 --measurement-time 3`, two runs | minimal `push_64b_keys0` 5.57 µs → **179.4 Kmsg/s** (5.43-5.71 µs); with two keys `push_64b_keys2` 6.14 µs → **162.9 Kmsg/s** (6.01-6.27 µs). Run-to-run change on the same tree was within ±3 % (p = 0.38-0.41), so the **−9 % throughput** between the two variants is larger than this machine's spread |

Two conclusions carried into the wire work. First, the cost is the bytes, not the encoding:
80 B more header for a 64-byte payload costs 9 % of the message rate, and the same two keys on
a 64 KiB payload are noise. Second, the 215 B figure is the **worst case**, an explicitly named
producer spelled as `sha256:<64 hex>`. Decision 0008 §4.4 settled the encoding from these
numbers: the receiver already knows the sending peer's fingerprint from the handshake, so the
producer key is omitted entirely in the default case and the default pays only the sequence's
6 B; where the producer is not the connection peer the key is a CBOR `bstr` holding the raw
32-byte digest, 35 B instead of 74 B, and the hex string stays presentation only. B-013
landed exactly that codec and its golden vectors; the measured frame above is unchanged,
because the runtime still sets both fields to `None` (§1, fourth increment).

**The baseline these figures sit on is gone.** Every frame above carried an unconditional
55-byte `traceparent` — 60 B of frame with its key and the length field — which B-246 removed:
a context is propagated and never minted
([0028](decisions/0028-trace-propagation-is-the-callers.md)). The minimal frame is now **76 B**
rather than 135 B, so the two keys' +81 B is now **106 %** of a frame rather than 59 % of one,
and the conclusion that the cost is the bytes rather than the encoding is unchanged. The
re-measurement is the next section; these numbers stay because they are what decision 0008 was
taken from.

### Verified results — what the frame costs once nothing mints a trace (B-246)

`data_header` used to set `traceparent: Some(…)` on **every** DATA header of every pattern and
mint a fresh root when the caller supplied none, so a 64-byte push paid a fixed 60 bytes of
frame for a trace context nobody asked for — the largest single item in it, against ZMTP's one
to nine bytes of framing per message and SP's eight. [GUIDE.md](GUIDE.md) §0.3's C8B arithmetic
is what made that urgent: ≈480 GB of an ≈1.08 TB message-to-everybody.
[0028](decisions/0028-trace-propagation-is-the-callers.md) decided the honest half of it — a
minted root is a fabricated fact, not a safe default — and this is what it measured.

`cargo bench -p weida --bench patterns -- header --warm-up-time 1 --measurement-time 3`, two
runs, release, loopback, same desktop. The bench's `wire_bytes` helper no longer rebuilds a
header with a hardcoded `traceparent`; it builds the header the send path builds, and a third
variant, `push_64b_traced`, measures a caller-supplied context in the same run.

| Variant, 64-byte payload | Frame | Median | Rate |
| --- | --- | --- | --- |
| `push_64b_keys0`, no trace context | **76 B** | 5.11-5.19 µs | **192.6-195.9 Kmsg/s** |
| `push_64b_traced`, caller's context | **136 B** (+60 B) | 5.84-5.92 µs | 168.9-171.2 Kmsg/s |
| `push_64b_keys2`, the two 0001/0008 keys | 157 B (+81 B) | 5.75-5.83 µs | 171.5-173.8 Kmsg/s |

**A 64-byte message is twelve bytes of framing**, half of it the six-byte endpoint path, so
84 % of the wire is payload. weida is now in ZMTP's and SP's class on small messages; it used
to be 71 bytes worse than either.

**A context costs 60 bytes of frame and 13-14 % of the message rate.** Both variants are
measured in one run, which is the only honest comparison on this machine: B-021 recorded a
run-to-run spread wider than several of the effects it wanted to see, and the 179.4 Kmsg/s of
B-009 is from another day. The 58 bytes of that 60 are the header (1 B key, 2 B `tstr` prefix,
55 B value) and the other 2 are the frame's own length field; a unit test asserts the 58 and
the bench prints the 60.

**What did not change**: `tracestate` still travels only where a context does, `IncomingMeta`
still yields `None` for an absent **and** for a malformed value, so no receiver gained a state
it did not already have, and a peer that writes key `3` on every frame is still conformant
([PROTOCOL.md](PROTOCOL.md) §6.2).

### Verified results — what parking a receipt costs the send path (B-032)

The drain of [0009](decisions/0009-drain.md) parks the receipt of every finished transfer
nobody is waiting on, which puts work on a path B-009 measured. Review asked for the number
before the design was accepted, so here it is, on the same bench, same machine, release
profile, loopback: `cargo bench -p weida --bench patterns -- header --warm-up-time 1
--measurement-time 3`, two runs per tree, throughput in Kmsg/s.

| Tree | `push_64b_keys0` | `push_64b_keys2` |
| --- | --- | --- |
| Before the drain (`9a82f4c`) | 163.0, 162.5 | 152.3, 153.6 |
| First cut: one runtime-wide `Mutex<VecDeque>`, reaping under the lock | 163.9, 162.7 | 147.6, 149.6 |
| Per connection: one uncontended lock, push only | 155.7, 162.2 | 147.6, 152.1 |

The first cut was the wrong shape regardless of the number — a structure shared by every
connection and every worker thread, plus a poll of quinn's connection lock under it, for an
operation that runs once at shutdown — and the restructure moved the parked set onto the
connection beside the sequencer and the gap detector, leaving the runtime-wide state with
only the admission flag, the connection list and the eviction counter.

**What the numbers do and do not show.** The keys2 column shows the first cut ~3 % below
the baseline in both runs and the per-connection version back inside it; that is the signal
that motivated the restructure. Nothing sharper than that is available from this bench:
this machine's run-to-run spread today is **wider** than the ±3 % B-009 recorded — the same
tree produced 155.7 and 162.2 Kmsg/s in consecutive keys0 runs, and lengthening the
measurement to 10 s per run did not settle it (baseline 149.0 and 162.4; per-connection
160.9, 163.6, 156.7). So the honest statement is: **the residual cost of parking is not
resolvable against this bench's noise**, and the claim in the code is worded as what it
does — one uncontended lock and a push, once per transfer — not as "free".

### Verified results — reassembly buffer under cross-stream reordering (B-010)

What an application-side reorder buffer costs when transfers complete out of dispatch order,
the second unmeasured cost of [0001](decisions/0001-sequence-field.md) §8. The probe is
`reverse_order_completion_measures_the_reorder_buffer` in `crates/weida/tests/streams.rs`: it
opens N transfers, writes a dispatch sequence into each, finishes them from the last to the
first, and reports each transfer's sequence from its own task, so what is measured is
completion order rather than the order a single-threaded reader imposes on itself. The buffer
is then simulated exactly: hold what cannot be released, release as soon as the next sequence
is present, record the peak. Debug build, loopback, same machine as the runs above; every
figure below was identical across five runs.

| Case | Command | Result |
| --- | --- | --- |
| N = 16, FINs queued back to back | `cargo test -p weida --test streams reverse_order_completion -- --nocapture` | **0 of 16** out of dispatch position, peak reorder buffer **0**. Reversing the FIN order changes nothing the application can see: the FINs are queued in one burst and quinn transmits the pending streams in its own order, so arrival order follows dispatch order |
| N = 256, FINs queued back to back | same | **90 of 256** out of dispatch position, peak reorder buffer **84** transfers (33 % of the transfers in flight), first divergence at position **29**. The first 29 arrive in order; beyond that, concurrent per-transfer tasks are the reordering source, not the FIN order |
| N = 16, each FIN awaited to its transport receipt | same | **16 of 16** out of dispatch position, peak reorder buffer **15** = N − 1, arrivals exactly reversed (`[15, 14, 13, …]`). Serializing the FINs is what makes the sender's order reach the receiver, and then the buffer must hold every outstanding transfer but one |

Two results matter for the reassembly mode of 0001 §7.5. First, the bound is N − 1 and it is
reachable: an eager reassembler must be capped by construction, which is what the INVARIANTS
follow-up of 0001 §8 asks for — the cap is a configured number of held transfers, not a hope
about arrival order. Second, at 256 transfers in flight a third of them were already held
without any adversarial pattern, so the cap has to be chosen against normal concurrency rather
than against the worst case alone. The probe asserts only the invariants — every transfer
arrives exactly once, the buffer drains empty, the peak stays below N — because QUIC promises
no ordering across streams and a test that pinned 84 would pin an accident of this machine.

### Verified results — what a connection to a peer costs (B-011)

The figure [0002](decisions/0002-control-and-bulk-separation.md) needs: what the *second*
connection to a peer that is already connected costs, in latency and in memory. The bench is
`crates/weida/benches/connections.rs`, loopback, pinned trust, no client identity (mTLS off).

The pool keys on `(host, port, ClientTls, address fingerprint)`, so a second connection to one
server on the same terms cannot be dialled from one runtime today — which is precisely why
0002 needs a second pool tier. The memory figures therefore use one client runtime per
connection against one server, with both ends in this process, and they separate the two
deltas: runtimes are built and measured *before* anything is dialled, so the per-connection
number excludes the per-runtime quinn endpoint and UDP socket.

| Check | Command | Result |
| --- | --- | --- |
| Cold handshake | `cargo bench -p weida --bench connections -- --warm-up-time 1 --measurement-time 3` | `connect/cold_handshake` **1.04-1.10 ms** over three runs (fresh runtime per iteration, only `connect` timed) |
| Pooled dial | same | `connect/pooled_dial` **3.8 µs** (3.27-4.36 µs): dialling a peer the pool already holds does no handshake and is ~**280x** cheaper than one |
| Handshakes in series | same, printed by the bench | 64 handshakes in **65.6-73.9 ms**, i.e. ~1.0-1.2 ms each with no measurable degradation: the first is 1.2-1.9 ms and the sixty-fourth 0.81-0.97 ms |
| Memory per idle runtime | same | **0-4 KiB** RSS: a `Runtime` that has dialled nothing holds nothing worth counting |
| Memory per live connection | same | **488-596 KiB** at 2 connections, **750-850 KiB** at 64 (46-53 MiB for 64), covering **both** ends. The single-connection figure is 1196-1260 KiB because it also pays the one-off crypto and endpoint state |

What this says about a second connection per peer: it costs about **one millisecond of
handshake and under a megabyte of resident memory for both ends together**, and the handshake
is the whole cost — there is no per-connection cost that grows with the number of connections
held. Against that, the head-of-line coupling one connection per path removes is unbounded:
one slow reader stalls every writer on the connection ([PATTERNS.md](PATTERNS.md) §1.3). That
is why the per-path split is worth a connection each and was built (B-017). It is also why the
*control* tier is not: a millisecond is cheap for isolation and expensive for nothing, and
[decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3 shows the tier would carry
nothing in v0. These numbers also set the scale for the per-peer ceiling: 64 connections to
one peer is ~50 MiB of transport state on the pair, which is why
`max_connections_per_peer` (64) is a named bound ([INVARIANTS.md](INVARIANTS.md)) rather than
the path count being the only one.

RSS is read from `/proc/self/status` `VmRSS` and the allocator does not return everything
between measurements, so each row's baseline is the previous row's residue; the per-connection
deltas are the trustworthy part and the absolute totals are not.

### Verified results — connections per dialled path (B-012)

The same bench, extended with the per-path fan of
[0002](decisions/0002-control-and-bulk-separation.md) and the point at which a server refuses.
Two shapes are measured because they are two different systems: today the pool keys on
`(host, port, ClientTls, address fingerprint)` and **not** on the path, so every path a runtime
dials shares one connection; 0002's bulk tier makes it one connection per path.

| Check | Command | Result |
| --- | --- | --- |
| 16 paths, pooled (today) | `cargo bench -p weida --bench connections` | **1.13-1.15 ms** for all 16 dials: one handshake and fifteen pool hits, 16 `(connection, path)` peer entries over **one** QUIC connection |
| 256 paths, pooled (today) | same | **1.47 ms** for all 256 dials, 256 peer entries over **one** connection. The fan does not exist yet: dialling more paths costs microseconds, not connections |
| 16 paths, one connection each (0002) | same | **22.6-23.6 ms** total, **1.41-1.47 ms** per path |
| 256 paths, one connection each (0002) | same | **277.7 ms** total, **1.08 ms** per path — no degradation with the count, and 995 KiB of RSS per connection for both ends together, consistent with B-011's 750-850 KiB at 64 |
| The refusal point | same | with `max_connections = 8`, connections 0 through 7 are accepted and the ninth dial fails with `Error::LimitExceeded` — "resource limit exceeded". The server completes the handshake first and then closes with `LIMIT_EXCEEDED` on purpose, so a peer can tell overload from a routing mistake (`crates/weida/src/listener.rs`) |

The consequence, now implemented (B-017): **a per-path connection is affordable in time and
linear in memory, but it converts a path count into a connection count against
`max_connections`** — a default of 1024 accepted connections per binding is 1024 paths' worth
of fan from a *single* client if nothing else bounds it, which is why `max_connections_per_peer`
(64) exists and counts per proved fingerprint
([INVARIANTS.md](INVARIANTS.md), [PROTOCOL.md](PROTOCOL.md) §10). A client that dials 256
paths pays 278 ms of handshakes where it paid 1.5 ms with one pooled connection, so a path
dialled is not a path used: connections are created on the first dial of a path and never
pre-created, which is what the pool does. No control connection is added on top
([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3).

**One regression found and fixed (B-025).** `connect/cold_handshake` had moved from
1.02-1.10 ms to **1.18-1.24 ms** (+12 %, p = 0.00) against criterion's stored baseline, which
predated B-016. The cause was in the code rather than inferred: `Exec::resolve` allocated the
host string, spawned a task and awaited its join handle on **every** `connect`, including for a
literal `127.0.0.1` that needs no lookup at all. `Exec::resolve` now parses an IP literal in
place and keeps the task only for real hostnames, which `lookup_host` needs for its Tokio
context. Measured after the fix: **1.05-1.09 ms**, `change: −11.6 % (p = 0.00)` — back to the
pre-B-016 level. Every pinned deployment dials literals, because a pinned address names a key
rather than a name, so this was the common path and not an edge case.

### Verified results — segment matching in the fan-out path (B-021)

The number [0007](decisions/0007-topic-namespace.md) §6 asked for. The objection the decision
overrode was that a matching language belongs nowhere near a publisher's hot path
([0007](decisions/0007-topic-namespace.md) §4.6); this is what it costs.

The `filters` group of `crates/weida/benches/patterns.rs` gives one connection *N* filters that
the published topic matches **none** of, so `publish` returns 0 and the measurement is *N*
matcher calls plus the fixed cost of a publish — no enqueue, no copy, no fan-out. Running the
same shape at `N = 1` and `N = 64` subtracts the fixed cost out: the difference over 63 is what
one more filter costs.

| Case | Command | Result |
| --- | --- | --- |
| Literal mismatch | `cargo bench -p weida --bench patterns -- filters` | 93.3 ns at one filter, **1.020 µs** at 64 → **14.7 ns per filter** |
| `*` in the middle | same | 97.4 ns at one filter, **999 ns** at 64 → **14.3 ns per filter** |
| Trailing `#` | same | **1.047 µs** at 64 → **15.1 ns per filter** |
| The replaced matcher, for scale | same | 64 `topic.starts_with(filter)` comparisons in a tight loop: **29.3 ns**, i.e. **0.46 ns each** |
| One matched fan-out, for scale | `cargo bench -p weida --bench patterns -- fanout` | `pub_1kib_8_subscribers` **64.5 µs** for publish plus eight receives |

**The shape does not matter.** A walk that fails in the first segment, one that has to cross a
`*` to reach the third, and one that meets a trailing `#` differ by 5 % — inside the spread of
this machine. That is the answer to the objection: the per-filter cost is dominated by
iterating the subscription registry, not by the grammar, and `#` being legal only in the final
segment is what keeps it that way (no backtracking, so the walk is linear in the filter).

**The scale is what settles it.** Sixty-four non-matching filters cost about **1 µs** of a
publish, against **64.5 µs** for one matched fan-out to eight subscribers — under 2 % — and a
publisher hits `max_subscriptions = 256` long before that becomes visible. The byte prefix it
replaced would have been ~0.5 ns per comparison in isolation, so a hot-path-only argument could
have preferred it; what it could not do is express a segment boundary at all, which is the
trade 0007 recorded and these numbers price.

### Verified results — per-subscriber fan-out at width (B-247)

The number the whole C8B argument of [GUIDE.md](GUIDE.md) §0.1 rests on, and the one this
repository had never taken: before this item the largest fan-out ever measured here was
**eight** subscribers (B-021's table above), while [PATTERNS.md](PATTERNS.md) §1.3 makes an
isolation claim — a stream nobody reads does not delay its siblings — whose whole point is
that it holds at width.

`crates/weida/benches/fanout.rs`, one client runtime per subscriber as in B-011, both ends in
this process, loopback QUIC, `RuntimeConfig::default()`, 1 KiB payload, release profile, three
consecutive runs on the same idle desktop. The publisher's own cost is a criterion
distribution; latency and memory are set measurements printed by the bench, because a *set* of
subscribers has no criterion shape — the same reason B-011's memory figures are printed.

**Two latencies are reported and they differ by two orders of magnitude**, which is why the
first version of this bench was wrong: publishing 64 messages back to back and then measuring
arrival gives the backlog, not the path. The *idle* figure publishes one message and waits for
every copy before the next; the *burst* figure publishes 64 without waiting. Both are below,
labelled.

| Measured | Command | Result |
| --- | --- | --- |
| Publish cost, width 1 | `cargo bench -p weida --bench fanout` | **287 ns** (282-294 ns) per call |
| Publish cost, width 16 | same | **3.04 µs** (3.01-3.08 µs) |
| Publish cost, width 256 | same | **22.94 µs** (22.78-23.12 µs) → **88.8 ns per additional subscriber** against width 1 |
| Idle latency, width 1 | same | **34.7-37.9 µs** median, **79-98 µs** p99 |
| Idle latency, width 16 | same | **76.2-77.8 µs** median, **142-494 µs** p99 |
| Idle latency, width 256 | same | **507-539 µs** median, **816-989 µs** p99, **0.91-1.22 ms** max |
| Burst drain, width 256 | same | 64 messages enqueued in **1.6-1.9 ms**, 16 384 copies delivered in **58.7-59.4 ms** → **276-279 Kcopies/s**; burst latency **20.6-22.4 ms** median |
| Transport state per subscriber | same | **396-436 KiB** at widths 16 and 256, covering **both** ends; width 1 is 1.63-1.76 MiB because it also pays the one-off crypto and endpoint state, exactly as B-011 saw |
| Drops while every subscriber keeps up | same | **zero**, at every width, asserted by the bench rather than observed |

**The per-subscriber publish cost falls as the width grows.** 184 ns per subscriber at width 16
against **88.8 ns** at 256: the fixed cost of a publish is amortized and the registry walk gets
its cache. So the number to plan with at scale is the larger width's, and a fan-out node's
publisher CPU is **~0.9 ms per message at 10⁴ subscribers** — arithmetic, from 88.8 ns.

**Which of the two ceilings binds is a function of the payload, and both sides are measured.**
A subscriber that stalls is bounded by its queue (`RuntimeConfig::endpoint_queue`, 256
messages) and by its byte budget (`Limits::subscriber_buffer_bytes`, 8 MiB). Those cross where
the message size is `8 MiB / 256` = **32 KiB**, so the bench stalls 16 subscribers on each side
of that and reports the drop **cause** from `dropped_on`:

| Payload | Messages accepted before the first drop | Held, per stalled subscriber | Bound reached |
| --- | --- | --- | --- |
| 1 KiB | **1153-1537** | **0.34-1.19 MiB** resident | **the queue** (`subscriber_queue`, budget counter zero) |
| 64 KiB | **219-281** | **9.3-10.7 MiB** resident | **the byte budget** (`subscriber_budget`, queue counter zero) |

Three things follow, and the third is the one that matters for sizing a node.

First, **the accepted count is throughput to first refusal, not held bytes**: a budget permit
is released when the copy leaves the queue for its stream, so a stalled subscriber absorbs
15-17 MiB of 64 KiB messages against an 8 MiB budget before anything is dropped — the rest is
in quinn's send state and the peer's receive state. The resident column is the cost; the
accepted column is the tolerance.

Second, **the budget is an accounting bound and not a memory bound.** One `publish` is one
`Bytes` allocation that every copy shares, so 16 stalled subscribers at 1 KiB accounted
18-24 MiB of budget while holding 5.4-19.0 MiB resident *in total* — the resident cost is one
payload per distinct message, not one per copy. Sizing a node by `width × subscriber_buffer_bytes`
overstates it by the width.

Third, **the fan-out width one node holds is bounded by copies per second, not by memory.**
At 402-436 KiB per subscriber, 64 GiB of transport state is on the order of 1.5·10⁵
subscribers; at 88.8 ns of publisher CPU per subscriber, 10⁴ subscribers is 0.9 ms per message.
Neither binds at 10⁴. The delivery rate does: **276-279 Kcopies/s** means one message to 10⁴
subscribers takes **~36 ms** of a node's delivery capacity and one message to 10³ takes
**~3.6 ms**, so a node at width 10⁴ sustains about **27 messages per second** and one at width
10³ about **276**. That is the number a topology is designed against, and it is not the one the
guide's first derivation used.

### Verified results — the dedup key's allocation per call (B-040)

`DedupWindow::is_duplicate` builds its lookup key with `scope: scope.into()`, which allocates
a `Box<str>` on **every** call — including the two paths that insert nothing: a duplicate that
is found, and the probe before an insert. The worker who wrote B-034 noticed it and refused to
restructure the key on suspicion; this is the number that decides it.

It cannot be measured through the public surface. `DedupWindow` is `pub(crate)` and its only
public path is a negotiated connection receiving DATA, where one loopback message costs
microseconds and would bury a nanosecond answer — so the `dedup_key` group of
`crates/weida/benches/patterns.rs` measures the two *shapes* as pure functions, the way B-021
measured the byte prefix it had replaced. Tables hold `max_dedup_entries = 4096` entries over
eight scopes, with a 19-byte scope string. Two runs, release, same idle machine.

| Shape | Miss | Hit |
| --- | --- | --- |
| **owned** — today: one flat `HashMap<Identity, _>`, key allocates per probe | **41.1-41.3 ns** | **43.8-48.1 ns** |
| the same table, key built once (the allocation removed, nothing else) | 31.4 ns | 35.1-35.6 ns |
| **borrowed** — the candidate: `HashMap<Box<str>, HashMap<(producer, sequence), _>>`, outer lookup borrows `&str` | 42.0-42.8 ns | 44.7-46.3 ns |

`cargo bench -p weida --bench patterns -- dedup_key --warm-up-time 1 --measurement-time 3`

**The allocation costs ~10 ns**, which the middle row isolates: 41.2 against 31.4 on a miss,
and the same gap on a hit.

**And removing it does not pay.** The candidate shape is *slower* on a miss — 42.0-42.8 ns
against 41.1-41.3 — and the same within noise on a hit, because a two-level table hashes twice
and the second hash costs what the allocation cost. That is the whole finding: the obvious
optimization buys nothing, so the flat key stays and `dedup.rs` now says so with the number
beside it. For scale, 10 ns is **0.13 %** of a 1 KiB push (~7.9 µs), and deduplication is
opt-in — a connection that negotiated `core` allocates nothing here at all, which the
capacity assertions in `dedup.rs` already prove.


### Verified results — what the local transports cost against QUIC (B-059)

The number [0010](decisions/0010-local-transport.md) §4.2 chose "the OS connection **is** the
stream, one per transfer" without: the same patterns over all three transports, at a payload
where the overhead is the whole figure and at one where the copies are. The bench is
`crates/weida/benches/transports.rs`, same machine as the runs above, release profile, both
ends in this process, loopback for the QUIC rows. Two runs; the spread below is the two runs'
point estimates.

`cargo bench -p weida --bench transports -- --warm-up-time 1 --measurement-time 3`

| Pattern | Payload | QUIC (loopback) | inproc | `AF_UNIX` |
| --- | --- | --- | --- | --- |
| Req/Rep round trip | 1 KiB | **58.3-58.8 µs** | **13.2-13.5 µs** (4.4×) | **50.6-50.8 µs** (1.2×) |
| Req/Rep round trip | 1 MiB | **2.70-2.73 ms** | **144-145 µs** (18.6-19.0×) | **266-312 µs** (8.7-10.3×) |
| Push one-way | 1 KiB | **7.97-8.08 µs** | **2.03-2.07 µs** (3.9×) | **24.0-24.1 µs** (0.33×) |
| Push one-way | 1 MiB | **1.358-1.362 ms** | **57.7-79.5 µs** (17.1-23.6×) | **130-147 µs** (9.2-10.5×) |
| RSS per live connection | — | **995 KiB** (B-012) | **430-1761 B** | **0-655 B** |

The ratio in brackets is against the QUIC row; above 1 means the local transport is that much
faster. The one-way rows compare enqueue rates rather than delivery: a Push returns when the
transport has taken the bytes, which locally is a kernel buffer or a channel slot. Req/Rep is
therefore the end-to-end timing comparison. The memory row is the delta over 200 live transfer
connections read from `VmRSS`, and it varies with what the process already has: 1761 B
(inproc) and 655 B (`AF_UNIX`) per connection when the report runs alone, 430 B and 0 B after
the timing rows have already grown the heap. Either way it is **two to three orders of
magnitude** under the 995 KiB a QUIC connection costs, and at the low end it is below what RSS
can resolve.

**What one connection per transfer costs, plainly.** Compare the two local transports at the
same payload: the only structural difference between them is that `AF_UNIX` mints a real OS
connection per transfer where inproc mints a channel pair. At 1 KiB that is **22 µs** per
one-way message (24.0 against 2.03) — a `connect`, a preamble write, an accept and the
credential check on the far side, all per message. At 1 MiB it is **~70 µs** against a figure
dominated by copying, i.e. the same fixed cost, no longer visible as a ratio. Holding the
connections open costs nothing worth counting: 200 live ones are 0-128 KiB of RSS in total.

**Where the answer changes is the small end, not the large one.** At 1 MiB the choice is free:
`AF_UNIX` beats loopback QUIC 9-10× on the round trip and inproc ~19×, and the per-transfer
connection is lost in the copies. At 1 KiB the same fixed 22 µs is most of a local round trip,
and an `AF_UNIX` round trip is only **1.2×** faster than a full QUIC one over loopback —
7.7 µs of margin against a transport that does TLS, congestion control and packet framing.
On the one-way row it has already crossed: a 1 KiB Push over `AF_UNIX` costs **3× a QUIC
one** (24.0 µs against 8.0), because QUIC opens a stream inside a connection it already has
while `AF_UNIX` opens a connection. So 0010 §4.2's structural argument holds for bulk and for
in-process work, and the transport to reach for at high message rates over `AF_UNIX` is the
one weida does not have — which is the price of not reimplementing QUIC's stream layer, now
with a number beside it.

**Two bugs, both found by running this bench rather than by reading.** First, a receipt
parked for the drain holds its send half, which locally is a descriptor, and the parked set
was sized by QUIC's stream budgets — so a sequential run exhausted `max_local_streams` with
nothing in flight (`81d70e7`, and the parked set is now bounded by half the local ceiling).
Second, the Push rows could not complete at all: a transfer holds its slot until both ends
are done with it, so a sender that outran its puller hit the ceiling and `open` **refused**
with `LimitExceeded` — `Reject` where [GUARANTEES.md](GUARANTEES.md) §6 promises `Block`. A
local `open` now waits for a slot exactly as a QUIC `open` waits on the peer's stream budget,
cancelled by dropping the future and bounded by the caller's own deadline;
`push_pull_waits_for_a_slot_over_{inproc,unix}` pins it and fails on the old code at the
256th send.

### Verified results — what a payload costs crossing into Python (B-112)

The number [0014](decisions/0014-parallel-libraries.md) §2 asks for by name: the `bytes`
boundary of `weida-py-core` "avoids a copy where PyO3 allows it and measures the copy where
it does not". Same machine as the runs above, release profile, `bytes` objects of four sizes,
the GIL held throughout as it is during a real conversion.

`cargo bench -p weida-py-core --bench bytes_boundary -- --warm-up-time 1 --measurement-time 3`

| Direction | 0 B | 64 B | 1 KiB | 1 MiB |
| --- | --- | --- | --- | --- |
| Python → Rust, **borrowed** (`payload`) | **16.5 ns** | **16.7 ns** | **17.0 ns** | **16.9 ns** |
| Python → Rust, owned (`payload_of`) | **11.0 ns** | **18.7 ns** | **28.9 ns** | **19.19 µs** |
| Rust → Python (`py_bytes`) | **6.5 ns** | **14.3 ns** | **42.8 ns** | **18.93 µs** |

**The borrowed row is flat, and that is the proof rather than the claim.** 16.5 ns at zero
bytes and 16.9 ns at a mebibyte is a reference count and a type check; nothing is copied, so
nothing grows with the payload. That is the path a binding takes when it only *reads* a
payload — a subscription prefix, a key, a length — and `weida-zmq-py` takes it for every
`subscribe`.

**The two copies that remain are forced, one by each language.** A `weida_zmq::Message` owns
a `Vec<u8>` because a frame outlives the call that queued it and is written by a reactor
thread long after, and CPython's allocation cannot become a Rust one: that is the owned row.
A CPython `bytes` object owns its storage inside its own allocation and no API — limited or
otherwise — adopts a foreign buffer: that is the Rust → Python row. At 1 MiB both are
~19 µs, i.e. **~55 GiB/s**, which is a `memcpy` and an allocation and nothing else; the
small sizes are the object, not the bytes.

**What was deliberately not done.** A `memoryview` over Rust-owned bytes would make the
Rust → Python direction zero-copy, and it is not used: the buffer protocol entered CPython's
limited API in 3.11 while B-058's wheel is `abi3` from 3.9, so it would be a zero-copy path
that exists on some interpreters and not others — a performance cliff rather than a feature.
The cost of not having it is the last column, 19 µs per mebibyte, against a 1 MiB local
round trip that the table above measures at 144 µs *inproc*: **about 13 %** of the cheapest
transport weida has, and proportionally less on anything that crosses a socket.

### Verified results — what a Python round trip costs against the Rust one (B-117)

The comparison B-117 asks for. Same machine as the runs above, release profile for both
halves, and the same shape on each side so that the difference is the language boundary and
not the measurement: one REQ/REP round trip — request out, reply back — over `inproc://` and
over loopback `tcp://`, at an empty payload and at 1 KiB, median of 2000 round trips.

```text
cargo run -p weida-zmq --release --example roundtrip_cost
.venv/bin/python crates/zmq/weida-zmq-py/roundtrip_cost.py
```

| Round trip | inproc, 0 B | inproc, 1 KiB | tcp, 0 B | tcp, 1 KiB |
| --- | --- | --- | --- | --- |
| `weida-zmq`, Rust | **16.5 µs** | **17.5 µs** | **32.4 µs** | **35.3 µs** |
| `weida_zmq.sync`, Python | **67.7 µs** (4.1×) | **72.3 µs** (4.1×) | **111 µs** (3.4×) | **116 µs** (3.3×) |
| `weida_zmq`, Python asyncio | **330 µs** (20×) | **334 µs** (19×) | **343 µs** (10.6×) | **346 µs** (9.8×) |
| `pyzmq` (libzmq 4.3.5) | **5.7 µs** | **6.0 µs** | **45.8 µs** | **47.2 µs** |

**The payload is not what costs.** 1 KiB adds 1-5 µs on every row, which is the copy
measured in B-112 above. Everything else in the gap is per-*operation* overhead, and a round
trip is four operations, so each row's excess divides by four for a per-call figure.

**The synchronous surface costs ~13 µs per operation over Rust** (67.7 µs against 16.5 µs,
four operations). That is a GIL release and re-acquire plus a thread handoff: the facade
parks the calling thread in `futures::executor::block_on` and a reactor worker has to wake
it. It is the price of a blocking API over an asynchronous library, and it is the same price
`weida-zmq`'s own `blocking` facade pays in Rust.

**The asyncio surface costs ~78 µs per operation, and the reason is one loop wakeup per
`await`.** Each awaited call creates a future on the running loop, the operation completes
on a reactor thread, and `loop.call_soon_threadsafe` writes to the loop's self-pipe so that
`epoll` wakes and the callback runs. That wakeup — not the GIL, not the copy, not the
protocol — is the 78 µs, and four of them are 313 µs of the 330. **There is a fast path this
did not take**, filed as B-179 rather than hinted at: an operation that can complete
without waiting (a `recv` whose message is already queued, a `send` with room) polled once
inline and handed back as an already-resolved future, which `await` consumes without
yielding to the loop at all.

**B-179 took it, and the saving is measured on the same script.** `Bridge::awaitable`'s
first step now polls the future once on the loop's thread, inside the reactor's context and
with a no-op waker; a ready outcome completes the delivery future in place and nothing is
spawned, a pending one is spawned as before and re-registers its task's waker on its next
poll. Measured on an idle machine — the table above was taken with four worktrees compiling,
so its absolute numbers are higher — before and after, release profile, median of 2000:

| `weida_zmq` asyncio round trip | inproc, 0 B | inproc, 1 KiB | tcp, 0 B | tcp, 1 KiB |
| --- | --- | --- | --- | --- |
| before, one wakeup per `await` | 145 µs | 144 µs | 157 µs | 155 µs |
| after, ready futures resolved inline | **78 µs** | **82 µs** | **125 µs** | **122 µs** |
| `weida_zmq.sync`, same runs | 17.8 µs | 19.1 µs | 39.1 µs | 40.4 µs |

A round trip is four operations and two of them are sends into a queue with room, so two
of the four wakeups go: **−67 µs on inproc, about 33 µs per wakeup saved on this machine**,
which is the per-`await` cost with the loop otherwise idle. The two receives still wait for
the peer and still pay the wakeup, which is why tcp saves less: the reply takes longer to
arrive than a queue takes to have room. Nothing pinned by B-111 and B-112 moved: the future
still starts at the first `await` (the inline poll runs inside the coroutine's first step,
after it has been scheduled), a cancelled `Task` still aborts the reactor task, a panic still
arrives as a `RuntimeError`, and the no-running-loop error is still raised before anything
is polled.

**Against `pyzmq` the honest reading has two halves.** Over `inproc://` libzmq is 12× faster
than this binding's synchronous surface and 58× faster than its asyncio one, because libzmq's
`inproc` is a memcpy between two threads of one process with no reactor in the middle. Over
`tcp://` the gap closes and reverses on the synchronous surface at the top end — 111 µs
against 45.8 µs — and the Rust library is *faster* than libzmq (32.4 µs against 45.8 µs),
which is where the implementation actually competes. The binding's overhead is a Python
problem to solve in the bridge, not a protocol one.

### Verified results — the nine fuzz targets, after the one that had stopped compiling

The fuzz crate declares its own `[workspace]`, so no workspace-wide build ever compiles it
and the green gate says nothing about it. `roundtrip` had therefore not compiled since DATA
keys 8-11 arrived: its `DataHeader` literal named eight of the twelve fields with no
`..Default::default()`, which is a hard `E0063`. Repaired, plus the `credit` and `error`
targets B-202's acceptance had asked for, and then all nine were run — the first run of the
set as a set:

| Target | Runs | Result |
| --- | --- | --- |
| `preamble`, `data_header`, `hello`, `traceparent`, `subscribe` | 100 000 each | no crash, no OOM, no artifact |
| `roundtrip` (repaired), `cursor`, `credit` (new), `error` (new) | 100 000 each | no crash, no OOM, no artifact |

`cargo +nightly fuzz run <target> -- -runs=100000 -max_len=20000`, same machine, nightly
1.100.0; every `fuzz/artifacts/<target>` directory is empty afterwards. The lesson is the
one the separate workspace hides: a target that does not compile is not a target that finds
nothing, and only running them says which of the two it is. CI for the fuzz crate stays
filed (B-061, whose acceptance keeps the fuzz targets out of the push gate deliberately:
they need nightly and a time budget).

### Verified results — voice beside bulk (B-289)

What a bulk upload to the same host costs a voice flow, by path and by congestion controller
([0034](decisions/0034-late-is-lost.md) §6). An in-process UDP shaper sits between one client
runtime and one server: client to server is a token bucket at **20 Mbit/s** feeding a
**64 KiB drop-tail** queue, server to client is unshaped. Voice is a flow of 200-byte
datagrams at 50 Hz, each carrying its send time so the receiver measures one-way latency on
one clock; bulk is 1 MiB `Pusher` transfers back to back. Voice is measured from second 2 to
second 20 of each run, after bulk has filled the queue. "Separate paths" is `/voice` and
`/bulk`, two connections and two controllers; "one path" is `/mixed`, one connection served
by one `Acceptor`. The controller is set on the client runtime (`Limits::congestion`), so it
governs both of the client's connections. AMD Ryzen 7 5800X, Linux 7.2.8, rustc 1.98.1,
release profile, loopback; two runs of the whole probe.

| Check | Command | Result |
| --- | --- | --- |
| Separate paths, CUBIC | `cargo run --release -p weida --example voice_beside_bulk` | voice p50 **43.0 / 43.9 ms**, p95 **81.6 / 91.7 ms**, p99 122.3 / 133.6 ms, loss 4.10 / 4.60 %; bulk 21 / 21 MiB delivered; 95 / 94 packets dropped at the queue |
| Separate paths, BBR | same | voice p50 **5.6 / 5.6 ms**, p95 **6.7 / 47.7 ms**, p99 50.0 / 61.5 ms, loss 8.69 / 8.69 %; bulk 21 / 17 MiB; 323 / 602 queue drops |
| One path, CUBIC | same | voice p50 42.7 / 42.2 ms, p95 49.5 / 49.5 ms, p99 52.9 / 52.8 ms, loss **0.00 / 0.00 %**; bulk 22 / 22 MiB; 84 / 88 queue drops |
| One path, BBR | same | voice p50 8.8 / 48.5 ms, p95 56.6 / 52.8 ms, p99 59.4 / 60.4 ms, loss 5.39 / 1.90 %; bulk 18 / 23 MiB; 1455 / 5578 queue drops |

**On separate paths BBR cut voice p95 by 92 % and 48 % in the two runs, past the 30 % the
note set in advance, so 0034 §6 now says a bulk profile that shares a bottleneck with media
runs `Congestion::Bbr`.** Its price is visible in the same rows: voice loss roughly doubles
(4-5 % to 9 %) because BBR's probing overflows a 64 KiB queue that CUBIC fills more slowly,
and the p95 spread between the two BBR runs is sevenfold, which is `quinn`'s own warning that
its BBR is experimental in numbers. **One path is the other finding**: voice on the bulk
connection lost nothing under CUBIC, because a datagram outranks every stream in `quinn`'s
packet assembly (0034 §2.3), but it waits in the sender for the congestion window, so its
p50 is the queue's ~43 ms either way. The default stays `Cubic`: the note's rule is about
a bulk profile, and a profile is chosen, not inherited. One named limit: loopback plus a
userspace shaper is not a residential uplink — the shaper's timer granularity is a
millisecond, and nothing here models cross traffic or a real modem's buffer.

---

## 5. Decisions

### Serialization: CBOR via `minicbor` (Phase 0)

Master doc §82 leaves the choice between MessagePack, CBOR and another compact evolvable
format open, with criteria: parsing speed, encoding speed, allocations, unknown-field
support, multi-language implementations, dependency cost, fuzzability, and deterministic
representation where needed. Phase 0 mandates selecting one.

CBOR is selected, encoded and decoded with `minicbor` (feature `alloc`):

- **Integer-keyed maps** give compact headers and a stable key space, decoupling the wire
  from field names.
- **Unknown-field support** is straightforward: unknown uint keys are skipped, satisfying
  master doc §15's requirement that unknown optional fields be ignorable.
- **No transitive dependencies**, satisfying the dependency discipline of master doc §72.
- **Multi-language support** is broad and mature (RFC 8949), which matters for the Phase 10
  bindings and the Phase 8 browser client.
- **Fuzz-friendly**: the codec is a pure function over bytes, so it fuzzes without a socket.
- **Deterministic representation** is achievable within our own encoder rules: definite
  length, ascending keys, minimal integers ([PROTOCOL.md](PROTOCOL.md) §5).

Encoders and decoders are hand-written rather than derived, because the strictness the
protocol requires — duplicate-key rejection, per-key string caps, cap-before-allocation,
depth-limited skipping — is not expressible through a derive macro. If `minicbor`'s public
API proves unable to express these bounds, the fallback is a hand-rolled CBOR-subset codec
inside `weida-protocol`; the wire bytes and the golden vectors do not change either way.

### Other Phase 0 decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| URL scheme | `weida://` | The master doc's `mq://` is a working name (§82). Single `SCHEME` constant in `crates/core/src/addr.rs`. |
| ALPN token | `weida/0` | Couples the TLS-level protocol identity to the wire protocol version, so a version-0 peer cannot silently talk to a future version. |
| Stream magic | `0x57` (ASCII `W`) | Cheap first-byte rejection of non-weida streams. |
| Wire protocol version | `0` | Experimental per master doc §15; independent of the library version. |
| Library version | `0.1.0` pre-releases (`0.1.0-alpha.N`; between releases the next one with `-dev`, e.g. `0.1.0-alpha.6-dev`) | Independent of the wire version. |
| Client trust | pinned public keys **or** configured anchors | A peer is accepted for a pinned fingerprint or for a chain to an explicitly configured anchor that names the host dialled. No platform root store and no insecure-skip mode ships in v0, and `Trust::by_address()` trusts nothing beyond what an address names. |
| TLS material source | file **or** in-memory PEM (`Pem`) | Requiring a path would force callers holding a key from a secret store to write it to disk first. Both sources are first class; `Identity::from_pem` and `Trust::anchor` never touch the filesystem. |
| Credential placement | identity per **binding**, trust per **dialling endpoint** | Credentials are transport-specific, so they belong neither on the Listener (a namespace) nor on the Runtime (a resource container). Consequence: the connection pool keys on `(host, port, ClientTls, address fingerprint)` — sharing on authority alone would hand one endpoint a peer authenticated on another endpoint's terms. |
| Rust edition | `2024` | Current stable edition. |
| MSRV | `1.88` | Highest requirement among the pinned dependencies. |

### Phase 3 re-architecture decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| Req/Rep transport | one client-opened **bidirectional** stream per exchange | The stream is the correlation. Deletes correlation ids, the per-connection pending table, the `Correlator` actor, `ReplyArrived` and the CANCEL frame in one move, and lets request and reply stream simultaneously on independent halves. |
| Application acknowledgements | **not in the core**; reserved for the L2 broker (Phase 6) | Brokerless, an application ACK only means "arrived in RAM" — QUIC already retransmits and already reports transport receipt. Accepted / Stored / Replicated / Processed describe responsibility transfer to a hop that has taken responsibility; without such a hop the words are unearned ([GUARANTEES.md](GUARANTEES.md)). |
| Delivery signal | `OutgoingTransfer::finish() -> Delivery`, `Delivery::delivered().await` | The honest signal QUIC can actually give: quinn's `stopped()` resolving `Ok(None)` means the peer acknowledged every byte "although not necessarily the processing of it". `finish()` is synchronous and dropping the `Delivery` is free, so fire-and-forget senders pay nothing ([FAILURE_MODEL.md](FAILURE_MODEL.md)). |
| Misroute refusal code | QUIC application error code `UNSUPPORTED = 9` | With ERROR frames confined to the reply half of an exchange, a unidirectional stream sent to a path that does not serve it has no frame to be refused with. A dedicated stop code keeps the refusal typed instead of collapsing it into `REJECTED` ([PROTOCOL.md](PROTOCOL.md) §7). |
| Raw stream API | `Peer` and `Acceptor` are public (`crates/weida/src/stream.rs`) | L0 is the product, not an implementation detail: if the patterns are the only way in, every unanticipated topology needs a new pattern. `Peer::open`/`open_bi` and `Acceptor::accept` expose the two QUIC stream kinds directly; the patterns are thin wrappers over them. |
| Stream limits | `max_pending` removed; `max_concurrent_bidi_streams = 1024` added | There is no pending-reply table left to bound. Concurrent exchanges are bounded by QUIC itself instead, and the bidirectional budget was previously hardcoded to `0`. Worst-case header memory becomes `max_header_bytes * (uni + bidi)` = **48 MiB**, up from 32 MiB ([INVARIANTS.md](INVARIANTS.md)). |

### Phase 3 identity and trust decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| Peer identity | SHA-256 of the leaf's DER `SubjectPublicKeyInfo` (`sha256:<64 hex>`) | Hashing the public key rather than the certificate keeps a pin valid across a renewal that reuses the key, and it is the same value `curl --pinnedpubkey` and HPKP pin, so an operator can compute and compare it without weida. |
| Address may name the peer | `weida://[sha256:<hex>@]host:port/path` | One string then carries both where to dial and whom to accept — which is exactly what a discovery record, a config line or a line pasted into a terminal has to survive as. Reaching a self-signed peer safely needs nothing else configured. |
| Address fingerprint overrides `Trust` | the named identity is the only one accepted on that connection | The most specific statement of intent wins. An operator who wrote down which peer must answer did not mean "or anybody else my CA vouches for". |
| Empty `Trust` dials nothing but pinned addresses | `Error::Tls("nothing to trust …")`, before any packet | The only alternative to failing here is trusting everything, which must never be reachable by omission. An endpoint with no trust configured stays usable — for addresses that name their peer, and for nothing else. |
| Client identity | optional on the dialling side; a binding may require it | Master doc §6 makes client identity optional and §46 asks for mTLS. `ClientTls` therefore carries `Option<Identity>`, and `ServerTls::require_client` takes a `ClientTrust`: `Trusted(trust)` refuses anonymous and untrusted peers at the handshake, and `AnyKey` refuses only anonymous ones, verifying the handshake signature and judging nothing about the key ([0035](decisions/0035-keys-proved-not-judged.md) §4.1). Requiring an empty trust is rejected at bind time, since it would accept nobody. The chain a peer presented is kept only up to 8 certificates and 32 KiB (`MAX_PEER_CHAIN_CERTS`, `MAX_PEER_CHAIN_BYTES`): constants rather than `Limits` fields, because they bound what weida keeps rather than tune a workload; an `AnyKey` binding fails a larger chain in the handshake, a `Trusted` one keeps none of it. |
| Certificate generation | `rcgen`, behind default feature `generate` | `Identity::generate()` is what lets a pinned deployment handle no PEM at all, so it is on by default; behind a feature so a deployment that only loads issued certificates does not link a certificate builder. |
| Fingerprint dependencies | `rustls-webpki` (SPKI parsing) and `ring` (SHA-256) as direct dependencies | Both were already in the tree through rustls, so naming them directly adds no third-party code and no build time, which is what master doc §72's dependency discipline asks. Neither parsing a leaf's SPKI nor hashing it is something rustls exposes. |

### Runtime ownership decisions (B-016)

| Decision | Value | Rationale |
| --- | --- | --- |
| Three constructors | `Runtime::new` (ambient reactor), `Runtime::with_handle(handle, config)`, `Runtime::owned(config)` | `quinn` needs a Tokio reactor and nothing else in the crate does. Making the reactor something the runtime holds — borrowed, handed over, or created — removes "you must already be inside `#[tokio::main]`" from weida's contract, which is the prerequisite for the Phase 10 bindings, where the host language owns the thread the call arrives on. |
| `worker_threads` | `RuntimeConfig::worker_threads`, default 1, `0` rejected | A messaging runtime is I/O bound, and every extra worker is a thread a library takes from its host process without being asked. `0` is a configuration error rather than something to correct silently ([GUARANTEES.md](GUARANTEES.md) §4). |
| Owned-runtime teardown | `shutdown_background()` from `Drop`, not a plain drop | The last `Runtime` clone may go out of scope on one of that runtime's own worker threads, where dropping a Tokio runtime panics inside a destructor — which aborts rather than unwinds. |
| One runtime surface | `Exec` in `crates/weida/src/runtime.rs`: `spawn`, `sleep`, `resolve`, `enter` | One place to see what the crate asks of tokio, and the thing that makes a non-tokio caller possible at all. `grep -n 'tokio::spawn\|tokio::time\|lookup_host' crates/weida/src` matches `runtime.rs` and one comment. |
| Client handshake placement | spawned **on** the runtime, awaited by the caller as a join handle | Completing a `quinn::Connecting` spawns the connection driver from inside the poll, so the polling thread would need an ambient reactor. Spawning it keeps `Runtime::connect` pollable from any executor and keeps the future `Send`, which holding an `EnterGuard` across the await would not. |
| `futures-io` beside `tokio::io` | both trait pairs on `OutgoingTransfer`/`IncomingTransfer`, `futures-io` delegating to the tokio impl | A caller on `futures`, `smol` or `async-std` should not have to wrap a compat shim around a payload stream. Delegating keeps the end-of-payload bookkeeping in one place. |
| `shutdown_timeout` | `RuntimeConfig::shutdown_timeout`, default 1 s (B-031) | `shutdown` closes every endpoint and then waited for the sockets to go idle **without a bound**, so the length of a process's exit was decided by the path rather than by the caller: measured against a peer that has gone silent, the draining period alone is **96 ms** on loopback and grows with round-trip time and loss. One second is generous on any network where a clean close was possible at all. The bound is one budget for the whole shutdown, not one per endpoint, because what a caller waits for is the call ([decisions/0009](decisions/0009-drain.md) §4.4). The same bound caps `Binding::close` (B-318, [decisions/0038](decisions/0038-restart-handoff.md) §4.6), which had the same unbounded wait, and `Binding::hand_over`'s wait for its held closes (B-320, [decisions/0039](decisions/0039-handover-says-when.md) §4.4). |
| Address selection | try every resolved address in order, capped by `Limits::max_resolved_addresses` (8), each attempt but the last bounded by `RuntimeConfig::connect_attempt_timeout` (250 ms) (B-029) | Taking the first address made `weida://localhost:…` unreachable wherever `localhost` resolves to `::1` before `127.0.0.1`, which is the common Linux ordering and this machine's. Sequential attempts with a per-attempt bound rather than RFC 8305's parallel happy-eyeballs: the failure being fixed is an address that answers *nothing*, where QUIC has no refusal to observe, and 250 ms is RFC 8305's own Connection Attempt Delay for exactly that case. Measured on the regression test: **30 s before, 1.3 s after**. The last address keeps the full handshake budget, so an IP literal and a single-address name behave exactly as before. |
| Loss cause | `Error::ConnectionLost(LossCause)` — `IdleTimeout`, `PeerClosed`, `LocallyClosed`, `Reset`, `TransportError` (B-028), and `PeerRestarting` (B-318) | `PeerSet::pick` reported a bare `ConnectionLost` for every closed peer, and `conn_error` mapped `TimedOut`, `Reset` and an unrecognized application close to the same value, so the distinction did not exist anywhere in the API. A payload rather than new top-level variants: the *outcome* is identical in all five cases — nothing in flight completed, `is_definite_failure()` stays true — and only the next action differs, so a second outcome vocabulary would have been the wrong shape ([FAILURE_MODEL.md](FAILURE_MODEL.md) §4). Five causes because each one changes what an application should do; `pick` now reports the cause of the peer it rejected. The sixth, `PeerRestarting`, is a peer's close with `RESTARTING` ([decisions/0038](decisions/0038-restart-handoff.md) §4.3): it too leaves the outcome alone and changes only the next action, which is a redial with no first delay, whatever `stop_on_peer_closed` says. |

---

### ZMTP codec decisions (Phase 9 slice 1, B-030)

| Decision | Value | Rationale |
| --- | --- | --- |
| Dependencies | **none**, not even `weida-core` | A codec checked against a foreign specification must not reach for weida's types, limits or error vocabulary. `Cargo.toml` has an empty `[dependencies]` on purpose, and the crate carries its own error vocabulary. |
| Where the cap lives | an argument to every decode entry point, never a constant | ZMTP grants no credit, a long frame may declare 2^63-1 octets, and `ZMQ_MAXMSGSIZE` is unlimited by default, so the caller's local limit is the whole defence. The effective cap is `min(argument, 2^63-1)`, so a caller cannot switch the check off. |
| What a decoder returns | borrowed slices (`Command<'a>`, `Metadata<'a>`) | A decoded frame body is a slice of the caller's buffer and a command points into that body, so the only allocation on the way in is a `READY` property list. |
| Illegal flag combinations | unrepresentable | MORE “SHALL be zero on command frames”, so `FrameKind` is `Message { more }` or `Command` — the illegal combination has no value. |
| Incomplete versus violated | separate per layer | A frame header can legitimately be short; a command body arrives whole, so a field running past its end is a violation with nothing to wait for. |
| Where the specification contradicts itself | follow the ABNF and libzmq, and say so | Command names are length-prefixed, not null-separated; an `ERROR` reason may contain spaces although `VCHAR` excludes them. The crate docs and golden vectors record both choices. |


### Connection-tier decisions (B-017)

| Decision | Value | Rationale |
| --- | --- | --- |
| Pool granularity | one connection per `(authority, terms, expected fingerprint, **path**)` | A QUIC connection's receive window is shared, so the only way two flows cannot stall each other is for them not to share a connection ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2). Measured as behaviour, not asserted: `a_stalled_path_does_not_stall_another_path` fills one path's window until a write parks and then sends on another path. Reverting the key to one connection per peer makes that test time out, which is the coupling the decision set out to remove. |
| The control tier | **not built, and now parked by decision** | In v0 a control connection would carry nothing: every connection performs its own HELLO (§2.2 of [PROTOCOL.md](PROTOCOL.md)), the credit frame of §6.6 rides the connection of the path it names rather than a control connection, and SUBSCRIBE could not move there because a publisher writes fan-out on the connection the SUBSCRIBE arrived on and no field names a peer's other per-path connection. That question is settled by [decisions/0011](decisions/0011-answered-where-it-arrived.md): a frame naming a path rides that path's connection, which takes SUBSCRIBE/UNSUBSCRIBE *and* the reserved credit frame out of the tier's cargo and leaves it empty, so it is parked with a revival condition (0011 §4.3) rather than left blocked. The named residual coupling is a SUBSCRIBE behind payload on the *same* path (0011 §4.4). |
| `Limits` scope | a **per-connection** profile; runtime-level numbers moved to `RuntimeConfig` | 0002 §6.4 wants one profile per tier. Two profiles are only honest if every field means something in both, so `max_connections`, `endpoint_queue` and `max_resolved_addresses` — per binding, per endpoint, per dial — left `Limits`, and `keep_alive`/`idle_timeout` joined it, because those *are* per connection. The second profile itself waits for the tier: a `Limits::CONTROL` nothing reads would be five numbers nobody has to justify. |
| Peer binding | compare against the peer's **live** connections, refuse a mismatch with `Error::Untrusted(fp)` | The fingerprint is the only thing that binds a peer's connections ([decisions/0008](decisions/0008-session-identity.md) §4.2), and dialling a second path is dialling the same peer. Live connections rather than a remembered value, because a peer is this peer only while a connection to it lives: once the last one is gone, a replacement server with a new key is a new peer and nothing should still be objecting to it. Proved by swapping a raw server's identity between two handshakes on one socket — the load-balancer case, which two `quinn` endpoints cannot reproduce because they cannot share a port. |
| `max_connections_per_peer` | 64, counted per proved fingerprint, released on close | One connection per path lets the dialling side choose the count, so `max_connections` alone would let one peer fill a binding. 64 is the number B-011 measured (~50 MiB of transport state across both ends for 64 connections to one peer) and a sixteenth of the default `max_connections`. Anonymous connections are not counted together: two of them cannot be shown to be one peer, so counting them as one would refuse strangers for each other's traffic. |

### Connection statistics decisions (B-298, B-299)

| Decision | Value | Rationale |
| --- | --- | --- |
| `quinn` floor | `quinn = "0.11.12"` in the workspace, locked at `quinn` 0.11.12 / `quinn-proto` 0.11.19 | `PathStats::min_rtt` exists only from `quinn-proto` 0.11.18, and the lock held 0.11.17; 0.11.12 is the first `quinn` whose manifest requires 0.11.18, so the floor is stated where a resolver reads it rather than left to whatever a lock file happens to hold ([decisions/0036](decisions/0036-connection-statistics.md) §2.1). |
| Where statistics are read | `connection_stats()` on `Peer` and every dialling endpoint and blocking twin, one record per **live slot** | A slot is what the application dialled and what outlives a connection, so its URL is the label and its redial count is the address's. A connection the pool shares between two endpoints is reported by both, with the same transport counters (0036 §4.6). The slot list is collected under its `std` mutex and `quinn`'s counters are read after it is released, so no `quinn` lock nests inside it. |
| Local transports | `transport: None`, `age` and `redials` still reported | No path to measure, and an empty vector has to keep meaning "no live connection" (0036 §3, option E). |
| `age` clock | `std::time::Instant` taken in `ConnCtx::spawn` | One instant per connection, after the QUIC handshake and before HELLO; a paused Tokio clock in a test does not stop it. |

---

## 6. Known debt and deferred work

Recorded deliberately, not discovered later.

- **OpenTelemetry SDK wiring deferred.** v0 propagates W3C `traceparent` and `tracestate` in
  transfer metadata and emits structured logs through `tracing`. No OTel exporter, no span
  export pipeline. That belongs to the observability work; the wire fields it needs are
  already carried, so enabling it does not change the protocol.
- **No retries.** A refused, lost or `Indeterminate` transfer is reported to the caller and
  never retried by the library ([GUARANTEES.md](GUARANTEES.md) §6). Retry policy arrives in
  Phase 4.
- **No persistence.** No payload store, no WAL, no recovery. Phase 5.
- **Deduplication is opt-in and bounded, not absent.** The entry here used to say weida had
  no dedup at all and no identifier to deduplicate on; both stopped being true with DATA keys
  6 and 7 ([decisions/0001](decisions/0001-sequence-field.md)). A connection that negotiated
  `Deduplication::Bounded(window)` suppresses a repeat of `(producer, scope, sequence)`
  inside `max_dedup_entries` (4096), and a `core` connection allocates nothing for it. What is
  still absent is an **application-level idempotency id** — the key is the producer's sequence,
  which only a sender that opted into per-producer ordering writes — so an `Indeterminate`
  result may still only be retried for idempotent operations, or by a sender that numbers its
  own transfers ([FAILURE_MODEL.md](FAILURE_MODEL.md) §5).
- **Single reply per exchange.** The reply half carries one DATA transfer or one ERROR and
  then FIN. Answering one request several times needs either several exchanges or a framing
  convention inside the payload; v0 offers neither.
- **Capability codes unused.** HELLO carries `capabilities` and `required_capabilities`, but
  no code is assigned and the supported set is empty.
- **Application acknowledgements: one of four, and no frame.** `Accepted` exists —
  `weida-broker` reports it for a message it has admitted into a queue, on the reply half of
  the producer's exchange, as DATA key `8` ([PROTOCOL.md](PROTOCOL.md) §6.2). `Processed` has
  its place decided and no code: the reply half of the delivery exchange, Phase 6's third
  slice. `Stored` and `Replicated` have **no code point and nothing to ask for**, because
  there is no store and no replica set to make them true, and reporting them for an in-memory
  queue is prohibited rather than optimistic ([GUARANTEES.md](GUARANTEES.md) §1). Outside a
  broker the core's only delivery signal is still the `Delivery` transport receipt
  ([GUARANTEES.md](GUARANTEES.md) §6).
- **No CI.** Still absent: the gate runs on a workstation and on one Windows VM, by hand
  (B-061 is blocked on a runner choice — the Forgejo host has 2 vCPUs, 3 GB RAM and a 600 s
  job limit, which is less than one cold `cargo build --workspace`).
- **Licence and publish metadata: done** (B-068). Dual `MIT OR Apache-2.0` with both texts at
  the repository root, and `license`/`repository`/`homepage`/`keywords`/`categories` inherited
  from `[workspace.package]` by all 26 publishable crates, so `cargo package --workspace`
  prints no metadata warning. Nothing is published anywhere yet: the form is ready, the
  decision to upload is separate.
- **A synchronous API wrapper exists now** (B-194): `weida::blocking` behind the non-default
  `blocking` feature, in the shape the four protocol libraries of
  [0013](decisions/0013-competitor-libraries.md) already had — every method a `block_on`
  around the asynchronous endpoint's own, over a [`Runtime::owned`] reactor the caller never
  sees, with payloads as `Vec<u8>` and an explicit byte ceiling on every receive. The entry
  this replaced said a blocking facade was "a binding-layer concern (Phase 10)", and that was
  wrong twice: the bindings are built on the facades rather than the reverse, and the
  repository's own `weida` binary was an async program for no reason a script cares about.
  What it adds beyond wrapping is one refusal: a call from inside a Tokio runtime would block
  the worker that has to drive what it waits for, so every entry point checks
  `Handle::try_current` and fails with `Error::Runtime` naming the fix rather than deadlocking.

  [`Runtime::owned`]: https://git.doodleshnookie.net/tuco86/weida/blob/main/crates/weida/src/runtime.rs
- **Fuzz runs need a nightly toolchain.** `cargo-fuzz` requires nightly, which is installed
  and was used: the `weida-protocol` targets ran 200 000 iterations each, the `weida-zmtp`
  targets and the five `weida-sp` targets 20 000 each, with no findings. On a host without
  nightly the targets are still committed and the deterministic `fuzz_smoke*` tests cover the
  same properties on stable, at lower depth.
- **`IncomingTransfer::read_capped` was added beyond the planned API surface.** The planned
  `collect(self, max_bytes)` cannot read a borrowed request body, so the borrowing form is
  the primitive and `collect` delegates to it.
- **A fan-out drop is visible to a subscriber that asked for detection.** This entry used to
  say the loss was invisible and would need a sequence field; the field arrived
  ([decisions/0001](decisions/0001-sequence-field.md)) and `PerProducer(detect)` now reports
  the gap to the subscriber, with `PerProducer(reassemble)` holding instead up to
  `max_reorder_hold`. What remains true is the **default**: a `core` subscriber past its byte
  budget at the publisher simply misses the message and only the publisher counts it
  (`Publisher::dropped`), because nothing on the wire reports a drop to a receiver that
  negotiated no ordering.
- **One subscriber per path per connection.** A `Subscriber` claims its path in the dialling
  connection's namespace, so two subscribers on one pooled connection asking for the same
  path collide with `AlreadyRegistered`. Fanning one subscription out to several in-process
  consumers is left to the application; silently multiplexing two subscribers onto one queue
  would be worse.
- **Publisher direction is fixed.** Pub and Pull bind; Sub and Push connect. The reverse
  directions wait for a use case that demands them.
- **A redial restores a transport, not a registration.** Since 0031 a lost address is
  redialled by the runtime and a subscriber's own filters are re-sent; nothing server-side
  survives the connection, and a stream that was in flight is the application's to reopen
  (`docs/PATTERNS.md` §1.8, §1.11).
- **Authorization hooks are not implemented.** Master doc §46's authorization surface does not
  exist. What ships is authentication plus the identity: applications decide on
  `IncomingMeta::peer`, and what is built in acts on a proved peer without a policy language: a
  binding's `Trust` of pins and anchors or `ClientTrust::AnyKey` in the handshake, a radio's
  `with_admission`/`evict` on joins, and `Binding::disconnect` on live connections
  ([0035](decisions/0035-keys-proved-not-judged.md)).
- **`stream_receive_window` is not a payload budget.** The DATA header spends the same window
  as the payload, and a receiver announces more window only per eighth of it, so a payload
  sized exactly to the window cannot be written until the application starts reading (§4).
  Correct QUIC behaviour that reads as a hang.
