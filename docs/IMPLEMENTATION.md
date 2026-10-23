# Implementation

Implementation status per phase, the process rules, the verified measurements, the decisions
the code carries and the known debt. It describes the current tree; the loop's own records are
[BACKLOG.md](BACKLOG.md) and [NIGHTLOG.md](NIGHTLOG.md).

Related: [ARCHITECTURE.md](ARCHITECTURE.md), [PROTOCOL.md](PROTOCOL.md),
[GUARANTEES.md](GUARANTEES.md), [FAILURE_MODEL.md](FAILURE_MODEL.md),
[INVARIANTS.md](INVARIANTS.md).

---

## 1. Phase tracker

Phases follow the dependency order of master doc §79; work MUST NOT be spread chaotically
across phases. A phase is `done`, `partial` (its row says what is missing) or `not started`.

| Phase | Name | Status | What exists |
| --- | --- | --- | --- |
| 0 | Architecture/specification | done | The specification set in `docs/` and the decision notes that settle its open questions. |
| 1 | Core model | done | `weida-core`, with no I/O: addresses, errors, limits, trace context and fingerprints. |
| 2 | Native QUIC transport | done | `weida-protocol`, the sans-I/O wire codec, and the QUIC transport of `weida`: negotiation, routing, exchanges, cancellation, limits, pooling and identity. |
| 3 | Brokerless messaging patterns | done | Six patterns plus RADIO/DISH, over QUIC, in-process, `AF_UNIX` and named pipes. Router/Dealer are answered as emergent ([ARCHITECTURE.md](ARCHITECTURE.md) §6a). |
| 4 | Reliability | partial | Indeterminate outcomes, opt-in per-producer ordering, bounded deduplication, `drain`, cursors and transparent redial with the outbox exist; retry policy, outgoing and incoming persistence, the coalescing helper and recovery do not. |
| 5 | Persistence subsystem | not started | Nothing is built. [STORE.md](STORE.md) specifies the interface. |
| 6 | Standalone broker | partial | `weida-broker`: queues, publisher confirm, absolute credit and settlement with requeue. Not built: the redelivery count, report relay, retirement and an inspection API beyond the in-process counters. |
| 7 | Broker clustering | not started | Nothing is built. `weida-raft` is the consensus transport it will use. |
| 8 | Web binding | not started | Nothing is built. |
| 9 | Standalone foreign-protocol libraries | partial | All six libraries with codecs, fuzzing and interop; the NATS interop is written and not run. |
| 10 | Language bindings | partial | Python for `weida` and the first five libraries; HTTP's is filed (B-371); no other language. |
| 11 | CLI and administration | partial | The `weida` binary with `serve`, `request`, `send` and `sub`; no broker or cluster inspection. |
| 12 | Documentation/site/stabilization | partial | [GUIDE.md](GUIDE.md) and the website, which lives in its own repository ([0033](decisions/0033-the-website-moves-out.md)). |

### Phase 0 — Architecture/specification

The specification set describes the system as built and, where a document says *spec ahead of
code*, as decided:

- Terminology, the layer model, addressing, runtime internals and the public API:
  [ARCHITECTURE.md](ARCHITECTURE.md) (§2, §7).
- The invariants every change is checked against: [INVARIANTS.md](INVARIANTS.md).
- The failure vocabulary: [FAILURE_MODEL.md](FAILURE_MODEL.md); the guarantee vocabulary:
  [GUARANTEES.md](GUARANTEES.md).
- The wire protocol, with its envelope (§3), version and capability negotiation (§2.3) and
  golden vectors (§8): [PROTOCOL.md](PROTOCOL.md).
- What each pattern does underneath, measured: [PATTERNS.md](PATTERNS.md). The persistence
  interface: [STORE.md](STORE.md). The guide: [GUIDE.md](GUIDE.md).
- The decisions that closed the master doc's open questions: [decisions/](decisions/README.md).
- Serialization is CBOR (§5 below).
- The deliberately unresolved questions are in §6 below and in [PROTOCOL.md](PROTOCOL.md) §11.

### Phase 1 — Core model

`weida-core` is an I/O-free crate, unit-tested without any networking:

- `Address`, the address of any transport — `EndpointAddr`
  (`weida://[sha256:<hex>@]host:port/path`), `InprocAddr`, `UnixAddr` and `PipeAddr` — with the
  endpoint path rules and the per-platform budgets (`src/addr.rs`).
- `Error`, `ErrorCode`, `LossCause` and `StopReason`, with hand-written `Display` and `Error`
  impls (`src/error.rs`).
- `Limits`, the per-connection resource profile, and the `Congestion` choice
  (`src/limits.rs`).
- `TraceContext`, with the W3C `traceparent` parser and formatter (`src/trace.rs`).
- `Fingerprint` — the SHA-256 of a leaf certificate's DER `SubjectPublicKeyInfo`, text form
  `sha256:<64 lowercase hex>` — and `PeerIdentity`, a key proved by TLS or a principal proved
  by the kernel (`src/identity.rs`).

There are no transfer ids, correlation tables, acknowledgement policy types or send and
receive state machines: Req/Rep rides one bidirectional stream, so the stream is the
correlation, and delivery is the transport receipt rather than an application acknowledgement
(§5, stream-core decisions; [ARCHITECTURE.md](ARCHITECTURE.md) §6a). The Runtime, Listener,
Binding and typed Endpoint abstractions of master doc §79's Phase 1 list live in crate
`weida`, because their shape is determined by the transport they front.

`weida-runtime` (`crates/runtime`) is the reactor and OS plumbing that weida and the
standalone libraries share, with no protocol in it. It holds:

- `Exec` — `spawn`, `sleep`, `within`, `enter` and the capped resolver with its IP-literal fast
  path — with the three reactor-ownership constructors and the background-shutdown discipline
  of an owned reactor;
- the `AF_UNIX` bind hygiene and `peer_credentials` of
  [0010](decisions/0010-local-transport.md) §4.5, and on Windows the named-pipe hygiene
  (`BoundPipe`, `connect_pipe`, `client_principal`, `server_principal`);
- a generic named-endpoint registry with a byte budget, and the bounded close budget.

`weida` takes its tasks, timers and name lookups from it, and the standalone libraries use it
with none of weida's frames ([0013](decisions/0013-competitor-libraries.md) §4.2, §5.3). It
depends on `weida-core`, on `tokio` and, on Windows only, on `weida-winpipe`: the one crate
that may use `unsafe`, whose every block is one Win32 call with its precondition stated, and
which is empty off Windows.

### Phase 2 — Native QUIC transport

`weida-protocol` is the wire codec and crate `weida` is the QUIC transport.

- **Wire codec** (`crates/protocol`, no I/O). RFC 9000 varints; the stream preamble
  `[0x57][kind][header_len][CBOR header]` with its cap-before-allocation check; the header
  codecs of the nine frame kinds (HELLO, DATA, ERROR, SUBSCRIBE, UNSUBSCRIBE, CREDIT, CURSOR,
  FLOW, REPORT), hand-written with duplicate-key rejection, per-key string caps and
  depth-limited skipping (§5, serialization); HELLO negotiation, which intersects versions,
  capabilities and guarantee sets (`negotiate.rs`); the datagram flow prefix (`datagram.rs`);
  the QUIC application error codes (`codes.rs`). The golden vectors of
  [PROTOCOL.md](PROTOCOL.md) §8 are asserted byte for byte in `tests/golden_vectors.rs`,
  deterministic mirrors of the fuzz targets run in `tests/fuzz_smoke.rs`, ten `cargo fuzz`
  targets live in `fuzz/` and the header bench is `benches/codec.rs`.
- **Connections.** quinn integration and connection establishment; one HELLO exchange per
  connection; endpoint routing through a flat namespace shared by every binding of a
  `Listener`; one connection actor per connection, which payload bytes never traverse
  (`conn.rs`, `listener.rs`; [ARCHITECTURE.md](ARCHITECTURE.md) §5). Client connections are
  pooled per dialled path: the pool key is `(authority, terms, expected fingerprint, path)`,
  so a reader that stalls one path cannot stall another, and a binding counts connections
  per proved fingerprint (`max_connections_per_peer`) (`pool.rs`; §5, connection-tier
  decisions).
- **Exchanges.** Req/Rep is one client-opened bidirectional stream: DATA with `endpoint`,
  payload and FIN on the initiating half; DATA without `endpoint` plus payload and FIN, or
  ERROR and FIN, on the reply half. The stream is the correlation, there is no ACK or CANCEL
  frame, and a requester abandoning a reply is `STOP_SENDING(CANCELED)` on the reply half. A
  stream sent to a path that does not serve it is refused with ERROR `UNSUPPORTED` on the
  reply half or `STOP_SENDING(UNSUPPORTED)` ([PROTOCOL.md](PROTOCOL.md) §4, §7). Tests:
  `tests/reqrep.rs`, `tests/hostile.rs`.
- **Transfers.** `OutgoingTransfer` and `IncomingTransfer` stream the payload and implement
  both the `tokio::io` and the `futures-io` traits. `finish()` is synchronous and returns a
  `Delivery`, the transport receipt: the peer's transport holds every byte, which is not the
  same as the application having read it ([FAILURE_MODEL.md](FAILURE_MODEL.md) §4).
  Cancellation is a reset in both directions. No path buffers a whole transfer:
  `tests/large.rs` echoes 1 GiB with a bounded peak RSS (§4.2).
- **Identity and trust.** A peer is its public key. `Identity` answers *who am I* and `Trust`
  (`by_address`, `pin`, `anchor`) answers *whom do I accept*; `ClientTls` and `ServerTls`
  combine them, and a fingerprint named in the address overrides both. A binding may require
  client identity with `ServerTls::require_client(ClientTrust)`: a `Trusted` set, or `AnyKey`,
  which verifies the handshake signature and judges nothing else
  ([0035](decisions/0035-keys-proved-not-judged.md)). What a peer proved reaches the receiver
  as `IncomingMeta::peer` and `IncomingMeta::peer_chain`, from the handshake and never from a
  header, and `Error::Untrusted(Fingerprint)` names the key that answered. An identity can be
  a source that rotates under a live binding (`IdentitySource`, `TrustSource`;
  [0032](decisions/0032-identity-sources-and-the-handoff.md)), and `weida-openbao`
  (`crates/openbao`) supplies OpenBao-backed sources. Tests: `tests/identity.rs`,
  `tests/identity_source.rs`.
- **Runtime ownership.** `Runtime::new` takes the ambient reactor, `Runtime::with_handle`
  somebody else's and `Runtime::owned` creates and owns one, so no caller has to stand inside
  `#[tokio::main]`; `tests/foreign_executor.rs` drives weida from a non-Tokio executor.
  `Runtime::shutdown` is abortive and bounded by `shutdown_timeout`.
- **Dialling.** A dial tries every resolved address in order, capped by
  `max_resolved_addresses` and with a per-attempt bound (`connect_attempt_timeout`), through
  the pluggable `Resolver` of `weida-runtime`.
- **Observation and restarts.** `connection_stats()` on every dialling handle reads the health
  of a connection, with the peer's own view when both sides enable `Limits::path_report`
  (frame kind 8; [0036](decisions/0036-connection-statistics.md); `tests/stats.rs`). A QUIC
  binding can serve on a socket the process received (`Listener::bind_quic_socket`) and
  `Binding::hand_over` passes it to a successor whose `take_over` replays the held closes
  ([0038](decisions/0038-restart-handoff.md), [0039](decisions/0039-handover-says-when.md)),
  so diallers redial at once.
- **Limits.** Every remote-influenced allocation has a ceiling: `Limits` is a per-connection
  profile and `RuntimeConfig` holds the per-runtime numbers (`max_connections`,
  `max_connections_per_peer`, `endpoint_queue`, `max_resolved_addresses`); the tables are in
  [PROTOCOL.md](PROTOCOL.md) §10 and [INVARIANTS.md](INVARIANTS.md), and worst-case header
  memory is 1 MiB: `max_header_bytes` times `max_header_streams`, the streams that may read a frame head at once.

### Phase 3 — Brokerless messaging patterns

Master doc §82 asks whether a smaller internal primitive set implements the patterns cleanly.
The answer is four primitives — P1 one-way transfer, P2 exchange (a bidirectional stream with
nothing behind it), P3 peer set plus selection policy, P4 bounded inbound queue behind an
opaque path — recorded with the Router/Dealer-is-emergent rationale in
[ARCHITECTURE.md](ARCHITECTURE.md) §6a.

- **L0, the stream core.** `Peer` (`connect`, `open`, `open_bi`) and `Acceptor`
  (`Listener::acceptor`, `accept()` returning `Incoming`) in `crates/weida/src/stream.rs`,
  proven by `tests/raw_streams.rs`; datagram flows (`Peer::open_flow`, `Flow`) in `flow.rs`
  ([0034](decisions/0034-late-is-lost.md); `tests/flows.rs`).
- **L1, the patterns** (`endpoint.rs`, `pubsub.rs`, `radio.rs`, `segment.rs`):
  - Req/Rep: `Requester` and `Replier` (`tests/reqrep.rs`).
  - Push/Pull: `Pusher` connects and round-robins over its peers, `Puller` binds
    (`tests/pushpull.rs`).
  - Pub/Sub: `Publisher` binds, `Subscriber` connects; segmented topic filters (`.`-separated
    segments with `*` and a trailing `#`, [0007](decisions/0007-topic-namespace.md)); fan-out
    with a per-subscriber byte budget and counted drops, so a slow subscriber never stalls the
    publisher; streaming fan-out through `Publisher::open` and `FanOut::write_now` or
    `write_within` (`tests/pubsub.rs`).
  - PAIR, SURVEY and BUS: `Paired`, `Surveyor` with `Respondent`, and `BusMember`, with no wire
    vocabulary of their own ([PATTERNS.md](PATTERNS.md) §6; `tests/pair.rs`,
    `tests/survey.rs`, `tests/bus.rs`).
  - RADIO/DISH: `Radio` and `Dish` fan segments out with supersession, per-dish expiry and
    layers, over streams and over datagram flows ([0034](decisions/0034-late-is-lost.md),
    [0037](decisions/0037-layered-segments.md); `tests/radio.rs`).
  - A pattern that meets an endpoint serving another one gets a refusal and the connection
    stays intact ([PROTOCOL.md](PROTOCOL.md) §9.4).
- **The transport boundary.** `Link`, `SendHalf` and `RecvHalf` are enums in `transport.rs`
  rather than trait objects, because the set of transports is closed and the payload path
  stays a direct call. Everything above the boundary is transport-blind, which is what makes
  [PROTOCOL.md](PROTOCOL.md) §2.1's "same frames, same HELLO, same negotiation" checkable:
  `tests/transports.rs` runs one body of each pattern over QUIC, in-process, `AF_UNIX` and, on
  Windows, named pipes.
  - *In-process* (`weida+inproc://<bus>/<path>`, `inproc.rs`): a process-global registry maps
    a bus name (at most 256 bytes) to a bound listener, and a stream is a `tokio::io::duplex`
    pair whose capacity is `stream_receive_window`.
  - *`AF_UNIX`* (`weida+unix://`, `unix.rs`) and *named pipes* (`weida+pipe://<name>/<path>`,
    `pipe.rs`, Windows only): the OS connection is the stream, one per transfer
    ([0010](decisions/0010-local-transport.md)). The grouping of
    [0012](decisions/0012-local-connection-grouping.md) is written once, generic over the
    transport's `Stream` trait (`grouped.rs`): the first connection a client opens is the
    control connection, which carries HELLO both ways and owns everything kept per peer;
    every further connection opens with preamble `0x02` and the 16-byte group token the
    server issued, and is admitted only if the token names a live peer and the kernel
    credentials match; dispatch is by path (`Link::dispatch_by_path`). Both frame a stream in
    chunks (`chunked.rs`: payload, FIN, RESET with a code), because a pipe has no half-close
    and a unix socket has no abort; a reader that stops drains the rest on the runtime, so a
    refusal reaches the writer through the reply.
  - *No TLS locally.* A local binding takes no identity, a local dial ignores the trust it is
    handed and the address form rejects `sha256:…@`. The kernel proves the peer:
    `IncomingMeta::peer` is a `PeerIdentity::Local` principal on `AF_UNIX` (`SO_PEERCRED`, or
    `LOCAL_PEERCRED` with no PID on macOS), `PeerIdentity::Windows` with the client's SID on a
    pipe, and `None` in process. The scheme alone picks the transport; nothing falls back from
    one to another.
  - *Bounds.* `max_local_streams` (255, the tightest platform limit) counts the live transfers
    on one local connection, and an `open` at the cap waits for one to end, as a QUIC `open`
    waits on the peer's stream budget. Bind hygiene — stale-node removal, an explicit `0600`,
    the `sun_path` budget, and for pipes the first-instance flag, an owner-only DACL and
    remote clients rejected — is `weida-runtime`'s. A local dial is a fresh connection: there
    is no pooling and no control/bulk split locally.
  - *The reverse pool.* A publisher has no stream to a peer that dialled it, so a `Subscriber`
    over a socket transport parks `Limits::max_parked_reverse` (8) connections, each opened
    with preamble `0x03` plus the group token and admitted by the same rule as a transfer
    connection. The publisher writes fan-out on them and the subscriber dispatches by path; a
    parked connection is replaced when the peer writes its first byte. The pool is capped by
    `max_parked_reverse` and by `max_local_streams`. An empty pool is a counted drop in
    `Publisher::dropped`, not a stall and not a teardown, and the subscription survives.
    `max_parked_reverse = 0` leaves no route for a copy at all, so `Subscriber::connect`
    returns `Unsupported` rather than registering a subscription that would be silent
    (`a_subscriber_that_parks_nothing_is_refused_at_connect`).

**Not built**

- **Coalescing backpressure** (master doc §27). Only `Block`, `Reject` and fan-out `Drop`
  exist. The level `Coalesce` stays reserved for the L2 broker with its definition written
  down — a keyed queue at a hop, the key being the DATA `topic` — and a fan-out coalescer at L1
  is refused, because holding the superseded value per key per subscriber is the
  materialization that streaming fan-out exists to avoid
  ([decisions/0016](decisions/0016-conflation.md)). "Keep the newest" is the producer's single
  slot over `Publisher::open` and `FanOut::write_now`.
- **A subscription verdict.** SUBSCRIBE has no reply half and weida adds no verdict frame
  ([decisions/0017](decisions/0017-subscription-verdict.md)). The refusals a subscriber sees
  are local — the reverse pool's `connect`-time refusal above — or `LIMIT_EXCEEDED` closing
  the connection past `max_subscriptions`.
- **Router/Dealer as first-class types**, and the broker work they would actually require
  ([ARCHITECTURE.md](ARCHITECTURE.md) §6a, master doc §47 and §85).

### Phase 4 — Reliability

Responsibility is explicit and opt-in; a `core` connection writes nothing it does not use.

- **Outcomes.** `Error::Indeterminate` reports a transfer whose outcome is unknown, and the
  library never retries it ([FAILURE_MODEL.md](FAILURE_MODEL.md) §5). `Delivery::delivered()`
  is the transport receipt, explicitly not an application acknowledgement.
- **Guarantee sets.** HELLO keys `5` and `6` carry `guarantees_offered` and
  `guarantees_required`, each a `GuaranteeSet`: ten `Copy` fields — delivery, acknowledgement,
  the durability and replica axes, ordering, deduplication and its window, backpressure,
  producer naming, control isolation — with `GuaranteeSet::CORE` as the default, which is never
  written. `negotiate()` computes the effective set as the weaker of the two offers
  (`GuaranteeSet::intersect`) and fails with `NEGOTIATION_FAILED` when it does not reach either
  side's requirement (`GuaranteeSet::reaches`). `RuntimeConfig::guarantees` is one set, offered
  and required, so a peer that cannot match it fails the handshake and the configured set is
  the live one ([0027](decisions/0027-the-negotiated-set-is-the-configured-set.md)). A
  malformed declaration is a framing violation (`tests/hostile.rs`).
- **Per-producer ordering, opt-in** (`src/ordering.rs`). With `OrderingMode::PerProducerDetect`
  a runtime numbers its one-way transfers in DATA key `6` — per dialled path for `Peer::open`
  and Push, per topic for a published message — and the receiving connection reports what is
  missing through `IncomingMeta::gap` (`Gap { expected, seen }`, `missed()`) while delivering
  every message as it arrives; exchanges are not numbered. `PerProducerReassemble` holds an
  arrival whose predecessors have not come and releases the run in sequence order. A held
  transfer is an unread stream, so the hold pins transport memory, bounded by
  `Limits::max_reorder_hold` (256) and by the peer's receive windows: the bytes stay in the
  transport, where the receiver's own window already bounds them and the sender feels the hold
  as backpressure, because reading eagerly into an application buffer (the proposal of
  [0002](decisions/0002-control-and-bulk-separation.md) §6.6) would materialize the payload.
  When an arrival does not fit, the oldest held transfer of the scope holding the most is
  released out of order with its gap reported. The hold is bounded in count, not in time: a
  hole at the very end of a producer's traffic holds its successors until the connection
  closes. `Sequencer`, `GapDetector` and `Reassembler<T>` are inert under `core` and allocate
  nothing, and the scope table is capped by `Limits::max_sequence_scopes` (1024). Tests:
  `tests/reorder.rs`, `tests/pubsub.rs` (a dropped fan-out copy shows up as a gap) and the unit
  tests of `ordering.rs`.
- **Bounded deduplication** (`src/dedup.rs`). `Deduplication::Bounded` with `dedup_window_ms`
  suppresses a repeated `(producer, scope, sequence)` inside the window. The producer is the
  connection's proved fingerprint unless DATA key `7` names another
  ([0008](decisions/0008-session-identity.md) §4.4); the scope is the topic for a published
  copy and the endpoint path otherwise. A suppressed transfer is read to EOF and discarded, so
  the sender sees an ordinary transport receipt; suppressions are counted per runtime
  (`Runtime::suppressed_duplicates`). The window is bounded in time and in count
  (`Limits::max_dedup_entries`, 4096; at the cap the oldest entry is evicted), and the check
  precedes the gap detector, so a duplicate the receiver never accepted cannot shift its idea
  of where the producer is. `tests/dedup.rs` replays from the raw wire, because the library's
  own sequencer is monotone.
- **`drain`** (`src/drain.rs`, [0009](decisions/0009-drain.md)). `Runtime::drain(Duration)`
  returns `Drained { delivered, outstanding }` and is the counterpart of the abortive
  `shutdown`: admission stops, finished transfers are awaited to their transport receipt, then
  the same close runs, bounded by what is left of the same deadline. The deadline is mandatory
  and finite, and an expired drain is not an error. A binding refuses an incoming handshake
  while draining, and an inbound stream on an existing connection is answered with
  `STOP_SENDING(SHUTDOWN)` (`StopReason::ShuttingDown`, which maps to `Error::Rejected`); no
  frame announces it. What the drain waits on is the receipt of every finished transfer, which
  `Delivery::drop` parks in its own connection's set (`ConnDrain`) and a published fan-out copy
  parks at `finish()`; the set is bounded by that connection's stream budgets (by half of
  `max_local_streams` on a local transport), and a receipt evicted at the cap counts as
  outstanding. `tests/drain.rs`.
- **Cursors** (`src/cursor.rs`, [0023](decisions/0023-completion-is-a-cursor.md),
  [0024](decisions/0024-three-families-one-back-channel.md)). A completion is a level plus an
  absolute byte offset, reported on frame kind `6`, a unidirectional stream of its own that
  never shares a stream with payload. A sender orders levels per transfer with
  `TransferMeta::with_report` and reads them from `Cursors` (`changed`, `changed_within`,
  `CursorSet`, `Reported`); a receiver reports through `Reporter`. Levels `0..=15` are weida's
  ladder and `16` and above (`APPLICATION_FLOOR`) the application's, carried and ordered but
  never interpreted. Nothing waits on a cursor and no pattern's topology changes.
  `tests/cursors.rs`.
- **Transparent redial and the sender outbox** (`src/reconnect.rs`,
  [0031](decisions/0031-transparent-redial-and-the-sender-outbox.md)). A dialled address
  outlives its connection: `connect` records a slot that the runtime redials under a
  `ReconnectPolicy` (exponential backoff with jitter, which restarts only after a connection that lived at least `max`; `ReconnectPolicy::never()` switches it
  off), re-registers a subscriber's own route and filters on the new connection, refuses a peer
  that answers with a different key (`GiveUp::PeerChanged`) and reports every transition on a
  bounded `PeerEvent` stream. `open` waits for a live peer, bounded by
  `RuntimeConfig::send_timeout`. `send(body)` is a message the runtime owns from the call: with
  no live peer it waits in a per-endpoint outbox bounded by `outbox_messages` and
  `outbox_bytes` (`OutboxFull::Block`, `Drop` or `Reject`), is drained in order after the
  redial, and is never resent once written to a live connection — at-most-once, so
  `Indeterminate` keeps its meaning. Tests: `tests/reconnect.rs`, `tests/interrupted.rs`.

**Not built:** a retry policy; outgoing and incoming persistence; the coalescing helper (the
producer's single slot of Phase 3 stands in for it); and recovery. An application-level
idempotency id is also absent (§6).

### Phase 6 — Standalone broker

`weida-broker` (`crates/broker`, L2) is a layer, not a fork: an ordinary weida process that
holds messages for a while, built from the patterns the workspace already has. It depends only
on `weida` ([0018](decisions/0018-minimal-broker.md)).

- **Queues.** `Broker::new(&listener, BrokerConfig::with_queues([...]))` registers one
  in-memory queue per configured endpoint path; there is no declare frame, and a path another
  pattern holds is refused (`broker.rs`, `queue.rs`). `queue_bytes` bounds what a queue holds,
  with a fixed per-message overhead (`PER_MESSAGE_OVERHEAD`, 1 KiB: what a message costs
  while it is out for delivery) so that it bounds the message count too; a full queue refuses
  with ERROR `REJECTED` and the connection survives (`tests/admission.rs`). Producers are
  read concurrently, each under a reservation against the same budget, `max_inflight_admissions`
  at a time and `io_timeout` per wait, and deliveries are written one per subscription by
  tasks of their own, so no peer's silence is another peer's wait (`tests/stalls.rs`). A
  consumer whose delivery write fails or stalls past `io_timeout` has its connection closed
  with `LIMIT_EXCEEDED` through `weida::Consumer::close`, because dropping its subscription
  alone would leave a client that still believes it is subscribed; its unsettled deliveries
  return to the queue and its redial subscribes again
  (`a_consumer_that_stops_reading_has_its_connection_closed`).
- **Publisher confirm.** A producer's exchange is answered on the reply half with `Accepted`
  (DATA key `8`); a one-way producer that orders `Accepted` gets it as a cursor on a stream of
  its own, and one that orders nothing gets the transport receipt. `Stored` is never reported:
  `Achieved` has one variant, because `Stored` for an in-memory queue is a prohibited claim
  ([GUARANTEES.md](GUARANTEES.md) §1; `tests/admission.rs`, `tests/cursors.rs`).
- **Credit.** A consumer is an ordinary `Subscriber` that grants an absolute, cumulative
  per-subscription delivery limit (frame kind `5`, `Subscriber::grant`); initial credit is
  zero, the highest limit seen wins, and each message goes to exactly one consumer with credit
  (`consumers.rs`, `tests/delivery.rs`).
- **Settlement.** A delivery orders `Processed` on its own DATA header and moves to an
  `unsettled` table, keeping its charge against `queue_bytes`. The consumer's `Processed`
  report settles it and frees the bytes; a report that ends without one — the reporter
  finishes, the stream is reset or the connection goes — requeues it at the head.
  `BrokerConfig::max_unsettled` stops delivery to a subscription with that many outstanding
  (`tests/settlement.rs`).
- **Counters.** `Broker::stats`, `consumer_count` and `delivered` read a queue in process.

**Not built:** the redelivery count on the wire (the queue counts attempts per message; DATA
key `12`, `delivery_attempt`, is reserved and nothing writes it); relaying a consumer's report
to the producer ([0029](decisions/0029-a-report-is-relayed-a-certificate-is-not.md) §4.3);
retiring a queue (`retire(deadline)`, [0018](decisions/0018-minimal-broker.md) §4.7); an
inspection API beyond the in-process counters; and everything that needs a store or a replica
set, which is Phases 5 and 7.

### Phase 9 — Standalone foreign-protocol libraries

Six independent families, each a sans-I/O codec with an empty `[dependencies]`, a native
library with a bounded resource model and a Python binding (Phase 10; HTTP's is B-371). A library depends on
`weida-core`, `weida-runtime` and its codec, never on `weida` or `weida-protocol`, needs none
of the later weida phases and keeps its own protocol semantics: no general bridge or mapping
onto weida patterns exists ([0013](decisions/0013-competitor-libraries.md),
[0014](decisions/0014-parallel-libraries.md)). Every codec has golden vectors, deterministic
`fuzz_smoke` mirrors and a `fuzz/` crate of its own, and every library has a parity document
that states, row by row, what it does against the reference implementation
([libraries/README.md](libraries/README.md)).

| Family | Codec | Library | Interop |
| --- | --- | --- | --- |
| ZeroMQ | `weida-zmtp` (ZMTP 3.1) | `weida-zmq`: every socket type of `zmq_socket(3)` bar `ZMQ_STREAM`, `tcp`/`ipc`/`inproc`, NULL/PLAIN/CURVE with ZAP, the option table, the monitor and the devices | libzmq 4.3.5 (`tests/interop_libzmq.rs`, feature `libzmq-interop`, `#[ignore]`d) and zmq.rs (`tests/interop_zmqrs.rs`), both roles |
| nanomsg SP | `weida-sp` | `weida-nng`: one socket type per protocol, contexts, endpoints and pipes, the option table, TLS and IPC credentials | NNG 1.4.0-rc.0 through the `nng` crate (`tests/interop_nng.rs`, feature `nng-interop`, `#[ignore]`d), both roles |
| MQTT 5 | `weida-mqtt-codec` | `weida-mqtt`: a client | rumqttd 0.20.0 and rmqtt 0.23.1 (`tests/interop_rumqttd.rs`, `tests/interop_mqtt5.rs`, `#[ignore]`d) |
| AMQP 1.0 | `weida-amqp-codec` | `weida-amqp`: a client | `fe2o3-amqp` 0.17.0, both roles (`tests/interop_fe2o3.rs`); RabbitMQ (`tests/interop_rabbitmq.rs`, `#[ignore]`d) is written and not run |
| Core NATS | `weida-nats-codec` | `weida-nats`: a client | `tests/interop_nats_server.rs` is written and `#[ignore]`d, not run: it needs a `nats-server` |
| HTTP | `weida-http-codec` (HTTP/1.1, HPACK, HTTP/2 frames, static QPACK, HTTP/3 frames) | `weida-http`: client and pull-style server, HTTP/1.1, HTTP/2 over TLS by ALPN, HTTP/3 over quinn behind the feature `h3` ([0044](decisions/0044-weida-http.md)) | hyper 1.11.1 for HTTP/1.1 and HTTP/2 and h3 0.0.8 on h3-quinn 0.0.10 for HTTP/3, both roles, always run (`tests/io/interop_hyper.rs`, `tests/io/interop_h3.rs`) |

**Not built:** MQTT's and AMQP's server halves ([0014](decisions/0014-parallel-libraries.md)
§2).

### Phase 10 — Language bindings

Python is built for weida and for the first five libraries; `weida-http-py` is filed as B-371.
Every binding has an asyncio surface and
a synchronous surface and stands on `weida-py-core` (`crates/py/weida-py-core`), the shared
PyO3 foundation ([0014](decisions/0014-parallel-libraries.md) §2): one exception class per
error variant, the bridge that drives a Rust future on the caller's event loop — it polls once
inline and spawns only a future that has to wait, so a ready operation costs no loop wakeup
(§4.15) — and the `bytes` boundary (§4.14). One `abi3` wheel serves CPython 3.9 and up.

- `weida-py` (`crates/py/weida-py`) binds weida itself: `weida.Runtime` on a reactor of its
  own, QUIC `bind`, all six patterns with their streamed forms, RADIO/DISH, the cursor surface
  (levels are integers) and connection statistics, with a `weida.sync` module over
  `weida::blocking` ([libraries/weida-py.md](libraries/weida-py.md)).
- `weida-zmq-py`, `weida-nng-py`, `weida-mqtt-py`, `weida-amqp-py` and `weida-nats-py` bind the
  first five libraries, each following its own library, with a parity document in
  `docs/libraries/`.
- `weida::blocking`, behind the non-default `blocking` feature of `weida`, is the synchronous
  facade, in the shape the libraries' own `blocking` facades have: every method is a `block_on`
  around the asynchronous endpoint's own, over a `Runtime::owned` reactor the caller never
  sees, with payloads as `Vec<u8>` and an explicit byte ceiling on every receive. A call from
  inside a Tokio runtime would block the worker that has to drive what it waits for, so every
  entry point checks `Handle::try_current` and fails with `Error::Runtime` instead of
  deadlocking (`tests/blocking.rs`). A synchronous wait always has a deadline
  ([0009](decisions/0009-drain.md) §4.4): `Cursors::changed(deadline)` is mandatory-deadline.

**Not built:** a binding for any other language. The order is Node, Erlang, C, Java
([0030](decisions/0030-which-runtimes-and-in-which-order.md)); the JVM design is
[0019](decisions/0019-jvm-binding.md).

### Phase 11 — CLI and administration

The `weida` binary (`crates/weida/src/bin/weida.rs`) is four verbs over the same library the
examples use: `serve` (`--echo`, `--sink`, `--pub`), `request`, `send` and `sub`. The payload
is on stdin and stdout and nothing else is, so `weida request … | sha256sum` means what it
looks like; addresses, receipts and topics go to stderr, except that the addresses `serve`
prints are its output. The exit code names the error vocabulary a script can branch on: `3`
refused, `4` unknown endpoint, `5` no reply, `6` untrusted, `7` connection lost, `2` usage and
`1` for everything else, deliberately including `Indeterminate`, which must not become a
matchable number.

Two behaviours are decisions rather than omissions. A **bind** address is parsed by the binary
rather than by `Address::parse`, because a bind address may say port `0` and carries no
fingerprint: the one `serve` prints is its output. And `weida+inproc://` is refused with a
sentence naming why, since an in-process bus cannot be reached from another process.
Standard input and output go through `std`, not tokio's `io-std` and `fs`, because those
features are not in the library's dependency set and a binary is no reason to widen what every
downstream crate compiles. `sub --framing line|raw|nul|length` separates payloads on stdout; a
newline after each payload is readable and is not framing. `length` is the one form that holds
a payload, so `--max-bytes` (64 MiB by default) caps the hold, and a payload above it ends the
command with `LimitExceeded`; a received body that does not finish within
`RuntimeConfig::receive_timeout` is abandoned by `serve --sink` and `serve --echo`, which
answers at most 64 requests at a time. `tests/cli.rs` drives the binary the way a user does.

**Not built:** broker and cluster inspection, and richer administration.

### Phase 12 — Documentation/site/stabilization

- [GUIDE.md](GUIDE.md) teaches, in the shape ZeroMQ's zguide established, around one question:
  C8B, how a software system scales to eight billion people
  ([0026](decisions/0026-the-guide-and-the-c8b-question.md)). §0 states the question and its
  arithmetic on this repository's own measured numbers (§4), chapter 1 teaches one transfer
  and chapter 2 many peers and the drop. Every program in it is a file in
  `crates/weida/examples`, and `crates/weida/tests/guide.rs` asserts the claim its text makes.
- The website lives in its own repository and imports this document set at build time
  ([0033](decisions/0033-the-website-moves-out.md)); normative text is never rewritten there.

**Not done:** the stabilization list of master doc §79 before protocol 1.0 — external testing,
interoperability testing of weida's own protocol, heavy fuzzing, soak tests, failure
injection, a protocol audit, a security review, an API review, a benchmark suite and
compatibility tests.

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

## 4. Verified results

Every figure below was measured on one machine, an AMD Ryzen 7 5800X running Linux. Each
subsection names the command that produced its numbers and, where a result depends on them,
the toolchain and the build profile. No threshold is asserted on a benchmark number: the
figures are the baseline a later run is compared against. Where a test asserts a bound (the
memory ceiling of §4.1, the race-free bounds of §4.3), the subsection says so.

### 4.1 Reference prototype acceptance

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
demonstrates it. Requirement 9 is the one the project decided against; its row says so
rather than quietly claiming it.

| # | Requirement | Demonstrated by |
| --- | --- | --- |
| 1 | Establish QUIC connection. | `echo_roundtrip` in `crates/weida/tests/reqrep.rs`; examples `transform_server` + `transform_client` |
| 2 | Negotiate protocol. | `echo_roundtrip`; `hello_with_an_unsupported_version_fails_negotiation` in `crates/weida/tests/hostile.rs` asserting close code `NEGOTIATION_FAILED` |
| 3 | Open uni request stream. | `echo_roundtrip` — on a **bidirectional** stream: an exchange is one bidirectional stream rather than §83's paired unidirectional streams ([PROTOCOL.md](PROTOCOL.md) §4). A unidirectional request stream is the Push/Pull shape (`push_best_effort` in `crates/weida/tests/pushpull.rs`) and the raw L0 `Peer::open` |
| 4 | Send metadata header. | golden-vector tests in `crates/protocol/tests/golden_vectors.rs`; `echo_roundtrip` |
| 5 | Stream arbitrary-sized input. | `streaming_overlap` in `crates/weida/tests/reqrep.rs`; example `large_stream` |
| 6 | Server begins processing before FIN where possible. | `streaming_overlap` (handler consumes the first chunk before the request FIN) |
| 7 | Server opens correlated reply stream before request necessarily completes. | `streaming_overlap` (the handler replies on the exchange's reply half after the first chunk; the client withholds the remaining payload until at least one reply byte has arrived, making the overlap deterministic). The reply half exists from the moment the exchange is opened, so "opens" is free — and the correlation is the stream, not an id |
| 8 | Stream reply simultaneously. | `streaming_overlap`; the two halves are independent QUIC streams |
| 9 | Send protocol ACK on a separate control stream. | **Deliberately not met.** The v0 core carries no application ACK: brokerless, it would only mean "arrived in RAM", which QUIC already guarantees (§5, stream-core decisions). What remains is the transport receipt, `push_delivery_receipt` in `crates/weida/tests/pushpull.rs` asserting `Delivery::delivered()`. Accepted / Stored / Replicated / Processed belong to the L2 broker hop ([GUARANTEES.md](GUARANTEES.md)) |
| 10 | Propagate OpenTelemetry trace context. | `trace_propagation` in `crates/weida/tests/reqrep.rs`; `traceparent` fuzz target under `crates/protocol/fuzz`; the trace id printed by `transform_client` and logged by `transform_server` |
| 11 | Cancel mid-transfer. | `cancel_mid_transfer` and `reply_abort_when_reply_stream_dropped` in `crates/weida/tests/reqrep.rs`; `canceled_resolves_when_the_requester_walks_away` for the handler-side signal |
| 12 | Test connection loss. | `crates/weida/tests/hostile.rs`: `a_server_that_never_answers_yields_indeterminate` (closed after FIN) and `a_server_that_disappears_mid_stream_yields_connection_lost` |
| 13 | Test malformed headers. | `crates/weida/tests/hostile.rs`: garbage preamble and `header_len = 1 MiB` both assert close code `PROTOCOL_VIOLATION`; `weida-protocol` unit tests for duplicate keys, missing required keys (`missing_required_keys_are_rejected`) and oversized lengths |
| 14 | Fuzz parser. | fuzz targets `preamble`, `data_header`, `hello`, `traceparent`, `roundtrip`, `subscribe`, `cursor`, `credit`, `error` and `flow` under `crates/protocol/fuzz`, plus the deterministic `fuzz_smoke_*` tests in `crates/protocol/tests/fuzz_smoke.rs`. The fuzz crate declares its own `[workspace]`, so no workspace-wide build compiles it; §4.16 records a run |
| 15 | Benchmark small and large transfers. | `crates/protocol/benches/codec.rs` (header encode/decode); `crates/weida/benches/echo.rs` (`echo_1kib_rtt`, `echo_1kib_rtt_explicit`, `stream_throughput_64mib`) |
| 16 | Demonstrate bounded memory with a multi-GB generated stream. | `large_stream_bounded_memory` in `crates/weida/tests/large.rs` (ignored by default; run with `--ignored`), asserting checksum equality and peak RSS below 512 MiB for a 1 GiB echo; example `large_stream` for ad-hoc runs |

This prototype proves the central architecture.

### 4.2 Bounded memory and end-to-end baselines

Stable Rust 1.97.1. The bounded-memory rows are the result requirement 16 of §4.1 asks for;
the codec and end-to-end benches are the baseline later measurements are read against.

| Check | Command | Result |
| --- | --- | --- |
| Bounded memory, in process | `cargo test --release -p weida --test large -- --ignored` | 1 GiB echoed each way, checksums equal, 905 MiB/s, peak RSS **24.0 MiB** against a 512 MiB ceiling |
| Bounded memory, two process | `large_stream --gib 4` against the release server | 4 GiB echoed byte for byte, 908.7 MiB/s both directions, peak RSS **14.4 MiB** |
| Header codec | `cargo bench -p weida-protocol` | DataHeader encode 100.2 ns / decode 60.8 ns minimal, 218.5 ns / 146.8 ns fully populated; preamble parse 6.9 ns |

Peak RSS two orders of magnitude below the payload size is the substantive result: no stage
of the path buffers a whole transfer, which is what requirement 16 exists to establish.

| Benchmark (`cargo bench -p weida`) | Result |
| --- | --- |
| `echo/echo_1kib_rtt` | 58.2 us |
| `echo/echo_1kib_rtt_explicit` | 59.3 us, `open_bi`/`finish`/`recv` on the raw handles |
| `stream/stream_throughput_64mib` | 130 ms |
| `push/push_1kib_best_effort` | 7.9 us |
| `push/push_1kib_delivered` | 26.2 ms |
| `fanout/pub_1kib_8_subscribers` | 62.1 us |

The echo number is the substantive one: an exchange is one bidirectional stream with no
correlation bookkeeping behind it, which is what removing work rather than tuning it looks like.

`push_1kib_delivered` at ~26 ms is not a weida cost. On an idle loopback connection the
peer delays its acknowledgement up to QUIC's max ack delay, and `delivered()` waits for
exactly that acknowledgement. The receipt is a correctness signal, not a latency-sensitive
one, which is why `Pusher::send` discards it and callers who want it reach for
`Pusher::open` + `finish()` + `delivered()`.

### 4.3 What QUIC streams promise, measured

Every stream number was measured on loopback in a debug build against `quinn 0.11.11` /
`quinn-proto 0.11.17`. The byte counts below are observations; each test asserts the race-free
bound around them, which is noted where the two differ.

| Check | Command | Result |
| --- | --- | --- |
| A receipt beyond the window implies the reader consumed | `a_receipt_beyond_the_window_implies_the_reader_consumed` | 160 KiB pushed into a 64 KiB stream receive window: no marker at all within 300 ms while nothing is read; with an 8 KiB-chunk reader, `write_all` returns at **131072 bytes consumed** and the receipt resolves at **163840** (the whole payload). The assertion is the flow-control bound, `payload - window = 98304` |
| The stream window is not a payload budget | `a_payload_the_size_of_the_stream_window_waits_for_the_reader` | a payload of exactly 65536 bytes against a 64 KiB window produces no `Wrote` within 300 ms although the header was already parsed; it completes as soon as the reader consumes one eighth of the window (8192 bytes). The DATA header spends the same window as the payload |
| The connection window is the shared resource | `a_stalled_stream_does_not_block_its_siblings` | 32 KiB payloads, 64 KiB stream window, 256 KiB connection window: siblings of an unread stream arrive and read back correctly, and the stall lands at **7 unread streams = 229376 of 262144 bytes**; reading one stream's 32 KiB releases the stalled writer. The test asserts the bound `1 + blocked_at <= connection_window / payload` and the unblocking, not the index |
| The stream budget is backpressure, and `open` is where it lands | `the_stream_budget_is_backpressure_not_an_error`, `a_deeper_endpoint_queue_does_not_raise_the_stream_budget` | with `max_concurrent_uni_streams = 2` the third transfer blocks in `Pusher::open` — not in `write_all`, not in the receipt — and never errors; reading one transfer to EOF releases it. `endpoint_queue = 8` changes nothing, because a transfer parked in the queue still owns its stream |
| Cancellation | `cancel_discards_unread_bytes_and_keeps_read_ones` | the 4 KiB already read still compares equal; after `cancel()` the reader is served the whole 4 KiB buffered remainder and only then fails, as `io::ErrorKind::ConnectionReset` on the `AsyncRead` and as `Error::Canceled` through `read_capped` — never EOF |
| Idle timeout is connection loss | `idle_timeout_reports_loss_within_the_window` | a 500 ms server idle timeout against the client's default 10 s keep-alive (asserted to be the longer of the two): after 1.5 s of silence the endpoint reports `PeerEvent::Lost { cause: IdleTimeout }` — the cause, not only the outcome — and, with the redial of 0031, the next request rides the redialled connection |
| A lost peer is reported, and the redial is the runtime's | `a_server_that_goes_away_is_reported_as_connection_lost`; `crates/weida/tests/reconnect.rs` | under `ReconnectPolicy::never()` the **first** send after the server closed fails with `ConnectionLost(PeerClosed)` and `peer_count()` drops to 0. Under the default policy (0031): a server restarted on the same address is redialled with `Lost`, `Retrying`, `Connected` on the event stream, over QUIC, `AF_UNIX` and inproc alike; a body sent during the outage arrives after it, 100 of them in order; `open` waits through the outage and `send_timeout` bounds the wait with the loss cause; a subscriber's filters — including one added during the outage — are on the new connection; a replacement server with a new key is refused as `GaveUp { PeerChanged }`; the outbox bound blocks or drops as `outbox_full` says and a body over `outbox_bytes` is `LimitExceeded`; once every address is given up a `send` fails with the loss even with bodies queued, and a sender blocked at the bound is woken with it |

The two probes that resisted determinism are recorded as observations only: the buffered tail
delivered after `cancel()` is a genuine race between the reset frame and the reader's polls,
and the exact stall index in the connection-window probe depends on header sizes. Both tests
assert the race-free part instead, and no stream probe is `#[ignore]`d.

### 4.4 DATA header cost at a high message rate

What the two DATA keys of [decision 0001](decisions/0001-sequence-field.md) cost per message.
Each key is simulated by an existing key with its exact wire shape: `content_len` for the
`uint` sequence, and a `sha256:<64 hex>` `content_type` for the producer identity. Both
endpoint paths are six bytes, so the two keys are the only difference. Release profile,
loopback, debug assertions off.

| Check | Command | Result |
| --- | --- | --- |
| Bytes per message | `cargo bench -p weida --bench patterns -- header` (printed by the bench) | minimal DATA frame **135 B** for a 64-byte payload; with the two keys **215 B**, i.e. **+80 B**, +59.3 % of the whole frame. The split is `content_len` 6 B (1 B key + 5 B `uint`) and the 71-byte producer string 74 B (1 B key + 2 B `tstr` prefix + 71 B) |
| Messages per second | `cargo bench -p weida --bench patterns -- header --warm-up-time 1 --measurement-time 3`, two runs | minimal `push_64b_keys0` 5.57 µs → **179.4 Kmsg/s** (5.43-5.71 µs); with two keys `push_64b_keys2` 6.14 µs → **162.9 Kmsg/s** (6.01-6.27 µs). Run-to-run change on the same tree was within ±3 % (p = 0.38-0.41), so the **−9 % throughput** between the two variants is larger than the machine's spread |

Two conclusions follow. First, the cost is the bytes, not the encoding:
80 B more header for a 64-byte payload costs 9 % of the message rate, and the same two keys on
a 64 KiB payload are noise. Second, the 215 B figure is the **worst case**, an explicitly named
producer spelled as `sha256:<64 hex>`. Decision 0008 §4.4 settled the encoding from these
numbers: the receiver already knows the sending peer's fingerprint from the handshake, so the
producer key is omitted entirely in the default case and the default pays only the sequence's
6 B; where the producer is not the connection peer the key is a CBOR `bstr` holding the raw
32-byte digest, 35 B instead of 74 B, and the hex string stays presentation only. The codec is
exactly that, with golden vectors in [PROTOCOL.md](PROTOCOL.md) §8; a connection on the `core`
guarantee set writes neither key.

**The 135 B baseline includes a trace context nobody asked for.** The frame measured here
carries an unconditional 55-byte `traceparent` — 60 B of frame with its key and the length
field. The send path writes a `traceparent` only for a caller-supplied context — a context is
propagated and never minted ([0028](decisions/0028-trace-propagation-is-the-callers.md)) — so
the minimal frame is **76 B** rather than 135 B, and the two keys' +81 B is **106 %** of a
frame rather than 59 % of one. The conclusion that the cost is the bytes rather than the
encoding is unchanged. §4.5 measures the frame without it; the numbers above stay because
decision 0008 was taken from them.

### 4.5 What the frame costs once nothing mints a trace

A DATA header carries a `traceparent` only when the caller supplies a context: a minted root
is a fabricated fact, not a safe default
([0028](decisions/0028-trace-propagation-is-the-callers.md)). An unconditional one would cost
a 64-byte push a fixed 60 bytes of frame for a trace context nobody asked for — the largest
single item in it, against ZMTP's one to nine bytes of framing per message and SP's eight —
and [GUIDE.md](GUIDE.md) §0.3's C8B arithmetic puts that at ≈480 GB of an ≈1.08 TB
message-to-everybody. This is what the frame costs without it.

`cargo bench -p weida --bench patterns -- header --warm-up-time 1 --measurement-time 3`, two
runs, release, loopback. The bench's `wire_bytes` helper builds the header the send path
builds, and a third variant, `push_64b_traced`, measures a caller-supplied context in the
same run.

| Variant, 64-byte payload | Frame | Median | Rate |
| --- | --- | --- | --- |
| `push_64b_keys0`, no trace context | **76 B** | 5.11-5.19 µs | **192.6-195.9 Kmsg/s** |
| `push_64b_traced`, caller's context | **136 B** (+60 B) | 5.84-5.92 µs | 168.9-171.2 Kmsg/s |
| `push_64b_keys2`, the two 0001/0008 keys | 157 B (+81 B) | 5.75-5.83 µs | 171.5-173.8 Kmsg/s |

**A 64-byte message is twelve bytes of framing**, half of it the six-byte endpoint path, so
84 % of the wire is payload. weida is in ZMTP's and SP's class on small messages.

**A context costs 60 bytes of frame and 13-14 % of the message rate.** Both variants are
measured in one run, which is the only honest comparison: the run-to-run spread of the
measuring machine is wider than several of the effects measured in §4.10, and the 179.4
Kmsg/s of §4.4 is from another day. The 58 bytes of that 60 are the header (1 B key, 2 B
`tstr` prefix, 55 B value) and the other 2 are the frame's own length field; a unit test
asserts the 58 and the bench prints the 60.

**What does not change**: `tracestate` travels only where a context does, `IncomingMeta`
yields `None` for an absent **and** for a malformed value, so no receiver has a state it did
not already have, and a peer that writes key `3` on every frame is conformant
([PROTOCOL.md](PROTOCOL.md) §6.2).

### 4.6 What parking a receipt costs the send path

The drain of [0009](decisions/0009-drain.md) parks the receipt of every finished transfer
nobody is waiting on, which puts work on the path §4.4 measures. This is what it costs, on the
same bench, release profile, loopback: `cargo bench -p weida --bench patterns -- header
--warm-up-time 1 --measurement-time 3`, two runs per tree, throughput in Kmsg/s.

| Tree | `push_64b_keys0` | `push_64b_keys2` |
| --- | --- | --- |
| Baseline: before the drain (`9a82f4c`) | 163.0, 162.5 | 152.3, 153.6 |
| Rejected shape: one runtime-wide `Mutex<VecDeque>`, reaping under the lock | 163.9, 162.7 | 147.6, 149.6 |
| Shipped shape: per connection, one uncontended lock, push only | 155.7, 162.2 | 147.6, 152.1 |

The runtime-wide shape is the wrong one regardless of the number — a structure shared by
every connection and every worker thread, plus a poll of quinn's connection lock under it,
for an operation that runs once at shutdown — so the parked set lives on the connection beside
the sequencer and the gap detector, and the runtime-wide state holds only the admission flag,
the connection list and the eviction counter.

**What the numbers do and do not show.** The keys2 column shows the runtime-wide shape ~3 %
below the baseline in both runs and the per-connection shape back inside it. Nothing sharper
than that is available from this bench: the run-to-run spread of the measuring machine is
**wider** than the ±3 % of §4.4 — the same tree produced 155.7 and 162.2 Kmsg/s in
consecutive keys0 runs, and lengthening the measurement to 10 s per run did not settle it
(baseline 149.0 and 162.4; per-connection 160.9, 163.6, 156.7). So the honest statement is:
**the residual cost of parking is not resolvable against this bench's noise**, and the claim in
the code is worded as what it does — one uncontended lock and a push, once per transfer — not
as "free".

### 4.7 Reassembly buffer under cross-stream reordering

What an application-side reorder buffer costs when transfers complete out of dispatch order,
a cost [0001](decisions/0001-sequence-field.md) §8 asks to be measured. The probe is
`reverse_order_completion_measures_the_reorder_buffer` in `crates/weida/tests/streams.rs`: it
opens N transfers, writes a dispatch sequence into each, finishes them from the last to the
first, and reports each transfer's sequence from its own task, so what is measured is
completion order rather than the order a single-threaded reader imposes on itself. The buffer
is then simulated exactly: hold what cannot be released, release as soon as the next sequence
is present, record the peak. Debug build, loopback; every figure below was identical across
five runs.

| Case | Command | Result |
| --- | --- | --- |
| N = 16, FINs queued back to back | `cargo test -p weida --test it streams::reverse_order_completion -- --nocapture` | **0 of 16** out of dispatch position, peak reorder buffer **0**. Reversing the FIN order changes nothing the application can see: the FINs are queued in one burst and quinn transmits the pending streams in its own order, so arrival order follows dispatch order |
| N = 256, FINs queued back to back | same | **90 of 256** out of dispatch position, peak reorder buffer **84** transfers (33 % of the transfers in flight), first divergence at position **29**. The first 29 arrive in order; beyond that, concurrent per-transfer tasks are the reordering source, not the FIN order |
| N = 16, each FIN awaited to its transport receipt | same | **16 of 16** out of dispatch position, peak reorder buffer **15** = N − 1, arrivals exactly reversed (`[15, 14, 13, …]`). Serializing the FINs is what makes the sender's order reach the receiver, and then the buffer must hold every outstanding transfer but one |

Two results matter for the reassembly mode of 0001 §7.5. First, the bound is N − 1 and it is
reachable: an eager reassembler must be capped by construction, which is what the INVARIANTS
follow-up of 0001 §8 asks for — the cap is a configured number of held transfers, not a hope
about arrival order. Second, at 256 transfers in flight a third of them were already held
without any adversarial pattern, so the cap has to be chosen against normal concurrency rather
than against the worst case alone. The probe asserts only the invariants — every transfer
arrives exactly once, the buffer drains empty, the peak stays below N — because QUIC promises
no ordering across streams and a test that pinned 84 would pin an accident of the machine.

### 4.8 What a connection to a peer costs

The figure [0002](decisions/0002-control-and-bulk-separation.md) needs: what the *second*
connection to a peer that is already connected costs, in latency and in memory. The bench is
`crates/weida/benches/connections.rs`, loopback, pinned trust, no client identity (mTLS off).

The memory figures use one client runtime per connection against one server, with both ends
in this process, and they separate the two deltas: runtimes are built and measured *before*
anything is dialled, so the per-connection number excludes the per-runtime quinn endpoint and
UDP socket.

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
is why the per-path split is worth a connection each, and is what the pool does. It is also why the
*control* tier is not: a millisecond is cheap for isolation and expensive for nothing, and
[decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3 shows the tier would carry
nothing in v0. These numbers also set the scale for the per-peer ceiling: 64 connections to
one peer is ~50 MiB of transport state on the pair, which is why
`max_connections_per_peer` (64) is a named bound ([INVARIANTS.md](INVARIANTS.md)) rather than
the path count being the only one.

RSS is read from `/proc/self/status` `VmRSS` and the allocator does not return everything
between measurements, so each row's baseline is the previous row's residue; the per-connection
deltas are the trustworthy part and the absolute totals are not.

### 4.9 Connections per dialled path

The bench of §4.8, extended with the per-path fan of
[0002](decisions/0002-control-and-bulk-separation.md) and the point at which a server refuses.
Two shapes are measured because they are two different systems: a pool keyed on
`(host, port, ClientTls, address fingerprint)` and **not** on the path, so that every path a
runtime dials shares one connection; and 0002's bulk tier, which makes it one connection per
path.

| Check | Command | Result |
| --- | --- | --- |
| 16 paths, pooled per authority | `cargo bench -p weida --bench connections` | **1.13-1.15 ms** for all 16 dials: one handshake and fifteen pool hits, 16 `(connection, path)` peer entries over **one** QUIC connection |
| 256 paths, pooled per authority | same | **1.47 ms** for all 256 dials, 256 peer entries over **one** connection. Dialling more paths costs microseconds, not connections |
| 16 paths, one connection each (0002) | same | **22.6-23.6 ms** total, **1.41-1.47 ms** per path |
| 256 paths, one connection each (0002) | same | **277.7 ms** total, **1.08 ms** per path — no degradation with the count, and 995 KiB of RSS per connection for both ends together, consistent with §4.8's 750-850 KiB at 64 |
| The refusal point | same | with `max_connections = 8`, connections 0 through 7 are accepted and the ninth dial fails at once: the server refuses its Initial with `CONNECTION_REFUSED` before the handshake is accepted and builds nothing for it (`crates/weida/src/listener.rs`) |

The consequence, as the pool implements it: **a per-path connection is affordable in time and
linear in memory, but it converts a path count into a connection count against
`max_connections`** — a default of 1024 accepted connections per binding is 1024 paths' worth
of fan from a *single* client if nothing else bounds it, which is why `max_connections_per_peer`
(64) exists and counts per proved fingerprint
([INVARIANTS.md](INVARIANTS.md), [PROTOCOL.md](PROTOCOL.md) §10). A client that dials 256
paths pays 278 ms of handshakes where one pooled connection pays 1.5 ms, so a path dialled is
not a path used: connections are created on the first dial of a path and never pre-created,
which is what the pool does. No control connection is added on top
([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3).

**An IP literal costs no lookup.** `Exec::resolve` parses an IP literal in place and keeps the
task only for real hostnames, which `lookup_host` needs for its Tokio context. Allocating the
host string, spawning a task and awaiting its join handle on **every** `connect`, including
for a literal `127.0.0.1` that needs no lookup at all, costs `connect/cold_handshake`
**1.18-1.24 ms** (+12 %, p = 0.00); with the literal parsed in place it measures
**1.05-1.09 ms**, `change: −11.6 % (p = 0.00)`. Every pinned deployment dials literals,
because a pinned address names a key rather than a name, so this is the common path and not an
edge case.

### 4.10 Segment matching in the fan-out path

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
| The byte-prefix matcher, for scale | same | 64 `topic.starts_with(filter)` comparisons in a tight loop: **29.3 ns**, i.e. **0.46 ns each** |
| One matched fan-out, for scale | `cargo bench -p weida --bench patterns -- fanout` | `pub_1kib_8_subscribers` **64.5 µs** for publish plus eight receives |

**The shape does not matter.** A walk that fails in the first segment, one that has to cross a
`*` to reach the third, and one that meets a trailing `#` differ by 5 % — inside the
run-to-run spread. That is the answer to the objection: the per-filter cost is dominated by
iterating the subscription registry, not by the grammar, and `#` being legal only in the final
segment is what keeps it that way (no backtracking, so the walk is linear in the filter).

**The scale is what settles it.** Sixty-four non-matching filters cost about **1 µs** of a
publish, against **64.5 µs** for one matched fan-out to eight subscribers — under 2 % — and a
publisher hits `max_subscriptions = 256` long before that becomes visible. The byte prefix that
segment matching replaces is ~0.5 ns per comparison in isolation, so a hot-path-only argument
could prefer it; what it cannot do is express a segment boundary at all, which is the trade
0007 recorded and these numbers price.

### 4.11 Per-subscriber fan-out at width

The number the whole C8B argument of [GUIDE.md](GUIDE.md) §0.1 rests on. The largest fan-out
of §4.10 is **eight** subscribers, while [PATTERNS.md](PATTERNS.md) §1.3 makes an isolation
claim — a stream nobody reads does not delay its siblings — whose whole point is that it holds
at width.

`crates/weida/benches/fanout.rs`, one client runtime per subscriber as in §4.8, both ends in
this process, loopback QUIC, `RuntimeConfig::default()`, 1 KiB payload, release profile, three
consecutive runs on an idle desktop. The publisher's own cost is a criterion
distribution; latency and memory are set measurements printed by the bench, because a *set* of
subscribers has no criterion shape — the same reason §4.8's memory figures are printed.

**Two latencies are reported and they differ by two orders of magnitude**, which is why a
burst is not a latency: publishing 64 messages back to back and then measuring arrival gives
the backlog, not the path. The *idle* figure publishes one message and waits for every copy
before the next; the *burst* figure publishes 64 without waiting. Both are below, labelled.

| Measured | Command | Result |
| --- | --- | --- |
| Publish cost, width 1 | `cargo bench -p weida --bench fanout` | **287 ns** (282-294 ns) per call |
| Publish cost, width 16 | same | **3.04 µs** (3.01-3.08 µs) |
| Publish cost, width 256 | same | **22.94 µs** (22.78-23.12 µs) → **88.8 ns per additional subscriber** against width 1 |
| Idle latency, width 1 | same | **34.7-37.9 µs** median, **79-98 µs** p99 |
| Idle latency, width 16 | same | **76.2-77.8 µs** median, **142-494 µs** p99 |
| Idle latency, width 256 | same | **507-539 µs** median, **816-989 µs** p99, **0.91-1.22 ms** max |
| Burst drain, width 256 | same | 64 messages enqueued in **1.6-1.9 ms**, 16 384 copies delivered in **58.7-59.4 ms** → **276-279 Kcopies/s**; burst latency **20.6-22.4 ms** median |
| Transport state per subscriber | same | **396-436 KiB** at widths 16 and 256, covering **both** ends; width 1 is 1.63-1.76 MiB because it also pays the one-off crypto and endpoint state, exactly as §4.8 shows |
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
10³ about **276**. That is the number a topology is designed against.

### 4.12 The dedup key's allocation per call

`DedupWindow::is_duplicate` builds its lookup key with `scope: scope.into()`, which allocates
a `Box<str>` on **every** call — including the two paths that insert nothing: a duplicate that
is found, and the probe before an insert. Restructuring the key on suspicion is not worth
doing; the number below is what decides it.

It cannot be measured through the public surface. `DedupWindow` is `pub(crate)` and its only
public path is a negotiated connection receiving DATA, where one loopback message costs
microseconds and would bury a nanosecond answer — so the `dedup_key` group of
`crates/weida/benches/patterns.rs` measures the two *shapes* as pure functions, the way §4.10
measures the byte prefix that segment matching replaces. Tables hold `max_dedup_entries = 4096`
entries over eight scopes, with a 19-byte scope string. Two runs, release, idle machine.

| Shape | Miss | Hit |
| --- | --- | --- |
| **owned** — the current shape: one flat `HashMap<Identity, _>`, key allocates per probe | **41.1-41.3 ns** | **43.8-48.1 ns** |
| the same table, key built once (the allocation removed, nothing else) | 31.4 ns | 35.1-35.6 ns |
| **borrowed** — the candidate: `HashMap<Box<str>, HashMap<(producer, sequence), _>>`, outer lookup borrows `&str` | 42.0-42.8 ns | 44.7-46.3 ns |

`cargo bench -p weida --bench patterns -- dedup_key --warm-up-time 1 --measurement-time 3`

**The allocation costs ~10 ns**, which the middle row isolates: 41.2 against 31.4 on a miss,
and the same gap on a hit.

**And removing it does not pay.** The candidate shape is *slower* on a miss — 42.0-42.8 ns
against 41.1-41.3 — and the same within noise on a hit, because a two-level table hashes twice
and the second hash costs what the allocation cost. That is the whole finding: the obvious
optimization buys nothing, so the flat key stays and `dedup.rs` says so with the number
beside it. For scale, 10 ns is **0.13 %** of a 1 KiB push (~7.9 µs), and deduplication is
opt-in — a connection that negotiated `core` allocates nothing here at all, which the
capacity assertions in `dedup.rs` already prove.


### 4.13 What the local transports cost against QUIC

The price of [0010](decisions/0010-local-transport.md) §4.2's choice, "the OS connection **is**
the stream, one per transfer": the same patterns over all three transports, at a payload
where the overhead is the whole figure and at one where the copies are. The bench is
`crates/weida/benches/transports.rs`, release profile, both ends in this process, loopback for
the QUIC rows. Two runs; the spread below is the two runs' point estimates.

`cargo bench -p weida --bench transports -- --warm-up-time 1 --measurement-time 3`

| Pattern | Payload | QUIC (loopback) | inproc | `AF_UNIX` |
| --- | --- | --- | --- | --- |
| Req/Rep round trip | 1 KiB | **58.3-58.8 µs** | **13.2-13.5 µs** (4.4×) | **50.6-50.8 µs** (1.2×) |
| Req/Rep round trip | 1 MiB | **2.70-2.73 ms** | **144-145 µs** (18.6-19.0×) | **266-312 µs** (8.7-10.3×) |
| Push one-way | 1 KiB | **7.97-8.08 µs** | **2.03-2.07 µs** (3.9×) | **24.0-24.1 µs** (0.33×) |
| Push one-way | 1 MiB | **1.358-1.362 ms** | **57.7-79.5 µs** (17.1-23.6×) | **130-147 µs** (9.2-10.5×) |
| RSS per live connection | — | **995 KiB** (§4.9) | **430-1761 B** | **0-655 B** |

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
one weida does not have — which is the price of not reimplementing QUIC's stream layer, with a
number beside it.

**Two rules this bench exposes.** A receipt parked for the drain holds its send half, which
locally is a descriptor, so the parked set is bounded by half the local ceiling rather than by
QUIC's stream budgets: a sequential run must not exhaust `max_local_streams` with nothing in
flight. And a transfer holds its slot until both ends are done with it, so a sender that
outruns its puller reaches the ceiling, where `open` must not refuse with `LimitExceeded` —
`Reject` where [GUARANTEES.md](GUARANTEES.md) §6 promises `Block`. A local `open` waits for a
slot exactly as a QUIC `open` waits on the peer's stream budget, cancelled by dropping the
future and bounded by the caller's own deadline;
`push_pull_waits_for_a_slot_over_{inproc,unix,pipe}` pins it.

**Named pipes, measured on wohnzimmer (Windows 10, debug build).** A pipe instance is one
connection, and a client that finds no instance listening retries on the runtime's timer,
which Windows rounds to its 15.6 ms tick. With one listening instance, every waiting client
woke on the same tick and one got in: 600 concurrent one-way sends did not finish within 10 s,
under 60 transfers a second, and a client gave up after 5 s of `ERROR_PIPE_BUSY`. The binding
now keeps up to 16 instances listening, each holding a place, and a busy pipe is waited out
for as long as it lasts, the caller's own deadline ending the wait: the same 600 sends take
1.42-1.87 s, **320-423 transfers a second**, three runs
(`cargo nextest run -p weida --profile ci -E 'test(push_pull_waits_for_a_slot_over_pipe)'`
with the send loop timed, run through `run_jobs` on `check-windows-amd64`).

### 4.14 What a payload costs crossing into Python

The number [0014](decisions/0014-parallel-libraries.md) §2 asks for by name: the `bytes`
boundary of `weida-py-core` "avoids a copy where PyO3 allows it and measures the copy where
it does not". Release profile, `bytes` objects of four sizes, the GIL held throughout as it is
during a real conversion.

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

**What is deliberately not done.** A `memoryview` over Rust-owned bytes would make the
Rust → Python direction zero-copy, and it is not used: the buffer protocol entered CPython's
limited API in 3.11 while the wheel is `abi3` from 3.9, so it would be a zero-copy path
that exists on some interpreters and not others — a performance cliff rather than a feature.
The cost of not having it is the last column, 19 µs per mebibyte, against a 1 MiB local
round trip that §4.13 measures at 144 µs *inproc*: **about 13 %** of the cheapest
transport weida has, and proportionally less on anything that crosses a socket.

### 4.15 What a Python round trip costs against the Rust one

The comparison of a Python round trip with the Rust one. Release profile for both halves, and
the same shape on each side so that the difference is the language boundary and not the
measurement: one REQ/REP round trip — request out, reply back — over `inproc://` and over
loopback `tcp://`, at an empty payload and at 1 KiB, median of 2000 round trips.

```text
cargo run -p weida-zmq --release --example roundtrip_cost
.venv/bin/python crates/zmq/weida-zmq-py/roundtrip_cost.py
```

| Round trip | inproc, 0 B | inproc, 1 KiB | tcp, 0 B | tcp, 1 KiB |
| --- | --- | --- | --- | --- |
| `weida-zmq`, Rust | **16.5 µs** | **17.5 µs** | **32.4 µs** | **35.3 µs** |
| `weida_zmq.sync`, Python | **67.7 µs** (4.1×) | **72.3 µs** (4.1×) | **111 µs** (3.4×) | **116 µs** (3.3×) |
| `weida_zmq`, Python asyncio, one wakeup per `await` | **330 µs** (20×) | **334 µs** (19×) | **343 µs** (10.6×) | **346 µs** (9.8×) |
| `pyzmq` (libzmq 4.3.5) | **5.7 µs** | **6.0 µs** | **45.8 µs** | **47.2 µs** |

**The payload is not what costs.** 1 KiB adds 1-5 µs on every row, which is the copy
measured in §4.14. Everything else in the gap is per-*operation* overhead, and a round
trip is four operations, so each row's excess divides by four for a per-call figure.

**The synchronous surface costs ~13 µs per operation over Rust** (67.7 µs against 16.5 µs,
four operations). That is a GIL release and re-acquire plus a thread handoff: the facade
parks the calling thread in `futures::executor::block_on` and a reactor worker has to wake
it. It is the price of a blocking API over an asynchronous library, and it is the same price
`weida-zmq`'s own `blocking` facade pays in Rust.

**The asyncio surface costs ~78 µs per operation that has to wait, and the reason is one loop
wakeup per `await`.** Each awaited call creates a future on the running loop, the operation
completes on a reactor thread, and `loop.call_soon_threadsafe` writes to the loop's self-pipe
so that `epoll` wakes and the callback runs. That wakeup — not the GIL, not the copy, not the
protocol — is the 78 µs, and four of them are 313 µs of the 330. An operation that can
complete without waiting (a `recv` whose message is already queued, a `send` with room) pays
none of it: `Bridge::awaitable`'s first step polls the future once on the loop's thread,
inside the reactor's context and with a no-op waker, and hands a ready outcome back as an
already-resolved future, which `await` consumes without yielding to the loop at all; a pending
one is spawned and re-registers its task's waker on its next poll. The saving is measured on
the same script on an idle machine — the table above was taken with four worktrees compiling,
so its absolute numbers are higher — with and without the inline poll, release profile,
median of 2000:

| `weida_zmq` asyncio round trip | inproc, 0 B | inproc, 1 KiB | tcp, 0 B | tcp, 1 KiB |
| --- | --- | --- | --- | --- |
| without the inline poll, one wakeup per `await` | 145 µs | 144 µs | 157 µs | 155 µs |
| with it, ready futures resolved inline | **78 µs** | **82 µs** | **125 µs** | **122 µs** |
| `weida_zmq.sync`, same runs | 17.8 µs | 19.1 µs | 39.1 µs | 40.4 µs |

A round trip is four operations and two of them are sends into a queue with room, so two
of the four wakeups go: **−67 µs on inproc, about 33 µs per wakeup saved**, which is the
per-`await` cost with the loop otherwise idle. The two receives still wait for the peer and
still pay the wakeup, which is why tcp saves less: the reply takes longer to arrive than a
queue takes to have room. The inline poll keeps what the binding's tests pin: the future
starts at the first `await` (the inline poll runs inside the coroutine's first step, after it
has been scheduled), a cancelled `Task` aborts the reactor task, a panic arrives as a
`RuntimeError`, and the no-running-loop error is raised before anything is polled.

**Against `pyzmq` the honest reading has two halves.** Over `inproc://` libzmq is 12× faster
than this binding's synchronous surface and 58× faster than its asyncio one without the inline
poll, because libzmq's `inproc` is a memcpy between two threads of one process with no
reactor in the middle. Over
`tcp://` the gap closes and reverses on the synchronous surface at the top end — 111 µs
against 45.8 µs — and the Rust library is *faster* than libzmq (32.4 µs against 45.8 µs),
which is where the implementation actually competes. The binding's overhead is a Python
problem to solve in the bridge, not a protocol one.

### 4.16 The protocol fuzz targets

The fuzz crate declares its own `[workspace]`, so no workspace-wide build compiles it and the
gate says nothing about it: a target that does not compile is not a target that finds
nothing, and only running the targets says which of the two it is. The run below covers the
nine targets `preamble`, `data_header`, `hello`, `traceparent`, `subscribe`, `roundtrip`,
`cursor`, `credit` and `error`:

| Target | Runs | Result |
| --- | --- | --- |
| `preamble`, `data_header`, `hello`, `traceparent`, `subscribe` | 100 000 each | no crash, no OOM, no artifact |
| `roundtrip`, `cursor`, `credit`, `error` | 100 000 each | no crash, no OOM, no artifact |

`cargo +nightly fuzz run <target> -- -runs=100000 -max_len=20000`, nightly 1.100.0; every
`fuzz/artifacts/<target>` directory is empty afterwards. The `flow` target (the FLOW header and
the datagram prefix) is not part of this run; `fuzz_smoke_flow` covers it on stable. The
targets stay out of the push gate of [LOOP.md](LOOP.md) §6 deliberately: they need nightly and
a time budget.

### 4.17 Voice beside bulk

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
note set in advance, so 0034 §6 says a bulk profile that shares a bottleneck with media
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

### 4.18 One SWARM fetch from one seeder

What [0041](decisions/0041-swarm-sessions.md) §4.4 asks for: whether a fetch over one session
keeps up with a normal download. griasdi measured 4-10 MB/s for one fetch over the per-ask wire
of 0040 against 53.7 MB/s for HTTPS on the same 27 ms path. The ignored test sends 256 MiB in
1 MiB pieces through `delay_proxy` (13 ms each way) twice: once as one streamed reply read by a
requester with the swarm's receive windows (32 MiB stream, 64 MiB connection), once as a fetch
with `per_peer` 16, `max_inflight` 32 and no list given; the seeder generates each piece when it
is ordered and the caller only counts bytes. The bench is the same fetch of 64 MiB on plain
loopback, the connection dialled once outside the measurement. AMD Ryzen 7 5800X, Linux
7.2.11, rustc 1.99.1, release profile.

| Check | Command | Result |
| --- | --- | --- |
| 256 MiB, 26 ms RTT, two runs | `cargo test -p weida --features swarm --release --test swarm_large -- --ignored --nocapture` | download **45.8 / 44.8 MB/s**, swarm **50.8 / 55.3 MB/s** (1.11x / 1.23x); peak RSS 82.3 / 78.9 MiB |
| 64 MiB, loopback | `cargo bench -p weida --features swarm --bench swarm -- --warm-up-time 1 --measurement-time 3` | **683 MiB/s** (662-701), 93.8 ms per fetch |

**One fetch now runs at the rate of a normal download on the same path, and above it**: the
session keeps the seeder's next answer queued, so the connection is never application-limited
between answers. Both figures on the proxied path sit near 45-55 MB/s, the plain stream
included, so the ceiling there is the path as built (a userspace forwarder that sleeps per
packet; not measured on its own) rather than the session: on plain loopback the same fetch
moves 683 MiB/s. Peak RSS stays under a third of the payload, so
nothing buffers the blob. One named limit: the proxy neither loses nor reorders, so this says
nothing about a fetch under loss; griasdi's measurement against its server is the field check.

---

## 5. Decisions

### Serialization: CBOR via `minicbor`

Master doc §82 leaves the choice between MessagePack, CBOR and another compact evolvable
format open, with criteria: parsing speed, encoding speed, allocations, unknown-field
support, multi-language implementations, dependency cost, fuzzability, and deterministic
representation where needed.

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

### Naming, versioning and trust decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| URL scheme | `weida://` and, for the local transports, `weida+inproc://`, `weida+unix://` and `weida+pipe://` | The master doc's `mq://` is a working name (§82). One scheme constant per transport in `crates/core/src/addr.rs`; the scheme alone picks the transport. |
| ALPN token | `weida/0` | Couples the TLS-level protocol identity to the wire protocol version, so a version-0 peer cannot silently talk to a future version. |
| Stream magic | `0x57` (ASCII `W`) | Cheap first-byte rejection of non-weida streams. |
| Wire protocol version | `0` | Experimental per master doc §15; independent of the library version. |
| Library version | `0.1.0` pre-releases (`0.1.0-alpha.N`; a tree between releases carries the next number with `-dev`) | Independent of the wire version. |
| Client trust | pinned public keys **or** configured anchors | A peer is accepted for a pinned fingerprint or for a chain to an explicitly configured anchor that names the host dialled. No platform root store and no insecure-skip mode ships in v0, and `Trust::by_address()` trusts nothing beyond what an address names. |
| TLS material source | file **or** in-memory PEM (`Pem`) | Requiring a path would force callers holding a key from a secret store to write it to disk first. Both sources are first class; `Identity::from_pem` and `Trust::anchor` never touch the filesystem. |
| Credential placement | identity per **binding**, trust per **dialling endpoint** | Credentials are transport-specific, so they belong neither on the Listener (a namespace) nor on the Runtime (a resource container). Consequence: the connection pool keys on `(host, port, ClientTls, address fingerprint)` plus the path — sharing on authority alone would hand one endpoint a peer authenticated on another endpoint's terms. |
| Rust edition | `2024` | Current stable edition. |
| MSRV | `1.88` | Highest requirement among the pinned dependencies. |

### Stream-core decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| Req/Rep transport | one client-opened **bidirectional** stream per exchange | The stream is the correlation. There are no correlation ids, no per-connection pending table, no correlator actor and no CANCEL frame, and request and reply stream simultaneously on independent halves. |
| Application acknowledgements | **not in the core**; they belong to the L2 broker hop | Brokerless, an application ACK only means "arrived in RAM" — QUIC already retransmits and already reports transport receipt. Accepted / Stored / Replicated / Processed describe responsibility transfer to a hop that has taken responsibility; without such a hop the words are unearned ([GUARANTEES.md](GUARANTEES.md)). |
| Delivery signal | `OutgoingTransfer::finish() -> Delivery`, `Delivery::delivered().await` | The honest signal QUIC can actually give: quinn's `stopped()` resolving `Ok(None)` means the peer acknowledged every byte "although not necessarily the processing of it". `finish()` is synchronous and dropping the `Delivery` costs one uncontended lock and a push (§4.6), so fire-and-forget senders pay almost nothing ([FAILURE_MODEL.md](FAILURE_MODEL.md)). |
| Misroute refusal code | QUIC application error code `UNSUPPORTED = 9` | With ERROR frames confined to the reply half of an exchange, a unidirectional stream sent to a path that does not serve it has no frame to be refused with. A dedicated stop code keeps the refusal typed instead of collapsing it into `REJECTED` ([PROTOCOL.md](PROTOCOL.md) §7). |
| Raw stream API | `Peer` and `Acceptor` are public (`crates/weida/src/stream.rs`) | L0 is the product, not an implementation detail: if the patterns are the only way in, every unanticipated topology needs a new pattern. `Peer::open`/`open_bi` and `Acceptor::accept` expose the two QUIC stream kinds directly; the patterns are thin wrappers over them. |
| Stream limits | `max_concurrent_bidi_streams = 1024`; no `max_pending` | There is no pending-reply table to bound. Concurrent exchanges are bounded by QUIC itself, through the bidirectional stream budget. Worst-case header memory is `max_header_bytes * min(max_header_streams, uni + bidi)` = **1 MiB** ([INVARIANTS.md](INVARIANTS.md)); only `max_header_streams` streams hold a frame head at once. |

### Identity and trust decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| Peer identity | SHA-256 of the leaf's DER `SubjectPublicKeyInfo` (`sha256:<64 hex>`) | Hashing the public key rather than the certificate keeps a pin valid across a renewal that reuses the key, and it is the same value `curl --pinnedpubkey` and HPKP pin, so an operator can compute and compare it without weida. |
| Address may name the peer | `weida://[sha256:<hex>@]host:port/path` | One string then carries both where to dial and whom to accept — which is exactly what a discovery record, a config line or a line pasted into a terminal has to survive as. Reaching a self-signed peer safely needs nothing else configured. |
| Address fingerprint overrides `Trust` | the named identity is the only one accepted on that connection | The most specific statement of intent wins. An operator who wrote down which peer must answer did not mean "or anybody else my CA vouches for". |
| Empty `Trust` dials nothing but pinned addresses | `Error::Tls("nothing to trust …")`, before any packet | The only alternative to failing here is trusting everything, which must never be reachable by omission. An endpoint with no trust configured stays usable — for addresses that name their peer, and for nothing else. |
| Client identity | optional on the dialling side; a binding may require it | Master doc §6 makes client identity optional and §46 asks for mTLS. `ClientTls` therefore carries `Option<Identity>`, and `ServerTls::require_client` takes a `ClientTrust`: `Trusted(trust)` refuses anonymous and untrusted peers at the handshake, and `AnyKey` refuses only anonymous ones, verifying the handshake signature and judging nothing about the key ([0035](decisions/0035-keys-proved-not-judged.md) §4.1). Requiring an empty trust is rejected at bind time, since it would accept nobody. The chain a peer presented is kept only up to 8 certificates and 32 KiB (`MAX_PEER_CHAIN_CERTS`, `MAX_PEER_CHAIN_BYTES`): constants rather than `Limits` fields, because they bound what weida keeps rather than tune a workload; an `AnyKey` binding fails a larger chain in the handshake, a `Trusted` one keeps none of it. |
| Certificate generation | `rcgen`, behind default feature `generate` | `Identity::generate()` is what lets a pinned deployment handle no PEM at all, so it is on by default; behind a feature so a deployment that only loads issued certificates does not link a certificate builder. |
| Fingerprint dependencies | `rustls-webpki` (SPKI parsing) and `ring` (SHA-256) as direct dependencies | Both were already in the tree through rustls, so naming them directly adds no third-party code and no build time, which is what master doc §72's dependency discipline asks. Neither parsing a leaf's SPKI nor hashing it is something rustls exposes. |

### Runtime ownership decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| Three constructors | `Runtime::new` (ambient reactor), `Runtime::with_handle(handle, config)`, `Runtime::owned(config)` | `quinn` needs a Tokio reactor and nothing else in the crate does. Making the reactor something the runtime holds — borrowed, handed over, or created — removes "you must already be inside `#[tokio::main]`" from weida's contract, which language bindings need, because the host language owns the thread the call arrives on. |
| `worker_threads` | `RuntimeConfig::worker_threads`, default 1, `0` rejected | A messaging runtime is I/O bound, and every extra worker is a thread a library takes from its host process without being asked. `0` is a configuration error rather than something to correct silently ([GUARANTEES.md](GUARANTEES.md) §4). |
| Owned-runtime teardown | `shutdown_background()` from `Drop`, not a plain drop | The last `Runtime` clone may go out of scope on one of that runtime's own worker threads, where dropping a Tokio runtime panics inside a destructor — which aborts rather than unwinds. |
| One runtime surface | `Exec` in `weida-runtime` (`crates/runtime/src/exec.rs`): `spawn`, `sleep`, `within`, `enter`, `resolve` | One place to see what the code asks of tokio, and the thing that makes a non-tokio caller possible at all: the library takes its tasks, timers and name lookups from an `Exec`. |
| Client handshake placement | spawned **on** the runtime, awaited by the caller as a join handle | Completing a `quinn::Connecting` spawns the connection driver from inside the poll, so the polling thread would need an ambient reactor. Spawning it keeps `Runtime::connect` pollable from any executor and keeps the future `Send`, which holding an `EnterGuard` across the await would not. |
| `futures-io` beside `tokio::io` | both trait pairs on `OutgoingTransfer`/`IncomingTransfer`, `futures-io` delegating to the tokio impl | A caller on `futures`, `smol` or `async-std` should not have to wrap a compat shim around a payload stream. Delegating keeps the end-of-payload bookkeeping in one place. |
| `shutdown_timeout` | `RuntimeConfig::shutdown_timeout`, default 1 s | `shutdown` closes every endpoint and then waits for the sockets to go idle; **without a bound** the length of a process's exit would be decided by the path rather than by the caller: measured against a peer that has gone silent, the draining period alone is **96 ms** on loopback and grows with round-trip time and loss. One second is generous on any network where a clean close was possible at all. The bound is one budget for the whole shutdown, not one per endpoint, because what a caller waits for is the call ([decisions/0009](decisions/0009-drain.md) §4.4). The same bound caps `Binding::close` ([decisions/0038](decisions/0038-restart-handoff.md) §4.6), which would otherwise wait without a bound too, and `Binding::hand_over`'s wait for its held closes ([decisions/0039](decisions/0039-handover-says-when.md) §4.4). |
| `receive_timeout` | `RuntimeConfig::receive_timeout`, default 30 s; `None` waits as long as the peer likes | The body of one received transfer, from its header to its end, must arrive within this, where the blocking receives and `weida serve` collect it. Without a bound one peer that sends a header and then stalls, or sends a byte inside every idle window, parks a single-threaded consumer and starves every other sender it has: a byte or a PING keeps the connection alive, so `idle_timeout` never fires. Expiry drops that transfer, which stops its stream, and fails the receive with `Error::Expired`; the next transfer is unaffected. |
| Address selection | try every resolved address in order, capped by `RuntimeConfig::max_resolved_addresses` (8), each attempt but the last bounded by `RuntimeConfig::connect_attempt_timeout` (250 ms) | Taking the first address would make `weida://localhost:…` unreachable wherever `localhost` resolves to `::1` before `127.0.0.1`, which is the common Linux ordering. Sequential attempts with a per-attempt bound rather than RFC 8305's parallel happy-eyeballs: the failure to handle is an address that answers *nothing*, where QUIC has no refusal to observe, and 250 ms is RFC 8305's own Connection Attempt Delay for exactly that case. On the regression test the bound is the difference between **30 s and 1.3 s**. The last address keeps the full handshake budget, so an IP literal and a single-address name keep the whole budget of a plain dial. |
| Loss cause | `Error::ConnectionLost(LossCause)` — `IdleTimeout`, `PeerClosed`, `PeerRestarting`, `LocallyClosed`, `Reset`, `TransportError` | A bare `ConnectionLost` would hide why a peer is gone: `TimedOut`, `Reset` and an unrecognized application close would map to the same value, so the distinction would exist nowhere in the API. A payload rather than new top-level variants: the *outcome* is identical in all six cases — nothing in flight completed, `is_definite_failure()` stays true — and only the next action differs, so a second outcome vocabulary would be the wrong shape ([FAILURE_MODEL.md](FAILURE_MODEL.md) §4). Each cause changes what an application should do, and `PeerSet::pick` reports the cause of the peer it rejected. `PeerRestarting` is a peer's close with `RESTARTING` ([decisions/0038](decisions/0038-restart-handoff.md) §4.3): it too leaves the outcome alone and changes only the next action, which is a redial with no first delay, whatever `stop_on_peer_closed` says. |

---

### ZMTP codec decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| Dependencies | **none**, not even `weida-core` | A codec checked against a foreign specification must not reach for weida's types, limits or error vocabulary. `Cargo.toml` has an empty `[dependencies]` on purpose, and the crate carries its own error vocabulary. |
| Where the cap lives | an argument to every decode entry point, never a constant | ZMTP grants no credit, a long frame may declare 2^63-1 octets, and `ZMQ_MAXMSGSIZE` is unlimited by default, so the caller's local limit is the whole defence. The effective cap is `min(argument, 2^63-1)`, so a caller cannot switch the check off. |
| What a decoder returns | borrowed slices (`Command<'a>`, `Metadata<'a>`) | A decoded frame body is a slice of the caller's buffer and a command points into that body, so the only allocation on the way in is a `READY` property list. |
| Illegal flag combinations | unrepresentable | MORE “SHALL be zero on command frames”, so `FrameKind` is `Message { more }` or `Command` — the illegal combination has no value. |
| Incomplete versus violated | separate per layer | A frame header can legitimately be short; a command body arrives whole, so a field running past its end is a violation with nothing to wait for. |
| Where the specification contradicts itself | follow the ABNF and libzmq, and say so | Command names are length-prefixed, not null-separated; an `ERROR` reason may contain spaces although `VCHAR` excludes them. The crate docs and golden vectors record both choices. |


### Connection-tier decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| Pool granularity | one connection per `(authority, terms, expected fingerprint, **path**)` | A QUIC connection's receive window is shared, so the only way two flows cannot stall each other is for them not to share a connection ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2). Measured as behaviour, not asserted: `a_stalled_path_does_not_stall_another_path` fills one path's window until a write parks and then sends on another path. Reverting the key to one connection per peer makes that test time out, which is the coupling the decision set out to remove. |
| The control tier | **not built, and parked by decision** | In v0 a control connection would carry nothing: every connection performs its own HELLO (§2.2 of [PROTOCOL.md](PROTOCOL.md)), the credit frame of §6.6 rides the connection of the path it names rather than a control connection, and SUBSCRIBE could not move there because a publisher writes fan-out on the connection the SUBSCRIBE arrived on and no field names a peer's other per-path connection. That question is settled by [decisions/0011](decisions/0011-answered-where-it-arrived.md): a frame naming a path rides that path's connection, which takes SUBSCRIBE/UNSUBSCRIBE *and* the credit frame out of the tier's cargo and leaves it empty, so it is parked with a revival condition (0011 §4.3) rather than left blocked. The named residual coupling is a SUBSCRIBE behind payload on the *same* path (0011 §4.4). |
| `Limits` scope | a **per-connection** profile; runtime-level numbers live on `RuntimeConfig` | 0002 §6.4 wants one profile per tier. Two profiles are only honest if every field means something in both, so `max_connections`, `endpoint_queue` and `max_resolved_addresses` — per binding, per endpoint, per dial — live on `RuntimeConfig`, and `keep_alive`/`idle_timeout` live in `Limits`, because those *are* per connection. The second profile itself waits for the tier: a `Limits::CONTROL` nothing reads would be five numbers nobody has to justify. |
| Peer binding | compare against the peer's **live** connections, refuse a mismatch with `Error::Untrusted(fp)` | The fingerprint is the only thing that binds a peer's connections ([decisions/0008](decisions/0008-session-identity.md) §4.2), and dialling a second path is dialling the same peer. Live connections rather than a remembered value, because a peer is this peer only while a connection to it lives: once the last one is gone, a replacement server with a new key is a new peer and nothing should still be objecting to it. Proved by swapping a raw server's identity between two handshakes on one socket — the load-balancer case, which two `quinn` endpoints cannot reproduce because they cannot share a port. |
| `max_connections_per_peer` | 64, counted per proved fingerprint, released on close | One connection per path lets the dialling side choose the count, so `max_connections` alone would let one peer fill a binding. 64 is the number §4.8 measured (~50 MiB of transport state across both ends for 64 connections to one peer) and a sixteenth of the default `max_connections`. Anonymous connections are not counted together: two of them cannot be shown to be one peer, so counting them as one would refuse strangers for each other's traffic. |

### Connection statistics decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| `quinn` floor | `quinn = "0.11.12"` in the workspace, locked at `quinn` 0.11.12 / `quinn-proto` 0.11.19 | `PathStats::min_rtt` exists only from `quinn-proto` 0.11.18, and 0.11.12 is the first `quinn` whose manifest requires 0.11.18, so the floor is stated where a resolver reads it rather than left to whatever a lock file happens to hold ([decisions/0036](decisions/0036-connection-statistics.md) §2.1). |
| Where statistics are read | `connection_stats()` on `Peer` and every dialling endpoint and blocking twin, one record per **live slot** | A slot is what the application dialled and what outlives a connection, so its URL is the label and its redial count is the address's. A connection the pool shares between two endpoints is reported by both, with the same transport counters (0036 §4.6). The slot list is collected under its `std` mutex and `quinn`'s counters are read after it is released, so no `quinn` lock nests inside it. |
| Local transports | `transport: None`, `age` and `redials` still reported | No path to measure, and an empty vector has to keep meaning "no live connection" (0036 §3, option E). |
| `age` clock | `std::time::Instant` taken in `ConnCtx::spawn` | One instant per connection, after the QUIC handshake and before HELLO; a paused Tokio clock in a test does not stop it. |

---

## 6. Known debt and deferred work

Recorded deliberately, not discovered later. Each entry was checked against the tree.

- **OpenTelemetry SDK wiring deferred.** v0 propagates W3C `traceparent` and `tracestate` in
  transfer metadata and emits structured logs through `tracing`. No OTel exporter, no span
  export pipeline. That belongs to the observability work; the wire fields it needs are
  already carried, so enabling it does not change the protocol.
- **No retries.** A refused, lost or `Indeterminate` transfer is reported to the caller and
  never retried by the library ([GUARANTEES.md](GUARANTEES.md) §6). A body held in the redial
  outbox is sent once the connection is back, and a transfer that reached a connection is
  never resent ([0031](decisions/0031-transparent-redial-and-the-sender-outbox.md)). Retry
  policy is Phase 4 work.
- **No persistence.** No payload store, no WAL, no recovery. That is Phase 5 work;
  [STORE.md](STORE.md) specifies the interface.
- **Deduplication is opt-in and bounded, not absent.** A connection that negotiated
  `Deduplication::Bounded(window)` suppresses a repeat of `(producer, scope, sequence)` inside
  its window and remembers at most `max_dedup_entries` (4096) identities, and a `core`
  connection allocates nothing for it ([decisions/0001](decisions/0001-sequence-field.md)).
  What is absent is an **application-level idempotency id** — the key is the producer's
  sequence, which only a sender that opted into per-producer ordering writes — so an
  `Indeterminate` result may only be retried for idempotent operations, or by a sender that
  numbers its own transfers ([FAILURE_MODEL.md](FAILURE_MODEL.md) §5).
- **Single reply per exchange.** The reply half carries one DATA transfer or one ERROR and
  then FIN. Answering one request several times needs either several exchanges or a framing
  convention inside the payload; v0 offers neither.
- **No CI.** The gate is the command list in [LOOP.md](LOOP.md) §6, run by hand.
- **Fuzz runs need a nightly toolchain.** `cargo-fuzz` requires nightly. On a host without it
  the targets are still committed, and the deterministic `fuzz_smoke*` tests cover the same
  properties on stable, at lower depth.
- **`IncomingTransfer::read_capped` is the primitive.** The planned `collect(self, max_bytes)`
  cannot read a borrowed request body, so the borrowing form is the primitive and `collect`
  delegates to it.
- **A fan-out drop is invisible to a `core` subscriber.** A subscriber that negotiated
  `PerProducer(detect)` sees the gap (`IncomingMeta::gap`), and one that negotiated
  `PerProducer(reassemble)` holds instead, up to `max_reorder_hold`. By default a `core`
  subscriber past its byte budget at the publisher simply misses the message and only the
  publisher counts it (`Publisher::dropped`), because nothing on the wire reports a drop to a
  receiver that negotiated no ordering.
- **One subscriber per path per connection.** A `Subscriber` claims its path in the dialling
  connection's namespace, so two subscribers on one pooled connection asking for the same
  path collide with `AlreadyRegistered`. Fanning one subscription out to several in-process
  consumers is left to the application; silently multiplexing two subscribers onto one queue
  would be worse.
- **Publisher direction is fixed.** Pub and Pull bind; Sub and Push connect. The reverse
  directions wait for a use case that demands them.
- **A redial restores a transport, not a registration.** The runtime redials a lost address
  and re-sends a subscriber's own filters
  ([0031](decisions/0031-transparent-redial-and-the-sender-outbox.md)); nothing server-side
  survives the connection, and a stream that was in flight is the application's to reopen
  ([PATTERNS.md](PATTERNS.md) §1.8, §1.11).
- **Authorization hooks are not implemented.** Master doc §46's authorization surface does not
  exist. What ships is authentication plus the identity: applications decide on
  `IncomingMeta::peer`, and what is built in acts on a proved peer without a policy language:
  a binding's `Trust` of pins and anchors or `ClientTrust::AnyKey` in the handshake, a radio's
  `with_admission`/`evict` on joins, and `Binding::disconnect` on live connections
  ([0035](decisions/0035-keys-proved-not-judged.md)).
- **`stream_receive_window` is not a payload budget.** The DATA header spends the same window
  as the payload, and a receiver announces more window only per eighth of it, so a payload
  sized exactly to the window cannot be written until the application starts reading (§4.3).
  Correct QUIC behaviour that reads as a hang.
- **`weida::conn` depends on `listener::Route`.** The connection driver (`conn.rs`) dispatches
  on the listener's `Route` enum and `Namespace`, while `listener.rs` builds the connection
  context and calls back into `conn`: the two modules depend on each other, and moving the
  route table below both would remove the cycle.
- **The shared foundation carries weida vocabulary.** `weida-runtime` carries weida's
  resolver seam (`Resolver`, `SystemResolver`) and a default port that is weida's
  (`weida_core::DEFAULT_PORT`, 7443), although the foreign libraries share the crate. Every
  foreign library links `weida-core` for its full `Error`: `weida-zmq`, `weida-nng` and
  `weida-mqtt` convert the few variants that can reach them and map the rest to a generic
  error, and `weida-amqp` and `weida-nats` carry the whole enum as `Error::Runtime`.
- **`weida-zmq` and `weida-nng` duplicate their core.** Each carries its own `context.rs` (the
  socket context), `pipe.rs` (the per-pipe queue pair) and `inproc.rs` (the `inproc://`
  namespace over `weida_runtime::NameRegistry`), with the same structure in both.
- **`weida-amqp`, `weida-nats`, `weida-mqtt` and `weida-http` duplicate their TCP+TLS
  `Stream`.** Each defines `Stream` in `transport.rs`: an enum over a `TcpStream` and a boxed
  `tokio_rustls` client stream (`weida-http` adds the server stream), with the same `AsyncRead`
  and `AsyncWrite` impls.
- **The Python bindings still duplicate some plumbing.** The lease (`Slot`/`Lease`), the seconds
  validation (`weida_py_core::duration`), the Ctrl-C-aware blocking wait
  (`weida_py_core::interruptible`) and the cached `bytes` getter (`LazyBytes`) now live in
  `weida-py-core`, and `weida-nats-py`, `weida-amqp-py`, `weida-mqtt-py` and `weida-nng-py` use
  them. `weida-zmq-py` keeps its own `lease.rs` because it additionally retires a slot when a
  socket is split, and its own seconds helpers; `weida-py` keeps a seconds helper of its own
  (a weida runtime error rather than the bindings' `ValueError`/`EINVAL`/`Configuration`), so
  the helpers still disagree on the error class there. `weida-nats-py` and `weida-amqp-py` each
  drive their synchronous surface through a `drive` of their own, where the other four bind
  their library's `blocking` facade.
- **Decision 0016's refusal is not enforced.** [0016](decisions/0016-conflation.md) §4.5 says
  `Coalesce` backpressure and `PerProducer(reassemble)` ordering are refused together at
  configuration time. `GuaranteeSet::validate` (`crates/protocol/src/header.rs`) checks the
  durability, replica and deduplication combinations and not this pair, and
  `Backpressure::Coalesce` is used nowhere beyond its definition.
- **A pooled client connection outlives every endpoint that used it.** The pool
  (`crates/weida/src/pool.rs`) keeps a connection until it closes, and a dialling connection
  sends keep-alives, so a connection no endpoint holds any more is never closed while the
  runtime lives: `Peer::disconnect` and dropping the endpoint release the endpoint's slot and
  routes, not the connection. A connection whose key is dialled again is reused, which is the
  pool's purpose; one whose key never comes back — a `ClientTls` with a throwaway identity, as
  griasdi's `OpenDial` and swarm make per dial — stays open on both sides, keep-alive every
  10 s, against the server's `max_connections`. Measured (B-383): four pushers dropped after
  one send each, three with a fresh identity, left four connections exchanging 96 datagrams
  per 120 s of silence. When an unused connection should close is the open question; B-383
  lists the options. On the local transports nothing is pooled and nothing can reuse the
  connection, yet a dropped endpoint's control connection also stays open for the runtime's
  life and, on `AF_UNIX` and named pipes, keeps its place in the binding's
  `max_connections`. Closing it at the last handle (B-385, on a branch) awaits an
  explanation of one darwin failure before it lands.
