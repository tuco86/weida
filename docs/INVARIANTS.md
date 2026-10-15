# Invariants

This file is deliberately short so that an implementation agent can consult it repeatedly.

Every design change MUST be validated against this list. A change that violates an
invariant is rejected, not accommodated; if an invariant is genuinely wrong, the invariant
is amended here first, with the reasoning recorded, and only then is code changed.

## Invariant list

Reproduced verbatim from master doc §77, with one amendment marked where it stands: the fourth
item carries a second half from [decisions/0034](decisions/0034-late-is-lost.md) §4.11. The flow
is still one stream in every sense the invariant protects — lifetime, refusal, cancellation,
accounting — and the datagrams are the carrier QUIC provides for units a stream would deliver
late.

- Endpoint paths are opaque identifiers.
- All user payloads may remain streams end-to-end.
- Core transport does not require payload materialization.
- One data flow maps naturally to one transport stream where the transport supports it; a flow
  of units that are worthless once late maps to one registration stream and the transport's
  datagrams (amended by [0034](decisions/0034-late-is-lost.md) §4.11).
- Replies and ACKs are distinct concepts.
- Transfer-related control messages do not require a permanent control stream.
- All guarantees are defined against the immediate next hop.
- Brokerless and brokered APIs share the same messaging concepts.
- A broker cluster appears as one logical broker.
- Raft coordinates control state, not bulk payload transport.
- Payload replication remains stream-oriented.
- Disabled guarantees should not participate in the hot path.
- No remote input can cause unbounded memory allocation.
- A managed Connector may claim only what its concrete source, Queue and sink can prove.

## v0 mechanical checks

The invariants above span the whole project. The table below records, for the invariants
the code enforces, where the enforcement lives. Invariants not listed here are not
mechanically checkable because the subsystem they constrain does not exist.

| Invariant | Enforced by |
| --- | --- |
| Endpoint paths are opaque identifiers | `EndpointAddr` in `weida-core` validates bytes and length only; the endpoint namespace in `weida` is a flat map keyed by the exact path string — no splitting, no prefix match, no wildcards. Pub/Sub **topics** are a separate namespace from endpoint paths and are matched by a segmented pattern with a one-segment and a trailing rest wildcard; that matching is on topics only and never on paths ([PROTOCOL.md](PROTOCOL.md) §6.4, [decisions/0007](decisions/0007-topic-namespace.md) §4.1-§4.2). The invariant was **considered for amendment and deliberately kept**: hierarchy lives in the topic namespace so that endpoint dispatch keeps exactly one answer per (stream kind, path) [0007 §4.1] |
| All user payloads may remain streams end-to-end | DATA payload is opaque bytes until FIN, with no internal framing ([PROTOCOL.md](PROTOCOL.md) §4). A datagram payload is materialized by definition and bounded by `max_datagram_size` (about a kilobyte); every payload larger than one packet still may be, and under RADIO is, a stream end to end ([decisions/0034](decisions/0034-late-is-lost.md) §4.11) |
| One data flow maps naturally to one transport stream | one QUIC stream per data flow, and QUIC's three carriers are the only primitives: a one-way transfer is one unidirectional stream; a Req/Rep exchange is one bidirectional stream whose initiating half carries the request and whose reply half carries the reply or an ERROR; HELLO, SUBSCRIBE and UNSUBSCRIBE each get their own short stream ([PROTOCOL.md](PROTOCOL.md) §4); a datagram flow is one FLOW stream that lives as long as the flow, with its units in QUIC DATAGRAM frames ([PROTOCOL.md](PROTOCOL.md) §6.8, §6.9). On the in-process transport the same holds by construction and more literally: a stream *is* a channel pair, minted when the stream is opened and dead with it ([decisions/0010](decisions/0010-local-transport.md) §4.2); on every local transport a flow's datagrams ride its FLOW stream as length-prefixed records ([PROTOCOL.md](PROTOCOL.md) §2.1) |
| Replies and ACKs are distinct concepts | held by construction: the v0 core has no application acknowledgement to confuse a reply with. The only delivery signal is `Delivery`, a sender-side transport receipt backed by QUIC's fin-acknowledgement, which is never a frame on the wire and never arrives where a reply would. Accepted / Stored / Replicated / Processed are reserved for the L2 broker ([GUARANTEES.md](GUARANTEES.md) §6) |
| Transfer-related control messages do not require a permanent control stream | ERROR rides the reply half of the exchange it concerns and nothing else; SUBSCRIBE and UNSUBSCRIBE are short header-only unidirectional streams; cancellation is `RESET_STREAM`/`STOP_SENDING`, transport signalling rather than a message. There is no multiplexed control stream anywhere in the implementation. A separate **control connection** per peer ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2-§6.3, parked by [decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3, [PROTOCOL.md](PROTOCOL.md) §2.5) would not violate this: the invariant forbids a permanent multiplexed control *stream* inside a connection, where transfer frames would queue behind each other; a separate connection carries its own short streams and removes that coupling rather than creating it |
| All guarantees are defined against the immediate next hop | `Delivery::delivered()` is defined strictly as "the next hop's **transport** acknowledged every byte and the FIN", explicitly not "the application read it" — quinn's `stopped()` says "although not necessarily the processing of it" ([GUARANTEES.md](GUARANTEES.md), [FAILURE_MODEL.md](FAILURE_MODEL.md) §4). A broker's certificate speaks for its own hop only: `Accepted` is never presented as a claim about a consumer ([GUARANTEES.md](GUARANTEES.md) §2) |
| Disabled guarantees should not participate in the hot path | `finish()` is synchronous and the payload path is `write` to the quinn `SendStream` with no task hop and no lock. A fire-and-forget sender registers nothing per *write* and awaits nothing anywhere; what it pays, once per transfer, is dropping the `Delivery`: the receipt is pushed onto the parked set of its own connection under one uncontended lock, so that `Runtime::drain` has something to wait on ([decisions/0009](decisions/0009-drain.md) §4.2). Nothing is polled, allocated or spawned there, and the cost is not resolvable in the header bench against that bench's run-to-run spread ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.6). A connection that negotiated `Ordering = None` and `Deduplication = None` — every connection that declares nothing, since `core` is the default guarantee set — allocates no sequencer, detector, reassembler or dedup window: the unit tests of `crates/weida/src/ordering.rs` and `crates/weida/src/dedup.rs` drive a thousand calls through the disabled structures and assert that the backing tables' capacity is still zero ([GUARANTEES.md](GUARANTEES.md) §3, [PROTOCOL.md](PROTOCOL.md) §6.5) |
| No remote input can cause unbounded memory allocation | every remote-influenced allocation has a cap named in **Bounds** below and checked before the allocation: the stream header against `max_header_bytes` ([PROTOCOL.md](PROTOCOL.md) §3.1), CBOR nesting by an iterative skip, payload and stream state by the QUIC windows and stream budgets, and every table a peer can grow by its own named cap |

## Bounds

A bound is named here before the allocation it caps exists, so that no implementation can land
without one. The numbers are the defaults of `Limits` and `RuntimeConfig`
([PROTOCOL.md](PROTOCOL.md) §10); the paragraphs after the table add the bounds that belong to
one feature, and the reasoning the table cannot carry.

| What a peer could make grow | Cap | Where it is enforced |
| --- | --- | --- |
| a stream header | `max_header_bytes` (16 KiB) | the preamble reader compares `header_len` **before** allocating ([PROTOCOL.md](PROTOCOL.md) §3.1) |
| CBOR nesting | `max_depth = 8` | the iterative skip of `weida-protocol` ([PROTOCOL.md](PROTOCOL.md) §5) |
| AMQP array of zero-width elements (a decode, codec `Limits`) | `max_zero_width_elements` (16 per decode, 256 for message bodies) | the count is charged from the array header, before anything is reserved, against one allowance per decode that every array of it shares ([libraries/amqp.md](libraries/amqp.md) §8) |
| AMQP octets queued for a peer that stopped reading | `transport::OUTBOX_HIGH_WATER` (256 KiB) and `ConnectionOptions::write_timeout` (10 s) | frames are queued, never awaited: past the mark the driver stops reading the peer and stops taking local frames, and a stall clock that restarts whenever the socket takes anything fails the connection with `Error::WriteTimeout`; ending a connection gives what is queued `close_budget` to leave ([libraries/amqp.md](libraries/amqp.md) §8) |
| what an AMQP link tells an application that never reads | `link::MAX_QUEUED_REPORTS` (1024); deliveries by the credit the application granted | the connection driver waits for no application: a delivery is queued only against credit the receiver granted itself (a delivery past every limit it announced is refused with `amqp:link:transfer-limit-exceeded`), consecutive credit updates are merged and the oldest unread report is let go |
| an AMQP receiver's unsettled deliveries | `settlement::DEFAULT_MAX_UNSETTLED` (2048) | an unsettled delivery that finds the map full is refused with `amqp:resource-limit-exceeded` instead of being delivered unrecorded, and `grant_credit` refuses a grant the map could not hold |
| the AMQP peer's link handles mapped onto our links | `handle-max` advertised in `begin` (255) | an `attach` above it closes the connection with `amqp:connection:framing-error`; a second `attach` for a link already answered detaches it with `amqp:link:stolen` instead of mapping another handle |
| buffered payload and concurrent stream state | `stream_receive_window`, `connection_receive_window`, `max_concurrent_uni_streams`, `max_concurrent_bidi_streams` | QUIC transport configuration |
| header memory per connection, worst case | `max_header_bytes * min(header_slots, max_concurrent_uni_streams + max_concurrent_bidi_streams)` = **1 MiB** (16 KiB × 64) | `max_header_streams` (64, never below 1: `Limits::header_slots`) inbound streams may hold a frame-head buffer at once: a stream takes a slot for its preamble and another for its header, and a stream waiting for negotiation holds none. The header is **decoded inside its slot** and the raw bytes are dropped before the slot is given back, so a stream parked on a full queue, or alive as a FLOW or REPORT stream, keeps only the decoded header, whose text is capped by the protocol (at most 1664 bytes for a DATA header) and which is part of the stream's state, counted by the stream budgets above and not here. Each read runs under `header_timeout_ms` (10 s), counted from the moment its slot is taken; a stream that misses it is stopped with `STOP_SENDING(CANCELED)` and the connection survives. A header buffer grows with the bytes that arrive, in 2 KiB steps past 192 bytes ([PROTOCOL.md](PROTOCOL.md) §10) |
| accepted connections | `max_connections` per binding (1024), `max_handshakes` (256) in progress, `retry_above_handshakes` (64) | a QUIC Initial that arrives at `max_connections` live connections, or at `max_handshakes` handshakes in progress, is refused with `CONNECTION_REFUSED` before its handshake is accepted, so nothing is built for it. Above `retry_above_handshakes` in progress, an Initial from an address the peer has not validated gets a stateless Retry instead. Refusals are logged at most once a second. A local socket takes one of the `max_connections` places before it is accepted and keeps it until its descriptor is closed, which for a refused transfer is the end of its drain, so the surplus waits in the kernel's backlog; its preamble must arrive within `hello_timeout_ms` or it is closed. A closed connection gives back its share of `max_connections` and `max_connections_per_peer`, and a local one its place, **before** the queues it consumed on are told it is gone: that notice waits for as long as a queue is full and unread, as a task that holds the queues and an id and nothing of the connection, at most `max_connections` of them per binding, and a connection past that keeps what counts it until it has told ([PROTOCOL.md](PROTOCOL.md) §2.1, §10) |
| connections one peer may hold | `max_connections_per_peer` (64), counted by the fingerprint the peer proved and released when a connection closes | the accept loop. One connection per dialled path lets the dialling side choose the count, which is why the bound exists. Connections that proved no identity are each their own peer, because two anonymous connections cannot be shown to be one, and are bounded only by `max_connections` |
| subscriptions | `max_subscriptions` filters per connection, summed over paths, each at most 256 B | counted per held filter: a repeated SUBSCRIBE of a filter the connection holds is ignored, and an UNSUBSCRIBE of a filter it does not hold releases nothing. The connection is closed with `LIMIT_EXCEEDED` past the cap. The registry keeps them under the connection's own record, and they are dropped wholesale when it closes, visiting only the paths that connection holds; a path nobody holds a filter on is not in the registry at all, so a peer that names paths grows nothing that outlives it ([PROTOCOL.md](PROTOCOL.md) §6.4). Matching a message costs its topic's segments and its matches however many exact or `#`-ending filters are held; only filters with a `*` segment are tested one by one, which is what the per-connection cap bounds |
| payload queued for one subscriber | `subscriber_buffer_bytes`, **per connection**, over every path it subscribes to | the publisher drops and counts a copy that does not fit. A subscriber is a connection, so subscribing to many paths multiplies nothing, and withdrawing and subscribing again does not hand the connection a second budget beside what the first still has queued |
| gap-detector and reassembler scopes | `max_sequence_scopes` (1024) `(path, topic)` pairs per connection | on the receiver, at the cap the least recently used scope that holds no transfer is forgotten to make room for a new one. A scope holding a transfer is never forgotten, and a new scope is passed through untracked only when every tracked scope holds one. On the sender, the `Sequencer` never evicts: a new scope past the cap is sent unnumbered (`Sequencer::next` returns `None`, logged once), so the receiver passes it through with no gap tracking and no deduplication, while the scopes already tracked keep counting (`crates/weida/src/ordering.rs`) |
| reassembly hold | `max_reorder_hold` (256) transfers over all scopes | at the cap the oldest held transfer of the scope holding the most is released out of order with its gap reported, never by growing; a held transfer is an unread stream, so the bytes it pins are quinn's and are bounded again by `connection_receive_window` |
| deduplication identities | `max_dedup_entries` (4096) per connection, each keyed by its `(path, topic)` scope, and the negotiated window in time | the oldest entry is evicted at the cap, so a peer that sends fast buys itself missed suppression rather than memory |
| a resolver answer | `max_resolved_addresses` (8) | a dial tries the addresses in order, and every attempt but the last is bounded by `connect_attempt_timeout` |
| outstanding system name lookups | `MAX_OUTSTANDING_LOOKUPS` (64), process-wide, in `weida-runtime` | `getaddrinfo` blocks a thread and no deadline can end it, so a dial that timed out leaves its lookup running; a lookup beyond the cap is refused with `Error::LimitExceeded` rather than queued, and the place frees when the lookup returns, not when its caller gave up |
| live transfers on one local connection | `max_local_streams` (255) | an `open` at the cap waits for a live transfer to end rather than allocating past it, the same wait a QUIC `open` makes on the peer's stream budget ([decisions/0010](decisions/0010-local-transport.md) §4.2, [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.13), because on a local transport the stream *is* the OS object. A transfer or parked connection also holds one of the binding's `max_connections` places until its last half is gone and no drain or end-marker task still owns its descriptor (see accepted connections above). The named-pipe instance count needs nothing beyond it: the "1-255" of `CreateNamedPipe` is the range of its `nMaxInstances` argument and 255 is `PIPE_UNLIMITED_INSTANCES`, so the pipe is created unlimited and the per-peer stream slots are the only ceiling, as on `AF_UNIX` |
| parked reverse connections | `max_parked_reverse` (8) | the pool's own ceiling and `max_local_streams`, which counts a parked connection like any other live local connection: it is a descriptor held open for a copy that may never come ([decisions/0012](decisions/0012-local-connection-grouping.md) §4.4) |
| a pipe chunk | the caller's buffer per read | the chunk framing ([PROTOCOL.md](PROTOCOL.md) §2.1) allocates nothing for a chunk length. The buffers the transport owns are constants: a 4 KiB read-ahead per chunked reader (`READ_AHEAD`) and the 8 KiB scratch of a draining reader (`DRAIN_BUF`), which gives up on a peer that is silent for 30 s (`DRAIN_IDLE`) and so releases its descriptor; a payload that keeps flowing drains to its FIN. A drain, like an end marker the socket did not take at once, keeps the connection's `max_connections` place and `max_local_streams` slot until it has let go of the descriptor, so a peer that keeps refused connections open holds places and not descriptors beyond them |
| the publisher's per-topic drop table, per publisher path | `max_sequence_scopes` (1024), the same ceiling as the sequencer's per-topic table, which never evicts (see the row above) | at the cap a new topic's drops count in the path's total only; the topics are the local application's, not a peer's, but a table nobody bounds grows for the life of the process |
| QUIC datagrams | `datagram_receive_bytes` (0 by default) | a connection advertises no `max_datagram_frame_size` and buffers no datagram unless its profile enables flows |
| a report order | `MAX_REPORT_LEVELS` (16) | the decoder, before the level vector is reserved; a non-ascending or repeated level is refused rather than stored, so the order costs at most sixteen `u64` pairs however many a peer writes |
| a CURSOR stream's reader | `MAX_CURSOR_RECORD_LEN` (16 B) | one 64-byte read buffer plus at most one partial record, because a record is self-delimiting and applied as soon as it is whole; the set it fills is a **fixed** `[(u64, u64); 16]` that allocates nothing |
| a CURSOR stream naming a `report_id` this side never handed out | no state at all | the stream is reset with `CANCELED` before the first record is read ([PROTOCOL.md](PROTOCOL.md) §6.7), which keeps the report table bounded by the `Cursors` handles this side holds rather than by what a peer sends |
| the MQTT client's queue of deliveries for the application | `Limits::incoming_queue` (1024) **and** `Limits::incoming_queue_bytes` (8 MiB) | `weida-mqtt`'s event queue: a delivery is charged its packet's length plus a fixed overhead, the reader stops reading while either number is spent, and an empty queue still admits one delivery of any size. Only the *socket* waits: keep-alive, commands and `disconnect` run throughout, and the final `Disconnected` event is not subject to the queue (`docs/libraries/mqtt.md` §9.1) |
| the MQTT client's QoS 1 and 2 messages awaiting acknowledgement | `Limits::max_inflight` (1024) and `Limits::max_inflight_bytes` (8 MiB), for the stored ones **and** again for the ones still waiting for the quota | `Session::allocate`: the send quota is the smaller of the server's `Receive Maximum` — the peer's number, 65,535 when it declares none — and `max_inflight`; a publish past either stalls in the connection task, in order, until an acknowledgement frees room. The stalled queue holds the same two ceilings, drops the entry of a publish whose future the application dropped, and answers a live publish past them with `QuotaExhausted` |
| the MQTT client's Topic Alias tables | `Limits::max_alias_bytes` (256 KiB) per table, with a fixed per-entry overhead | `InboundAliases::resolve` refuses an alias that would pass it with 0x94; `OutboundAliases::publish` sends a topic that would pass it without an alias |
| the MQTT client's acknowledgements owed to a peer that is not reading | `MAX_UNSENT_ACKS` (4096) in `weida-mqtt`'s task | the reader stops reading until the socket has taken some, and a socket that takes nothing for `ConnectOptions::write_timeout` ends the connection with `Timeout("write")` |
| one MQTT enhanced-authentication exchange | `MAX_AUTH_ROUNDS` (8) and, for a re-authentication, `ConnectOptions::connect_timeout` | the handshake counts the AUTH packets before it answers one; the live connection counts them beside its `Reauthentication` and arms a deadline with it |
| the handoff a successor takes over | `MAX_HELD_DATAGRAMS` (4096) datagrams of at most `MAX_HELD_DATAGRAM_BYTES` (1500) each | `handoff::decode` refuses more, a larger datagram and a destination no client has (multicast, broadcast, unspecified, port 0, an IPv4-mapped IPv6 address judged as the IPv4 address it names, which is how a dual-stack socket sends it), before anything is sent; the predecessor's held socket keeps no more than the same. The bytes still need the integrity of the channel that carries them ([decisions/0039](decisions/0039-handover-says-when.md)) |

**Datagram flows and RADIO/DISH are inside this invariant with a cap each**
([decisions/0034](decisions/0034-late-is-lost.md)). `datagram_receive_bytes` (0 by default,
64 KiB when enabled) bounds what `quinn` holds unread per connection, oldest first;
`max_flows` (64) bounds the inbound flows one connection may hold, each a FLOW stream the peer
opened; `flow_queue_bytes` (16 KiB) bounds the unread datagrams of one flow, charged with a
per-entry overhead so a flood of empty datagrams is bounded by the same cap; `flow_early_bytes`
(4 KiB) and `flow_early_hold` (1 s) bound the one per-connection ring a peer can fill with
datagrams for ids it never registered; a local flow's record is refused above 1200 bytes, so a
reader's reassembly buffer is a constant. On the receiving side every connection keeps one
freshness table (`segments_in`), shared by its dishes and every other route, of at most
`max_sequence_scopes` `(path, topic)` entries, each a segment number and a 16-bit layer mask —
an untracked key is simply never stale — and a dish's queue is `endpoint_queue` deep and
discards rather than blocks; a dialling connection numbers `Peer::segment` in a table
(`segments_out`) of the same cap ([decisions/0037](decisions/0037-layered-segments.md) §4.11).
On the radio side the topic table is capped at `max_sequence_scopes` too and evicts only a topic
with no copy in flight; a copy has at most `MAX_SEGMENT_LAYERS` (16) layer streams, each with
at most 64 queued chunks, all charged against one budget: a dish's `subscriber_buffer_bytes`
exactly as a subscriber's are, or for `Peer::segment` the `Peer`'s, over all its segments.
`Radio::dish_drops` holds one record per joined dish connection, removed with it, so at most
`max_connections`. A datagram flow a dish refuses waits out a backoff, 100 ms doubling to 30 s
(`FLOW_BACKOFF_START`, `FLOW_BACKOFF_MAX`), before the radio opens another for its topic, so a dish
that refuses every flow costs a bounded number of stream opens. DATA key `14` and SUBSCRIBE key `3`
are refused above 15 at the decoder
([decisions/0037](decisions/0037-layered-segments.md) §4.3).

**The chain a peer presented is inside this invariant with a cap of its own**
([decisions/0035](decisions/0035-keys-proved-not-judged.md) §4.2). A connection keeps the
certificate chain behind a proved key for its whole life and shares it into every arrival, so
it is bounded before it is kept: at most 8 certificates and 32 KiB together
(`MAX_PEER_CHAIN_CERTS`, `MAX_PEER_CHAIN_BYTES` in `crates/weida/src/tls.rs`). A binding that
requires `ClientTrust::AnyKey` refuses a larger chain in the handshake; a `Trusted` binding and
the dialling side keep none of it and know the peer by its fingerprint alone. The per-peer table
of the accept loop holds connection handles rather than a count, so that `Binding::disconnect`
can close them, and is bounded by `max_connections`.

**The sender outbox and the event stream of
[0031](decisions/0031-transparent-redial-and-the-sender-outbox.md) are inside this invariant too, and
neither is remote input.** An outbox holds bodies the local application handed to `send`
while no peer was live, bounded in count by `RuntimeConfig::outbox_messages` (1000, ZeroMQ's
`ZMQ_SNDHWM`) and in bytes by `RuntimeConfig::outbox_bytes` (8 MiB — the bound ZeroMQ lacks),
with `outbox_full` deciding what a `send` does at either: block, drop and count, or fail. A
body larger than the byte bound is refused at the call rather than blocking forever. A peer
influences none of it: the numbers are the sender's, and what the peer's absence changes is
only *whether* the bound is reached. The `PeerEvent` stream is a broadcast channel of 64
events per endpoint; a reader that falls behind is told `Missed(n)` and the events are gone,
never queued. A slot's redial task holds the endpoint weakly and ends at the endpoint's
drop, so an application that connects and forgets holds one task per address and no more.

**An identity source of [0032](decisions/0032-identity-sources-and-the-handoff.md) holds one
identity and a 64-event stream, and reads nothing on a timer.** A `files` source checks two
modification times at most once per `FilesOptions::poll` (10 s), and only when the identity
is used — no task, no reactor, no inotify; the parse of a changed file happens then, once,
and a file that does not parse leaves the previous identity in service with `RenewalFailed`
on the stream. A rustls resolver caches one certified key per source generation: an unchanged
source costs a handshake a generation check, and a file source also its due check under a mutex.
A rebuild that fails leaves the previous key in service and is retried only when the generation
moves, so its failure is reported once per generation. The client verifier is cached the same way.
No TLS session ticket is issued and no server session is stored, so every handshake is a full one
that the verifier judges: a withdrawn trust applies to the next handshake, not to a resumed one.
What `weida-openbao` adds runs as tasks
that hold the client and a *weak* handle on the source, and end with it: one renewal per
`PkiSign`, one refresh per `PkiAnchor` and `Kv`, one token renewal per client; each sleeps
between rounds, none queues, and a renewal task returns the first time it wakes to find the
application has dropped the source. The client reads an answer under a named cap
(`MAX_RESPONSE_BYTES`, 16 MiB, refused by its declared length and again as it streams), keeps at
most 16 entries of 1 KiB of a server's error text, follows no redirect, speaks `http://` only
when `Config::allow_insecure` says so, and holds every token, secret id and wrapping token as
a `Secret` that is wiped on drop and redacted in `Debug`. A renewal that fails is retried
inside what is left of the token (a backoff of 5 s doubling to a minute, never past a quarter
of the remaining life), and a token with no TTL or one the server stopped honouring is not
renewed at all. The JSON itself is parsed into a `serde_json::Value`, which is the one place
this crate trusts a server it authenticated to with a token — the same trust the `bao` CLI
extends.

**The cursor stream is inside this invariant, and the shape that keeps it there is worth
naming.** A report is the first thing a peer sends that is *about* a transfer rather than part
of one, so the tempting implementation is a table keyed by whatever the peer says. It is not
one: the key is a `report_id` **this** side allocated, so a stream naming an id nobody handed
out is reset with `CANCELED` and allocates nothing, and the table is bounded by the
`Cursors` handles the application holds rather than by transfers it has ever sent — each entry
is released when its handle drops. The three remote-controlled sizes inside a report are all
capped at the decoder: sixteen levels per order, sixteen bytes per record, one fixed
`[(u64, u64); 16]` per transfer that allocates nothing. A bus member's writer queue is a
bound that is *local* rather than remote input — the member's own `endpoint_queue` depth of
messages per peer, past which a copy is dropped and counted — and it is named here
anyway, because an unbounded queue is an unbounded queue whoever fills it.

**Standalone foreign-protocol libraries are inside this invariant too**
([decisions/0013](decisions/0013-competitor-libraries.md)). Their bounds carry the foreign
protocol's own names, because users of those protocols must recognize them —
`ZMQ_SNDHWM`, `ZMQ_RCVHWM`, `ZMQ_MAXMSGSIZE`, `ZMQ_MAX_SOCKETS`, `ZMQ_BACKLOG`,
`ZMQ_HANDSHAKE_IVL`. Two defaults deliberately differ from libzmq's and say so in the parity
table: `ZMQ_MAXMSGSIZE` is finite because a ZMTP frame may declare 2^63-1 octets, and
`ZMQ_LINGER` is finite rather than infinite [0013 §4.4].

**`weida-nng` bounds what a connection can make it hold, and what a connection can make it
wait for, with named numbers** ([`docs/libraries/nng.md`](libraries/nng.md) §9 has the table).
What a peer can make it allocate: `NNG_OPT_RECVMAXSZ` (1 MiB, judged from the declared length
before a body is read; for the tag-stack protocols the stack rides on top, at most `4 ×
(MAXTTL + 1)` octets), a body buffer that grows only with the octets that arrived, per-pipe
queues of 128 messages and 8 MiB per direction (PUB and PUSH hold nothing a peer writes to them),
the socket's buffer for messages from pipes that have gone (one pipe's depth **and** octets),
and one survey's answers (the receive depth and 8 MiB). What it can make it hold: `max_pipes`
(1024), `max_pipes_per_peer` (256, counted by IPv4 address or IPv6 `/64`, by uid on `ipc`,
handshaking connections included) and `max_tls_handshakes` (64 per listener). What it can make
it wait for: `handshake_timeout` (10 s, for each TCP connect attempt and then for the TLS
handshake and the protocol header together, with the pipe slot reserved first), TCP keepalive
on every connection, and the opt-in `message_timeout` and `idle_timeout`. A reply to a pipe
that cannot take it is discarded, never waited for, so no peer can park a replier.
**An XPUB's memory of its subscribers is bounded by the tables' own product.** What its
application was handed for each peer — the record a departure is turned into unsubscribes
from — holds at most `max_subscriptions` prefixes of `max_subscription_bytes` for each peer,
with a count apiece: one unsubscribe is owed for every subscribe handed over that no
unsubscribe handed over has answered, repeats included, which is what an upstream XSUB that
counts each forwarded subscribe needs back. A departed peer is expanded into unsubscribes one
at a time, ahead of any new event, so the records of the departed and of the live together
never outnumber the peers that were live (`weida-zmq`'s `split::XPubEvents`).

**A message count is not a memory bound**, and that is the one place a standalone library
adds a name of its own rather than the foreign protocol's. `ZMQ_SNDHWM` and `ZMQ_RCVHWM`
count messages, so at libzmq's own defaults one peer could hold `1000 × ZMQ_MAXMSGSIZE` per
direction; `weida-zmq`'s `QueueConfig::max_bytes` (`DEFAULT_QUEUE_BYTES`, 8 MiB per
direction per peer) bounds the bytes instead, with the message count left at exactly what
`zmq_setsockopt(3)` documents. The bytes are **accounted, not only counted**: a queued
message is charged its payload plus `QUEUE_FRAME_OVERHEAD` (64 B) for every frame and
`QUEUE_MESSAGE_OVERHEAD` (64 B) for the message (`Queue::charge`), because a frame of no
bytes still costs the receiver a handle and an allocation — a message of a thousand empty
frames is two kilobytes to its sender and 64 KiB to the queue, so the ceiling admits 127 of
them instead of a thousand. A queue that is empty still accepts one message of any size,
because a bound that can refuse the only message in a queue is a deadlock and not a bound,
so the exposure is `max_bytes - 1` plus one message's charge, `max_message_size +
max_message_frames × 64 B + 64 B`: about 9.1 MiB at the defaults, with `ZMQ_MAXMSGSIZE` and
`max_message_frames` bounding the single-message case (`docs/libraries/zmq.md` §9).
`weida-nats` states the same rule for
Core NATS, where the protocol bounds neither side of it: `subscription_queue` counts messages
per subscription, so `ConnectionOptions::max_queued_bytes` (64 MiB per connection, the server's
own `max_pending`) charges every message a reader has not yet taken — a subscription's or a
pending request's — its octets plus a 4 KiB floor for the read-buffer chunk its slices keep
alive, and drops and counts what does not fit. The largest message accepted at all is
`max_inbound_payload` (8 MiB), the smaller of it and the server's `INFO.max_payload` being the
decode bound, because that number is announced by the peer and may lower the bound but must
not raise it (`docs/libraries/nats.md` §7).
`weida-mqtt` applies it to the application's delivery queue, where QoS 0 has no flow
control at all: `Limits::incoming_queue` (1024) is joined by
`Limits::incoming_queue_bytes` (8 MiB), each delivery being charged its packet's length plus
a fixed overhead and an empty queue still admitting one of any size. The deliveries are
copied out of the receive buffer, once, rather than sliced from it, so that a small queued
message cannot keep a large packet's buffer alive behind the budget's back
(`docs/libraries/mqtt.md` §9.1).

**`weida-zmq` bounds what a connection can make it hold and what it can make it wait for, too**
(`docs/libraries/zmq.md` §9 has the table). Before the handshake ends a frame is at most 64 KiB
(`HANDSHAKE_MAX_FRAME`) and a metadata dictionary at most 64 properties (`MAX_PROPERTIES`); a
message that has started must keep arriving (`MESSAGE_IVL` 30 s, extended by the octets that
arrive at `MIN_MESSAGE_RATE`); a connection stops taking messages from its pipe at
`OUT_HIGH_WATER` of unwritten output; one remote host holds at most `max_peers / 8` peers
(at least 8; a host is an IPv4 address or an IPv6 `/64`, an IPv4-mapped address counts as the
IPv4 one, and loopback in any form, `ipc` and `inproc` are exempt) and a handshake that stalls for
`HANDSHAKE_EVICT_AFTER` gives its slot to a newcomer; and an accept that fails backs off from
10 ms to 1 s instead of ending the listener. A received frame is a slice of its read buffer
only when it is at least a quarter of the allocation it was read into; any other is copied out,
so a few octets never pin a read buffer, and a connection that went quiet after one large
frame does not keep the allocation it came in.

**And a payload-byte budget is not a bound either, if a message may be empty.** The L2
broker's queue is the case: `weida_broker::BrokerConfig::queue_bytes` (8 MiB per queue)
bounds what a queue holds, but a producer of zero-byte messages costs zero payload bytes and
one live `QueuedMessage` each, so the byte budget alone would bound nothing at all. Each
message is therefore charged `PER_MESSAGE_OVERHEAD` (1 KiB) plus its body's **allocation**
plus the labels the queue retains, which makes `queue_bytes / PER_MESSAGE_OVERHEAD` the
ceiling on the message count as well. The overhead is that large because the charge of a
message is one number for its whole life and a message out for delivery is about a kilobyte
— its table entry, the task watching its report and the connection's cursor state — so a
queue of unreported deliveries stays near `queue_bytes` of resident memory. The allocation
rather than the payload length: the broker reads a payload into a buffer that grows by
doubling and trims it to the payload, so for a message that came through admission the two
differ by an allocator's rounding. **Producers are read concurrently, so what they are about to
buffer is promised first**: a reader reserves its message's overhead and labels, then its
payload in steps (a first step of at most 1 KiB, whatever `content_len` says), and a
reservation counts against `queue_bytes` exactly as a held message does, so `charged +
reserved` never passes the budget however many producers are mid-message. At most
`max_inflight_admissions` (1024) admissions run per queue, and the loop stops reading streams
while they all do; every wait on a peer — a payload, a delivery write, a confirm, a refusal, a
cursor — is under `io_timeout` (10 s) with a minimum rate (`MIN_PEER_RATE`, 16 KiB/s), so a
peer that stops or trickles holds one admission and its few bytes for one window, and no other
peer waits for it; and one **connection** holds at most `max_inflight_admissions_per_connection`
(256) of those permits — the connections of one proved key hold that many together, while every
anonymous connection counts for itself, told apart by the connection serial weida's incoming
streams carry (`IncomingTransfer::connection`) — a stream past that is refused at once, so what a
stalled connection can empty is its own share and not the queue's. One further bound belongs to
that config rather than to `Limits`, because
a queue outlives every connection that touches it: `max_queues`, checked before any path is
claimed. `max_unsettled` bounds, **per subscription**, the deliveries a queue has handed out
and not seen reported, and `max_unsettled_per_consumer` (1024) bounds them per **connection**,
over every filter it holds, because the filters are the peer's to multiply. Both are enforced
by who is *ready* — a subscription at either bound is not in the set the delivery scan walks —
which is the one place they can be; they could not be enforced against a *cumulative* credit
limit without capping a subscription's lifetime delivery count rather than its outstanding
one, which is why they are separate numbers rather than a clamp on credit. An unsettled
delivery **keeps its charge** against `queue_bytes`, so the bounds compose: a consumer that
stops reporting costs its own slots and the queue's budget, and nothing else; one that stops
*reading* holds one write for `io_timeout` and then has its connection closed with
`LIMIT_EXCEEDED` — its client redials and subscribes again — with everything it held back in the
queue. A delivery whose header the consumer's connection will not read is no stall: that message
goes back for other consumers, that connection is never offered it again, and after
`MAX_REFUSALS` (16) connections refused it the queue drops it and counts it
(`QueueStats::dropped`). A delivery's write and its report watcher live exactly as long as the
queue's entry for it, so a delivery the queue takes back leaves no task and no report entry on
the consumer's connection. The delivery scan costs what is new to each ready subscription —
every one remembers the newest message it was offered — and never the backlog nobody ready
wants. The payload is read under the queue's *remaining* budget, never under the
producer's advisory `content_len`, so an over-budget message is refused rather than buffered
and then rejected ([decisions/0018](decisions/0018-minimal-broker.md) §4.8).

**A grant that arrives before its subscription is state a peer can create, so it is capped
too.** A CREDIT frame and the SUBSCRIBE it belongs to ride two separate unidirectional
streams handled by two independent tasks, so the credit can arrive first; dropping it would leave
the consumer starved in silence, so the broker holds it until the subscription lands. The
peer chooses both halves of the key — its connection and the filter — and a connection that
only ever grants credit registers no consumer route, so nothing ever tells the queue that it
went away. The table is therefore a fixed 64 entries with the oldest evicted rather than a
map that grows until an `Unsubscribed` that may never come, and eviction rather than refusal
because refusing the newest entry would let one peer's stale grants deny the mechanism to
every other consumer on the queue (`crates/broker/src/consumers.rs`).

**The queue's refusal is the opposite choice from `weida-zmq`'s, and deliberately so.** A
ZeroMQ queue that refused the only message in an empty queue would deadlock a pattern that
has nowhere else to put it; a broker that accepted a message larger than the queue it is
held in would break the bound it exists to keep, and the producer *does* have somewhere to
put the refusal — it is an exchange, and `{REJECTED}` is an answer. Same invariant, two
shapes, because the escape route differs.

The drain of [decisions/0009](decisions/0009-drain.md) has **no number of its own**, which is
worth stating because "wait until things finish" is exactly the shape that usually grows a
queue. It holds something, though: a finished transfer whose `Delivery` the application
dropped has its receipt parked **on its own connection**, because that receipt is the only
handle to the acknowledgement. Per connection, and not per runtime, for the hot-path
invariant: parking must not touch a structure every connection and every worker thread
shares. The set is capped by that connection's stream budgets, `max_concurrent_uni_streams +
max_concurrent_bidi_streams` — what can be unacknowledged at once is what can be in flight
at once — and it is walked only when it is full, where settled receipts are reaped before
anything is thrown away, and once more at drain time. It is local either way: a peer cannot
grow it by sending, only this process can, by finishing transfers. A receipt evicted at the
cap is counted as outstanding by the next drain rather than assumed delivered.

A local peer is proved by the **kernel** rather than by a key, which is the one place the
identity invariant reads differently: `IncomingMeta::peer` carries a key or a local principal,
an in-process peer carries neither, and a PID is an observation that MUST NOT be authorized on
([ARCHITECTURE.md](ARCHITECTURE.md) §2, [decisions/0010](decisions/0010-local-transport.md)
§4.4). What does not change is that an identity is proved and never claimed. Both kinds
exist: `IncomingMeta::peer` is `Option<PeerIdentity>`, `None` in process and for an
anonymous TLS client, `Key` on QUIC, and `Local { uid, gid, pid }` on `AF_UNIX`, taken from
the kernel at connect time. The `sha256:…@` userinfo is refused on every local scheme so
that no local address can look authenticated [0010 §4.8], and what binds a local peer's
several connections together is a group token *plus* those same credentials, never the
token alone ([decisions/0012](decisions/0012-local-connection-grouping.md) §4.2).

**No statistics type carries an address**
([decisions/0036](decisions/0036-connection-statistics.md) §4.4). `PathStats`, `FlowStats`,
`UdpCounts`, `TransportStats`, `RemoteStats` and `ConnectionStats` describe a connection by the URL the
application dialled and by numbers, and by nothing else: no socket address, remote or local,
resolved or observed, so a statistics screen built on them cannot show an address the
application did not write. It is checked at compile time by
`stats::tests::statistics_types_carry_no_address` in `crates/weida/src/stats.rs`, which names
every field of every one of these types and bounds each by a marker trait that no address type
implements: a new field does not compile until it is named there, and an address-typed one not
until someone writes the implementation a review exists to refuse.

**The remote view is one fixed-size slot per connection**
([decisions/0036](decisions/0036-connection-statistics.md) §4.5, [PROTOCOL.md](PROTOCOL.md)
§6.10). A peer that agreed capability code `2` sends one REPORT stream, and the receiver keeps
only its latest record — twelve numbers and an arrival instant — replacing the one before, so
a peer's sending rate costs reads and never memory. The read buffer holds at most one partial
record and one read, because a record above 256 bytes is refused by its length prefix before
any of it is read, and a second REPORT stream on the connection is a violation rather than a
second slot.

Invariants deferred with their subsystems: brokerless/brokered API parity, broker cluster
as one logical broker, Raft scope, stream-oriented payload replication, and Connector
guarantee honesty. None of the v0 code may be shaped in a way that forecloses them. The
acknowledgement vocabulary they need — Accepted, Stored, Replicated, Processed — is
reserved for the L2 broker rather than approximated on the v0 wire, precisely so that a
broker hop can define it against real responsibility transfer instead of inheriting a
brokerless ACK that only ever meant "arrived in RAM"
([IMPLEMENTATION.md](IMPLEMENTATION.md) §1, [GUARANTEES.md](GUARANTEES.md)).

Standalone foreign-protocol libraries terminate no weida guarantee chain and have no global
socket-to-pattern mapping. Future broker Connectors are inside the honesty invariant as concrete
managed resources: each must name its source or sink, Queue, conversion policy, limits and
achieved guarantee level, and resource application must refuse a composition it cannot honour.
One Connector's explicit policy never becomes a framework-wide equivalence between protocols
([decisions/0013](decisions/0013-competitor-libraries.md) §4.6).
