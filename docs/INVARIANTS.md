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
| buffered payload and concurrent stream state | `stream_receive_window`, `connection_receive_window`, `max_concurrent_uni_streams`, `max_concurrent_bidi_streams` | QUIC transport configuration |
| header memory per connection, worst case | `max_header_bytes * (max_concurrent_uni_streams + max_concurrent_bidi_streams)` = **48 MiB** | the product of the caps above ([PROTOCOL.md](PROTOCOL.md) §10) |
| accepted connections | `max_connections` per binding | excess connections are closed immediately with `LIMIT_EXCEEDED` |
| connections one peer may hold | `max_connections_per_peer` (64), counted by the fingerprint the peer proved and released when a connection closes | the accept loop. One connection per dialled path lets the dialling side choose the count, which is why the bound exists. Connections that proved no identity are each their own peer, because two anonymous connections cannot be shown to be one, and are bounded only by `max_connections` |
| subscriptions | `max_subscriptions` filters per connection, each at most 256 B | the connection is closed with `LIMIT_EXCEEDED` past the cap. The registry keeps them under the connection's own record, and they are dropped wholesale when it closes, visiting only the paths that connection holds; a path nobody holds a filter on is not in the registry at all, so a peer that names paths grows nothing that outlives it ([PROTOCOL.md](PROTOCOL.md) §6.4). Matching a message costs its topic's segments and its matches however many exact or `#`-ending filters are held; only filters with a `*` segment are tested one by one, which is what the per-connection cap bounds |
| payload queued for one subscriber | `subscriber_buffer_bytes`, **per connection**, over every path it subscribes to | the publisher drops and counts a copy that does not fit. A subscriber is a connection, so subscribing to many paths multiplies nothing, and withdrawing and subscribing again does not hand the connection a second budget beside what the first still has queued |
| gap-detector and reassembler scopes | `max_sequence_scopes` (1024) per connection | a new scope at the cap is left untracked rather than inserted |
| reassembly hold | `max_reorder_hold` (256) transfers over all scopes | at the cap the oldest held transfer is released out of order with its gap reported, never by growing; a held transfer is an unread stream, so the bytes it pins are quinn's and are bounded again by `connection_receive_window` |
| deduplication identities | `max_dedup_entries` (4096) per connection, and the negotiated window in time | the oldest entry is evicted at the cap, so a peer that sends fast buys itself missed suppression rather than memory |
| a resolver answer | `max_resolved_addresses` (8) | a dial tries the addresses in order, and every attempt but the last is bounded by `connect_attempt_timeout` |
| live transfers on one local connection | `max_local_streams` (255) | an `open` at the cap waits for a live transfer to end rather than allocating past it, the same wait a QUIC `open` makes on the peer's stream budget ([decisions/0010](decisions/0010-local-transport.md) §4.2, [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.13), because on a local transport the stream *is* the OS object. The named-pipe instance count needs nothing beyond it: the "1-255" of `CreateNamedPipe` is the range of its `nMaxInstances` argument and 255 is `PIPE_UNLIMITED_INSTANCES`, so the pipe is created unlimited and the per-peer stream slots are the only ceiling, as on `AF_UNIX` |
| parked reverse connections | `max_parked_reverse` (8) | the pool's own ceiling and `max_local_streams`, which counts a parked connection like any other live local connection: it is a descriptor held open for a copy that may never come ([decisions/0012](decisions/0012-local-connection-grouping.md) §4.4) |
| a pipe chunk | the caller's buffer per read | the chunk framing ([PROTOCOL.md](PROTOCOL.md) §2.1) allocates nothing for a chunk length; the one buffer the transport owns, the 8 KiB scratch of a draining reader, is a constant |
| the publisher's per-topic drop table | `max_sequence_scopes` (1024), the same ceiling as the sequencer's per-topic table | at the cap a topic's drops count in the aggregate only; the topics are the local application's, not a peer's, but a table nobody bounds grows for the life of the process |
| QUIC datagrams | `datagram_receive_bytes` (0 by default) | a connection advertises no `max_datagram_frame_size` and buffers no datagram unless its profile enables flows |
| a report order | `MAX_REPORT_LEVELS` (16) | the decoder, before the level vector is reserved; a non-ascending or repeated level is refused rather than stored, so the order costs at most sixteen `u64` pairs however many a peer writes |
| a CURSOR stream's reader | `MAX_CURSOR_RECORD_LEN` (16 B) | one 64-byte read buffer plus at most one partial record, because a record is self-delimiting and applied as soon as it is whole; the set it fills is a **fixed** `[(u64, u64); 16]` that allocates nothing |
| a CURSOR stream naming a `report_id` this side never handed out | no state at all | the stream is reset with `CANCELED` before the first record is read ([PROTOCOL.md](PROTOCOL.md) §6.7), which keeps the report table bounded by the `Cursors` handles this side holds rather than by what a peer sends |
| the handoff a successor takes over | `MAX_HELD_DATAGRAMS` (4096) datagrams of at most `MAX_HELD_DATAGRAM_BYTES` (1500) each | `handoff::decode` refuses more, a larger datagram and a destination no client has (multicast, broadcast, unspecified, port 0), before anything is sent; the predecessor's held socket keeps no more than the same. The bytes still need the integrity of the channel that carries them ([decisions/0039](decisions/0039-handover-says-when.md)) |

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
on the stream. A rustls resolver caches one certified key per source generation, so an
unchanged source costs a handshake one atomic load. What `weida-openbao` adds runs as tasks
that hold the source and the client and end with them: one renewal per `PkiSign`, one
refresh per `PkiAnchor` and `Kv`, one token renewal per client; each sleeps between
rounds and none queues. A response from OpenBao is bounded by `reqwest`'s body read into a
`serde_json::Value`, which is the one place this crate trusts a server it authenticated to
with a token — the same trust the `bao` CLI extends.

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

**A message count is not a memory bound**, and that is the one place a standalone library
adds a name of its own rather than the foreign protocol's. `ZMQ_SNDHWM` and `ZMQ_RCVHWM`
count messages, so at libzmq's own defaults one peer could hold `1000 × ZMQ_MAXMSGSIZE` per
direction; `weida-zmq`'s `QueueConfig::max_bytes` (`DEFAULT_QUEUE_BYTES`, 8 MiB per
direction per peer) bounds the bytes instead, with the message count left at exactly what
`zmq_setsockopt(3)` documents. A queue that is empty still accepts one message of any size,
because a bound that can refuse the only message in a queue is a deadlock and not a bound,
so the exposure is `max_bytes - 1 + max_message_size` and `ZMQ_MAXMSGSIZE` is what bounds
the single-message case (`docs/libraries/zmq.md` §9).

**And a payload-byte budget is not a bound either, if a message may be empty.** The L2
broker's queue is the case: `weida_broker::BrokerConfig::queue_bytes` (8 MiB per queue)
bounds what a queue holds, but a producer of zero-byte messages costs zero payload bytes and
one live `QueuedMessage` each, so the byte budget alone would bound nothing at all. Each
message is therefore charged `PER_MESSAGE_OVERHEAD` (256 B) plus its body's **allocation**
plus the labels the queue retains, which makes `queue_bytes / PER_MESSAGE_OVERHEAD` the
ceiling on the message count as well. The allocation rather than the payload length, because
a body arrives through `IncomingTransfer::collect`, which grows a `Vec` from empty in 64 KiB
reads: a 2 MiB + 64 KiB message comes to rest in a 4 MiB allocation, so charging its length
would let a queue hold close to twice `queue_bytes` of resident payload, with the factor
chosen by the producer's message size. One further bound belongs to that config rather than
to `Limits`, because a queue outlives every connection that touches it: `max_queues`,
checked before any path is claimed. `max_unsettled` bounds, **per subscription**, the
deliveries a queue has handed out and not seen reported, and is enforced in
the delivery scan, which is the one place it can be: a subscription at its bound is not
eligible. It could not be enforced against a *cumulative* credit limit without capping a
subscription's lifetime delivery count rather than its outstanding one, which is why it is a
separate number rather than a clamp on credit. An unsettled delivery **keeps its charge**
against `queue_bytes`, so the two bounds compose: a consumer that stops reporting costs its
own slots and the queue's budget, and nothing else. The payload is read under the queue's
*remaining* budget,
never under the producer's advisory `content_len`, so an over-budget message is refused
rather than buffered and then rejected
([decisions/0018](decisions/0018-minimal-broker.md) §4.8).

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
