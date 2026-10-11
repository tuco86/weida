# Architecture

This document describes the shape of the system: the surface a user chooses from, the
terminology, the crate boundaries, the v0 runtime design, and how the ZeroMQ pattern family maps
onto it — including where it deliberately does not. It derives from master doc §0, §3, §4, §48,
§73 and §74.

Related: [PROTOCOL.md](PROTOCOL.md), [GUARANTEES.md](GUARANTEES.md),
[FAILURE_MODEL.md](FAILURE_MODEL.md), [INVARIANTS.md](INVARIANTS.md),
[IMPLEMENTATION.md](IMPLEMENTATION.md).

---

## 1. What a user chooses from

weida is **three carriers and three vocabularies over them**, plus adapters. A user picks the
vocabulary their problem has; there is no stack to climb, and no vocabulary is further from the
metal than another ([decisions/0024](decisions/0024-three-families-one-back-channel.md)).

| Vocabulary | What it gives you | What it is made of |
| --- | --- | --- |
| **stream** | `Peer`, `Acceptor`, one-way transfers, exchanges, datagram flows, `Delivery`, cancellation | the three carriers QUIC has — uni streams, bidi streams, datagrams — nothing invented |
| **message** | the ZeroMQ/nanomsg family: Req/Rep, Push/Pull, Pub/Sub, PAIR, SURVEY, BUS, and RADIO/DISH for lossy fan-out of segments | the same carriers, plus a selection policy and a name |
| **broker** | queues, confirms, subscriptions, redelivery | an ordinary weida process using the two above, plus a store |

The families **overlap on purpose**. Req/Rep is a stream pattern and a message pattern at once,
because a request is a stream and a completed request is a message — "**a message is a stream that
reached FIN**" ([PATTERNS.md](PATTERNS.md) §1.11). What the message vocabulary adds is not a layer
but an assumption: the payload is whole before it is used. What the broker vocabulary adds is a
hop that outlives the sender.

Across all three runs **one** extra mechanism, and it is the only significant addition to the
ZeroMQ base: a sender may state, with a message, which completion levels it wants reported —
`written`, `synced`, `processed`, or its own stages — as **cursors** over the bytes
([decisions/0023](decisions/0023-completion-is-a-cursor.md)). They travel on their own
unidirectional stream, never mixed into payload, so **no pattern's stream topology changes to gain
them**: a Push transfer stays one unidirectional stream and an exchange keeps its reply half for
the application's answer.
Without it an application that wants progress builds a back-propagation stream and a correlation
scheme by hand; with QUIC it is nearly free, and the broker helps itself to the same mechanism
rather than to a private one.

```text
                         APPLICATIONS
                              │
                    idiomatic language APIs
                              │
      ┌───────────────────────┼───────────────────────┐
      │                       │                       │
   stream                  message                 broker
   primitives              patterns                patterns
   Peer/Acceptor           Req/Rep, Push/Pull      queues, confirms,
   transfers, exchanges    Pub/Sub, PAIR,          subscriptions,
                           SURVEY, BUS             redelivery
      └───────────────────────┴───────────────────────┘
                              │
     three carriers + the cursor stream (uni, never mixed with payload)
                              │
              ┌───────────────┴────────────────────────┐
              │                                        │
          native QUIC                              adapters
                                                  Web
                                                  ZeroMQ
                                                  MQTT
                                                  AMQP 0-9-1
                                                  ...
```

**Streaming is the headline, and messaging is first-class.** The difference shows in a video
upload: the uploader is satisfied by the **first** hop's transport receipt, and that hop — a load
balancer — does not have to materialize the whole message to give it. If every hop keeps working
in streams, the payload may never be materialized anywhere, and each hop's guarantee is still
honest, because guarantees are hop-local
([GUARANTEES.md](GUARANTEES.md) §2, [decisions/0024](decisions/0024-three-families-one-back-channel.md) §4.5).
A message system forces a materialization boundary wherever it wants to acknowledge; this one does
not, because its acknowledgements are cursors over bytes rather than verdicts about objects.

**The dependency direction is a packaging fact, not a mental model**, and it is strict: `weida`
never depends on the broker, and a brokerless program links none of it (§4). The names **L0**
(stream core), **L1** (patterns) and **L2** (broker semantics) survive in this document where they
name exactly that boundary — which is what they are for.

### L0 — the stream core

The socket replacement: ZeroMQ's idea rebuilt directly on QUIC. Its primitives are exactly
the three carriers QUIC has, and nothing else:

- a **one-way transfer** — one unidirectional stream: bytes in one direction, ended by FIN,
  with a transport receipt and refusal by stop code;
- an **exchange** — one bidirectional stream: the **initiating half** carries a request, the
  **reply half** carries the reply or an ERROR;
- a **datagram flow** — one registration stream plus QUIC datagrams: the FLOW stream is the
  flow's lifetime and its refusal, and its units travel as datagrams that are never sent late
  on purpose and are dropped oldest-first at either end
  ([decisions/0034](decisions/0034-late-is-lost.md) §4.2).

Its guarantees are exactly QUIC's: in-order bytes within a stream, **no order across
streams**, flow control per stream and per connection, cancellation via RESET_STREAM and
STOP_SENDING, and a transport delivery receipt. L0 invents no delivery semantics on top of
that, which is the whole point of calling it a core.

The layer is public API, not an internal detail. `crates/weida/src/stream.rs` exports the
two entry points, and both layers share the transfer handles they hand out:

| Type | What it is |
| --- | --- |
| `Peer` | the dialling side: `open()` a one-way transfer, `open_bi()` an exchange; `connection_stats()` reads each live connection's path and traffic |
| `Acceptor` | the bound side: one path, both stream kinds, one queue |
| `Incoming` | what an `Acceptor` yields: `Stream(..)`, `Exchange(..)` or `Flow(..)` |
| `OutgoingTransfer` | the write half; `finish()` yields a `Delivery` |
| `Delivery` | the transport receipt — QUIC's own fin-acknowledgement |
| `IncomingTransfer` | the read half plus the metadata that described it |
| `IncomingRequest` | an accepted exchange: the body plus the reply half it owes |
| `ReplyStream` | the requester's half of an exchange; dropping it cancels the reply |
| `Flow` / `IncomingFlow` | a datagram flow's two ends: `send` never waits, `recv` yields the oldest datagram still held; `FlowStats` says what was lost and `PathStats` what the path looks like |
| `ConnectionStats` | one live connection of a dialling endpoint, labelled by the URL as dialled: `age`, `redials`, and `TransportStats { path, tx, rx }` on QUIC; no address in it ([decisions/0036](decisions/0036-connection-statistics.md)) |

### L1 — patterns

The ZeroMQ/nanomsg pattern family as thin wrappers over L0. Req/Rep, Push/Pull, Pub/Sub, PAIR,
SURVEY and BUS are all implemented;
DEALER/ROUTER are emergent rather than types of their own (§6a). A pattern contributes a
selection policy, a queue and vocabulary — never a delivery guarantee, because there is no
layer beneath L0 from which it could get one.

### L2 — broker semantics

The RabbitMQ-analog layer: managed queues, publisher confirms, consumer reports with
redelivery, storage and replication. The current `weida-broker` slice registers the queues
named by its process-local configuration, admits a producer's message into memory, reports
`Accepted`, and delivers under absolute per-subscription credit
([decisions/0018](decisions/0018-minimal-broker.md)).

The cluster design replaces that process-local inventory with a **declarative control
plane**. An administrative API commits long-lived resources to the control Raft; controllers
reconcile those specifications into running queues and protocol connectors. A committed
specification says what should exist, not that it is ready: each resource carries a generation
and an observed generation plus a small status. Create, update and delete are therefore
durable lifecycle operations rather than calls that happen to spawn or stop a task
([decisions/0022](decisions/0022-consensus-topology.md) §4.1).

The data plane still has no declare operation. Sending to a path never creates infrastructure;
an unknown or not-ready queue is refused. Node bootstrap configuration — identities, storage
paths, listeners and cluster seeds — remains local, while queue and connector specifications,
placement and ownership live in the control group.

### Foreign protocols and managed connectors

The foreign-protocol crates are standalone implementations, not weida adapters. A ZeroMQ user
uses `weida-zmq` to speak to ZeroMQ peers; the same is true for NNG, MQTT, AMQP and NATS. The
workspace defines no global equation between a foreign socket type and a weida pattern.

Where the broker later exposes a foreign protocol, it does so through a managed **Connector**
resource: an explicit source or sink attached to a named queue, with its protocol, direction,
address, limits and conversion policy in that resource's specification. A broker deployment may
link a protocol-specific Connector controller built on a standalone library; it exists only
under broker resource lifecycle and is not a free-standing bridge product or a dependency of
`weida`.

### Why the split exists

Earlier drafts had no L0/L2 line. They mixed RabbitMQ semantics into the socket layer — an
application acknowledgement frame, an acknowledgement mode on every transfer — and got less
than either system alone. Two removals fixed it, and both teach the same lesson.

1. **A brokerless application acknowledgement means "arrived in RAM".** QUIC already
   retransmits, and already reports that the peer's transport holds every byte. An
   acknowledgement frame layered on that borrowed RabbitMQ's vocabulary without RabbitMQ's
   substance: nobody had taken responsibility for the message, because there was nobody but
   the peer to take it. An acknowledgement earns a wire frame only when it transfers
   responsibility to a hop that outlives the sender — which is what L2 is for.
2. **The correlation machinery existed only because replies rode separate streams.** A reply
   on its own unidirectional stream must be matched back to its request, so the design
   carried per-transfer identifiers, a per-connection pending-reply table, a cancellation
   frame and a reply-arrival notification — machinery whose entire job was to undo a choice
   made one layer down. Req/Rep on one bidirectional stream deletes all of it: the stream
   *is* the correlation, and nothing on the wire names an exchange.

Guarantees belong to the layer that can underwrite them. QUIC underwrites bytes; a broker
underwrites responsibility; the socket layer between them should invent neither.

The master doc's rule survives the restructure:

> The framework provides one messaging model. Brokerless peer-to-peer operation and brokered
> operation use the same patterns and semantics. A broker adds discovery, shared state,
> persistence, load distribution, replication and consistency; it does not introduce a second
> programming model.

L2 is a layer, not a fork: it adds guarantees to the same patterns rather than a second way
of writing programs. The v0 implementation is L0 + L1 over native QUIC, and it MUST NOT
acquire a second messaging model when L2 is built.

---

## 2. Terminology

The term `Node` is deliberately not used; it is too overloaded.

### Runtime

A process-level execution/resource container. The asynchronous Rust implementation is
primary. The Runtime owns or coordinates asynchronous execution, timers, DNS, QUIC runtime
integration, connection pooling, buffer pools, transport/adapter registrations, shutdown,
common resource limits and observability infrastructure.

A process MAY create multiple Runtimes, especially in tests or deliberately isolated
applications.

### Listener

A Listener represents one logical externally reachable messaging namespace.

A Listener is **not** equivalent to one OS socket. A Listener may have several physical
bindings:

```text
Listener
 ├── QUIC IPv4 :7443
 ├── QUIC IPv6 :7443
 └── Web :443          (WebSocket / WebTransport / SSE)
```

All bindings belonging to the same Listener expose the same endpoint namespace:

```text
Listener
  bindings:
      quic://0.0.0.0:7443
      quic://[::]:7443
      web://0.0.0.0:443

  endpoints:
      /jobs
      /events
      /users
```

Different network interfaces required purely for IPv4/IPv6/network-segment reachability
belong to one Listener. If two externally reachable interfaces are meant to be
fundamentally isolated and expose different namespaces or security policies, use two
Listeners.

### Binding

A Binding represents one concrete reachable transport of **this** protocol. Native QUIC is the
reference transport and the only externally reachable one; the local transports of
[decisions/0010](decisions/0010-local-transport.md) — in-process, `AF_UNIX`, Windows named
pipes — are bindings too, reachable only on the machine and carrying the same frames without
TLS (§3, [PROTOCOL.md](PROTOCOL.md) §2.1). The Web adapter (master doc §38, §39) is the
framework's way into browsers and exposes the same namespace over WebSocket or WebTransport.
This semantic division between Listener and Binding is mandatory, and it is what lets one
Listener answer on a QUIC port and a UNIX socket at once.

A binding must be able to carry weida's addressing model: an opaque endpoint path inside a
namespace, several endpoints per binding. That is what makes QUIC and Web bindings of one
Listener — both can name `/jobs` and `/events` on the same port.

**Foreign-protocol libraries are not bindings.** ZeroMQ, nanomsg/NNG, MQTT, AMQP and NATS
have separate crates, implemented natively in Rust and hosted by `weida-runtime` or by an
extension of it. They are usable standalone and expose their own protocol's addressing and
semantics. A ZeroMQ socket, for example, is one ZeroMQ endpoint addressed by that protocol;
it is not a restricted weida `Binding`, and no implicit conversion makes it one.

The broker may later materialize a managed Connector backed by one of those libraries. That is
an explicitly configured broker resource attached to a queue, not a transport hidden behind
`Requester`, `Pusher`, `Subscriber` or any other weida pattern.

An **Identity** is a property of the Binding, not of the Listener. A QUIC binding needs a
certificate chain and the private key behind it; a Web binding terminates TLS on its own
terms; and two interfaces of the same service may present different identities — an internal
one issued by an internal CA, a public one facing outward. Putting server identity on the
Listener would make the namespace object depend on one transport's notion of identity. A
binding MAY additionally require an identity from every peer that dials it
(`ServerTls::require_client`, taking a `ClientTrust`: a trust the key must satisfy, or
`ClientTrust::AnyKey`, any key the client proves it holds), which is per binding for the same
reason: two interfaces of one service may differ in whom they let in.

### Identity and Trust

Two questions, two types (`crates/weida/src/config.rs`):

- **Identity** — *who am I*: a certificate chain plus the private key behind it. Its
  `fingerprint()` is the SHA-256 digest of the leaf certificate's DER
  `SubjectPublicKeyInfo`, written `sha256:<64 lowercase hex>`
  (`Fingerprint`, `crates/core/src/identity.rs`). The key is hashed rather than the
  certificate, so a pin survives a certificate renewal that reuses the key, and the value is
  the same one `curl --pinnedpubkey` and HPKP pin.
- **Trust** — *whom do I accept*: pinned fingerprints and certificate-authority anchors. A
  peer is accepted if its fingerprint is pinned, **or** if its chain reaches an anchor and
  the certificate names the host that was dialled. A pin consults no name; an anchor does.

An address may name the peer it expects (§3), and that fingerprint **overrides** the
endpoint's `Trust`: it is then the only identity accepted on that connection, whatever else
the endpoint would have trusted. `Trust::by_address()` is empty and accepts nothing but what
addresses name — dialling a plain address under it fails with `Error::Tls` before a packet
goes out. There is no platform root store and no verification bypass anywhere in the shipped
code.

The two combinations are `ClientTls { trust, identity }` — a dialling endpoint always needs
trust and may present an identity — and `ServerTls { identity, client_trust }` — a binding
always needs an identity and may require its clients to present a key, one a `Trust` accepts
or, under `ClientTrust::AnyKey`, any key they prove
([decisions/0035](decisions/0035-keys-proved-not-judged.md) §4.1). Both compare by content,
and the client connection pool keys on `ClientTls` together with the authority and the
fingerprint the address named (§5).

Identity is symmetric on the wire. Whatever a peer proved in the handshake is surfaced to the
receiving application as `IncomingMeta::peer` — `None` for a client that dialled
anonymously. It comes from the handshake and never from a header, so it cannot be claimed,
only proved (master doc §47). Beside it, `IncomingMeta::peer_chain` (and
`FlowInfo::peer_chain`) carries the certificate chain the peer presented, DER and leaf first,
bounded to 8 certificates and 32 KiB; weida parses only the leaf's public key, and what the
rest certifies is the application's to read ([0035](decisions/0035-keys-proved-not-judged.md)
§4.2).

**There are two kinds of proof, and a local peer uses the other one**
([decisions/0010](decisions/0010-local-transport.md) §4.4). A local transport runs no TLS, so
there is no key to present; the prover is the kernel instead, which is a stronger statement
than a certificate makes about a process on the same machine. `IncomingMeta::peer` therefore
carries either a **key** — the fingerprint above — or a **local principal**: `uid`/`gid`/`pid`
from `SO_PEERCRED` on Linux, effective `uid` and groups from `LOCAL_PEERCRED` on macOS, which
carries **no PID**, or on Windows a **`WindowsPrincipal`**, the account SID — the client's
token through `ImpersonateNamedPipeClient` on the accepting side, the pipe object's owner on
the dialling side — with the pid the pipe reports. An in-process peer is `None`, like an
anonymous client, because there is nobody else to prove. Three rules travel with it: the
credential is the one captured when the connection was made, not when a transfer was sent; a
PID is an observation and MUST NOT be the thing authorized on; and the rule this amends is
only the *count* — 0008 §4.1 said the fingerprint was the only identity, and what survives
unchanged is that an identity is proved and never claimed.

### Endpoint

An Endpoint is the actual typed messaging object: `Endpoint<Req>`, `Endpoint<Rep>`,
`Endpoint<Push>`, `Endpoint<Pull>`, `Endpoint<Pub>`, `Endpoint<Sub>`, and later the rest.

Rust uses strong types rather than one dynamically configured monomorphic socket object.
The user does not manipulate a generic `Socket` concept merely because ZeroMQ historically
did. Public pattern names are not overfitted before protocol semantics are finalized.

### Transfer

A Transfer is one user data flow. Each Transfer maps to exactly one transport stream
(master doc §7, §8): a protocol header followed by an opaque user byte stream terminated by
FIN. The fundamental API is `open transfer` / `write or read stream` / `finish`; small
message convenience APIs are built on top of it and never the reverse. A 40-byte transfer
and a 40-GB transfer use the same protocol semantics.

Two stream kinds means two shapes of transfer, and the vocabulary keeps them apart:

- a **one-way transfer** is one unidirectional stream — Push/Pull, and every copy of a
  Pub/Sub fan-out;
- an **exchange** is one bidirectional stream — one Req/Rep pair. Its **initiating half**
  carries the request, its **reply half** carries the reply or an ERROR.

The two halves of an exchange are independent streams as far as the transport is concerned,
so request bytes and reply bytes flow simultaneously: a responder may open its reply long
before the request reaches FIN. Correlation needs no identifier and no table, because an
exchange is one stream and a stream cannot be mistaken for another one.

### Delivery

`OutgoingTransfer::finish()` hands back a `Delivery`: the **transport receipt**, which is
QUIC's own fin-acknowledgement. Awaiting it means *the peer's transport holds every byte* —
explicitly not *the peer's application read them*. quinn documents the resolving case as the
peer acknowledging receipt of all stream data "although not necessarily the processing of
it", and that parenthesis is the entire distinction. Anything stronger is broker vocabulary
and belongs to L2 ([GUARANTEES.md](GUARANTEES.md),
[FAILURE_MODEL.md](FAILURE_MODEL.md)).

---

## 3. Addressing

The address syntax is a URL:

```text
weida://[sha256:<hex>@]host:port/path
```

The master doc writes this as `mq://`. `weida://` is the concrete scheme chosen for this
implementation; per master doc §82 the names in the master doc are working names. The scheme
exists as a single constant `SCHEME` in `crates/core/src/addr.rs`.

The optional fingerprint in the userinfo position names the peer expected to answer, so one
string carries both *where* to dial and *whom* to accept:
`weida://sha256:9f86…@10.0.0.8:7443/samples`. It parses into `EndpointAddr::peer`
(`Option<Fingerprint>`) and a malformed value is `Error::InvalidFingerprint`. When present it
overrides the dialling endpoint's `Trust` (§2), which is what makes a discovery record, a
config line or a pasted terminal line a complete address rather than half of one.

Path rules:

- The path is an **opaque endpoint identifier**.
- It MUST start with `/`.
- It MUST be 1..=512 bytes long.
- It MUST NOT contain any byte below `0x20`.

The core MUST NOT interpret the path beyond opaque lookup (master doc §81 rule 5):

- `/foo/bar` is **not** a hierarchy.
- There are no routing wildcards on endpoint paths.
- Path components carry no topic semantics.

Pattern-specific filtering — for example Pub/Sub subscription matching — happens after an
Endpoint has been reached and has nothing to do with endpoint routing.

Port is required. IPv6 literals use bracket form, e.g. `weida://[::1]:7443/x`.

### Local addresses

A local transport is named by its own scheme, because the transport is part of the address and
weida never falls back from one to another on its own
([decisions/0010](decisions/0010-local-transport.md) §4.6, §4.8):

```text
weida+inproc://<bus>/<path>            in-process, one bus name per process
weida+unix://<percent-encoded>/<path>  AF_UNIX SOCK_STREAM, Linux and macOS
weida+pipe://<name>/<path>             \\.\pipe\<name>, Windows, never a UNC path
```

The endpoint path keeps every rule above: opaque, leading `/`, 1..=512 bytes. What changes is
the authority, and each form has one validation rule that is not optional:

- **`weida+inproc`** — the bus name is at most 256 bytes, which is the budget libzmq uses for
  the same thing, and is unique within the process. Two processes may use the same name and
  will not meet.
- **`weida+unix`** — the socket path is **percent-encoded**, because it contains the same
  separator the endpoint path uses. After decoding it MUST fit the platform's budget: **107
  bytes** on Linux and **104** on macOS, both including the terminating NUL. The check happens
  before `bind` and before `connect`, on the *expanded* path, since a container or App Group
  prefix consumes most of the budget.
- **`weida+pipe`** — the name maps to `\\.\pipe\<name>` and MUST NOT be a UNC path naming
  another host; that is the address-level half of `PIPE_REJECT_REMOTE_CLIENTS`.

**The `sha256:<hex>@` form is rejected on all three.** There is no key to pin, because there is
no TLS handshake to prove one; who may connect is stated in the binding's configuration as
accepted local principals. An address that looks authenticated and is not would be worse than
one that plainly is not [0010 §4.8].

---

## 4. Crate map

```text
crates/
    core/                      →  weida-core        I/O-free model
    protocol/                  →  weida-protocol    wire codec, no I/O
    runtime/                   →  weida-runtime     reactor, resolver, OS hygiene
    winpipe/                   →  weida-winpipe     the Win32 calls a named pipe needs;
                                                    the one crate that may use `unsafe`,
                                                    empty off Windows
    weida/                     →  weida             runtime + QUIC and local transports
                                                    + stream core + patterns
    broker/                    →  weida-broker      L2: queues at endpoint paths
    raft/weida-raft/           →  weida-raft        openraft plus the I/O it lacks
    zmq/weida-zmtp/            →  weida-zmtp        ZMTP 3.1 codec, no I/O and no
                                                    weida dependency
    zmq/weida-zmq/             →  weida-zmq         the ZeroMQ implementation
    zmq/weida-zmq-py/          →  weida-zmq-py      its Python binding
    nng/weida-sp/              →  weida-sp          SP codec, no I/O and no weida
                                                    dependency
    nng/weida-nng/             →  weida-nng         the NNG implementation
    nng/weida-nng-py/          →  weida-nng-py      its Python binding
    mqtt/weida-mqtt-codec/     →  weida-mqtt-codec  MQTT 5 codec, no I/O
    mqtt/weida-mqtt/           →  weida-mqtt        the MQTT client
    mqtt/weida-mqtt-py/        →  weida-mqtt-py     its Python binding
    amqp/weida-amqp-codec/     →  weida-amqp-codec  AMQP 1.0 codec, no I/O
    amqp/weida-amqp/           →  weida-amqp        the AMQP 1.0 client
    amqp/weida-amqp-py/        →  weida-amqp-py     its Python binding
    nats/weida-nats-codec/     →  weida-nats-codec  NATS codec, no I/O
    nats/weida-nats/           →  weida-nats        the NATS client
    nats/weida-nats-py/        →  weida-nats-py     its Python binding
    py/weida-py-core/          →  weida-py-core     the shared PyO3 foundation
    py/weida-py/               →  weida-py          weida's own Python binding
```

This is the family layout [decisions/0013](decisions/0013-competitor-libraries.md) §4 chose:
each foreign protocol keeps its codec, standalone library and bindings together. `weida-zmq`
is complete against [libraries/zmq.md](libraries/zmq.md), and `weida-nng` exists beside it
([libraries/nng.md](libraries/nng.md)). Directories are named for the **protocol family**
rather than for a role in a weida deployment, so the directory list answers “does this
repository ship a ZeroMQ implementation?”

`weida-zmtp` depends on **nothing at all** — not even `weida-core`. That is deliberate and
stronger than the dependency rule below: code checked byte-for-byte against a foreign
specification must not reach for weida's types, limits or error vocabulary, or the check
quietly becomes a check against our reading of the protocol.

`weida-zmq` owns ZeroMQ's session, handshake, heartbeat, subscription counting and devices.
Its `proxy` and `proxy_steerable` helpers run the protocol's own queue, forwarder and streamer
topologies on the reactor. They do not name `weida`, and no second crate assigns global weida
counterparts to ZeroMQ socket types. The same boundary applies to every foreign library.

`weida-broker` is the L2 layer of §1. Its current slice is an in-memory queue at each
configured endpoint path; the cluster design moves queue existence and configuration into
control-Raft resources without adding a declare operation to the messaging data plane
([decisions/0018](decisions/0018-minimal-broker.md),
[decisions/0022](decisions/0022-consensus-topology.md)). It is a separate crate because it is
a separate layer: it depends on `weida`, nothing in the core may depend on it, and a
brokerless deployment does not link it.

Planned, not yet present: `weida-web` (the Web binding) and protocol-specific Connector
crates that let the broker reconcile foreign-protocol listeners or diallers as managed
resources. Each Connector names one concrete source or sink and one queue; it does not define
a universal mapping between two messaging models. The standalone foreign libraries remain
separately useful and never depend on `weida` or `weida-protocol`.

### `weida-core`

The I/O-free model. No tokio, no sockets, no quinn. Contains `Error` with the `ErrorCode`
and `StopReason` vocabularies, `EndpointAddr` and the `SCHEME` constant, `Limits`, and
`TraceContext` with its W3C traceparent codec. It is deliberately smaller than it was: the
send, receive and correlation state machines it used to hold described the acknowledgement
model that no longer exists, and the correlation table was replaced by nothing at all when
Req/Rep moved onto a bidirectional stream. Because the crate is I/O-free, the error
vocabulary and its stop-code mapping ([FAILURE_MODEL.md](FAILURE_MODEL.md) §4) are
unit-testable without a network.

### `weida-protocol`

The wire codec, also with no I/O: RFC 9000 varints, the stream preamble with its
cap-before-allocation check, the seven frame headers behind the eight frame kinds — SUBSCRIBE
and UNSUBSCRIBE share one — with hand-written CBOR encoders and decoders, the cursor record's
varint pair, the datagram flow prefix, the negotiation
function, and the QUIC application error code constants. Being I/O-free makes it directly
fuzzable: a fuzz target feeds it arbitrary bytes with no socket in the way.

### `weida-runtime`

The reactor, the resolver and the OS plumbing, with no protocol in it. Public surface, all
of it documented for a reader with no weida in the picture: `Exec` — `spawn`, `sleep`,
`within`, `enter` and the capped `resolve` — with `Exec::current`, `Exec::from_handle` and
`Exec::owned` as the three reactor-ownership constructors and `OwnedReactor` as the
background-shutdown discipline; `CloseBudget`, a finite budget the phases of one shutdown
share; `NameRegistry<T>`, an in-process namespace of bound names under a byte budget,
generic over what a bound name hands its acceptor; on unix `BoundUnixSocket`, which
carries the `AF_UNIX` bind hygiene of [0010](decisions/0010-local-transport.md) §4.5
(`sun_path` budget, socket-type check, unlink-then-bind, explicit `0600`, node removed on
drop), plus `peer_credentials`; and on Windows `BoundPipe`, which creates the instances of
a named pipe with an owner-only DACL, `PIPE_REJECT_REMOTE_CLIENTS` and the first-instance
flag, `connect_pipe`, which waits out `ERROR_PIPE_BUSY` on the runtime's timer, and
`client_principal` / `server_principal`. Dependencies: `weida-core` for one error
vocabulary, `tokio`, and on Windows `weida-winpipe`. Nothing else.

### `weida-winpipe`

The exception to `unsafe_code = "forbid"`, and the reason every other crate can keep it. A
named pipe cannot be made private, and its peer cannot be identified, without four things
`std` and `tokio` do not expose: a security descriptor on `CreateNamedPipe`,
`ImpersonateNamedPipeClient` followed by a read of the impersonation token's user SID, the
pipe object's owner SID, and the two process ids. This crate is those four things behind a
safe surface — `OwnerOnlyDacl`, `create_instance`, `open_client`, `client_peer`,
`server_peer` — and nothing else: no protocol, no address form, no peer type. Every `unsafe`
block wraps one Win32 call and states what it relies on; `unsafe_op_in_unsafe_fn` is
forbidden so none can hide inside an `unsafe fn`. The rule that unsafe code lives in a
dependency rather than in the workspace ([0013](decisions/0013-competitor-libraries.md)
§4.3's argument for `crypto_box` and `tokio-rustls`) is kept in spirit: no dependency
exposes these calls safely, so the smallest possible crate does, and it is a dependency of
`weida-runtime` only on Windows. Off Windows it is empty.

### `weida`

The runtime and the native QUIC transport: configuration, TLS setup, the `Runtime`, the
client connection pool, the per-connection driver, the Listener/Binding/namespace machinery,
the L0 stream core in module `stream`, the typed endpoints of L1, and the transfer handles
both layers share. The QUIC-specific code lives in module `transport`. The reactor it runs
on, the resolver it dials with, the bus registry it binds names in and the `AF_UNIX`
hygiene it binds sockets with are `weida-runtime`'s; `Exec` is re-exported `pub(crate)` in
`runtime.rs`, so every module keeps taking it from the `ConnCtx` or `RuntimeInner` it
already holds and `weida` has no second way to reach the reactor.

### `weida-broker`

L2, currently implemented as the smallest useful local slice: an in-memory queue per
configured endpoint path, bounded admission, `Accepted` on the producer hop, absolute consumer
credit, settlement and requeue. Its whole dependency list is `weida`; the layer below is the
public API, not its internals, and a brokerless deployment does not link it.

The cluster target is a managed resource system. A control Raft stores queue and Connector
specifications, generations, placement and ownership; one Raft group per replicated queue
stores that queue's commit records and consumer state. Controllers reconcile committed desired
state into runtime objects. The administrative API owns lifecycle; the messaging protocol has
no declare frame and never creates a queue as a side effect
([decisions/0022](decisions/0022-consensus-topology.md)).

### `weida-raft`

openraft plus the I/O openraft deliberately leaves out. The engine is the Raft mechanics, the
clock and the task driving; what it asks for as traits is a **transport** and a **store**, and
this crate fills the first with weida — one exchange per RPC on its own ALPN, TLS-proved node
identity, and a snapshot that travels as a stream rather than as chunked calls. A service that
wants replication then writes a state machine instead of a network layer
([decisions/0021](decisions/0021-consensus-openraft.md) §4.1).

**One connection carries many groups.** Consensus is one group for the cluster plus one per
replicated queue ([decisions/0022](decisions/0022-consensus-topology.md)), and because the
transport is QUIC, every group between two nodes shares one connection as independent streams:
adding a queue adds an election timer and a log, not a socket and not a congestion controller.
That is where this design is structurally cheaper than the precedent rather than merely different
— RabbitMQ's groups ride Erlang distribution, Kafka's replication one TCP connection per broker
pair.

It is the **one crate in this workspace whose public API exposes a foreign type on purpose**:
openraft is re-exported, because a user implements openraft's own traits and two versions of them
in one binary do not compose. The cost — openraft is pre-1.0 — is paid by naming one openraft
minor line per `weida-raft` release, the contract `tokio-rustls` has with `rustls`.

`weida-broker` depends on it behind the non-default `cluster` feature; nothing in the core depends
on it at all.

### Dependency direction

```text
core
 ↑        ↖
protocol   runtime
 ↑          ↑    ↖
weida ──────┘     weida-zmq ──→ weida-zmtp   (the codec, which depends on nothing)

weida-broker        →  weida + weida-raft     L2; cluster support is optional
weida-raft          →  weida + openraft       consensus with I/O
```

`weida-core` MUST NOT depend on `weida-protocol` or `weida`. `weida-protocol` MUST NOT
depend on `weida`. `weida-runtime` MUST NOT depend on `weida-protocol` or `weida`, and a
foreign-protocol library MUST NOT depend on `weida` or `weida-protocol`
([0013](decisions/0013-competitor-libraries.md) §4.2). Future Connector crates may name the
broker and one foreign library; neither side may depend back on them. Circular architectural
dependencies are prohibited (master doc §74).

### Why runtime and transport are one crate

Master doc §73 lists `runtime/` and `transport-quic/` as separate crates. They are merged
into `crates/weida` for this increment, on the authority of §73's own rule:

> Do not create dozens of tiny crates prematurely.
> Start with strong module boundaries and split crates where the dependency/ownership
> boundary is real.

The boundary was not real when that was written. Exactly one transport existed, so a
`runtime` crate would have had exactly one consumer and a `transport-quic` crate exactly one
dependent; the split would have bought no isolation and no independent versioning while
adding a public API surface between two halves of one design. The QUIC code is confined to
module `transport` inside `crates/weida`, so extracting it later is a mechanical move: the
module boundary that a crate split needs already exists and is already enforced by the
module system. The split becomes worthwhile when a second consumer of the runtime appears.

**The trigger has fired, and the cut is not the one §73 named.** The second consumer arrived
as the standalone competitor libraries of
[0013](decisions/0013-competitor-libraries.md), so `weida-runtime` **is** extracted: B-070,
`crates/runtime`, with `weida` as its first consumer. What it takes is the **reactor and
the OS**: `Exec` with `spawn`, `sleep`, `within`, `enter` and the capped resolver, the three
reactor-ownership constructors with their background-shutdown discipline, the `AF_UNIX` bind
hygiene and peer credentials of [0010](decisions/0010-local-transport.md) §4.5, a generic
named-endpoint registry and the bounded close budget. What it deliberately leaves behind is
QUIC: §73's `transport-quic` still has exactly one dependent, because a ZeroMQ
implementation has no use for it, so `Link`, the connection pool, the actor and every line
of [0012](decisions/0012-local-connection-grouping.md) stay here with the protocol they
serve. No public item of `weida` changes [0013 §4.2].

That last sentence is checked rather than asserted: the item list `cargo doc` generates for
`weida` — every page and every rendered signature — is identical before and after the
extraction, because `Exec` was `pub(crate)`, so publishing it in a new crate added surface
there and removed none here. The only observable difference for a user of `Runtime` is one
more node in `cargo tree`.

---

## 5. Runtime internals, v0

### Runtime ownership: one surface onto tokio

`quinn` needs a Tokio reactor. Nothing else in the crate does, so the reactor is something
the `Runtime` holds rather than something every caller must already be standing in. Three
constructors, differing only in where it comes from:

| Constructor | Reactor | Fails when |
| --- | --- | --- |
| `Runtime::new(config)` | the ambient one | there is none: `Error::Runtime` |
| `Runtime::with_handle(handle, config)` | the one `handle` names | never |
| `Runtime::owned(config)` | a multi-thread runtime it creates and owns, `config.worker_threads` workers (default 1) | `worker_threads == 0`, or the OS refuses the threads |

An owned runtime lives as long as the last `Runtime` clone and every endpoint made from it.
It is shut down in the background rather than dropped, because the last handle may go out of
scope on one of that runtime's own worker threads, where dropping a Tokio runtime panics.
All three constructors are `weida-runtime`'s — `Exec::current`, `Exec::from_handle` and
`Exec::owned` — and the background shutdown is its `OwnedReactor`, which `Runtime::owned`
holds for as long as the runtime that created it.

`Exec` is the **whole** surface onto the async runtime: `spawn`, `sleep`, `within`,
`resolve` (DNS) and `enter`. It lives in `crates/runtime/src/exec.rs` since B-070 and is
re-exported `pub(crate)` from `weida`'s `runtime.rs`. Nothing else calls `tokio::spawn`,
`tokio::time` or `lookup_host`, and the check is two greps — one per crate, because the
surface moved but the rule did not:

```text
grep -rn 'tokio::spawn\|tokio::time\|lookup_host' crates/weida/src   # no call outside #[cfg(test)]
grep -rn 'tokio::spawn\|tokio::time\|lookup_host' crates/runtime/src # only exec.rs calls them
```

The two accept loops, the connection actor, the HELLO deadline and the per-subscriber writer
all take their `Exec` from the `ConnCtx` they already hold. Three consequences worth
stating:

- **A caller's executor need not be Tokio.** `futures::executor::block_on` drives a full
  Req/Rep round trip against an owned runtime (`crates/weida/tests/foreign_executor.rs`),
  and both transfer handles implement the `futures-io` traits beside the `tokio::io` ones.
- **Two places enter the runtime context**, both synchronous and neither across an await:
  the `quinn::Endpoint` constructors, which register a socket with the reactor. A third
  place hands work to the runtime instead of entering it — the client handshake, because
  completing it spawns `quinn`'s connection driver from inside the poll.
- **The payload path is untouched.** `Exec` appears in connection setup, never between
  `write_all` and the socket: no task hop and no allocation was added to the hot path
  ([INVARIANTS.md](INVARIANTS.md)).

### The transport boundary

One place in the runtime knows which transport a connection is: `crates/weida/src/transport.rs`,
which defines exactly three types — `Link`, a connection that opens and accepts streams,
and `SendHalf`/`RecvHalf`, the two halves of one stream. Everything above it — the frame
readers, the patterns, the guarantees, the drain — is written once and runs over any
transport, which is what makes `docs/PROTOCOL.md` §2.1's "same frames, same HELLO, same
negotiation" a fact about the code rather than an intention.

It is an **enum, not a trait object**. The set of transports is closed, small and decided
in this crate, and the payload path must stay a direct call: dispatching `write_all`
through a vtable would put an indirection on exactly the path that is otherwise free of
task hops and locks. `AF_UNIX` and named pipes are one variant each
([decisions/0010](decisions/0010-local-transport.md) §4.1) and no new concept.

The first non-QUIC variant is the in-process transport, `crates/weida/src/inproc.rs`: a
process-global registry of bus names — `weida-runtime`'s `NameRegistry<LocalConn>` under
`weida-core`'s 256-byte budget — and a connection that mints a channel pair per
stream. That pair *is* the stream, which is §4.2's "the OS connection is the stream" with
the only object an in-process transport has, and it is why `max_local_streams` bounds live
transfers there. A local connection carries no TLS, so there is no key and no identity:
`IncomingMeta::peer` is `None` [0010 §4.4].

The two kernel-mediated variants share one implementation. `crates/weida/src/grouped.rs`
is the grouping of [0012](decisions/0012-local-connection-grouping.md) — the preamble, the
token registry, the admission rule, the reverse pool, the stream slots — written once over
a `Stream` trait with exactly the eight things the two transports differ in: how a
connection is dialled, how the kernel names its peer, how it splits into halves, when two
principals are the same peer, what identity a principal is, and how a stream ends —
`finish`, `reset`, `stop`. `unix.rs` implements it for `tokio::net::UnixStream` in forty
lines, because a socket ends a stream by half-close and carries no code. `pipe.rs` is
longer, because a pipe has no half-close: it carries each stream in chunks — payload, FIN,
RESET with a code — over `tokio::io::split` halves ([PROTOCOL.md](PROTOCOL.md) §2.1), and
a reader that stops drains the rest in the background rather than closing a handle the
reply still needs. The chunk layer is the one thing on the pipe's wire that is not on the
socket's, and it is the transport's, not the protocol's: above `transport.rs` nothing knows
it exists. The `Link`, `SendHalf` and `RecvHalf` enums carry `Unix` and `Pipe` variants
over the same generic types, gated on the platform.

### Connection driver

There is **one connection actor task per connection** — `conn::driver`, holding a
`ConnCtx` — running identical code on both
sides. Both carry an `Arc<Namespace>`: on a server it is the Listener's, on a client it
starts empty and a `Subscriber` registers its path there, so fanned-out copies arriving on a
dialled connection have somewhere to land.

It owns no tables. The correlation and acknowledgement maps it used to hold went away with
the model that needed them: an exchange is one stream, so there is nothing per connection to
index, and there are no acknowledgements to await. What is left is a serialization point for
the only two frames a **destructor** may still need to emit:

```text
Ctl::SendUnsubscribe { path, filter }   // a dropped Subscriber withdraws its filters
Ctl::ReplyError      { send, code }     // a dropped IncomingRequest reports NO_REPLY
```

Both exist for one narrow reason: `Drop` may run on a thread with no Tokio reactor, so it
can neither await nor safely spawn. Handles hand the work over a bounded `mpsc` (depth 1024)
with a non-blocking `notify` and never wait for an answer — no oneshot reply channel remains
anywhere in the actor protocol, so no API call is ever serialized behind the actor.

### Payload bytes never traverse the actor

This is a hard rule. Handles own their quinn `SendStream` and `RecvStream` directly, so the
hot path from `write_all` to the wire has **no task hop and no lock**. Routing payload
through the actor would reintroduce exactly the per-transfer heavyweight actor that
master doc §49 forbids. With the tables gone the rule is nearly free: on a healthy
connection the actor sees no messages at all.

### Nothing to register

`open()` picks a peer, writes the DATA header and returns. An earlier design reserved an
identifier and awaited the actor's confirmation *before* touching the network, to close the
race where an acknowledgement could arrive before its bookkeeping entry existed. No entry,
no race, and no round trip on the critical path: `push_1kib_best_effort` is 72 % faster than
the registering version it replaced ([IMPLEMENTATION.md](IMPLEMENTATION.md)).

### Accept dispatch

Per connection there are **two** accept loops, one per stream kind, each spawning one task
per inbound stream. Concurrency is bounded by the QUIC `max_concurrent_uni_streams` and
`max_concurrent_bidi_streams` limits rather than by an unbounded spawn: the peer cannot
create more parse tasks than the transport lets it open streams. Each task reads the
preamble and header — with the length cap checked before allocation — and then dispatches.

Unidirectional streams:

- **HELLO** → negotiate, publish the result to a `watch<Option<Agreed>>`.
- **SUBSCRIBE / UNSUBSCRIBE** → apply to the connection's subscription registry.
- **DATA** → `endpoint` is required here, and its absence closes the connection with
  `PROTOCOL_VIOLATION`. A puller, subscriber or `Acceptor` path queues the transfer; a
  replier or publisher path gets `STOP_SENDING(UNSUPPORTED)`; an unknown path gets
  `STOP_SENDING(UNKNOWN_ENDPOINT)`.
- **ERROR** → a violation. An ERROR is the alternative to a reply, and a unidirectional
  stream has no reply to be an alternative to.

Bidirectional streams, one exchange each:

- Only **DATA** may open one; any other kind closes the connection.
- `endpoint` is required on the initiating half, same rule and same consequence.
- A replier or `Acceptor` path queues the exchange.
- A puller or publisher path is refused *on the reply half*, with a real `ERROR{UNSUPPORTED}`
  frame plus `STOP_SENDING(UNSUPPORTED)` on the initiating half; the connection survives.
- An unknown path is refused the same way, with `ERROR{UNKNOWN_ENDPOINT}`.

Misroute reporting is asymmetric because the stream kinds are: an exchange has a reply half
to carry a typed ERROR, a one-way transfer has only the stop code — which says the same
thing, since [PROTOCOL.md](PROTOCOL.md) §7 gives every refusal a code. Nothing ever answers
on a stream of its own.

Both loops park non-HELLO streams on the negotiated-`watch` until the peer's HELLO has been
processed. This is the parking rule of [PROTOCOL.md](PROTOCOL.md) §2.2, and it is why an
early DATA stream is never a protocol violation.

### Client connection pool

The `Runtime` holds one lazily-bound client `quinn::Endpoint` (`[::]:0`, falling back to
`0.0.0.0:0` where IPv6 is unavailable) and a map keyed by
`((host, port, ClientTls, Option<Fingerprint> from the address), path)`
(`crates/weida/src/pool.rs`). Every part of that key is load-bearing: two endpoints dialling
one authority under different trust, under different client identities, or expecting different
peers must never share a connection, or one would be using a peer authenticated on the other's
terms — and the **path** is part of the key because one connection per dialled endpoint path is
what keeps two paths from stalling each other
([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2). A QUIC connection's
receive window is shared by everything on it, so the only way two flows cannot stall each other
is for them not to share a connection: `a_stalled_path_does_not_stall_another_path`
(`crates/weida/tests/streams.rs`) fills one path's connection window until a write parks and
then sends on another path, which arrives.

`connect()` resolves the host, dials, uses the host string as written for the TLS server name —
which only an anchor check consults, since a pin ignores names — and waits for the HELLO
exchange before returning, so a returned connection is always negotiated. A closed entry is
evicted when the same key is dialled again; a handshake the verifier refused becomes
`Error::Untrusted(fp)`, carrying the fingerprint that actually answered so an operator can
decide whether to pin it. The dialled peers themselves live in the `PeerShared` behind a `Peer`
(module `stream`): each `connect()` adds a slot for the URL as dialled, live, down while a redial
works on it, or gone; `pick()` round-robins across the live ones and waits for a redial when
none is, `peer_count()` counts only slots whose connection is still open, and `disconnect(url)`
removes a slot and ends its redial task, so the set holds one slot per dialled address and
cannot grow with uptime.

**What binds a peer's connections.** The **proved fingerprint, and nothing else**
([decisions/0008](decisions/0008-session-identity.md) §4.2): no field names a peer's other
connections, so before a freshly dialled connection is pooled the pool compares the identity it
proved against the identity this peer's *live* connections proved and refuses a mismatch with
`Error::Untrusted(fp)`. That is the load-balancer case — two dials to one authority reaching two
different servers — and it is checked against live connections rather than a remembered value,
because a peer is this peer only while a connection to it lives: once the last one is gone, a
replacement server with a new key is a new peer and nothing should still be objecting to it.
Two connections that proved no key at all — anonymous clients — are never treated as one peer.
`Option<Fingerprint> from the address` remains a *dialling expectation*; the identity is what
the handshake proved.

Server side, `max_connections_per_peer` (default 64) bounds what one peer may hold on one
binding, counted by the fingerprint it proved and released when a connection closes. It exists
because one connection per path lets the dialling side choose the number: 64 connections to one
peer measured ~50 MiB of transport state across both ends, against a ~1.1 ms handshake each
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-011). Anonymous connections are not counted
together, because two of them cannot be shown to be one peer; a binding that wants the bound
requires a client identity.

**The control tier is parked** ([decisions/0011](decisions/0011-answered-where-it-arrived.md)
§4.3, [PROTOCOL.md](PROTOCOL.md) §2.5). The rule that settles it: a side writes traffic it
originates on the connection the peer's registration arrived on, so a frame naming a path rides
that path's connection [0011 §4.1-§4.2]. That is every frame weida has or reserves except
HELLO, which each connection performs for itself — so a per-peer connection would pay a
handshake and a timer pair to carry nothing. `Limits` is a per-connection profile, ready for a
second profile, and the second profile arrives with the tier rather than before it.

### TLS

Server: the binding's `Identity` supplies the certificate chain and key — from files or from
PEM already in memory, because a key held in a secret store must not have to be written to
disk first — `alpn_protocols` is set to exactly `[b"weida/0"]`, and the transport
configuration applies the stream and connection windows, both stream-count limits and the
idle timeout from `Limits`. The bidirectional limit was hardcoded to `0` while Req/Rep rode
unidirectional streams — the transport refused bidirectional streams outright — and is now
`max_concurrent_bidi_streams`, which is what bounds the exchanges a peer may hold open on us.
A binding whose `client_trust` is set requires client authentication: an anonymous client
fails the handshake and sees `Error::Tls`, and so does a client whose identity a
`ClientTrust::Trusted` trust does not accept. `ClientTrust::AnyKey` refuses no key: it checks
that the leaf parses and the chain fits the bound, and verifies the handshake signature
([decisions/0035](decisions/0035-keys-proved-not-judged.md) §4.1). Requiring an empty `Trust`
is rejected at bind time, because it would accept nobody.

Client: same ALPN, plus the 10 s keep-alive. Keep-alives are sent by the dialling side only,
so an idle connection is held open by the client alone.

One verification policy serves both directions (`Policy` in `crates/weida/src/tls.rs`): the
leaf's fingerprint is computed, a fingerprint the address named decides alone, a pin accepts
outright, and anything else must chain to an anchor under the usual webpki rules. Whatever
the trust path, the handshake signature is verified with the crypto provider's algorithms, so
a peer is only ever accepted for a key it proved it holds. The provider is named explicitly
rather than taken from the rustls process default: a library must not install global state in
its host application.

**Authentication is not authorization**, and since
[decisions/0015](decisions/0015-peer-authorization.md) that is a decision rather than a
description of what happens to be missing. A completed handshake says which key answered and
nothing about what that peer may do. Deciding that is the acceptor's job, on
`IncomingMeta::peer`: the identity is on every inbound transfer and request, so a handler can
refuse per endpoint, per topic or per payload. Built into weida are the admission a binding
applies in the handshake — a `Trust` of pins and anchors, or `ClientTrust::AnyKey`, which admits
any proved key and leaves every decision to the application
([decisions/0035](decisions/0035-keys-proved-not-judged.md)) — plus two local decision points
that act on a proved peer after it is connected: a radio's admission and eviction of joins
(`Radio::with_admission`, `Radio::evict`) and `Binding::disconnect(fingerprint)`, which closes
a peer's connections and is not a ban. The authorization hooks of master doc §46 are not
implemented **by decision** — 0015 asked whether the handshake should carry an application
credential and answered no, so there is nothing for a hook to carry that `(proved peer,
dispatched path)` does not already say.

Per endpoint path, an acceptor MAY decide whether the path exists at all (a non-registration
answers `UNKNOWN_ENDPOINT`, which deliberately does not distinguish "no such endpoint" from
"not for you"), whether to accept an individual transfer or exchange that dispatched to it
(`REJECTED`), whether a given subscriber gets a given topic, and whether the peer may connect
at all (`Trust`). It MUST NOT authorize on anything claimed rather than proved, read
structure into an opaque path, expect a one-way refusal to be observed
([decisions/0005](decisions/0005-refusal-race.md)), treat two anonymous connections as one
peer, or take a decision on one connection and signal it on another
([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.1).

---

## 6. One Req/Rep exchange

Three streams are involved: two HELLOs and the exchange. The HELLOs are unidirectional; the
exchange is one bidirectional stream. There is no permanent control stream anywhere.

```text
  Requester                                                     Responder
      │                                                             │
      │  QUIC handshake, ALPN "weida/0"                             │
      │◄═══════════════════════════════════════════════════════════►│
      │                                                             │
      │  uni stream:  HELLO, FIN                                    │
      │────────────────────────────────────────────────────────────►│
      │                                    uni stream:  HELLO, FIN  │
      │◄────────────────────────────────────────────────────────────│
      │           negotiate() on both sides -> Agreed               │
      │                                                             │
      │  bidi stream, initiating half:                              │
      │      DATA{endpoint:"/transform"}                            │
      │────────────────────────────────────────────────────────────►│
      │  request payload ...                                        │  namespace lookup,
      │────────────────────────────────────────────────────────────►│  hand to application
      │                                                             │
      │      reply half:  DATA{}      (empty header map)            │  reply opened BEFORE
      │◄────────────────────────────────────────────────────────────│  the request FIN
      │  ... more request payload  ────────────────────────────────►│
      │  ◄──────────────────────────────  ... reply payload ...     │
      │  FIN (request)  ───────────────────────────────────────────►│  read to FIN
      │  finish() -> Delivery                                       │
      │  ◄─────────────────────────────  FIN (reply)                │
      │  ReplyStream::recv() -> IncomingTransfer                    │
      │                                                             │
```

The reply header carries no endpoint and no identifier — in the minimal case it is the empty
map. The stream it arrives on is the only correlation there is, and that correlation is
unforgeable by construction: a peer cannot answer an exchange it was not given. If the
responder will not answer at all, the reply half carries `ERROR{NO_REPLY}` + FIN instead of a
DATA header, and `recv()` returns `Error::NoReply`.

The two halves are independent for flow control, so the interleaving in the diagram is real
rather than illustrative. The stream count went from five to three, and `echo_1kib_rtt` fell
about 30 % with it ([IMPLEMENTATION.md](IMPLEMENTATION.md)).

`Delivery` is orthogonal to the reply and usually much slower than it: it reports that the
responder's *transport* took the request, and on an idle connection the peer's delayed
acknowledgement puts that tens of milliseconds after the reply has already been read. A
requester that wants the answer therefore ignores the receipt; a sender with no reply to wait
for is the one it exists for.

---

## 6a. Pattern taxonomy

Master doc §82 leaves open "whether a smaller internal primitive set can implement the
patterns cleanly". Answered from the built system: yes — and the set got simpler, not richer,
when Req/Rep moved onto a bidirectional stream.

| # | Primitive | Where it lives | Used by |
| --- | --- | --- | --- |
| P1 | one-way transfer: open uni → DATA header → payload → FIN → receipt | `Peer::open`, `OutgoingTransfer` | Push, Pub (per copy) |
| P2 | exchange: open bidi → request on one half, reply or ERROR on the other | `Peer::open_bi`, `ReplyStream`, `IncomingRequest` | Req/Rep only |
| P3 | peer set plus a selection policy | `PeerShared`'s slots in `stream.rs`; fan-out in `SubRegistry` | Req, Push (round-robin), Sub (all peers), Pub (fan-out) |
| P4 | bounded inbound queue behind an opaque path | `Namespace` + per-endpoint mpsc | Rep, Pull, Sub, Acceptor |

So: **Req = P2 + P3**, **Push = P1 + P3**, **Rep = P4**, **Pull = P4**, **Sub = P3 + P4**,
and **Pub = P1 per matching subscriber under a fan-out selection**.

P2 used to be something else entirely: a correlation table, a pending-reply map keyed by an
identifier the DATA header carried, owned by the connection actor and reachable only by
message. It is deleted, and nothing implements it now — **the bidirectional stream is the
correlation**. That is the difference between a primitive and machinery: P1 and P2 are the
two stream kinds QUIC already gives us, and the patterns add only selection (P3) and
queueing (P4) on top.

Cancellation and backpressure stay pattern-independent because they live in the primitives:
reset and stop codes in P1 and P2, `Limits::endpoint_queue` plus QUIC's own windows in P4. A
pattern chooses how peers are picked and where inbound work lands, and nothing else. This is
what "patterns are orthogonal to guarantees" (master doc §16) buys concretely — best-effort
Push and receipted Push are one code path, differing only in whether the caller awaits the
`Delivery`.

### Router/Dealer are emergent, not missing

ZeroMQ needs Dealer and Router because one socket is one ordered pipe: Dealer exists to
multiplex unsynchronized requests onto that pipe, Router to address replies back to a
specific peer identity.

Neither constraint exists here.

- **Unsynchronized multiplexed requests**: every request is already its own bidirectional
  QUIC stream, and the stream is the correlation. `Requester::open()` permits unlimited
  concurrent in-flight exchanges with no lockstep, bounded only by
  `max_concurrent_bidi_streams`. That is what Dealer provides.
- **Identity-addressed replies**: a `Replier` answers on the reply half of the very stream
  the request arrived on, so peer identity is implicit in the connection rather than carried
  in an envelope. That is what Router provides for the direct-peer case.

What Router adds *beyond* that — forwarding to third parties, explicit identity envelopes,
routing tables — is broker work (master doc §47, §85), belongs to L2 in Phase 6, and would be
a new component rather than a new socket type. Introducing `Endpoint<Router>` in v0 would
name a distinction the transport does not have.

### Why fan-out is unordered

P1 is one transfer per stream, and QUIC does not order streams relative to one another. A
publisher's per-subscriber writer is serialized, so copies are handed to the transport in
publication order — but that is a property of one hop's implementation, not a guarantee.
Making per-producer ordering real requires a sequence number in the DATA header and
reassembly on the receiving side; that is a deliberate later protocol addition rather than
something to imply from the current behaviour ([GUARANTEES.md](GUARANTEES.md) §6).

---

## 6b. The ZeroMQ pattern family, mapped

Every pattern in the family has a place in this model, and **every one of them is now built**.

| zmq/nanomsg | weida | Status |
| --- | --- | --- |
| REQ/REP | one exchange | implemented |
| DEALER/ROUTER | emergent: unlimited concurrent exchanges, identity = connection | no separate type |
| PUSH/PULL | one-way transfer, round-robin out, fan-in on the bound side | implemented |
| PUB/SUB | one-way fan-out plus SUBSCRIBE/UNSUBSCRIBE, publisher-side prefix filter | implemented |
| PAIR | one connection, one peer, one-way transfers in both directions | implemented |
| BUS | n members, each bound on its own path and dialling the others | implemented |
| SURVEYOR/RESPONDENT | fan-out of exchanges with a deadline | implemented |

"Mapped, unimplemented" **was** a status rather than a backlog entry, and the question it left
open — is this vocabulary worth a type? — has been answered twice: by the owner (**yes, the
full family**) and then by building all three (B-236, B-237, B-238), which completes the
nanomsg set, whose patterns are PAIR, REQREP, PUBSUB, PIPELINE, SURVEY and BUS. Each maps onto
stream kinds the wire already carries and primitives §6a already lists, so each is API surface
rather than protocol — and building them proved it: **not one byte of wire vocabulary was
added for any of the three.** A test has a `Paired` talking to a bare `Peer` and `Acceptor` on
the same path; a respondent's route is byte-for-byte a replier's; a bus member's is a
puller's.

Four things the table cannot say, each one found by building rather than by mapping.

- **PAIR is one-way transfers in both directions**, not "one exchange or one one-way transfer
  each way" as this row once read: an exchange's reply half would make one side a replier, and
  PAIR is symmetric. The one-peer rule is a claim taken at dispatch, and the **first** peer is
  kept — ZeroMQ drops the newcomer silently, weida refuses it with `LIMIT_EXCEEDED` and says so
  ([0005](decisions/0005-refusal-race.md)).
- **A bound side learns its peer only when that peer speaks.** A bound pair's first send
  therefore waits for the peer rather than buffering; the accepting side of this library has no
  other way to address a peer it has not heard from, and inventing a queue there would have been
  a guarantee nobody asked for.
- **SURVEY needed one primitive, not a pattern's worth.** `Peer::open_bi` picks *one* peer,
  which is Req/Rep's selection policy; a survey needs the same write against every peer, so the
  body moved into `stream::open_exchange_on` and the pattern is a fan-out of it plus a deadline.
  The deadline is the caller's, nothing on the wire carries it, and a late reply is dropped and
  **counted** where the reply arrives rather than where the caller reads — so the number means
  "after the deadline" whatever the caller does with its handle.
- **BUS needs a writer per member**, exactly as the fan-out does. Without one, a member that
  stops reading stalls the sender: its stream tasks hold streams open, the sender exhausts
  `max_concurrent_uni_streams`, and `open_uni` blocks. With one, a slow member costs its own
  copies — counted in `BusMember::dropped`, at the queue when it is full and at the wire when a
  write fails — and never the sender's time. That is the same rule Pub/Sub states in
  [GUARANTEES.md](GUARANTEES.md) §6, and BUS is the second pattern to need it, which is what
  makes it a rule rather than a Pub/Sub detail.

---

## 6c. Deviations from ZeroMQ, with reasons

weida is ZeroMQ's idea on QUIC, not ZeroMQ's behaviour. Five differences are deliberate, and
each is a choice rather than an omission.

1. **No mute state for an endpoint that never connected.** A ZeroMQ socket with no peer blocks
   silently; a weida endpoint that was never given an address returns `Error::NotConnected`,
   because a program that never connected should be told so, not hang. An endpoint that *was*
   connected and has lost every peer behaves as ZeroMQ's does: the address is redialled by the
   runtime, `open` waits for the next live connection — bounded by
   `RuntimeConfig::send_timeout` — and `send` is held in a bounded outbox
   ([decisions/0031](decisions/0031-transparent-redial-and-the-sender-outbox.md)). Where
   ZeroMQ's socket is silent about all of it, the endpoint's `PeerEvent` stream is not.
2. **Publisher-side filtering.** Subscriptions travel to the publisher and matching happens
   there, so a payload nobody subscribed to never crosses the network. ZeroMQ made the same
   move in 3.x; the deviation is only from the 2.x behaviour some people still expect.
3. **Streams subsume multipart messages.** A QUIC stream is already a framed, ordered byte
   sequence, so a multipart envelope would re-implement inside the payload exactly what the
   transport does outside it. There is no message-part concept anywhere in v0.
4. **bind/connect is fixed per pattern in v0.** Rep, Pull and Pub bind; Req, Push and Sub
   connect. ZeroMQ allows either side to do either; weida defers that until a use case asks
   for it.
5. **HWM ≙ QUIC windows plus `endpoint_queue`.** There is no high-water-mark setting.
   `stream_receive_window`, `connection_receive_window` and `endpoint_queue` together do the
   job a high-water mark does, and they do it by exerting backpressure rather than by
   discarding. `subscriber_buffer_bytes` is the one place in v0 where overload is answered by
   dropping ([GUARANTEES.md](GUARANTEES.md)).

---

## 7. Public API v0

The surface of crate `weida`, grouped by layer.

```rust
// re-exports from weida-core: Error, ErrorCode, StopReason, Limits, TraceContext,
//     EndpointAddr (with .peer: Option<Fingerprint>), Fingerprint
// new error variants: Error::InvalidFingerprint(String), Error::Untrusted(Fingerprint)
pub struct RuntimeConfig { pub limits: Limits,
    pub keep_alive: Duration /*10s*/, pub idle_timeout: Duration /*30s*/,
    pub worker_threads: usize /*1; Runtime::owned only*/ }                   // Default impl
pub enum Pem { Bytes(Vec<u8>), File(PathBuf) }              // TLS material need not be a file
pub struct Identity { pub cert_chain: Pem, pub key: Pem }   // who I am; Debug never prints the key
impl Identity {
    pub fn generate() -> Result<Identity, Error>;            // feature `generate`, on by default
    pub fn generate_for(names: impl IntoIterator<Item = impl Into<String>>)
        -> Result<Identity, Error>;                          // self-signed, also usable as an anchor
    pub fn from_pem(cert_chain: impl Into<Vec<u8>>, key: impl Into<Vec<u8>>) -> Identity;
    pub fn from_pem_files(cert_chain: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Identity;
    pub fn from_pem_file(path: impl Into<PathBuf>) -> Identity;    // one file, chain and key
    pub fn fingerprint(&self) -> Result<Fingerprint, Error>;  // what peers pin
    pub fn certificate_pem(&self) -> Result<String, Error>;   // publishable, carries no key
    pub fn to_pem(&self) -> Result<String, Error>;            // chain + key, for persisting
}
pub struct Trust { pub anchors: Vec<Pem>, pub pins: Vec<Fingerprint> }   // whom I accept
impl Trust {
    pub fn by_address() -> Trust;                             // empty: only what an address names
    pub fn pin(fingerprint: Fingerprint) -> Trust;
    pub fn anchor(pem: impl Into<Vec<u8>>) -> Trust;
    pub fn anchor_file(path: impl Into<PathBuf>) -> Trust;
    pub fn and_pin(self, fingerprint: Fingerprint) -> Trust;  // builders
    pub fn and_anchor(self, pem: impl Into<Vec<u8>>) -> Trust;
    pub fn and_anchor_file(self, path: impl Into<PathBuf>) -> Trust;
    pub fn is_empty(&self) -> bool;
}
pub struct ClientTls { pub trust: Trust, pub identity: Option<Identity> } // Eq+Hash: pool keys on it
impl ClientTls { pub fn new(trust: Trust) -> Self;            // dials anonymously
    pub fn with_identity(self, identity: Identity) -> Self; }  // From<Trust> for ClientTls
pub enum ClientTrust { Trusted(TrustSource), AnyKey }         // From<Trust>, From<TrustSource>
pub struct ServerTls { pub identity: Identity, pub client_trust: Option<ClientTrust> }
impl ServerTls { pub fn new(identity: Identity) -> Self;      // accepts anonymous peers
    pub fn require_client(self, trust: impl Into<ClientTrust>) -> Self; } // From<Identity> for ServerTls

pub struct Runtime;                                          // Clone (Arc inner); owns or borrows a tokio reactor
impl Runtime {
    pub fn new(config: RuntimeConfig) -> Result<Runtime, Error>;   // Error::Runtime if no ambient tokio handle
    pub fn with_handle(handle: tokio::runtime::Handle, config: RuntimeConfig) -> Runtime; // somebody else's reactor
    pub fn owned(config: RuntimeConfig) -> Result<Runtime, Error>; // owns one: worker_threads, default 1
    pub fn listener(&self) -> Listener;                      // a namespace; credentials belong to bindings
    pub fn peer(&self, tls: impl Into<ClientTls>) -> Peer;   // L0: streams, no pattern vocabulary
    // Trust is per dialling endpoint, mirroring per-binding server identity: one
    // process may talk to an internal CA and a public one without two runtimes.
    // A bare `Trust` converts, so an endpoint that presents no identity says so by omission.
    pub fn requester(&self, tls: impl Into<ClientTls>) -> Requester;
    pub fn pusher(&self, tls: impl Into<ClientTls>) -> Pusher;          // Push connects, Pull binds
    pub fn subscriber(&self, tls: impl Into<ClientTls>) -> Subscriber;  // Sub connects, Pub binds
    pub fn suppressed_duplicates(&self) -> u64;              // receiving-side counterpart of Publisher::dropped
    pub async fn shutdown(self);                             // abortive: close all conns/bindings code SHUTDOWN, bounded wait_idle
    // The bounded counterpart of 0009: stop admitting, let finished transfers
    // reach the peer's transport, then the same close. The deadline is
    // mandatory and finite; an expired drain is a count, not an error.
    pub async fn drain(self, deadline: Duration) -> Drained;
}
pub struct Drained { pub delivered: u64, pub outstanding: u64 } // Copy; local counts, never a claim about the peer
pub struct Listener;                                         // owns Namespace shared by all bindings
impl Listener {
    // Server identity is per binding: transports differ in what they need, and two
    // interfaces of one service may present different identities.
    pub async fn bind_quic(&self, addr: SocketAddr, tls: impl Into<ServerTls>)
        -> Result<Binding, Error>;                           // a bare Identity works
    // The same, on a UDP socket this process received (from its predecessor over
    // SCM_RIGHTS, or by socket activation) instead of one it binds [0038 §4.5].
    pub async fn bind_quic_socket(&self, socket: std::net::UdpSocket, tls: impl Into<ServerTls>)
        -> Result<Binding, Error>;
    // The same endpoints on the in-process transport: no socket, no TLS, no
    // credentials, bus name ≤ 256 B and unique in the process [0010 §4.1, §4.8].
    pub fn bind_inproc(&self, bus: &str) -> Result<LocalBinding, Error>;
    // AF_UNIX, Unix only: SOCK_STREAM on a path the caller's directory protects,
    // mode 0600 set explicitly, unlink-then-bind [0010 §4.5, 0012 §4.1].
    #[cfg(unix)]
    pub fn bind_unix(&self, path: impl AsRef<Path>) -> Result<UnixBinding, Error>;
    pub fn replier(&self, path: &str) -> Result<Replier, Error>;   // Error::InvalidEndpointPath / AlreadyRegistered
    pub fn puller(&self, path: &str) -> Result<Puller, Error>;     // same path-uniqueness rule
    pub fn publisher(&self, path: &str) -> Result<Publisher, Error>;
    pub fn acceptor(&self, path: &str) -> Result<Acceptor, Error>; // L0: both stream kinds, one queue
}
pub struct Binding;
impl Binding { pub fn local_addr(&self) -> SocketAddr;
    pub async fn close(&self);                                 // SHUTDOWN, idle wait bounded by shutdown_timeout [0038 §4.6]
    pub async fn restart(&self);                               // the same with RESTARTING: diallers see PeerRestarting, redial at once
    pub fn disconnect(&self, peer: Fingerprint) -> usize; }  // closes with REJECTED; not a ban [0035 §4.4]
pub struct LocalBinding;                                     // unbinds the bus when dropped
impl LocalBinding { pub fn bus(&self) -> &str; }
#[cfg(unix)]
pub struct UnixBinding;                                      // removes the socket file when dropped
#[cfg(unix)]
impl UnixBinding { pub fn path(&self) -> &Path; }
// Who a peer is, once proved: a key from TLS, or a principal from the kernel
// on a local transport [0010 §4.4]. `None` for an anonymous or in-process peer.
pub enum PeerIdentity { Key(Fingerprint), Local(LocalPrincipal) }
pub struct LocalPrincipal { pub uid: u32, pub gid: u32, pub pid: Option<u32> } // a PID is an observation

// ---- L0: the stream core -------------------------------------------------------------
pub struct Peer;                                             // dialling side; multi-peer, round-robin
impl Peer {
    pub async fn connect(&self, url: &str) -> Result<(), Error>;   // pooled per (authority, ClientTls, address pin)
    pub fn peer_count(&self) -> usize;                             // live peers only
    pub fn connection_stats(&self) -> Vec<ConnectionStats>;        // one per live slot, by URL [0036]
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error>;  // one-way transfer
    pub async fn open_bi(&self, meta: TransferMeta)
        -> Result<(OutgoingTransfer, ReplyStream), Error>;                            // exchange
}
pub enum Incoming { Stream(IncomingTransfer), Exchange(IncomingRequest) }
pub struct Acceptor;                                         // bound side; one path, both stream kinds
impl Acceptor { pub fn path(&self) -> &str;
    pub async fn accept(&self) -> Result<Incoming, Error>; }

// ---- L1: patterns --------------------------------------------------------------------
pub struct Endpoint<P: Pattern>;             // Pattern sealed; markers Req, Rep, Push, Pull, Pub, Sub
pub type Requester = Endpoint<Req>; pub type Replier = Endpoint<Rep>;
pub type Pusher = Endpoint<Push>;   pub type Puller = Endpoint<Pull>;
pub type Publisher = Endpoint<Pub>; pub type Subscriber = Endpoint<Sub>;

impl Requester {                                             // multi-peer: connects append; open() round-robins
    pub async fn connect(&self, url: &str) -> Result<(), Error>;   // weida://[sha256:<hex>@]host:port/path
    pub fn peer_count(&self) -> usize;
    pub fn connection_stats(&self) -> Vec<ConnectionStats>;        // on every dialling endpoint
    pub async fn open(&self, meta: TransferMeta) -> Result<(OutgoingTransfer, ReplyStream), Error>;
    pub async fn request(&self, body: &[u8]) -> Result<IncomingTransfer, Error>;   // open+write+finish+recv
    pub async fn request_with(&self, meta: TransferMeta, body: &[u8])
        -> Result<IncomingTransfer, Error>;
}
impl Replier { pub fn path(&self) -> &str;
    pub async fn accept(&self) -> Result<IncomingRequest, Error>; }

impl Pusher {                                                // same peer set and round-robin as Requester
    pub async fn connect(&self, url: &str) -> Result<(), Error>;
    pub fn peer_count(&self) -> usize;
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error>; // keeps the receipt
    pub async fn send(&self, body: &[u8]) -> Result<(), Error>;                      // discards the receipt
    pub async fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error>;
}
impl Puller { pub fn path(&self) -> &str;
    pub async fn recv(&self) -> Result<IncomingTransfer, Error>; }

impl Publisher {                                             // synchronous: never awaits a subscriber
    pub fn path(&self) -> &str;
    pub fn publish(&self, topic: &str, payload: impl Into<Bytes>) -> Result<usize, Error>;
    pub fn publish_with_trace(&self, topic: &str, payload: impl Into<Bytes>, trace: TraceContext)
        -> Result<usize, Error>;
    pub fn subscriber_count(&self) -> usize;                 // ops metrics
    pub fn filter_count(&self) -> usize;
    pub fn dropped(&self) -> u64;                            // messages lost to slow subscribers
}
impl Subscriber {
    pub async fn connect(&self, url: &str) -> Result<(), Error>;   // claims path in the client conn's namespace
    pub fn peer_count(&self) -> usize;
    pub async fn subscribe(&self, filter: &str) -> Result<(), Error>;   // byte prefix; "" = everything
    pub async fn unsubscribe(&self, filter: &str) -> Result<(), Error>;
    pub fn filter_count(&self) -> usize;
    pub async fn recv(&self) -> Result<IncomingTransfer, Error>;   // topic on IncomingMeta::topic
}

// ---- transfer handles, shared by both layers ------------------------------------------
#[derive(Default, Clone)] pub struct TransferMeta { pub content_type: Option<String>,
    pub content_len: Option<u64>, pub trace: Option<TraceContext> }  // None trace → generate ids
                                       // builders: with_content_type / with_content_len / with_trace

pub struct OutgoingTransfer;                                 // impl tokio::io::AsyncWrite + futures_io::AsyncWrite
impl OutgoingTransfer {
    pub fn trace(&self) -> TraceContext;
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error>;
    pub fn finish(self) -> Result<Delivery, Error>;          // sync: marks FIN, hands back the receipt
    pub fn cancel(self);                                     // RESET_STREAM(CANCELED)
}
pub struct Delivery;                                         // dropping it is the fire-and-forget path
impl Delivery { pub async fn delivered(self) -> Result<(), Error>; }  // QUIC's fin-ack, not an app ack

pub struct IncomingTransfer;                                 // impl tokio::io::AsyncRead + futures_io::AsyncRead
impl IncomingTransfer { pub fn meta(&self) -> &IncomingMeta; // endpoint, content_*, trace, topic, peer
    pub async fn read_capped(&mut self, max_bytes: usize) -> Result<Vec<u8>, Error>;
    pub async fn collect(self, max_bytes: usize) -> Result<Vec<u8>, Error>; } // LimitExceeded over cap
pub struct IncomingRequest;                                  // one accepted exchange
impl IncomingRequest { pub fn meta(&self) -> &IncomingMeta;
    pub fn body(&mut self) -> &mut IncomingTransfer;
    pub fn take_body(&mut self) -> IncomingTransfer;         // detach, to read while replying
    pub fn canceled(&self) -> impl Future<Output = ()>;      // the reply half's STOP_SENDING
    pub async fn reply(self, meta: TransferMeta) -> Result<OutgoingTransfer, Error>;  // exactly one
    pub async fn refuse(self, code: ErrorCode); }             // ERROR instead of a reply
pub struct ReplyStream;                                      // Drop before recv → STOP_SENDING(CANCELED)
impl ReplyStream { pub async fn recv(self) -> Result<IncomingTransfer, Error>; }
```

Type by type:

- **`RuntimeConfig`** — everything a `Runtime` needs: resource limits, the two timers and the
  worker count of a runtime it owns itself. Has a `Default`.
- **`Identity`** — who a binding or a dialling endpoint is: a certificate chain and the key
  behind it, from files or from memory. `generate()` (default feature `generate`) produces a
  self-signed identity carrying no names, made for pinning; `fingerprint()` is the value
  peers pin, embed in an address or list in a `Trust`.
- **`Trust`** — whom an endpoint accepts: pinned fingerprints, CA anchors, or nothing beyond
  what the dialled address names (`Trust::by_address()`).
- **`Fingerprint`** — the SHA-256 of a peer's DER `SubjectPublicKeyInfo`, `Display` and
  `FromStr` as `sha256:<64 hex>`. The one identity value in the system: pinned in a `Trust`,
  embedded in an address, reported on `IncomingMeta::peer`, carried by `Error::Untrusted`.
- **`ClientTls`** — a dialling endpoint's `Trust` plus an optional `Identity` to present. A
  bare `Trust` converts into it. Required to `connect()`; there is no platform-root or
  skip-verification path in v0.
- **`ServerTls`** — a binding's `Identity` plus an optional `ClientTrust`. A bare `Identity`
  converts into it; `require_client(trust)` makes the binding authenticate its clients against
  a `Trust`, and `require_client(ClientTrust::AnyKey)` makes it require a proved key and judge
  none ([decisions/0035](decisions/0035-keys-proved-not-judged.md)).
- **`ClientTrust`** — whom a binding that requires clients lets in: `Trusted(TrustSource)` or
  `AnyKey`.
- **`PeerChain`** — the certificate chain a peer presented, DER, leaf first, at most 8
  certificates and 32 KiB; `leaf()` and `certs()`.
- **`Runtime`** — the process-level container of §2. `Clone`, sharing an `Arc` inner. It
  holds the reactor rather than requiring one: `new` takes the ambient tokio runtime and
  fails with `Error::Runtime` when there is none, `with_handle` takes somebody else's, and
  `owned` creates and owns one (§5, Runtime ownership).
- **`Listener`** — one logical messaging namespace, owning the endpoint `Namespace` shared by
  all of its bindings.
- **`Binding`** — one concrete QUIC binding; exposes its resolved local address, which is how
  tests learn an ephemeral port, and `disconnect(fingerprint)`, which closes every live
  connection of one proved key with `REJECTED` — the dialler redials, so it is not a ban.
  `close` and `restart` close every connection with `SHUTDOWN` and `RESTARTING` and wait for
  the socket to go idle for at most `shutdown_timeout`; `restart` is the predecessor's half of a
  handoff whose successor serves the same socket through `Listener::bind_quic_socket`
  ([decisions/0038](decisions/0038-restart-handoff.md)).
- **`Peer`** — the L0 dialling side: a set of connections, the terms they were authenticated
  on (`ClientTls`, plus whatever each address named), and the two open calls. `peer_count`
  reports live peers only — a closed connection leaves the set when the next `connect()` adds
  a live one. `connection_stats` returns one record per live connection, labelled by the URL
  as dialled. Every dialling pattern is this plus a selection policy and some vocabulary.
- **`ConnectionStats`** — a dialling endpoint's view of one live connection: the URL as
  dialled, `age` (since the connection was established, so a redial resets it), `redials` of
  that address, and `transport: Option<TransportStats>` — `PathStats` (smoothed and minimum
  RTT, window, congestion events, lost packets and bytes, sent packets, MTU, datagram size)
  plus `UdpCounts` sent and received — `None` on a local transport. No statistics type carries
  a socket address ([decisions/0036](decisions/0036-connection-statistics.md) §4.4).
- **`Acceptor`** — the L0 bound side: one path, both stream kinds, one queue. Where a
  `Replier` accepts only exchanges and a `Puller` only one-way transfers, an `Acceptor` takes
  whatever arrives and lets the application decide.
- **`Incoming`** — what `Acceptor::accept()` yields: `Stream` for a one-way transfer,
  `Exchange` for a bidirectional one.
- **`Endpoint<P>`** — the typed L1 messaging object. `Pattern` is a sealed trait; the v0
  markers are `Req`, `Rep`, `Push`, `Pull`, `Pub` and `Sub`.
- **`Requester`** — `Endpoint<Req>`. Multi-peer: each `connect()` appends a peer and `open()`
  round-robins across them, preserving the ZeroMQ multi-peer property. `open()` returns both
  halves of one exchange.
- **`Replier`** — `Endpoint<Rep>`. `accept()` yields inbound exchanges from the endpoint
  queue.
- **`TransferMeta`** — per-transfer outbound metadata: content type, advisory length, trace
  context. A `None` trace means the runtime generates fresh trace and span ids.
- **`OutgoingTransfer`** — the write half of a transfer, an `AsyncWrite` in both the
  `tokio::io` and the `futures-io` sense. `finish()` is
  synchronous: it marks the FIN and hands back the receipt without waiting for it. `cancel()`
  resets the stream with `CANCELED`, and so does dropping the handle unfinished.
- **`Delivery`** — the transport receipt. `delivered()` resolves `Ok(())` once the peer's
  transport has acknowledged every byte and the FIN, yields the peer's typed refusal if it
  stopped the stream instead, and yields `Error::Indeterminate` if the connection was lost
  after the FIN went out ([FAILURE_MODEL.md](FAILURE_MODEL.md) §4). Dropping it is free, and
  that is the fire-and-forget path `Pusher::send` takes.
- **`IncomingTransfer`** — the read half of a transfer, an `AsyncRead`, plus its metadata.
  Reaching EOF is just EOF; the v0 core emits nothing in response. `collect(max_bytes)` is the
  opt-in materialization convenience with a mandatory cap; it is never used internally.
  `meta().peer` is the fingerprint the sender proved in the handshake, `None` for an
  anonymous client, and it is what an application authorizes on; `meta().peer_chain` is the
  chain behind it, for an application that derives more from the key than the key itself.
- **`IncomingRequest`** — an accepted exchange: its metadata, its body as an
  `IncomingTransfer`, and the reply half it owes the requester. `reply()` consumes it, because
  an exchange has exactly one reply and a second one should not be representable. It may be
  called before the request body reaches FIN — but it then drops whatever is left of the body,
  refusing the remainder with `REJECTED`, so a handler that wants to read while it writes
  calls `take_body()` first. The reply defaults its trace context to the request's.
  `canceled()` is the reply half's `stopped()` future and must be taken before `reply()`
  consumes the request. Dropping the handle without replying puts `ERROR{NO_REPLY}` on the
  reply half, so an unanswered request fails fast instead of hanging — and `refuse(code)` is
  the same thing said on purpose: the only place where an **application** decides an outcome
  the requester sees, which is what [decisions/0005](decisions/0005-refusal-race.md) §4.3
  means by "the ERROR frame is written by the application". `Rejected` where this side
  declines, `NoReply` where the request was taken and no reply will exist.
- **`ReplyStream`** — the requester's half of an exchange. `recv()` yields the reply's
  `IncomingTransfer`, or the peer's typed `Error` when the reply half carried an ERROR instead.
  Dropping it before `recv()` stops that half with `CANCELED`, so a responder streaming a long
  reply learns that nobody is listening.
