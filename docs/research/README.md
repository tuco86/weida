# Protocol research catalogue

Before weida speaks a foreign protocol, that protocol is researched on its own terms and the
result is written down here, one sheet per protocol, in one fixed shape. The sheets are the
raw material; the mapping onto weida happens afterwards, in a separate synthesis, so that the
research is not bent toward the answer we hope for.

Two rules make a sheet useful rather than merely long:

1. **Protocol-native vocabulary only.** A sheet describes sockets, links, sessions, queues,
   exchanges, QoS levels — whatever the protocol calls its things — and never translates them
   into somebody else's terms. No sentence in a sheet should mention weida.
2. **Every claim has a source with a version or date.** Official specification first,
   official guide second, maintainer-written material third, everything else marked as such.
   Where a guide and the specification disagree, both are quoted and the disagreement is
   recorded. Where a claim is the author's inference, it says `[inference]`.

## Template

Every sheet uses these sections, in this order, with these headings. A section that does not
apply says so in one line rather than being omitted.

```
# <Protocol> <version(s)>

## 0. Identity card
Name, versions in use, governing body, specification documents, year of the version researched,
reference implementation, wire type (binary/text), transports the specification defines.

## 1. Connection and session lifecycle
Handshake, negotiation, authentication step, keep-alive/heartbeat, idle behaviour, orderly
close, abortive close, reconnection rules the protocol itself defines.

## 2. Primitives
The nouns: what a program creates and holds (sockets, channels, sessions, links, subscriptions,
consumers...). For each: cardinality, lifetime, thread/ownership rules, what state it holds.

## 3. Message model
Framing, size limits, multipart or single, headers/properties, payload typing, message identity
(ids, sequence numbers), what is opaque and what the protocol interprets.

## 4. Patterns and topologies
As the official guides present them: each named pattern, which primitives it composes, who binds
and who connects, routing/selection rule inbound and outbound, behaviour when there is nobody to
talk to. Reproduce the guide's own explanations and diagrams in prose.

## 5. Flow control and backpressure
Unit of credit (bytes, messages, packets), who grants it, defaults, what happens on exhaustion
(block, drop, disconnect, error), per-connection vs per-channel vs per-subscription.

## 6. Delivery guarantees and acknowledgement
Every acknowledgement or confirmation the protocol has, from whom to whom, what state it
certifies, and what it does not. At-most/at-least/exactly-once as the protocol itself defines
them. Redelivery rules.

## 7. Ordering and duplicates
What order is promised and within which scope; where duplicates can arise; what the protocol
offers to detect them.

## 8. Failure behaviour
A table: event (peer crash, network partition, broker restart, slow consumer, oversized
message, unknown destination, auth failure...) → what each side observes, what is lost, what is
ambiguous.

## 9. Reliability recipes
The distributed-systems recipes the guides teach on top of the primitives (heartbeating,
retry, failover pairs, state replication, persistence, disk-backed delivery, service brokers).
For each: name as the guide names it, problem, mechanism, guarantee, cost, known failure modes.

## 10. Security and identity
Authentication mechanisms, authorization model, whether an identity travels with a message,
transport security.

## 11. Limits and resource bounds
Every limit a peer can configure against the other side; defaults; what an unbounded resource
looks like in practice.

## 12. Answers to the problem catalogue
One entry per problem P1..P18 below, in protocol-native terms, or "no answer in this protocol".

## 13. Ecosystem
Implementations by language with maintenance status, notable deployments, known
incompatibilities between implementations, Rust crates.

## 14. Sources
Numbered list: title, URL, version/date, what it was used for.
```

## Problem catalogue

The problems every messaging system has to answer somehow. Phrased without reference to any
protocol so that each sheet answers them in its own words. Section 12 of every sheet goes
through this list; the synthesis compares the answers side by side.

| # | Problem |
| --- | --- |
| P1 | A request or message may be lost: how is loss detected and how is a safe retry made (duplicates, idempotency)? |
| P2 | A peer may be dead or unreachable: how is liveness detected, in what time, and what happens to its state (last will, session, in-flight messages)? |
| P3 | Work must be spread over several workers by capacity, not by turn: what mechanism, what unit? |
| P4 | A consumer is slower than the producer: where does the backlog sit, what is its bound, and what happens at the bound (block, drop, disconnect, error)? |
| P5 | A late joiner needs current state (last value, snapshot, replay): what does the protocol offer? |
| P6 | A broker or peer fails over to another: what does the client do, what is re-established automatically (subscriptions, sessions, in-flight)? |
| P7 | Delivery must survive a restart: what is persisted, who decides, what is the acknowledgement that certifies it? |
| P8 | What ordering is guaranteed and within which scope (connection, channel, key, partition, topic)? |
| P9 | Duplicates: where can they arise and what suppresses them; what does "exactly once" mean, if offered? |
| P10 | Request/reply: how is a reply correlated and routed back through intermediaries? |
| P11 | Topology: who binds and who connects, can both sides be either, brokered vs brokerless, discovery. |
| P12 | Flow-control credit: unit, grantor, default, exhaustion behaviour. |
| P13 | Large messages and streaming bodies: maximum size, whether a body can be delivered before it is complete. |
| P14 | Identity: how peers authenticate, whether an authenticated identity is visible per message, authorization granularity. |
| P15 | Resource bounds: what a hostile or buggy peer can make the other side allocate, and which limits exist against it. |
| P16 | Observability: what the protocol lets a party measure about delivery (confirms, receipts, counters, tracing headers). |
| P17 | Shutdown: what happens to in-flight and queued messages on orderly close (linger, drain). |
| P18 | Transports: which transports the protocol defines (TCP, TLS, QUIC, WebSocket, IPC, in-process) and what changes between them. |

## Sheets

| File | Protocol | Status |
| --- | --- | --- |
| `zeromq.md` | ZeroMQ: ZMTP 3.1, the zguide patterns, CURVE/ZAP | done; §1, §6 and §13 carry what an interop run against `zeromq` 0.6.0 measured [40] |
| `rabbitmq-amqp091.md` | RabbitMQ and AMQP 0-9-1 with RabbitMQ extensions | done |
| `amqp10.md` | AMQP 1.0 (ISO/IEC 19464) | done; §1, §3, §6.2, §6.3 and §13 carry what a client run against `fe2o3-amqp` 0.17.0 in both roles measured [67], including `rcv-settle-mode=second` honoured by an implementation the sheet's incompatibility list had only seen refused. RabbitMQ 4.x was unreachable, so its four published non-supports stay documentation and are listed as an open question |
| `mqtt5.md` | MQTT 5.0, with 3.1.1 differences | done |
| `nats.md` | NATS core and JetStream | done; §1, §3, §4 and §13 carry what writing a client from the protocol reference settled where the reference is silent [30] and what the server's own subject validator enforces [31]. **No `nats-server` was reachable**, so nothing added is an observation of a server and the sheet says so at the source |
| `nanomsg-nng.md` | nanomsg / NNG scalability protocols | done; §1, §3, §4, §6, §8, §11 and §13 carry what an implementation run against NNG 1.4.0-rc.0 through `nng` 1.0.1 observed [30], including the byte-level layouts the RFCs leave in prose |
| `http.md` | HTTP semantics, HTTP/1.1, HTTP/2 with HPACK, HTTP/3 with QPACK | done |
| `kafka.md` | Apache Kafka protocol and client semantics | done |
| `ipc.md` | Inter-process and in-process transports per platform | done |
| `quic-standards.md` | QUIC RFCs, extensions, drafts and measurement literature | done |
| `prior-art.md` | Messaging systems on and near QUIC: Zenoh, iroh, EMQX, MoQ, libp2p and others | done |
| `SYNTHESIS.md` | The problem catalogue answered side by side, plus what weida takes from each | done |

The synthesis — the same problems answered side by side, and what weida takes from each — is
`SYNTHESIS.md`, written only after the sheets are complete.

`quic-standards.md` and `prior-art.md` use their own layouts rather than the template above,
because one describes a transport and the other surveys existing systems rather than describing a
single protocol; `SYNTHESIS.md` predates both and does not yet incorporate them.
