# 0013 — Standalone foreign-protocol libraries beside weida

- **Status:** accepted
- **Date:** 2026-09-11
- **Revised:** 2026-09-18
- **Related:** [ARCHITECTURE.md](../ARCHITECTURE.md) §4;
  [LOOP.md](../LOOP.md) §9; [0014](0014-parallel-libraries.md)

## 1. The question

Where should implementations of ZeroMQ, nanomsg/NNG, MQTT, AMQP and NATS live, and may their
semantics appear behind weida's patterns?

## 2. Constraints

- A user of a foreign protocol must be able to use its library without linking weida's
  protocol, QUIC transport or pattern layer.
- A foreign protocol keeps its own socket or client model, framing, addressing, security,
  options, error vocabulary and transfer points.
- One public type must not hide two protocol semantics. `Requester`, `Publisher` and the other
  weida patterns never acquire a foreign-protocol backend.
- Code checked byte-for-byte against a foreign specification must not depend on weida's wire
  types.
- Composition between two public libraries is application code unless the broker owns it as an
  explicit managed resource.

## 3. Rejected shapes

1. **A foreign backend inside weida patterns.** Rejected because similar pattern names do not
   establish equal state machines, overload policies, identities or guarantees.
2. **A general bridge or forwarder product category.** Rejected because it requires a global
   socket-to-pattern mapping that does not exist, creates a second deployed hop for ordinary
   library composition, and makes protocol-specific losses look like framework policy.
3. **One repository per protocol.** Rejected because the runtime and OS plumbing are genuinely
   shared and version skew would add no product value.

## 4. Decision

The repository holds two kinds of product:

- **weida** — its protocol, libraries, bindings and broker;
- **standalone foreign-protocol implementations** — native libraries and bindings that share
  runtime plumbing but none of weida's protocol.

There is no general weida-to-foreign bridge crate and no global equivalence table between
foreign socket types and weida patterns.

### 4.1 Crate layout

One directory per foreign protocol family:

```text
crates/zmq/   weida-zmtp, weida-zmq, weida-zmq-py
crates/nng/   weida-sp, weida-nng, weida-nng-py
crates/mqtt/  weida-mqtt-codec, weida-mqtt, weida-mqtt-py
crates/amqp/  weida-amqp-codec, weida-amqp, weida-amqp-py
crates/nats/  weida-nats-codec, weida-nats, weida-nats-py
crates/http/  weida-http-codec, weida-http
```

The directory names the protocol family, not a role in a weida deployment.

### 4.2 Dependency direction

`weida-runtime` may provide reactor ownership, timers, DNS and OS hygiene.
`weida-core` may provide protocol-neutral OS error and local-principal types. A
foreign-protocol library MUST NOT depend on `weida` or `weida-protocol`; neither may the core
depend on a foreign library.

A future broker Connector crate may depend on `weida-broker` and exactly the foreign library
it operates. Neither dependency may point back toward the Connector.

### 4.3 Sans-I/O codecs

Each byte-level codec keeps an empty `[dependencies]` where practical. Decode entry points take
their resource cap as an argument and reject oversized declarations before allocation. Crypto,
I/O, reconnect and policy belong in the library above the codec.

### 4.4 Library API

Each library exposes the native protocol's model with typed states where the protocol has them,
is async first, offers a blocking facade where the ecosystem requires one, and honours or
explicitly refuses every exposed option. Foreign identities remain distinct types; there is no
`From`, `Into`, `AsRef` or `Deref` conversion to a weida identity.

### 4.5 Native devices and application composition

Protocol-native devices remain in their library. In particular, `weida_zmq::proxy` and
`proxy_steerable` implement the standard ZeroMQ queue, forwarder and streamer topologies by
moving ZeroMQ multipart messages between compatible ZeroMQ sockets. They do not speak weida and
are not the rejected bridge category.

An application may use a foreign library and `weida` in one process and write its own explicit
conversion. That code owns its schema and policy; the workspace does not promote it into a
universal mapping.

### 4.6 Broker-managed connectors

Broker integration is a different boundary. A Connector is a long-lived resource committed
through the control plane and attached to a named Queue. Its specification names one protocol,
one direction, one address, limits and an explicit application conversion policy. A Connector
is reconciled and observed like every other broker resource. It is not a backend of a weida
pattern and does not establish equivalence for any other deployment.

### 4.7 Definition of first-class

A foreign implementation is first-class only when all six clauses are checkable:

1. the protocol's public primitive inventory has typed API coverage;
2. the required transports are implemented or refused with reasons;
3. security and authorization mechanisms are implemented or refused with reasons;
4. options and resource bounds are covered row by row;
5. monitoring and native device surfaces are covered where the protocol defines them;
6. a parity document records every row as present, refused with a reason or absent with a
   reason, backed by interop against a named upstream implementation and version.

## 5. Consequences

- `weida-zmq-bridge`, `weida-nng-bridge` and the cross-protocol bridge test crate are removed.
- `docs/adapters/` is retained only as stable links that record the rejected design; it carries
  no normative mappings.
- The standalone libraries, codecs, bindings, interop tests and parity documents remain.
- Broker work models Queue and Connector lifecycle in the control Raft and queue traffic in one
  Raft group per replicated queue.
