# Library parity documents

This directory documents the six foreign-protocol libraries with their bindings — ZeroMQ,
nanomsg/NNG, MQTT 5, AMQP 1.0, Core NATS and HTTP — plus weida's own Python binding
([`weida-py.md`](weida-py.md)) and the OpenBao companion crate
([`weida-openbao.md`](weida-openbao.md)). Each foreign protocol is a **standalone library**, a
crate usable with no weida in the picture ([0013](../decisions/0013-competitor-libraries.md)
§4, §4.7). Its document is the parity table: it states, row by row, what the library does with
every inventory item of the protocol's reference implementation.

## A library document is not a research sheet

The documents answer different questions:

| | `docs/research/<proto>.md` | `docs/libraries/<proto>.md` |
| --- | --- | --- |
| Subject | the upstream protocol and implementations | this repository's standalone library |
| Question | what does the protocol specify and what do real peers do | does this implementation cover the reference inventory |
| Vocabulary | the foreign protocol's only | the foreign protocol's public API and this library's implementation |
| Verdicts | sourced fact or named open question | present, refused with a reason, or absent with a reason |

The repository defines no global mapping between a foreign protocol and weida patterns.
Applications may compose the public libraries explicitly. Future broker integration is
documented by the schema of one managed Connector resource and its concrete conversion policy,
not by a protocol-wide equivalence table.

## Required sections

Every parity document answers all of these, in this order. A section that does not apply says
so in one line rather than being omitted.

```
# <Protocol> (<reference implementation and version>) — feature parity

## 1. What a row means
The three verdicts, the rule against "partial", and the versions every measured claim was
measured against.

## 2. <Primitive> types
Every socket/link/channel kind of the reference implementation's own table, with the rows that
table has, and where each one is implemented or why it is absent.

## 3. Transports
Every transport the sheet inventories, present or absent-with-reason.

## 4. Mechanisms and authentication
Every security mechanism, and the authentication dialog if the protocol has one.

## 5. Options
Every configuration option of the reference implementation, honoured under the name this
library gives it or refused with the reason.

## 6. Observability
The event set, counters or tracing surface, and what a row's value carries where it differs.

## 7. Devices and helpers
The intermediaries the reference implementation ships as API.

## 8. Interop evidence
Which implementations were run against this one, in which roles, how many pairings, and what
disagreed — each disagreement measured rather than inferred.

## 9. Deliberate deviations and bounds the reference implementation lacks
Where this library is on purpose not identical, and every ceiling it adds.

## 10. The definition of done
The clauses this library was built against, each with its verdict and the section that proves
it.

## 11. Sources
The research sheet with section numbers, the decisions, and the code paths the table's
verdicts were read from.
```

Two rules keep these documents honest:

1. **Every row carries its evidence.** A "present" row names the module or the test; a
   "refused" row names the reason the code gives at configuration time; an "absent" row names
   what is missing. A row whose verdict cannot be checked from the repository does not belong
   here.
2. **No aggregate verdicts.** "Mostly complete", "partial support" and "planned" are not
   verdicts. Either the row is present, or the document says what a caller who wants it does
   not get.

## Documents

| File | Library crates | Reference implementation | Sheet | Status |
| --- | --- | --- | --- | --- |
| [`zmq.md`](zmq.md) | `weida-zmtp` (codec), `weida-zmq` (implementation) | libzmq 4.3.5, plus the pure-Rust `zeromq` 0.6.0 | [`zeromq.md`](../research/zeromq.md) | complete against 0013 §4.7's six clauses |
| [`zmq-py.md`](zmq-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-zmq-py` (the binding) | pyzmq 27.2.0 over libzmq 4.3.5 | [`zeromq.md`](../research/zeromq.md) | covers: all eleven socket types, the three transports, the 98-row option table, PLAIN/CURVE/ZAP with a handler in Python, the monitor, the devices, and a synchronous surface beside the asyncio one. Does not cover: `zmq.STREAM` and the DRAFT socket types, `zmq.Poller`/`sock.fd`, `send_string`/`send_json`/`Frame`, pyzmq's `zmq.auth` policy framework, and **concurrent send and recv on one socket** — see §9.1, which is the library's `&mut self` and is filed against `weida-zmq` |
| [`nng.md`](nng.md) | `weida-sp` (codec), `weida-nng` (implementation) | NNG 1.4.0-rc.0, through the `nng` crate 1.0.1 | [`nanomsg-nng.md`](../research/nanomsg-nng.md) | complete against 0013 §4.7's six clauses, read for SP |
| [`nng-py.md`](nng-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-nng-py` (the binding) | pynng 0.9.0 over NNG 1.11.0 — a **second C release** beside the Rust suite's 1.4.0-rc.0, which §1 states per claim | [`nanomsg-nng.md`](../research/nanomsg-nng.md) | covers: all eleven protocol classes and three context classes, `tcp`/`ipc`/`inproc`, the 50-row option table with 34 honoured and 16 refused by name, 23 client failures as exception classes, six interop pairings against pynng, and a synchronous surface over `weida-nng`'s `blocking` feature. Does not cover: `tls+tcp` (the library has the transport, this surface has no certificate API), raw sockets and `nng_device`, pipe callbacks and per-endpoint options; the wheel is built and run with no Rust toolchain by `package.sh`, as for every other binding (§10) |
| [`mqtt.md`](mqtt.md) | `weida-mqtt-codec` (codec), `weida-mqtt` (client) | rumqttd 0.20.0 and rmqtt 0.23.1 | [`mqtt5.md`](../research/mqtt5.md) | complete against 0013 §4.7's six clauses, read for MQTT. Its protocol is **asymmetric**: it is a client, a server is a broker ([0014](../decisions/0014-parallel-libraries.md) §2), and §0 draws that line before the first row so a reader learns why there is no server column |
| [`mqtt-py.md`](mqtt-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-mqtt-py` (the binding) | paho-mqtt 2.x as the documented reference, **not pinned and not run**; measured against rumqttd 0.20.0 | [`mqtt5.md`](../research/mqtt5.md) | covers: connect with 19 keyword options each honoured or refused by name, the session, publish returning a `Completion`, subscriptions as iterators of `Delivery`, all four CONNACK-declared limits readable, 50 exception classes (36 reason codes plus 13 without a byte plus the base), and a synchronous surface sharing every value class with the asyncio one. Does not cover: TLS — absent by construction, the library takes the caller's `rustls::ClientConfig` and `is_encrypted` is always `False` (§3, §9.1) — WebSockets, multi-step AUTH from Python, paho's `loop*` family, and interop against paho itself, which §10 records as not met |
| [`amqp.md`](amqp.md) | `weida-amqp-codec` (codec), `weida-amqp` (client) | `fe2o3-amqp` 0.18.4, the pure-Rust peer, through its `acceptor`; RabbitMQ 4.x **not run**, its binary absent | [`amqp10.md`](../research/amqp10.md) | complete against 0013 §4.7's six clauses, read for AMQP. Covers: all nine performatives, all nine message sections, both credit schemes, the settle modes and the five delivery states, `ANONYMOUS`/`PLAIN`/`EXTERNAL` and both routes to TLS. Does not cover: Part 4 transactions, link resumption, any interpretation of the three filter families (the field is carried), the WebSocket binding, and a broker — the standard defines no node model |
| [`amqp-py.md`](amqp-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-amqp-py` (the binding) | python-qpid-proton 0.40.0; the interop peer is `fe2o3-amqp` 0.18.4 | [`amqp10.md`](../research/amqp10.md) | covers: connection, session and link as objects, `await send` returning the delivery's terminal state, an async iterator of deliveries with all seven section accessors, credit granted explicitly, 28 conditions plus 16 client failures as exception classes, and a synchronous surface beside the asyncio one. Does not cover: TLS - the library takes the caller's `rustls::ClientConfig`, which has no Python spelling and is filed rather than guessed at (§9.5) - Proton's container and handler classes, the AMQP type wrappers, Part 4 transactions, and a listener |
| [`nats.md`](nats.md) | `weida-nats-codec` (codec), `weida-nats` (client) | `async-nats` 0.50 as the peer, **through a `nats-server` that is not available**: interop is written and `#[ignore]`d, not run | [`nats.md`](../research/nats.md) | five of 0013 §4.7's six clauses met; **clause 4, interop, is explicitly not met** and says so, because `nats-server` is absent and `async-nats` is a client rather than a substitute for one. Covers: all twelve verbs, both wildcards, queue groups, request-reply over an inbox with the 503 kept distinct from a timeout, all five credential forms, TCP and TLS. Does not cover: JetStream, a reconnect loop, the WebSocket transport, UNIX-domain sockets, and any cryptographic implementation of NKey or JWT signing — the caller supplies a closure |
| [`nats-py.md`](nats-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-nats-py` (the binding) | nats-py 2.15.0 | [`nats.md`](../research/nats.md) | covers: connect with 24 refused-by-name options, publish with headers, subscriptions as async iterators with the `sid` and the `NATS/1.0` status readable, queue groups asserted on the wire, `unsubscribe_after`, `request` with a mandatory timeout, `request_many`, the 503 as its own class beside the timeout, 24 client failures as exception classes, and a synchronous surface nats-py does not have at all. Does not cover: TLS - the library takes the caller's `rustls::ClientConfig`, which has no Python spelling (§8.5) - JetStream, `nats.micro`, push callbacks, drain, a server pool and any reconnect loop. **Interop is not met**: `nats-server` is absent, so the two tests that need one are skipped |
| [`http.md`](http.md) | `weida-http-codec` (codec), `weida-http` (client and server) | hyper 1.11.1 (HTTP/1.1, HTTP/2) and h3 0.0.8 on h3-quinn 0.0.10 (HTTP/3), all run in both roles; curl 8.23.0 as a smoke | [`http.md`](../research/http.md) | complete against 0013 §4.7's six clauses. Covers: HTTP/1.1, HTTP/2 over TLS by ALPN and HTTP/3 over QUIC (feature `h3`), client and pull-style server, chunked bodies, trailers, 100-continue, pipelined answers, flow control, GOAWAY, credentials never compressed or printed. Does not cover: redirects, cookies, content coding, proxies, Alt-Svc, h2c, server push, CONNECT/Upgrade/WebSocket, the QPACK dynamic table (0044 §6), and a Python binding (B-371) |
| [`weida-py.md`](weida-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-py` (the binding) | **none — this is weida's own binding**, so the reference is the **Rust API of `crates/weida`** it binds, which its §1 states before the first row: weida has no other Python client and the protocol has one implementation | none; the normative specification is [`PROTOCOL.md`](../PROTOCOL.md) | covers: `Runtime` on a reactor of its own, QUIC `bind`, **all six of weida's patterns** — Req/Rep, Push/Pull, Pub/Sub, PAIR, SURVEY and BUS — plus RADIO/DISH, with the streamed form wherever the pattern has one, both fan-out write forms, the **cursor surface** with levels as integers, 25 exception classes (24 named plus the base), and a `sync` module over `weida::blocking` carrying the same eighteen classes. Absent: the raw L0 surface and type stubs |
| [`weida-openbao.md`](weida-openbao.md) | `weida-openbao` (client and identity sources) | **not a foreign protocol**: OpenBao 2.6.0's HTTP API, exercised by `tests/bao_dev.rs` against the local binary in dev mode; the scripted tests in the gate | none; the decision is [0032](../decisions/0032-identity-sources-and-the-handoff.md) | covers: token, AppRole and the wrapped hand-off with theft detection, token renewal, `PkiSign` over the local key with renewal, `PkiAnchor`, `Kv`, `post_wrapped`/`revoke_accessor` for a controller. Does not cover: `pki/issue` (refused by decision), revocation checks, a Python surface |

Every binding this repository ships has its document above; there is no planned entry.
