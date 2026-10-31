# weida

weida is a messaging framework for Rust built from first principles around QUIC. Every user
data flow maps to one QUIC stream, and a flow whose units are worthless once late maps to QUIC
datagrams, so a 40-byte transfer and a 40-GB transfer use the same protocol semantics and
neither requires the payload to be materialized in memory.

It is built in layers. **L0** is a stream core: ZeroMQ's idea rebuilt on QUIC, whose
primitives are unidirectional and bidirectional streams and datagram flows with exactly the
guarantees QUIC gives — ordered bytes within a stream, none across streams, flow control, a
transport delivery receipt, and cancellation by reset. **L1** is the ZeroMQ/nanomsg pattern
family as thin wrappers over L0: Req/Rep, Push/Pull, Pub/Sub, PAIR, SURVEY and BUS — the whole
nanomsg set — and RADIO/DISH for lossy fan-out of segments; only RADIO/DISH adds wire
vocabulary (DATA key 13, SUBSCRIBE key 2). **L2** is a RabbitMQ-analog broker,
`weida-broker`: queues at endpoint paths, publisher confirms, an absolute per-subscription
credit, and consumer settlement with requeue when a consumer is lost. The redelivery count on
the wire, relaying a consumer's report to the producer and retiring a queue are still to come.

A completion is a **cursor**, not a verdict: a level plus an **absolute byte offset**, reported
on a unidirectional stream of its own that never shares a stream with payload. So a Push
producer gets a reliable `Accepted` without an exchange, a reader learns how far the far end
got rather than only whether it finished, and no pattern changes shape to gain any of it.

**Status:** alpha. `weida`, `weida-core`, `weida-protocol`, `weida-runtime`, `weida-winpipe`,
`weida-broker`, `weida-raft`, `weida-openbao`, `weida-http-codec` and `weida-http` are
published on crates.io, all at `0.1.0-alpha.10`. The other seventeen members are not
published: `weida-py-core`, every Python binding, the other five protocol libraries with
their codecs, and `weida-testkit`, which only tests use. The one exception on crates.io,
`weida-zmtp` `0.1.0-alpha.2`, is an old release that receives no updates. Nothing is on PyPI
and there are no prebuilt binaries. Wire protocol version `0` is experimental and breaking
changes are permitted within `0.x`. Phase status lives in
[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) §1.

Beside weida the repository ships **six protocol libraries**, none of which has weida in its
picture: ZeroMQ (`weida-zmtp` + `weida-zmq` + `weida-zmq-py`), the nanomsg SP protocols
(`weida-sp` + `weida-nng` + `weida-nng-py`), MQTT 5 (`weida-mqtt-codec` + `weida-mqtt` +
`weida-mqtt-py`), AMQP 1.0 (`weida-amqp-codec` + `weida-amqp` + `weida-amqp-py`), Core NATS
(`weida-nats-codec` + `weida-nats` + `weida-nats-py`) and HTTP/1.1, HTTP/2 and HTTP/3
(`weida-http-codec` + `weida-http`), each with a parity document under
[docs/libraries/](docs/libraries/) and each measured against a real peer where one exists.
`weida-zmq` is complete against
[decisions/0013](docs/decisions/0013-competitor-libraries.md) §4.7's definition of
first-class — every socket type of `zmq_socket(3)` bar `ZMQ_STREAM`, `tcp`/`ipc`/`inproc`,
NULL/PLAIN/CURVE with ZAP, the option table honoured or refused row by row, the monitor and
the devices, the zguide's canonical recipes as examples that assert the guide's own claims,
and interop against libzmq 4.3.5 and the pure-Rust `zeromq` crate in both roles
([docs/libraries/zmq.md](docs/libraries/zmq.md)). Its typed `proxy` and
`proxy_steerable` devices include the canonical ROUTER/DEALER queue, XSUB/XPUB
forwarder and PULL/PUSH streamer without involving the weida protocol.

## Documents

**Start with the guide.** The rest of this table specifies; [docs/GUIDE.md](docs/GUIDE.md)
teaches, in the shape ZeroMQ's zguide established — every program in it is a file in this
repository and a test asserts the claim the text makes. It is organised around one question,
**C8B: how do you scale a software system to eight billion people?**, which is this project's
C10K: utopian on purpose, and useful because the arithmetic settles structural questions before
anything is built ([decisions/0026](docs/decisions/0026-the-guide-and-the-c8b-question.md)).

| Document | What it covers |
| --- | --- |
| [docs/GUIDE.md](docs/GUIDE.md) | the guide: the C8B question and its arithmetic, then one chapter per scaling step, each with programs you can run |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | layer model, terminology, addressing, crate map, runtime internals, public API v0 |
| [docs/PATTERNS.md](docs/PATTERNS.md) | the pattern reference: per-pattern tables in the shape of `zmq_socket(3)`, and what QUIC streams do underneath, measured |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | normative wire protocol v0: framing, frame headers, golden vectors, limits |
| [docs/GUARANTEES.md](docs/GUARANTEES.md) | guarantee vocabulary and what is actually implemented in v0 |
| [docs/FAILURE_MODEL.md](docs/FAILURE_MODEL.md) | failure scope, required scenarios, sender outcome rules, `Indeterminate` |
| [docs/INVARIANTS.md](docs/INVARIANTS.md) | the short invariant list every change is checked against |
| [docs/STORE.md](docs/STORE.md) | the store interface, specified ahead of the store |
| [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | phase tracker, development loop, agent rules, verified results, decisions, known debt |
| [docs/STATUS.md](docs/STATUS.md) | where the project stands, layer by layer |
| [docs/libraries/zmq.md](docs/libraries/zmq.md) | the ZeroMQ feature-parity table against libzmq 4.3.5, row by row, with [docs/libraries/README.md](docs/libraries/README.md) as the parity-document index |
| [Master Architecture and Implementation Plan — QUIC-native Messaging Framework.md](Master%20Architecture%20and%20Implementation%20Plan%20%E2%80%94%20QUIC-native%20Messaging%20Framework.md) | the normative architectural source of truth for the whole project |

## Crates

This repository holds **two kinds of product**: weida itself, and standalone implementations
of foreign protocols that are usable with no weida in the picture, with a Python binding
beside each finished library. A user may compose those public APIs explicitly. The workspace
does not define a general translation from foreign socket types onto weida patterns.
The `kind` column says which product a crate belongs to
([decisions/0013](docs/decisions/0013-competitor-libraries.md) §4). The table lists **every
one of the twenty-eight `[workspace] members`** and is read off `cargo metadata`, not kept by
hand.

| Path | Package | kind | Responsibility |
| --- | --- | --- | --- |
| `crates/core` | `weida-core` | weida | I/O-free model: errors, endpoint addresses, limits, trace context |
| `crates/protocol` | `weida-protocol` | weida | wire codec, no I/O: varints, framing, CBOR headers, negotiation, error codes |
| `crates/runtime` | `weida-runtime` | weida | the reactor and the OS plumbing, with no protocol in it: tasks, timers, DNS with a capped resolver, the three reactor-ownership constructors, a bounded close budget, an in-process name registry, `AF_UNIX` bind hygiene with peer credentials and, on Windows, named-pipe hygiene with the client's SID |
| `crates/winpipe` | `weida-winpipe` | weida | the Win32 calls a named pipe needs and nothing else — an owner-only DACL, the client's token SID, the pipe's owner SID, both process ids — behind a safe surface; the one crate that may use `unsafe`, and empty off Windows |
| `crates/weida` | `weida` | weida | runtime, the QUIC, in-process, `AF_UNIX` and named-pipe transports, the raw stream core, all six patterns — Req/Rep, Push/Pull, Pub/Sub, PAIR, SURVEY, BUS — and the cursor back channel |
| `crates/broker` | `weida-broker` | weida | L2, and a layer rather than a fork: queues at endpoint paths, the publisher confirm, the absolute per-subscription credit and consumer settlement with requeue; depends only on `weida`, and nothing depends on it ([0018](docs/decisions/0018-minimal-broker.md) §4.1) |
| `crates/raft/weida-raft` | `weida-raft` | weida | openraft plus the I/O it deliberately does not have — weida's transport under it — so a Raft service supplies only its state machine and log store ([0021](docs/decisions/0021-consensus-openraft.md)) |
| `crates/openbao/weida-openbao` | `weida-openbao` | weida | OpenBao/Vault for weida: token, AppRole and wrapped hand-off authentication, and identity sources signed by a PKI mount, anchored on its CA, or read from KV; its own crate because it carries the HTTP client and JSON the core does not ([0032](docs/decisions/0032-identity-sources-and-the-handoff.md) §4.3, [docs/libraries/weida-openbao.md](docs/libraries/weida-openbao.md)) |
| `crates/testkit` | `weida-testkit` | test support | waits for tests that report instead of killing: an expected time, a "still waiting" line at each multiple of it, and a hard limit only on a paused clock, where reaching it means a hang; a dev-dependency named by path, never published |
| `crates/py/weida-py-core` | `weida-py-core` | foundation | the shared PyO3 foundation under every binding, with nothing protocol-specific in it: errno exception families, the asyncio bridge that drives a Rust future on the caller's loop, and the bytes boundary ([0014](docs/decisions/0014-parallel-libraries.md) §2) |
| `crates/zmq/weida-zmtp` | `weida-zmtp` | codec | ZMTP 3.1 — greeting, framing, commands, metadata — with no I/O and no dependency on weida at all |
| `crates/zmq/weida-zmq` | `weida-zmq` | library | the ZeroMQ implementation: every socket type of `zmq_socket(3)` bar `ZMQ_STREAM`, `tcp`/`ipc`/`inproc`, NULL/PLAIN/CURVE with ZAP, the option table, the monitor and the devices, interop-tested against libzmq 4.3.5 in both roles ([docs/libraries/zmq.md](docs/libraries/zmq.md)) |
| `crates/zmq/weida-zmq-py` | `weida-zmq-py` | binding | the Python ZeroMQ surface: all eleven socket types, the three transports, the option table, PLAIN/CURVE/ZAP with a handler in Python, and a synchronous surface beside the asyncio one ([docs/libraries/zmq-py.md](docs/libraries/zmq-py.md)) |
| `crates/nng/weida-sp` | `weida-sp` | codec | nanomsg/NNG Scalability Protocols — the eight-octet header, the 64-bit framing, the REQ/REP tag stacks — with no I/O and no dependency on weida |
| `crates/nng/weida-nng` | `weida-nng` | library | the SP implementation: one socket type per protocol, endpoints and pipes, the option table, TLS and IPC credentials, interop-tested against NNG 1.4.0-rc.0 in both roles ([docs/libraries/nng.md](docs/libraries/nng.md)) |
| `crates/nng/weida-nng-py` | `weida-nng-py` | binding | the Python SP surface: one class per protocol, contexts as objects, asyncio and synchronous |
| `crates/mqtt/weida-mqtt-codec` | `weida-mqtt-codec` | codec | MQTT 5.0 — the fixed header, Variable Byte Integers, the property framework and every control packet — sans-I/O |
| `crates/mqtt/weida-mqtt` | `weida-mqtt` | library | the MQTT 5.0 **client**: connection, session, both QoS machines, subscriptions, retain and Will, aliases, TLS and AUTH, interop-tested against `rumqttd` 0.20.0 and `rmqtt` 0.23.1 ([docs/libraries/mqtt.md](docs/libraries/mqtt.md)) |
| `crates/mqtt/weida-mqtt-py` | `weida-mqtt-py` | binding | the Python MQTT surface: publish, subscribe and an iterator of deliveries, 51 exception classes, asyncio and synchronous |
| `crates/amqp/weida-amqp-codec` | `weida-amqp-codec` | codec | AMQP 1.0 — the Part 1 type system, frames, performatives and message sections — sans-I/O |
| `crates/amqp/weida-amqp` | `weida-amqp` | library | the AMQP 1.0 client: connection, session and link with both credit schemes, the settle modes, dispositions and SASL, interop-tested against `fe2o3-amqp` 0.17.0 in both roles ([docs/libraries/amqp.md](docs/libraries/amqp.md)) |
| `crates/amqp/weida-amqp-py` | `weida-amqp-py` | binding | the Python AMQP surface: connection, session and link as objects, `send` returning the delivery's terminal state, asyncio and synchronous |
| `crates/nats/weida-nats-codec` | `weida-nats-codec` | codec | the NATS client protocol — twelve control-line verbs and the `NATS/1.0` header block — sans-I/O |
| `crates/nats/weida-nats` | `weida-nats` | library | the Core NATS client: subjects and wildcards, queue groups, request-reply over an inbox, five credential forms and TLS; its interop tests are written and **not run**, because they need a `nats-server` ([docs/libraries/nats.md](docs/libraries/nats.md)) |
| `crates/nats/weida-nats-py` | `weida-nats-py` | binding | the Python NATS surface: publish with headers, subscriptions as iterators, queue groups, `request` with a mandatory timeout, asyncio and synchronous |
| `crates/http/weida-http-codec` | `weida-http-codec` | codec | HTTP/1.1 heads and chunked bodies, HPACK with Huffman, HTTP/2 frames, QUIC varints, static-table QPACK and HTTP/3 frames — sans-I/O, every cap an argument |
| `crates/http/weida-http` | `weida-http` | library | the HTTP client and pull-style server: HTTP/1.1, HTTP/2 over TLS by ALPN, HTTP/3 over QUIC (feature `h3`), per-origin pooling, interop-tested against hyper 1.11.1 and h3 0.0.8 in both roles ([docs/libraries/http.md](docs/libraries/http.md)) |

## Quick start, without writing a program

```
$ cargo run -q -p weida --bin weida -- serve --echo 'weida://127.0.0.1:7443/echo'
weida://sha256:22ed30a8…9f25@127.0.0.1:7443/echo
```

```
$ printf 'hello weida' | cargo run -q -p weida --bin weida -- request 'weida://sha256:22ed30a8…9f25@127.0.0.1:7443/echo'
hello weida
```

Four verbs: `serve` (with `--echo`, `--sink` or `--pub`), `request`, `send` and `sub`. The
payload is on stdin and stdout and nothing else is, so `weida request … | sha256sum` means
what it looks like; addresses, receipts and topics go to stderr. The address `serve` printed
is the whole client configuration, exactly as in the section below. Every transport works
the same way — `weida+unix://%2Ftmp%2Fs.sock/echo` for a socket, `weida+pipe://NAME/echo` for
a Windows pipe — and the exit code names the error a script can act on: `3` refused, `4`
unknown endpoint, `5` no reply, `6` untrusted, `7` connection lost. `weida --help` lists every
option, including `sub --framing line|raw|nul|length`, because a newline after each payload is
readable and is not framing.

## Identity in one line

A weida peer is its public key. The server generates an identity and prints an address that
names it; the client trusts that address and nothing else:

```
$ cargo run -p weida --example transform_server -- --bind 127.0.0.1:7443
weida://sha256:22ed30a8…9f25@127.0.0.1:7443/transform
weida://sha256:22ed30a8…9f25@127.0.0.1:7443/echo
```

```
$ printf 'hello weida' | cargo run -p weida --example transform_client -- 'weida://sha256:22ed30a8…9f25@127.0.0.1:7443/transform'
HELLO WEIDA
```

No certificate file changes hands, no CA exists, and a peer with any other key is refused with
`Untrusted(sha256:…)` naming the key that answered. `--identity PATH` keeps the server's key
across restarts so the address stays stable; `--cert-out PATH` plus `transform_client --ca
PATH` is the same exchange through a trusted certificate and a plain address instead. In
code: `Identity::generate()`, `Trust::by_address()` / `Trust::pin(fp)` / `Trust::anchor(pem)`,
`ServerTls::require_client(trust)` for mutual identity or
`ServerTls::require_client(ClientTrust::AnyKey)` for clients whose key is their only identity,
and `IncomingMeta::peer` to see who sent what.

Expected on stderr: `delivered` — QUIC's transport receipt for the request, not an application
acknowledgement — and a trace id that also appears in the server's log line for the request.

## Other patterns

Push/Pull and Pub/Sub each have a self-contained example: both halves run in one
process on loopback, so there is nothing to configure.

```
cargo run -p weida --example push_pull   # fire-and-forget, and one transport receipt
cargo run -p weida --example pub_sub     # prefix-filtered topics, two subscribers
```

Messages arrive out of order in both. Each transfer is its own QUIC stream, so ordering
is `None` for these patterns — see [docs/PATTERNS.md](docs/PATTERNS.md) §1.7.

## No reactor of your own

weida does not require you to be inside `#[tokio::main]`. `quinn` needs a Tokio reactor and
nothing else in the library does, so `Runtime::owned` creates and owns one — one worker
thread by default — and hands back ordinary futures that any executor can drive. Transfers
implement both the `tokio::io` and the `futures-io` trait pairs for the same reason.

```
cargo run -p weida --example owned_runtime   # a full exchange from a plain `fn main`
```

`Runtime::new` takes the ambient reactor and fails with `Error::Runtime` when there is none;
`Runtime::with_handle` takes somebody else's. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) §5.

## Build and test

```
cargo test --workspace
cargo test -p weida --test it streams::          # QUIC stream mechanics, measured
cargo test -p weida --test it transports::       # one pattern suite over QUIC, inproc and AF_UNIX
cargo test -p weida --test it identity::         # pins, anchors, addresses, client identity
cargo test -p weida --test large -- --ignored     # 1 GiB echo, asserts bounded peak RSS
cargo test -p weida-zmtp                         # ZMTP golden vectors and hostile input
cargo test -p weida-zmq                         # native ZeroMQ sockets, devices and interop
cargo bench                                      # codec and loopback QUIC throughput
cargo test -p weida-nng                         # native SP sockets and protocol semantics
```

## The website

[weida.doodleshnookie.net](https://weida.doodleshnookie.net) is this document set, rendered.
It is built in a separate repository that imports `README.md` and `docs/` at build time and
changes nothing in them but links and code-fence labels, so every sentence on a reference page
is a sentence in this tree; the landing page is the only page written there, and it quotes
this file, `cargo metadata` and crates.io
([decisions/0033](docs/decisions/0033-the-website-moves-out.md)). The loop's
bookkeeping, third parties' requirements, the research sheets and the master plan are not
published there; links to them lead into this repository.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option. This is the Rust ecosystem's own form: the MIT arm is the permissive minimum,
and the Apache-2.0 arm carries an explicit patent grant.

Unless you state otherwise, any contribution intentionally submitted for inclusion in this
work, as defined in the Apache-2.0 license, shall be dual-licensed as above, with no
additional terms or conditions.
