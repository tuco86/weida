# 0014 — Four parallel library workstreams, and bindings that follow each library

- **Status:** accepted; §2's foundation rule superseded for weida's binding by [0045](0045-the-c-abi-is-the-boundary.md)
- **Date:** 2026-09-12
- **Related:** [0013](0013-competitor-libraries.md) §4.1, §4.4;
  [LOOP.md](../LOOP.md) §9 Phase B, Phase C, Phase D;
  [research/nanomsg-nng.md](../research/nanomsg-nng.md); [research/mqtt5.md](../research/mqtt5.md);
  [research/amqp10.md](../research/amqp10.md); [research/nats.md](../research/nats.md)

## 1. The question

[0013](0013-competitor-libraries.md) settled *what* a foreign implementation is — a
standalone library in this repository, sharing `weida-runtime` and none of weida's protocol —
and proved the shape on ZeroMQ. What it did not settle is how many implementations run at
once. Nothing makes NNG, MQTT, AMQP or NATS depend on another foreign library finishing: they
share `weida-runtime` and nothing else. The Python binding was parked behind “Phase C waits for
B3” when Phase C meant one binding of one library; with several libraries it is several
bindings, each blocked only by its own library.

## 2. The decision

**Four workstreams, run in parallel, each its own library in 0013's shape, each with its own
worktree.** The user decided this; this note records it and the rules that keep the four from
colliding.

| # | Products | Interop peer |
| --- | --- | --- |
| W1 | `weida-zmq-py` — the Python binding of the finished ZeroMQ library, asyncio surface first, then a sync surface over the `blocking` facade of [0013](0013-competitor-libraries.md) §4.4 item 3 | libzmq through `pyzmq`, plus the Rust interop bench of B-088/B-089 |
| W2 | `weida-nng` — the NNG/SP library on the existing `weida-sp` codec, then `weida-nng-py` | the `nng` crate over the vendored NNG C library [nanomsg-nng §13] |
| W3 | `weida-mqtt` — the MQTT 5 **client** library on a new sans-I/O `weida-mqtt-codec`, then `weida-mqtt-py` | `rumqttd` (pure Rust) for what it speaks, a 5.0 broker behind `#[ignore]` for the rest [mqtt5 §13] |
| W4 | `weida-amqp` (AMQP 1.0 client) and `weida-nats` (NATS core client), two smaller libraries in one workstream, then their bindings | `fe2o3-amqp` and RabbitMQ 4.x [amqp10 §13]; `nats-server` through the supervisor, else an `async-nats` peer [nats §13] |

Crate layout extends [0013](0013-competitor-libraries.md) §4.1's rule — one directory per
protocol family, named for the family and not for the role: `crates/nng/weida-nng`,
`crates/mqtt/{weida-mqtt-codec, weida-mqtt}`, `crates/amqp/{weida-amqp-codec, weida-amqp}`,
`crates/nats/{weida-nats-codec, weida-nats}`, and every Python binding beside the library it
binds (`crates/zmq/weida-zmq-py`, `crates/nng/weida-nng-py`, and so on). Every codec keeps the
empty `[dependencies]` of [0013](0013-competitor-libraries.md) §4.3, which is what makes it
checkable against the foreign specification rather than against our reading of it.

**Phase C is unparked, and B-058 with it.** "Phase C waits for B3" was a statement about
scarcity, not about dependency: Phase C's stated prerequisite is A8's runtime ownership, which
landed as B-016, and the ZeroMQ library it would bind has been finished since B-095. The rule
that replaces it is narrower and mechanical: **a binding follows its own library and nothing
else — asyncio first, then sync.** Asyncio first because the libraries are async underneath and
the sync surface is a facade over `Context::owned`
([0013](0013-competitor-libraries.md) §4.4 item 3), so building sync first would mean building
the facade twice.

**One shared PyO3 foundation, and no second one.** `crates/py/weida-py-core` holds what every
binding needs and no protocol specifics: the error mapping from a library's errno-named enum to
a distinct Python exception class per variant, the `Runtime`/`Context` bridge that drives a
library future on the running asyncio loop and hands back an awaitable, cancellation from the
Python side onto the Rust future, and the `bytes` boundary that avoids a copy where PyO3 allows
it and measures the copy where it does not. Every binding depends on it; no binding re-derives
it. Its item is the prerequisite of the first W1 binding item, so W1 pays for it once and
W2-W4's bindings inherit it. `weida-py-core` MUST NOT depend on `weida`, on `weida-protocol` or
on any protocol library — it is the same dependency-direction rule
[0013](0013-competitor-libraries.md) §4.2 gave `weida-runtime`, one layer up.

**The broker boundary, stated once so it is not re-derived per protocol.** W3 and W4 build
**clients**. An MQTT server is a broker — sessions that outlive connections, retained-message
storage, shared-subscription dispatch — and the broker is Phase D
([LOOP.md](../LOOP.md) §9). The same line runs through AMQP: a client attaches links to a
peer's nodes, and AMQP 1.0 "defines no operation to create, configure, enumerate or delete a
node" [amqp10 §2], so there is no server to build from the core standard alone. NATS is a
client for the same reason: routes, gateways, leaf nodes and JetStream are server features
[nats §4]. A client library may hold the *client half* of a server-side concept where the
protocol defines it for a client — an MQTT session with its expiry, a shared subscription
joined with `$share/{name}/{filter}` — and that is not a broker.

**Each workstream has its own worktree, its own `CARGO_TARGET_DIR`, and never runs cargo in
this tree.** `../weida-w1` … `../weida-w4`, branch `b<id>-<slug>` per item, as
[LOOP.md](../LOOP.md) §1.4 already requires of subagents. The reason is mechanical rather than
stylistic: one cargo invocation per build directory at a time, so a second one blocks on the
lock until the first finishes, and a workstream that gated in the main tree would serialize the
other three behind it. The main tree runs the gate once, on merge, and it is the only place a
merge happens.

## 3. Consequences and follow-ups

1. [LOOP.md](../LOOP.md) §9 is amended on acceptance: Phase C's paragraph becomes "bindings
   follow each library, async then sync, per this note", and B2, B3 and B4 name their crates
   and say "the library per [0013](0013-competitor-libraries.md)".
2. The four workstreams are filed as backlog items in 0013 §5.3's template — kind, size,
   `needs` by id, a one-line acceptance and a note naming the workstream, the worktree and the
   branch. B-058 is un-parked as the first W1 binding item and B-062 becomes the first W3 item.
3. **What the sheets could not answer, recorded rather than guessed.** `rumqttd` 0.20.0 is
   MQTT **3.1.1 only** — its own workspace checklist leaves MQTT 5 unchecked [mqtt5 §13] — so
   it can be the interop peer for what both speak and cannot exercise properties, reason codes,
   session expiry or shared subscriptions. Those need a 5.0 broker, which on this machine means
   Mosquitto behind `#[ignore]` with its install command ([LOOP.md](../LOOP.md) §2). The
   backlog files that as two items rather than one honest-looking one.
4. General foreign-protocol forwarders and protocol-wide mapping documents are not products.
   Applications may compose public APIs explicitly. Broker integration is later Phase D work:
   a managed Connector resource names one concrete source or sink, Queue and conversion policy
   ([0013](0013-competitor-libraries.md) §4.6).
5. Sizing is per standalone library and binding. Native interop, protocol inventories and
   parity documents determine completion; no bridge or cross-protocol slice is counted.
