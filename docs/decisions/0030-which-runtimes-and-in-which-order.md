# 0030 — Which runtimes Phase C reaches, and in which order

- **Status:** provisional; superseded in part by [0045](0045-the-c-abi-is-the-boundary.md) (§3.1, §3.4, §5)
- **Date:** 2026-09-15
- **Items:** B-264; orders B-266 and the unfiled Node, Erlang and C rows
- **Supersedes:** the ordering clause of [LOOP.md](../LOOP.md) §9 ("Python first, then Java,
  then Node")
- **Related:** [0014](0014-parallel-libraries.md) §2, §3; [0019](0019-jvm-binding.md);
  [0013](0013-competitor-libraries.md) §4.2, §4.4; [0026](0026-the-guide-and-the-c8b-question.md)
  §0, §1; [libraries/weida-py.md](../libraries/weida-py.md); [GUIDE.md](../GUIDE.md) §0.1

## 1. The question

[LOOP.md](../LOOP.md) §9 has said this since the first week:

> Python (PyO3 plus maturin) first, then Java, then Node.

Nothing justified it. [0019](0019-jvm-binding.md) answered *how* a JVM binding would be built —
JNI rather than Panama, against an LTS floor a user may be pinned to — and explicitly not
whether the JVM is the next row worth its cost; its own §"The Node row" says "N-API against WASM
is a different question with different constraints, and nothing here is a precedent for it".
So the order was an assertion, and the one thing this project has been strict about is that an
assertion with a number attached to it belongs in a note.

The owner reordered it in conversation and named the yardstick with it: **reach**, in
[0026](0026-the-guide-and-the-c8b-question.md)'s sense. Not "which language is popular" but
*how many runtimes can call this, and through what substrate*. This note records the order, the
reason for each position, and what each position costs.

## 2. The evidence, condensed

**What a row costs, measured on the one that is finished.** The Python row is
`weida-py-core` — three things and no fourth: an errno-to-exception family, a bridge from a Rust
future to the caller's own loop, and a bytes boundary with the copies stated where they happen —
plus one binding per library and one parity document each. Five bindings ride it today
(`weida-zmq-py`, `weida-nng-py`, `weida-mqtt-py`, `weida-nats-py`, `weida-py`), the last of them
56 names and 32 tests wide, and **the foundation did not change to admit any of them**. That is
the evidence that a per-language foundation is the right unit of work, and it is why this note
orders *rows* rather than languages.

**What the bridge costs, per runtime, is the part that differs.** The Python bridge is the
expensive one: the library owns its reactor, the caller owns an event loop, "neither may drive
the other" ([0019](0019-jvm-binding.md) §2 quoting `bridge.rs`), so every completion goes
through `loop.call_soon_threadsafe` — one loop wakeup per `await`, which
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §4 measures as the **~78 µs per operation** that
dominates this binding's cost. Every target's position below is mostly a statement about how
much that machinery it needs.

**What this project is not.** There is no community; every binding here is written by this
project. That single fact is what demotes the C ABI: a C ABI's value is mostly that *other
people* bind it, and where nobody else does, a Rust-native binding is cheaper to build and
cheaper to keep in step — one language's idioms instead of two boundaries.

**What is installed on the machine this is built on** (2026-09-15): Node 26.8, npm, Bun 1.4.0,
Deno 2.9.6, Erlang/OTP 29 (erts 17.0.6, JIT), Elixir 1.20.2, rebar3 3.27. No JDK, no Go, no
.NET, no Zig, no `cbindgen`.

## 3. The order, and why each position is where it is

**3.1 Python — done.** PyO3 plus maturin, one `abi3` wheel from CPython 3.9, five bindings, five
parity documents. It was the right first row for a reason worth keeping: it is the ecosystem
where a *client* is most often the whole program, so the binding's surface is the product rather
than a component of one. Nothing here reopens it.

**3.2 Node — next, and it is one row for three runtimes.** N-API is a **stable ABI across Node
versions**, so one prebuilt addon per platform serves every release, distributed through npm's
`optionalDependencies` the way every native package does. Bun and Deno both implement Node-API,
so the same artifact reaches all three — which is why "Node, Bun and Deno" is not three rows,
and also why each of them needs its own smoke test rather than an assumption: Node-API coverage
outside Node is a claim to verify per runtime, exactly as `package.sh` verifies the wheel rather
than trusting maturin. The bridge is a solved shape (`napi-rs` turns a Rust future into a
`Promise` over a threadsafe function), the bytes boundary is `Buffer`/`Uint8Array`, and the
handle model is a JS object holding a `#[napi]` struct with the GC as its `Cleaner`.

**3.3 Erlang and Elixir — third, and the cheapest bridge of any target.** The machinery §2 calls
expensive largely **disappears** on the BEAM, and that is not a preference but a property of the
runtime:

- `enif_send` delivers a completion **into a process mailbox**, so `receive {weida, Ref, {:data,
  Bin}}` is the idiomatic shape *and* the mechanism. No `call_soon_threadsafe`, no
  `CompletableFuture`, no loop wakeup per await — the reactor writes to a mailbox and the
  scheduler does the rest.
- `ResourceArc` is a handle table with GC-driven cleanup, so the generation-tagged table and
  `Cleaner` backstop [0019](0019-jvm-binding.md) §4.2 needs for the JVM are simply not needed.
- Binaries are reference-counted and immutable, so a payload crossing in either direction is
  near-zero-copy — better than CPython, where the limited API forces one copy outbound
  ([libraries/weida-py.md](../libraries/weida-py.md) §8).
- Dirty schedulers are needed only where a NIF blocks, and none of them has to: the async core
  plus mailbox delivery lets every NIF return immediately.

It is third rather than second only because Node's reach is wider; on cost alone it would be
first.

**3.4 A C ABI — fourth, and it is the blocking facade with opaque handles.** `extern "C"` gives
the calling convention for free and nothing else: generics, traits, `impl Into<ClientTls>`,
`Endpoint<Pattern>`, `Result` and `async` all have to go. What remains is a shape this workspace
already has — `weida::blocking`, which since B-244 covers all six patterns and since B-243 the
cursor surface — plus one opaque handle per role, an explicit free per handle, `weida_errno()`
and `weida_strerror()` in ZeroMQ's shape, caller-owned buffers with lengths (which weida's
mandatory receive ceiling already matches one-to-one), `catch_unwind` at every boundary because
unwinding across FFI is undefined behaviour, and `cbindgen` for the header. What it does **not**
adopt is ZeroMQ's polymorphic socket: a type that answers "what does `send` mean" with a runtime
enum is the thing weida deliberately does not have.

It is fourth because its consumers are the targets with **no Rust-native path** — Go, .NET, C,
Zig, Lua — and none of them is asked for yet. A row that exists to be bound by people who are
not here yet is a row that waits.

**3.5 Java — last, and the reason is written down rather than implied.** The reach is real:
enterprise messaging is where JMS, Kafka and AMQP clients live, and all four competitor
libraries here have a JVM audience. Against it, three costs that do not shrink: JNI boilerplate
with a handle table and a `Cleaner` backstop ([0019](0019-jvm-binding.md) §4.2); the heaviest
release story of any row — Maven Central with signing keys and per-platform natives in one jar,
against `maturin build` plus a wheel; and the one that decides the position, **nobody here reads
Java**. A binding this project cannot maintain is worse than a binding this project does not
have, because the absence is honest and the rot is not. 0019 stays as the design it is, provisional
and unbuilt, and gains a pointer here.

## 4. What follows from it

**4.1 Each row is filed as its own items when it starts**, in 0014 §3's shape: a foundation, one
binding per library it serves, one parity document each. This note orders rows; it does not file
them.

**4.2 The foundation rule is unchanged and applies to every row.** `weida`, `weida-protocol`
and every protocol library stay **absent** from a foundation's dependencies
([0014](0014-parallel-libraries.md) §2): a foundation carries error mapping, the bridge and the
bytes boundary, and no protocol. A row that re-derives protocol behaviour is the second
implementation [0013](0013-competitor-libraries.md) §4.4 exists to prevent.

**4.3 Async first, then a synchronous surface**, per 0014 §2 — except where a runtime makes the
distinction meaningless. The BEAM is that case: a mailbox *is* the asynchronous surface and a
receive with a timeout *is* the synchronous one, so the row does not need two.

**4.4 Every row proves its own artifact rather than trusting its packager.** The Python row's
`package.sh` builds a release wheel, checks its name contains `abi3`, installs it into a fresh
virtualenv with `PATH=/usr/bin:/bin` and runs `smoke.py`. Each row owes the equivalent: an npm
pack installed into an empty project and exercised under Node, Bun **and** Deno; a hex package
and an `escript` or `mix` project for the BEAM; a `weida.h` compiled by a C file that links only
the library.

## 5. What this does not decide

**Nothing about WASM or the browser.** weida is QUIC over UDP and a browser cannot open a UDP
socket; WebTransport is HTTP/3-based and would be an **adapter** with its own framing question,
not a binding. It is not a fifth position on this list.

**Nothing about Erlang distribution.** Carrying `-proto_dist` over weida is a protocol adapter
rather than a binding, filed as B-265 and parked: the binding of §3.3 and that adapter share a
toolchain and nothing else.

**Nothing about Kotlin, Scala, Clojure, Android or GraalVM**, which 0019 already lists as
consumers of the Java row's bytecode rather than rows of their own. The same reading applies to
Elixir against §3.3: one BEAM row, several languages.

**Whether a second Python client exists for PyPy.** `abi3` is CPython's, so PyPy needs `cffi`
over §3.4's C ABI — which makes it a consumer of the fourth position and not a row. B-266
answers it with the measurement that decides it.
