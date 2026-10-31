# 0045 — The C ABI is weida's language boundary

- **Status:** accepted
- **Date:** 2026-10-31
- **Supersedes:** [0030](0030-which-runtimes-and-in-which-order.md) §3.1, §3.4 and the last
  paragraph of §5; [0014](0014-parallel-libraries.md) §2's one-foundation rule, for weida's
  binding only
- **Related:** [0019](0019-jvm-binding.md); [IMPLEMENTATION.md](../IMPLEMENTATION.md) §4;
  [c/README.md](../c/README.md); [libraries/weida-c.md](../libraries/weida-c.md);
  [libraries/weida-py.md](../libraries/weida-py.md)

## 1. The question

weida reached Python through PyO3 (`crates/py/weida-py` on `weida-py-core`), and every further
language would have needed its own Rust-native foundation in the same shape. The owner asked
for the opposite: one C ABI, designed as deliberately as `zmq.h`, as the single boundary every
non-Rust language crosses, with Python moved onto it through cffi so that PyPy can load it too.
And while weida is in alpha, the things weida carried only for the PyO3 binding and its blocking
facade go, with no compatibility shims.

So the question is: **what does that ABI look like, and what goes when it arrives.**

## 2. The evidence

- [0030](0030-which-runtimes-and-in-which-order.md) §3.4 already sketched the C ABI as "the
  blocking facade with opaque handles" and placed it fourth because nobody outside this project
  was asking for it. §2 of the same note records why a per-language Rust foundation was
  preferred: every binding is written here, and one boundary per language was cheaper than two.
  That argument assumed one language per foundation; PyPy, Go, .NET, Zig, Lua and the JVM are
  each one more foundation, and a C ABI is one for all of them.
- The PyO3 binding's two surfaces cost **~13 µs** (sync) and **~78 µs** (asyncio) per operation
  over Rust ([IMPLEMENTATION.md](../IMPLEMENTATION.md) §4). The sync cost is a thread handoff:
  the caller parks in `futures::executor::block_on` while a reactor worker completes the future.
  The asyncio cost is one `loop.call_soon_threadsafe` wakeup per `await`.
- Signals: `weida-py-core/src/interrupt.rs` waits in **100 ms slices** on the main thread
  (`SLICE`), because the reactor threads may receive a process signal and the waiting thread
  would never see it. That is a latency bound on Ctrl-C and a wakeup ten times a second for
  every blocked call. ZeroMQ answers the same problem by blocking signals on its I/O threads, so
  a caller blocked in `poll` returns `EINTR`.
- Items weida carries only because of the binding and its blocking facade:
  - `crates/weida/src/blocking.rs`, the blocking facade over every pattern;
  - `Cursors::changed_within` and `weida::Reported`, added by `9bbebcc feat(py): the cursor
    surface on both surfaces` for the sync surface;
  - `RuntimeConfig::receive_timeout`, whose own doc (`config.rs:229-240`) names its users:
    "the blocking receives of `weida::blocking` and `weida serve`";
  - the `blocking` feature and its optional `futures` dependency, and the CI feature rows
    that build it.
- `AsyncRead for RecvHalf` and `AsyncWrite for SendHalf` (`transport.rs:661-763`) report a
  peer's `CANCELED` or `REJECTED` as an opaque `io::Error` (quinn's `ReadError` inside, or
  `"stream reset by the peer"` on inproc); only `read_capped` maps the outcome. A C `read` that
  polls those traits needs the outcome.

## 3. The options

**A. Keep PyO3 and add a C ABI beside it.** Loses: two boundaries to keep in step for weida's
own surface, and the binding-only items in §2 stay. PyPy still needs the C ABI.

**B. A C ABI over the blocking facade** (0030 §3.4's sketch). Loses: async callers. A
blocking-only ABI forces every event loop into a thread per wait, which is the 78 µs again plus
a thread, and it keeps `weida::blocking` alive as the substrate.

**C. A C ABI with callbacks into foreign code.** Loses: thread safety at the boundary. A
callback runs on a reactor thread, which in Python means taking the GIL there and in every
other runtime means a thread the runtime does not own; libraries that do this (libuv bindings,
gRPC core) carry a dispatch layer per language to undo it.

**D. A readiness-based C ABI, as ZeroMQ's.** Every handle that can become ready exposes one
waitable descriptor; tries run on the caller's thread with a timeout; long operations are
spawned on the reactor as typed pending handles with a descriptor of their own. Any event loop
integrates by watching descriptors, and blocking callers wait in `poll`. Loses: one descriptor
pair per waitable handle, created lazily, and a waiting call costs a `poll` plus a byte
through a socket pair.

## 4. The decision

**D, and weida's Python binding moves onto it.** The owner fixed these points:

1. **Location and name.** `crates/weida-c`, package `weida-c`, `publish = false`; its lib target
   is named `weida` (`cdylib` and `rlib`), so the library is `libweida.so`, `libweida.dylib`,
   `weida.dll`, linked with `-lweida`; the header is `weida.h`. Inside it the Rust crate is
   `weida_rs`. Accepted cost: on Windows debug builds the CLI's `weida.pdb` and the DLL's collide,
   which cargo reports as a warning.
2. **Readiness-based async, as in ZeroMQ.** One waitable descriptor per handle that can become
   ready (a Unix fd, a socket on Windows). No callbacks into foreign code.
3. **Handles are typed per role.** No polymorphic socket.
4. **The final scope is all of weida**; the first stage is a vertical slice, and
   [libraries/weida-c.md](../libraries/weida-c.md) lists the rest as absent.
5. **Python uses cffi in out-of-line ABI mode** (`dlopen`): one `py3-none-<platform>` wheel per
   platform.
6. **The Python API mirrors the C ABI.** It is not compatible with the old PyO3 module.
7. **Errors.** Every fallible function returns an `int` status; details of the last failure are
   read from thread-local accessors.
8. **Received memory.** A collected payload is a library-owned `weida_msg`; streamed reads go
   into caller buffers.
9. **Configuration.** Opaque builders with one setter per field.
10. **Inverted control is a queue.** Events, resolver lookups and reports are read from handles
    like messages. No function pointers.
11. **Header.** Generated by cbindgen, committed, and checked for staleness in CI. Normative
    prose lives in [docs/c/](../c/README.md).
12. **Thread safety.** Every handle is usable from any thread concurrently.
13. **Interruption is EINTR, as in ZeroMQ.** No operation sends twice on a retry.
14. **Long operations are typed pending handles.**
15. **Timeouts are a per-call `int64_t timeout_ms`.**
16. **`weida_poll` exists.**
17. **Python has both surfaces.** Sync in `weida`, asyncio in `weida.asyncio`, sharing every
    value class.
18. **The PyO3 `weida-py` is removed first.**
19. **This stage's extent.** Rules, spec and the vertical slice: runtime, configuration, TLS
    values, Req/Rep with streamed bodies, delivery receipts, readiness descriptors and
    `weida_poll`.
20. **`weida::blocking` is deleted**, and with it `Cursors::changed_within`, `weida::Reported`,
    `RuntimeConfig::receive_timeout`, the `blocking` feature, the optional `futures` dependency
    and the CI feature rows.
21. **Rust weida stays executor-agnostic.** The `futures-io` impls, `tests/foreign_executor.rs`
    and `examples/owned_runtime.rs` stay; the C ABI relies on it, since it polls queue takes and
    stream reads and writes on the caller's thread.
22. **The C boundary is stream-first, like Rust.** `accept` and `request` hand over a transfer
    as soon as its header arrives; a body is read like a socket or collected whole with a
    per-call ceiling; a reply is written like a socket and finished into a delivery receipt.
23. **The library is `libweida`** (point 1).

The ABI's rules R1 to R13 (names, returns, the async mapping, timeouts, handles and threads,
consuming calls, values, messages, errors, panics, readiness, signals, runtime lifetime) are
normative and live in [c/README.md](../c/README.md).

Two prerequisite fixes in weida come with it: stream errors keep weida's outcome through
`AsyncRead` and `AsyncWrite` (`From<Error> for io::Error` and back), and an owned reactor's
threads block every signal except the synchronous ones, so a process signal reaches a caller
thread (R12).

**What this supersedes.** 0030 §3.1 (Python is no longer a finished PyO3 row), §3.4 (the C ABI
is no longer fourth and no longer the blocking facade) and the last paragraph of §5 (PyPy is
answered: cffi over this ABI). [0014](0014-parallel-libraries.md) §2's rule that every Python
binding sits on `weida-py-core` no longer holds for weida's own binding; the foreign-protocol
bindings keep it.

**For [0019](0019-jvm-binding.md):** a JVM row would bind this ABI rather than a JNI foundation
of its own.

## 5. Follow-ups

Each is a later plan under the same rules; [libraries/weida-c.md](../libraries/weida-c.md)
tracks them as absent rows by these area names.

- **patterns:** every other pattern (Push/Pull, Pub/Sub, Radio/Dish, segments, datagram flows).
- **streams:** `Requester::open` and `ReplyStream`.
- **cursors:** the cursor surface, as queues.
- **flows:** datagram flows.
- **raw peers:** the raw `Peer` and `Acceptor`.
- **swarm:** SWARM.
- **identity:** identity and trust sources, their events as queues.
- **resolver:** the resolver as a queue.
- **config:** the remaining `RuntimeConfig` setters and `TransferMeta` setters.
- **local transports:** the unix and pipe bindings.
- **binding lifecycle:** `Binding::close`, `restart`, `hand_over`, `take_over`.
- **stats:** connection statistics.
- **drain:** `Runtime::drain`.
- **refusals:** `refuse` and `canceled` on incoming transfers.
