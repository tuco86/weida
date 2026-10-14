# 0019 — How a JVM binding reaches these libraries

- **Status:** provisional
- **Date:** 2026-09-14
- **Related:** B-199; [0013](0013-competitor-libraries.md) §4.2, §4.4, §4.6, §4.7;
  [0014](0014-parallel-libraries.md) §2, §3; [LOOP.md](../LOOP.md) §2, §5, §9 Phase C;
  [STATUS.md](../STATUS.md) ("What that leaves"); [libraries/README.md](../libraries/README.md)
  ("Required sections"); [libraries/zmq-py.md](../libraries/zmq-py.md) §8, §9, §10;
  [libraries/mqtt-py.md](../libraries/mqtt-py.md) §8, §9.1, §9.2, §9.7, §10;
  [research/ipc.md](../research/ipc.md) §5; [research/zeromq.md](../research/zeromq.md) §13;
  [research/mqtt5.md](../research/mqtt5.md) §1, §13; [research/nats.md](../research/nats.md) §13.

Provisional, and **last in the order**
([0030](0030-which-runtimes-and-in-which-order.md) §3.5): this note answers *how* a JVM binding
would be built and never whether the JVM is the next row worth its cost. 0030 answers that with
the reason recorded — the reach is real, the maintenance is not, and nobody on this project
reads Java — so the design below stands unbuilt until that changes.

## 1. The question

B-199, verbatim in its acceptance line: "the decision note Phase C's second row needs
([LOOP.md](../LOOP.md) §9, [0014](0014-parallel-libraries.md) §2: 'Python first, then Java, then
Node')… **JNI or the Panama FFI**, against a stated MSRV-equivalent floor for the JVM (a Panama
binding needs JDK 22+, which excludes the LTS a user may be pinned to), what the `weida-py-core`
equivalent is for the JVM (error mapping, the reactor bridge, the `byte[]`/`ByteBuffer` boundary)
and whether it is one shared artifact as `weida-py-core` is, how a Java caller gets an executor
bridge without a second reactor, and how the binding is built and published (Gradle, Maven
coordinates) given that the Python row's answer was maturin plus a wheel" [BACKLOG B-199].

The Python row is finished and documented, so this is not a question about whether the shape
works; it is a question about which of its five answers survive a change of runtime.
[STATUS.md](../STATUS.md) states the position the note is written from: what remains is "**Phase
C's Java and Node rows**, untouched and unfiled", with the Java row explicitly offered "now that
the Python row is four bindings wide and `weida-py-core` has proved the shape a per-language
foundation takes" [STATUS §"What that leaves", and the closing paragraph].

## 2. The evidence, condensed

**What the Python foundation actually is, because the JVM one is defined against it.**
`crates/py/weida-py-core/src/lib.rs:18-41` names four things and no fifth: `Errno`/`ErrorFamily`
(one Python exception class per errno variant under one base, so "an errno vocabulary that
arrives as one class is an errno vocabulary a caller cannot branch on"), `Bridge` (a Rust future
turned into something the caller's *running* loop can await), cancellation propagated rather than
swallowed, and `bytes` (the boundary "with the one copy each direction forces stated where it
happens and measured"). Its dependency rule is the hard part: "**`weida`, `weida-protocol` and
every protocol library are absent and must stay absent**… the same dependency-direction rule 0013
§4.2 gave `weida-runtime`, one layer up" (`lib.rs:12-16`), which
[0014](0014-parallel-libraries.md) §2 states as "no binding re-derives it".

**The bridge's mechanism, which is the part that does not transfer.** `bridge.rs:5-23`: the
library owns its reactor (`weida_runtime::Exec`) and the caller owns an event loop, "neither may
drive the other", so the two are joined by `loop.create_future()` completed through
`loop.call_soon_threadsafe`, "which is asyncio's only thread-safe entry point". The reactor is
not replaced: `bridge.rs:30-40` refuses `pyo3-async-runtimes` because "it owns a Tokio runtime…
and asks the library to use *its* reactor", and "two runtimes in one process, one per binding, is
the cost of not saying so". Cancellation is a done-callback that aborts the Rust task, so the
Rust future "is **dropped** at its next suspension point" (`bridge.rs:42-51`); a panic is caught
and becomes an exception and a closed loop is a deliberate no-op, so neither is a hang
(`bridge.rs:53-65`); and the value crossing is `Send + 'static` "because it is produced on a
reactor thread after the Python call has already returned" (`bridge.rs:83-93`).

**The bytes boundary, and the refusal that sets the precedent.** Python to Rust needs no copy
where the binding only reads (`bytes.rs:10-18`); it costs exactly one where the library takes
ownership, "straight out of the interpreter's buffer into the `Vec` the library keeps. Not
twice" (`bytes.rs:20-28`); Rust to Python costs one, because "a CPython `bytes` object owns its
storage inside its own allocation and there is no API — limited or otherwise — that adopts a
foreign buffer" (`bytes.rs:29-37`). The refusal matters more than the copies: a `memoryview` over
Rust-owned bytes is **not** used, because the buffer protocol is outside the limited API before
CPython 3.11 and "a zero-copy path that only exists on some interpreters is a performance cliff,
not a feature" (`bytes.rs:29-37`).

**What a binding may and may not re-derive.** [0014](0014-parallel-libraries.md) §2: one shared
foundation that "no binding re-derives"; a binding "follows its own library and nothing else —
asyncio first, then sync", because the sync surface is a facade over the library's own blocking
feature and "building sync first would mean building the facade twice".
[0013](0013-competitor-libraries.md) §4.4 item 3 states it one layer down — the blocking facade is
"one wrapper per socket over a `Context::owned` reactor, and **no second implementation of
anything**" — §4.6 forbids the mirror image (no foreign protocol inside weida's primitives), and
§4.4 item 6 keeps identity types apart with "no `From`, `Into`, `AsRef` or `Deref` between the
two groups… anywhere in the workspace". Of §4.7's definition of done a binding inherits clause 4
(interop against the reference implementation, in both roles) and clause 6 (no row says "partial"
without saying what is missing).

**What a finished binding parity document contains, and the deviations a binding is allowed.**
[libraries/README.md](../libraries/README.md) fixes eleven sections and two rules: every row
carries its evidence, and there are no aggregate verdicts. The deviations are visible in the two
documents that have them. [zmq-py.md](../libraries/zmq-py.md) §9: one operation at a time per
socket with `split()` where the protocol permits it; *every* awaiting call a coroutine, including
ones the Rust library answers synchronously, because "a synchronous Python method that waited for
it would block the event-loop thread"; work starting at the first `await`, after the eager
version was "caught losing a message to a task cancelled before it ran"; seconds instead of
milliseconds; 19 classes instead of an errno number; and ceilings libzmq does not have.
[mqtt-py.md](../libraries/mqtt-py.md) §9.1 adds the strongest form — a capability may be **absent
by construction** (TLS, whose `rustls::ClientConfig` has no Python spelling) as long as the
document says so — and §9.2 that the two surfaces share every value class, differing only where
"an awaitable and a blocked thread are not the same object". §9.7 and §10 are the other half of
the discipline: the paho column is "**the documented public API of paho-mqtt 2.x, not a
measurement**", and the interop clause is recorded as **not met** rather than softened.

**How the Python row ships.** One `cdylib` per binding (`crates/zmq/weida-zmq-py/Cargo.toml:16`),
one `abi3` wheel covering "every CPython from 3.9 on" (root `Cargo.toml:109-113`), and
`package.sh:1-48`, which builds it, refuses a wheel whose name is not `abi3`, installs it into a
fresh temporary virtualenv and runs the smoke program with `PATH` scrubbed to `/usr/bin:/bin`, so
"the wheel needs no Rust toolchain" is "a fact this script establishes rather than an
expectation". Two workspace facts travel with it: `unsafe_code` is `allow` in exactly the binding
crates, with a test asserting no hand-written `unsafe` in `src`
(`crates/py/weida-py-core/Cargo.toml:33-39`, `crates/zmq/weida-zmq-py/Cargo.toml:53-54`), and one
PyO3 version is shared because "two PyO3 versions in one interpreter cannot share a type object"
(root `Cargo.toml:109-113`). The cost is measured: a REQ/REP round trip is 16.5 µs in Rust,
67.7 µs through the sync surface, 330 µs through asyncio and 5.7 µs through pyzmq over
`inproc://` [zmq-py §8].

**The JVM's own constraints.** This repository documents the JVM only as a foreign ecosystem —
JEP 380 for Unix-domain socket channels [ipc.md §5, source 152], Paho Java's reconnect backoff and
persistence contract [mqtt5 §1, §13, sources 39-41], `nats.java` as the official Java client
[nats §13], Proton-J and Qpid JMS [amqp10 §13] — so everything in this paragraph is primary
knowledge carried in with its JEP or JSR number, marked where it goes past the document's
headline.

- **JNI** is part of every JDK and needs no minimum beyond the one the binding compiles for
  [inference: JNI has shipped since JDK 1.1 and carries no JEP number of its own]. Build: the
  Rust side is a `cdylib` exporting `Java_<mangled package>_<method>` symbols, so the Java
  package name is part of the ABI and a class rename is a runtime link error. Safety: the
  unsafety is on the Rust side — `JNIEnv`, raw `jlong` handles, local and global references —
  where this workspace already has a net (`tests/no_handwritten_unsafe.rs`). Threads: one the JVM
  did not create must attach before calling in and detach, or attach as a daemon, before it exits
  [inference: `AttachCurrentThread`, `AttachCurrentThreadAsDaemon`, `DetachCurrentThread`].
- **Panama**, the Foreign Function & Memory API (`java.lang.foreign`), was finalized by **JEP 454
  in JDK 22**. Build: the Rust side is a plain `extern "C"` `cdylib` with no JVM knowledge and no
  `jni` dependency; the Java side describes each function with `Linker`, `FunctionDescriptor` and
  `MemorySegment`. Safety: the unsafety moves to Java, where this repository has no test net and
  no lint, and restricted methods must be unlocked per module [inference: `--enable-native-access`,
  tightened by JEP 472 in JDK 24, which prepares the same restriction for JNI]. Threads: a
  downcall needs no attachment; an upcall stub is how native calls back [inference:
  `Linker.upcallStub`; whether it attaches an unknown thread implicitly was not verified here].
  Minimum JDK: **22**.
- **The LTS situation is the constraint that decides it.** JDK **17** and **21** are long-term
  support releases and **25** is the newest one [inference: long-term support is a vendor
  designation with no JEP or JSR number; the release cadence itself is JEP 322]. A floor of 22 is
  therefore not "one version newer": it is *above two of the three LTS releases a deployment may
  be pinned to*, and a binding a user cannot load is not a binding.
- **Virtual threads** are final since **JEP 444 in JDK 21**. A virtual thread blocked on a
  `Future` unmounts its carrier; a virtual thread executing a **native frame is pinned** to its
  carrier for the duration [inference: JEP 444 names native frames among the pinning cases, and
  JEP 491 in JDK 24 removed the `synchronized` case but not this one].
- **`CompletableFuture`** is standard from Java 8 [inference: JSR 337], and `defaultExecutor()`
  plus `newIncompleteFuture()` — which let a subclass decide where *derived* stages run — arrived
  in Java 9 [inference: JEP 266]. `complete` is callable from any thread; a dependent stage added
  without an explicit executor runs **on whichever thread completed the future**.
- **`byte[]` and `ByteBuffer`.** A `byte[]` lives on the Java heap and may be moved by the
  collector, so JNI's copying accessors are the only unconditionally safe way to read it; the
  non-copying one may pin the heap and forbids blocking or any further JNI call while held
  [inference: `GetByteArrayRegion`, `SetByteArrayRegion`, `GetPrimitiveArrayCritical`]. A
  **direct** `ByteBuffer` is off-heap with a stable address [inference: JSR 51;
  `NewDirectByteBuffer`, `GetDirectBufferAddress`].
- **This machine has no JVM toolchain.** `command -v java javac gradle mvn kotlin` finds nothing
  (run for this note: exit 1, no output). Under [LOOP.md](../LOOP.md) §2 — no package
  installation, "a missing system dependency makes the item `blocked: needs <package>`" — the
  first JVM item is blocked the moment it is taken, which is scheduling and not this decision.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — JNI, one `cdylib` per binding** | the Rust crate exports `Java_*` symbols through the `jni` crate; the Java side is plain classes with `native` methods | the Python row exactly: a `cdylib` per binding on one shared foundation crate (`crates/zmq/weida-zmq-py/Cargo.toml:16`, [0014](0014-parallel-libraries.md) §2) | The Java package name is part of the native ABI, so a class rename is a runtime link failure; no compiler checks that the Java signature and the Rust function agree; every reactor thread that completes a call must be attached to the JVM; `unsafe_code` must be `allow` in one more crate |
| B — Panama (JEP 454) over a plain C ABI | Rust exposes `extern "C"` and knows nothing of the JVM; Java writes the binding with `Linker` and `MemorySegment` | none in this repository | **Floor of JDK 22**, above the 17 and 21 that [§2] names as LTS; the unsafety moves to a language where this workspace has no lint, no test and no reviewer; the descriptor for every function is hand-written Java that nothing type-checks against the Rust signature |
| C — both, over one C ABI core | a C ABI core crate, a JNI shim for ≤ 21 and a Panama front end for ≥ 22 | none | Two front ends, two test matrices and a third surface (the C ABI) to keep honest, bought before any user has asked for either; the first binding would pay for a portability nobody has requested |
| D — a pure-Java implementation of the protocol | no native code at all; the protocol is written a second time in Java | the JVM ecosystem's own habit [inference: pure-Java clients such as `nats.java` [nats §13] and Proton-J [amqp10 §13] are the norm there] | "**No second implementation of any protocol behaviour**" ([0013](0013-competitor-libraries.md) §4.4 item 3) is the rule the whole binding row exists under; this is that rule broken at the largest possible scale, and it makes the parity documents meaningless because the two implementations would drift |
| E — a sidecar process, Java over a local socket | the Rust library runs as a process; Java speaks to it over `AF_UNIX` or a named pipe | [0010](0010-local-transport.md)'s transports exist and would carry it | A second process to supervise, a second protocol to specify and version, and a guarantee chain that now ends at the sidecar edge with everything [0006](0006-guarantee-sets.md) §4.6-§4.7 requires of an edge; the binding would stop being a binding |

## 4. Decision

**Option A: JNI, floor JDK 17, one shared foundation per language, `CompletableFuture` as the
bridge, `byte[]` as the boundary with the copy named, Gradle with a native classifier per
platform, and ZeroMQ first.** Panama is refused for this row and reconsidered when the oldest
LTS a user is pinned to is 22 or later — which is a date, not an argument.

**4.1 JNI, and the floor is `--release 17`.** The mechanism is decided by the floor, not the other
way round: Panama's minimum is JDK 22 (JEP 454) and two of the three LTS releases named in §2 are
below it, so a Panama binding is unusable by a deployment pinned to 17 or 21 while a JNI binding
is usable by all of them. The floor is stated as a number the way `rust-version = "1.88"` is
stated once at the workspace root (`Cargo.toml:40`): **the bytecode target is 17**, rather than
11 or 8 because the foundation uses sealed types, records and `CompletableFuture.defaultExecutor()`
(§4.3), and rather than 21 because virtual threads are wanted by §4.3 and never required — the
caller supplies the executor and the foundation never names the class. Raising the floor later is
a version bump with a note, as raising the MSRV is; lowering it below 17 is not offered.

**4.2 The JVM foundation is one shared artifact, and it is one concept larger than the Python
one.** `crates/jvm/weida-jvm-core` is the analogue of `crates/py/weida-py-core`, under the same
prohibition: it depends on `pyo3`'s counterpart (`jni`) and `weida-runtime`, and **`weida`,
`weida-protocol` and every protocol library stay absent**, which is `lib.rs:12-16`'s rule
unchanged. It ships as a *pair* that is versioned and released together — the Rust crate, and one
`weida-jvm-core` jar holding the Java half — because a JNI boundary has two sides and a mismatched
pair is a runtime link failure (§3, option A's named loss). It contains the four things of
`lib.rs:18-41`:

1. **Error mapping.** One Java exception class per errno variant under one base class per library,
   generated from the same enum the Python classes are generated from, so `catch (EAGAIN e)` is
   what `except EAGAIN` is [zmq-py §9.6: 19 classes; mqtt-py §9.3: 51].
2. **The bridge** of §4.3.
3. **The bytes boundary** of §4.4.
4. **The handle table — the one thing `weida-py-core` did not need.** PyO3's `#[pyclass]` owns the
   Rust object and CPython's reference count decides when it dies; JNI has no such thing, and the
   usual answer is a `long` field holding a pointer. A `long` a Java caller can fabricate, reuse
   after close or race two threads on is a pointer dereference in Rust. So **a handle is an index
   into a generation-tagged table, never an address**: a stale or forged one fails the generation
   check and raises a named exception instead of touching freed memory, and every native-backed
   object is `AutoCloseable` with a `Cleaner` backstop that reports a leak rather than hiding it.

**4.3 The executor bridge: `CompletableFuture`, completed by the reactor, never *running* on it.**
The Python bridge's problem does not exist here and a different one takes its place. asyncio needs
`call_soon_threadsafe` because a `Future` may only be touched from the loop thread
(`bridge.rs:5-23`); `CompletableFuture.complete` is safe from any thread, so the reactor thread
completes it directly, attached to the JVM as a daemon (§2). The new problem is the other end: a
dependent stage registered with `thenApply` and no executor runs **on the thread that completed
the future** — a reactor worker, which is the mistake asyncio avoids by construction. The answer
is the one the JDK already provides:

- `WeidaFuture<T> extends CompletableFuture<T>` overrides **`defaultExecutor()`** to return the
  executor the context was built with, and **`newIncompleteFuture()`** so every derived stage
  inherits it [inference: both are Java 9 API, JEP 266]. Continuations added with `*Async` and no
  explicit executor therefore run on the caller's executor; the reactor's threads complete futures
  and run nothing of the caller's.
- **There is no second reactor and no thread pool of the binding's own**, which is
  `bridge.rs:30-40`'s rule with the names changed: the library owns its `Exec` through
  `weida-runtime`'s three constructors, and the JVM foundation drives futures on it.
- The **default executor** is the common pool only if the caller says so; the recommended one,
  documented rather than imposed, is `Executors.newVirtualThreadPerTaskExecutor()` on JDK 21 and
  up (JEP 444). The foundation compiles against 17 and never names that class.
- **Cancellation** mirrors `bridge.rs:42-51`. `CompletableFuture.cancel` interrupts nothing by
  itself, so the foundation registers a completion handler that aborts the Rust task driving the
  future, after which that future is dropped at its next suspension point and the socket it
  borrowed is usable again. Omitting it would "leave a Rust task running for a result no one will
  read" (`bridge.rs:51`).
- **A panic is an exception, not a hang**, and a JVM that is shutting down when the result arrives
  is a deliberate no-op, matching `bridge.rs:53-65` clause for clause.
- **The blocking surface blocks in Java, never in native.** The JVM analogue of
  `weida_zmq.sync` — the second surface [0014](0014-parallel-libraries.md) §2 orders after the
  asynchronous one — is `future.get(timeout)`, *not* a `native` method that waits inside Rust. The
  reason is mechanical: a virtual thread blocked on a future unmounts its carrier, while a virtual
  thread inside a native frame is pinned to it (§2, JEP 444), so a native wait would turn a
  cheap blocked task into a held platform thread. This is the JVM's version of
  `Python::detach`-while-blocking (`lib.rs:78-86`) and it points the opposite way, which is
  exactly why it is written down here.

**4.4 The `byte[]`/`ByteBuffer` boundary: one copy each direction on `byte[]`, and no Rust memory
ever escapes into a Java object.** The rule of `bytes.rs:1-8` carries over unchanged — no copy
where the runtime allows none, and the copy measured where it does not — and the answers differ:

| Direction | Surface | Cost | Why |
| --- | --- | --- | --- |
| Java → Rust, library takes ownership | `byte[]` | **one copy**, into the `Vec<u8>` the frame keeps | the same reason as `bytes.rs:20-28`: a frame outlives the call that queued it and is sent by a reactor thread later. A Java array cannot become a Rust allocation |
| Java → Rust, read-only and bounded | `byte[]` | one copy | the non-copying accessor may pin the heap and forbids blocking or any further JNI call while held [inference: `GetPrimitiveArrayCritical`], and the send path does both. **Refused**, for the reason `bytes.rs:29-37` refuses `memoryview`: a fast path that only sometimes exists is a cliff, not a feature |
| Rust → Java | `byte[]` | **one copy**, allocate and fill in one step | a `byte[]` owns its storage on the Java heap and no API adopts a foreign buffer — the `bytes.rs:29-37` sentence with "CPython" replaced |
| Rust → Java, allocation-sensitive callers | caller-supplied **direct** `ByteBuffer` | one copy, **zero allocations** per message | a direct buffer has a stable off-heap address [inference: JSR 51], so Rust writes into the caller's buffer. The copy is not removed; the per-message allocation is |
| Rust-owned memory wrapped as a `ByteBuffer` | — | — | **refused.** Wrapping a Rust buffer [inference: `NewDirectByteBuffer`] hands the JVM an object whose lifetime Java decides and Rust owns; a reference kept past the frame's life is a use-after-free with no exception to raise. §4.2's generation check exists precisely so that no such object is needed |

The numbers go into [IMPLEMENTATION.md](../IMPLEMENTATION.md) beside the Python ones, measured the
way `bytes.rs:39-42` measures, and the round-trip figure is read against [zmq-py §8]'s four.

**4.5 Built with Gradle, published to Maven Central as one pure-Java jar plus one native jar per
platform.** The Python row's precedent is one wheel per binding that a smoke test proves needs no
Rust toolchain (`package.sh:1-48`); the JVM's difference is that bytecode is portable and the
native library is not, so the wheel's single artifact becomes two kinds:

1. **Gradle**, not Maven, with the `maven-publish` plugin: the build invokes cargo once per target
   and packages its output, which is a task with declared inputs and outputs in Gradle's model and
   a plugin in Maven's. Nothing else of the ecosystem is used, so the choice stays small.
2. **Coordinates.** `groupId = net.doodleshnookie.weida`, the source host in reverse, with
   `artifactId` per binding — `weida-jvm-core`, `weida-zmq` — and the workspace version (root
   `Cargo.toml:41`) as the only source of the version, never typed twice, which is what
   `package.sh:6-9` already guarantees for the wheel. The Java package and the JPMS module name
   are `net.doodleshnookie.weida.zmq` [inference: JEP 261]. **One open coordinate:** Central
   requires a namespace whose ownership can be proved and no document here records a domain the
   project owns, so B-215 decides it against the registry rather than this note deciding it here.
3. **The artifacts.** One classifier-less jar of pure bytecode, and one jar per platform carrying
   only the native library as a resource — `linux-x86_64`, `linux-aarch64`, `macos-aarch64`,
   `windows-x86_64` to start, the platform set the Rust gate already covers [NIGHTLOG, B-188's
   Windows gate]. The loader extracts the resource matching the running platform and **fails with
   a named exception listing the classifiers it looked for**, not with an `UnsatisfiedLinkError`.
4. **The proof, in `package.sh`'s shape.** A script that builds the jars, installs them into a
   fresh directory containing nothing else, and runs a smoke `Main` with a `PATH` holding no
   `cargo`, `rustc` or Gradle — so "the jar needs no Rust toolchain" is established rather than
   expected, for the reason `package.sh:12-14` gives.
5. **One `jni` version across all bindings**, for the reason the workspace already pins one PyO3:
   two native libraries in one JVM each defining the same Java class are a conflict, and the
   shared foundation is what makes them one (root `Cargo.toml:109-113`).

**4.6 ZeroMQ gets the first JVM binding.** `crates/zmq/weida-zmq-jvm`, beside the library it
binds, as [0014](0014-parallel-libraries.md) §2's layout rule requires of every binding. Three
reasons, in the order that decided it:

1. **It is the only one of the five whose interop needs no third process.** ZeroMQ is
   peer-to-peer, so [LOOP.md](../LOOP.md) §9 Phase C's bench — "binding client against the Rust
   server" — is two objects in one test. MQTT, AMQP and NATS each need a broker or server
   ([0014](0014-parallel-libraries.md) §2's broker boundary), and the cost of that is already
   measured in this repository: the NATS row's interop clause is **not met** because
   `nats-server` was absent [libraries/README.md, the `nats.md` row], and the MQTT Python row's
   reference column was never run at all [mqtt-py §8, §9.7]. On a machine that may install
   nothing ([LOOP.md](../LOOP.md) §2) that difference decides it.
2. **Its surface is the largest, and a foundation is proved by the largest one.** Eleven socket
   types, 98 option rows, 19 exception classes, the monitor, the devices and two surfaces
   [zmq-py §2, §5, §7, §9]. A foundation proved on the smallest binding would be re-derived at
   the first hard one, which [0014](0014-parallel-libraries.md) §2 forbids in those words.
3. **Its Python twin is the only one whose definition-of-done table has every clause met**
   [zmq-py §10], including the measured round-trip costs of §8. Every JVM answer therefore has a
   measured Python answer beside it to be read against, which none of the other four can offer.

**The named loss of this choice, stated rather than discovered later:**
[research/zeromq.md](../research/zeromq.md) §13 names **no JVM implementation of ZeroMQ at all**
(no occurrence of "Java" anywhere in that sheet), where the MQTT, NATS and AMQP sheets each name
their ecosystem's canonical Java client [mqtt5 §1 and §13, sources 39-41; nats §13; amqp10 §13].
So [0013](0013-competitor-libraries.md) §4.7 clause 4 read for a binding — interop against the
reference implementation *in the binding's language* — cannot be satisfied from the repository as
it stands. A pure-Java ZeroMQ implementation does exist [inference: JeroMQ, in no sheet here and
not verified for this note], and §5 files the missing sheet row as its own item rather than
letting the binding invent a peer. Until it exists the interop is against the Rust library and
through it against pyzmq — what [LOOP.md](../LOOP.md) §9 Phase C asks for first — and the parity
document names the unmet half, as [libraries/README.md](../libraries/README.md)'s second rule
requires.

**4.7 What a JVM binding may not do.** Stated once so it is not re-derived per binding, mirroring
[0013](0013-competitor-libraries.md) §4.6: no protocol behaviour in Java — the Java side converts
arguments, holds handles and shapes futures, exactly as `weida_mqtt.sync` "implements **no
protocol behaviour**" [mqtt-py §9.2]; no second reactor and no thread pool the caller did not ask
for (§4.3); no conversion between a library's identity types and weida's, which
[0013](0013-competitor-libraries.md) §4.4 item 6 forbids "anywhere in the workspace"; and no
dependency from `weida-jvm-core` on `weida`, `weida-protocol` or any protocol library (§4.2).

**4.8 Status is `provisional` and why.** Nothing here changes a line of shipped code: no crate
exists yet, no public API moves, and the note is reversible by writing a different first item. It
stays provisional for one reason beyond that — **no JVM toolchain was available to this session**
(§2), so every JVM claim is primary knowledge with a JEP or JSR number rather than a measurement,
and §4.3's bridge is a design reasoned about and not run. The first item to land on a machine with
a JDK makes it accepted or amends it.

## 5. Consequences and follow-ups

- **[LOOP.md](../LOOP.md) §9 Phase C is wrong in one clause as written.** It says "every binding
  sits on the one shared `weida-py-core`", which is true of the Python row and cannot be true of
  the Java one. It becomes "every binding sits on the one shared foundation **of its language** —
  `weida-py-core` for Python, `weida-jvm-core` for the JVM — and no binding re-derives it". Not
  edited here; it is a `spec` item, and [0014](0014-parallel-libraries.md) §2's "One shared PyO3
  foundation, and no second one" is extended the same way rather than contradicted: one foundation
  per language, under the same dependency-direction rule. 0014 stays accepted and unedited.
- **[STATUS.md](../STATUS.md)'s "Phase C's Java and Node rows, untouched and unfiled"** stops
  being true for Java when the items below are filed.
- **[decisions/README.md](README.md)** gains this note's row, as
  [LOOP.md](../LOOP.md) §5 requires of every `research` item ("the README table of that directory
  updated"). Filed as part of the item that lands this file, not done here.
- **A new crate directory, `crates/jvm/`**, holding `weida-jvm-core` beside the existing
  `crates/py/`, and `crates/zmq/weida-zmq-jvm` beside `crates/zmq/weida-zmq-py` — the layout rule
  of [0014](0014-parallel-libraries.md) §2 with no exception asked for.
- **The gate grows a second toolchain, or it does not run.** A JVM binding's tests are not
  cargo's, and [LOOP.md](../LOOP.md) §6's four commands say nothing about them. B-215 owns that
  question; until a JDK exists here every item below is `blocked: needs a JDK` by §2's rule.

The items below are written to be pasted into [BACKLOG.md](../BACKLOG.md), so their links are
relative to that file rather than to this one.

### B-211 — The JVM foundation: `weida-jvm-core`
kind: code | size: 90 | status: ready | needs: []
acceptance: `crates/jvm/weida-jvm-core` and its `weida-jvm-core` jar, versioned and released as one pair, holding the four things of [0019](0019-jvm-binding.md) §4.2 and no protocol: the errno-to-exception-class generator (one class per variant under one base, the shape `crates/py/weida-py-core/src/errors.rs` has for Python), the `CompletableFuture` bridge of §4.3 over `weida_runtime::Exec` with `defaultExecutor()` and `newIncompleteFuture()` overridden, cancellation propagated into a Rust task abort, a panic surfacing as an exception rather than a hang, and the generation-tagged handle table with `AutoCloseable` plus a `Cleaner` backstop. `[dependencies]` is `jni` and `weida-runtime`; `weida`, `weida-protocol` and every protocol library are absent, asserted by a test over the manifest as the Python foundation's is. Bytecode target 17. Three JUnit tests that would fail on a plausible bug: a future completed from a reactor thread whose `thenApply` continuation observes the caller's executor and never a reactor thread; a cancelled future whose Rust task is observably dropped; a forged and a reused handle both raising the named exception instead of dereferencing anything.
note: blocked on a JDK the moment it is taken — `command -v java javac gradle mvn` finds nothing on this machine ([LOOP.md](../LOOP.md) §2: `blocked: needs <package>`). The item is filed `ready` because the blocker is a machine fact, not a dependency.

### B-212 — `weida-zmq-jvm`: context, REQ/REP and PUSH/PULL
kind: code | size: 90 | status: ready | needs: [B-211]
acceptance: `crates/zmq/weida-zmq-jvm` binding `weida-zmq`'s `Context` and four socket types on the foundation, asynchronous surface first per [0014](0014-parallel-libraries.md) §2: `bind` returning the chosen endpoint, `connect`, `send`, `recv` and `close`, every waiting call returning a `WeidaFuture`, payloads as `byte[]` with the copy of [0019](0019-jvm-binding.md) §4.4 and no `ByteBuffer` path yet, and libzmq's errno names as exception classes. A test that round-trips REQ/REP and PUSH/PULL over `tcp://127.0.0.1:0` and `inproc://`, one that asserts a closed socket's handle raises rather than crashes, and one that asserts no continuation ever observes a reactor thread.
note: the slice that proves the foundation end to end on the smallest useful surface of the largest library; the remaining seven socket types are B-213 deliberately.

### B-213 — The rest of the ZeroMQ surface from the JVM
kind: code | size: 90 | status: ready | needs: [B-212]
acceptance: the remaining seven socket types (DEALER, ROUTER, PUB, SUB, XPUB, XSUB, PAIR), the 98-row option table reachable by libzmq's own option names with every refusal raising at construction and naming its reason — no transport, draft only, deprecated in favour of ZAP, replaced by a `weida-runtime` construct, absent — and the monitor as a stream of typed events. A test walks all 98 rows as `tests/test_options.py` does for Python, and `subscribe`/`unsubscribe` are asserted additive and non-idempotent.

### B-214 — The blocking surface, and the pinning rule proved
kind: code | size: 60 | status: ready | needs: [B-212]
acceptance: the synchronous surface over `weida-zmq`'s `blocking` feature, sharing every value class with the asynchronous one as [mqtt-py.md](../libraries/mqtt-py.md) §9.2 requires, with each blocking call waiting on the future **in Java** and never inside a native frame ([0019](0019-jvm-binding.md) §4.3). One test on JDK 21 or later asserts that N concurrent blocking round trips on virtual threads occupy far fewer carrier threads than N — the observable form of "a native wait would pin the carrier" — and is skipped with its reason on an older JDK; every wait is bounded so a regression fails instead of hanging ([LOOP.md](../LOOP.md) §2).

### B-215 — The jars, and the proof they need no Rust toolchain
kind: code | size: 60 | status: ready | needs: [B-213]
acceptance: a Gradle build producing one classifier-less bytecode jar and one native jar per platform (`linux-x86_64`, `linux-aarch64`, `macos-aarch64`, `windows-x86_64`), publishable with `maven-publish` under the coordinates of [0019](0019-jvm-binding.md) §4.5 with the workspace version as the only source of the version; the loader picking the running platform's resource and failing with a named exception listing the classifiers it looked for; and a `package.sh` counterpart that installs the jars into a fresh directory and runs a smoke `Main` with `PATH` holding no `cargo`, `rustc` or Gradle, exactly as `crates/zmq/weida-zmq-py/package.sh:12-14` proves it for the wheel. The Central namespace question of §4.5 is answered here, against the registry.

### B-216 — `docs/libraries/zmq-jvm.md`, the parity document
kind: spec | size: 60 | status: ready | needs: [B-213, B-214, B-215]
acceptance: the eleven sections [libraries/README.md](../libraries/README.md) fixes, row by row against the reference implementation column, with three verdicts and no "partial" that does not say what is missing; a §9 naming this binding's deliberate deviations the way [zmq-py.md](../libraries/zmq-py.md) §9 does (futures where Rust answers synchronously, `byte[]` and its copy, handles and `AutoCloseable`, the executor rule, seconds rather than milliseconds); §8 stating plainly that clause 4's same-language interop is unmet until B-217 lands, and against what it *was* measured; and the index row in `docs/libraries/README.md`.

### B-217 — The ZeroMQ sheet has no JVM implementation
kind: research | size: 45 | status: ready | needs: []
acceptance: [research/zeromq.md](../research/zeromq.md) §13 gains the JVM row it does not have — the sheet contains no occurrence of "Java" — naming each implementation, its licence, its version, whether it is a pure-Java implementation of ZMTP or a binding over libzmq, and what it does not implement, each with a source in §14's form. That row is what makes [0013](0013-competitor-libraries.md) §4.7 clause 4 answerable for the JVM binding; until it exists, [0019](0019-jvm-binding.md) §4.6's named loss stands.

## 6. What this note does not decide

- **Whether a Panama front end is ever added.** §4.1 fixes the condition for reconsidering it —
  the oldest LTS in use being 22 or later — and §3 option C prices it: a second front end over
  the same handle table, not a rewrite of the Rust side.
- **The Node row.** N-API against WASM is a different question with different constraints, and
  nothing here is a precedent for it beyond "one foundation per language".
- **Kotlin, Scala, Clojure, Android and GraalVM `native-image`.** The artifact is bytecode those
  languages can call; a coroutine adapter, a Reactive Streams surface or a JMS facade is its own
  product with its own parity document; Android is a different JNI environment and a different
  packaging story; and `native-image` has a reachability question this note has no evidence about
  [inference: no Android or GraalVM claim here was verified].
- **Which binding is second.** §4.6 chooses only the first; the second is chosen after B-216 says
  what the foundation actually cost.
- **weida's own JVM binding.** B-200 files the Python one for weida itself; the JVM equivalent is
  not filed here, for the same reason B-200 waits on B-194 — a binding before its blocking facade
  builds the facade twice ([0014](0014-parallel-libraries.md) §2).

**What would reopen this.** Two things, both checkable. A deployment whose floor is JDK 22 or
later, which removes option B's only named loss and makes the mechanism a free choice again. Or a
measurement from B-212: if the `CompletableFuture` round trip costs an order of magnitude more
than [zmq-py §8]'s 330 µs asyncio figure, the bridge — not the FFI mechanism — is the thing to
redesign, and §4.3's executor rule is where to look first.

## 7. Sources

weida documents: [LOOP.md](../LOOP.md) §2, §5, §6, §9 Phase C; [BACKLOG.md](../BACKLOG.md) B-199,
B-200; [STATUS.md](../STATUS.md) ("What that leaves" and the closing choice);
[libraries/README.md](../libraries/README.md) (the eleven required sections and the two honesty
rules); [libraries/zmq-py.md](../libraries/zmq-py.md) §2, §5, §7, §8, §9, §10;
[libraries/mqtt-py.md](../libraries/mqtt-py.md) §8, §9.1, §9.2, §9.3, §9.7, §10;
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §4; [0013](0013-competitor-libraries.md) §4.2, §4.4
items 3 and 6, §4.6, §4.7 clauses 4 and 6; [0014](0014-parallel-libraries.md) §2, §3;
[0006](0006-guarantee-sets.md) §4.6, §4.7; [0010](0010-local-transport.md).

Code read: `crates/py/weida-py-core/src/lib.rs` (the four things, the dependency prohibition, the
GIL rule); `crates/py/weida-py-core/src/bridge.rs` (the loop bridge, the refusal of
`pyo3-async-runtimes`, cancellation, the failure modes, the `Send + 'static` bound);
`crates/py/weida-py-core/src/bytes.rs` (both directions and the `memoryview` refusal);
`crates/py/weida-py-core/Cargo.toml` and `crates/zmq/weida-zmq-py/Cargo.toml` (the `unsafe_code`
exception with its test, `cdylib`); `crates/zmq/weida-zmq-py/src/lib.rs` (asyncio first, how it
is built); `crates/zmq/weida-zmq-py/package.sh`; root `Cargo.toml` (`rust-version = "1.88"`, the
one shared PyO3 with its reason).

Research sheets: [research/zeromq.md](../research/zeromq.md) §13 (the ecosystem inventory, which
names no JVM implementation); [research/mqtt5.md](../research/mqtt5.md) §1 and §13 with sources
39-41 (Paho Java's backoff, persistence contract and `MemoryPersistence` warning);
[research/nats.md](../research/nats.md) §13 (`nats.java`);
[research/amqp10.md](../research/amqp10.md) §13 (Proton-J, Qpid JMS);
[research/ipc.md](../research/ipc.md) §5 with source 152 (JEP 380 — how this repository cites a JEP).

External specifications, none of which is documented in this repository and each carried in with
its number: **JEP 454** (Foreign Function & Memory API, final in JDK 22); **JEP 444** (virtual
threads, final in JDK 21); **JEP 266** [inference] (`CompletableFuture.defaultExecutor()` and
`newIncompleteFuture()`, Java 9); **JEP 472** [inference] (preparing to restrict JNI, JDK 24);
**JEP 491** [inference] (`synchronized` no longer pins a virtual thread, JDK 24); **JEP 261**
[inference] (the module system and module names); **JEP 322** [inference] (the release cadence
that produces the LTS pattern; long-term support itself is a vendor designation with no JEP);
**JSR 337** [inference] (Java SE 8, where `CompletableFuture` arrives); **JSR 51** [inference]
(the `java.nio` direct buffers); and the **JNI specification**'s `AttachCurrentThreadAsDaemon`,
`DetachCurrentThread`, `GetByteArrayRegion`, `SetByteArrayRegion`, `GetPrimitiveArrayCritical`,
`NewDirectByteBuffer` and `GetDirectBufferAddress` [inference: cited by function name, since that
specification has no clause numbers].

Machine fact established for this note: `command -v java javac gradle mvn kotlin` produces no
output and exits 1 — there is no JVM toolchain here, which is why §4.8 is provisional.
