# 0046 — How each language binds libweida

- **Status:** accepted
- **Date:** 2026-11-01
- **Supersedes:** [0019](0019-jvm-binding.md)'s foundation design (`weida-jvm-core` and its
  handle table); [0030](0030-which-runtimes-and-in-which-order.md) §3's order
- **Related:** [0045](0045-the-c-abi-is-the-boundary.md); [c/README.md](../c/README.md);
  [libraries/weida-c.md](../libraries/weida-c.md); [libraries/weida-py.md](../libraries/weida-py.md)

## 1. The question

[0045](0045-the-c-abi-is-the-boundary.md) made `libweida` (`crates/weida-c`, `weida.h`) the one
boundary every non-Rust language crosses, and moved Python onto it through cffi. The owner wants
full packages for JavaScript (Node, Bun, Deno), the JVM plus Android, Erlang/Elixir, Go and
Swift/iOS on the same boundary. Each runtime offers more than one way to call a C library, and
each has its own event loop that must learn when one of `libweida`'s readiness descriptors
(R11 in [c/README.md](../c/README.md)) fires.

So the question is: **through which mechanism does each language call `libweida`, and how does
its async surface wait on the readiness descriptors.**

## 2. The evidence

**What ZeroMQ's own bindings do.** ZeroMQ is the closest precedent: one C library (`libzmq`,
`zmq.h`) with readiness descriptors (`ZMQ_FD`) and a binding per language.

- pyzmq: a Cython backend on CPython, and a cffi backend for PyPy.
- zeromq.js: a C++ Node-API addon with `libzmq` linked in, prebuilt per platform.
- jzmq: JNI, abandoned in favour of JeroMQ, a pure-Java rewrite of the protocol.
- erlzmq2: a C NIF.
- pebbe/zmq4: cgo.
- iOS: `libzmq` as a static library, called from Swift.

Every living binding is native glue against the C header, except JeroMQ, which reimplements the
protocol. weida's protocol is QUIC with TLS 1.3 and its own framing; a rewrite per language is a
QUIC stack per language, which is the cost the C ABI exists to avoid.

**Further evidence.**

- Android's runtime (ART) has no `java.lang.foreign`: the FFM API (JEP 454, final in JDK 22) is
  a desktop JVM path only, and JNI is the one native interface both the JVM and Android share.
- `node:ffi` (Node 26.1 and later) and `bun:ffi` are experimental: neither promises a stable
  surface, and they differ from each other and from Deno's `Deno.dlopen`.
- Bun and Deno implement Node-API, including threadsafe functions
  (`napi_create_threadsafe_function`), so one Node-API addon serves all three runtimes.
- Google ships the Android NDK for linux-x86_64 hosts only, so Android's native build runs on
  linux-amd64.
- Python's cffi in out-of-line ABI mode already works on CPython, free-threaded CPython and PyPy
  from one `py3-none-<platform>` wheel ([0045](0045-the-c-abi-is-the-boundary.md) §4.5).

## 3. The options

**A. One Rust-native foundation per language** ([0019](0019-jvm-binding.md)'s `weida-jvm-core`,
[0030](0030-which-runtimes-and-in-which-order.md) §2's unit). Loses: a second boundary beside
`libweida` for each language, each with its own error mapping, handle table and bridge, which is
what 0045 replaced for Python.

**B. Each runtime's own FFI** (FFM on the JVM, `node:ffi`, `bun:ffi`, `Deno.dlopen`). Loses:
Android, which has no FFM, so the JVM needs a second native path anyway; and on JavaScript three
experimental, mutually incompatible FFIs instead of one stable ABI.

**C. A pure reimplementation per language** (JeroMQ's shape). Loses: a QUIC and TLS stack per
language and the protocol kept in step N times.

**D. Native glue against `weida.h` per language**, the shape every living ZeroMQ binding but
JeroMQ has. Loses: a small C (or cgo, or Swift module map) layer per language and one prebuilt
native artifact per platform, built in that language's toolchain.

## 4. The decision

**D. Every package binds `libweida` and nothing else.**

| Language | Binding | How async waits on the readiness descriptors |
| --- | --- | --- |
| Python | stays cffi in out-of-line ABI mode: one `py3-none-<platform>` wheel serves CPython, 3.14t and PyPy | `loop.add_reader` |
| JavaScript (Node, Bun, Deno) | a Node-API addon written in C against `weida.h`, one prebuilt `.node` per platform | one waiter thread per process runs `weida_poll` and reports through `napi_threadsafe_function` |
| JVM and Android | one JNI glue layer in C, shared by the desktop JVM and Android; one Kotlin/Java API | one waiter thread completing `CompletableFuture`s. FFM is not used: Android has no `java.lang.foreign`, so it would mean a second native path |
| Erlang/Elixir | a C NIF against `weida.h` | `enif_select` delivers readiness to the waiting process's mailbox |
| Go | cgo against `weida.h`, `libweida` linked statically | `os.NewFile` puts the descriptor into Go's netpoller |
| Swift/iOS | an XCFramework of static `libweida.a`; Swift imports the C module directly | `DispatchSource` read sources |

The order of the packages is **JS, JVM/Android, Erlang/Elixir, Go, Swift/iOS**.

Each language's runtime lives in a CI container image (`.zeughaus-ci/<lang>.Containerfile`) on
linux-amd64 and linux-arm64, plus an Android cross-build image on linux-amd64; the `c-abi-linux-*`
jobs build `libweida` once per architecture and hand it to those images as an artifact.

**What this supersedes.**

- [0019](0019-jvm-binding.md)'s foundation design (`weida-jvm-core` and its handle table): the
  JVM binds `libweida` through JNI, and `libweida`'s handles are the handles.
- [0030](0030-which-runtimes-and-in-which-order.md) §3's order: the order is now JS,
  JVM/Android, Erlang/Elixir, Go, Swift/iOS.

## 5. Follow-ups

- One plan per package, in the order of §4.
- Xcode installed on atik before the Swift/iOS plan.
- `staticlib` added to `weida-c`'s crate types in the Go and Swift plans.
