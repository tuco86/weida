# weida for JavaScript (`crates/js/weida-js`, against the C ABI `libweida`) — feature parity

## 1. What a row means

The package is one Node-API addon written in C against `weida.h`
([decisions/0046](../decisions/0046-how-each-language-binds-libweida.md)), serving Node, Bun and
Deno, with an ES module on top. Its API mirrors the C ABI: one class per handle, one method per
function with the `weida_<type>_` prefix dropped and the rest in camelCase. Its reference is
therefore [weida-c.md](weida-c.md): every row present there is present here unless a row below
says otherwise, and every row absent there is absent here for the same reason. This document
lists what the JavaScript surface adds or changes. Verdicts are **present** (with the test),
**refused** (with the reason) or **absent** (with the reason); there is no "partial".

The tests are `crates/js/weida-js/test/unit/` (in-process transport only, one second each) and
`test/integration/` (QUIC on localhost, ten seconds each), run with `node --test`; `smoke.mjs`
makes one round trip under each runtime, and `package.sh` runs it from a fresh project that
installed the packed tarball. Measured on Node 26, Bun 1.4.4 and Deno 2.9.8 on linux-amd64 and
linux-arm64 (the `js-linux-*` jobs).

## 2. Classes

| C ABI | JavaScript (`weida`, ESM) | Verdict |
| --- | --- | --- |
| `weida_runtime_config` | `RuntimeConfig({ workerThreads, shutdownTimeoutMs })`, a plain value turned into a handle by `Runtime.owned` | present; `errors.test.js` "a runtime with no worker threads is a runtime failure" |
| `weida_runtime` | `Runtime.owned(config?)`, `.listener()`, `.requester(clientTls)`, `.shutdown(opts)` (consumes), `.drop()` | present; every round-trip test |
| `weida_trust` | `new Trust()`, `.addPin(fp)` returning the trust | present; `errors.test.js` "a malformed pin is an invalid fingerprint", `quic.test.js` "a wrong pin rejects with WEIDA_EUNTRUSTED carrying the fingerprint" |
| `weida_identity` | `Identity.generate()`, `Identity.fromPem(cert, key)`, `.fingerprint()` | present; `quic.test.js` "a round trip over QUIC with a pinned key" |
| `weida_client_tls`, `weida_server_tls` | `new ClientTls(trust)`, `new ServerTls(identity)` | present |
| `weida_transfer_meta` | a plain object `{ contentType? }`, turned into a handle per call | present; `inproc.test.js` "a streamed round trip carries the body and the content type" |
| `weida_listener` | `Listener.bindQuic(addr, serverTls, opts) -> Promise<Binding>`, `.bindInproc(bus)`, `.replier(path)` | present |
| `weida_binding`, `weida_local_binding` | `Binding.localAddr()`, `LocalBinding.bus()` | present |
| `weida_requester` | `Requester.connect(url, opts)`, `.request(body, opts) -> Promise<IncomingTransfer>`, `body` a `Uint8Array` or a string sent as UTF-8 | present; `inproc.test.js` "a streamed round trip carries the body and the content type" |
| `weida_replier` | `Replier.path()`, `.accept(opts) -> Promise<IncomingRequest>` | present; `inproc.test.js` "two concurrent accepts on one replier serve two requests" |
| `weida_incoming_request` | `IncomingRequest.meta()`, `.takeBody()`, `.reply(meta?, opts)` (consumes) | present; `inproc.test.js` "a second reply rejects with WEIDA_ECLOSED" |
| `weida_incoming_transfer` | `IncomingTransfer.meta()`, `.read(cap = 65536, opts) -> Promise<Uint8Array>` (empty at the end), `.collect(maxBytes, opts) -> Promise<Message>` (consumes) | present; `inproc.test.js` "reads of 3 bytes reassemble the body", "a collect over its ceiling rejects with WEIDA_ELIMIT_EXCEEDED", "a reply dropped unfinished reads as WEIDA_ECANCELED" |
| `weida_outgoing_transfer` | `OutgoingTransfer.write(data, opts) -> Promise<number>`, `.writeAll(data, opts)`, `.finish() -> Delivery` (consumes) | present; `writeAll` is the JavaScript loop R3c leaves to the caller |
| `weida_delivery` | `Delivery.delivered(opts)` (consumes) | present |
| `weida_incoming_meta` | a frozen plain object `{ endpoint, contentType, topic, peerFingerprint, contentLen, sequence }`, copied out at once; absent values are `null`, the two counters `BigInt` | present |
| `weida_msg` | a frozen `{ payload: Uint8Array, meta }`, copied out at once | present |
| the ops (`*_op`) | not exposed: a waiting method starts its op, waits, and drops it in `finally`, which cancels it only when it did not finish | refused: an op is the C caller's way to wait, not a value a JavaScript caller needs |
| the `_fd` functions, `weida_poll` | not exposed: the addon's waiter thread polls the descriptors (§4) | refused: no JavaScript runtime can watch a raw descriptor itself |
| `weida_version`, `weida_strerror` | `version()`, `strerror(code)` | present; `smoke.mjs`, `errors.test.js` "strerror of an unknown code is 'unknown error'" |

Every handle class releases its handle when the garbage collector finalizes it, and `.drop()`
and `[Symbol.dispose]()` (`using`) release it at once. A dropped or consumed object throws
`WEIDA_ECLOSED` without calling C; its pending waits reject with `WEIDA_ECLOSED` before its
descriptor closes. Present; `inproc.test.js` "dropping a replier rejects its pending accept with
WEIDA_ECLOSED".

## 3. Errors

One class, `WeidaError`, with `code` (the R9 constant name, `"WEIDA_EUNTRUSTED"`), `errno`
(the positive code), `message` (the `Display` text), and the thread-local record as
`lossCause` (`"WEIDA_LOSS_*"` or `null`), `fingerprint`, `max` and `osCode` (each `null` when
absent), read by the addon in the same native call that failed. A JavaScript caller branches on
`error.code`, as on Node's own `SystemError`, so there are no subclasses. Present;
`errors.test.js` "every status code is a WeidaError carrying its R9 name", `quic.test.js` "a
wrong pin rejects with WEIDA_EUNTRUSTED carrying the fingerprint".

## 4. Waiting

| Behaviour | Verdict |
| --- | --- |
| The JS thread never blocks: every native call passes `timeout_ms = 0`, and a try or op wait that would block returns `AGAIN` to the JavaScript layer | present by construction (`src/addon.c`) |
| One waiter thread per runtime environment runs `weida_poll` over the descriptors of every pending wait plus a wake pipe, and reports each readable one, once, to the JS thread through one `napi_threadsafe_function`; the call is then tried again | present; every unit test past its first `AGAIN` |
| Two waits on one handle (two `accept`s on a replier) both watch its descriptor and are both retried; the one that finds nothing watches again | present; `inproc.test.js` "two concurrent accepts on one replier serve two requests" |
| An idle process exits: the threadsafe function is referenced only while a wait is pending | present; `smoke.mjs` exits on its own under all three runtimes |
| Every waiting method takes `{ signal }`; aborting rejects with `signal.reason` and cancels the operation (its op is dropped). `AbortSignal.timeout(ms)` is the timeout | present; `inproc.test.js` "an aborted request rejects with the reason and the runtime still shuts down" |
| The waiter thread blocks every signal, so a process-directed signal reaches the JS thread, whose runtime handles it | present by construction (`src/waiter.c`) |
| Worker threads: each `napi_env` has its own instance data and waiter | present by construction; not run |

## 5. Packaging

| Row | Verdict |
| --- | --- |
| One package `weida` per platform: `lib/`, `build/weida.node` and `libweida` beside it, found through the addon's rpath (`$ORIGIN`, `@loader_path`) | present; `package.sh` |
| The packed version is the workspace version (`0.1.0-alpha.11-dev` stays as it is, already semver); the committed `package.json` says `0.0.0-dev` | present; `package.sh` checks the installed version |
| The tarball installs offline into a fresh project and runs with no Rust toolchain on `PATH` | present; `package.sh` runs `smoke.mjs` with `PATH` reduced to node, bun, deno and `/usr/bin:/bin` |
| Node 20 and up (`engines`), Bun and Deno load the same addon through `createRequire` | present; Node 26, Bun 1.4.4, Deno 2.9.8 run (`js-linux-amd64`, `js-linux-arm64`) |
| macOS | absent: `build.sh` has the macOS link flags, but no job builds or runs the addon there |
| Windows | absent: no Windows JavaScript job, and the waiter's wake pipe is POSIX |
| npm registry | absent: nothing of this repository is published to npm |

## 6. What is not mapped

Every pattern, stream, cursor and statistic the C ABI does not map yet is absent here exactly
as it is absent from the C ABI ([weida-c.md](weida-c.md) §5 to §7), and arrives when the ABI
maps it.

## 7. Sources

`crates/js/weida-js/src/` (`addon.c`, `waiter.c`, `addon.h`), `lib/` (`index.js`,
`index.d.ts`, `native.js`, `errors.js`, `wait.js`), `build.sh`, `develop.sh`, `package.sh`,
`smoke.mjs`, `test/`; `.zeughaus-ci/js-linux-amd64.sh`, `.zeughaus-ci/js-linux-arm64.sh`;
[weida-c.md](weida-c.md); [docs/c/](../c/README.md);
[decisions/0046](../decisions/0046-how-each-language-binds-libweida.md).
