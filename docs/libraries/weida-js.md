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
installed the packed tarball. Measured on Node 26, Bun 1.4.4 and Deno 2.9.8 on linux-amd64,
linux-arm64, darwin-arm64 and windows-amd64 (the `js-linux-*` jobs, `js-darwin-arm64` and
`js-windows-amd64`).

## 2. Classes

| C ABI | JavaScript (`weida`, ESM) | Verdict |
| --- | --- | --- |
| `weida_runtime_config`, `weida_limits`, `weida_guarantee_set`, `weida_reconnect_policy` | `RuntimeConfig({ ... })` or a plain object, turned into handles by `Runtime.owned`: every field of `weida_runtime_config` (`workerThreads`, `shutdownTimeoutMs`, `maxConnections`, `maxConnectionsPerPeer`, `endpointQueue`, `maxResolvedAddresses`, `connectAttemptTimeoutMs`, `sendTimeoutMs`, `outboxMessages`, `outboxBytes`, `outboxFull`, `maxHandshakes`, `retryAboveHandshakes`, `discovery`) and the nested objects `limits` (every `Limits` field), `guarantees` (every dimension) and `reconnect` (every field, `never: true` to start from `ReconnectPolicy::never()`); enumerations are the constant names as strings, an unknown field is a `TypeError` | present; `config.test.js` "a full configuration builds a runtime", "a guarantee the dialler does not offer fails with WEIDA_ENEGOTIATION", "configuration input errors are thrown at once", `errors.test.js` "a runtime with no worker threads is a runtime failure" |
| `weida_runtime` | `Runtime.owned(config?)`, `.listener()`, `.requester(clientTls)`, `.shutdown(opts)` (consumes), `.drop()` | present; every round-trip test |
| `weida_trust` | `new Trust()`, `.addPin(fp)`, `.addAnchor(X509Certificate \| bytes)`, `.addAnchorFile(path)`, each returning the trust | present; `errors.test.js` "a malformed pin is an invalid fingerprint", `quic.test.js` "a wrong pin rejects with WEIDA_EUNTRUSTED carrying the fingerprint", `mtls.test.js` "an anchor accepts the dialled name", "identity and anchor files read back" |
| `weida_identity` | `Identity.generate()`, `.generateFor(names)`, `.fromPem(cert, key)`, `.from({ cert, key })` (`X509Certificate`, an array of them or bytes; a private `KeyObject` or bytes), `.fromPemFile(path)`, `.fromPemFiles(cert, key)`, `.fingerprint()`, `.certificatePem(): string`, `.toPem(): Uint8Array`, `.certificates(): X509Certificate[]` | present; `quic.test.js` "a round trip over QUIC with a pinned key", `identity.test.js` (all five), `mtls.test.js` "identity and anchor files read back", `smoke.mjs` |
| `weida_pem` | not exposed: copied out at once into a string (`certificatePem`) or a `Uint8Array` (`toPem`) and dropped, which wipes it | refused as a class: a JavaScript caller needs the bytes, not the handle |
| `weida_client_tls`, `weida_server_tls` | `new ClientTls(trust, { identity? })`, `new ServerTls(identity, { requireClient? })` with `requireClient` a `Trust` or `"any-key"` | present; `mtls.test.js` "a pinned client key is accepted and the server sees its fingerprint", "an anonymous or unknown client is refused with WEIDA_ETLS and nothing reaches the server", "any-key exposes the fingerprint and the chain", `smoke.mjs` |
| `weida_transfer_meta` | a plain object `{ contentType?, contentLen?, trace?, topic?, achieved? }`, turned into a handle per call; an unknown field is a `TypeError` | present; `inproc.test.js` "a streamed round trip carries the body and the content type", `config.test.js` "metadata travels with a request and its reply" |
| `weida_listener` | `Listener.bindQuic(addr, serverTls, opts) -> Promise<Binding>`, `.bindInproc(bus)`, `.replier(path)` | present |
| `weida_binding`, `weida_local_binding` | `Binding.localAddr()`, `LocalBinding.bus()` | present |
| `weida_requester` | `Requester.connect(url, opts)`, `.request(body, opts) -> Promise<IncomingTransfer>`, `.requestWith(meta, body, opts)`, `body` a `Uint8Array` or a string sent as UTF-8 | present; `inproc.test.js` "a streamed round trip carries the body and the content type", `config.test.js` "metadata travels with a request and its reply" |
| `weida_replier` | `Replier.path()`, `.accept(opts) -> Promise<IncomingRequest>` | present; `inproc.test.js` "two concurrent accepts on one replier serve two requests" |
| `weida_incoming_request` | `IncomingRequest.meta()`, `.takeBody()`, `.reply(meta?, opts)` (consumes), `.refuse(code, opts)` (consumes; `code` a `WEIDA_ERROR_CODE_*` name, an unknown name a `TypeError` that consumes nothing), `.canceled(opts)` (started at the call, outlives the request) | present; `inproc.test.js` "a second reply rejects with WEIDA_ECLOSED", "a refused request rejects the requester with the refused code's error", "a refusal code outside WEIDA_ERROR_CODE_* is a TypeError and leaves the request usable", "canceled settles once the requester gives up" (*refusals*) |
| `weida_incoming_transfer` | `IncomingTransfer.meta()`, `.read(cap = 65536, opts) -> Promise<Uint8Array>` (empty at the end), `.collect(maxBytes, opts) -> Promise<Message>` (consumes) | present; `inproc.test.js` "reads of 3 bytes reassemble the body", "a collect over its ceiling rejects with WEIDA_ELIMIT_EXCEEDED", "a reply dropped unfinished reads as WEIDA_ECANCELED" |
| `weida_outgoing_transfer` | `OutgoingTransfer.write(data, opts) -> Promise<number>`, `.writeAll(data, opts)`, `.trace()`, `.setPriority(n)`, `.expireAfterMs(ms)`, `.finish() -> Delivery` (consumes) | present; `writeAll` is the JavaScript loop R3c leaves to the caller; `config.test.js` "metadata travels with a request and its reply", "an expired reply fails its finish with WEIDA_EEXPIRED" |
| `weida_delivery` | `Delivery.delivered(opts)` (consumes) | present |
| `weida_incoming_meta` | a frozen plain object `{ endpoint, contentType, topic, peerFingerprint, peerChain, contentLen, sequence, trace, tracestate, achieved }`, copied out at once; absent values are `null`, the two counters `BigInt`, `peerChain` a frozen array of `X509Certificate`, leaf first, `achieved` a `"WEIDA_ACKNOWLEDGEMENT_*"` name | present; `mtls.test.js` "any-key exposes the fingerprint and the chain", `config.test.js` "metadata travels with a request and its reply" |
| `weida_msg` | a frozen `{ payload: Uint8Array, meta }`, copied out at once | present |
| the ops (`*_op`) | not exposed: a waiting method starts its op, waits, and drops it in `finally`, which cancels it only when it did not finish | refused: an op is the C caller's way to wait, not a value a JavaScript caller needs |
| the `_fd` functions, `weida_poll` | not exposed: the addon's waiter thread polls the descriptors (§4) | refused: no JavaScript runtime can watch a raw descriptor itself |
| `weida_version`, `weida_strerror`, `weida_new_trace` | `version()`, `strerror(code)`, `newTrace()` | present; `smoke.mjs`, `errors.test.js` "strerror of an unknown code is 'unknown error'", `config.test.js` "newTrace is a sampled W3C traceparent" |
| the enumeration constants (`WEIDA_CONGESTION_*`, `WEIDA_ERROR_CODE_*` and the rest, R7) | their names as strings, read from the addon's `CONSTANTS` | present; `config.test.js` "configuration input errors are thrown at once", `inproc.test.js` "a refusal code outside WEIDA_ERROR_CODE_* is a TypeError and leaves the request usable" |

The conversions between Node's `KeyObject` and `X509Certificate` and the ABI's PEM and DER
follow [0047](../decisions/0047-key-material-in-the-language-packages.md).

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
| One waiter thread per runtime environment runs `weida_poll` over the descriptors of every pending wait plus a wake pipe (on Windows a loopback UDP socket connected to itself, since `WSAPoll` takes only sockets), and reports each readable one, once, to the JS thread through one `napi_threadsafe_function`; the call is then tried again | present; every unit test past its first `AGAIN` |
| Two waits on one handle (two `accept`s on a replier) both watch its descriptor and are both retried; the one that finds nothing watches again | present; `inproc.test.js` "two concurrent accepts on one replier serve two requests" |
| An idle process exits: the threadsafe function is referenced only while a wait is pending | present; `smoke.mjs` exits on its own under all three runtimes |
| Every waiting method takes `{ signal }`; aborting rejects with `signal.reason` and cancels the operation (its op is dropped). `AbortSignal.timeout(ms)` is the timeout | present; `inproc.test.js` "an aborted request rejects with the reason and the runtime still shuts down" |
| The waiter thread blocks every signal, so a process-directed signal reaches the JS thread, whose runtime handles it | present by construction (`src/waiter.c`); on Windows there are none to block |
| Worker threads: each `napi_env` has its own instance data and waiter | present by construction; not run |

## 5. Packaging

| Row | Verdict |
| --- | --- |
| One package `@tuco86/weida` for every platform: `lib/`, and per platform `prebuilds/<platform>-<arch>/weida.node` with `libweida` beside it, found through the addon's rpath (`$ORIGIN`, `@loader_path`; libweida.dylib's install name is `@rpath/libweida.dylib`); `lib/native.js` loads the one of `process.platform`-`process.arch` and names the missing path otherwise; on Windows `weida.dll` sits beside the addon, which links it and `node.lib` delay-loaded: `src/win32.c` binds `node.exe` to the running executable and loads `weida.dll` from the addon's directory | present; `package.sh` |
| The packed version is the workspace version (`0.1.0-alpha.11-dev` stays as it is, already semver); the committed `package.json` says `0.0.0-dev` | present; `package.sh` checks the installed version |
| The tarball installs offline into a fresh project and runs with no Rust toolchain on `PATH` | present; `package.sh` (`package.ps1` on Windows) runs `smoke.mjs` with `PATH` reduced to node, bun, deno and `/usr/bin:/bin` (`%SystemRoot%\System32` and `%SystemRoot%` on Windows) |
| Node 20 and up (`engines`), Bun and Deno load the same addon through `createRequire` | present; Node 26, Bun 1.4.4, Deno 2.9.8 run (`js-linux-amd64`, `js-linux-arm64`, `js-darwin-arm64`, `js-windows-amd64`) |
| macOS | present on arm64; `js-darwin-arm64` runs the same tests and smoke with runtimes `js_env` downloads |
| Windows | present on x64: `js-windows-amd64` builds the addon with MSVC (`build.ps1`) on wohnzimmer and runs the same tests and smoke with runtimes it downloads; the addon and `weida.dll` link the C runtime statically |
| npm registry | present: a `v*` tag publishes `@tuco86/weida` with the linux-x64, linux-arm64, darwin-arm64 and win32-x64 prebuilds to `https://git.doodleshnookie.net/api/packages/tuco86/npm/` under the dist-tag `latest`; `publish-npm` |

## 6. What is not mapped

Every pattern, stream, cursor and statistic the C ABI does not map yet is absent here exactly
as it is absent from the C ABI ([weida-c.md](weida-c.md) §5 to §7), and arrives when the ABI
maps it.

## 7. Sources

`crates/js/weida-js/src/` (`addon.c`, `waiter.c`, `win32.c`, `addon.h`), `lib/` (`index.js`,
`index.d.ts`, `native.js`, `errors.js`, `wait.js`), `build.sh`, `build.ps1`, `develop.sh`,
`package.sh`, `package.ps1`, `smoke.mjs`, `test/`; `.zeughaus-ci/js-linux-amd64.sh`,
`.zeughaus-ci/js-linux-arm64.sh`, `.zeughaus-ci/js-darwin-arm64.sh`,
`.zeughaus-ci/js-windows-amd64.ps1`, `.zeughaus-ci/publish-npm.sh`, `js_env` and `js_check` in
`.zeughaus-ci/build.sh`;
[weida-c.md](weida-c.md); [docs/c/](../c/README.md);
[decisions/0046](../decisions/0046-how-each-language-binds-libweida.md).
