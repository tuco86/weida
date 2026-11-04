# weida for Swift (`crates/swift/weida`, against the C ABI `libweida`) — feature parity

## 1. What a row means

The package is one Swift package with the module `Weida` over the C module `CWeida`, an
XCFramework of static `libweida.a` that `build-xcframework.sh` builds
([decisions/0046](../decisions/0046-how-each-language-binds-libweida.md)). Its API mirrors the C
ABI: one class per handle, one method per function with the `weida_<type>_` prefix dropped and
the rest in Swift's spelling, and `async throws` on every method that waits. Its reference is
therefore [weida-c.md](weida-c.md): every row present there is present here unless a row below
says otherwise, and every row absent there is absent here for the same reason. This document
lists what the Swift surface adds or changes. Verdicts are **present** (with the test),
**refused** (with the reason) or **absent** (with the reason); there is no "partial".

The tests are swift-testing suites in `Tests/WeidaTests/`: `UnitTests.swift` (the unit tier:
in-process transport only), `IdentityTests.swift` (the unit tier: key material in and out of an
identity, no file or socket) and `IntegrationTests.swift` (the integration tier, run with
`WEIDA_INTEGRATION=1`: QUIC on localhost, and identity and anchor files). Measured with Swift
6.4 and Xcode 26.6 on macOS 26.6 on darwin-arm64 (the `swift-darwin-arm64` job on atik).

## 2. Types

| C ABI | Swift (`Weida`) | Verdict |
| --- | --- | --- |
| `weida_runtime_config` | `RuntimeConfig(workerThreads: UInt32, shutdownTimeout: Duration)`, turned into a handle by `Runtime.owned`; `Runtime.owned()` takes weida's defaults, a config sets both fields | present; `aRuntimeWithNoWorkerThreadsIsARuntimeFailure` |
| `weida_runtime` | `Runtime.owned(_:)`, `.listener()`, `.requester(_: ClientTLS)`, `.shutdown() async throws` (consumes), `.close()` | present; every round-trip test |
| `weida_trust` | `Trust()`, `.addPin(_:)`, `.addAnchor(_: Data)` (PEM), `.addAnchor(_: [SecCertificate])` (their PEM goes to `addAnchor(_: Data)`; none is `.inval`), `.addAnchor(file: URL)`, each returning the trust | present; `aMalformedPinIsAnInvalidFingerprint`, `aWrongPinIsUntrustedAndCarriesTheFingerprint`, `anAnchorAcceptsTheDialledNameAndRefusesAnother`, `identityAndAnchorFilesReadBack`, `argumentErrors` |
| `weida_identity` | `Identity.generate()`, `.generate(for: [String])`, `Identity(pemCertificate:pemKey:)` (`Data`), `Identity(pemFile:)`, `Identity(pemCertificateFile:keyFile:)` (file `URL`s; nothing is read until first use: a missing file is `.tls` there), `Identity(certificates: [SecCertificate], privateKey: SecKey)`, `.fingerprint` (a throwing property), `.certificatePEM()` and `.toPEM()` (`Data`; `toPEM` holds the key and the caller wipes it with `resetBytes(in:)`), `.certificates() -> [SecCertificate]` | present; `toPEMReadsBackWithTheSameFingerprint`, `certificatesAreTheIdentitysKey`, `aSecKeyAndSecCertificatesMakeTheSameIdentity`, `anRSAKeyGoesToLibweidaAsPKCS8`, `identityAndAnchorFilesReadBack` |
| `weida_client_tls`, `weida_server_tls` | `ClientTLS(trust:)`, `.setIdentity(_:)`, `ServerTLS(identity:)`, `.requireClient(_: Trust)`, `.requireAnyKey()`; the argument is copied, the last call wins, an empty trust fails the bind with `.tls` | present; `aPinnedClientKeyIsAcceptedAndTheServerSeesItsKeyAndChain`, `anAnonymousOrUnknownClientIsRefusedWithTLSAndNothingReachesTheServer`, `aBindingThatRequiresClientsFromAnEmptyTrustDoesNotBind`, `anyKeyExposesTheFingerprintAndTheChain` |
| `weida_pem` | not exposed: `certificatePEM` and `toPEM` copy it into a `Data` and drop it, which wipes it | refused: a Swift caller needs the bytes, not a handle |
| `weida_transfer_meta` | `TransferMeta(contentType: String? = nil)`, a value turned into a handle per call | present; `aStreamedRoundTripCarriesTheBodyAndTheContentType` |
| `weida_listener` | `Listener.bindQUIC(_:tls:) async throws -> Binding`, `.bindInproc(_:)`, `.replier(_:)` | present |
| `weida_binding`, `weida_local_binding` | `Binding.localAddr`, `LocalBinding.bus`, each a `String`, `""` once closed | present |
| `weida_requester` | `Requester.connect(_:) async throws`, `.request(_: Data) async throws -> IncomingTransfer` | present; `aStreamedRoundTripCarriesTheBodyAndTheContentType` |
| `weida_replier` | `Replier.path`, `.accept() async throws -> IncomingRequest` | present; `twoTasksAcceptingOnOneReplierBothGetARequest` |
| `weida_incoming_request` | `IncomingRequest.meta()`, `.takeBody()`, `.reply(_: TransferMeta) async throws -> OutgoingTransfer` (consumes) | present; `aSecondReplyIsClosed` |
| `weida_incoming_transfer` | `IncomingTransfer.meta()`, `.read(upTo:) async throws -> Data` (empty at the end; a count of 0 is `.inval`), `.collect(maxBytes:) async throws -> Message` (consumes) | present; `readsOf3BytesReassembleTheBody`, `aCollectOverItsCeilingIsLimitExceeded`, `aReplyDroppedUnfinishedReadsAsCanceled` |
| `weida_outgoing_transfer` | `OutgoingTransfer.write(_:) async throws -> Int` (one try's worth), `.writeAll(_:) async throws` (the Swift loop R3c leaves to the caller), `.finish() throws -> Delivery` (consumes) | present |
| `weida_delivery` | `Delivery.delivered() async throws` (consumes) | present |
| `weida_incoming_meta` | `IncomingMeta`, a struct copied out at once: `endpoint`, `contentType`, `topic`, `peerFingerprint` as `String?`, `contentLen`, `sequence` as `UInt64?`, nil when absent; `peerChainDER: [Data]?`, the certificates the peer presented, leaf first, and `peerChain() throws -> [SecCertificate]?`, which parses them on each call (`.tls` when one does not parse, so copying the metadata never fails over a certificate) | present; `aPinnedClientKeyIsAcceptedAndTheServerSeesItsKeyAndChain`, `anyKeyExposesTheFingerprintAndTheChain`, `anAnchorAcceptsTheDialledNameAndRefusesAnother` |
| `weida_msg` | `Message(payload: Data, meta: IncomingMeta)`, copied out at once and the C message dropped in the same call | present |
| the ops (`*_op`) | not exposed: a waiting method starts its op, waits, and drops it with `defer`, which cancels it only when it did not finish | refused: an op is the C caller's way to wait, not a value a Swift caller needs |
| the `_fd` functions, `weida_poll` | not exposed: the package hands the descriptor to a `DispatchSource` (§4) | refused: a task waits by suspending, not on a descriptor |
| `weida_version`, `weida_strerror` | `Weida.version`, `WeidaError.Code.description` | present; `everyCodeIsTheHeadersCode` |

The conversions to Security's types follow
[0047](../decisions/0047-key-material-in-the-language-packages.md). A certificate is a
`SecCertificate`. A private key is a `SecKey` exported with `SecKeyCopyExternalRepresentation`:
an EC key on P-256, P-384 or P-521 becomes PKCS#8 through CryptoKit, an RSA key's PKCS#1 is
wrapped into PKCS#8 by the package. A key that cannot be exported, such as one in the Secure
Enclave, is case 2 of the decision: it needs a signer handle, which weida does not have yet, so
the initializer throws `.inval` naming it.

Every handle class has `close()`, which releases its handle at once; one never closed is released
when its last reference goes. A call holds the handle's lock for its C call only; `close()` and
the consuming methods take the lock too, so no call starts after the C drop returned (R5), and a
closed or consumed object answers every call with `.closed` without calling C. Closing a handle
ends the waits suspended on it, in any task, with `.closed`. Present;
`closingAReplierEndsAPendingAcceptWithClosed`, `aCallOnAClosedHandleIsClosed`.

## 3. Errors

`WeidaError` with `code` (a `WeidaError.Code`, one case per positive R9 code: the 24 variants
`runtime` .. `transport`, then `again`, `intr`, `inval`, `closed` and `panic`), `message` (the
`Display` text, or the code's description), and the thread-local record as `lossCause`
(`WeidaError.LossCause?`), `fingerprint` (`String?`), `max` (`Int64?`) and `osCode` (`Int32?`),
nil when absent. The record is read right after the failing call, in the same synchronous Swift
function, so no suspension point lets the task move to another thread in between. A string
argument with a NUL in it is `.inval`, where C would see it cut short. Present;
`everyCodeIsTheHeadersCode`, `aStringWithANulIsInval`, `aWrongPinIsUntrustedAndCarriesTheFingerprint`.

## 4. Waiting

| Behaviour | Verdict |
| --- | --- |
| No call blocks in C: every C call passes `timeout_ms = 0`, and a try or op wait that would block returns `EAGAIN` to the Swift side | present by construction (`Handle.swift`, `wait`) |
| The waiting task suspends in a `CheckedContinuation` on a one-shot `DispatchSource` read source over a duplicate of the handle's readiness descriptor (`F_DUPFD_CLOEXEC`), whose event handler cancels the source and resumes the task, which retries the call; the source's cancel handler closes the duplicate, so no source ever watches a descriptor libweida closed | present; `twoTasksAcceptingOnOneReplierBothGetARequest` and `closingAReplierEndsAPendingAcceptWithClosed` wait until their tasks are suspended on the handle |
| Every suspension watches a duplicate of its own, so two tasks waiting on one handle both wake on news, and the one that finds nothing suspends again | present; `twoTasksAcceptingOnOneReplierBothGetARequest` |
| Cancelling the task ends a wait with `CancellationError`, and an op is dropped, which cancels it; the runtime stays usable. A timeout is the caller's (a task group or a `Task` it cancels) | present; `aCancelledRequestEndsAndTheRuntimeStillShutsDown` |
| Closing a handle cancels the sources of every wait suspended on it and resumes them with `.closed`, before the C drop | present; `closingAReplierEndsAPendingAcceptWithClosed` |
| Signals and `EINTR`: no thread waits in C, so `.intr` never reaches a caller | present by construction |

## 5. Packaging

| Row | Verdict |
| --- | --- |
| `CWeida.xcframework`: static `libweida.a` for macOS (arm64 and x86_64, joined with `lipo`), iOS devices (arm64) and the iOS simulator (arm64 and x86_64), each slice with `weida.h` and the module map; `build-xcframework.sh` builds each slice with `cargo rustc -p weida-c --lib --crate-type staticlib --target <t>` (not one of the manifest's crate types: as one it failed the Windows debug links on `weida.pdb`) | present; the `swift-darwin-arm64` job builds every slice on the nightly and tags, the macOS arm64 slice alone on a push |
| The native libraries the static library needs (Security, CoreFoundation, iconv) are `linkerSettings` in `Package.swift`, read with `--print native-static-libs` | present; macOS links and runs |
| macOS 13 and up | present; the tests run on darwin-arm64 |
| iOS 16 and up, devices and the simulator | present for building and linking: the nightly and tags build the test bundle with `xcodebuild build-for-testing` for `generic/platform=iOS` and `generic/platform=iOS Simulator`, which links `libweida.a` into it; absent for running: no test runs on a device or a simulator |
| The iOS slices build with libc held at 0.2.189 in `Cargo.lock`: libc 0.2.190 put `SYSPROTO_CONTROL` behind `target_os = "macos"`, which nix's `socket` feature names for every Apple target (nix-rust/nix#2830) | present; the comment on `nix` in the workspace `Cargo.toml` says so |
| The XCFramework is published (a release asset, a remote `binaryTarget`) | absent: it is built, not published; the package refers to `build/CWeida.xcframework` by path |
| Linux, Windows | absent: the package waits on `DispatchSource` read sources over POSIX descriptors and imports Security; no job builds it there |

## 6. What is not mapped

Every pattern, stream, cursor and statistic the C ABI does not map yet is absent here exactly as
it is absent from the C ABI ([weida-c.md](weida-c.md) §5 to §7), and arrives when the ABI maps it.

The C ABI's *config* area is absent here: every row of [weida-c.md](weida-c.md) marked
(*config*), that is the rest of `RuntimeConfig`, `Limits`, `GuaranteeSet`, `ReconnectPolicy`,
the `TransferMeta` setters beyond the content type, `Requester::request_with`, the trace and
achieved getters and `OutgoingTransfer::trace`, `set_priority` and `expire_at`. This package
keeps its two configuration fields and the content type.

The C ABI's *refusals* area is absent here: the rows of [weida-c.md](weida-c.md) marked
(*refusals*), `IncomingRequest::refuse` with the `WEIDA_ERROR_CODE_*` constants and
`IncomingRequest::canceled`. A request is still declined only by dropping it, which answers
`NO_REPLY`.

The C ABI's *streams* area is absent here: the rows of [weida-c.md](weida-c.md) marked
(*streams*), `Requester::open` with `weida_reply_stream` and its `recv`. A request body is still
sent whole, as one buffer.

The C ABI's *stats* area is absent here: the rows of [weida-c.md](weida-c.md) marked
(*stats*), the connection statistics snapshot, the runtime's two counters, connection numbers
and the peer event queue.

The C ABI's *drain* area is absent here: the rows of [weida-c.md](weida-c.md) marked (*drain*),
`weida_runtime_drain` and its `weida_drained` report. A runtime still ends only by shutdown, which
is abortive.

The C ABI's *binding lifecycle* area is absent here: the rows of [weida-c.md](weida-c.md)
marked (*binding lifecycle*), a binding's close, hand-over and take-over with `weida_handoff`,
`weida_listener_bind_quic_socket`, `weida_binding_disconnect`, and relay serving
(`weida_listener_bind_relay`, `weida_listener_relay`, `weida_relay_request`,
`weida_binding_relay_events`). A QUIC binding still ends with its runtime's shutdown.

The C ABI's *local transports* area is absent here: the rows of [weida-c.md](weida-c.md)
marked (*local transports*), `weida_listener_bind_unix`, `weida_listener_bind_pipe` with
`weida_unix_binding` and `weida_pipe_binding`, and the local peer principal
(`weida_incoming_meta_peer_uid`, `_peer_gid`, `_peer_pid`, `_peer_sid`). A listener still
serves QUIC and in-process bindings only.

## 7. Sources

`crates/swift/weida/` (`Package.swift`, `build-xcframework.sh`, `Sources/Weida/` with
`Errors.swift`, `Handle.swift`, `Runtime.swift`, `ReqRep.swift`, `KeyMaterial.swift`,
`Tests/WeidaTests/`); `crates/weida-c/Cargo.toml`; `.zeughaus-ci/swift-darwin-arm64.sh`;
[weida-c.md](weida-c.md); [docs/c/](../c/README.md);
[decisions/0046](../decisions/0046-how-each-language-binds-libweida.md),
[decisions/0047](../decisions/0047-key-material-in-the-language-packages.md).
