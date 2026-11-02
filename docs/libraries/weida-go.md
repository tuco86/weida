# weida for Go (`crates/go/weida`, against the C ABI `libweida`) — feature parity

## 1. What a row means

The package is one Go module, `git.doodleshnookie.net/tuco86/weida/crates/go/weida` (package
`weida`): cgo against `weida.h` with `libweida` linked statically
([decisions/0046](../decisions/0046-how-each-language-binds-libweida.md)). Its API mirrors the C
ABI: one type per handle, one method per function with the `weida_<type>_` prefix dropped and
the rest in Go's spelling, and a `context.Context` first on every method that waits. Its
reference is therefore [weida-c.md](weida-c.md): every row present there is present here unless
a row below says otherwise, and every row absent there is absent here for the same reason. This
document lists what the Go surface adds or changes. Verdicts are **present** (with the test),
**refused** (with the reason) or **absent** (with the reason); there is no "partial".

The tests are `weida_test.go` (the unit tier: in-process transport only, each test well under a
second), `identity_test.go` (the unit tier: key material in and out of an identity, no file or
socket) and `quic_test.go` and `mtls_test.go` (the integration tier, build tag `integration`:
QUIC on localhost).
`cmd/weida-smoke` makes one QUIC round trip from a binary that carries `libweida` inside it.
Measured on Go 1.27.3 on linux-amd64 and linux-arm64 (the `go-linux-*` jobs); the unit tier
also under `-race` and `GOEXPERIMENT=cgocheck2` on the workstation.

## 2. Types

| C ABI | Go (`weida`) | Verdict |
| --- | --- | --- |
| `weida_runtime_config` | `RuntimeConfig{WorkerThreads uint32; ShutdownTimeout time.Duration}`, turned into a handle by `NewRuntime`; `NewRuntime(nil)` takes weida's defaults, a config sets both fields | present; `TestARuntimeWithNoWorkerThreadsIsARuntimeFailure` |
| `weida_runtime` | `NewRuntime(*RuntimeConfig)`, `.Listener()`, `.Requester(*ClientTLS)`, `.Shutdown(ctx)` (consumes), `.Close()` | present; every round-trip test |
| `weida_trust` | `NewTrust()`, `.AddPin(fp) error`, `.AddAnchor(pem []byte) error`, `.AddAnchorFile(path) error`, `.AddAnchorCertificates(...*x509.Certificate) error` (their PEM goes to `AddAnchor`; none or a nil one is `ErrInval`) | present; `TestAMalformedPinIsAnInvalidFingerprint`, `TestAWrongPinIsUntrustedAndCarriesTheFingerprint`, `TestArgumentErrors`, `TestAnAnchorAcceptsTheDialledNameAndRefusesAnother`, `TestIdentityAndAnchorFilesReadBack` |
| `weida_identity` | `GenerateIdentity()`, `GenerateIdentityFor(names ...string)`, `IdentityFromPEM(cert, key []byte)`, `IdentityFromPEMFile(path)`, `IdentityFromPEMFiles(certChain, key)` (nothing is read until first use: a missing file is `ErrTLS` there), `IdentityFromTLS(tls.Certificate)` (the key an `*ecdsa.PrivateKey`, `*rsa.PrivateKey` or `ed25519.PrivateKey`, through PKCS#8), `.Fingerprint() (string, error)`, `.CertificatePEM()` and `.ToPEM()` (`[]byte`; `ToPEM` holds the key and the caller wipes it with `clear`), `.Certificates() ([]*x509.Certificate, error)`, `.TLSCertificate() (tls.Certificate, error)` | present; `TestARoundTripOverQUICWithAPinnedKey`, `TestToPEMReadsBackWithTheSameFingerprint`, `TestCertificatesAreTheIdentitysKey`, `TestGenerateIdentityForNamesTheCertificate`, `TestIdentityFromTLSTakesGosKeyTypes`, `TestASignerThatIsNotExportableNamesCase2`, `TestArgumentErrors`, `TestIdentityAndAnchorFilesReadBack` |
| `weida_client_tls`, `weida_server_tls` | `NewClientTLS(*Trust)`, `(*ClientTLS).SetIdentity(*Identity)`, `NewServerTLS(*Identity)`, `(*ServerTLS).RequireClient(*Trust)`, `.RequireAnyKey()`; the argument is copied, the last call wins, an empty trust fails the bind with `ErrTLS` | present; `TestAPinnedClientKeyIsAcceptedAndTheServerSeesItsKeyAndChain`, `TestAnAnonymousOrUnknownClientIsRefusedWithTLSAndNothingReachesTheServer`, `TestABindingThatRequiresClientsFromAnEmptyTrustDoesNotBind`, `TestAnyKeyExposesTheFingerprintAndTheChain`, `TestArgumentErrors` |
| `weida_pem` | not exposed: `CertificatePEM` and `ToPEM` copy it into a `[]byte` and drop it (which wipes it) in the same cgo call | refused: a Go caller needs the bytes, not a handle |
| `weida_transfer_meta` | `TransferMeta{ContentType string}`, turned into a handle per call; `""` sets no content type | present; `TestAStreamedRoundTripCarriesTheBodyAndTheContentType` |
| `weida_listener` | `(*Listener).BindQUIC(ctx, addr, *ServerTLS) (*Binding, error)`, `.BindInproc(bus) (*LocalBinding, error)`, `.Replier(path)` | present |
| `weida_binding`, `weida_local_binding` | `(*Binding).LocalAddr()`, `(*LocalBinding).Bus()`, each a `string`, `""` once closed | present |
| `weida_requester` | `(*Requester).Connect(ctx, url) error`, `.Request(ctx, body []byte) (*IncomingTransfer, error)` | present; `TestAStreamedRoundTripCarriesTheBodyAndTheContentType` |
| `weida_replier` | `(*Replier).Path()`, `.Accept(ctx) (*IncomingRequest, error)` | present; `TestTwoGoroutinesAcceptingOnOneReplierBothGetARequest` |
| `weida_incoming_request` | `(*IncomingRequest).Meta()`, `.TakeBody()`, `.Reply(ctx, *TransferMeta) (*OutgoingTransfer, error)` (consumes) | present; `TestASecondReplyIsClosed` |
| `weida_incoming_transfer` | `(*IncomingTransfer).Meta()`, `.Read(p) (int, error)` implementing `io.Reader` (`io.EOF` at the end), `.ReadContext(ctx, p)`, `.Collect(ctx, maxBytes uint64) (*Message, error)` (consumes) | present; `TestReadsOf3BytesReassembleTheBody`, `TestACollectOverItsCeilingIsLimitExceeded`, `TestAReplyDroppedUnfinishedReadsAsCanceled` (through `io.ReadAll`) |
| `weida_outgoing_transfer` | `(*OutgoingTransfer).Write(p) (int, error)` implementing `io.Writer`, `.WriteContext(ctx, p)`, `.Finish() (*Delivery, error)` (consumes) | present; `Write` writes all of `p`, the Go loop R3c leaves to the caller, and returns how much it wrote when it fails |
| `weida_delivery` | `(*Delivery).Delivered(ctx) error` (consumes) | present |
| `weida_incoming_meta` | `IncomingMeta`, a struct copied out at once: `Endpoint`, `ContentType`, `Topic`, `PeerFingerprint` as `*string`, `ContentLen`, `Sequence` as `*uint64`, `nil` when absent; `PeerChainDER [][]byte`, the certificates the peer presented, leaf first, `nil` when absent, and `PeerChain() ([]*x509.Certificate, error)`, which parses them on each call (`ErrTLS` when one does not parse, so copying the metadata never fails over a certificate) | present; `TestAPinnedClientKeyIsAcceptedAndTheServerSeesItsKeyAndChain`, `TestAnyKeyExposesTheFingerprintAndTheChain`, `TestAnAnchorAcceptsTheDialledNameAndRefusesAnother` |
| `weida_msg` | `Message{Payload []byte; Meta IncomingMeta}`, copied out at once and the C message dropped in the same call | present |
| the ops (`*_op`) | not exposed: a waiting method starts its op, waits, and drops it with `defer`, which cancels it only when it did not finish | refused: an op is the C caller's way to wait, not a value a Go caller needs |
| the `_fd` functions, `weida_poll` | not exposed: the package hands the descriptor to Go's netpoller (§4) | refused: a goroutine waits by blocking, not on a descriptor |
| `weida_version`, `weida_strerror` | `Version()`, `Code.String()` | present; `cmd/weida-smoke`, `TestAnUnknownCodeIsAnUnknownError` |

The conversions to Go's types follow [0047](../decisions/0047-key-material-in-the-language-packages.md):
an `*x509.CertPool` is refused as an anchor, since it cannot be enumerated (`AddAnchorCertificates`
takes the certificates themselves), and any other `crypto.Signer` is case 2 of the decision: a key
that never leaves its store needs a signer handle, which weida does not have yet, so
`IdentityFromTLS` returns `ErrInval` naming it.

Every handle type has `Close() error` (an `io.Closer`), which releases its handle at once; one
never closed is released by a `runtime.AddCleanup` once it is unreachable. A call holds the
handle's read lock for its C call only; `Close` and the consuming methods take the write lock, so
no call starts after the C drop returned (R5), and a closed or consumed value answers every call
with `ErrClosed` without calling C. Closing a handle ends the waits parked on it, in any
goroutine, with `ErrClosed`. Present; `TestClosingAReplierEndsAPendingAcceptWithClosed`.

## 3. Errors

`*Error` with `Code` (the positive R9 code as a `Code`), `Message` (the `Display` text, or the
code's description), and the thread-local record as `LossCause` (0 or one of `LossIdleTimeout`
.. `LossTransportError`), `Fingerprint` (`""` when absent), `Max` (`-1` when absent) and
`OSCode`. There is one sentinel per code, `ErrRuntime` .. `ErrTransport` for the 24 variants plus
`ErrAgain`, `ErrIntr`, `ErrInval`, `ErrClosed` and `ErrPanic`, and `(*Error).Is` matches equal
codes, so a caller writes `errors.Is(err, weida.ErrUntrusted)` and `errors.As` for the record.
A goroutine may move to another OS thread between two cgo calls, so every fallible function is
called through a C wrapper in `weida_go.h` that copies the record in the same cgo call. A string
argument with a NUL in it is `ErrInval`, where C would see it cut short; a nil `*ClientTLS`,
`*ServerTLS`, `*Trust` or `*Identity` argument is `ErrInval` too. Present;
`TestEveryCodeMatchesItsSentinel`, `TestAWrongPinIsUntrustedAndCarriesTheFingerprint`.

## 4. Waiting

| Behaviour | Verdict |
| --- | --- |
| No call blocks in C: every C call passes `timeout_ms = 0`, and a try or op wait that would block returns `EAGAIN` to the Go side | present by construction (`weida_go.h`) |
| The waiting goroutine parks in Go's netpoller: on `EAGAIN` the package duplicates the handle's readiness descriptor (`F_DUPFD_CLOEXEC`), opens it with `os.NewFile` (the descriptor is non-blocking, R11, so the file joins the netpoller), and retries the call in `SyscallConn().Read`'s callback each time the descriptor turns readable | present; `TestTwoGoroutinesAcceptingOnOneReplierBothGetARequest` and `TestClosingAReplierEndsAPendingAcceptWithClosed` wait until their goroutines are in `IO wait` inside the package's `wait` |
| Every park watches a duplicate of its own, so two goroutines waiting on one handle neither queue behind each other on the file's read lock nor share a read deadline; both wake on news, and the one that finds nothing parks again | present; `TestTwoGoroutinesAcceptingOnOneReplierBothGetARequest` |
| A context ends a wait: `context.AfterFunc` sets a read deadline in the past, the call returns `ctx.Err()`, and an op is dropped, which cancels it; the runtime stays usable. A deadline is the timeout | present; `TestARequestWhoseContextExpiresLeavesTheRuntimeAbleToShutDown` |
| `Read` and `Write` without a context wait without bound | present by construction |
| Signals and `EINTR`: no thread waits in C, so `ErrIntr` never reaches a caller | present by construction |

## 5. Packaging

| Row | Verdict |
| --- | --- |
| `libweida` is linked statically: the `c-abi-linux-*` jobs build `libweida.a` with `cargo rustc -p weida-c --lib --crate-type staticlib` (not one of the manifest's crate types: as one it failed the Windows debug links on `weida.pdb`) and hand it on in `lib-static/`, and the caller names its directory in `CGO_LDFLAGS=-L<dir>`; the module commits nothing machine-specific | present; the `go-linux-*` jobs fail when `ldd` of the smoke binary lists `libweida` |
| The native libraries the static library needs are in `cgo.go`'s `#cgo linux LDFLAGS`, read with `--print native-static-libs` | present; linux-amd64 and linux-arm64 link and run |
| macOS | absent: `cgo.go` has the darwin link flags, read with `--print native-static-libs` on darwin-arm64, but no job builds or runs the package there |
| Windows | absent: no Windows Go job, and the package waits on POSIX descriptors |
| The module is used by path or a `replace` directive; `cgo.go` finds `weida.h` in the repository (`${SRCDIR}/../../weida-c/include`) | present; `develop.sh` and the jobs build in the checkout |
| A tagged Go module release | absent: no tag of this repository names the Go module |

## 6. What is not mapped

Every pattern, stream, cursor and statistic the C ABI does not map yet is absent here exactly as
it is absent from the C ABI ([weida-c.md](weida-c.md) §5 to §7), and arrives when the ABI maps it.

## 7. Sources

`crates/go/weida/` (`cgo.go`, `weida_go.h`, `errors.go`, `handle.go`, `runtime.go`,
`reqrep.go`, `go.mod`, `develop.sh`, `weida_test.go`, `identity_test.go`, `quic_test.go`,
`mtls_test.go`, `cmd/weida-smoke/`);
`crates/weida-c/Cargo.toml`; `.zeughaus-ci/build.sh` (`c_abi`), `.zeughaus-ci/go-linux-amd64.sh`,
`.zeughaus-ci/go-linux-arm64.sh`; [weida-c.md](weida-c.md); [docs/c/](../c/README.md);
[decisions/0046](../decisions/0046-how-each-language-binds-libweida.md).
