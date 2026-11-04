# weida for the JVM and Android (`crates/jvm/weida-jvm`, against the C ABI `libweida`) — feature parity

## 1. What a row means

The package is one JNI library written in C against `weida.h`, shared by the desktop JVM and
Android ([decisions/0046](../decisions/0046-how-each-language-binds-libweida.md)), with a Java
API on top in the package `net.doodleshnookie.weida` (the coordinates of
[0019](../decisions/0019-jvm-binding.md) §4.5). Its API mirrors the C ABI: one class per handle,
one method per function with the `weida_<type>_` prefix dropped and the rest in camelCase. Its
reference is therefore [weida-c.md](weida-c.md): every row present there is present here unless
a row below says otherwise, and every row absent there is absent here for the same reason. This
document lists what the Java surface adds or changes. Verdicts are **present** (with the test),
**refused** (with the reason) or **absent** (with the reason); there is no "partial".

The tests are `crates/jvm/weida-jvm/src/test/java/net/doodleshnookie/weida/`: `ErrorsTest`,
`IdentityTest` and `InprocTest` (no files or sockets, in-process transport only, one second each)
and `QuicTest` and `MtlsTest`, tagged `io` (QUIC on
localhost, ten seconds each). `test.sh` runs them with the JUnit Platform Console Standalone
6.1.3 and `-Xcheck:jni`, the unit tier on every push and both on the nightly and tags;
`smoke/Smoke.java` makes one QUIC round trip, and `package.sh` runs it from a fresh directory
holding only the jar and the native libraries. Measured on Temurin 25 on linux-amd64 and
linux-arm64 (the `jvm-linux-*` jobs) and OpenJDK 27 on linux-amd64. Android is built and
compiled against, not run (§5).

## 2. Classes

| C ABI | Java (`net.doodleshnookie.weida`) | Verdict |
| --- | --- | --- |
| `weida_runtime_config` | `RuntimeConfig`, a value with `workerThreads(int)` and `shutdownTimeout(long, TimeUnit)` (whole milliseconds), turned into a handle by `Runtime.owned` | present; `ErrorsTest.aRuntimeWithNoWorkerThreadsIsARuntimeFailure` |
| `weida_runtime` | `Runtime.owned()`, `Runtime.owned(RuntimeConfig)`, `.listener()`, `.requester(ClientTls)`, `.shutdownAsync()` and `.shutdown(timeout, unit)` (consumes), `.close()` | present; every round-trip test. The name shadows `java.lang.Runtime`: a file that imports `net.doodleshnookie.weida.*` also imports `net.doodleshnookie.weida.Runtime` by name |
| `weida_trust` | `new Trust()`, `.addPin(String)`, `.addAnchor(byte[])`, `.addAnchor(X509Certificate)` and `.addAnchorFile(File)`, each returning the trust | present; `ErrorsTest.aMalformedPinIsAnInvalidFingerprint`, `QuicTest.aWrongPinIsUntrustedCarryingTheFingerprint`, `MtlsTest.anAnchorAcceptsTheDialledNameAndRefusesAnother`, `MtlsTest.identityAndAnchorFilesReadBack` |
| `weida_identity` | `Identity.generate()`, `Identity.generateFor(String...)`, `Identity.fromPem(byte[], byte[])`, `Identity.fromPemFile(File)`, `Identity.fromPemFiles(File, File)`, `Identity.from(PrivateKey, X509Certificate...)`, `Identity.fromKeyStore(KeyStore, String, char[])`, `.fingerprint()`, `.certificatePem()` and `.toPem()` as `byte[]` (`toPem()` holds the private key: the caller wipes it with `Arrays.fill`), `.certificates()` as `X509Certificate[]`, leaf first | present; `QuicTest.aRoundTripOverQuicWithAPinnedKey`, `IdentityTest.toPemReadsBackWithTheSameFingerprint`, `IdentityTest.certificatesAreTheIdentitysKey`, `IdentityTest.generateForNamesTheCertificate`, `IdentityTest.fromTakesJavasKeyTypes`, `IdentityTest.fromKeyStoreReadsAPkcs12Entry`, `IdentityTest.aKeyThatCannotBeExportedNamesCase2`, `IdentityTest.argumentErrors`, `MtlsTest.identityAndAnchorFilesReadBack` |
| `weida_client_tls`, `weida_server_tls` | `new ClientTls(Trust)`, `.setIdentity(Identity)`; `new ServerTls(Identity)`, `.requireClient(Trust)`, `.requireAnyKey()`; each setter returns its terms | present; `MtlsTest.aPinnedClientKeyIsAcceptedAndTheServerSeesItsKeyAndChain`, `MtlsTest.anAnonymousOrUnknownClientIsRefusedWithTlsAndNothingReachesTheServer`, `MtlsTest.aBindingThatRequiresClientsFromAnEmptyTrustDoesNotBind`, `MtlsTest.anyKeyExposesTheFingerprintAndTheChain`, `IdentityTest.argumentErrors` |
| `weida_transfer_meta` | `TransferMeta`, a value with `contentType(String)`, turned into a handle per call; `null` is empty metadata | present; `InprocTest.aStreamedRoundTripCarriesTheBodyAndTheContentType` |
| `weida_listener` | `Listener.bindQuicAsync(addr, ServerTls)` returning `CompletableFuture<Binding>` and `.bindQuic(addr, tls, timeout, unit)`, `.bindInproc(bus)`, `.replier(path)` | present |
| `weida_binding`, `weida_local_binding` | `Binding.localAddr()`, `LocalBinding.bus()` | present |
| `weida_requester` | `Requester.connectAsync(url)` and `.connect(url, timeout, unit)`, `.requestAsync(byte[])` and `.request(body, timeout, unit)` returning the `IncomingTransfer` | present; `InprocTest.aStreamedRoundTripCarriesTheBodyAndTheContentType` |
| `weida_replier` | `Replier.path()`, `.acceptAsync()` and `.accept(timeout, unit)` | present; `InprocTest.twoConcurrentAcceptsOnOneReplierServeTwoRequests`, `InprocTest.anAcceptThatTimesOutThrowsAgainAndTheNextOneServes` |
| `weida_incoming_request` | `IncomingRequest.meta()`, `.takeBody()`, `.replyAsync(TransferMeta)` and `.reply(meta, timeout, unit)` (consumes) | present; `InprocTest.aSecondReplyIsClosed` |
| `weida_incoming_transfer` | `IncomingTransfer.meta()`, `.readAsync(byte[], off, len)` and `.read(.., timeout, unit)` (the count, -1 at the end, as `InputStream.read` does), `.collectAsync(maxBytes)` and `.collect(maxBytes, timeout, unit)` returning a `Message` (consumes; the ceiling is at most `Integer.MAX_VALUE - 8`, the largest Java array) | present; `InprocTest.readsOf3BytesReassembleTheBody`, `InprocTest.aCollectOverItsCeilingIsLimitExceeded`, `InprocTest.aReplyClosedUnfinishedReadsAsCanceled` |
| `weida_outgoing_transfer` | `OutgoingTransfer.writeAsync(byte[], off, len)` and `.write(..)`, `.writeAllAsync(byte[])` and `.writeAll(data, timeout, unit)`, `.finish()` returning the `Delivery` (consumes) | present; `writeAll` is the Java loop R3c leaves to the caller, its timeout bounding the whole call |
| `weida_delivery` | `Delivery.deliveredAsync()` and `.delivered(timeout, unit)` (consumes) | present |
| `weida_incoming_meta` | `IncomingMeta`, an immutable value copied out at once: `endpoint()`, `contentType()`, `topic()`, `peerFingerprint()` as `String` or `null`, `contentLen()` and `sequence()` as `OptionalLong` (unsigned 64-bit values in a `long`), `peerChain()` as `X509Certificate[]`, leaf first, or `null`; the DER is kept and parsed on each call, so copying the metadata never fails over a certificate, and a certificate that does not parse throws `TLS` | present; `MtlsTest.aPinnedClientKeyIsAcceptedAndTheServerSeesItsKeyAndChain`, `MtlsTest.anyKeyExposesTheFingerprintAndTheChain`, `MtlsTest.anAnchorAcceptsTheDialledNameAndRefusesAnother` |
| `weida_msg` | `Message`: `payload()` as `byte[]`, `meta()`, copied out at once | present |
| `weida_pem` | not exposed: copied into a `byte[]` and dropped (which wipes its bytes) inside the native call | refused: a Java caller needs the bytes, not a handle |
| the ops (`*_op`) | not exposed: a waiting method starts its op, waits, and closes it when the wait ends, which cancels it only when it did not finish | refused: an op is the C caller's way to wait, not a value a Java caller needs |
| the `_fd` functions, `weida_poll` | not exposed: the waiter thread polls the descriptors (§4) | refused: the JVM has no API that watches a raw descriptor; NIO selectors take channels only |
| `weida_version`, `weida_strerror` | `Weida.version()`, `Weida.strerror(int)` | present; `smoke/Smoke.java`, `ErrorsTest.strerrorOfAnUnknownCodeIsUnknownError` |

The conversions follow [0047](../decisions/0047-key-material-in-the-language-packages.md): a key
whose `getEncoded()` is `null` (an Android Keystore key) is case 2 and `Identity.from` refuses
it, naming 0047, until the ABI has a signer handle.

Every handle class is `AutoCloseable`; `close()` releases the handle now, a second `close()`
does nothing, and a closed or consumed object throws `CLOSED` without calling C. Its pending
futures complete exceptionally with `CLOSED`. A call takes a lease on the handle for its
duration, so a `close()` racing it defers the C drop until the call has returned (R5), and a
consuming method waits for the leases of calls in progress. A handle never closed is dropped by
the daemon thread `weida-cleaner` once the garbage collector finds it unreachable (a
`PhantomReference`; Android API 24 has no `java.lang.ref.Cleaner`). Present;
`ErrorsTest.aClosedObjectThrowsClosedWithoutCallingC`,
`InprocTest.closingAReplierCompletesItsPendingAcceptWithClosed`,
`InprocTest.aBindingNeverClosedIsDroppedOnceUnreachableAndFreesItsBus`.

## 3. Errors

One class, `WeidaException extends RuntimeException`, with `code()` (the enum
`WeidaException.Code`, one constant per R9 code: `RUNTIME` to `TRANSPORT`, then `AGAIN`, `INTR`,
`INVAL`, `CLOSED`, `PANIC`), `errno()` (the positive code), `getMessage()` (the `Display` text),
and the thread-local record as `lossCause()` (`WeidaException.LossCause` or `null`),
`fingerprint()` (`String` or `null`), `max()` (`OptionalLong`) and `osCode()` (`OptionalInt`),
read by the JNI glue in the same native call that failed. A Java caller branches on `code()`,
so there are no subclasses. Present; `ErrorsTest.everyStatusCodeHasItsOwnConstantAndLibweidasText`,
`QuicTest.aWrongPinIsUntrustedCarryingTheFingerprint`.

Strings cross as UTF-8, converted by the glue rather than through JNI's modified UTF-8: a Java
string holding a NUL character is refused with `INVAL` before C sees it, because a C string
cannot carry one; a string out keeps an interior NUL as U+0000 and a supplementary character as
a surrogate pair. Present; `ErrorsTest.aStringHoldingANulIsAnInvalidArgument`.

## 4. Waiting

| Behaviour | Verdict |
| --- | --- |
| No Java thread blocks inside C: every native call passes `timeout_ms = 0`, and the glue returns `AGAIN` as a value (0, -1 or "not done") instead of throwing it | present by construction (`src/main/c/weida_jni.c`) |
| One daemon thread per process, `weida-waiter`, runs `weida_poll` over the descriptors of every pending wait plus a wake pipe; a registration is one-shot, and when its descriptor turns readable the timeout-0 attempt runs again there. A result completes the future through `ForkJoinPool.commonPool()`, so no user stage runs on the waiter thread | present; every `InprocTest` past its first `AGAIN` |
| A registration holds a lease on its handle until the poll round after it was removed has returned, so no descriptor is closed while it is polled | present by construction (`Waiter.java`) |
| Two waits on one handle (two `acceptAsync` on a replier) both watch its descriptor and are both retried; the one that finds nothing watches again | present; `InprocTest.twoConcurrentAcceptsOnOneReplierServeTwoRequests` |
| Every waiting operation twice: `xAsync(..)` returns a `CompletableFuture`, `x(.., timeout, unit)` waits for it; a passed timeout cancels the operation and throws `AGAIN`, an interrupt cancels it and rethrows `InterruptedException` | present; `InprocTest.anAcceptThatTimesOutThrowsAgainAndTheNextOneServes` |
| `cancel` on the returned future unregisters the wait and closes its op, which aborts the operation (R3b); a cancelled accept or read loses nothing, its in-flight work stays in the handle (R3a) | present; `InprocTest.aCancelledRequestEndsAndTheRuntimeStillShutsDown` |
| Signals: the JVM handles them itself; an `EINTR` of the waiter's poll is retried there | present by construction (`Waiter.java`) |
| An idle JVM exits: the waiter and cleaner threads are daemons | present; `smoke/Smoke.java` exits on its own |

## 5. Android

| Row | Verdict |
| --- | --- |
| The JNI library for arm64-v8a, armeabi-v7a, x86_64 and x86, compiled with the NDK's clang for API 24 against that ABI's `libweida.so`, `jni.h` from the NDK's sysroot, each checked for its ELF machine and its `NEEDED libweida.so` | present; `android-libweida` (nightly and tags), handed on as `jniLibs/<abi>/libweida_jni.so` beside `libweida.so` |
| The Java sources use only what Android API 24 has: no `java.time`, `java.lang.ref.Cleaner`, `java.lang.foreign` or records, and no lambdas in the main sources, because API 24's `android.jar` has no `java.lang.invoke` | present; `android-libweida` compiles them against `platforms/android-24/android.jar`, which fails on any class or method API 24 lacks. That compile is at language level 8: javac compiles for 9 and up only against a modular platform, which `android.jar` is not |
| Loading on Android: `System.loadLibrary("weida_jni")` from the APK's `jniLibs`, `libweida.so` beside it | present by construction (`Native.java`); not run |
| Run on a device or an emulator | absent: no job has one |
| Paths are `java.io.File` and PEM is encoded by `Pem.java`, because API 24 has no `java.nio.file` and no `java.util.Base64` | present; `android-libweida` |
| An AAR | absent: no Gradle in this repository's CI, so 0019 §4.5's Gradle build is not done; the libraries and sources are what an app's build takes |

## 6. Packaging

| Row | Verdict |
| --- | --- |
| `build/weida-jvm.jar` (class files for Java 17, `--release 17`), with the native libraries beside it in `build/native/`: `libweida_jni.so` with the rpath `$ORIGIN` (`@loader_path` on macOS) and `libweida.so` | present; `build.sh` |
| The jar's `Implementation-Version` is the workspace version; no version is committed in the package | present; `smoke/Smoke.java` checks it against `Weida.version()` |
| The jar and the native libraries run alone with no Rust toolchain on `PATH` | present; `package.sh` runs `smoke/Smoke.java` from a fresh directory with `PATH` reduced to `$JAVA_HOME/bin:/usr/bin:/bin` |
| The JNI library is found through the system property `weida.jni.path`, else `java.library.path` | present; every test run and `package.sh` |
| The native libraries inside the jar, extracted at load | absent: they ship beside the jar |
| Maven Central | absent: nothing of this repository is published there |
| macOS | absent: `build.sh` has the macOS include directory and link flags, but no job builds or runs the package there |
| Windows | absent: no Windows JVM job, and the waiter's wake pipe is POSIX |

## 7. What is not mapped

Every pattern, stream, cursor and statistic the C ABI does not map yet is absent here exactly
as it is absent from the C ABI ([weida-c.md](weida-c.md) §5 to §7), and arrives when the ABI
maps it.

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

The sources and ALPN of the C ABI's *identity* area are absent here: `weida_trust_is_empty`,
the ALPN setters (`weida_client_tls_set_alpn`, `weida_server_tls_set_alpn`), the `_source` forms
of the TLS terms, `weida_identity_source`, `weida_trust_source`, their weak forms,
`weida_files_options`, and the event queue `weida_identity_events` with `weida_identity_event`.
TLS terms still take an identity and a trust as values, under `weida/0`.

Push/Pull, Pub/Sub, PAIR, SURVEY, BUS and RADIO/DISH of the C ABI's *patterns* area are absent
here: `weida_pusher`, `weida_puller`, `weida_publisher`, `weida_subscriber`, `weida_fan_out`,
`weida_topic_drops`, `weida_incoming_meta_gap`, `weida_paired`, `weida_surveyor`,
`weida_survey_run`, `weida_survey_answer`, `weida_respondent`, `weida_bus_member`, `weida_radio`,
`weida_segment`, `weida_segment_terms`, `weida_dish`, `weida_join_terms`, `weida_received`,
`weida_dish_drops`, `weida_incoming_meta_segment` and `weida_incoming_meta_layer`.

The C ABI's *cursors* area is absent here: `weida_transfer_meta_set_report`,
`weida_transfer_meta_set_report_mode`, `weida_incoming_meta_report`,
`weida_incoming_meta_report_mode`, `weida_incoming_meta_report_id`,
`weida_outgoing_transfer_cursors`, `weida_cursors`, `weida_cursor_set`,
`weida_incoming_transfer_reporter` and `weida_reporter`.

The C ABI's *flows* area is absent here: `weida_runtime_peer`, `weida_peer`,
`weida_listener_acceptor`, `weida_acceptor`, `weida_incoming`, `weida_flow_meta`, `weida_flow`,
`weida_incoming_flow`, `weida_datagram` and `weida_path_stats`.

## 8. Sources

`crates/jvm/weida-jvm/src/main/c/weida_jni.c`, `src/main/java/net/doodleshnookie/weida/`
(`Native.java`, `Handle.java`, `Waiter.java`, `Wait.java`, `WeidaException.java` and one file
per class), `build.sh`, `test.sh`, `develop.sh`, `package.sh`, `smoke/Smoke.java`,
`src/test/`; `.zeughaus-ci/jvm-linux-amd64.sh`, `.zeughaus-ci/jvm-linux-arm64.sh`,
`.zeughaus-ci/android-libweida.sh`, `.zeughaus-ci/jvm.Containerfile`,
`.zeughaus-ci/android.Containerfile`; [weida-c.md](weida-c.md); [docs/c/](../c/README.md);
[decisions/0046](../decisions/0046-how-each-language-binds-libweida.md),
[decisions/0019](../decisions/0019-jvm-binding.md).
