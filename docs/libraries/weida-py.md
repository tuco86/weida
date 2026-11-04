# weida for Python (`crates/py/weida-py`, against the C ABI `libweida`) — feature parity

## 1. What a row means

The package is cffi in out-of-line ABI mode over `libweida`
([decisions/0045](../decisions/0045-the-c-abi-is-the-boundary.md)), and its API mirrors the C
ABI: one class per handle, one method per function with the `weida_<type>_` prefix dropped. Its
reference is therefore [weida-c.md](weida-c.md): every row present there is present here
unless a row below says otherwise, and every row absent there is absent here for the same
reason. This document lists what the Python surface adds or changes. Verdicts are **present**
(with the test), **refused** (with the reason) or **absent** (with the reason); there is no
"partial".

The tests are `crates/py/weida-py/tests/unit/` (no sockets, one second each) and
`tests/integration/` (localhost, ten seconds each); `smoke.py` runs from a fresh virtualenv
against the built wheel. Measured on CPython 3.13 on linux-amd64.

## 2. Classes

| C ABI | Python (`weida`, and `weida.asyncio` for the waiting ones) | Verdict |
| --- | --- | --- |
| `weida_runtime_config` | `RuntimeConfig(worker_threads, shutdown_timeout)`, seconds as floats | present; `test_errors.py::test_a_runtime_with_no_worker_threads_is_a_runtime_failure` |
| `weida_runtime` | `Runtime.owned(config)`, `.listener()`, `.requester(tls)`, `.shutdown(timeout)` (consumes), `.drop()` | present; every integration test |
| `weida_trust` | `Trust()`, `.add_pin(fp)`, `.add_anchor(pem \| x509.Certificate)`, `.add_anchor_file(path)`, each returning the trust | present; `test_errors.py::test_a_malformed_pin_is_an_invalid_fingerprint`, `test_sync.py::test_a_wrong_pin_raises_untrusted`, `test_mtls.py::test_an_anchor_accepts_the_dialled_name_and_refuses_another`, `test_mtls.py::test_identity_and_anchor_files_read_back` |
| `weida_identity` | `Identity.generate()`, `.generate_for(names)`, `.from_pem(cert, key)` (any bytes-like), `.from_pem_file(path)`, `.from_pem_files(cert_chain, key)` (`str`, `bytes` or `os.PathLike`, as `ssl` takes them), `.from_cryptography(cert, key)`, `.fingerprint()`, `.certificate_pem() -> bytes`, `.to_pem() -> bytearray` (holds the key; the caller wipes it), `.certificates() -> list[bytes]` (DER, leaf first) | present; the `test_identity.py` tests, `test_mtls.py::test_identity_and_anchor_files_read_back` |
| `weida_pem` | not exposed: copied out at once into `bytes` (`certificate_pem`) or a `bytearray` (`to_pem`) and dropped, which wipes it | refused: a Python caller needs the bytes, not the handle |
| `weida_client_tls`, `weida_server_tls` | `ClientTls(trust, identity=None)`, `ServerTls(identity, require_client=None \| Trust \| ANY_KEY)` | present; `test_mtls.py::test_a_pinned_client_key_is_accepted_and_each_side_sees_the_others_chain`, `test_mtls.py::test_an_anonymous_or_unknown_client_is_refused_with_tls_and_nothing_reaches_the_server`, `test_mtls.py::test_a_binding_that_requires_clients_from_an_empty_trust_does_not_bind`, `test_mtls.py::test_any_key_exposes_the_fingerprint_and_the_chain` |
| `weida_transfer_meta` | `TransferMeta(content_type=None)`, a plain value turned into a handle per call | present; `test_sync.py::test_a_streamed_round_trip` |
| `weida_listener` | `Listener.bind_quic(addr, tls, timeout)`, `.bind_inproc(bus)`, `.replier(path)` | present |
| `weida_binding`, `weida_local_binding` | `Binding.local_addr()`, `LocalBinding.bus()` | present |
| `weida_requester` | `Requester.connect(url, timeout)`, `.request(body, timeout) -> IncomingTransfer` | present; `test_sync.py::test_a_streamed_round_trip`, `test_asyncio.py::test_a_round_trip_over_quic` |
| `weida_replier` | `Replier.path()`, `.fileno()`, `.accept(timeout)` | present; `test_sync.py::test_an_accept_with_nothing_pending_raises_again`, `test_asyncio.py::test_two_accepts_on_one_replier_serve_two_requests` |
| `weida_incoming_request` | `IncomingRequest.meta()`, `.take_body()`, `.reply(meta, timeout)` (consumes) | present; `test_sync.py::test_a_second_reply_raises_closed` |
| `weida_incoming_transfer` | `IncomingTransfer.meta()`, `.fileno()`, `.readinto(buffer, timeout)`, `.read(n, timeout)` (`b""` at the end), `.collect(max_bytes, timeout) -> Message` (consumes) | present; `test_sync.py::test_small_reads_reassemble_the_body`, `test_sync.py::test_a_collect_over_its_ceiling_raises_limit_exceeded`, `test_sync.py::test_a_reply_dropped_unfinished_reads_as_canceled` |
| `weida_outgoing_transfer` | `OutgoingTransfer.fileno()`, `.write(data, timeout)`, `.write_all(data, timeout)`, `.finish()` (consumes) | present; `write_all` is the Python loop R3c leaves to the caller, its timeout bounding the whole call |
| `weida_delivery` | `Delivery.delivered(timeout)` (consumes) | present |
| `weida_incoming_meta` | `IncomingMeta`, a frozen dataclass copied out at once; `peer_chain` is a tuple of DER `bytes`, leaf first, or `None` | present; `test_mtls.py::test_any_key_exposes_the_fingerprint_and_the_chain` |
| `weida_msg` | `Message(payload: bytes, meta: IncomingMeta)`, copied out at once | present |
| the ops (`*_op`) | not exposed: a waiting method starts its op, waits, and drops it in `finally`, which cancels it only when it did not finish | refused: an op is the C caller's way to wait, not a value a Python caller needs |
| `weida_poll` | not exposed | refused: `select`, `selectors` and the event loop take the `fileno()` descriptors directly |
| `weida_version` | `weida.version()` | present; `smoke.py` |

The conversions between `cryptography`'s certificates and private keys, paths and bytes and
the ABI's PEM and DER follow
[0047](../decisions/0047-key-material-in-the-language-packages.md); `cryptography` is
optional, and without it bytes and paths work alone.

Every handle class releases its handle through `ffi.gc`, and `.drop()` and `with` release it at
once. A dropped or consumed object raises `Closed` without calling C.

## 3. Errors

`WeidaError` with `.code` (the positive `WEIDA_E*`) and `.detail`, and one subclass per code:
the 24 variants under their Rust names (`Runtime` is `RuntimeFailure`, so that it does not
shadow the `Runtime` class), plus `Again` (also a `TimeoutError`), `Interrupted`,
`InvalidArgument` (also a `ValueError`), `Closed` and `Panic`. `ConnectionLost.cause`,
`Untrusted.fingerprint`, `TooLarge.max` and `Io.os_errno` carry the thread-local record, read
right after the failing call on the same thread. Present;
`test_errors.py::test_every_code_raises_its_own_class_carrying_the_code`,
`test_sync.py::test_a_wrong_pin_raises_untrusted`.

## 4. Waiting

| Behaviour | Verdict |
| --- | --- |
| A `timeout` in seconds, `None` waiting forever, `0` only trying; a passed timeout raises `Again` | present; `test_sync.py::test_an_accept_with_nothing_pending_raises_again` |
| Ctrl-C ends a blocked call with `KeyboardInterrupt` on Unix, with no time slices: the C wait returns `EINTR`, Python runs its handler, and the call's in-flight work stays in the handle | present; `test_sync.py::test_ctrl_c_ends_a_blocked_accept`, which sends a process-directed SIGINT as a terminal does |
| Ctrl-C on Windows: each wait is cut into 100 ms slices, because Windows has no `EINTR` | present by construction; not run, no Windows Python on the CI |
| The GIL is released for every C call (cffi does it), so a blocked call holds no other Python thread | present |
| asyncio: one `loop.add_reader` registration per object and a FIFO of waiting coroutines, each retried with a timeout-0 try when the descriptor is readable | present; `test_asyncio.py::test_two_accepts_on_one_replier_serve_two_requests` |
| asyncio cancellation cancels the operation (its op is dropped) | present; `test_asyncio.py::test_a_cancelled_request_ends_and_the_runtime_still_shuts_down` |
| The Windows Proactor loop, which cannot watch a descriptor: the wait runs in the default executor, and cancellation drops the object, which ends that wait with `Closed` | present by construction; not run |

## 5. Packaging

| Row | Verdict |
| --- | --- |
| One `py3-none-<platform>` wheel per platform, with `libweida` and the generated `_ffi.py` inside | present; `package.sh` refuses any other tag |
| The version is the workspace version in PEP 440 spelling (`0.1.0-alpha.11-dev` is `0.1.0a11.dev0`) | present; `smoke.py` |
| The wheel runs with no Rust toolchain on `PATH` | present; `package.sh` runs `smoke.py` with `PATH=/usr/bin:/bin` |
| CPython 3.9 and up | present by `requires-python`; 3.13 (the `c-abi-linux-*` jobs, editable install) and 3.14 (the `python-linux-*` jobs, the wheel) are run |
| PyPy | present: PyPy 3.11 runs the tests against the wheel in the `python-linux-*` jobs, amd64 and arm64 |
| Free-threaded CPython (3.13t, 3.14t) | 3.14t present: the `python-linux-*` jobs run the tests against the wheel and check that importing `weida` leaves the GIL disabled; 3.13t not run |
| `cryptography` | optional: imported per call, never a dependency; installed for the CPython test runs, absent on 3.14t and PyPy | present; `test_identity.py::test_without_cryptography_bytes_convert_and_objects_are_type_errors` |
| macOS | absent: `_native.py` and `hatch_build.py` load and package `libweida.dylib`, but no job builds or runs the package there; atik's Python is the Xcode one (`check-darwin-arm64.sh`) |
| Windows | absent: `_native.py` and `hatch_build.py` load and package `weida.dll`, and §4's Windows waits are written, but no job builds or runs the package there; wohnzimmer has no Python (`windows.ps1`) |
| PyPI | absent: nothing of this repository is on PyPI |

## 6. What the PyO3 binding had and this one does not

The PyO3 `weida-py` this package replaces reached all six patterns, RADIO/DISH, the cursor
surface and connection statistics. Each of those is absent here exactly as it is absent from
the C ABI ([weida-c.md](weida-c.md) §5 to §7), and returns when the ABI maps it. The new
package is not compatible with the old module's API (0045 decision 6).

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

## 7. Sources

`crates/py/weida-py/src/weida/` (`__init__.py`, `_native.py`, `_errors.py`, `_wait.py`,
`_values.py`, `_crypto.py`, `_sync.py`, `asyncio.py`, `_cdef.h`), `hatch_build.py`, `pyproject.toml`,
`develop.sh`, `package.sh`, `smoke.py`, `tests/`; [weida-c.md](weida-c.md);
[docs/c/](../c/README.md); [decisions/0045](../decisions/0045-the-c-abi-is-the-boundary.md).
