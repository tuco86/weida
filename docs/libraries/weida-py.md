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
| `weida_trust` | `Trust()`, `.add_pin(fp) -> Trust` | present; `test_errors.py::test_a_malformed_pin_is_an_invalid_fingerprint`, `test_sync.py::test_a_wrong_pin_raises_untrusted` |
| `weida_identity` | `Identity.generate()`, `Identity.from_pem(cert, key)`, `.fingerprint()` | present; `test_sync.py::test_a_streamed_round_trip[quic]` |
| `weida_client_tls`, `weida_server_tls` | `ClientTls(trust)`, `ServerTls(identity)` | present |
| `weida_transfer_meta` | `TransferMeta(content_type=None)`, a plain value turned into a handle per call | present; `test_sync.py::test_a_streamed_round_trip` |
| `weida_listener` | `Listener.bind_quic(addr, tls, timeout)`, `.bind_inproc(bus)`, `.replier(path)` | present |
| `weida_binding`, `weida_local_binding` | `Binding.local_addr()`, `LocalBinding.bus()` | present |
| `weida_requester` | `Requester.connect(url, timeout)`, `.request(body, timeout) -> IncomingTransfer` | present; `test_sync.py::test_a_streamed_round_trip`, `test_asyncio.py::test_a_round_trip_over_quic` |
| `weida_replier` | `Replier.path()`, `.fileno()`, `.accept(timeout)` | present; `test_sync.py::test_an_accept_with_nothing_pending_raises_again`, `test_asyncio.py::test_two_accepts_on_one_replier_serve_two_requests` |
| `weida_incoming_request` | `IncomingRequest.meta()`, `.take_body()`, `.reply(meta, timeout)` (consumes) | present; `test_sync.py::test_a_second_reply_raises_closed` |
| `weida_incoming_transfer` | `IncomingTransfer.meta()`, `.fileno()`, `.readinto(buffer, timeout)`, `.read(n, timeout)` (`b""` at the end), `.collect(max_bytes, timeout) -> Message` (consumes) | present; `test_sync.py::test_small_reads_reassemble_the_body`, `test_sync.py::test_a_collect_over_its_ceiling_raises_limit_exceeded`, `test_sync.py::test_a_reply_dropped_unfinished_reads_as_canceled` |
| `weida_outgoing_transfer` | `OutgoingTransfer.fileno()`, `.write(data, timeout)`, `.write_all(data, timeout)`, `.finish()` (consumes) | present; `write_all` is the Python loop R3c leaves to the caller, its timeout bounding the whole call |
| `weida_delivery` | `Delivery.delivered(timeout)` (consumes) | present |
| `weida_incoming_meta` | `IncomingMeta`, a frozen dataclass copied out at once | present |
| `weida_msg` | `Message(payload: bytes, meta: IncomingMeta)`, copied out at once | present |
| the ops (`*_op`) | not exposed: a waiting method starts its op, waits, and drops it in `finally`, which cancels it only when it did not finish | refused: an op is the C caller's way to wait, not a value a Python caller needs |
| `weida_poll` | not exposed | refused: `select`, `selectors` and the event loop take the `fileno()` descriptors directly |
| `weida_version` | `weida.version()` | present; `smoke.py` |

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
| CPython 3.9 and up | present by `requires-python`; only 3.13 is run |
| PyPy | absent: not run, no interpreter on the CI images |
| Free-threaded CPython (3.13t, 3.14t) | absent: not run, no interpreter on the CI images |
| PyPI | absent: nothing of this repository is on PyPI |

## 6. What the PyO3 binding had and this one does not

The PyO3 `weida-py` this package replaces reached all six patterns, RADIO/DISH, the cursor
surface and connection statistics. Each of those is absent here exactly as it is absent from
the C ABI ([weida-c.md](weida-c.md) §5 to §7), and returns when the ABI maps it. The new
package is not compatible with the old module's API (0045 decision 6).

## 7. Sources

`crates/py/weida-py/src/weida/` (`__init__.py`, `_native.py`, `_errors.py`, `_wait.py`,
`_values.py`, `_sync.py`, `asyncio.py`, `_cdef.h`), `hatch_build.py`, `pyproject.toml`,
`develop.sh`, `package.sh`, `smoke.py`, `tests/`; [weida-c.md](weida-c.md);
[docs/c/](../c/README.md); [decisions/0045](../decisions/0045-the-c-abi-is-the-boundary.md).
