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

Python shapes. A duration is seconds as an `int` or `float` and its name drops `_ms`
(`shutdown_timeout`, `expire_after`); an optional one takes `None`. An enumeration is an
`enum.IntEnum` in `weida`, named after weida's Rust type, its members the suffixes of the
`WEIDA_<TYPE>_*` constants (`weida.Congestion.BBR`); an argument must be a member of that class,
anything else is a `TypeError`, and a getter returns a member. A configuration value
(`RuntimeConfig`, `Limits`, `GuaranteeSet`, `ReconnectPolicy`) takes keyword fields only; an
unknown one is a `TypeError` naming it, and the values are converted when the call that takes it
builds the handle (`Runtime.owned`), where a wrong type is a `TypeError` and a negative or
non-finite number a `ValueError`. A value the C ABI copies out at once is a frozen dataclass.

The tests are `crates/py/weida-py/tests/unit/` (no sockets: the in-process transport only, one
second each) and `tests/integration/` (localhost, ten seconds each); `smoke.py` runs from a fresh
virtualenv against the built wheel. Measured on CPython 3.13 on linux-amd64.

## 2. Classes

| C ABI | Python (`weida`, and `weida.asyncio` for the waiting ones) | Verdict |
| --- | --- | --- |
| `weida_runtime_config`, `weida_limits`, `weida_guarantee_set`, `weida_reconnect_policy` | `RuntimeConfig(**fields)` with every field of `weida_runtime_config` but the resolver (`worker_threads`, `shutdown_timeout`, `max_connections`, `max_connections_per_peer`, `endpoint_queue`, `max_resolved_addresses`, `connect_attempt_timeout`, `send_timeout`, `outbox_messages`, `outbox_bytes`, `outbox_full`, `max_handshakes`, `retry_above_handshakes`, `discovery`) and the nested `limits=Limits(**fields)` (every `Limits` field), `guarantees=GuaranteeSet(**fields)` (every dimension) and `reconnect=ReconnectPolicy(**fields)` or `ReconnectPolicy.never(**fields)`; converted when `Runtime.owned` builds the handles (§1) | present; `test_config.py::test_a_full_configuration_builds_a_runtime`, `test_config.py::test_configuration_input_errors_raise_at_once`, `test_config.py::test_a_guarantee_the_dialler_does_not_offer_raises_negotiation`, `test_errors.py::test_a_runtime_with_no_worker_threads_is_a_runtime_failure` |
| `weida_runtime` | `Runtime.owned(config)`, `.listener()`, `.requester(tls)`, `.shutdown(timeout)` (consumes), `.drop()` | present; every integration test |
| `weida_trust` | `Trust()`, `.add_pin(fp)`, `.add_anchor(pem \| x509.Certificate)`, `.add_anchor_file(path)`, each returning the trust | present; `test_errors.py::test_a_malformed_pin_is_an_invalid_fingerprint`, `test_sync.py::test_a_wrong_pin_raises_untrusted`, `test_mtls.py::test_an_anchor_accepts_the_dialled_name_and_refuses_another`, `test_mtls.py::test_identity_and_anchor_files_read_back` |
| `weida_identity` | `Identity.generate()`, `.generate_for(names)`, `.from_pem(cert, key)` (any bytes-like), `.from_pem_file(path)`, `.from_pem_files(cert_chain, key)` (`str`, `bytes` or `os.PathLike`, as `ssl` takes them), `.from_cryptography(cert, key)`, `.fingerprint()`, `.certificate_pem() -> bytes`, `.to_pem() -> bytearray` (holds the key; the caller wipes it), `.certificates() -> list[bytes]` (DER, leaf first) | present; the `test_identity.py` tests, `test_mtls.py::test_identity_and_anchor_files_read_back` |
| `weida_pem` | not exposed: copied out at once into `bytes` (`certificate_pem`) or a `bytearray` (`to_pem`) and dropped, which wipes it | refused: a Python caller needs the bytes, not the handle |
| `weida_client_tls`, `weida_server_tls` | `ClientTls(trust, identity=None)`, `ServerTls(identity, require_client=None \| Trust \| ANY_KEY)` | present; `test_mtls.py::test_a_pinned_client_key_is_accepted_and_each_side_sees_the_others_chain`, `test_mtls.py::test_an_anonymous_or_unknown_client_is_refused_with_tls_and_nothing_reaches_the_server`, `test_mtls.py::test_a_binding_that_requires_clients_from_an_empty_trust_does_not_bind`, `test_mtls.py::test_any_key_exposes_the_fingerprint_and_the_chain` |
| `weida_transfer_meta` | `TransferMeta(content_type=None, *, content_len=None, trace=None, topic=None, achieved=None)`, a plain value turned into a handle per call; `trace` a traceparent (a malformed one raises `InvalidArgument`), `achieved` an `Acknowledgement` | present; `test_sync.py::test_a_streamed_round_trip`, `test_config.py::test_metadata_travels_with_a_request_and_its_reply` |
| `weida_listener` | `Listener.bind_quic(addr, tls, timeout)`, `.bind_inproc(bus)`, `.replier(path)` | present |
| `weida_binding`, `weida_local_binding` | `Binding.local_addr()`, `LocalBinding.bus()` | present |
| `weida_requester` | `Requester.connect(url, timeout)`, `.request(body, timeout) -> IncomingTransfer`, `.request_with(meta, body, timeout) -> IncomingTransfer` | present; `test_sync.py::test_a_streamed_round_trip`, `test_asyncio.py::test_a_round_trip_over_quic`, `test_config.py::test_metadata_travels_with_a_request_and_its_reply`, `test_asyncio.py::test_request_with_carries_metadata_on_the_loop` |
| `weida_replier` | `Replier.path()`, `.fileno()`, `.accept(timeout)` | present; `test_sync.py::test_an_accept_with_nothing_pending_raises_again`, `test_asyncio.py::test_two_accepts_on_one_replier_serve_two_requests` |
| `weida_incoming_request` | `IncomingRequest.meta()`, `.take_body()`, `.reply(meta, timeout)` (consumes), `.refuse(code, timeout)` (consumes; `code` an `ErrorCode`, anything else a `TypeError` that consumes nothing), `.canceled(timeout)` (the watch starts at the call and outlives the request; in `weida.asyncio` a plain method that starts it and returns the awaitable, so it can be taken before `reply` or `refuse`) | present; `test_sync.py::test_a_second_reply_raises_closed`, `test_refusals.py::test_a_refused_request_raises_the_refused_codes_error_at_the_requester`, `test_refusals.py::test_a_refusal_code_that_is_not_an_error_code_is_a_type_error_and_leaves_the_request_usable`, `test_refusals.py::test_canceled_returns_once_the_requester_gives_up`, `test_asyncio.py::test_canceled_starts_at_the_call_and_returns_once_the_requester_gives_up` |
| `weida_incoming_transfer` | `IncomingTransfer.meta()`, `.fileno()`, `.readinto(buffer, timeout)`, `.read(n, timeout)` (`b""` at the end), `.collect(max_bytes, timeout) -> Message` (consumes) | present; `test_sync.py::test_small_reads_reassemble_the_body`, `test_sync.py::test_a_collect_over_its_ceiling_raises_limit_exceeded`, `test_sync.py::test_a_reply_dropped_unfinished_reads_as_canceled` |
| `weida_outgoing_transfer` | `OutgoingTransfer.fileno()`, `.write(data, timeout)`, `.write_all(data, timeout)`, `.trace()`, `.set_priority(n)`, `.expire_after(seconds)` (`weida_outgoing_transfer_expire_after_ms` in seconds, §1), `.finish()` (consumes) | present; `write_all` is the Python loop R3c leaves to the caller, its timeout bounding the whole call; `test_config.py::test_metadata_travels_with_a_request_and_its_reply`, `test_config.py::test_an_expired_reply_fails_its_finish_with_expired` |
| `weida_delivery` | `Delivery.delivered(timeout)` (consumes) | present |
| `weida_incoming_meta` | `IncomingMeta`, a frozen dataclass copied out at once; `peer_chain` is a tuple of DER `bytes`, leaf first, or `None`; `trace` and `tracestate` a `str` or `None`; `achieved` an `Acknowledgement` or `None` | present; `test_mtls.py::test_any_key_exposes_the_fingerprint_and_the_chain`, `test_config.py::test_metadata_travels_with_a_request_and_its_reply` |
| `weida_msg` | `Message(payload: bytes, meta: IncomingMeta)`, copied out at once | present |
| the ops (`*_op`) | not exposed: a waiting method starts its op, waits, and drops it in `finally`, which cancels it only when it did not finish; the asyncio `canceled` holds its op from the call to the end of its awaitable, and the collector drops one never awaited | refused: an op is the C caller's way to wait, not a value a Python caller needs |
| `weida_poll` | not exposed | refused: `select`, `selectors` and the event loop take the `fileno()` descriptors directly |
| `weida_version`, `weida_strerror`, `weida_new_trace` | `weida.version()`, `weida.strerror(code)`, `weida.new_trace()` | present; `smoke.py`, `test_errors.py::test_strerror_of_an_unknown_code_is_unknown_error`, `test_config.py::test_new_trace_is_a_sampled_traceparent` |
| the enumeration constants `WEIDA_CONGESTION_*`, `WEIDA_DISCOVERY_*`, `WEIDA_OUTBOX_FULL_*`, `WEIDA_ERROR_CODE_*` and the six guarantee dimensions, `WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES` | the `IntEnum`s `Congestion`, `Discovery`, `OutboxFull`, `DeliveryLevel`, `Acknowledgement`, `Durability`, `OrderingMode`, `Deduplication`, `Backpressure`, `ProducerNaming`, `ErrorCode` (§1), and `weida.DEFAULT_DATAGRAM_RECEIVE_BYTES` | present; `test_config.py::test_a_full_configuration_builds_a_runtime`, `test_config.py::test_configuration_input_errors_raise_at_once`, `test_refusals.py::test_a_refusal_code_that_is_not_an_error_code_is_a_type_error_and_leaves_the_request_usable` |

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
| Ctrl-C ends a blocked call with `KeyboardInterrupt` on Unix, with no time slices: the C wait returns `EINTR`, the binding runs the pending handlers itself (`signal.pthread_sigmask`, which calls `PyErr_CheckSignals`; PyPy would otherwise hand a signal another thread noticed to the main thread only after a later GIL switch, and the next wait would block), and the call's in-flight work stays in the handle | present; `test_sync.py::test_ctrl_c_ends_a_blocked_accept`, which sends a process-directed SIGINT as a terminal does |
| Ctrl-C on Windows: each wait is cut into 100 ms slices, because Windows has no `EINTR` | present: every waiting call of the tests in `python-windows-amd64` runs in slices; Ctrl-C itself is not sent there (`test_sync.py::test_ctrl_c_ends_a_blocked_accept` is skipped on Windows) |
| The GIL is released for every C call (cffi does it), so a blocked call holds no other Python thread | present |
| asyncio: one `loop.add_reader` registration per object and a FIFO of waiting coroutines, each retried with a timeout-0 try when the descriptor is readable | present; `test_asyncio.py::test_two_accepts_on_one_replier_serve_two_requests` |
| asyncio cancellation cancels the operation (its op is dropped) | present; `test_asyncio.py::test_a_cancelled_request_ends_and_the_runtime_still_shuts_down` |
| The Windows Proactor loop, which cannot watch a descriptor: the wait runs in the default executor, and cancellation drops the object, which ends that wait with `Closed` | present; `test_asyncio.py` runs on Windows' default Proactor loop in `python-windows-amd64` (the nightly and a release tag) |

## 5. Packaging

| Row | Verdict |
| --- | --- |
| One `py3-none-<platform>` wheel per platform, with `libweida` and the generated `_ffi.py` inside | present; `package.sh` refuses any other tag |
| Platform tags PyPI accepts: `manylinux_2_<glibc>_x86_64` and `_aarch64`, `macosx_11_0_arm64`, `win_amd64` | present: the `python-linux-*` jobs retag the `c-abi-linux-*` wheel with `auditwheel repair` (the glibc floor of the trixie build image; `libweida` is the only native file), `hatch_build.py` tags a macOS wheel with the deployment target `libweida` is built for (`MACOSX_DEPLOYMENT_TARGET`, else rustc's 11.0), and each python job checks its wheel's file name and runs `twine check` before it hands the wheel on |
| The version is the workspace version in PEP 440 spelling (`0.1.0-alpha.11-dev` is `0.1.0a11.dev0`) | present; `smoke.py` |
| The wheel runs with no Rust toolchain on `PATH` | present; `package.sh` runs `smoke.py` with `PATH=/usr/bin:/bin`, and on the nightly and a release tag every python job runs it from a virtualenv holding only its wheel |
| CPython 3.9 and up | present by `requires-python`; 3.13 (the `c-abi-linux-*` jobs, editable install; `python-darwin-arm64` and `python-windows-amd64`, the wheel) and 3.14 (every python job, the wheel) are run |
| PyPy | present: PyPy 3.11 runs the tests against the wheel in every python job (linux-amd64, linux-arm64, darwin-arm64, windows-amd64) |
| Free-threaded CPython (3.13t, 3.14t) | 3.14t present: every python job runs the tests against the wheel and checks that importing `weida` leaves the GIL disabled; 3.13t not run |
| `cryptography` | optional: imported per call, never a dependency; installed for the CPython test runs, absent on 3.14t and PyPy | present; `test_identity.py::test_without_cryptography_bytes_convert_and_objects_are_type_errors` |
| macOS | present: `python-darwin-arm64` builds `libweida` and the `macosx_11_0_arm64` wheel on atik with uv's CPython (not the Xcode one) and runs the tests against it |
| Windows | present: `python-windows-amd64` builds `libweida`, statically linked to the C runtime, and the `win_amd64` wheel on wohnzimmer with uv's CPython and runs the tests against it |
| PyPI | absent: nothing of this repository is on PyPI |

## 6. What the PyO3 binding had and this one does not

The PyO3 `weida-py` this package replaces reached all six patterns, RADIO/DISH, the cursor
surface and connection statistics. Each of those is absent here exactly as it is absent from
the C ABI ([weida-c.md](weida-c.md) §5 to §7), and returns when the ABI maps it. The new
package is not compatible with the old module's API (0045 decision 6).

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

The C ABI's *resolver* area is absent here: `weida_runtime_config_set_resolver`, `weida_resolver`
and `weida_resolved`. A name still resolves through the system resolver.

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

The C ABI's *flows* and *raw peers* areas are absent here: `weida_runtime_peer`, `weida_peer`,
`weida_listener_acceptor`, `weida_acceptor`, `weida_incoming`, `weida_consumer`,
`weida_credit_grant`, `weida_flow_meta`, `weida_flow`, `weida_incoming_flow`, `weida_datagram`
and `weida_path_stats`.

The C ABI's *swarm* area is absent here: `weida_runtime_swarm`, `weida_swarm`,
`weida_fetch_options`, `weida_fetch`, `weida_piece`, `weida_listener_seeder`, `weida_seeder`,
`weida_seed_session`, `weida_blob`, `weida_piece_list`, `weida_tree_hasher`, `weida_bits` and
`weida_blob_id_of`.

## 7. Sources

`crates/py/weida-py/src/weida/` (`__init__.py`, `_native.py`, `_errors.py`, `_wait.py`,
`_enums.py`, `_values.py`, `_crypto.py`, `_sync.py`, `asyncio.py`, `_cdef.h`), `hatch_build.py`,
`pyproject.toml`, `develop.sh`, `package.sh`, `smoke.py`, `tests/`; [weida-c.md](weida-c.md);
[docs/c/](../c/README.md); [decisions/0045](../decisions/0045-the-c-abi-is-the-boundary.md).
