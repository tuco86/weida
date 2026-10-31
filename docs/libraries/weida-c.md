# weida's C ABI (`libweida`, against the Rust API of `crates/weida`) — feature parity

## 1. What a row means

The reference is not a foreign implementation: `libweida` binds weida itself, so the reference
column is the **Rust API of `crates/weida`**, every item `crates/weida/src/lib.rs` re-exports,
and every public method of the types this stage maps
([decisions/0045](../decisions/0045-the-c-abi-is-the-boundary.md)). Each row has one of three
verdicts:

- **present**: the C function that maps it, by R1's names, and the test that proves it;
- **refused**: deliberately not mapped, with the rule that refuses it;
- **absent**: not mapped yet, with the follow-up area of 0045 §5 it belongs to.

There is no "partial": a type with some methods mapped has one row per method. The rules R1 to
R13 are [docs/c/README.md](../c/README.md); the per-function contracts are the other pages of
[docs/c/](../c/README.md). Measured on linux-amd64 (the CI image `trixie`), with clippy and the
unit tier also on windows-amd64 and darwin-arm64.

The tests named below are `crates/weida-c/tests/io/abi.rs` (`abi::<name>`, the integration
tier), the C program `crates/weida-c/tests/c/smoke.c` (`smoke.c`, run by `smoke.sh`), and the
unit tests in `crates/weida-c/src/`.

## 2. Runtime, listener and bindings

| `crates/weida` | C ABI | Verdict |
| --- | --- | --- |
| `Runtime::owned` | `weida_runtime_owned` | present; every `abi::` test, `smoke.c` |
| `Runtime::new`, `Runtime::with_handle` | — | refused: a C caller has no Tokio runtime to hand over; the ABI owns its reactor (R13) |
| `Runtime::listener` | `weida_runtime_listener` | present; `abi::a_streamed_round_trip_over_inproc` |
| `Runtime::requester` | `weida_runtime_requester` | present; `abi::a_streamed_round_trip_over_inproc` |
| `Runtime::shutdown` | `weida_runtime_shutdown` (op) | present; `abi::a_shutdown_finishes_with_an_exchange_abandoned` |
| `Runtime::drain` | — | absent: *drain* |
| `Runtime::peer`, `pusher`, `subscriber`, `dish`, `pair`, `surveyor`, `swarm` | — | absent: *patterns*, *raw peers*, *swarm* |
| `Runtime::config`, `exec`, `suppressed_duplicates`, `expired_transfers` | — | absent: *config*, *stats*; `exec` refused, it is the binding's own seam |
| `Drop for Runtime` | `weida_runtime_drop` | present; R13, the reactor ends with its last user |
| `Listener::bind_quic` | `weida_listener_bind_quic` (op) | present; `abi::a_server_outside_the_pins_is_untrusted`, `smoke.c` |
| `Listener::bind_inproc` | `weida_listener_bind_inproc` | present; `abi::a_streamed_round_trip_over_inproc` |
| `Listener::replier` | `weida_listener_replier` | present; every `abi::` test |
| `Listener::bind_quic_socket`, `bind_unix`, `bind_pipe` | — | absent: *binding lifecycle*, *local transports* |
| `Listener::puller`, `publisher`, `radio`, `acceptor`, `pair`, `respondent`, `seeder`, `bus` | — | absent: *patterns*, *raw peers*, *swarm* |
| `Binding::local_addr` | `weida_binding_local_addr` | present; `abi::a_server_outside_the_pins_is_untrusted`, `smoke.c` |
| `Binding::close`, `hand_over`, `take_over`, `disconnect` | — | absent: *binding lifecycle* |
| `LocalBinding::bus` | `weida_local_binding_bus` | present; R7 getter |
| `UnixBinding`, `PipeBinding`, `Handoff` | — | absent: *local transports*, *binding lifecycle* |

## 3. Req/Rep, streamed

| `crates/weida` | C ABI | Verdict |
| --- | --- | --- |
| `Requester::connect` | `weida_requester_connect` (op) | present; `abi::a_streamed_round_trip_over_inproc`, `smoke.c` |
| `Requester::request` | `weida_requester_request` (op), the reply handed over at its header | present; `abi::a_streamed_round_trip_over_inproc` |
| `Requester::request_with` | — | absent: *streams* |
| `Requester::open`, `ReplyStream` | — | absent: *streams* |
| `Requester::connection_stats`, `events` | — | absent: *stats* |
| `Replier::accept` | `weida_replier_accept` (try, R3a) | present; `abi::an_accept_with_nothing_pending_answers_eagain`, `abi::a_repliers_descriptor_follows_its_queue`, `abi::a_signal_ends_a_blocked_accept_with_eintr`, `abi::dropping_a_replier_ends_a_blocked_accept_with_eclosed` |
| `Replier::path` | `weida_replier_path` | present; R7 getter |
| `IncomingRequest::meta` | `weida_incoming_request_meta` | present; `abi::a_streamed_round_trip_over_inproc` (`endpoint`) |
| `IncomingRequest::take_body` | `weida_incoming_request_take_body`; a second call is `-WEIDA_ECLOSED` where Rust panics | present; `abi::a_streamed_round_trip_over_inproc` |
| `IncomingRequest::body` | — | refused: R1, a lent `&mut` sub-handle has no C lifetime; `take_body` is its owning twin |
| `IncomingRequest::reply` | `weida_incoming_request_reply` (op, consumes) | present; `abi::a_streamed_round_trip_over_inproc` |
| `IncomingRequest::refuse`, `canceled` | — | absent: *refusals* |
| `IncomingRequest::connection` | — | absent: *stats* |
| `Drop for IncomingRequest` (`NO_REPLY`) | `weida_incoming_request_drop` | present; R1 |

## 4. Transfers and receipts

| `crates/weida` | C ABI | Verdict |
| --- | --- | --- |
| `AsyncRead for IncomingTransfer` | `weida_incoming_transfer_read` (try) | present; `abi::a_streamed_round_trip_over_inproc` (pieces of 3), `abi::a_reply_dropped_unfinished_reads_as_canceled` |
| `IncomingTransfer::collect` | `weida_incoming_transfer_collect` (op, consumes) | present; `abi::a_collect_over_its_ceiling_fails_with_limit_exceeded` |
| `IncomingTransfer::read_capped` | — | refused: R3c, a loop over `read` the caller writes |
| `IncomingTransfer::meta` | `weida_incoming_transfer_meta` | present; `abi::a_streamed_round_trip_over_inproc` (`content_type`) |
| `IncomingTransfer::reporter`, `connection` | — | absent: *cursors*, *stats* |
| `Drop for IncomingTransfer` (`REJECTED`) | `weida_incoming_transfer_drop` | present; `streams::a_refused_payload_reports_rejected_through_async_write` proves the outcome in Rust |
| `AsyncWrite for OutgoingTransfer` | `weida_outgoing_transfer_write` (try) | present; `abi::a_streamed_round_trip_over_inproc`, `smoke.c` |
| `OutgoingTransfer::write_all` | — | refused: R3c |
| `OutgoingTransfer::finish` | `weida_outgoing_transfer_finish` (consumes) | present; `abi::a_streamed_round_trip_over_inproc` |
| `OutgoingTransfer::cancel` | `weida_outgoing_transfer_drop` (an unfinished transfer is reset `CANCELED`) | present; `abi::a_reply_dropped_unfinished_reads_as_canceled` |
| `OutgoingTransfer::trace`, `cursors`, `set_priority`, `expire_at` | — | absent: *cursors*, *config* |
| `Delivery::delivered` | `weida_delivery_delivered` (op, consumes) | present; `abi::a_streamed_round_trip_over_inproc` |
| `Drop for Delivery` | `weida_delivery_drop` | present |
| `ConnectionId` | — | absent: *stats* |

## 5. Values and metadata

| `crates/weida` | C ABI | Verdict |
| --- | --- | --- |
| `RuntimeConfig::worker_threads`, `shutdown_timeout` | `weida_runtime_config_set_worker_threads`, `_set_shutdown_timeout_ms` | present; `test_errors.py` (Python) for `worker_threads = 0` |
| every other `RuntimeConfig` field, `Limits`, `Discovery`, `Network`, `ReconnectPolicy`, `OutboxFull`, `GuaranteeSet` and its dimensions | — | absent: *config* |
| `Trust::by_address` | `weida_trust_new` | present; `smoke.c` |
| `Trust::and_pin` | `weida_trust_add_pin` | present; `abi::a_malformed_pin_is_an_invalid_fingerprint`, `abi::a_server_outside_the_pins_is_untrusted` |
| `Trust::pin`, `anchor`, `and_anchor`, `anchor_file`, `and_anchor_file`, `is_empty` | — | absent: *identity* (`pin` is `new` plus `add_pin`) |
| `Identity::generate`, `from_pem`, `fingerprint` | `weida_identity_generate`, `_from_pem`, `_fingerprint` | present; `smoke.c`, `abi::a_server_outside_the_pins_is_untrusted` |
| `Identity::generate_for`, `from_pem_files`, `from_pem_file`, `certificate_pem`, `to_pem`, `Pem` | — | absent: *identity* |
| `ClientTls::new` | `weida_client_tls_new` | present |
| `ClientTls::with_identity`, `with_alpn`, `ClientTrust`, `Alpn` | — | absent: *identity* |
| `ServerTls::new` | `weida_server_tls_new` | present |
| `ServerTls::require_client`, `with_alpn` | — | absent: *identity* |
| `IdentitySource`, `TrustSource`, their weak forms, `FilesOptions`, `IdentityEvent(s)` | — | absent: *identity* (events as queues, 0045 decision 10) |
| `TransferMeta::with_content_type` | `weida_transfer_meta_set_content_type` | present; `abi::a_streamed_round_trip_over_inproc` |
| `TransferMeta::with_content_len`, `with_trace`, `with_topic`, `with_achieved`, `with_report`, `with_report_mode` | — | absent: *config* |
| `IncomingMeta::endpoint`, `content_type`, `topic`, `content_len`, `sequence` | `weida_incoming_meta_*` | present; `endpoint` and `content_type` by `abi::a_streamed_round_trip_over_inproc` |
| `IncomingMeta::peer` (a key) | `weida_incoming_meta_peer_fingerprint` | present for `PeerIdentity::Key`; a local principal is absent: *local transports* |
| `IncomingMeta::trace`, `tracestate`, `peer_chain`, `gap`, `achieved`, `report`, `report_mode`, `report_id`, `segment`, `layer` | — | absent: *config*, *cursors*, *patterns* |
| `Fingerprint` | `sha256:` text everywhere | present |
| `Error`, `LossCause` | the R9 constants and the thread-local record | present; `error::tests::every_variant_has_its_own_code_below_the_abi_codes`, `abi::a_server_outside_the_pins_is_untrusted` (`weida_last_error_fingerprint`) |
| `ErrorCode`, `StopReason`, `codes` | — | refused: wire vocabulary, reached through the R9 outcome codes |
| `Address`, `EndpointAddr`, `InprocAddr`, `UnixAddr`, `PipeAddr`, `DEFAULT_PORT` | URLs as strings | refused: the C caller passes the text, weida parses it |
| `new_trace`, `TraceContext` | — | absent: *config* |
| `Resolver`, `SharedResolver`, `SystemResolver`, `Resolved` | — | absent: *resolver* (a queue, decision 10) |
| `SimNet`, `SimStats` | — | refused: a test double of the Rust test suite (`test-util`) |
| `Result`, `Pattern`, `Endpoint` and the pattern markers | — | refused: Rust type machinery; the ABI has one handle type per role (decision 3) |

## 6. Observability

| `crates/weida` | Verdict |
| --- | --- |
| `ConnectionStats`, `PathStats`, `RemoteStats`, `TransportStats`, `UdpCounts`, `connection_stats()` | absent: *stats* |
| `PeerEvent`, `PeerEvents`, `GiveUp` | absent: *identity* and *patterns* (events as queues) |
| `DishDrops`, `TopicDrops`, `Drained` | absent: *patterns*, *drain* |

## 7. Patterns and helpers not yet mapped

Push/Pull (`Pusher`, `Puller`), Pub/Sub (`Publisher`, `Subscriber`, `FanOut`), PAIR (`Paired`),
SURVEY (`Surveyor`, `SurveyRun`, `Respondent`), BUS (`BusMember`), RADIO/DISH (`Radio`, `Dish`,
`Join`, `JoinTerms`, `Received`, `Segment`, `SegmentTerms`, `MAX_SEGMENT_LAYERS`), datagram flows
(`Flow`, `FlowInfo`, `FlowMeta`, `FlowStats`, `IncomingFlow`), the raw `Peer`, `Acceptor`,
`Incoming`, `Consumer`, `ConsumerId`, `CreditGrant`, the cursor surface (`Cursors`, `CursorSet`,
`Reporter`, `CursorLevel`, `ReportMode`), and SWARM (`Seeder`, `Swarm`, `Fetch`, `Blob`, `BlobId`
and the rest behind the `swarm` feature): each absent, in the areas *patterns*, *flows*,
*raw peers*, *cursors* and *swarm*.

## 8. Interop evidence

The ABI has no second implementation to run against; what proves it is that it is called the
way a foreign caller calls it:

- `smoke.c`, compiled from `weida.h` under `-std=c11 -Wall -Wextra -Werror`, linked with
  `-lweida`: a QUIC Req/Rep with a generated identity, a pinned trust, streamed bodies, a
  content type and a delivery receipt, every wait a timeout-0 call plus `weida_poll`;
- the ten `abi::` tests, which call the exports as C does (raw pointers, out-parameters,
  descriptors, threads, a signal);
- the Python package `crates/py/weida-py`, a cffi caller in ABI mode
  ([weida-py.md](weida-py.md)).

## 9. Deliberate deviations and bounds

- `take_body` twice is `-WEIDA_ECLOSED`; Rust panics.
- A collect's `max_bytes` is `uint64_t`; above `usize::MAX` it is `usize::MAX`.
- A readiness descriptor costs a socket pair per handle, created on first use (R11).
- On Windows a blocking wait cannot be interrupted (R4); callers bound their waits.

## 10. The definition of done

The plan of this stage asked for the rules, the decision, and one vertical slice: runtime,
configuration, TLS values, Req/Rep with streamed bodies, delivery receipts, readiness
descriptors and `weida_poll`, through Rust, the header, a C program, Python and the CI. Every
row of §2 to §5 marked present is that slice; every absent row names its area, and the slice
is complete when no present row lacks a test.

## 11. Sources

`crates/weida-c/src/` (`error.rs`, `ready.rs`, `try_handle.rs`, `op.rs`, `handle.rs`,
`runtime.rs`, `tls.rs`, `listener.rs`, `transfer.rs`, `requester.rs`, `poll.rs`),
`crates/weida-c/include/weida.h`, `crates/weida-c/tests/`, `crates/weida/src/lib.rs` for the
reference column, [decisions/0045](../decisions/0045-the-c-abi-is-the-boundary.md), and
[docs/c/](../c/README.md).
