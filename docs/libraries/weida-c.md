# weida's C ABI (`libweida`, against the Rust API of `crates/weida`) — feature parity

## 1. What a row means

The reference is not a foreign implementation: `libweida` binds weida itself, so the reference
column is the **Rust API of `crates/weida`**, every item `crates/weida/src/lib.rs` re-exports,
and every public method of the types this stage maps
([decisions/0045](../decisions/0045-the-c-abi-is-the-boundary.md)). Each row has one of three
verdicts:

- **present**: the C function that maps it, by R1's names, and the test that proves it; a row
  an area's plan made present keeps that area's name after its evidence, as in
  `present; abi::<test> (*drain*)`;
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
| `Runtime::drain` | `weida_runtime_drain` (op, consumes; its wait yields a `weida_drained`) | present; `abi::a_drain_counts_a_reply_finished_before_it_as_delivered`, `abi::a_drain_with_nothing_pending_returns_at_once` (*drain*) |
| `Runtime::pusher`, `subscriber` | `weida_runtime_pusher`, `weida_runtime_subscriber` | present; `abi::push_to_pull_over_inproc_arrives_in_order`, `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `Runtime::pair`, `surveyor` | `weida_runtime_pair`, `weida_runtime_surveyor` | present; `abi::a_pair_exchanges_in_both_directions`, `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `Runtime::dish` | `weida_runtime_dish` | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Runtime::peer`, `swarm` | — | absent: *raw peers*, *swarm* |
| `Runtime::config` | — | refused: the configuration is the builder the caller passed to `weida_runtime_owned`, which copied it |
| `Runtime::suppressed_duplicates`, `expired_transfers` | `weida_runtime_suppressed_duplicates`, `weida_runtime_expired_transfers` | present; `abi::an_expired_reply_fails_its_finish_and_cancels_the_reader` (counts 0 then 1), `abi::an_inproc_connection_is_reported_with_its_url_and_no_transport` (0 without deduplication) (*stats*) |
| `Runtime::exec` | — | refused: it is the binding's own seam |
| `Drop for Runtime` | `weida_runtime_drop` | present; R13, the reactor ends with its last user |
| `Listener::bind_quic` | `weida_listener_bind_quic` (op) | present; `abi::a_server_outside_the_pins_is_untrusted`, `smoke.c` |
| `Listener::bind_inproc` | `weida_listener_bind_inproc` | present; `abi::a_streamed_round_trip_over_inproc` |
| `Listener::replier` | `weida_listener_replier` | present; every `abi::` test |
| `Listener::bind_quic_socket` | `weida_listener_bind_quic_socket` (op; takes ownership of the descriptor) | present; `abi::a_hand_over_keeps_a_requesters_connection` (*binding lifecycle*) |
| `Listener::bind_unix`, `bind_pipe` | `weida_listener_bind_unix`, `weida_listener_bind_pipe` (plain functions, R3; `-WEIDA_EUNSUPPORTED` on the other platform) | present; `abi::a_round_trip_over_unix_names_the_clients_uid`, `abi::a_round_trip_over_a_pipe_names_the_clients_sid` (*local transports*) |
| `Listener::bind_relay` | `weida_listener_bind_relay` (op; its wait yields a relayed `weida_binding`). Dialling a `weida+relay://` URL needs nothing more: the ABI passes URL text | present; `abi::a_dial_only_replier_answers_through_a_relay` (*binding lifecycle*) |
| `Listener::relay`, `Relay::accept` | `weida_listener_relay`, `weida_relay_accept` (try, R3a) | present; `abi::a_dial_only_replier_answers_through_a_relay` (*binding lifecycle*) |
| `RelayRequest::peer`, `target` | `weida_relay_request_peer_fingerprint`, `_target` (`sha256:` text) | present; `abi::a_dial_only_replier_answers_through_a_relay` (*binding lifecycle*) |
| `RelayRequest::admit`, `refuse` | `weida_relay_request_admit`, `_refuse` (ops, consume) | present; `abi::a_dial_only_replier_answers_through_a_relay` (*binding lifecycle*) |
| `Drop for Relay`, `Drop for RelayRequest` (`NO_REPLY`) | `weida_relay_drop`, `weida_relay_request_drop` | present; `abi::a_dial_only_replier_answers_through_a_relay` (*binding lifecycle*) |
| `Binding::relay_events` | `weida_binding_relay_events` (a `weida_peer_events` queue; 0 for a binding not relayed) | present; `abi::a_dial_only_replier_answers_through_a_relay` (*binding lifecycle*) |
| `Listener::puller`, `publisher` | `weida_listener_puller`, `weida_listener_publisher` | present; `abi::push_to_pull_over_inproc_arrives_in_order`, `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `Listener::pair`, `respondent`, `bus` | `weida_listener_pair`, `weida_listener_respondent`, `weida_listener_bus` | present; `abi::a_pair_exchanges_in_both_directions`, `abi::a_survey_collects_two_respondents_answers_within_its_window`, `abi::a_bus_message_reaches_every_other_member` (*patterns*) |
| `Listener::radio` | `weida_listener_radio` | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Listener::acceptor`, `seeder` | — | absent: *raw peers*, *swarm* |
| `Binding::local_addr` | `weida_binding_local_addr` | present; `abi::a_server_outside_the_pins_is_untrusted`, `smoke.c` |
| `Binding::close` | `weida_binding_close` (op) | present; `abi::a_closed_binding_refuses_the_next_dial` (*binding lifecycle*) |
| `Binding::hand_over`, `Handoff::held`, `local_addr`, `into_parts` | `weida_binding_hand_over` (op, consumes; its wait yields a `weida_handoff`), `weida_handoff_held`, `_local_addr`, `_take_socket`, `_bytes` | present; `abi::a_hand_over_keeps_a_requesters_connection` (*binding lifecycle*) |
| `Binding::take_over` | `weida_binding_take_over` (op) | present; `abi::a_hand_over_keeps_a_requesters_connection` (*binding lifecycle*) |
| `Binding::disconnect` | `weida_binding_disconnect` | present; `abi::a_binding_disconnects_a_peer_by_its_key` (*binding lifecycle*) |
| `LocalBinding::bus` | `weida_local_binding_bus` | present; R7 getter |
| `UnixBinding::path`, `PipeBinding::name` | `weida_unix_binding_path`, `weida_pipe_binding_name` (R7 getters) | present; `abi::a_round_trip_over_unix_names_the_clients_uid`, `abi::a_round_trip_over_a_pipe_names_the_clients_sid` (*local transports*) |
| `Drop for UnixBinding` (removes the socket file), `Drop for PipeBinding` (releases the name) | `weida_unix_binding_drop`, `weida_pipe_binding_drop` | present; `abi::a_round_trip_over_unix_names_the_clients_uid`, `abi::a_round_trip_over_a_pipe_names_the_clients_sid` (*local transports*) |

## 3. Req/Rep, streamed

| `crates/weida` | C ABI | Verdict |
| --- | --- | --- |
| `Requester::connect` | `weida_requester_connect` (op) | present; `abi::a_streamed_round_trip_over_inproc`, `smoke.c` |
| `Requester::request` | `weida_requester_request` (op), the reply handed over at its header | present; `abi::a_streamed_round_trip_over_inproc` |
| `Requester::request_with` | `weida_requester_request_with` (op) | present; `abi::metadata_travels_with_a_request_and_its_reply` (*config*) |
| `Requester::open` | `weida_requester_open` (op; its wait yields the request body as a `weida_outgoing_transfer` and the `weida_reply_stream`) | present; `abi::an_opened_exchange_reads_its_reply_while_the_request_is_still_open` (*streams*) |
| `ReplyStream::recv` | `weida_reply_stream_recv` (op, consumes) | present; `abi::an_opened_exchange_reads_its_reply_while_the_request_is_still_open` (*streams*) |
| `Drop for ReplyStream` (`CANCELED`) | `weida_reply_stream_drop` | present; `abi::dropping_a_reply_stream_cancels_the_exchange` (*streams*) |
| `Requester::connection_stats` | `weida_requester_connection_stats` (a snapshot handle, [stats.md](../c/stats.md)) | present; `abi::an_inproc_connection_is_reported_with_its_url_and_no_transport`, `abi::a_quic_connection_reports_its_transport` (*stats*) |
| `Requester::events` | `weida_requester_events` (a `weida_peer_events` queue) | present; `abi::a_replier_runtime_shut_down_reports_lost_then_retrying` (*stats*) |
| `Replier::accept` | `weida_replier_accept` (try, R3a) | present; `abi::an_accept_with_nothing_pending_answers_eagain`, `abi::a_repliers_descriptor_follows_its_queue`, `abi::a_signal_ends_a_blocked_accept_with_eintr`, `abi::dropping_a_replier_ends_a_blocked_accept_with_eclosed` |
| `Replier::path` | `weida_replier_path` | present; R7 getter |
| `IncomingRequest::meta` | `weida_incoming_request_meta` | present; `abi::a_streamed_round_trip_over_inproc` (`endpoint`) |
| `IncomingRequest::take_body` | `weida_incoming_request_take_body`; a second call is `-WEIDA_ECLOSED` where Rust panics | present; `abi::a_streamed_round_trip_over_inproc` |
| `IncomingRequest::body` | — | refused: R1, a lent `&mut` sub-handle has no C lifetime; `take_body` is its owning twin |
| `IncomingRequest::reply` | `weida_incoming_request_reply` (op, consumes) | present; `abi::a_streamed_round_trip_over_inproc` |
| `IncomingRequest::refuse`, `ErrorCode` | `weida_incoming_request_refuse` (op, consumes), `WEIDA_ERROR_CODE_*` | present; `abi::a_refused_request_fails_the_requester_with_its_codes_error` (*refusals*) |
| `IncomingRequest::canceled` | `weida_incoming_request_canceled` (op, outlives the request) | present; `abi::canceled_settles_once_the_requester_drops_its_request` (*refusals*) |
| `IncomingRequest::connection` | `weida_incoming_request_connection` | present; `abi::a_request_and_its_body_name_the_same_connection` (*stats*) |
| `Drop for IncomingRequest` (`NO_REPLY`) | `weida_incoming_request_drop` | present; R1 |

## 4. Transfers and receipts

| `crates/weida` | C ABI | Verdict |
| --- | --- | --- |
| `AsyncRead for IncomingTransfer` | `weida_incoming_transfer_read` (try) | present; `abi::a_streamed_round_trip_over_inproc` (pieces of 3), `abi::a_reply_dropped_unfinished_reads_as_canceled` |
| `IncomingTransfer::collect` | `weida_incoming_transfer_collect` (op, consumes) | present; `abi::a_collect_over_its_ceiling_fails_with_limit_exceeded` |
| `IncomingTransfer::read_capped` | — | refused: R3c, a loop over `read` the caller writes |
| `IncomingTransfer::meta` | `weida_incoming_transfer_meta` | present; `abi::a_streamed_round_trip_over_inproc` (`content_type`) |
| `IncomingTransfer::reporter` | — | absent: *cursors* |
| `IncomingTransfer::connection` | `weida_incoming_transfer_connection` | present; `abi::a_request_and_its_body_name_the_same_connection` (*stats*) |
| `Drop for IncomingTransfer` (`REJECTED`) | `weida_incoming_transfer_drop` | present; `streams::a_refused_payload_reports_rejected_through_async_write` proves the outcome in Rust |
| `AsyncWrite for OutgoingTransfer` | `weida_outgoing_transfer_write` (try) | present; `abi::a_streamed_round_trip_over_inproc`, `smoke.c` |
| `OutgoingTransfer::write_all` | — | refused: R3c |
| `OutgoingTransfer::finish` | `weida_outgoing_transfer_finish` (consumes) | present; `abi::a_streamed_round_trip_over_inproc` |
| `OutgoingTransfer::cancel` | `weida_outgoing_transfer_drop` (an unfinished transfer is reset `CANCELED`) | present; `abi::a_reply_dropped_unfinished_reads_as_canceled` |
| `OutgoingTransfer::trace`, `set_priority`, `expire_at` | `weida_outgoing_transfer_trace`, `_set_priority`, `_expire_after_ms` | present; `abi::metadata_travels_with_a_request_and_its_reply`, `abi::an_expired_reply_fails_its_finish_and_cancels_the_reader` (*config*) |
| `OutgoingTransfer::cursors` | — | absent: *cursors* |
| `Delivery::delivered` | `weida_delivery_delivered` (op, consumes) | present; `abi::a_streamed_round_trip_over_inproc` |
| `Drop for Delivery` | `weida_delivery_drop` | present |
| `ConnectionId` | its number, `uint64_t` (`ConnectionId::get`) | present; `abi::a_request_and_its_body_name_the_same_connection` (*stats*) |

## 5. Values and metadata

| `crates/weida` | C ABI | Verdict |
| --- | --- | --- |
| `RuntimeConfig::worker_threads`, `shutdown_timeout` | `weida_runtime_config_set_worker_threads`, `_set_shutdown_timeout_ms` | present; `test_errors.py` (Python) for `worker_threads = 0` |
| every other `RuntimeConfig` field (`limits`, `max_connections`, `max_connections_per_peer`, `endpoint_queue`, `max_resolved_addresses`, `guarantees`, `discovery`, `connect_attempt_timeout`, `reconnect`, `send_timeout`, `outbox_messages`, `outbox_bytes`, `outbox_full`, `max_handshakes`, `retry_above_handshakes`) | `weida_runtime_config_set_*` | present; `runtime::tests::every_setter_lands_on_its_field`, `abi::a_send_timeout_ends_a_request_to_a_peer_that_is_gone` for `send_timeout` (*config*) |
| `RuntimeConfig::resolver` | `weida_runtime_config_set_resolver` (shares the queue; NULL is the system resolver) | present; `abi::a_name_only_the_resolver_knows_is_dialled_through_it` (*resolver*) |
| `RuntimeConfig::network`, `Network` | — | refused: a `SimNet` exists only under `test-util` |
| `Limits`, `Congestion`, `DEFAULT_DATAGRAM_RECEIVE_BYTES` | `weida_limits` and its setters, `WEIDA_CONGESTION_*`, `WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES` | present; `runtime::tests::every_setter_lands_on_its_field`, `abi::limits_reach_the_quic_transport` (*config*) |
| `Limits::header_slots`, `worst_case_header_memory` | — | refused: computations over the caller's own values |
| `Discovery`, `OutboxFull` | `WEIDA_DISCOVERY_*`, `WEIDA_OUTBOX_FULL_*` | present; `runtime::tests::every_setter_lands_on_its_field` (*config*) |
| `ReconnectPolicy` (fields and `never`) | `weida_reconnect_policy` and its setters, `weida_reconnect_policy_never` | present; `runtime::tests::every_setter_lands_on_its_field` (*config*) |
| `ReconnectPolicy::redials`, `delay` | — | refused: computations over the caller's own values |
| `GuaranteeSet` and its dimensions (`DeliveryLevel`, `Acknowledgement`, `Durability`, `OrderingMode`, `Deduplication`, `Backpressure`, `ProducerNaming`) | `weida_guarantee_set` and its setters, the `WEIDA_<DIMENSION>_*` constants | present; `runtime::tests::every_setter_lands_on_its_field`, `abi::a_guarantee_the_dialler_does_not_offer_fails_negotiation` (*config*) |
| `GuaranteeSet::is_core`, `intersect`, `reaches` | — | refused: the runtime negotiates, and the ABI hands a caller no set to compare |
| `Trust::by_address` | `weida_trust_new` | present; `smoke.c` |
| `Trust::and_pin` | `weida_trust_add_pin` | present; `abi::a_malformed_pin_is_an_invalid_fingerprint`, `abi::a_server_outside_the_pins_is_untrusted` |
| `Trust::and_anchor`, `and_anchor_file` | `weida_trust_add_anchor`, `_add_anchor_file` | present; `abi::a_trust_anchor_checks_the_name_the_dial_used` (*identity*) |
| `Trust::pin`, `anchor`, `anchor_file` | `weida_trust_new` plus the matching `_add_*` | present (*identity*) |
| `Trust::is_empty` | `weida_trust_is_empty` | present; `tls::tests::alpn_setters_take_the_two_tokens_and_refuse_others` (*identity*) |
| `Identity::generate`, `from_pem`, `fingerprint` | `weida_identity_generate`, `_from_pem`, `_fingerprint` | present; `smoke.c`, `abi::a_server_outside_the_pins_is_untrusted` |
| `Identity::generate_for` | `weida_identity_generate_for` | present; `tls::tests::generate_for_refuses_a_null_name`, `abi::a_trust_anchor_checks_the_name_the_dial_used` (*identity*) |
| `Identity::from_pem_files`, `from_pem_file` | `weida_identity_from_pem_files`, `_from_pem_file` | present; `abi::an_identity_written_to_a_file_reads_back_with_its_fingerprint` (*identity*) |
| `Identity::certificate_pem`, `to_pem`, `Pem` | `weida_identity_certificate_pem`, `_to_pem`, the owned `weida_pem` wiped on drop | present; `tls::tests::a_pem_written_by_to_pem_reads_back_with_the_same_fingerprint` (*identity*) |
| `ClientTls::new` | `weida_client_tls_new` | present |
| `ClientTls::with_identity` | `weida_client_tls_set_identity` | present; `abi::a_binding_that_requires_a_pinned_client_refuses_anonymous_and_strangers`, `abi::a_binding_that_requires_any_key_exposes_the_fingerprint_and_chain` (*identity*) |
| `ClientTls::with_alpn`, `Alpn` | `weida_client_tls_set_alpn`, `WEIDA_ALPN_WEIDA`, `WEIDA_ALPN_RAFT` | present; `abi::a_mismatched_alpn_fails_the_dial_with_etls`, `tls::tests::alpn_setters_take_the_two_tokens_and_refuse_others` (*identity*) |
| `ServerTls::new` | `weida_server_tls_new` | present |
| `ServerTls::require_client`, `ClientTrust` | `weida_server_tls_require_client` (`Trusted`), `_require_any_key` (`AnyKey`) | present; `abi::a_binding_that_requires_a_pinned_client_refuses_anonymous_and_strangers`, `abi::a_binding_that_requires_any_key_exposes_the_fingerprint_and_chain` (*identity*) |
| `ServerTls::with_alpn` | `weida_server_tls_set_alpn` | present; `abi::a_mismatched_alpn_fails_the_dial_with_etls`, `tls::tests::alpn_setters_take_the_two_tokens_and_refuse_others` (*identity*) |
| `Alpn::token` | — | refused: wire vocabulary; each constant names its token |
| `ClientTls::new`, `with_identity`, `ServerTls::new`, `require_client` given a source | `weida_client_tls_new_source`, `_set_identity_source`, `weida_server_tls_new_source`, `_require_client_source` (shared, not copied) | present; `abi::a_files_identity_source_rotates_a_bindings_certificate`, `abi::a_mismatched_alpn_fails_the_dial_with_etls`, `identity::tests::the_source_forms_share_their_source` (*identity*) |
| `IdentitySource::from_identity`, `ephemeral`, `external`, `files` | `weida_identity_source_from_identity`, `_ephemeral`, `_external`, `_files` | present; `abi::a_files_identity_source_rotates_a_bindings_certificate`, `abi::a_mismatched_alpn_fails_the_dial_with_etls`, `identity::tests::an_external_source_reports_its_updates_through_the_queue` (*identity*) |
| `IdentitySource::current`, `fingerprint` | `weida_identity_source_current` (a new `weida_identity`), `_fingerprint` (into the caller's buffer) | present; `abi::a_files_identity_source_rotates_a_bindings_certificate`, `identity::tests::an_external_source_reports_its_updates_through_the_queue` (*identity*) |
| `IdentitySource::update`, `report_failure`, `report_handoff_stolen`, `reload` | `weida_identity_source_update`, `_report_failure`, `_report_handoff_stolen`, `_reload` (1, 0 or the error) | present; `identity::tests::an_external_source_reports_its_updates_through_the_queue`, `abi::a_files_identity_source_rotates_a_bindings_certificate` (*identity*) |
| `IdentitySource::downgrade`, `WeakIdentitySource::upgrade`, `Drop` | `weida_identity_source_downgrade`, `weida_weak_identity_source_upgrade` (1, or 0 with NULL), `weida_identity_source_drop`, `weida_weak_identity_source_drop` | present; `identity::tests::an_external_source_reports_its_updates_through_the_queue` (*identity*) |
| `TrustSource::from_trust`, `external`, `current`, `update`, `report_failure`, `is_empty` | `weida_trust_source_from_trust`, `_external`, `_current`, `_update`, `_report_failure`, `_is_empty` | present; `identity::tests::a_trust_source_reports_a_refresh_and_reads_back_empty`, `abi::a_mismatched_alpn_fails_the_dial_with_etls` (*identity*) |
| `TrustSource::downgrade`, `WeakTrustSource::upgrade` | `weida_trust_source_downgrade`, `weida_weak_trust_source_upgrade` | present; `identity::tests::a_trust_source_reports_a_refresh_and_reads_back_empty` (*identity*) |
| `FilesOptions` (`names`, `poll`) | `weida_files_options_new`, `_set_names`, `_set_poll_ms`; NULL is the default | present; `identity::tests::files_options_setters_land_on_their_fields`, `abi::a_files_identity_source_rotates_a_bindings_certificate` (*identity*) |
| `TransferMeta::with_content_type` | `weida_transfer_meta_set_content_type` | present; `abi::a_streamed_round_trip_over_inproc` |
| `TransferMeta::with_content_len`, `with_trace`, `with_topic`, `with_achieved` | `weida_transfer_meta_set_content_len`, `_set_trace`, `_set_topic`, `_set_achieved` | present; `abi::metadata_travels_with_a_request_and_its_reply` (*config*) |
| `TransferMeta::with_report`, `with_report_mode` | — | absent: *cursors* |
| `IncomingMeta::endpoint`, `content_type`, `topic`, `content_len`, `sequence` | `weida_incoming_meta_*` | present; `endpoint` and `content_type` by `abi::a_streamed_round_trip_over_inproc` |
| `IncomingMeta::peer` | `weida_incoming_meta_peer_fingerprint` (`PeerIdentity::Key`), `_peer_uid`, `_peer_gid`, `_peer_pid` (`Local`), `_peer_sid`, `_peer_pid` (`Windows`) | present; `abi::a_binding_that_requires_any_key_exposes_the_fingerprint_and_chain`, `abi::a_round_trip_over_unix_names_the_clients_uid`, `abi::a_round_trip_over_a_pipe_names_the_clients_sid`, `transfer::tests::every_peer_identity_reads_back_through_its_getters` (*local transports*) |
| `IncomingMeta::peer_chain`, `PeerChain` | `weida_incoming_meta_peer_chain_len`, `_peer_chain_cert` (DER, leaf first) | present; `abi::a_binding_that_requires_any_key_exposes_the_fingerprint_and_chain` |
| `IncomingMeta::trace`, `tracestate`, `achieved` | `weida_incoming_meta_trace`, `_tracestate`, `_achieved` | present; `abi::metadata_travels_with_a_request_and_its_reply` (*config*) |
| `IncomingMeta::gap`, `Gap` | `weida_incoming_meta_gap` (both fields) | present; `transfer::tests::a_gap_reads_back_through_its_getter`, `abi::a_skipped_sequence_reads_as_a_gap_under_detect` (*patterns*) |
| `Gap::missed` | — | refused: a computation over the two values the getter returns |
| `IncomingMeta::segment`, `layer` | `weida_incoming_meta_segment`, `_layer` | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `IncomingMeta::report`, `report_mode`, `report_id` | — | absent: *cursors* |
| `Fingerprint` | `sha256:` text everywhere | present |
| `Error`, `LossCause` | the R9 constants and the thread-local record | present; `error::tests::every_variant_has_its_own_code_below_the_abi_codes`, `abi::a_server_outside_the_pins_is_untrusted` (`weida_last_error_fingerprint`) |
| `StopReason`, `codes` | — | refused: wire vocabulary, reached through the R9 outcome codes |
| `Address`, `EndpointAddr`, `InprocAddr`, `UnixAddr`, `PipeAddr`, `RelayAddr`, `DEFAULT_PORT`, `RELAY_PATH`, `SCHEME_RELAY` | URLs as strings | refused: the C caller passes the text, weida parses it |
| `new_trace`, `TraceContext` | `weida_new_trace`; a trace context is its `traceparent` text (R7) | present; `tls::tests::new_trace_writes_a_traceparent_that_sets_back` (*config*) |
| `Resolver`, `SharedResolver`, `SystemResolver`, `Resolved` | `weida_resolver` (`_new`, `_fd`, `_recv` a try (R3a), `_drop`: the lookups as a queue, decision 10), `weida_resolved` (`_name`, `_port`, `_max_addresses`, `_answer` and `_refuse` (consume), `_drop`); `SharedResolver` is the handle, shared by every configuration it is set on; `SystemResolver` is NULL in the setter; the `Exec` argument is not mapped (the binding's own seam) | present; `resolver::tests::a_lookup_reads_back_and_its_answer_reaches_the_dial`, `resolver::tests::an_answer_outside_the_contract_fails_the_dial`, `resolver::tests::a_refused_or_dropped_lookup_fails_the_dial_with_invalid_address`, `resolver::tests::the_queue_ends_once_no_configuration_holds_the_resolver`, `abi::a_name_only_the_resolver_knows_is_dialled_through_it`, `abi::a_refused_lookup_fails_the_connect_with_einvalid_address` (*resolver*) |
| `SimNet`, `SimStats` | — | refused: a test double of the Rust test suite (`test-util`) |
| `Result`, `Pattern`, `Endpoint` and the pattern markers | — | refused: Rust type machinery; the ABI has one handle type per role (decision 3) |

## 6. Observability

| `crates/weida` | Verdict |
| --- | --- |
| `ConnectionStats`, `PathStats`, `RemoteStats`, `TransportStats`, `UdpCounts`, `connection_stats()` | present: `weida_connection_stats` and its getters ([stats.md](../c/stats.md)); `abi::an_inproc_connection_is_reported_with_its_url_and_no_transport`, `abi::a_quic_connection_reports_its_transport` (*stats*) |
| `PeerEvent`, `PeerEvents`, `GiveUp` | present: `weida_peer_events` (a try handle) and `weida_peer_event`, `WEIDA_PEER_EVENT_*`, `WEIDA_GIVE_UP_*`; `abi::a_replier_runtime_shut_down_reports_lost_then_retrying`, `stats::tests::every_peer_event_kind_reads_back_through_its_getters` (*stats*) |
| `IdentityEvent`, `IdentityEvents`, `IdentitySource::events`, `TrustSource::events` | present: `weida_identity_events` (a try handle) and `weida_identity_event`, `WEIDA_IDENTITY_EVENT_*` ([identity.md](../c/identity.md)); `abi::a_files_identity_source_rotates_a_bindings_certificate`, `identity::tests::every_identity_event_kind_reads_back_through_its_getters`, `identity::tests::an_external_source_reports_its_updates_through_the_queue` (*identity*) |
| `TopicDrops` | present: `weida_topic_drops` and its getters ([pubsub.md](../c/pubsub.md)); `abi::a_skipped_sequence_reads_as_a_gap_under_detect` (*patterns*) |
| `TopicDrops::total` | refused: a computation over the eight counts the getters return |
| `DishDrops` | present: `weida_dish_drops` and its getters ([radio.md](../c/radio.md)); `abi::a_superseded_segment_counts_in_dish_drops`, `abi::a_radio_datagram_reaches_a_dish_that_carries_datagrams`, `abi::a_radio_evicts_a_dish_by_its_key` (*patterns*) |
| `Drained` | present: `weida_drained`, `weida_drained_delivered`, `weida_drained_outstanding` ([runtime.md](../c/runtime.md)); `abi::a_drain_counts_a_reply_finished_before_it_as_delivered` (*drain*) |

## 7. Patterns

| `crates/weida` | C ABI | Verdict |
| --- | --- | --- |
| `Pusher::connect` | `weida_pusher_connect` (op) | present; `abi::push_to_pull_over_inproc_arrives_in_order` (*patterns*) |
| `Pusher::disconnect`, `peer_count` | `weida_pusher_disconnect` (1 or 0), `weida_pusher_peer_count` | present; `abi::push_to_pull_over_inproc_arrives_in_order` (*patterns*) |
| `Pusher::connection_stats`, `events` | `weida_pusher_connection_stats` (a snapshot handle), `weida_pusher_events` (a `weida_peer_events` queue) | present; `abi::push_to_pull_over_inproc_arrives_in_order` (*patterns*) |
| `Pusher::dropped` | `weida_pusher_dropped` | present; `abi::push_to_pull_over_inproc_arrives_in_order` (*patterns*) |
| `Pusher::open` | `weida_pusher_open` (op; its wait yields a `weida_outgoing_transfer`) | present; `abi::push_to_pull_over_inproc_arrives_in_order` (*patterns*) |
| `Pusher::send`, `send_with` | `weida_pusher_send`, `_send_with` (ops) | present; `abi::push_to_pull_over_inproc_arrives_in_order` (*patterns*) |
| `Puller::path` | `weida_puller_path` | present; `abi::push_to_pull_over_inproc_arrives_in_order` (*patterns*) |
| `Puller::recv` | `weida_puller_recv` (try, R3a) | present; `abi::push_to_pull_over_inproc_arrives_in_order` (*patterns*) |
| `Publisher::path` | `weida_publisher_path` | present; R7 getter (*patterns*) |
| `Publisher::publish`, `publish_with_trace` | `weida_publisher_publish`, `_publish_with_trace` (plain functions, R3) | present; `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `Publisher::open`, `open_with_trace` | `weida_publisher_open`, `_open_with_trace` (a `weida_fan_out`) | present; `abi::a_fan_out_streams_one_payload_to_every_subscriber` (*patterns*) |
| `Publisher::subscriber_count`, `filter_count` | `weida_publisher_subscriber_count`, `_filter_count` | present; `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `Publisher::dropped` | `weida_publisher_dropped` | present; `abi::a_skipped_sequence_reads_as_a_gap_under_detect` (*patterns*) |
| `Publisher::dropped_on`, `drops` | `weida_publisher_dropped_on`, `_drops` (`weida_topic_drops` snapshots) | present; `abi::a_skipped_sequence_reads_as_a_gap_under_detect` (*patterns*) |
| `Subscriber::connect` | `weida_subscriber_connect` (op) | present; `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `Subscriber::disconnect`, `peer_count` | `weida_subscriber_disconnect` (1 or 0), `weida_subscriber_peer_count` | present; `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `Subscriber::connection_stats`, `events` | `weida_subscriber_connection_stats`, `weida_subscriber_events` | present; `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `Subscriber::subscribe`, `unsubscribe` | `weida_subscriber_subscribe`, `_unsubscribe` (ops) | present; `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `Subscriber::filter_count` | `weida_subscriber_filter_count` | present; `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `Subscriber::recv` | `weida_subscriber_recv` (try, R3a) | present; `abi::a_subscriber_receives_only_its_topic`, `abi::a_skipped_sequence_reads_as_a_gap_under_detect` (*patterns*) |
| `Subscriber::grant` | `weida_subscriber_grant` (op) | present; `abi::a_subscriber_receives_only_its_topic` (*patterns*) |
| `FanOut::topic`, `subscribers` | `weida_fan_out_topic`, `_subscribers` | present; `abi::a_fan_out_streams_one_payload_to_every_subscriber` (*patterns*) |
| `FanOut::write_within` | `weida_fan_out_write_within` (op) | present; `abi::a_fan_out_streams_one_payload_to_every_subscriber` (*patterns*) |
| `FanOut::write_now` | `weida_fan_out_write_now` | present; `abi::a_fan_out_streams_one_payload_to_every_subscriber` (*patterns*) |
| `FanOut::finish` | `weida_fan_out_finish` (consumes) | present; `abi::a_fan_out_streams_one_payload_to_every_subscriber` (*patterns*) |
| `Drop for FanOut` (aborts every copy) | `weida_fan_out_drop` | present; R1 (*patterns*) |
| `Paired::path` | `weida_paired_path` (empty on a dialling pair) | present; `abi::a_pair_exchanges_in_both_directions` (*patterns*) |
| `Paired::connect` | `weida_paired_connect` (op) | present; `abi::a_pair_exchanges_in_both_directions` (*patterns*) |
| `Paired::peer_count` | `weida_paired_peer_count` | present; `abi::a_pair_exchanges_in_both_directions` (*patterns*) |
| `Paired::connection_stats` | `weida_paired_connection_stats` (a snapshot handle) | present; `abi::a_pair_exchanges_in_both_directions` (*patterns*) |
| `Paired::events` | `weida_paired_events` (1 and a `weida_peer_events` queue, or 0 for a bound pair) | present; `abi::a_pair_exchanges_in_both_directions` (*patterns*) |
| `Paired::dropped` | `weida_paired_dropped` | present; `abi::a_pair_exchanges_in_both_directions` (*patterns*) |
| `Paired::open` | `weida_paired_open` (op; its wait yields a `weida_outgoing_transfer`) | present; `abi::a_pair_exchanges_in_both_directions` (*patterns*) |
| `Paired::send`, `send_with` | `weida_paired_send`, `_send_with` (ops) | present; `abi::a_pair_exchanges_in_both_directions` (*patterns*) |
| `Paired::recv` | `weida_paired_recv` (try, R3a) | present; `abi::a_pair_exchanges_in_both_directions` (*patterns*) |
| `Drop for Paired` | `weida_paired_drop` | present; R1 (*patterns*) |
| `Surveyor::connect` | `weida_surveyor_connect` (op) | present; `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `Surveyor::peer_count` | `weida_surveyor_peer_count` | present; `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `Surveyor::connection_stats` | `weida_surveyor_connection_stats` (a snapshot handle) | present; `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `Surveyor::survey`, `survey_with` | `weida_surveyor_survey`, `_survey_with` (ops; their wait yields a `weida_survey_run`) | present; `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `SurveyRun::next` | `weida_survey_run_next` (try, R3a; an answer is a `weida_survey_answer`) | present; `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `SurveyRun::late` | `weida_survey_run_late` | present; `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `SurveyRun::respondents` | `weida_survey_run_respondents` | present; `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `Drop for SurveyRun` (resets the unanswered exchanges) | `weida_survey_run_drop` | present; R1 (*patterns*) |
| `Respondent::path` | `weida_respondent_path` | present; `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `Respondent::accept` | `weida_respondent_accept` (try, R3a; a `weida_incoming_request`) | present; `abi::a_survey_collects_two_respondents_answers_within_its_window` (*patterns*) |
| `BusMember::path` | `weida_bus_member_path` | present; `abi::a_bus_message_reaches_every_other_member` (*patterns*) |
| `BusMember::connect` | `weida_bus_member_connect` (op) | present; `abi::a_bus_message_reaches_every_other_member` (*patterns*) |
| `BusMember::peer_count` | `weida_bus_member_peer_count` | present; `abi::a_bus_message_reaches_every_other_member` (*patterns*) |
| `BusMember::connection_stats` | `weida_bus_member_connection_stats` (a snapshot handle) | present; `abi::a_bus_message_reaches_every_other_member` (*patterns*) |
| `BusMember::send`, `send_with` | `weida_bus_member_send`, `_send_with` (ops; their wait writes the members reached) | present; `abi::a_bus_message_reaches_every_other_member` (*patterns*) |
| `BusMember::recv` | `weida_bus_member_recv` (try, R3a) | present; `abi::a_bus_message_reaches_every_other_member` (*patterns*) |
| `BusMember::dropped` | `weida_bus_member_dropped` | present; `abi::a_bus_message_reaches_every_other_member` (*patterns*) |
| `Radio::path` | `weida_radio_path` | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Radio::segment` | `weida_radio_segment` (a `weida_segment`) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Radio::datagram` | `weida_radio_datagram` (writes the dishes handed) | present; `abi::a_radio_datagram_reaches_a_dish_that_carries_datagrams` (*patterns*) |
| `Radio::dish_count` | `weida_radio_dish_count` | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Radio::dropped` | `weida_radio_dropped` | present; `abi::a_superseded_segment_counts_in_dish_drops` (*patterns*) |
| `Radio::dropped_on`, `drops` | `weida_radio_dropped_on`, `_drops` (`weida_topic_drops` snapshots) | present; `abi::a_superseded_segment_counts_in_dish_drops`, `abi::a_radio_datagram_reaches_a_dish_that_carries_datagrams` (*patterns*) |
| `Radio::evict` | `weida_radio_evict` (key as `sha256:` text) | present; `abi::a_radio_evicts_a_dish_by_its_key` (*patterns*) |
| `Radio::dish_drops` | `weida_radio_dish_drops` (a `weida_dish_drops` snapshot) | present; `abi::a_superseded_segment_counts_in_dish_drops`, `abi::a_radio_datagram_reaches_a_dish_that_carries_datagrams`, `abi::a_radio_evicts_a_dish_by_its_key` (*patterns*) |
| `Drop for Radio` (releases the path with the last handle) | `weida_radio_drop` | present; R1 (*patterns*) |
| `Radio::with_admission`, `Join` | — | refused: a policy the connection's task calls synchronously before it records a join. 0045 decision 10 maps inverted control to queues and has no function pointers, and a queue would answer after the join was recorded. `weida_radio_evict` withdraws a join afterwards |
| `Clone for Radio` | — | refused: one handle serves every thread (R5) |
| `Segment::number`, `topic` | `weida_segment_number`, `_topic` | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Segment::write`, `write_layer` | `weida_segment_write`, `_write_layer` (plain functions, R3) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Segment::finish_layer` | `weida_segment_finish_layer` | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Segment::finish` | `weida_segment_finish` (consumes) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers`, `abi::a_superseded_segment_counts_in_dish_drops` (*patterns*) |
| `Drop for Segment` (resets unfinished layers) | `weida_segment_drop` | present; R1 (*patterns*) |
| `SegmentTerms` | `weida_segment_terms_*` (a builder) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `JoinTerms` | `weida_join_terms_*` (a builder) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `MAX_SEGMENT_LAYERS` | `WEIDA_MAX_SEGMENT_LAYERS` (asserted at compile time) | present (*patterns*) |
| `Dish::connect` | `weida_dish_connect` (op) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Dish::peer_count`, `connection_stats`, `events` | `weida_dish_peer_count`, `_connection_stats` (a snapshot handle), `_events` (a `weida_peer_events` queue) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Dish::join`, `leave` | `weida_dish_join`, `_leave` (ops) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Dish::recv` | `weida_dish_recv` (try, R3a; a `weida_received`) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers`, `abi::a_radio_datagram_reaches_a_dish_that_carries_datagrams` (*patterns*) |
| `Dish::stale`, `overflow` | `weida_dish_stale`, `_overflow` | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers` (*patterns*) |
| `Drop for Dish` | `weida_dish_drop` | present; R1 (*patterns*) |
| `Received` | `weida_received` (`_kind`, `_take_segment`, `_topic`, `_segment`, `_payload`; `WEIDA_RECEIVED_*`) | present; `abi::a_dish_receives_the_radios_segments_with_their_numbers_and_layers`, `abi::a_radio_datagram_reaches_a_dish_that_carries_datagrams` (*patterns*) |

Datagram flows (`Flow`, `FlowInfo`, `FlowMeta`, `FlowStats`, `IncomingFlow`), the raw `Peer`,
`Acceptor`, `Incoming`, `Consumer`, `ConsumerId`, `CreditGrant`, the cursor surface (`Cursors`,
`CursorSet`, `Reporter`, `CursorLevel`, `ReportMode`), and SWARM (`Seeder`, `Swarm`, `Fetch`,
`Blob`, `BlobId` and the rest behind the `swarm` feature): each absent, in the areas *flows*,
*raw peers*, *cursors* and *swarm*.

## 8. Interop evidence

The ABI has no second implementation to run against; what proves it is that it is called the
way a foreign caller calls it:

- `smoke.c`, compiled from `weida.h` under `-std=c11 -Wall -Wextra -Werror`, linked with
  `-lweida`: a QUIC Req/Rep with a generated identity, a pinned trust, streamed bodies, a
  content type and a delivery receipt, every wait a timeout-0 call plus `weida_poll`;
- the fifty `abi::` tests, which call the exports as C does (raw pointers, out-parameters,
  descriptors, threads, a signal);
- the Python package `crates/py/weida-py`, a cffi caller in ABI mode
  ([weida-py.md](weida-py.md)).

## 9. Deliberate deviations and bounds

- `take_body` twice is `-WEIDA_ECLOSED`; Rust panics.
- A collect's `max_bytes` is `uint64_t`; above `usize::MAX` it is `usize::MAX`.
- A readiness descriptor costs a socket pair per handle, created on first use (R11).
- On Windows a blocking wait cannot be interrupted (R4); callers bound their waits.
- `expire_at` takes milliseconds from the call (`weida_outgoing_transfer_expire_after_ms`),
  because C has no `Instant`.
- `endpoint_queue` 0 is `-WEIDA_EINVAL` at the setter, where Rust panics at the first queue.
- An optional value in is negative for `None` (R7).
- A count in above `usize::MAX` becomes `usize::MAX` (R7).
- A refusal code outside `WEIDA_ERROR_CODE_*` is `-WEIDA_EINVAL`; the request is consumed all
  the same (R6) and answers `NO_REPLY`.
- `canceled` is an op started from the request, and like Rust's future it outlives it; start
  it before `reply` or `refuse` consumes the request.
- A duration out is microseconds (`_us`, R7).
- `connection_stats()` is one snapshot handle whose records are read by index; Rust returns a
  `Vec`.
- The end of a peer event stream (`None`) is a 0 return with `*out == NULL`, as a read's end is
  `*out_n == 0`.
- `ConnectionId` is its number (`ConnectionId::get`).
- `Runtime::drain` returns `Drained` by value; the ABI's op yields it as a `weida_drained` handle
  read by two getters. Its deadline is milliseconds (R7) and, as in Rust, mandatory.
- `Handoff::into_parts` is two calls: `weida_handoff_take_socket` hands the descriptor over
  once, and `weida_handoff_bytes` lends the held packets for as long as the handoff lives.
  Dropping a handoff whose socket was not taken closes it.
- A socket in or out is its descriptor as `int64_t`: a file descriptor on Unix, a `SOCKET` on
  Windows.
- `Binding::hand_over` panics in Rust on a binding without an OS socket. The ABI returns
  `-WEIDA_EUNSUPPORTED` for a relayed binding and `-WEIDA_ECLOSED` for one a
  `weida_binding_close` was started on, and consumes the binding either way (R6).
- `Binding::disconnect` and `RelayRequest::peer`/`target` take and give keys as `sha256:` text.
- `weida_binding_drop` releases the handle only; the binding serves on, as a dropped Rust
  `Binding` does, until `weida_binding_close`, a hand-over or the runtime's shutdown.
- `bind_unix` exists in Rust only on Unix and `bind_pipe` only on Windows. The ABI exports both
  everywhere and the other platform's returns `-WEIDA_EUNSUPPORTED`, so one `weida.h` serves
  every platform.
- A socket path is UTF-8 text (R7); a path that is not UTF-8 cannot be bound through the ABI.
- `LocalPrincipal` and `WindowsPrincipal` are read through four getters on the metadata
  (`weida_incoming_meta_peer_uid`, `_peer_gid`, `_peer_pid`, `_peer_sid`), not as values.
- A Rust parameter that takes a value or a source (`impl Into<IdentitySource>`,
  `impl Into<TrustSource>`) maps twice: the value form keeps its name and copies, the form with
  the `_source` suffix takes a `weida_identity_source` or `weida_trust_source` and shares it.
- The fingerprint of an identity source is written into the caller's buffer of
  `WEIDA_FINGERPRINT_LEN + 1` bytes, because it changes under the handle and handle-owned text
  (R7) could not stay valid.
- `IdentitySource::reload`'s `Result<bool>` is 1, 0 or a negative code.
- `WeakIdentitySource::upgrade`'s and `WeakTrustSource::upgrade`'s `None` is a 0 return with
  `*out == NULL`.
- The end of an identity event stream (`None`) is a 0 return with `*out == NULL`, as for peer
  events.
- `FilesOptions::poll` is milliseconds (`_set_poll_ms`, R7); a file source's directory is UTF-8
  text (R7).
- A call on a `weida_fan_out` while its `write_within` op runs is `-WEIDA_EINVAL`, where Rust's
  borrow rules forbid it; `finish` then consumes the handle and the copies abort when the write
  ends.
- `Publisher::dropped_on`'s `Option` is a snapshot of 0 or 1 record.
- `SurveyRun::next` is a try (R3a), as `PeerEvents::recv` is; a read in flight keeps the
  `max_bytes` of the try that started it. Its `None` is a 0 return with `*out == NULL`, and an
  answer that failed returns its code while the run goes on.
- An answer is a `weida_survey_answer` (`_data`, `_len`), the `Vec<u8>` Rust returns; it
  carries no metadata, as in Rust.
- `weida_survey_run_late` during a `weida_survey_run_next` in flight is the count as of the
  last read that ended.
- `Paired::events`' `None` (a bound pair) is a 0 return with `out` untouched, as for
  `Binding::relay_events`.
- A `deadline_ms` the clock cannot add is `-WEIDA_EINVAL` at `weida_surveyor_survey`, where
  Rust panics.
- `Received` is a tagged handle, `weida_received`: `weida_received_kind` names the variant,
  `weida_received_take_segment` hands a stream segment's transfer over once (a second call is
  `-WEIDA_ECLOSED`, a datagram `-WEIDA_EINVAL`), and a datagram's fields are getters that read
  absent on a stream segment.
- `Radio::evict` takes the dish's key as `sha256:` text, and `DishDrops::peer` reads as
  `weida_dish_drops_peer_fingerprint`, NULL for an anonymous peer or one that proved no key.
- `SegmentTerms::priority` is `int32_t` in; outside `int16_t` it is `-WEIDA_EINVAL`.
- `JoinTerms::max_layer` is an optional value in (R7); 16 to 255 fail the join with
  `-WEIDA_ELIMIT_EXCEEDED`, as in Rust.
- A segment's calls take its lock in turn, where Rust's `&mut self` forbids concurrent use.

## 10. The definition of done

The plan of this stage asked for the rules, the decision, and one vertical slice: runtime,
configuration, TLS values, Req/Rep with streamed bodies, delivery receipts, readiness
descriptors and `weida_poll`, through Rust, the header, a C program, Python and the CI. Every
row of §2 to §5 marked present without an area name is that slice, and a present row that
names an area was mapped by that area's plan; every absent row names its area, and the slice
is complete when no present row lacks a test.

## 11. Sources

`crates/weida-c/src/` (`error.rs`, `ready.rs`, `try_handle.rs`, `op.rs`, `handle.rs`,
`config.rs`, `runtime.rs`, `tls.rs`, `identity.rs`, `listener.rs`, `relay.rs`, `transfer.rs`,
`requester.rs`, `push.rs`, `pubsub.rs`, `pair.rs`, `survey.rs`, `bus.rs`, `radio.rs`,
`segment.rs`, `poll.rs`, `stats.rs`),
`crates/weida-c/include/weida.h`, `crates/weida-c/tests/`, `crates/weida/src/lib.rs` for the
reference column, [decisions/0045](../decisions/0045-the-c-abi-is-the-boundary.md), and
[docs/c/](../c/README.md).
