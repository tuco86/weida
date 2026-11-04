#define WEIDA_BLOB_ID_LEN 32

#define WEIDA_MAX_PIECES 1048576

#define WEIDA_PIECE_SIZE_MIN_LOG2 10

#define WEIDA_PIECE_SIZE_MAX_LOG2 24

#define WEIDA_PIECE_SIZE_DEFAULT_LOG2 20

#define WEIDA_CONGESTION_CUBIC 0

#define WEIDA_CONGESTION_NEW_RENO 1

#define WEIDA_CONGESTION_BBR 2

#define WEIDA_DISCOVERY_AWARE 0

#define WEIDA_DISCOVERY_SINGLE 1

#define WEIDA_OUTBOX_FULL_BLOCK 0

#define WEIDA_OUTBOX_FULL_DROP 1

#define WEIDA_OUTBOX_FULL_REJECT 2

#define WEIDA_DELIVERY_LEVEL_BEST_EFFORT 0

#define WEIDA_DELIVERY_LEVEL_AT_MOST_ONCE 1

#define WEIDA_DELIVERY_LEVEL_AT_LEAST_ONCE 2

#define WEIDA_ACKNOWLEDGEMENT_NONE 0

#define WEIDA_ACKNOWLEDGEMENT_TRANSPORT_RECEIPT 1

#define WEIDA_ACKNOWLEDGEMENT_ACCEPTED 2

#define WEIDA_ACKNOWLEDGEMENT_STORED 3

#define WEIDA_ACKNOWLEDGEMENT_REPLICATED 4

#define WEIDA_ACKNOWLEDGEMENT_PROCESSED 5

#define WEIDA_DURABILITY_WRITTEN 0

#define WEIDA_DURABILITY_FLUSHED 1

#define WEIDA_ORDERING_MODE_NONE 0

#define WEIDA_ORDERING_MODE_PER_PRODUCER_DETECT 1

#define WEIDA_ORDERING_MODE_PER_PRODUCER_REASSEMBLE 2

#define WEIDA_ORDERING_MODE_PER_KEY 3

#define WEIDA_ORDERING_MODE_TOTAL 4

#define WEIDA_DEDUPLICATION_NONE 0

#define WEIDA_DEDUPLICATION_BOUNDED 1

#define WEIDA_DEDUPLICATION_DURABLE 2

#define WEIDA_BACKPRESSURE_BLOCK 0

#define WEIDA_BACKPRESSURE_REJECT 1

#define WEIDA_BACKPRESSURE_DROP 2

#define WEIDA_BACKPRESSURE_SPILL 3

#define WEIDA_BACKPRESSURE_COALESCE 4

#define WEIDA_PRODUCER_NAMING_FINGERPRINT 0

#define WEIDA_PRODUCER_NAMING_STABLE 1

#define WEIDA_ALPN_WEIDA 0

#define WEIDA_ALPN_RAFT 1

#define WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES 65536

#define WEIDA_TRACEPARENT_LEN 55

#define WEIDA_FINGERPRINT_LEN 71

#define WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR 16

#define WEIDA_REPORT_MODE_PROGRESS 0

#define WEIDA_REPORT_MODE_FINAL_ONLY 1

#define WEIDA_REPORTER_DEFAULT_BYTES 1048576

#define WEIDA_REPORTER_DEFAULT_INTERVAL_MS 100

#define WEIDA_ERUNTIME 1

#define WEIDA_EINVALID_ADDRESS 2

#define WEIDA_EINVALID_ENDPOINT_PATH 3

#define WEIDA_EINVALID_FINGERPRINT 4

#define WEIDA_EALREADY_REGISTERED 5

#define WEIDA_ENOT_CONNECTED 6

#define WEIDA_ECONNECTION_LOST 7

#define WEIDA_ENEGOTIATION 8

#define WEIDA_EPROTOCOL 9

#define WEIDA_EREJECTED 10

#define WEIDA_EUNKNOWN_ENDPOINT 11

#define WEIDA_EUNSUPPORTED 12

#define WEIDA_ENO_PARKED_CONNECTION 13

#define WEIDA_ENO_REPLY 14

#define WEIDA_ECANCELED 15

#define WEIDA_EEXPIRED 16

#define WEIDA_EINDETERMINATE 17

#define WEIDA_ELIMIT_EXCEEDED 18

#define WEIDA_EDATAGRAMS_UNAVAILABLE 19

#define WEIDA_ETOO_LARGE 20

#define WEIDA_ETLS 21

#define WEIDA_EUNTRUSTED 22

#define WEIDA_EIO 23

#define WEIDA_ETRANSPORT 24

#define WEIDA_EAGAIN 100

#define WEIDA_EINTR 101

#define WEIDA_EINVAL 102

#define WEIDA_ECLOSED 103

#define WEIDA_EPANIC 104

#define WEIDA_LOSS_IDLE_TIMEOUT 1

#define WEIDA_LOSS_PEER_CLOSED 2

#define WEIDA_LOSS_PEER_RESTARTING 3

#define WEIDA_LOSS_LOCALLY_CLOSED 4

#define WEIDA_LOSS_RESET 5

#define WEIDA_LOSS_TRANSPORT_ERROR 6

#define WEIDA_ERROR_CODE_UNKNOWN_ENDPOINT 1

#define WEIDA_ERROR_CODE_REJECTED 2

#define WEIDA_ERROR_CODE_UNSUPPORTED 3

#define WEIDA_ERROR_CODE_INTERNAL 4

#define WEIDA_ERROR_CODE_NO_REPLY 5

#define WEIDA_IDENTITY_EVENT_LOADED 0

#define WEIDA_IDENTITY_EVENT_RENEWED 1

#define WEIDA_IDENTITY_EVENT_KEY_CHANGED 2

#define WEIDA_IDENTITY_EVENT_RENEWAL_FAILED 3

#define WEIDA_IDENTITY_EVENT_HANDOFF_STOLEN 4

#define WEIDA_IDENTITY_EVENT_TRUST_REFRESHED 5

#define WEIDA_IDENTITY_EVENT_MISSED 6

#define WEIDA_INCOMING_STREAM 0

#define WEIDA_INCOMING_EXCHANGE 1

#define WEIDA_INCOMING_SUBSCRIBED 2

#define WEIDA_INCOMING_CREDIT 3

#define WEIDA_INCOMING_UNSUBSCRIBED 4

#define WEIDA_INCOMING_FLOW 5

#define WEIDA_POLLIN 1

#define WEIDA_RECEIVED_SEGMENT 0

#define WEIDA_RECEIVED_DATAGRAM 1

#define WEIDA_MAX_SEGMENT_LAYERS 16

#define WEIDA_PEER_EVENT_CONNECTED 0

#define WEIDA_PEER_EVENT_LOST 1

#define WEIDA_PEER_EVENT_RETRYING 2

#define WEIDA_PEER_EVENT_GAVE_UP 3

#define WEIDA_PEER_EVENT_MISSED 4

#define WEIDA_GIVE_UP_POLICY 0

#define WEIDA_GIVE_UP_PEER_CHANGED 1

#define WEIDA_GIVE_UP_FAILED 2

#define WEIDA_ASK_LIST 0

#define WEIDA_ASK_HAVE 1

#define WEIDA_ASK_PIECE 2

#define WEIDA_MAX_PENDING_SESSIONS 64

#define WEIDA_SEED_STALL_MS 30000

#define WEIDA_MIN_RATE 65536

typedef struct weida_acceptor weida_acceptor;

typedef struct weida_binding weida_binding;

typedef struct weida_binding_close_op weida_binding_close_op;

typedef struct weida_binding_hand_over_op weida_binding_hand_over_op;

typedef struct weida_binding_take_over_op weida_binding_take_over_op;

typedef struct weida_bits weida_bits;

typedef struct weida_blob weida_blob;

typedef struct weida_bus_member weida_bus_member;

typedef struct weida_bus_member_connect_op weida_bus_member_connect_op;

typedef struct weida_bus_member_send_op weida_bus_member_send_op;

typedef struct weida_client_tls weida_client_tls;

typedef struct weida_connection_stats weida_connection_stats;

typedef struct weida_consumer weida_consumer;

typedef struct weida_consumer_deliver_op weida_consumer_deliver_op;

typedef struct weida_consumer_open_op weida_consumer_open_op;

typedef struct weida_credit_grant weida_credit_grant;

typedef struct weida_cursor_set weida_cursor_set;

typedef struct weida_cursors weida_cursors;

typedef struct weida_datagram weida_datagram;

typedef struct weida_delivery weida_delivery;

typedef struct weida_delivery_delivered_op weida_delivery_delivered_op;

typedef struct weida_dish weida_dish;

typedef struct weida_dish_connect_op weida_dish_connect_op;

typedef struct weida_dish_drops weida_dish_drops;

typedef struct weida_dish_join_op weida_dish_join_op;

typedef struct weida_dish_leave_op weida_dish_leave_op;

typedef struct weida_drained weida_drained;

typedef struct weida_fan_out weida_fan_out;

typedef struct weida_fan_out_write_within_op weida_fan_out_write_within_op;

typedef struct weida_fetch weida_fetch;

typedef struct weida_fetch_options weida_fetch_options;

typedef struct weida_files_options weida_files_options;

typedef struct weida_flow weida_flow;

typedef struct weida_flow_meta weida_flow_meta;

typedef struct weida_guarantee_set weida_guarantee_set;

typedef struct weida_handoff weida_handoff;

typedef struct weida_identity weida_identity;

typedef struct weida_identity_event weida_identity_event;

typedef struct weida_identity_events weida_identity_events;

typedef struct weida_identity_source weida_identity_source;

typedef struct weida_incoming weida_incoming;

typedef struct weida_incoming_flow weida_incoming_flow;

typedef struct weida_incoming_meta weida_incoming_meta;

typedef struct weida_incoming_request weida_incoming_request;

typedef struct weida_incoming_request_canceled_op weida_incoming_request_canceled_op;

typedef struct weida_incoming_request_refuse_op weida_incoming_request_refuse_op;

typedef struct weida_incoming_request_reply_op weida_incoming_request_reply_op;

typedef struct weida_incoming_transfer weida_incoming_transfer;

typedef struct weida_incoming_transfer_collect_op weida_incoming_transfer_collect_op;

typedef struct weida_join_terms weida_join_terms;

typedef struct weida_limits weida_limits;

typedef struct weida_listener weida_listener;

typedef struct weida_listener_bind_quic_op weida_listener_bind_quic_op;

typedef struct weida_listener_bind_quic_socket_op weida_listener_bind_quic_socket_op;

typedef struct weida_listener_bind_relay_op weida_listener_bind_relay_op;

typedef struct weida_local_binding weida_local_binding;

typedef struct weida_msg weida_msg;

typedef struct weida_outgoing_transfer weida_outgoing_transfer;

typedef struct weida_paired weida_paired;

typedef struct weida_paired_connect_op weida_paired_connect_op;

typedef struct weida_paired_open_op weida_paired_open_op;

typedef struct weida_paired_send_op weida_paired_send_op;

typedef struct weida_path_stats weida_path_stats;

typedef struct weida_peer weida_peer;

typedef struct weida_peer_connect_op weida_peer_connect_op;

typedef struct weida_peer_event weida_peer_event;

typedef struct weida_peer_events weida_peer_events;

typedef struct weida_peer_open_bi_op weida_peer_open_bi_op;

typedef struct weida_peer_open_flow_op weida_peer_open_flow_op;

typedef struct weida_peer_open_op weida_peer_open_op;

typedef struct weida_peer_segment_op weida_peer_segment_op;

typedef struct weida_peer_send_op weida_peer_send_op;

typedef struct weida_pem weida_pem;

typedef struct weida_piece weida_piece;

typedef struct weida_piece_list weida_piece_list;

typedef struct weida_pipe_binding weida_pipe_binding;

typedef struct weida_publisher weida_publisher;

typedef struct weida_puller weida_puller;

typedef struct weida_pusher weida_pusher;

typedef struct weida_pusher_connect_op weida_pusher_connect_op;

typedef struct weida_pusher_open_op weida_pusher_open_op;

typedef struct weida_pusher_send_op weida_pusher_send_op;

typedef struct weida_radio weida_radio;

typedef struct weida_received weida_received;

typedef struct weida_reconnect_policy weida_reconnect_policy;

typedef struct weida_relay weida_relay;

typedef struct weida_relay_request weida_relay_request;

typedef struct weida_relay_request_admit_op weida_relay_request_admit_op;

typedef struct weida_relay_request_refuse_op weida_relay_request_refuse_op;

typedef struct weida_replier weida_replier;

typedef struct weida_reply_stream weida_reply_stream;

typedef struct weida_reply_stream_recv_op weida_reply_stream_recv_op;

typedef struct weida_reporter weida_reporter;

typedef struct weida_reporter_finish_op weida_reporter_finish_op;

typedef struct weida_reporter_report_op weida_reporter_report_op;

typedef struct weida_requester weida_requester;

typedef struct weida_requester_connect_op weida_requester_connect_op;

typedef struct weida_requester_open_op weida_requester_open_op;

typedef struct weida_requester_request_op weida_requester_request_op;

typedef struct weida_resolved weida_resolved;

typedef struct weida_resolver weida_resolver;

typedef struct weida_respondent weida_respondent;

typedef struct weida_runtime weida_runtime;

typedef struct weida_runtime_config weida_runtime_config;

typedef struct weida_runtime_drain_op weida_runtime_drain_op;

typedef struct weida_runtime_shutdown_op weida_runtime_shutdown_op;

typedef struct weida_seed_session weida_seed_session;

typedef struct weida_seed_session_answer_have_op weida_seed_session_answer_have_op;

typedef struct weida_seed_session_answer_list_op weida_seed_session_answer_list_op;

typedef struct weida_seed_session_answer_piece_op weida_seed_session_answer_piece_op;

typedef struct weida_seed_session_decline_op weida_seed_session_decline_op;

typedef struct weida_seeder weida_seeder;

typedef struct weida_segment weida_segment;

typedef struct weida_segment_terms weida_segment_terms;

typedef struct weida_server_tls weida_server_tls;

typedef struct weida_subscriber weida_subscriber;

typedef struct weida_subscriber_connect_op weida_subscriber_connect_op;

typedef struct weida_subscriber_grant_op weida_subscriber_grant_op;

typedef struct weida_subscriber_subscribe_op weida_subscriber_subscribe_op;

typedef struct weida_subscriber_unsubscribe_op weida_subscriber_unsubscribe_op;

typedef struct weida_survey_answer weida_survey_answer;

typedef struct weida_survey_run weida_survey_run;

typedef struct weida_surveyor weida_surveyor;

typedef struct weida_surveyor_connect_op weida_surveyor_connect_op;

typedef struct weida_surveyor_survey_op weida_surveyor_survey_op;

typedef struct weida_swarm weida_swarm;

typedef struct weida_swarm_connect_op weida_swarm_connect_op;

typedef struct weida_topic_drops weida_topic_drops;

typedef struct weida_transfer_meta weida_transfer_meta;

typedef struct weida_tree_hasher weida_tree_hasher;

typedef struct weida_trust weida_trust;

typedef struct weida_trust_source weida_trust_source;

typedef struct weida_unix_binding weida_unix_binding;

typedef struct weida_weak_identity_source weida_weak_identity_source;

typedef struct weida_weak_trust_source weida_weak_trust_source;

typedef struct {
  int64_t fd;
  int16_t events;
  int16_t revents;
} weida_poll_item;

const char *weida_version(void);

int weida_blob_id_of(const uint8_t *data, size_t len, uint8_t *out);

int weida_blob_new(const uint8_t *root, uint64_t size, uint8_t piece_log2, weida_blob **out);

int weida_blob_root(const weida_blob *b, uint8_t *out);

uint64_t weida_blob_size(const weida_blob *b);

uint8_t weida_blob_piece_log2(const weida_blob *b);

uint32_t weida_blob_count(const weida_blob *b);

int weida_blob_span(const weida_blob *b, uint32_t index, uint64_t *out_start, uint64_t *out_end);

void weida_blob_drop(weida_blob *b);

int weida_piece_list_of(const uint8_t *data,
                        size_t len,
                        uint8_t piece_log2,
                        weida_piece_list **out);

int weida_piece_list_parse(const weida_blob *blob,
                           const uint8_t *data,
                           size_t len,
                           weida_piece_list **out);

int weida_piece_list_blob(const weida_piece_list *l, weida_blob **out);

const uint8_t *weida_piece_list_to_bytes(const weida_piece_list *l, size_t *len_out);

int weida_piece_list_check(const weida_piece_list *l,
                           uint32_t index,
                           const uint8_t *piece,
                           size_t len);

void weida_piece_list_drop(weida_piece_list *l);

int weida_tree_hasher_new(uint8_t piece_log2, weida_tree_hasher **out);

int weida_tree_hasher_update(const weida_tree_hasher *h, const uint8_t *data, size_t len);

int weida_tree_hasher_finish(weida_tree_hasher *h, weida_piece_list **out);

void weida_tree_hasher_drop(weida_tree_hasher *h);

int weida_bits_new(uint32_t len, weida_bits **out);

int weida_bits_full(uint32_t len, weida_bits **out);

int weida_bits_from_bytes(uint32_t len, const uint8_t *data, size_t n, weida_bits **out);

int weida_bits_as_bytes(const weida_bits *b, uint8_t *buf, size_t cap, size_t *out_n);

uint32_t weida_bits_len(const weida_bits *b);

uint32_t weida_bits_ones(const weida_bits *b);

int weida_bits_get(const weida_bits *b, uint32_t index);

int weida_bits_all(const weida_bits *b);

int weida_bits_set(const weida_bits *b, uint32_t index, int on);

void weida_bits_drop(weida_bits *b);

int weida_listener_bus(const weida_listener *l,
                       const char *path,
                       const weida_client_tls *tls,
                       weida_bus_member **out);

const char *weida_bus_member_path(const weida_bus_member *m, size_t *len_out);

int weida_bus_member_connect(const weida_bus_member *m,
                             const char *url,
                             weida_bus_member_connect_op **out);

int weida_bus_member_connect_op_fd(const weida_bus_member_connect_op *op, int64_t *out);

int weida_bus_member_connect_op_wait(const weida_bus_member_connect_op *op, int64_t timeout_ms);

void weida_bus_member_connect_op_drop(weida_bus_member_connect_op *op);

size_t weida_bus_member_peer_count(const weida_bus_member *m);

int weida_bus_member_connection_stats(const weida_bus_member *m, weida_connection_stats **out);

uint64_t weida_bus_member_dropped(const weida_bus_member *m);

int weida_bus_member_send(const weida_bus_member *m,
                          const uint8_t *body,
                          size_t len,
                          weida_bus_member_send_op **out);

int weida_bus_member_send_with(const weida_bus_member *m,
                               const weida_transfer_meta *meta,
                               const uint8_t *body,
                               size_t len,
                               weida_bus_member_send_op **out);

int weida_bus_member_send_op_fd(const weida_bus_member_send_op *op, int64_t *out);

int weida_bus_member_send_op_wait(const weida_bus_member_send_op *op,
                                  int64_t timeout_ms,
                                  size_t *out);

void weida_bus_member_send_op_drop(weida_bus_member_send_op *op);

int weida_bus_member_fd(const weida_bus_member *m, int64_t *out);

int weida_bus_member_recv(const weida_bus_member *m,
                          int64_t timeout_ms,
                          weida_incoming_transfer **out);

void weida_bus_member_drop(weida_bus_member *m);

int weida_limits_new(weida_limits **out);

void weida_limits_drop(weida_limits *l);

int weida_limits_set_max_header_bytes(const weida_limits *l, uint64_t v);

int weida_limits_set_max_concurrent_uni_streams(const weida_limits *l, uint32_t v);

int weida_limits_set_max_concurrent_bidi_streams(const weida_limits *l, uint32_t v);

int weida_limits_set_stream_receive_window(const weida_limits *l, uint64_t v);

int weida_limits_set_connection_receive_window(const weida_limits *l, uint64_t v);

int weida_limits_set_keep_alive_ms(const weida_limits *l, uint64_t v);

int weida_limits_set_idle_timeout_ms(const weida_limits *l, uint64_t v);

int weida_limits_set_flow_early_hold_ms(const weida_limits *l, uint64_t v);

int weida_limits_set_hello_timeout_ms(const weida_limits *l, uint64_t v);

int weida_limits_set_header_timeout_ms(const weida_limits *l, uint64_t v);

int weida_limits_set_max_subscriptions(const weida_limits *l, uint64_t v);

int weida_limits_set_subscriber_buffer_bytes(const weida_limits *l, uint64_t v);

int weida_limits_set_max_sequence_scopes(const weida_limits *l, uint64_t v);

int weida_limits_set_max_reorder_hold(const weida_limits *l, uint64_t v);

int weida_limits_set_max_local_streams(const weida_limits *l, uint64_t v);

int weida_limits_set_max_parked_reverse(const weida_limits *l, uint64_t v);

int weida_limits_set_max_dedup_entries(const weida_limits *l, uint64_t v);

int weida_limits_set_datagram_receive_bytes(const weida_limits *l, uint64_t v);

int weida_limits_set_datagram_send_bytes(const weida_limits *l, uint64_t v);

int weida_limits_set_max_flows(const weida_limits *l, uint64_t v);

int weida_limits_set_flow_queue_bytes(const weida_limits *l, uint64_t v);

int weida_limits_set_flow_early_bytes(const weida_limits *l, uint64_t v);

int weida_limits_set_max_header_streams(const weida_limits *l, uint64_t v);

int weida_limits_set_path_report(const weida_limits *l, int v);

int weida_limits_set_congestion(const weida_limits *l, int v);

int weida_guarantee_set_new(weida_guarantee_set **out);

void weida_guarantee_set_drop(weida_guarantee_set *g);

int weida_guarantee_set_set_delivery(const weida_guarantee_set *g, int v);

int weida_guarantee_set_set_acknowledgement(const weida_guarantee_set *g, int v);

int weida_guarantee_set_set_durability(const weida_guarantee_set *g, int v);

int weida_guarantee_set_set_replicas(const weida_guarantee_set *g, int64_t v);

int weida_guarantee_set_set_ordering(const weida_guarantee_set *g, int v);

int weida_guarantee_set_set_deduplication(const weida_guarantee_set *g, int v);

int weida_guarantee_set_set_dedup_window_ms(const weida_guarantee_set *g, int64_t v);

int weida_guarantee_set_set_backpressure(const weida_guarantee_set *g, int v);

int weida_guarantee_set_set_producer_naming(const weida_guarantee_set *g, int v);

int weida_guarantee_set_set_control_isolated(const weida_guarantee_set *g, int v);

int weida_reconnect_policy_new(weida_reconnect_policy **out);

int weida_reconnect_policy_never(weida_reconnect_policy **out);

void weida_reconnect_policy_drop(weida_reconnect_policy *p);

int weida_reconnect_policy_set_initial_ms(const weida_reconnect_policy *p, uint64_t v);

int weida_reconnect_policy_set_max_ms(const weida_reconnect_policy *p, uint64_t v);

int weida_reconnect_policy_set_jitter(const weida_reconnect_policy *p, int v);

int weida_reconnect_policy_set_max_attempts(const weida_reconnect_policy *p, int64_t v);

int weida_reconnect_policy_set_stop_on_peer_closed(const weida_reconnect_policy *p, int v);

int weida_files_options_new(weida_files_options **out);

void weida_files_options_drop(weida_files_options *o);

int weida_files_options_set_names(const weida_files_options *o, const char *const *names, size_t n);

int weida_files_options_set_poll_ms(const weida_files_options *o, uint64_t v);

int weida_join_terms_new(weida_join_terms **out);

void weida_join_terms_drop(weida_join_terms *t);

int weida_join_terms_set_max_age_ms(const weida_join_terms *t, int64_t v);

int weida_join_terms_set_max_layer(const weida_join_terms *t, int64_t v);

int weida_segment_terms_new(weida_segment_terms **out);

void weida_segment_terms_drop(weida_segment_terms *t);

int weida_segment_terms_set_max_age_ms(const weida_segment_terms *t, int64_t v);

int weida_segment_terms_set_priority(const weida_segment_terms *t, int32_t v);

int weida_segment_terms_set_follows_upstream(const weida_segment_terms *t, int v);

int weida_flow_meta_new(weida_flow_meta **out);

void weida_flow_meta_drop(weida_flow_meta *m);

int weida_flow_meta_set_content_type(const weida_flow_meta *m, const char *content_type);

int weida_flow_meta_set_topic(const weida_flow_meta *m, const char *topic);

int weida_flow_meta_set_trace(const weida_flow_meta *m, const char *traceparent);

int weida_fetch_options_new(weida_fetch_options **out);

void weida_fetch_options_drop(weida_fetch_options *o);

int weida_fetch_options_set_list(const weida_fetch_options *o, const weida_piece_list *list);

int weida_fetch_options_set_stall_ms(const weida_fetch_options *o, uint64_t v);

int weida_fetch_options_set_min_rate(const weida_fetch_options *o, uint64_t v);

int weida_fetch_options_set_per_peer(const weida_fetch_options *o, uint64_t v);

int weida_fetch_options_set_max_inflight(const weida_fetch_options *o, uint64_t v);

int weida_fetch_options_set_max_peers(const weida_fetch_options *o, uint64_t v);

int weida_fetch_options_set_have_refresh_ms(const weida_fetch_options *o, uint64_t v);

uint64_t weida_consumer_id(const weida_consumer *c);

const char *weida_consumer_filter(const weida_consumer *c, size_t *len_out);

const char *weida_consumer_path(const weida_consumer *c, size_t *len_out);

int weida_consumer_open(const weida_consumer *c,
                        const weida_transfer_meta *meta,
                        weida_consumer_open_op **out);

int weida_consumer_open_op_fd(const weida_consumer_open_op *op, int64_t *out);

int weida_consumer_open_op_wait(const weida_consumer_open_op *op,
                                int64_t timeout_ms,
                                weida_outgoing_transfer **out);

void weida_consumer_open_op_drop(weida_consumer_open_op *op);

int weida_consumer_deliver(const weida_consumer *c,
                           const weida_transfer_meta *meta,
                           const uint8_t *body,
                           size_t len,
                           weida_consumer_deliver_op **out);

int weida_consumer_deliver_op_fd(const weida_consumer_deliver_op *op, int64_t *out);

int weida_consumer_deliver_op_wait(const weida_consumer_deliver_op *op, int64_t timeout_ms);

void weida_consumer_deliver_op_drop(weida_consumer_deliver_op *op);

int weida_consumer_close(const weida_consumer *c, uint64_t code, const char *reason);

void weida_consumer_drop(weida_consumer *c);

uint64_t weida_credit_grant_id(const weida_credit_grant *g);

const char *weida_credit_grant_filter(const weida_credit_grant *g, size_t *len_out);

uint64_t weida_credit_grant_limit(const weida_credit_grant *g);

void weida_credit_grant_drop(weida_credit_grant *g);

int weida_cursors_fd(const weida_cursors *c, int64_t *out);

int weida_cursors_recv(const weida_cursors *c, int64_t timeout_ms, weida_cursor_set **out);

void weida_cursors_drop(weida_cursors *c);

size_t weida_cursor_set_len(const weida_cursor_set *s);

int weida_cursor_set_level(const weida_cursor_set *s,
                           size_t index,
                           uint64_t *out_level,
                           uint64_t *out_offset);

int weida_cursor_set_offset(const weida_cursor_set *s, uint64_t level, uint64_t *out);

void weida_cursor_set_drop(weida_cursor_set *s);

const uint64_t *weida_reporter_levels(const weida_reporter *r, size_t *len_out);

int weida_reporter_mode(const weida_reporter *r);

int weida_reporter_set_granularity(const weida_reporter *r, uint64_t bytes, uint64_t interval_ms);

int weida_reporter_report(const weida_reporter *r,
                          uint64_t level,
                          uint64_t offset,
                          weida_reporter_report_op **out);

int weida_reporter_report_op_fd(const weida_reporter_report_op *op, int64_t *out);

int weida_reporter_report_op_wait(const weida_reporter_report_op *op, int64_t timeout_ms);

void weida_reporter_report_op_drop(weida_reporter_report_op *op);

int weida_reporter_finish(weida_reporter *r, weida_reporter_finish_op **out);

int weida_reporter_finish_op_fd(const weida_reporter_finish_op *op, int64_t *out);

int weida_reporter_finish_op_wait(const weida_reporter_finish_op *op, int64_t timeout_ms);

void weida_reporter_finish_op_drop(weida_reporter_finish_op *op);

void weida_reporter_drop(weida_reporter *r);

const char *weida_strerror(int code);

const char *weida_last_error_message(void);

int weida_last_error_loss_cause(void);

const char *weida_last_error_fingerprint(void);

int64_t weida_last_error_max(void);

int weida_last_error_os_code(void);

int weida_flow_send(const weida_flow *f, const uint8_t *payload, size_t len);

int weida_flow_max_payload(const weida_flow *f, size_t *out);

uint64_t weida_flow_sent(const weida_flow *f);

uint64_t weida_flow_too_large(const weida_flow *f);

uint64_t weida_flow_discarded(const weida_flow *f);

uint64_t weida_flow_not_live(const weida_flow *f);

int weida_flow_path_stats(const weida_flow *f, weida_path_stats **out);

void weida_flow_abort(weida_flow *f);

void weida_flow_drop(weida_flow *f);

int weida_incoming_flow_info(const weida_incoming_flow *f, weida_incoming_meta **out);

int weida_incoming_flow_fd(const weida_incoming_flow *f, int64_t *out);

int weida_incoming_flow_recv(const weida_incoming_flow *f,
                             int64_t timeout_ms,
                             weida_datagram **out);

uint64_t weida_incoming_flow_received(const weida_incoming_flow *f);

uint64_t weida_incoming_flow_overflow(const weida_incoming_flow *f);

int weida_incoming_flow_path_stats(const weida_incoming_flow *f, weida_path_stats **out);

void weida_incoming_flow_refuse(weida_incoming_flow *f);

void weida_incoming_flow_drop(weida_incoming_flow *f);

const uint8_t *weida_datagram_data(const weida_datagram *d);

size_t weida_datagram_len(const weida_datagram *d);

void weida_datagram_drop(weida_datagram *d);

int weida_identity_source_from_identity(const weida_identity *id, weida_identity_source **out);

int weida_identity_source_ephemeral(const char *const *names,
                                    size_t n,
                                    weida_identity_source **out);

int weida_identity_source_external(const weida_identity *initial, weida_identity_source **out);

int weida_identity_source_files(const char *dir,
                                const weida_files_options *options,
                                weida_identity_source **out);

int weida_identity_source_current(const weida_identity_source *src, weida_identity **out);

int weida_identity_source_fingerprint(const weida_identity_source *src, char *out, size_t len);

int weida_identity_source_update(const weida_identity_source *src, const weida_identity *id);

int weida_identity_source_report_failure(const weida_identity_source *src, const char *reason);

int weida_identity_source_report_handoff_stolen(const weida_identity_source *src);

int weida_identity_source_reload(const weida_identity_source *src);

int weida_identity_source_events(const weida_identity_source *src, weida_identity_events **out);

int weida_identity_source_downgrade(const weida_identity_source *src,
                                    weida_weak_identity_source **out);

void weida_identity_source_drop(weida_identity_source *src);

int weida_weak_identity_source_upgrade(const weida_weak_identity_source *w,
                                       weida_identity_source **out);

void weida_weak_identity_source_drop(weida_weak_identity_source *w);

int weida_trust_source_from_trust(const weida_trust *t, weida_trust_source **out);

int weida_trust_source_external(const weida_trust *initial, weida_trust_source **out);

int weida_trust_source_current(const weida_trust_source *ts, weida_trust **out);

int weida_trust_source_update(const weida_trust_source *ts, const weida_trust *t);

int weida_trust_source_report_failure(const weida_trust_source *ts, const char *reason);

int weida_trust_source_is_empty(const weida_trust_source *ts);

int weida_trust_source_events(const weida_trust_source *ts, weida_identity_events **out);

int weida_trust_source_downgrade(const weida_trust_source *ts, weida_weak_trust_source **out);

void weida_trust_source_drop(weida_trust_source *ts);

int weida_weak_trust_source_upgrade(const weida_weak_trust_source *w, weida_trust_source **out);

void weida_weak_trust_source_drop(weida_weak_trust_source *w);

int weida_identity_events_fd(const weida_identity_events *e, int64_t *out);

int weida_identity_events_recv(const weida_identity_events *e,
                               int64_t timeout_ms,
                               weida_identity_event **out);

void weida_identity_events_drop(weida_identity_events *e);

int weida_identity_event_kind(const weida_identity_event *e);

const char *weida_identity_event_fingerprint(const weida_identity_event *e, size_t *len_out);

const char *weida_identity_event_from(const weida_identity_event *e, size_t *len_out);

const char *weida_identity_event_to(const weida_identity_event *e, size_t *len_out);

const char *weida_identity_event_message(const weida_identity_event *e, size_t *len_out);

int weida_identity_event_missed(const weida_identity_event *e, uint64_t *out);

void weida_identity_event_drop(weida_identity_event *e);

int weida_runtime_listener(const weida_runtime *rt, weida_listener **out);

int weida_listener_bind_quic(const weida_listener *l,
                             const char *addr,
                             const weida_server_tls *tls,
                             weida_listener_bind_quic_op **out);

int weida_listener_bind_quic_op_fd(const weida_listener_bind_quic_op *op, int64_t *out);

int weida_listener_bind_quic_op_wait(const weida_listener_bind_quic_op *op,
                                     int64_t timeout_ms,
                                     weida_binding **out);

void weida_listener_bind_quic_op_drop(weida_listener_bind_quic_op *op);

int weida_listener_bind_quic_socket(const weida_listener *l,
                                    int64_t socket,
                                    const weida_server_tls *tls,
                                    weida_listener_bind_quic_socket_op **out);

int weida_listener_bind_quic_socket_op_fd(const weida_listener_bind_quic_socket_op *op,
                                          int64_t *out);

int weida_listener_bind_quic_socket_op_wait(const weida_listener_bind_quic_socket_op *op,
                                            int64_t timeout_ms,
                                            weida_binding **out);

void weida_listener_bind_quic_socket_op_drop(weida_listener_bind_quic_socket_op *op);

int weida_listener_bind_relay(const weida_listener *l,
                              const char *relay,
                              const weida_client_tls *via,
                              const weida_server_tls *tls,
                              weida_listener_bind_relay_op **out);

int weida_listener_bind_relay_op_fd(const weida_listener_bind_relay_op *op, int64_t *out);

int weida_listener_bind_relay_op_wait(const weida_listener_bind_relay_op *op,
                                      int64_t timeout_ms,
                                      weida_binding **out);

void weida_listener_bind_relay_op_drop(weida_listener_bind_relay_op *op);

int weida_listener_bind_inproc(const weida_listener *l, const char *bus, weida_local_binding **out);

int weida_listener_bind_unix(const weida_listener *l, const char *path, weida_unix_binding **out);

int weida_listener_bind_pipe(const weida_listener *l, const char *name, weida_pipe_binding **out);

int weida_listener_replier(const weida_listener *l, const char *path, weida_replier **out);

void weida_listener_drop(weida_listener *l);

const char *weida_binding_local_addr(const weida_binding *b, size_t *len_out);

int weida_binding_close(const weida_binding *b, weida_binding_close_op **out);

int weida_binding_close_op_fd(const weida_binding_close_op *op, int64_t *out);

int weida_binding_close_op_wait(const weida_binding_close_op *op, int64_t timeout_ms);

void weida_binding_close_op_drop(weida_binding_close_op *op);

int weida_binding_hand_over(weida_binding *b, weida_binding_hand_over_op **out);

int weida_binding_hand_over_op_fd(const weida_binding_hand_over_op *op, int64_t *out);

int weida_binding_hand_over_op_wait(const weida_binding_hand_over_op *op,
                                    int64_t timeout_ms,
                                    weida_handoff **out);

void weida_binding_hand_over_op_drop(weida_binding_hand_over_op *op);

int weida_binding_take_over(const weida_binding *b,
                            const uint8_t *held,
                            size_t len,
                            weida_binding_take_over_op **out);

int weida_binding_take_over_op_fd(const weida_binding_take_over_op *op, int64_t *out);

int weida_binding_take_over_op_wait(const weida_binding_take_over_op *op,
                                    int64_t timeout_ms,
                                    size_t *out);

void weida_binding_take_over_op_drop(weida_binding_take_over_op *op);

int weida_binding_disconnect(const weida_binding *b, const char *peer, size_t *out);

int weida_binding_relay_events(const weida_binding *b, weida_peer_events **out);

size_t weida_handoff_held(const weida_handoff *h);

const char *weida_handoff_local_addr(const weida_handoff *h, size_t *len_out);

const uint8_t *weida_handoff_bytes(const weida_handoff *h, size_t *len_out);

int weida_handoff_take_socket(const weida_handoff *h, int64_t *out);

void weida_handoff_drop(weida_handoff *h);

void weida_binding_drop(weida_binding *b);

const char *weida_local_binding_bus(const weida_local_binding *b, size_t *len_out);

void weida_local_binding_drop(weida_local_binding *b);

const char *weida_unix_binding_path(const weida_unix_binding *b, size_t *len_out);

void weida_unix_binding_drop(weida_unix_binding *b);

const char *weida_pipe_binding_name(const weida_pipe_binding *b, size_t *len_out);

void weida_pipe_binding_drop(weida_pipe_binding *b);

const char *weida_replier_path(const weida_replier *r, size_t *len_out);

int weida_replier_fd(const weida_replier *r, int64_t *out);

int weida_replier_accept(const weida_replier *r, int64_t timeout_ms, weida_incoming_request **out);

void weida_replier_drop(weida_replier *r);

int weida_incoming_request_meta(const weida_incoming_request *q, weida_incoming_meta **out);

int weida_incoming_request_connection(const weida_incoming_request *q, uint64_t *out);

int weida_incoming_request_take_body(const weida_incoming_request *q,
                                     weida_incoming_transfer **out);

int weida_incoming_request_reply(weida_incoming_request *q,
                                 const weida_transfer_meta *meta,
                                 weida_incoming_request_reply_op **out);

int weida_incoming_request_reply_op_fd(const weida_incoming_request_reply_op *op, int64_t *out);

int weida_incoming_request_reply_op_wait(const weida_incoming_request_reply_op *op,
                                         int64_t timeout_ms,
                                         weida_outgoing_transfer **out);

void weida_incoming_request_reply_op_drop(weida_incoming_request_reply_op *op);

int weida_incoming_request_refuse(weida_incoming_request *q,
                                  int code,
                                  weida_incoming_request_refuse_op **out);

int weida_incoming_request_refuse_op_fd(const weida_incoming_request_refuse_op *op, int64_t *out);

int weida_incoming_request_refuse_op_wait(const weida_incoming_request_refuse_op *op,
                                          int64_t timeout_ms);

void weida_incoming_request_refuse_op_drop(weida_incoming_request_refuse_op *op);

int weida_incoming_request_canceled(const weida_incoming_request *q,
                                    weida_incoming_request_canceled_op **out);

int weida_incoming_request_canceled_op_fd(const weida_incoming_request_canceled_op *op,
                                          int64_t *out);

int weida_incoming_request_canceled_op_wait(const weida_incoming_request_canceled_op *op,
                                            int64_t timeout_ms);

void weida_incoming_request_canceled_op_drop(weida_incoming_request_canceled_op *op);

void weida_incoming_request_drop(weida_incoming_request *q);

int weida_runtime_pair(const weida_runtime *rt, const weida_client_tls *tls, weida_paired **out);

int weida_listener_pair(const weida_listener *l, const char *path, weida_paired **out);

const char *weida_paired_path(const weida_paired *p, size_t *len_out);

int weida_paired_connect(const weida_paired *p, const char *url, weida_paired_connect_op **out);

int weida_paired_connect_op_fd(const weida_paired_connect_op *op, int64_t *out);

int weida_paired_connect_op_wait(const weida_paired_connect_op *op, int64_t timeout_ms);

void weida_paired_connect_op_drop(weida_paired_connect_op *op);

size_t weida_paired_peer_count(const weida_paired *p);

uint64_t weida_paired_dropped(const weida_paired *p);

int weida_paired_connection_stats(const weida_paired *p, weida_connection_stats **out);

int weida_paired_events(const weida_paired *p, weida_peer_events **out);

int weida_paired_open(const weida_paired *p,
                      const weida_transfer_meta *meta,
                      weida_paired_open_op **out);

int weida_paired_open_op_fd(const weida_paired_open_op *op, int64_t *out);

int weida_paired_open_op_wait(const weida_paired_open_op *op,
                              int64_t timeout_ms,
                              weida_outgoing_transfer **out);

void weida_paired_open_op_drop(weida_paired_open_op *op);

int weida_paired_send(const weida_paired *p,
                      const uint8_t *body,
                      size_t len,
                      weida_paired_send_op **out);

int weida_paired_send_with(const weida_paired *p,
                           const weida_transfer_meta *meta,
                           const uint8_t *body,
                           size_t len,
                           weida_paired_send_op **out);

int weida_paired_send_op_fd(const weida_paired_send_op *op, int64_t *out);

int weida_paired_send_op_wait(const weida_paired_send_op *op, int64_t timeout_ms);

void weida_paired_send_op_drop(weida_paired_send_op *op);

int weida_paired_fd(const weida_paired *p, int64_t *out);

int weida_paired_recv(const weida_paired *p, int64_t timeout_ms, weida_incoming_transfer **out);

void weida_paired_drop(weida_paired *p);

int weida_runtime_peer(const weida_runtime *rt, const weida_client_tls *tls, weida_peer **out);

int weida_peer_connect(const weida_peer *p, const char *url, weida_peer_connect_op **out);

int weida_peer_connect_op_fd(const weida_peer_connect_op *op, int64_t *out);

int weida_peer_connect_op_wait(const weida_peer_connect_op *op, int64_t timeout_ms);

void weida_peer_connect_op_drop(weida_peer_connect_op *op);

int weida_peer_open_flow(const weida_peer *p,
                         const weida_flow_meta *meta,
                         weida_peer_open_flow_op **out);

int weida_peer_open_flow_op_fd(const weida_peer_open_flow_op *op, int64_t *out);

int weida_peer_open_flow_op_wait(const weida_peer_open_flow_op *op,
                                 int64_t timeout_ms,
                                 weida_flow **out);

void weida_peer_open_flow_op_drop(weida_peer_open_flow_op *op);

int weida_peer_disconnect(const weida_peer *p, const char *url);

size_t weida_peer_peer_count(const weida_peer *p);

uint64_t weida_peer_dropped(const weida_peer *p);

int weida_peer_connection_stats(const weida_peer *p, weida_connection_stats **out);

int weida_peer_peer_events(const weida_peer *p, weida_peer_events **out);

int weida_peer_open(const weida_peer *p, const weida_transfer_meta *meta, weida_peer_open_op **out);

int weida_peer_open_op_fd(const weida_peer_open_op *op, int64_t *out);

int weida_peer_open_op_wait(const weida_peer_open_op *op,
                            int64_t timeout_ms,
                            weida_outgoing_transfer **out);

void weida_peer_open_op_drop(weida_peer_open_op *op);

int weida_peer_open_bi(const weida_peer *p,
                       const weida_transfer_meta *meta,
                       weida_peer_open_bi_op **out);

int weida_peer_open_bi_op_fd(const weida_peer_open_bi_op *op, int64_t *out);

int weida_peer_open_bi_op_wait(const weida_peer_open_bi_op *op,
                               int64_t timeout_ms,
                               weida_outgoing_transfer **out_body,
                               weida_reply_stream **out_reply);

void weida_peer_open_bi_op_drop(weida_peer_open_bi_op *op);

int weida_peer_send(const weida_peer *p,
                    const weida_transfer_meta *meta,
                    const uint8_t *body,
                    size_t len,
                    weida_peer_send_op **out);

int weida_peer_send_op_fd(const weida_peer_send_op *op, int64_t *out);

int weida_peer_send_op_wait(const weida_peer_send_op *op, int64_t timeout_ms);

void weida_peer_send_op_drop(weida_peer_send_op *op);

int weida_peer_segment(const weida_peer *p,
                       const char *topic,
                       const weida_segment_terms *terms,
                       weida_peer_segment_op **out);

int weida_peer_segment_op_fd(const weida_peer_segment_op *op, int64_t *out);

int weida_peer_segment_op_wait(const weida_peer_segment_op *op,
                               int64_t timeout_ms,
                               weida_segment **out);

void weida_peer_segment_op_drop(weida_peer_segment_op *op);

int weida_peer_segment_drops(const weida_peer *p, const char *topic, weida_topic_drops **out);

void weida_peer_drop(weida_peer *p);

int weida_listener_acceptor(const weida_listener *l, const char *path, weida_acceptor **out);

const char *weida_acceptor_path(const weida_acceptor *a, size_t *len_out);

int weida_acceptor_fd(const weida_acceptor *a, int64_t *out);

int weida_acceptor_accept(const weida_acceptor *a, int64_t timeout_ms, weida_incoming **out);

void weida_acceptor_drop(weida_acceptor *a);

int weida_incoming_kind(const weida_incoming *i);

int weida_incoming_take_stream(const weida_incoming *i, weida_incoming_transfer **out);

int weida_incoming_take_exchange(const weida_incoming *i, weida_incoming_request **out);

int weida_incoming_take_subscribed(const weida_incoming *i, weida_consumer **out);

int weida_incoming_take_credit(const weida_incoming *i, weida_credit_grant **out);

int weida_incoming_take_flow(const weida_incoming *i, weida_incoming_flow **out);

int weida_incoming_unsubscribed_id(const weida_incoming *i, uint64_t *out);

const char *weida_incoming_unsubscribed_filter(const weida_incoming *i, size_t *len_out);

void weida_incoming_drop(weida_incoming *i);

int weida_poll(weida_poll_item *items, size_t n, int64_t timeout_ms);

int weida_listener_publisher(const weida_listener *l, const char *path, weida_publisher **out);

const char *weida_publisher_path(const weida_publisher *p, size_t *len_out);

int weida_publisher_publish(const weida_publisher *p,
                            const char *topic,
                            const uint8_t *data,
                            size_t len,
                            size_t *out);

int weida_publisher_publish_with_trace(const weida_publisher *p,
                                       const char *topic,
                                       const uint8_t *data,
                                       size_t len,
                                       const char *traceparent,
                                       size_t *out);

int weida_publisher_open(const weida_publisher *p, const char *topic, weida_fan_out **out);

int weida_publisher_open_with_trace(const weida_publisher *p,
                                    const char *topic,
                                    const char *traceparent,
                                    weida_fan_out **out);

size_t weida_publisher_subscriber_count(const weida_publisher *p);

size_t weida_publisher_filter_count(const weida_publisher *p);

uint64_t weida_publisher_dropped(const weida_publisher *p);

int weida_publisher_dropped_on(const weida_publisher *p,
                               const char *topic,
                               weida_topic_drops **out);

int weida_publisher_drops(const weida_publisher *p, weida_topic_drops **out);

void weida_publisher_drop(weida_publisher *p);

const char *weida_fan_out_topic(const weida_fan_out *f, size_t *len_out);

int weida_fan_out_subscribers(const weida_fan_out *f, size_t *out);

int weida_fan_out_write_within(const weida_fan_out *f,
                               const uint8_t *chunk,
                               size_t len,
                               uint64_t limit_ms,
                               weida_fan_out_write_within_op **out);

int weida_fan_out_write_within_op_fd(const weida_fan_out_write_within_op *op, int64_t *out);

int weida_fan_out_write_within_op_wait(const weida_fan_out_write_within_op *op,
                                       int64_t timeout_ms,
                                       size_t *out);

void weida_fan_out_write_within_op_drop(weida_fan_out_write_within_op *op);

int weida_fan_out_write_now(const weida_fan_out *f, const uint8_t *chunk, size_t len, size_t *out);

int weida_fan_out_finish(weida_fan_out *f, size_t *out);

void weida_fan_out_drop(weida_fan_out *f);

size_t weida_topic_drops_len(const weida_topic_drops *d);

const char *weida_topic_drops_topic(const weida_topic_drops *d, size_t index, size_t *len_out);

int weida_topic_drops_subscriber_budget(const weida_topic_drops *d, size_t index, uint64_t *out);

int weida_topic_drops_subscriber_queue(const weida_topic_drops *d, size_t index, uint64_t *out);

int weida_topic_drops_no_parked_connection(const weida_topic_drops *d, size_t index, uint64_t *out);

int weida_topic_drops_superseded(const weida_topic_drops *d, size_t index, uint64_t *out);

int weida_topic_drops_expired(const weida_topic_drops *d, size_t index, uint64_t *out);

int weida_topic_drops_too_large(const weida_topic_drops *d, size_t index, uint64_t *out);

int weida_topic_drops_no_datagrams(const weida_topic_drops *d, size_t index, uint64_t *out);

int weida_topic_drops_layers_cut(const weida_topic_drops *d, size_t index, uint64_t *out);

void weida_topic_drops_drop(weida_topic_drops *d);

int weida_runtime_subscriber(const weida_runtime *rt,
                             const weida_client_tls *tls,
                             weida_subscriber **out);

int weida_subscriber_connect(const weida_subscriber *s,
                             const char *url,
                             weida_subscriber_connect_op **out);

int weida_subscriber_connect_op_fd(const weida_subscriber_connect_op *op, int64_t *out);

int weida_subscriber_connect_op_wait(const weida_subscriber_connect_op *op, int64_t timeout_ms);

void weida_subscriber_connect_op_drop(weida_subscriber_connect_op *op);

int weida_subscriber_disconnect(const weida_subscriber *s, const char *url);

size_t weida_subscriber_peer_count(const weida_subscriber *s);

int weida_subscriber_connection_stats(const weida_subscriber *s, weida_connection_stats **out);

int weida_subscriber_events(const weida_subscriber *s, weida_peer_events **out);

int weida_subscriber_subscribe(const weida_subscriber *s,
                               const char *filter,
                               weida_subscriber_subscribe_op **out);

int weida_subscriber_subscribe_op_fd(const weida_subscriber_subscribe_op *op, int64_t *out);

int weida_subscriber_subscribe_op_wait(const weida_subscriber_subscribe_op *op, int64_t timeout_ms);

void weida_subscriber_subscribe_op_drop(weida_subscriber_subscribe_op *op);

int weida_subscriber_unsubscribe(const weida_subscriber *s,
                                 const char *filter,
                                 weida_subscriber_unsubscribe_op **out);

int weida_subscriber_unsubscribe_op_fd(const weida_subscriber_unsubscribe_op *op, int64_t *out);

int weida_subscriber_unsubscribe_op_wait(const weida_subscriber_unsubscribe_op *op,
                                         int64_t timeout_ms);

void weida_subscriber_unsubscribe_op_drop(weida_subscriber_unsubscribe_op *op);

int weida_subscriber_grant(const weida_subscriber *s,
                           const char *filter,
                           uint64_t limit,
                           weida_subscriber_grant_op **out);

int weida_subscriber_grant_op_fd(const weida_subscriber_grant_op *op, int64_t *out);

int weida_subscriber_grant_op_wait(const weida_subscriber_grant_op *op, int64_t timeout_ms);

void weida_subscriber_grant_op_drop(weida_subscriber_grant_op *op);

size_t weida_subscriber_filter_count(const weida_subscriber *s);

int weida_subscriber_fd(const weida_subscriber *s, int64_t *out);

int weida_subscriber_recv(const weida_subscriber *s,
                          int64_t timeout_ms,
                          weida_incoming_transfer **out);

void weida_subscriber_drop(weida_subscriber *s);

int weida_runtime_pusher(const weida_runtime *rt, const weida_client_tls *tls, weida_pusher **out);

int weida_pusher_connect(const weida_pusher *p, const char *url, weida_pusher_connect_op **out);

int weida_pusher_connect_op_fd(const weida_pusher_connect_op *op, int64_t *out);

int weida_pusher_connect_op_wait(const weida_pusher_connect_op *op, int64_t timeout_ms);

void weida_pusher_connect_op_drop(weida_pusher_connect_op *op);

int weida_pusher_disconnect(const weida_pusher *p, const char *url);

size_t weida_pusher_peer_count(const weida_pusher *p);

uint64_t weida_pusher_dropped(const weida_pusher *p);

int weida_pusher_connection_stats(const weida_pusher *p, weida_connection_stats **out);

int weida_pusher_events(const weida_pusher *p, weida_peer_events **out);

int weida_pusher_open(const weida_pusher *p,
                      const weida_transfer_meta *meta,
                      weida_pusher_open_op **out);

int weida_pusher_open_op_fd(const weida_pusher_open_op *op, int64_t *out);

int weida_pusher_open_op_wait(const weida_pusher_open_op *op,
                              int64_t timeout_ms,
                              weida_outgoing_transfer **out);

void weida_pusher_open_op_drop(weida_pusher_open_op *op);

int weida_pusher_send(const weida_pusher *p,
                      const uint8_t *body,
                      size_t len,
                      weida_pusher_send_op **out);

int weida_pusher_send_with(const weida_pusher *p,
                           const weida_transfer_meta *meta,
                           const uint8_t *body,
                           size_t len,
                           weida_pusher_send_op **out);

int weida_pusher_send_op_fd(const weida_pusher_send_op *op, int64_t *out);

int weida_pusher_send_op_wait(const weida_pusher_send_op *op, int64_t timeout_ms);

void weida_pusher_send_op_drop(weida_pusher_send_op *op);

void weida_pusher_drop(weida_pusher *p);

int weida_listener_puller(const weida_listener *l, const char *path, weida_puller **out);

const char *weida_puller_path(const weida_puller *p, size_t *len_out);

int weida_puller_fd(const weida_puller *p, int64_t *out);

int weida_puller_recv(const weida_puller *p, int64_t timeout_ms, weida_incoming_transfer **out);

void weida_puller_drop(weida_puller *p);

int weida_listener_radio(const weida_listener *l, const char *path, weida_radio **out);

const char *weida_radio_path(const weida_radio *r, size_t *len_out);

int weida_radio_segment(const weida_radio *r,
                        const char *topic,
                        const weida_segment_terms *terms,
                        weida_segment **out);

int weida_radio_datagram(const weida_radio *r,
                         const char *topic,
                         const uint8_t *payload,
                         size_t len,
                         size_t *out);

size_t weida_radio_dish_count(const weida_radio *r);

uint64_t weida_radio_dropped(const weida_radio *r);

int weida_radio_dropped_on(const weida_radio *r, const char *topic, weida_topic_drops **out);

int weida_radio_drops(const weida_radio *r, weida_topic_drops **out);

int weida_radio_evict(const weida_radio *r, const char *peer, const char *filter, size_t *out);

int weida_radio_dish_drops(const weida_radio *r, weida_dish_drops **out);

void weida_radio_drop(weida_radio *r);

size_t weida_dish_drops_len(const weida_dish_drops *d);

const char *weida_dish_drops_peer_fingerprint(const weida_dish_drops *d,
                                              size_t index,
                                              size_t *len_out);

int weida_dish_drops_subscriber_budget(const weida_dish_drops *d, size_t index, uint64_t *out);

int weida_dish_drops_subscriber_queue(const weida_dish_drops *d, size_t index, uint64_t *out);

int weida_dish_drops_superseded(const weida_dish_drops *d, size_t index, uint64_t *out);

int weida_dish_drops_expired(const weida_dish_drops *d, size_t index, uint64_t *out);

int weida_dish_drops_too_large(const weida_dish_drops *d, size_t index, uint64_t *out);

int weida_dish_drops_no_datagrams(const weida_dish_drops *d, size_t index, uint64_t *out);

int weida_dish_drops_layers_cut(const weida_dish_drops *d, size_t index, uint64_t *out);

void weida_dish_drops_drop(weida_dish_drops *d);

int weida_runtime_dish(const weida_runtime *rt, const weida_client_tls *tls, weida_dish **out);

int weida_dish_connect(const weida_dish *d, const char *url, weida_dish_connect_op **out);

int weida_dish_connect_op_fd(const weida_dish_connect_op *op, int64_t *out);

int weida_dish_connect_op_wait(const weida_dish_connect_op *op, int64_t timeout_ms);

void weida_dish_connect_op_drop(weida_dish_connect_op *op);

size_t weida_dish_peer_count(const weida_dish *d);

int weida_dish_connection_stats(const weida_dish *d, weida_connection_stats **out);

int weida_dish_events(const weida_dish *d, weida_peer_events **out);

int weida_dish_join(const weida_dish *d,
                    const char *filter,
                    const weida_join_terms *terms,
                    weida_dish_join_op **out);

int weida_dish_join_op_fd(const weida_dish_join_op *op, int64_t *out);

int weida_dish_join_op_wait(const weida_dish_join_op *op, int64_t timeout_ms);

void weida_dish_join_op_drop(weida_dish_join_op *op);

int weida_dish_leave(const weida_dish *d, const char *filter, weida_dish_leave_op **out);

int weida_dish_leave_op_fd(const weida_dish_leave_op *op, int64_t *out);

int weida_dish_leave_op_wait(const weida_dish_leave_op *op, int64_t timeout_ms);

void weida_dish_leave_op_drop(weida_dish_leave_op *op);

int weida_dish_fd(const weida_dish *d, int64_t *out);

int weida_dish_recv(const weida_dish *d, int64_t timeout_ms, weida_received **out);

uint64_t weida_dish_stale(const weida_dish *d);

uint64_t weida_dish_overflow(const weida_dish *d);

void weida_dish_drop(weida_dish *d);

int weida_received_kind(const weida_received *r);

int weida_received_take_segment(const weida_received *r, weida_incoming_transfer **out);

const char *weida_received_topic(const weida_received *r, size_t *len_out);

int weida_received_segment(const weida_received *r, uint64_t *out);

const uint8_t *weida_received_payload(const weida_received *r, size_t *len_out);

void weida_received_drop(weida_received *r);

int weida_listener_relay(const weida_listener *l, weida_relay **out);

int weida_relay_fd(const weida_relay *r, int64_t *out);

int weida_relay_accept(const weida_relay *r, int64_t timeout_ms, weida_relay_request **out);

void weida_relay_drop(weida_relay *r);

const char *weida_relay_request_peer_fingerprint(const weida_relay_request *q, size_t *len_out);

const char *weida_relay_request_target(const weida_relay_request *q, size_t *len_out);

int weida_relay_request_admit(weida_relay_request *q, weida_relay_request_admit_op **out);

int weida_relay_request_admit_op_fd(const weida_relay_request_admit_op *op, int64_t *out);

int weida_relay_request_admit_op_wait(const weida_relay_request_admit_op *op, int64_t timeout_ms);

void weida_relay_request_admit_op_drop(weida_relay_request_admit_op *op);

int weida_relay_request_refuse(weida_relay_request *q, weida_relay_request_refuse_op **out);

int weida_relay_request_refuse_op_fd(const weida_relay_request_refuse_op *op, int64_t *out);

int weida_relay_request_refuse_op_wait(const weida_relay_request_refuse_op *op, int64_t timeout_ms);

void weida_relay_request_refuse_op_drop(weida_relay_request_refuse_op *op);

void weida_relay_request_drop(weida_relay_request *q);

int weida_runtime_requester(const weida_runtime *rt,
                            const weida_client_tls *tls,
                            weida_requester **out);

int weida_requester_connect(const weida_requester *r,
                            const char *url,
                            weida_requester_connect_op **out);

int weida_requester_connect_op_fd(const weida_requester_connect_op *op, int64_t *out);

int weida_requester_connect_op_wait(const weida_requester_connect_op *op, int64_t timeout_ms);

void weida_requester_connect_op_drop(weida_requester_connect_op *op);

int weida_requester_request(const weida_requester *r,
                            const uint8_t *body,
                            size_t len,
                            weida_requester_request_op **out);

int weida_requester_request_with(const weida_requester *r,
                                 const weida_transfer_meta *meta,
                                 const uint8_t *body,
                                 size_t len,
                                 weida_requester_request_op **out);

int weida_requester_request_op_fd(const weida_requester_request_op *op, int64_t *out);

int weida_requester_request_op_wait(const weida_requester_request_op *op,
                                    int64_t timeout_ms,
                                    weida_incoming_transfer **out);

void weida_requester_request_op_drop(weida_requester_request_op *op);

int weida_requester_open(const weida_requester *r,
                         const weida_transfer_meta *meta,
                         weida_requester_open_op **out);

int weida_requester_open_op_fd(const weida_requester_open_op *op, int64_t *out);

int weida_requester_open_op_wait(const weida_requester_open_op *op,
                                 int64_t timeout_ms,
                                 weida_outgoing_transfer **out_body,
                                 weida_reply_stream **out_reply);

void weida_requester_open_op_drop(weida_requester_open_op *op);

int weida_reply_stream_recv(weida_reply_stream *s, weida_reply_stream_recv_op **out);

int weida_reply_stream_recv_op_fd(const weida_reply_stream_recv_op *op, int64_t *out);

int weida_reply_stream_recv_op_wait(const weida_reply_stream_recv_op *op,
                                    int64_t timeout_ms,
                                    weida_incoming_transfer **out);

void weida_reply_stream_recv_op_drop(weida_reply_stream_recv_op *op);

void weida_reply_stream_drop(weida_reply_stream *s);

int weida_requester_connection_stats(const weida_requester *r, weida_connection_stats **out);

int weida_requester_events(const weida_requester *r, weida_peer_events **out);

void weida_requester_drop(weida_requester *r);

int weida_resolver_new(weida_resolver **out);

int weida_resolver_fd(const weida_resolver *r, int64_t *out);

int weida_resolver_recv(const weida_resolver *r, int64_t timeout_ms, weida_resolved **out);

void weida_resolver_drop(weida_resolver *r);

const char *weida_resolved_name(const weida_resolved *q, size_t *len_out);

int weida_resolved_port(const weida_resolved *q, uint16_t *out);

uint64_t weida_resolved_max_addresses(const weida_resolved *q);

int weida_resolved_answer(weida_resolved *q, const char *const *addrs, size_t n);

int weida_resolved_refuse(weida_resolved *q, const char *reason);

void weida_resolved_drop(weida_resolved *q);

int weida_runtime_config_new(weida_runtime_config **out);

int weida_runtime_config_set_worker_threads(const weida_runtime_config *cfg, uint32_t threads);

int weida_runtime_config_set_shutdown_timeout_ms(const weida_runtime_config *cfg, uint64_t ms);

int weida_runtime_config_set_max_connections(const weida_runtime_config *cfg, uint64_t v);

int weida_runtime_config_set_max_connections_per_peer(const weida_runtime_config *cfg, uint64_t v);

int weida_runtime_config_set_max_resolved_addresses(const weida_runtime_config *cfg, uint64_t v);

int weida_runtime_config_set_outbox_messages(const weida_runtime_config *cfg, uint64_t v);

int weida_runtime_config_set_outbox_bytes(const weida_runtime_config *cfg, uint64_t v);

int weida_runtime_config_set_max_handshakes(const weida_runtime_config *cfg, uint64_t v);

int weida_runtime_config_set_retry_above_handshakes(const weida_runtime_config *cfg, uint64_t v);

int weida_runtime_config_set_endpoint_queue(const weida_runtime_config *cfg, uint64_t v);

int weida_runtime_config_set_connect_attempt_timeout_ms(const weida_runtime_config *cfg,
                                                        uint64_t v);

int weida_runtime_config_set_send_timeout_ms(const weida_runtime_config *cfg, int64_t v);

int weida_runtime_config_set_outbox_full(const weida_runtime_config *cfg, int v);

int weida_runtime_config_set_discovery(const weida_runtime_config *cfg, int v);

int weida_runtime_config_set_limits(const weida_runtime_config *cfg, const weida_limits *v);

int weida_runtime_config_set_guarantees(const weida_runtime_config *cfg,
                                        const weida_guarantee_set *v);

int weida_runtime_config_set_reconnect(const weida_runtime_config *cfg,
                                       const weida_reconnect_policy *v);

int weida_runtime_config_set_resolver(const weida_runtime_config *cfg, const weida_resolver *r);

void weida_runtime_config_drop(weida_runtime_config *cfg);

int weida_runtime_owned(const weida_runtime_config *cfg, weida_runtime **out);

int weida_runtime_shutdown(weida_runtime *rt, weida_runtime_shutdown_op **out);

int weida_runtime_shutdown_op_fd(const weida_runtime_shutdown_op *op, int64_t *out);

int weida_runtime_shutdown_op_wait(const weida_runtime_shutdown_op *op, int64_t timeout_ms);

void weida_runtime_shutdown_op_drop(weida_runtime_shutdown_op *op);

int weida_runtime_drain(weida_runtime *rt, uint64_t deadline_ms, weida_runtime_drain_op **out);

int weida_runtime_drain_op_fd(const weida_runtime_drain_op *op, int64_t *out);

int weida_runtime_drain_op_wait(const weida_runtime_drain_op *op,
                                int64_t timeout_ms,
                                weida_drained **out);

void weida_runtime_drain_op_drop(weida_runtime_drain_op *op);

uint64_t weida_drained_delivered(const weida_drained *d);

uint64_t weida_drained_outstanding(const weida_drained *d);

void weida_drained_drop(weida_drained *d);

uint64_t weida_runtime_suppressed_duplicates(const weida_runtime *rt);

uint64_t weida_runtime_expired_transfers(const weida_runtime *rt);

void weida_runtime_drop(weida_runtime *rt);

uint64_t weida_segment_number(const weida_segment *s);

const char *weida_segment_topic(const weida_segment *s, size_t *len_out);

int weida_segment_write(const weida_segment *s, const uint8_t *chunk, size_t len, size_t *out);

int weida_segment_write_layer(const weida_segment *s,
                              uint8_t layer,
                              const uint8_t *chunk,
                              size_t len,
                              size_t *out);

int weida_segment_finish_layer(const weida_segment *s, uint8_t layer);

int weida_segment_finish(weida_segment *s, size_t *out);

void weida_segment_drop(weida_segment *s);

size_t weida_connection_stats_len(const weida_connection_stats *s);

const char *weida_connection_stats_url(const weida_connection_stats *s,
                                       size_t index,
                                       size_t *len_out);

int weida_connection_stats_age_us(const weida_connection_stats *s, size_t index, uint64_t *out);

int weida_connection_stats_redials(const weida_connection_stats *s, size_t index, uint64_t *out);

int weida_connection_stats_transport_path_rtt_us(const weida_connection_stats *s,
                                                 size_t index,
                                                 uint64_t *out);

int weida_connection_stats_transport_path_min_rtt_us(const weida_connection_stats *s,
                                                     size_t index,
                                                     uint64_t *out);

int weida_connection_stats_transport_path_cwnd(const weida_connection_stats *s,
                                               size_t index,
                                               uint64_t *out);

int weida_connection_stats_transport_path_congestion_events(const weida_connection_stats *s,
                                                            size_t index,
                                                            uint64_t *out);

int weida_connection_stats_transport_path_lost_packets(const weida_connection_stats *s,
                                                       size_t index,
                                                       uint64_t *out);

int weida_connection_stats_transport_path_lost_bytes(const weida_connection_stats *s,
                                                     size_t index,
                                                     uint64_t *out);

int weida_connection_stats_transport_path_sent_packets(const weida_connection_stats *s,
                                                       size_t index,
                                                       uint64_t *out);

int weida_connection_stats_transport_path_current_mtu(const weida_connection_stats *s,
                                                      size_t index,
                                                      uint64_t *out);

int weida_connection_stats_transport_path_max_datagram_size(const weida_connection_stats *s,
                                                            size_t index,
                                                            uint64_t *out);

int weida_connection_stats_transport_tx_datagrams(const weida_connection_stats *s,
                                                  size_t index,
                                                  uint64_t *out);

int weida_connection_stats_transport_tx_bytes(const weida_connection_stats *s,
                                              size_t index,
                                              uint64_t *out);

int weida_connection_stats_transport_rx_datagrams(const weida_connection_stats *s,
                                                  size_t index,
                                                  uint64_t *out);

int weida_connection_stats_transport_rx_bytes(const weida_connection_stats *s,
                                              size_t index,
                                              uint64_t *out);

int weida_connection_stats_remote_rtt_us(const weida_connection_stats *s,
                                         size_t index,
                                         uint64_t *out);

int weida_connection_stats_remote_min_rtt_us(const weida_connection_stats *s,
                                             size_t index,
                                             uint64_t *out);

int weida_connection_stats_remote_cwnd(const weida_connection_stats *s,
                                       size_t index,
                                       uint64_t *out);

int weida_connection_stats_remote_congestion_events(const weida_connection_stats *s,
                                                    size_t index,
                                                    uint64_t *out);

int weida_connection_stats_remote_lost_packets(const weida_connection_stats *s,
                                               size_t index,
                                               uint64_t *out);

int weida_connection_stats_remote_lost_bytes(const weida_connection_stats *s,
                                             size_t index,
                                             uint64_t *out);

int weida_connection_stats_remote_sent_packets(const weida_connection_stats *s,
                                               size_t index,
                                               uint64_t *out);

int weida_connection_stats_remote_current_mtu(const weida_connection_stats *s,
                                              size_t index,
                                              uint64_t *out);

int weida_connection_stats_remote_tx_datagrams(const weida_connection_stats *s,
                                               size_t index,
                                               uint64_t *out);

int weida_connection_stats_remote_tx_bytes(const weida_connection_stats *s,
                                           size_t index,
                                           uint64_t *out);

int weida_connection_stats_remote_rx_datagrams(const weida_connection_stats *s,
                                               size_t index,
                                               uint64_t *out);

int weida_connection_stats_remote_rx_bytes(const weida_connection_stats *s,
                                           size_t index,
                                           uint64_t *out);

int weida_connection_stats_remote_age_us(const weida_connection_stats *s,
                                         size_t index,
                                         uint64_t *out);

void weida_connection_stats_drop(weida_connection_stats *s);

uint64_t weida_path_stats_rtt_us(const weida_path_stats *s);

uint64_t weida_path_stats_min_rtt_us(const weida_path_stats *s);

uint64_t weida_path_stats_cwnd(const weida_path_stats *s);

uint64_t weida_path_stats_congestion_events(const weida_path_stats *s);

uint64_t weida_path_stats_lost_packets(const weida_path_stats *s);

uint64_t weida_path_stats_lost_bytes(const weida_path_stats *s);

uint64_t weida_path_stats_sent_packets(const weida_path_stats *s);

uint64_t weida_path_stats_current_mtu(const weida_path_stats *s);

int weida_path_stats_max_datagram_size(const weida_path_stats *s, uint64_t *out);

void weida_path_stats_drop(weida_path_stats *s);

int weida_peer_events_fd(const weida_peer_events *e, int64_t *out);

int weida_peer_events_recv(const weida_peer_events *e, int64_t timeout_ms, weida_peer_event **out);

void weida_peer_events_drop(weida_peer_events *e);

int weida_peer_event_kind(const weida_peer_event *e);

const char *weida_peer_event_url(const weida_peer_event *e, size_t *len_out);

const char *weida_peer_event_peer_fingerprint(const weida_peer_event *e, size_t *len_out);

int weida_peer_event_cause(const weida_peer_event *e, int *out);

int weida_peer_event_attempt(const weida_peer_event *e, uint32_t *out);

int weida_peer_event_delay_us(const weida_peer_event *e, uint64_t *out);

int weida_peer_event_why(const weida_peer_event *e, int *out);

int weida_peer_event_why_attempts(const weida_peer_event *e, uint32_t *out);

const char *weida_peer_event_why_presented(const weida_peer_event *e, size_t *len_out);

const char *weida_peer_event_why_message(const weida_peer_event *e, size_t *len_out);

int weida_peer_event_missed(const weida_peer_event *e, uint64_t *out);

void weida_peer_event_drop(weida_peer_event *e);

int weida_runtime_surveyor(const weida_runtime *rt,
                           const weida_client_tls *tls,
                           weida_surveyor **out);

int weida_surveyor_connect(const weida_surveyor *s,
                           const char *url,
                           weida_surveyor_connect_op **out);

int weida_surveyor_connect_op_fd(const weida_surveyor_connect_op *op, int64_t *out);

int weida_surveyor_connect_op_wait(const weida_surveyor_connect_op *op, int64_t timeout_ms);

void weida_surveyor_connect_op_drop(weida_surveyor_connect_op *op);

size_t weida_surveyor_peer_count(const weida_surveyor *s);

int weida_surveyor_connection_stats(const weida_surveyor *s, weida_connection_stats **out);

int weida_surveyor_survey(const weida_surveyor *s,
                          const uint8_t *body,
                          size_t len,
                          uint64_t deadline_ms,
                          weida_surveyor_survey_op **out);

int weida_surveyor_survey_with(const weida_surveyor *s,
                               const weida_transfer_meta *meta,
                               const uint8_t *body,
                               size_t len,
                               uint64_t deadline_ms,
                               weida_surveyor_survey_op **out);

int weida_surveyor_survey_op_fd(const weida_surveyor_survey_op *op, int64_t *out);

int weida_surveyor_survey_op_wait(const weida_surveyor_survey_op *op,
                                  int64_t timeout_ms,
                                  weida_survey_run **out);

void weida_surveyor_survey_op_drop(weida_surveyor_survey_op *op);

void weida_surveyor_drop(weida_surveyor *s);

int weida_survey_run_fd(const weida_survey_run *r, int64_t *out);

int weida_survey_run_next(const weida_survey_run *r,
                          uint64_t max_bytes,
                          int64_t timeout_ms,
                          weida_survey_answer **out);

uint64_t weida_survey_run_late(const weida_survey_run *r);

size_t weida_survey_run_respondents(const weida_survey_run *r);

void weida_survey_run_drop(weida_survey_run *r);

const uint8_t *weida_survey_answer_data(const weida_survey_answer *a);

size_t weida_survey_answer_len(const weida_survey_answer *a);

void weida_survey_answer_drop(weida_survey_answer *a);

int weida_listener_respondent(const weida_listener *l, const char *path, weida_respondent **out);

const char *weida_respondent_path(const weida_respondent *r, size_t *len_out);

int weida_respondent_fd(const weida_respondent *r, int64_t *out);

int weida_respondent_accept(const weida_respondent *r,
                            int64_t timeout_ms,
                            weida_incoming_request **out);

void weida_respondent_drop(weida_respondent *r);

int weida_runtime_swarm(const weida_runtime *rt, const weida_client_tls *tls, weida_swarm **out);

int weida_swarm_connect(const weida_swarm *s, const char *url, weida_swarm_connect_op **out);

int weida_swarm_connect_op_fd(const weida_swarm_connect_op *op, int64_t *out);

int weida_swarm_connect_op_wait(const weida_swarm_connect_op *op, int64_t timeout_ms);

void weida_swarm_connect_op_drop(weida_swarm_connect_op *op);

int weida_swarm_disconnect(const weida_swarm *s, const char *url);

size_t weida_swarm_peer_count(const weida_swarm *s);

int weida_swarm_connection_stats(const weida_swarm *s, weida_connection_stats **out);

int weida_swarm_events(const weida_swarm *s, weida_peer_events **out);

int weida_swarm_fetch(const weida_swarm *s,
                      const weida_blob *blob,
                      const weida_bits *have,
                      const weida_fetch_options *options,
                      weida_fetch **out);

void weida_swarm_drop(weida_swarm *s);

int weida_fetch_fd(const weida_fetch *f, int64_t *out);

int weida_fetch_next(const weida_fetch *f, int64_t timeout_ms, weida_piece **out);

int weida_fetch_blob(const weida_fetch *f, weida_blob **out);

int weida_fetch_list(const weida_fetch *f, weida_piece_list **out);

int weida_fetch_delivered(const weida_fetch *f, weida_bits **out);

int weida_fetch_in_flight(const weida_fetch *f, weida_bits **out);

void weida_fetch_drop(weida_fetch *f);

uint32_t weida_piece_index(const weida_piece *p);

const uint8_t *weida_piece_bytes(const weida_piece *p, size_t *len_out);

void weida_piece_drop(weida_piece *p);

int weida_listener_seeder(const weida_listener *l, const char *path, weida_seeder **out);

const char *weida_seeder_path(const weida_seeder *s, size_t *len_out);

int weida_seeder_fd(const weida_seeder *s, int64_t *out);

int weida_seeder_accept(const weida_seeder *s, int64_t timeout_ms, weida_seed_session **out);

void weida_seeder_drop(weida_seeder *s);

int weida_seed_session_blob(const weida_seed_session *s, weida_blob **out);

int weida_seed_session_meta(const weida_seed_session *s, weida_incoming_meta **out);

uint64_t weida_seed_session_connection(const weida_seed_session *s);

int weida_seed_session_fd(const weida_seed_session *s, int64_t *out);

int weida_seed_session_next(const weida_seed_session *s,
                            int64_t timeout_ms,
                            int *out_ask,
                            uint32_t *out_index);

int weida_seed_session_answer_list(const weida_seed_session *s,
                                   const weida_piece_list *list,
                                   weida_seed_session_answer_list_op **out);

int weida_seed_session_answer_list_op_fd(const weida_seed_session_answer_list_op *op, int64_t *out);

int weida_seed_session_answer_list_op_wait(const weida_seed_session_answer_list_op *op,
                                           int64_t timeout_ms);

void weida_seed_session_answer_list_op_drop(weida_seed_session_answer_list_op *op);

int weida_seed_session_answer_have(const weida_seed_session *s,
                                   const weida_bits *have,
                                   weida_seed_session_answer_have_op **out);

int weida_seed_session_answer_have_op_fd(const weida_seed_session_answer_have_op *op, int64_t *out);

int weida_seed_session_answer_have_op_wait(const weida_seed_session_answer_have_op *op,
                                           int64_t timeout_ms);

void weida_seed_session_answer_have_op_drop(weida_seed_session_answer_have_op *op);

int weida_seed_session_answer_piece(const weida_seed_session *s,
                                    const uint8_t *data,
                                    size_t len,
                                    weida_seed_session_answer_piece_op **out);

int weida_seed_session_answer_piece_op_fd(const weida_seed_session_answer_piece_op *op,
                                          int64_t *out);

int weida_seed_session_answer_piece_op_wait(const weida_seed_session_answer_piece_op *op,
                                            int64_t timeout_ms);

void weida_seed_session_answer_piece_op_drop(weida_seed_session_answer_piece_op *op);

int weida_seed_session_decline(weida_seed_session *s, weida_seed_session_decline_op **out);

int weida_seed_session_decline_op_fd(const weida_seed_session_decline_op *op, int64_t *out);

int weida_seed_session_decline_op_wait(const weida_seed_session_decline_op *op, int64_t timeout_ms);

void weida_seed_session_decline_op_drop(weida_seed_session_decline_op *op);

void weida_seed_session_drop(weida_seed_session *s);

int weida_trust_new(weida_trust **out);

int weida_trust_add_pin(const weida_trust *t, const char *fingerprint);

int weida_trust_add_anchor(const weida_trust *t, const uint8_t *pem, size_t len);

int weida_trust_add_anchor_file(const weida_trust *t, const char *path);

int weida_trust_is_empty(const weida_trust *t);

void weida_trust_drop(weida_trust *t);

int weida_identity_generate(weida_identity **out);

int weida_identity_generate_for(const char *const *names, size_t n, weida_identity **out);

int weida_identity_from_pem(const uint8_t *cert,
                            size_t cert_len,
                            const uint8_t *key,
                            size_t key_len,
                            weida_identity **out);

int weida_identity_from_pem_files(const char *cert_chain, const char *key, weida_identity **out);

int weida_identity_from_pem_file(const char *path, weida_identity **out);

int weida_identity_fingerprint(const weida_identity *id, const char **out);

int weida_identity_certificate_pem(const weida_identity *id, weida_pem **out);

int weida_identity_to_pem(const weida_identity *id, weida_pem **out);

void weida_identity_drop(weida_identity *id);

const uint8_t *weida_pem_data(const weida_pem *p);

size_t weida_pem_len(const weida_pem *p);

void weida_pem_drop(weida_pem *p);

int weida_client_tls_new(const weida_trust *trust, weida_client_tls **out);

int weida_client_tls_new_source(const weida_trust_source *trust, weida_client_tls **out);

int weida_client_tls_set_identity(const weida_client_tls *tls, const weida_identity *identity);

int weida_client_tls_set_identity_source(const weida_client_tls *tls,
                                         const weida_identity_source *identity);

int weida_client_tls_set_alpn(const weida_client_tls *tls, int alpn);

void weida_client_tls_drop(weida_client_tls *tls);

int weida_server_tls_new(const weida_identity *identity, weida_server_tls **out);

int weida_server_tls_new_source(const weida_identity_source *identity, weida_server_tls **out);

int weida_server_tls_require_client(const weida_server_tls *tls, const weida_trust *trust);

int weida_server_tls_require_client_source(const weida_server_tls *tls,
                                           const weida_trust_source *trust);

int weida_server_tls_require_any_key(const weida_server_tls *tls);

int weida_server_tls_set_alpn(const weida_server_tls *tls, int alpn);

void weida_server_tls_drop(weida_server_tls *tls);

int weida_transfer_meta_new(weida_transfer_meta **out);

int weida_transfer_meta_set_content_type(const weida_transfer_meta *m, const char *content_type);

int weida_transfer_meta_set_content_len(const weida_transfer_meta *m, uint64_t len);

int weida_transfer_meta_set_topic(const weida_transfer_meta *m, const char *topic);

int weida_transfer_meta_set_trace(const weida_transfer_meta *m, const char *traceparent);

int weida_transfer_meta_set_achieved(const weida_transfer_meta *m, int level);

int weida_transfer_meta_set_report(const weida_transfer_meta *m,
                                   const uint64_t *levels,
                                   size_t len);

int weida_transfer_meta_set_report_mode(const weida_transfer_meta *m, int mode);

int weida_new_trace(char *out, size_t len);

void weida_transfer_meta_drop(weida_transfer_meta *m);

const char *weida_incoming_meta_endpoint(const weida_incoming_meta *m, size_t *len_out);

const char *weida_incoming_meta_content_type(const weida_incoming_meta *m, size_t *len_out);

const char *weida_incoming_meta_topic(const weida_incoming_meta *m, size_t *len_out);

const char *weida_incoming_meta_peer_fingerprint(const weida_incoming_meta *m, size_t *len_out);

int weida_incoming_meta_peer_chain_len(const weida_incoming_meta *m, size_t *out);

const uint8_t *weida_incoming_meta_peer_chain_cert(const weida_incoming_meta *m,
                                                   size_t index,
                                                   size_t *len_out);

int weida_incoming_meta_content_len(const weida_incoming_meta *m, uint64_t *out);

int weida_incoming_meta_sequence(const weida_incoming_meta *m, uint64_t *out);

int weida_incoming_meta_gap(const weida_incoming_meta *m,
                            uint64_t *out_expected,
                            uint64_t *out_seen);

int weida_incoming_meta_segment(const weida_incoming_meta *m, uint64_t *out);

int weida_incoming_meta_layer(const weida_incoming_meta *m, uint8_t *out);

const uint64_t *weida_incoming_meta_report(const weida_incoming_meta *m, size_t *len_out);

int weida_incoming_meta_report_mode(const weida_incoming_meta *m);

int weida_incoming_meta_report_id(const weida_incoming_meta *m, uint64_t *out);

const char *weida_incoming_meta_trace(const weida_incoming_meta *m, size_t *len_out);

const char *weida_incoming_meta_tracestate(const weida_incoming_meta *m, size_t *len_out);

int weida_incoming_meta_achieved(const weida_incoming_meta *m, int *out);

int weida_incoming_meta_peer_uid(const weida_incoming_meta *m, uint32_t *out);

int weida_incoming_meta_peer_gid(const weida_incoming_meta *m, uint32_t *out);

int weida_incoming_meta_peer_pid(const weida_incoming_meta *m, uint32_t *out);

const char *weida_incoming_meta_peer_sid(const weida_incoming_meta *m, size_t *len_out);

void weida_incoming_meta_drop(weida_incoming_meta *m);

const uint8_t *weida_msg_data(const weida_msg *msg);

size_t weida_msg_len(const weida_msg *msg);

int weida_msg_meta(const weida_msg *msg, weida_incoming_meta **out);

void weida_msg_drop(weida_msg *msg);

int weida_incoming_transfer_meta(const weida_incoming_transfer *t, weida_incoming_meta **out);

int weida_incoming_transfer_connection(const weida_incoming_transfer *t, uint64_t *out);

int weida_incoming_transfer_reporter(const weida_incoming_transfer *t, weida_reporter **out);

int weida_incoming_transfer_fd(const weida_incoming_transfer *t, int64_t *out);

int weida_incoming_transfer_read(const weida_incoming_transfer *t,
                                 uint8_t *buf,
                                 size_t cap,
                                 int64_t timeout_ms,
                                 size_t *out_n);

int weida_incoming_transfer_collect(weida_incoming_transfer *t,
                                    uint64_t max_bytes,
                                    weida_incoming_transfer_collect_op **out);

int weida_incoming_transfer_collect_op_fd(const weida_incoming_transfer_collect_op *op,
                                          int64_t *out);

int weida_incoming_transfer_collect_op_wait(const weida_incoming_transfer_collect_op *op,
                                            int64_t timeout_ms,
                                            weida_msg **out);

void weida_incoming_transfer_collect_op_drop(weida_incoming_transfer_collect_op *op);

void weida_incoming_transfer_drop(weida_incoming_transfer *t);

int weida_outgoing_transfer_fd(const weida_outgoing_transfer *t, int64_t *out);

int weida_outgoing_transfer_write(const weida_outgoing_transfer *t,
                                  const uint8_t *buf,
                                  size_t len,
                                  int64_t timeout_ms,
                                  size_t *out_n);

const char *weida_outgoing_transfer_trace(const weida_outgoing_transfer *t, size_t *len_out);

int weida_outgoing_transfer_set_priority(const weida_outgoing_transfer *t, int32_t priority);

int weida_outgoing_transfer_expire_after_ms(const weida_outgoing_transfer *t, uint64_t ms);

int weida_outgoing_transfer_cursors(const weida_outgoing_transfer *t, weida_cursors **out);

int weida_outgoing_transfer_finish(weida_outgoing_transfer *t, weida_delivery **out);

void weida_outgoing_transfer_drop(weida_outgoing_transfer *t);

int weida_delivery_delivered(weida_delivery *d, weida_delivery_delivered_op **out);

int weida_delivery_delivered_op_fd(const weida_delivery_delivered_op *op, int64_t *out);

int weida_delivery_delivered_op_wait(const weida_delivery_delivered_op *op, int64_t timeout_ms);

void weida_delivery_delivered_op_drop(weida_delivery_delivered_op *op);

void weida_delivery_drop(weida_delivery *d);
