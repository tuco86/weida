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

#define WEIDA_POLLIN 1

typedef struct weida_binding weida_binding;

typedef struct weida_client_tls weida_client_tls;

typedef struct weida_delivery weida_delivery;

typedef struct weida_delivery_delivered_op weida_delivery_delivered_op;

typedef struct weida_identity weida_identity;

typedef struct weida_incoming_meta weida_incoming_meta;

typedef struct weida_incoming_request weida_incoming_request;

typedef struct weida_incoming_request_reply_op weida_incoming_request_reply_op;

typedef struct weida_incoming_transfer weida_incoming_transfer;

typedef struct weida_incoming_transfer_collect_op weida_incoming_transfer_collect_op;

typedef struct weida_listener weida_listener;

typedef struct weida_listener_bind_quic_op weida_listener_bind_quic_op;

typedef struct weida_local_binding weida_local_binding;

typedef struct weida_msg weida_msg;

typedef struct weida_outgoing_transfer weida_outgoing_transfer;

typedef struct weida_replier weida_replier;

typedef struct weida_requester weida_requester;

typedef struct weida_requester_connect_op weida_requester_connect_op;

typedef struct weida_requester_request_op weida_requester_request_op;

typedef struct weida_runtime weida_runtime;

typedef struct weida_runtime_config weida_runtime_config;

typedef struct weida_runtime_shutdown_op weida_runtime_shutdown_op;

typedef struct weida_server_tls weida_server_tls;

typedef struct weida_transfer_meta weida_transfer_meta;

typedef struct weida_trust weida_trust;

typedef struct {
  int64_t fd;
  int16_t events;
  int16_t revents;
} weida_poll_item;

const char *weida_version(void);

const char *weida_strerror(int code);

const char *weida_last_error_message(void);

int weida_last_error_loss_cause(void);

const char *weida_last_error_fingerprint(void);

int64_t weida_last_error_max(void);

int weida_last_error_os_code(void);

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

int weida_listener_bind_inproc(const weida_listener *l, const char *bus, weida_local_binding **out);

int weida_listener_replier(const weida_listener *l, const char *path, weida_replier **out);

void weida_listener_drop(weida_listener *l);

const char *weida_binding_local_addr(const weida_binding *b, size_t *len_out);

void weida_binding_drop(weida_binding *b);

const char *weida_local_binding_bus(const weida_local_binding *b, size_t *len_out);

void weida_local_binding_drop(weida_local_binding *b);

const char *weida_replier_path(const weida_replier *r, size_t *len_out);

int weida_replier_fd(const weida_replier *r, int64_t *out);

int weida_replier_accept(const weida_replier *r, int64_t timeout_ms, weida_incoming_request **out);

void weida_replier_drop(weida_replier *r);

int weida_incoming_request_meta(const weida_incoming_request *q, weida_incoming_meta **out);

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

void weida_incoming_request_drop(weida_incoming_request *q);

int weida_poll(weida_poll_item *items, size_t n, int64_t timeout_ms);

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

int weida_requester_request_op_fd(const weida_requester_request_op *op, int64_t *out);

int weida_requester_request_op_wait(const weida_requester_request_op *op,
                                    int64_t timeout_ms,
                                    weida_incoming_transfer **out);

void weida_requester_request_op_drop(weida_requester_request_op *op);

void weida_requester_drop(weida_requester *r);

int weida_runtime_config_new(weida_runtime_config **out);

int weida_runtime_config_set_worker_threads(const weida_runtime_config *cfg, uint32_t threads);

int weida_runtime_config_set_shutdown_timeout_ms(const weida_runtime_config *cfg, uint64_t ms);

void weida_runtime_config_drop(weida_runtime_config *cfg);

int weida_runtime_owned(const weida_runtime_config *cfg, weida_runtime **out);

int weida_runtime_shutdown(weida_runtime *rt, weida_runtime_shutdown_op **out);

int weida_runtime_shutdown_op_fd(const weida_runtime_shutdown_op *op, int64_t *out);

int weida_runtime_shutdown_op_wait(const weida_runtime_shutdown_op *op, int64_t timeout_ms);

void weida_runtime_shutdown_op_drop(weida_runtime_shutdown_op *op);

void weida_runtime_drop(weida_runtime *rt);

int weida_trust_new(weida_trust **out);

int weida_trust_add_pin(const weida_trust *t, const char *fingerprint);

void weida_trust_drop(weida_trust *t);

int weida_identity_generate(weida_identity **out);

int weida_identity_from_pem(const uint8_t *cert,
                            size_t cert_len,
                            const uint8_t *key,
                            size_t key_len,
                            weida_identity **out);

int weida_identity_fingerprint(const weida_identity *id, const char **out);

void weida_identity_drop(weida_identity *id);

int weida_client_tls_new(const weida_trust *trust, weida_client_tls **out);

void weida_client_tls_drop(weida_client_tls *tls);

int weida_server_tls_new(const weida_identity *identity, weida_server_tls **out);

void weida_server_tls_drop(weida_server_tls *tls);

int weida_transfer_meta_new(weida_transfer_meta **out);

int weida_transfer_meta_set_content_type(const weida_transfer_meta *m, const char *content_type);

void weida_transfer_meta_drop(weida_transfer_meta *m);

const char *weida_incoming_meta_endpoint(const weida_incoming_meta *m, size_t *len_out);

const char *weida_incoming_meta_content_type(const weida_incoming_meta *m, size_t *len_out);

const char *weida_incoming_meta_topic(const weida_incoming_meta *m, size_t *len_out);

const char *weida_incoming_meta_peer_fingerprint(const weida_incoming_meta *m, size_t *len_out);

int weida_incoming_meta_content_len(const weida_incoming_meta *m, uint64_t *out);

int weida_incoming_meta_sequence(const weida_incoming_meta *m, uint64_t *out);

void weida_incoming_meta_drop(weida_incoming_meta *m);

const uint8_t *weida_msg_data(const weida_msg *msg);

size_t weida_msg_len(const weida_msg *msg);

int weida_msg_meta(const weida_msg *msg, weida_incoming_meta **out);

void weida_msg_drop(weida_msg *msg);

int weida_incoming_transfer_meta(const weida_incoming_transfer *t, weida_incoming_meta **out);

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

int weida_outgoing_transfer_finish(weida_outgoing_transfer *t, weida_delivery **out);

void weida_outgoing_transfer_drop(weida_outgoing_transfer *t);

int weida_delivery_delivered(weida_delivery *d, weida_delivery_delivered_op **out);

int weida_delivery_delivered_op_fd(const weida_delivery_delivered_op *op, int64_t *out);

int weida_delivery_delivered_op_wait(const weida_delivery_delivered_op *op, int64_t timeout_ms);

void weida_delivery_delivered_op_drop(weida_delivery_delivered_op *op);

void weida_delivery_drop(weida_delivery *d);
