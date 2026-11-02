/*
 * weida_go.h: the C side of the Go package, included by every Go file that
 * calls libweida.
 *
 * libweida keeps the record of a failure in thread-local storage (R9), and a
 * goroutine may move to another OS thread between two cgo calls. So every
 * fallible function is called through a `go_<name>` wrapper below, which
 * calls it and, when it fails, copies the record into a weida_go_error in
 * the same cgo call. The wrappers of the waiting functions pass
 * `timeout_ms = 0`: the Go side never blocks in C, it waits in the netpoller.
 */

#ifndef WEIDA_GO_H
#define WEIDA_GO_H

#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "weida.h"

/* The thread-local record of one failure, copied out. The strings are
 * malloc'd copies the Go side frees; they are left NULL for -WEIDA_EAGAIN,
 * which the Go side only ever retries. */
typedef struct {
    int code;
    char *message;
    int loss_cause;
    char *fingerprint;
    int64_t max;
    int os_code;
} weida_go_error;

static inline char *weida_go_strdup(const char *s) {
    return s == NULL ? NULL : strdup(s);
}

static inline int weida_go_record(int rc, weida_go_error *e) {
    if (rc < 0) {
        e->code = -rc;
        e->loss_cause = weida_last_error_loss_cause();
        e->max = weida_last_error_max();
        e->os_code = weida_last_error_os_code();
        if (rc != -WEIDA_EAGAIN) {
            e->message = weida_go_strdup(weida_last_error_message());
            e->fingerprint = weida_go_strdup(weida_last_error_fingerprint());
        }
    }
    return rc;
}

/* A close-on-exec duplicate of a readiness descriptor, for the netpoller
 * to own; -1 with errno on failure. */
static inline int weida_go_dup(int64_t fd) {
    return fcntl((int)fd, F_DUPFD_CLOEXEC, 0);
}

/* go_<name>(params..., e) calls weida_<name>(args) and records a failure. */
#define WEIDA_GO_CALL(name, params, args) \
    static inline int go_##name params { return weida_go_record(weida_##name args, e); }

WEIDA_GO_CALL(runtime_config_new, (weida_runtime_config **out, weida_go_error *e), (out))
WEIDA_GO_CALL(runtime_config_set_worker_threads,
              (const weida_runtime_config *cfg, uint32_t threads, weida_go_error *e), (cfg, threads))
WEIDA_GO_CALL(runtime_config_set_shutdown_timeout_ms,
              (const weida_runtime_config *cfg, uint64_t ms, weida_go_error *e), (cfg, ms))
WEIDA_GO_CALL(runtime_owned,
              (const weida_runtime_config *cfg, weida_runtime **out, weida_go_error *e), (cfg, out))
WEIDA_GO_CALL(runtime_listener, (const weida_runtime *rt, weida_listener **out, weida_go_error *e),
              (rt, out))
WEIDA_GO_CALL(runtime_requester,
              (const weida_runtime *rt, const weida_client_tls *tls, weida_requester **out,
               weida_go_error *e),
              (rt, tls, out))
WEIDA_GO_CALL(runtime_shutdown, (weida_runtime *rt, weida_runtime_shutdown_op **out, weida_go_error *e),
              (rt, out))
WEIDA_GO_CALL(runtime_shutdown_op_fd,
              (const weida_runtime_shutdown_op *op, int64_t *out, weida_go_error *e), (op, out))
WEIDA_GO_CALL(runtime_shutdown_op_wait, (const weida_runtime_shutdown_op *op, weida_go_error *e),
              (op, 0))

WEIDA_GO_CALL(trust_new, (weida_trust **out, weida_go_error *e), (out))
WEIDA_GO_CALL(trust_add_pin, (const weida_trust *t, const char *fingerprint, weida_go_error *e),
              (t, fingerprint))
WEIDA_GO_CALL(trust_add_anchor,
              (const weida_trust *t, const uint8_t *pem, size_t len, weida_go_error *e),
              (t, pem, len))
WEIDA_GO_CALL(trust_add_anchor_file,
              (const weida_trust *t, const char *path, weida_go_error *e), (t, path))
WEIDA_GO_CALL(identity_generate, (weida_identity **out, weida_go_error *e), (out))
WEIDA_GO_CALL(identity_generate_for,
              (const char *const *names, size_t n, weida_identity **out, weida_go_error *e),
              (names, n, out))
WEIDA_GO_CALL(identity_from_pem,
              (const uint8_t *cert, size_t cert_len, const uint8_t *key, size_t key_len,
               weida_identity **out, weida_go_error *e),
              (cert, cert_len, key, key_len, out))
WEIDA_GO_CALL(identity_from_pem_files,
              (const char *cert_chain, const char *key, weida_identity **out, weida_go_error *e),
              (cert_chain, key, out))
WEIDA_GO_CALL(identity_from_pem_file,
              (const char *path, weida_identity **out, weida_go_error *e), (path, out))
WEIDA_GO_CALL(identity_fingerprint, (const weida_identity *id, const char **out, weida_go_error *e),
              (id, out))
WEIDA_GO_CALL(identity_certificate_pem,
              (const weida_identity *id, weida_pem **out, weida_go_error *e), (id, out))
WEIDA_GO_CALL(identity_to_pem, (const weida_identity *id, weida_pem **out, weida_go_error *e),
              (id, out))
WEIDA_GO_CALL(client_tls_new, (const weida_trust *trust, weida_client_tls **out, weida_go_error *e),
              (trust, out))
WEIDA_GO_CALL(client_tls_set_identity,
              (const weida_client_tls *tls, const weida_identity *identity, weida_go_error *e),
              (tls, identity))
WEIDA_GO_CALL(server_tls_new,
              (const weida_identity *identity, weida_server_tls **out, weida_go_error *e),
              (identity, out))
WEIDA_GO_CALL(server_tls_require_client,
              (const weida_server_tls *tls, const weida_trust *trust, weida_go_error *e),
              (tls, trust))
WEIDA_GO_CALL(server_tls_require_any_key, (const weida_server_tls *tls, weida_go_error *e), (tls))
WEIDA_GO_CALL(transfer_meta_new, (weida_transfer_meta **out, weida_go_error *e), (out))
WEIDA_GO_CALL(transfer_meta_set_content_type,
              (const weida_transfer_meta *m, const char *content_type, weida_go_error *e),
              (m, content_type))

WEIDA_GO_CALL(listener_bind_quic,
              (const weida_listener *l, const char *addr, const weida_server_tls *tls,
               weida_listener_bind_quic_op **out, weida_go_error *e),
              (l, addr, tls, out))
WEIDA_GO_CALL(listener_bind_quic_op_fd,
              (const weida_listener_bind_quic_op *op, int64_t *out, weida_go_error *e), (op, out))
WEIDA_GO_CALL(listener_bind_quic_op_wait,
              (const weida_listener_bind_quic_op *op, weida_binding **out, weida_go_error *e),
              (op, 0, out))
WEIDA_GO_CALL(listener_bind_inproc,
              (const weida_listener *l, const char *bus, weida_local_binding **out, weida_go_error *e),
              (l, bus, out))
WEIDA_GO_CALL(listener_replier,
              (const weida_listener *l, const char *path, weida_replier **out, weida_go_error *e),
              (l, path, out))

WEIDA_GO_CALL(replier_fd, (const weida_replier *r, int64_t *out, weida_go_error *e), (r, out))
WEIDA_GO_CALL(replier_accept,
              (const weida_replier *r, weida_incoming_request **out, weida_go_error *e), (r, 0, out))
WEIDA_GO_CALL(incoming_request_meta,
              (const weida_incoming_request *q, weida_incoming_meta **out, weida_go_error *e), (q, out))
WEIDA_GO_CALL(incoming_request_take_body,
              (const weida_incoming_request *q, weida_incoming_transfer **out, weida_go_error *e),
              (q, out))
WEIDA_GO_CALL(incoming_request_reply,
              (weida_incoming_request *q, const weida_transfer_meta *meta,
               weida_incoming_request_reply_op **out, weida_go_error *e),
              (q, meta, out))
WEIDA_GO_CALL(incoming_request_reply_op_fd,
              (const weida_incoming_request_reply_op *op, int64_t *out, weida_go_error *e), (op, out))
WEIDA_GO_CALL(incoming_request_reply_op_wait,
              (const weida_incoming_request_reply_op *op, weida_outgoing_transfer **out,
               weida_go_error *e),
              (op, 0, out))

WEIDA_GO_CALL(requester_connect,
              (const weida_requester *r, const char *url, weida_requester_connect_op **out,
               weida_go_error *e),
              (r, url, out))
WEIDA_GO_CALL(requester_connect_op_fd,
              (const weida_requester_connect_op *op, int64_t *out, weida_go_error *e), (op, out))
WEIDA_GO_CALL(requester_connect_op_wait, (const weida_requester_connect_op *op, weida_go_error *e),
              (op, 0))
WEIDA_GO_CALL(requester_request,
              (const weida_requester *r, const uint8_t *body, size_t len,
               weida_requester_request_op **out, weida_go_error *e),
              (r, body, len, out))
WEIDA_GO_CALL(requester_request_op_fd,
              (const weida_requester_request_op *op, int64_t *out, weida_go_error *e), (op, out))
WEIDA_GO_CALL(requester_request_op_wait,
              (const weida_requester_request_op *op, weida_incoming_transfer **out, weida_go_error *e),
              (op, 0, out))

WEIDA_GO_CALL(msg_meta, (const weida_msg *msg, weida_incoming_meta **out, weida_go_error *e),
              (msg, out))
WEIDA_GO_CALL(incoming_transfer_meta,
              (const weida_incoming_transfer *t, weida_incoming_meta **out, weida_go_error *e),
              (t, out))
WEIDA_GO_CALL(incoming_transfer_fd, (const weida_incoming_transfer *t, int64_t *out, weida_go_error *e),
              (t, out))
WEIDA_GO_CALL(incoming_transfer_read,
              (const weida_incoming_transfer *t, uint8_t *buf, size_t cap, size_t *out_n,
               weida_go_error *e),
              (t, buf, cap, 0, out_n))
WEIDA_GO_CALL(incoming_transfer_collect,
              (weida_incoming_transfer *t, uint64_t max_bytes, weida_incoming_transfer_collect_op **out,
               weida_go_error *e),
              (t, max_bytes, out))
WEIDA_GO_CALL(incoming_transfer_collect_op_fd,
              (const weida_incoming_transfer_collect_op *op, int64_t *out, weida_go_error *e), (op, out))
WEIDA_GO_CALL(incoming_transfer_collect_op_wait,
              (const weida_incoming_transfer_collect_op *op, weida_msg **out, weida_go_error *e),
              (op, 0, out))

WEIDA_GO_CALL(outgoing_transfer_fd, (const weida_outgoing_transfer *t, int64_t *out, weida_go_error *e),
              (t, out))
WEIDA_GO_CALL(outgoing_transfer_write,
              (const weida_outgoing_transfer *t, const uint8_t *buf, size_t len, size_t *out_n,
               weida_go_error *e),
              (t, buf, len, 0, out_n))
WEIDA_GO_CALL(outgoing_transfer_finish,
              (weida_outgoing_transfer *t, weida_delivery **out, weida_go_error *e), (t, out))
WEIDA_GO_CALL(delivery_delivered,
              (weida_delivery *d, weida_delivery_delivered_op **out, weida_go_error *e), (d, out))
WEIDA_GO_CALL(delivery_delivered_op_fd,
              (const weida_delivery_delivered_op *op, int64_t *out, weida_go_error *e), (op, out))
WEIDA_GO_CALL(delivery_delivered_op_wait, (const weida_delivery_delivered_op *op, weida_go_error *e),
              (op, 0))

#endif /* WEIDA_GO_H */
