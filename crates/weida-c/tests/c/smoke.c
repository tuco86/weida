/*
 * The C ABI from C: a streamed Req/Rep over QUIC on 127.0.0.1, with a
 * generated identity, a pinned trust and a content type, where every wait is
 * a timeout-0 try or op wait followed by weida_poll on the handle's
 * descriptor. Built and run by crates/weida-c/smoke.sh.
 */
#include <stdio.h>
#include <string.h>

#include "weida.h"

/* How long one weida_poll may wait before the smoke gives up. */
#define WAIT_MS 30000

static int fail(const char *what, int rc) {
    const char *detail = weida_last_error_message();
    fprintf(stderr, "c smoke: %s failed: %s (%d): %s\n", what, weida_strerror(rc), rc,
            detail ? detail : "(no detail)");
    return 1;
}

#define CHECK(expr)                                                                     \
    do {                                                                                \
        int rc_ = (expr);                                                               \
        if (rc_ < 0)                                                                    \
            return fail(#expr, rc_);                                                    \
    } while (0)

/* Waits until `fd` is readable. */
static int wait_readable(int64_t fd) {
    weida_poll_item item = {fd, WEIDA_POLLIN, 0};
    int n = weida_poll(&item, 1, WAIT_MS);
    if (n < 0)
        return n;
    if (n == 0)
        return -WEIDA_EAGAIN;
    return 0;
}

/* Runs `attempt` (a timeout-0 try or op wait) until it stops answering
 * EAGAIN, waiting on the descriptor `fd_of(handle)` between attempts. */
#define SETTLE(attempt, fd_of, handle)                                                  \
    do {                                                                                \
        for (;;) {                                                                      \
            int rc_ = (attempt);                                                        \
            if (rc_ != -WEIDA_EAGAIN) {                                                 \
                if (rc_ < 0)                                                            \
                    return fail(#attempt, rc_);                                         \
                break;                                                                  \
            }                                                                           \
            int64_t fd_ = -1;                                                           \
            CHECK(fd_of((handle), &fd_));                                               \
            CHECK(wait_readable(fd_));                                                  \
        }                                                                               \
    } while (0)

static int write_all(weida_outgoing_transfer *out, const char *data, size_t len) {
    while (len > 0) {
        size_t n = 0;
        SETTLE(weida_outgoing_transfer_write(out, (const uint8_t *)data, len, 0, &n),
               weida_outgoing_transfer_fd, out);
        data += n;
        len -= n;
    }
    return 0;
}

int main(void) {
    weida_runtime *rt = NULL;
    weida_listener *listener = NULL;
    weida_identity *identity = NULL;
    weida_server_tls *server_tls = NULL;
    weida_listener_bind_quic_op *bind = NULL;
    weida_binding *binding = NULL;
    weida_replier *replier = NULL;
    const char *fingerprint = NULL;

    CHECK(weida_runtime_owned(NULL, &rt));
    CHECK(weida_runtime_listener(rt, &listener));
    CHECK(weida_identity_generate(&identity));
    CHECK(weida_identity_fingerprint(identity, &fingerprint));
    CHECK(weida_server_tls_new(identity, &server_tls));
    CHECK(weida_listener_bind_quic(listener, "127.0.0.1:0", server_tls, &bind));
    SETTLE(weida_listener_bind_quic_op_wait(bind, 0, &binding), weida_listener_bind_quic_op_fd,
           bind);
    weida_listener_bind_quic_op_drop(bind);
    weida_server_tls_drop(server_tls);
    CHECK(weida_listener_replier(listener, "/echo", &replier));

    const char *local = weida_binding_local_addr(binding, NULL);
    char url[256];
    snprintf(url, sizeof url, "weida://%s@%s/echo", fingerprint, local);

    weida_trust *trust = NULL;
    weida_client_tls *client_tls = NULL;
    weida_requester *requester = NULL;
    weida_requester_connect_op *connect = NULL;
    CHECK(weida_trust_new(&trust));
    CHECK(weida_trust_add_pin(trust, fingerprint));
    CHECK(weida_client_tls_new(trust, &client_tls));
    CHECK(weida_runtime_requester(rt, client_tls, &requester));
    weida_client_tls_drop(client_tls);
    weida_trust_drop(trust);
    CHECK(weida_requester_connect(requester, url, &connect));
    SETTLE(weida_requester_connect_op_wait(connect, 0), weida_requester_connect_op_fd, connect);
    weida_requester_connect_op_drop(connect);

    weida_requester_request_op *exchange = NULL;
    CHECK(weida_requester_request(requester, (const uint8_t *)"ping", 4, &exchange));

    /* The replier: accept, read the body in pieces, reply with a type. */
    weida_incoming_request *request = NULL;
    SETTLE(weida_replier_accept(replier, 0, &request), weida_replier_fd, replier);
    weida_incoming_transfer *body = NULL;
    CHECK(weida_incoming_request_take_body(request, &body));
    char got[16];
    size_t total = 0;
    for (;;) {
        size_t n = 0;
        SETTLE(weida_incoming_transfer_read(body, (uint8_t *)got + total, 3, 0, &n),
               weida_incoming_transfer_fd, body);
        if (n == 0)
            break;
        total += n;
        if (total > sizeof got - 3) {
            fprintf(stderr, "c smoke: the request body is too long\n");
            return 1;
        }
    }
    weida_incoming_transfer_drop(body);
    if (total != 4 || memcmp(got, "ping", 4) != 0) {
        fprintf(stderr, "c smoke: the request body is wrong\n");
        return 1;
    }

    weida_transfer_meta *meta = NULL;
    weida_incoming_request_reply_op *opening = NULL;
    weida_outgoing_transfer *reply = NULL;
    CHECK(weida_transfer_meta_new(&meta));
    CHECK(weida_transfer_meta_set_content_type(meta, "text/plain"));
    CHECK(weida_incoming_request_reply(request, meta, &opening));
    weida_transfer_meta_drop(meta);
    SETTLE(weida_incoming_request_reply_op_wait(opening, 0, &reply),
           weida_incoming_request_reply_op_fd, opening);
    weida_incoming_request_reply_op_drop(opening);
    if (write_all(reply, "pong", 4) != 0)
        return 1;
    weida_delivery *delivery = NULL;
    weida_delivery_delivered_op *delivered = NULL;
    CHECK(weida_outgoing_transfer_finish(reply, &delivery));
    CHECK(weida_delivery_delivered(delivery, &delivered));
    SETTLE(weida_delivery_delivered_op_wait(delivered, 0), weida_delivery_delivered_op_fd,
           delivered);
    weida_delivery_delivered_op_drop(delivered);

    /* The requester: the reply stream, its type, and its body collected. */
    weida_incoming_transfer *answer = NULL;
    SETTLE(weida_requester_request_op_wait(exchange, 0, &answer), weida_requester_request_op_fd,
           exchange);
    weida_requester_request_op_drop(exchange);
    weida_incoming_meta *answer_meta = NULL;
    CHECK(weida_incoming_transfer_meta(answer, &answer_meta));
    size_t type_len = 0;
    const char *type = weida_incoming_meta_content_type(answer_meta, &type_len);
    if (type == NULL || type_len != 10 || memcmp(type, "text/plain", 10) != 0) {
        fprintf(stderr, "c smoke: the reply's content type is wrong\n");
        return 1;
    }
    weida_incoming_meta_drop(answer_meta);
    weida_incoming_transfer_collect_op *collecting = NULL;
    weida_msg *msg = NULL;
    CHECK(weida_incoming_transfer_collect(answer, 1024, &collecting));
    SETTLE(weida_incoming_transfer_collect_op_wait(collecting, 0, &msg),
           weida_incoming_transfer_collect_op_fd, collecting);
    weida_incoming_transfer_collect_op_drop(collecting);
    if (weida_msg_len(msg) != 4 || memcmp(weida_msg_data(msg), "pong", 4) != 0) {
        fprintf(stderr, "c smoke: the reply body is wrong\n");
        return 1;
    }
    weida_msg_drop(msg);

    weida_requester_drop(requester);
    weida_replier_drop(replier);
    weida_binding_drop(binding);
    weida_listener_drop(listener);
    weida_identity_drop(identity);
    weida_runtime_shutdown_op *shutdown = NULL;
    CHECK(weida_runtime_shutdown(rt, &shutdown));
    SETTLE(weida_runtime_shutdown_op_wait(shutdown, 0), weida_runtime_shutdown_op_fd, shutdown);
    weida_runtime_shutdown_op_drop(shutdown);

    printf("c smoke: ok\n");
    return 0;
}
