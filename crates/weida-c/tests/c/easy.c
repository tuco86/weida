/*
 * The easy tier from C: a QUIC Req/Rep on 127.0.0.1 through weida_easy.h,
 * the replier on its own thread since weida_easy_call blocks until the
 * answer arrives. Checks the round trip with its content type, an accept
 * with nothing pending, and a reply over the client's ceiling. Built and run
 * by crates/weida-c/smoke.sh.
 */
#define _POSIX_C_SOURCE 200809L

#include <pthread.h>
#include <stdio.h>
#include <string.h>

#include "weida_easy.h"

/* A positive bound no run reaches, so the helpers' deadline arithmetic runs
 * without putting a time limit on the test. */
#define LONG_MS INT64_C(86400000)

static int fail(const char *what, int rc) {
    const char *detail = weida_last_error_message();
    fprintf(stderr, "c easy: %s failed: %s (%d): %s\n", what, weida_strerror(rc), rc,
            detail ? detail : "(no detail)");
    return 1;
}

#define CHECK(expr)                                                                     \
    do {                                                                                \
        int rc_ = (expr);                                                               \
        if (rc_ < 0)                                                                    \
            return fail(#expr, rc_);                                                    \
    } while (0)

struct server {
    weida_replier *replier;
    int rc;
};

/* The replier: answers "ping" with "pong" as text/plain, then answers the
 * second request with more than the client accepts. */
static int serve(weida_replier *replier) {
    weida_incoming_request *request = NULL;
    weida_msg *body = NULL;
    CHECK(weida_easy_accept(replier, 1024, LONG_MS, &request, &body));
    if (weida_msg_len(body) != 4 || memcmp(weida_msg_data(body), "ping", 4) != 0) {
        fprintf(stderr, "c easy: the request body is wrong\n");
        return 1;
    }
    weida_msg_drop(body);
    weida_transfer_meta *meta = NULL;
    CHECK(weida_transfer_meta_new(&meta));
    CHECK(weida_transfer_meta_set_content_type(meta, "text/plain"));
    CHECK(weida_easy_reply(request, meta, (const uint8_t *)"pong", 4, LONG_MS));
    weida_transfer_meta_drop(meta);

    request = NULL;
    body = NULL;
    CHECK(weida_easy_accept(replier, 1024, LONG_MS, &request, &body));
    weida_msg_drop(body);
    uint8_t big[64];
    memset(big, 'x', sizeof big);
    /* The client refuses this reply over its ceiling, so it is either
     * delivered or refused, and not checked; it settles either way, since
     * delivered resolves on the transport's acks or on the refusal. */
    (void)weida_easy_reply(request, NULL, big, sizeof big, LONG_MS);
    return 0;
}

static void *serve_thread(void *arg) {
    struct server *server = arg;
    server->rc = serve(server->replier);
    return NULL;
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
    CHECK(weida_listener_bind_quic_op_wait(bind, -1, &binding));
    weida_listener_bind_quic_op_drop(bind);
    weida_server_tls_drop(server_tls);
    CHECK(weida_listener_replier(listener, "/echo", &replier));

    const char *local = weida_binding_local_addr(binding, NULL);
    char url[256];
    snprintf(url, sizeof url, "weida://%s@%s/echo", fingerprint, local);

    /* Nothing pending: a try, then a short negative window. */
    weida_incoming_request *none = NULL;
    weida_msg *none_body = NULL;
    int rc = weida_easy_accept(replier, 1024, 0, &none, &none_body);
    if (rc != -WEIDA_EAGAIN || none != NULL || none_body != NULL) {
        fprintf(stderr, "c easy: the accept try with nothing pending is wrong: %s (%d)\n",
                weida_strerror(rc), rc);
        return 1;
    }
    rc = weida_easy_accept(replier, 1024, 100, &none, &none_body);
    if (rc != -WEIDA_EAGAIN || none != NULL || none_body != NULL) {
        fprintf(stderr, "c easy: the accept wait with nothing pending is wrong: %s (%d)\n",
                weida_strerror(rc), rc);
        return 1;
    }

    struct server server = {replier, 1};
    pthread_t thread;
    if (pthread_create(&thread, NULL, serve_thread, &server) != 0) {
        fprintf(stderr, "c easy: pthread_create failed\n");
        return 1;
    }

    /* The client. */
    weida_client_tls *client_tls = NULL;
    weida_requester *requester = NULL;
    weida_msg *msg = NULL;
    CHECK(weida_easy_client_tls_pinned(fingerprint, &client_tls));
    CHECK(weida_runtime_requester(rt, client_tls, &requester));
    weida_client_tls_drop(client_tls);
    CHECK(weida_easy_connect(requester, url, LONG_MS));
    CHECK(weida_easy_call(requester, (const uint8_t *)"ping", 4, 1024, LONG_MS, &msg));
    if (weida_msg_len(msg) != 4 || memcmp(weida_msg_data(msg), "pong", 4) != 0) {
        fprintf(stderr, "c easy: the reply body is wrong\n");
        return 1;
    }
    weida_incoming_meta *meta = NULL;
    CHECK(weida_msg_meta(msg, &meta));
    size_t type_len = 0;
    const char *type = weida_incoming_meta_content_type(meta, &type_len);
    if (type == NULL || type_len != 10 || memcmp(type, "text/plain", 10) != 0) {
        fprintf(stderr, "c easy: the reply's content type is wrong\n");
        return 1;
    }
    weida_incoming_meta_drop(meta);
    weida_msg_drop(msg);
    msg = NULL;

    /* Over the ceiling: the collect's code and record come back unchanged. */
    rc = weida_easy_call(requester, (const uint8_t *)"ping", 4, 2, LONG_MS, &msg);
    if (rc != -WEIDA_ELIMIT_EXCEEDED || msg != NULL) {
        fprintf(stderr, "c easy: the call over the ceiling is wrong: %s (%d)\n",
                weida_strerror(rc), rc);
        return 1;
    }
    const char *message = weida_last_error_message();
    if (message == NULL || strcmp(message, weida_strerror(-WEIDA_ELIMIT_EXCEEDED)) != 0) {
        fprintf(stderr, "c easy: the record after the call over the ceiling is wrong: %s\n",
                message ? message : "(none)");
        return 1;
    }

    if (pthread_join(thread, NULL) != 0) {
        fprintf(stderr, "c easy: pthread_join failed\n");
        return 1;
    }
    if (server.rc != 0)
        return 1;

    weida_requester_drop(requester);
    weida_replier_drop(replier);
    weida_binding_drop(binding);
    weida_listener_drop(listener);
    weida_identity_drop(identity);
    CHECK(weida_easy_shutdown(rt, LONG_MS));

    printf("c easy: ok\n");
    return 0;
}
