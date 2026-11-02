/*
 * The NIF over libweida (docs/libraries/weida-beam.md): one function per
 * weida.h function of the slice, named like it without the `weida_`
 * prefix, plus version/0, strerror/1, drop/1, select/2 and
 * select_cancel/2.
 *
 * No call blocks a scheduler: every C call passes timeout_ms = 0, a try or
 * an op wait that would block returns the atom `again`, and src/weida.erl
 * waits for the handle's readiness descriptor in a `receive`. Any other
 * failure returns {error, Map} with the thread-local record (R9) read in
 * the same call.
 *
 * Every C handle lives in one resource type, `weida_handle`, tagged with
 * its kind. drop/1 and the consuming functions (R6) clear its pointer, so
 * a later call returns the `closed` error without calling C; the
 * destructor drops a pointer still set.
 *
 * Waiting: select/2 queues the calling process in the handle's FIFO of
 * waiters. The head of the FIFO is selected with enif_select_read on a
 * duplicate of the handle's descriptor and receives
 * {select, H, Ref, ready_input}; select_cancel/2 removes a waiter, and a
 * removed head hands the selection to the next one, which is notified at
 * once while the descriptor is still readable (R11). The duplicate is the
 * VM's own: it closes in the `stop` callback, independent of when the C
 * handle drops its descriptor. It is stopped as soon as the FIFO is empty,
 * so an idle handle holds no select relation and its resource is
 * destructed like any other. Each waiter is monitored; a waiter that dies
 * leaves the FIFO in the `down` callback.
 */
#define _POSIX_C_SOURCE 200809L

#include <erl_nif.h>

#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "weida.h"

/* ---- kinds ---- */

#define KINDS(X) \
    X(runtime) \
    X(runtime_shutdown_op) \
    X(listener) \
    X(listener_bind_quic_op) \
    X(binding) \
    X(local_binding) \
    X(replier) \
    X(incoming_request) \
    X(incoming_request_reply_op) \
    X(requester) \
    X(requester_connect_op) \
    X(requester_request_op) \
    X(trust) \
    X(identity) \
    X(client_tls) \
    X(server_tls) \
    X(incoming_transfer) \
    X(incoming_transfer_collect_op) \
    X(outgoing_transfer) \
    X(delivery) \
    X(delivery_delivered_op)

/* The kinds with a readiness descriptor (`weida_<t>_fd`). */
#define FD_KINDS(X) \
    X(runtime_shutdown_op) \
    X(listener_bind_quic_op) \
    X(replier) \
    X(incoming_request_reply_op) \
    X(requester_connect_op) \
    X(requester_request_op) \
    X(incoming_transfer) \
    X(incoming_transfer_collect_op) \
    X(outgoing_transfer) \
    X(delivery_delivered_op)

enum kind {
#define KIND(t) K_##t,
    KINDS(KIND)
#undef KIND
        K_COUNT
};

typedef void (*drop_fn)(void *);
typedef int (*fd_fn)(const void *, int64_t *);

#define DROP(t) \
    static void drop_##t(void *p) { weida_##t##_drop(p); }
KINDS(DROP)
#undef DROP

#define FD(t) \
    static int fd_##t(const void *p, int64_t *out) { return weida_##t##_fd(p, out); }
FD_KINDS(FD)
#undef FD

static const drop_fn DROPS[K_COUNT] = {
#define DROP_ENTRY(t) [K_##t] = drop_##t,
    KINDS(DROP_ENTRY)
#undef DROP_ENTRY
};

static const fd_fn FDS[K_COUNT] = {
#define FD_ENTRY(t) [K_##t] = fd_##t,
    FD_KINDS(FD_ENTRY)
#undef FD_ENTRY
};

/* ---- atoms ---- */

static ERL_NIF_TERM A_ok, A_error, A_again, A_eof, A_undefined, A_select, A_ready_input,
    A_weida_closed, A_enomem, A_select_failed, A_code, A_message, A_loss_cause, A_fingerprint,
    A_max, A_os_code, A_endpoint, A_content_type, A_topic, A_peer_fingerprint, A_content_len,
    A_sequence, A_payload, A_meta, A_worker_threads, A_shutdown_timeout_ms, A_peer_chain;

/* docs/c/README.md R9, as snake_case atoms. */
static const struct {
    int code;
    const char *name;
} CODES[] = {
    {WEIDA_ERUNTIME, "runtime"},
    {WEIDA_EINVALID_ADDRESS, "invalid_address"},
    {WEIDA_EINVALID_ENDPOINT_PATH, "invalid_endpoint_path"},
    {WEIDA_EINVALID_FINGERPRINT, "invalid_fingerprint"},
    {WEIDA_EALREADY_REGISTERED, "already_registered"},
    {WEIDA_ENOT_CONNECTED, "not_connected"},
    {WEIDA_ECONNECTION_LOST, "connection_lost"},
    {WEIDA_ENEGOTIATION, "negotiation"},
    {WEIDA_EPROTOCOL, "protocol"},
    {WEIDA_EREJECTED, "rejected"},
    {WEIDA_EUNKNOWN_ENDPOINT, "unknown_endpoint"},
    {WEIDA_EUNSUPPORTED, "unsupported"},
    {WEIDA_ENO_PARKED_CONNECTION, "no_parked_connection"},
    {WEIDA_ENO_REPLY, "no_reply"},
    {WEIDA_ECANCELED, "canceled"},
    {WEIDA_EEXPIRED, "expired"},
    {WEIDA_EINDETERMINATE, "indeterminate"},
    {WEIDA_ELIMIT_EXCEEDED, "limit_exceeded"},
    {WEIDA_EDATAGRAMS_UNAVAILABLE, "datagrams_unavailable"},
    {WEIDA_ETOO_LARGE, "too_large"},
    {WEIDA_ETLS, "tls"},
    {WEIDA_EUNTRUSTED, "untrusted"},
    {WEIDA_EIO, "io"},
    {WEIDA_ETRANSPORT, "transport"},
    {WEIDA_EAGAIN, "again"},
    {WEIDA_EINTR, "intr"},
    {WEIDA_EINVAL, "inval"},
    {WEIDA_ECLOSED, "closed"},
    {WEIDA_EPANIC, "panic"},
};
#define NCODES (sizeof CODES / sizeof CODES[0])
static ERL_NIF_TERM CODE_ATOMS[NCODES];

/* Indexed by the WEIDA_LOSS_* value. */
static const char *const LOSSES[] = {
    NULL,
    "idle_timeout",
    "peer_closed",
    "peer_restarting",
    "locally_closed",
    "reset",
    "transport_error",
};
#define NLOSSES (sizeof LOSSES / sizeof LOSSES[0])
static ERL_NIF_TERM LOSS_ATOMS[NLOSSES];

/* ---- handles ---- */

struct waiter {
    struct waiter *next;
    ErlNifPid pid;
    ErlNifMonitor mon;
    ErlNifEnv *env; /* holds `ref` */
    ERL_NIF_TERM ref;
};

struct handle {
    enum kind kind;
    ErlNifMutex *mu; /* guards everything below */
    void *ptr;       /* NULL once dropped or consumed */
    int ev;          /* the VM's duplicate of the descriptor while selected, else -1 */
    struct waiter *head, *tail;
};

static ErlNifResourceType *HANDLE;

#define SELECT_FAILED (-1000)

#define NIF(name) \
    static ERL_NIF_TERM name(ErlNifEnv *env, int argc __attribute__((unused)), \
                             const ERL_NIF_TERM argv[] __attribute__((unused)))

static ERL_NIF_TERM ok_tuple(ErlNifEnv *env, ERL_NIF_TERM v) {
    return enif_make_tuple2(env, A_ok, v);
}

static ERL_NIF_TERM make_handle(ErlNifEnv *env, enum kind k, void *ptr) {
    struct handle *h = enif_alloc_resource(HANDLE, sizeof *h);
    if (!h) {
        DROPS[k](ptr);
        return enif_raise_exception(env, A_enomem);
    }
    h->kind = k;
    h->ptr = ptr;
    h->ev = -1;
    h->head = h->tail = NULL;
    h->mu = enif_mutex_create("weida_handle");
    if (!h->mu) {
        enif_release_resource(h); /* the destructor drops `ptr` */
        return enif_raise_exception(env, A_enomem);
    }
    ERL_NIF_TERM t = enif_make_resource(env, h);
    enif_release_resource(h);
    return t;
}

/* The handle `t` names, of kind `k` (any kind when `k` < 0), or NULL. */
static struct handle *arg(ErlNifEnv *env, ERL_NIF_TERM t, int k) {
    struct handle *h = NULL;
    if (!enif_get_resource(env, t, HANDLE, (void **)&h))
        return NULL;
    if (k >= 0 && (int)h->kind != k)
        return NULL;
    return h;
}

/* Locks `h` and returns 1 when it is live; returns 0, unlocked, when it was
 * dropped or consumed. */
static int lock_live(struct handle *h) {
    enif_mutex_lock(h->mu);
    if (h->ptr)
        return 1;
    enif_mutex_unlock(h->mu);
    return 0;
}

/* ---- errors ---- */

static ERL_NIF_TERM code_atom(ErlNifEnv *env, int code) {
    for (size_t i = 0; i < NCODES; i++)
        if (CODES[i].code == code)
            return CODE_ATOMS[i];
    return enif_make_int(env, code);
}

static ERL_NIF_TERM cstr(ErlNifEnv *env, const char *s, size_t len) {
    ERL_NIF_TERM t;
    if (!s)
        return A_undefined;
    unsigned char *d = enif_make_new_binary(env, len, &t);
    if (len)
        memcpy(d, s, len);
    return t;
}

static void put(ErlNifEnv *env, ERL_NIF_TERM *map, ERL_NIF_TERM key, ERL_NIF_TERM value) {
    enif_make_map_put(env, *map, key, value, map);
}

/* {error, Map} for `code` (positive). With `record`, the thread-local
 * record of the C call that just failed on this thread fills it. */
static ERL_NIF_TERM error_tuple(ErlNifEnv *env, int code, int record) {
    ERL_NIF_TERM m = enif_make_new_map(env);
    const char *message = NULL;
    put(env, &m, A_code, code_atom(env, code));
    if (record) {
        message = weida_last_error_message();
        int loss = weida_last_error_loss_cause();
        if (loss > 0 && (size_t)loss < NLOSSES)
            put(env, &m, A_loss_cause, LOSS_ATOMS[loss]);
        const char *fp = weida_last_error_fingerprint();
        if (fp)
            put(env, &m, A_fingerprint, cstr(env, fp, strlen(fp)));
        int64_t max = weida_last_error_max();
        if (max >= 0)
            put(env, &m, A_max, enif_make_int64(env, max));
        int os = weida_last_error_os_code();
        if (os != 0)
            put(env, &m, A_os_code, enif_make_int(env, os));
    }
    if (!message)
        message = weida_strerror(code);
    put(env, &m, A_message, cstr(env, message, strlen(message)));
    return enif_make_tuple2(env, A_error, m);
}

/* What a failed C call (`rc` negative) returns. */
static ERL_NIF_TERM fail(ErlNifEnv *env, int rc) {
    return rc == -WEIDA_EAGAIN ? A_again : error_tuple(env, -rc, 1);
}

static ERL_NIF_TERM closed(ErlNifEnv *env) {
    return error_tuple(env, WEIDA_ECLOSED, 0);
}

/* ---- values ---- */

/* A NUL-terminated copy (enif_alloc) of a binary or iolist (R7): 1, or 0
 * when `t` is neither or holds a NUL. */
static int get_cstr(ErlNifEnv *env, ERL_NIF_TERM t, char **out) {
    ErlNifBinary b;
    if (!enif_inspect_iolist_as_binary(env, t, &b) || memchr(b.data, 0, b.size))
        return 0;
    char *s = enif_alloc(b.size + 1);
    if (!s)
        return 0;
    if (b.size)
        memcpy(s, b.data, b.size);
    s[b.size] = 0;
    *out = s;
    return 1;
}

static ERL_NIF_TERM optional_u64(ErlNifEnv *env, int present, uint64_t n) {
    return present == 1 ? enif_make_uint64(env, n) : A_undefined;
}

/* Copies a weida_incoming_meta out into a map and drops it. */
static ERL_NIF_TERM meta_value(ErlNifEnv *env, weida_incoming_meta *m) {
    ERL_NIF_TERM map = enif_make_new_map(env);
    size_t len = 0;
    uint64_t n = 0;
    const char *s = weida_incoming_meta_endpoint(m, &len);
    put(env, &map, A_endpoint, cstr(env, s, len));
    s = weida_incoming_meta_content_type(m, &len);
    put(env, &map, A_content_type, cstr(env, s, len));
    s = weida_incoming_meta_topic(m, &len);
    put(env, &map, A_topic, cstr(env, s, len));
    s = weida_incoming_meta_peer_fingerprint(m, &len);
    put(env, &map, A_peer_fingerprint, cstr(env, s, len));
    int present = weida_incoming_meta_content_len(m, &n);
    put(env, &map, A_content_len, optional_u64(env, present, n));
    present = weida_incoming_meta_sequence(m, &n);
    put(env, &map, A_sequence, optional_u64(env, present, n));
    size_t count = 0;
    ERL_NIF_TERM chain = A_undefined;
    if (weida_incoming_meta_peer_chain_len(m, &count) == 1) {
        chain = enif_make_list(env, 0);
        for (size_t i = count; i-- > 0;) {
            size_t der_len = 0;
            const uint8_t *der = weida_incoming_meta_peer_chain_cert(m, i, &der_len);
            ERL_NIF_TERM cert;
            unsigned char *d = enif_make_new_binary(env, der_len, &cert);
            if (der_len)
                memcpy(d, der, der_len);
            chain = enif_make_list_cell(env, cert, chain);
        }
    }
    put(env, &map, A_peer_chain, chain);
    weida_incoming_meta_drop(m);
    return map;
}

/* Copies a weida_msg out into #{payload, meta} and drops it. */
static ERL_NIF_TERM msg_value(ErlNifEnv *env, weida_msg *msg) {
    weida_incoming_meta *m = NULL;
    int rc = weida_msg_meta(msg, &m);
    if (rc < 0) {
        ERL_NIF_TERM e = fail(env, rc);
        weida_msg_drop(msg);
        return e;
    }
    ERL_NIF_TERM payload;
    size_t len = weida_msg_len(msg);
    unsigned char *d = enif_make_new_binary(env, len, &payload);
    if (len)
        memcpy(d, weida_msg_data(msg), len);
    weida_msg_drop(msg);
    ERL_NIF_TERM map = enif_make_new_map(env);
    put(env, &map, A_payload, payload);
    put(env, &map, A_meta, meta_value(env, m));
    return ok_tuple(env, map);
}

/* ---- waiting ---- */

static void free_waiter(struct waiter *w) {
    enif_free_env(w->env);
    enif_free(w);
}

/* Selects the descriptor for the head waiter. Under `h->mu`, with a head;
 * the handle is live unless it is already selected. Returns 0, a negative
 * weida code, or SELECT_FAILED. */
static int arm(ErlNifEnv *env, struct handle *h) {
    if (h->ev < 0) {
        int64_t fd = -1;
        int rc = FDS[h->kind](h->ptr, &fd);
        if (rc < 0)
            return rc;
        int ev = fcntl((int)fd, F_DUPFD_CLOEXEC, 0);
        if (ev < 0)
            return SELECT_FAILED;
        h->ev = ev;
    }
    struct waiter *w = h->head;
    ERL_NIF_TERM msg = enif_make_tuple4(env, A_select, enif_make_resource(env, h),
                                        enif_make_copy(env, w->ref), A_ready_input);
    if (enif_select_read(env, (ErlNifEvent)h->ev, h, &w->pid, msg, NULL) < 0)
        return SELECT_FAILED;
    return 0;
}

/* Ends the selection: the `stop` callback closes the duplicate once the VM
 * no longer watches it. Under `h->mu`. */
static void disarm(ErlNifEnv *env, struct handle *h) {
    if (h->ev < 0)
        return;
    if (enif_select(env, (ErlNifEvent)h->ev, ERL_NIF_SELECT_STOP, h, NULL, A_undefined) < 0)
        close(h->ev); /* never selected: no stop callback will run */
    h->ev = -1;
}

/* Sends every waiter {weida_closed, Ref} (`closing`) or its select
 * notification, and empties the FIFO. Under `h->mu`. */
static void release_all(ErlNifEnv *env, struct handle *h, int closing) {
    struct waiter *w = h->head;
    h->head = h->tail = NULL;
    while (w) {
        struct waiter *next = w->next;
        ERL_NIF_TERM msg =
            closing ? enif_make_tuple2(w->env, A_weida_closed, w->ref)
                    : enif_make_tuple4(w->env, A_select, enif_make_resource(w->env, h), w->ref,
                                       A_ready_input);
        enif_demonitor_process(env, h, &w->mon);
        enif_send(env, &w->pid, w->env, msg);
        free_waiter(w);
        w = next;
    }
}

/* After the head left the FIFO: selects for the new head, or stops when
 * the FIFO is empty. A head that cannot be selected wakes every waiter, so
 * each retries and meets the failure itself. Under `h->mu`. */
static void advance(ErlNifEnv *env, struct handle *h) {
    if (h->head && h->ev >= 0) {
        enif_select(env, (ErlNifEvent)h->ev, ERL_NIF_SELECT_CANCEL | ERL_NIF_SELECT_READ, h, NULL,
                    A_undefined);
        if (arm(env, h) == 0)
            return;
    }
    release_all(env, h, 0);
    disarm(env, h);
}

/* Unlinks `w` from the FIFO; returns whether it was the head. */
static int unlink_waiter(struct handle *h, struct waiter *w) {
    struct waiter **pp = &h->head, *prev = NULL;
    while (*pp != w) {
        prev = *pp;
        pp = &(*pp)->next;
    }
    *pp = w->next;
    if (h->tail == w)
        h->tail = prev;
    return prev == NULL;
}

/* Takes the pointer out of a handle and ends every wait on it: drop/1 and
 * the consuming functions (R6). Under `h->mu`. */
static void *detach(ErlNifEnv *env, struct handle *h) {
    void *p = h->ptr;
    h->ptr = NULL;
    release_all(env, h, 1);
    disarm(env, h);
    return p;
}

static void handle_dtor(ErlNifEnv *env, void *obj) {
    (void)env;
    struct handle *h = obj;
    while (h->head) {
        struct waiter *w = h->head;
        h->head = w->next;
        free_waiter(w);
    }
    if (h->ev >= 0)
        close(h->ev);
    if (h->ptr)
        DROPS[h->kind](h->ptr);
    if (h->mu)
        enif_mutex_destroy(h->mu);
}

static void handle_stop(ErlNifEnv *env, void *obj, ErlNifEvent event, int is_direct_call) {
    (void)env;
    (void)obj;
    (void)is_direct_call;
    close((int)event);
}

static void handle_down(ErlNifEnv *env, void *obj, ErlNifPid *pid, ErlNifMonitor *mon) {
    (void)pid;
    struct handle *h = obj;
    enif_mutex_lock(h->mu);
    for (struct waiter *w = h->head; w; w = w->next) {
        if (enif_compare_monitors(&w->mon, mon) == 0) {
            int was_head = unlink_waiter(h, w);
            free_waiter(w);
            if (was_head)
                advance(env, h);
            break;
        }
    }
    enif_mutex_unlock(h->mu);
}

/* select(H, Ref): queues the caller for H's readiness; the head of the
 * queue receives {select, H, Ref, ready_input}. */
NIF(nif_select) {
    struct handle *h = arg(env, argv[0], -1);
    if (!h || !FDS[h->kind] || !enif_is_ref(env, argv[1]))
        return enif_make_badarg(env);
    struct waiter *w = enif_alloc(sizeof *w);
    if (!w)
        return enif_raise_exception(env, A_enomem);
    w->next = NULL;
    w->env = enif_alloc_env();
    w->ref = enif_make_copy(w->env, argv[1]);
    enif_self(env, &w->pid);
    if (!lock_live(h)) {
        free_waiter(w);
        return closed(env);
    }
    if (enif_monitor_process(env, h, &w->pid, &w->mon) != 0) {
        enif_mutex_unlock(h->mu);
        free_waiter(w);
        return enif_make_badarg(env);
    }
    if (h->tail)
        h->tail->next = w;
    else
        h->head = w;
    h->tail = w;
    if (h->head == w) {
        int rc = arm(env, h);
        if (rc != 0) {
            h->head = h->tail = NULL;
            enif_demonitor_process(env, h, &w->mon);
            free_waiter(w);
            disarm(env, h);
            enif_mutex_unlock(h->mu);
            if (rc == SELECT_FAILED)
                return enif_raise_exception(env, A_select_failed);
            return fail(env, rc);
        }
    }
    enif_mutex_unlock(h->mu);
    return A_ok;
}

/* select_cancel(H, Ref): leaves H's queue, after a notification or in
 * place of one. A late notification may still arrive. */
NIF(nif_select_cancel) {
    struct handle *h = arg(env, argv[0], -1);
    if (!h || !enif_is_ref(env, argv[1]))
        return enif_make_badarg(env);
    enif_mutex_lock(h->mu);
    for (struct waiter *w = h->head; w; w = w->next) {
        if (enif_is_identical(w->ref, argv[1])) {
            int was_head = unlink_waiter(h, w);
            enif_demonitor_process(env, h, &w->mon);
            free_waiter(w);
            if (was_head)
                advance(env, h);
            break;
        }
    }
    enif_mutex_unlock(h->mu);
    return A_ok;
}

NIF(nif_drop) {
    struct handle *h = arg(env, argv[0], -1);
    if (!h)
        return enif_make_badarg(env);
    enif_mutex_lock(h->mu);
    void *p = detach(env, h);
    enif_mutex_unlock(h->mu);
    if (p)
        DROPS[h->kind](p);
    return A_ok;
}

/* ---- generated shapes ---- */

/* `<op>_wait(Op) -> ok | again | {error, _}` */
#define WAIT0(name, t) \
    NIF(name) { \
        struct handle *h = arg(env, argv[0], K_##t); \
        if (!h) \
            return enif_make_badarg(env); \
        if (!lock_live(h)) \
            return closed(env); \
        int rc = weida_##t##_wait(h->ptr, 0); \
        enif_mutex_unlock(h->mu); \
        return rc < 0 ? fail(env, rc) : A_ok; \
    }

/* `<op>_wait(Op) -> {ok, Value} | again | {error, _}` */
#define WAIT1(name, t, out_t, value) \
    NIF(name) { \
        struct handle *h = arg(env, argv[0], K_##t); \
        out_t *out = NULL; \
        if (!h) \
            return enif_make_badarg(env); \
        if (!lock_live(h)) \
            return closed(env); \
        int rc = weida_##t##_wait(h->ptr, 0, &out); \
        enif_mutex_unlock(h->mu); \
        return rc < 0 ? fail(env, rc) : value; \
    }

/* `<t>_<x>(H) -> binary() | undefined` (R7 strings out) */
#define TEXT(name, t, x) \
    NIF(name) { \
        struct handle *h = arg(env, argv[0], K_##t); \
        size_t len = 0; \
        if (!h) \
            return enif_make_badarg(env); \
        if (!lock_live(h)) \
            return closed(env); \
        const char *s = weida_##t##_##x(h->ptr, &len); \
        ERL_NIF_TERM v = cstr(env, s, len); \
        enif_mutex_unlock(h->mu); \
        return v; \
    }

/* `<t>_new() -> {ok, H}` and `<t>_generate() -> {ok, H}` */
#define CTOR0(name, t, c) \
    NIF(name) { \
        weida_##t *out = NULL; \
        int rc = c(&out); \
        return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_##t, out)); \
    }

/* `<t>_<m>(H) -> {ok, Out}` on a handle of kind `in`, creating kind `t` */
#define FROM1(name, in, c, t) \
    NIF(name) { \
        struct handle *h = arg(env, argv[0], K_##in); \
        weida_##t *out = NULL; \
        if (!h) \
            return enif_make_badarg(env); \
        if (!lock_live(h)) \
            return closed(env); \
        int rc = c(h->ptr, &out); \
        enif_mutex_unlock(h->mu); \
        return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_##t, out)); \
    }

/* A consuming function (R6) `<t>_<m>(H) -> {ok, Out}`: the handle is spent
 * on every path. */
#define CONSUME1(name, in, c, t) \
    NIF(name) { \
        struct handle *h = arg(env, argv[0], K_##in); \
        weida_##t *out = NULL; \
        if (!h) \
            return enif_make_badarg(env); \
        enif_mutex_lock(h->mu); \
        void *p = detach(env, h); \
        enif_mutex_unlock(h->mu); \
        if (!p) \
            return closed(env); \
        int rc = c(p, &out); \
        return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_##t, out)); \
    }

/* `<t>_<m>(H, String) -> {ok, Out}` */
#define FROM_STR(name, in, c, t) \
    NIF(name) { \
        struct handle *h = arg(env, argv[0], K_##in); \
        char *s = NULL; \
        weida_##t *out = NULL; \
        if (!h || !get_cstr(env, argv[1], &s)) \
            return enif_make_badarg(env); \
        if (!lock_live(h)) { \
            enif_free(s); \
            return closed(env); \
        } \
        int rc = c(h->ptr, s, &out); \
        enif_mutex_unlock(h->mu); \
        enif_free(s); \
        return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_##t, out)); \
    }

/* `<t>_<m>(A, B) -> {ok, Out}` on two handles, locked in argument order */
#define FROM2(name, ka, kb, c, t) \
    NIF(name) { \
        struct handle *a = arg(env, argv[0], K_##ka), *b = arg(env, argv[1], K_##kb); \
        weida_##t *out = NULL; \
        if (!a || !b) \
            return enif_make_badarg(env); \
        if (!lock_live(a)) \
            return closed(env); \
        if (!lock_live(b)) { \
            enif_mutex_unlock(a->mu); \
            return closed(env); \
        } \
        int rc = c(a->ptr, b->ptr, &out); \
        enif_mutex_unlock(b->mu); \
        enif_mutex_unlock(a->mu); \
        return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_##t, out)); \
    }

/* ---- the functions ---- */

NIF(nif_version) {
    const char *v = weida_version();
    return cstr(env, v, strlen(v));
}

/* strerror(Code): Code an integer of either sign or an R9 atom. */
NIF(nif_strerror) {
    int code = 0;
    if (!enif_get_int(env, argv[0], &code)) {
        size_t i = 0;
        while (i < NCODES && !enif_is_identical(argv[0], CODE_ATOMS[i]))
            i++;
        if (i == NCODES)
            return enif_make_badarg(env);
        code = CODES[i].code;
    }
    const char *s = weida_strerror(code);
    return cstr(env, s, strlen(s));
}

/* runtime_owned(#{worker_threads => N, shutdown_timeout_ms => Ms}) */
NIF(nif_runtime_owned) {
    ERL_NIF_TERM v;
    unsigned int threads = 0;
    ErlNifUInt64 ms = 0;
    if (!enif_is_map(env, argv[0]))
        return enif_make_badarg(env);
    int has_threads = enif_get_map_value(env, argv[0], A_worker_threads, &v);
    if (has_threads && !enif_get_uint(env, v, &threads))
        return enif_make_badarg(env);
    int has_ms = enif_get_map_value(env, argv[0], A_shutdown_timeout_ms, &v);
    if (has_ms && !enif_get_uint64(env, v, &ms))
        return enif_make_badarg(env);
    weida_runtime_config *cfg = NULL;
    weida_runtime *rt = NULL;
    int rc = weida_runtime_config_new(&cfg);
    if (rc == 0 && has_threads)
        rc = weida_runtime_config_set_worker_threads(cfg, threads);
    if (rc == 0 && has_ms)
        rc = weida_runtime_config_set_shutdown_timeout_ms(cfg, ms);
    if (rc == 0)
        rc = weida_runtime_owned(cfg, &rt);
    ERL_NIF_TERM result = rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_runtime, rt));
    weida_runtime_config_drop(cfg);
    return result;
}

FROM1(nif_runtime_listener, runtime, weida_runtime_listener, listener)
FROM2(nif_runtime_requester, runtime, client_tls, weida_runtime_requester, requester)
CONSUME1(nif_runtime_shutdown, runtime, weida_runtime_shutdown, runtime_shutdown_op)
WAIT0(nif_runtime_shutdown_op_wait, runtime_shutdown_op)

/* TLS values */

CTOR0(nif_trust_new, trust, weida_trust_new)

NIF(nif_trust_add_pin) {
    struct handle *h = arg(env, argv[0], K_trust);
    char *fp = NULL;
    if (!h || !get_cstr(env, argv[1], &fp))
        return enif_make_badarg(env);
    if (!lock_live(h)) {
        enif_free(fp);
        return closed(env);
    }
    int rc = weida_trust_add_pin(h->ptr, fp);
    enif_mutex_unlock(h->mu);
    enif_free(fp);
    return rc < 0 ? fail(env, rc) : A_ok;
}

NIF(nif_trust_add_anchor) {
    struct handle *h = arg(env, argv[0], K_trust);
    ErlNifBinary pem;
    if (!h || !enif_inspect_iolist_as_binary(env, argv[1], &pem))
        return enif_make_badarg(env);
    if (!lock_live(h))
        return closed(env);
    int rc = weida_trust_add_anchor(h->ptr, pem.data, pem.size);
    enif_mutex_unlock(h->mu);
    return rc < 0 ? fail(env, rc) : A_ok;
}

NIF(nif_trust_add_anchor_file) {
    struct handle *h = arg(env, argv[0], K_trust);
    char *path = NULL;
    if (!h || !get_cstr(env, argv[1], &path))
        return enif_make_badarg(env);
    if (!lock_live(h)) {
        enif_free(path);
        return closed(env);
    }
    int rc = weida_trust_add_anchor_file(h->ptr, path);
    enif_mutex_unlock(h->mu);
    enif_free(path);
    return rc < 0 ? fail(env, rc) : A_ok;
}

CTOR0(nif_identity_generate, identity, weida_identity_generate)

/* identity_generate_for([Name]): every name a binary or iolist without NUL. */
NIF(nif_identity_generate_for) {
    unsigned int n = 0;
    weida_identity *out = NULL;
    if (!enif_get_list_length(env, argv[0], &n))
        return enif_make_badarg(env);
    char **names = enif_alloc(sizeof *names * (n ? n : 1));
    if (!names)
        return enif_raise_exception(env, A_enomem);
    unsigned int got = 0;
    ERL_NIF_TERM list = argv[0], head;
    while (got < n && enif_get_list_cell(env, list, &head, &list) &&
           get_cstr(env, head, &names[got]))
        got++;
    int rc = got < n ? 0 : weida_identity_generate_for((const char *const *)names, n, &out);
    for (unsigned int i = 0; i < got; i++)
        enif_free(names[i]);
    enif_free(names);
    if (got < n)
        return enif_make_badarg(env);
    return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_identity, out));
}

NIF(nif_identity_from_pem_file) {
    char *path = NULL;
    weida_identity *out = NULL;
    if (!get_cstr(env, argv[0], &path))
        return enif_make_badarg(env);
    int rc = weida_identity_from_pem_file(path, &out);
    enif_free(path);
    return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_identity, out));
}

NIF(nif_identity_from_pem_files) {
    char *cert = NULL, *key = NULL;
    weida_identity *out = NULL;
    if (!get_cstr(env, argv[0], &cert))
        return enif_make_badarg(env);
    if (!get_cstr(env, argv[1], &key)) {
        enif_free(cert);
        return enif_make_badarg(env);
    }
    int rc = weida_identity_from_pem_files(cert, key, &out);
    enif_free(cert);
    enif_free(key);
    return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_identity, out));
}

NIF(nif_identity_from_pem) {
    ErlNifBinary cert, key;
    weida_identity *out = NULL;
    if (!enif_inspect_iolist_as_binary(env, argv[0], &cert) ||
        !enif_inspect_iolist_as_binary(env, argv[1], &key))
        return enif_make_badarg(env);
    int rc = weida_identity_from_pem(cert.data, cert.size, key.data, key.size, &out);
    return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_identity, out));
}

NIF(nif_identity_fingerprint) {
    struct handle *h = arg(env, argv[0], K_identity);
    const char *fp = NULL;
    if (!h)
        return enif_make_badarg(env);
    if (!lock_live(h))
        return closed(env);
    int rc = weida_identity_fingerprint(h->ptr, &fp);
    ERL_NIF_TERM v = rc < 0 ? fail(env, rc) : ok_tuple(env, cstr(env, fp, strlen(fp)));
    enif_mutex_unlock(h->mu);
    return v;
}

/* `f(Identity) -> {ok, Pem}`: copies the weida_pem into a binary and drops
 * (wipes) it. */
static ERL_NIF_TERM pem_out(ErlNifEnv *env, ERL_NIF_TERM t,
                            int (*f)(const weida_identity *, weida_pem **)) {
    struct handle *h = arg(env, t, K_identity);
    weida_pem *p = NULL;
    if (!h)
        return enif_make_badarg(env);
    if (!lock_live(h))
        return closed(env);
    int rc = f(h->ptr, &p);
    ERL_NIF_TERM v;
    if (rc < 0) {
        v = fail(env, rc);
    } else {
        size_t len = weida_pem_len(p);
        ERL_NIF_TERM bin;
        unsigned char *d = enif_make_new_binary(env, len, &bin);
        if (len)
            memcpy(d, weida_pem_data(p), len);
        weida_pem_drop(p);
        v = ok_tuple(env, bin);
    }
    enif_mutex_unlock(h->mu);
    return v;
}

NIF(nif_identity_certificate_pem) {
    return pem_out(env, argv[0], weida_identity_certificate_pem);
}

NIF(nif_identity_to_pem) {
    return pem_out(env, argv[0], weida_identity_to_pem);
}

FROM1(nif_client_tls_new, trust, weida_client_tls_new, client_tls)
FROM1(nif_server_tls_new, identity, weida_server_tls_new, server_tls)

/* `<t>_<m>(A, B) -> ok` on two handles, locked in argument order */
#define ACT2(name, ka, kb, c) \
    NIF(name) { \
        struct handle *a = arg(env, argv[0], K_##ka), *b = arg(env, argv[1], K_##kb); \
        if (!a || !b) \
            return enif_make_badarg(env); \
        if (!lock_live(a)) \
            return closed(env); \
        if (!lock_live(b)) { \
            enif_mutex_unlock(a->mu); \
            return closed(env); \
        } \
        int rc = c(a->ptr, b->ptr); \
        enif_mutex_unlock(b->mu); \
        enif_mutex_unlock(a->mu); \
        return rc < 0 ? fail(env, rc) : A_ok; \
    }

ACT2(nif_client_tls_set_identity, client_tls, identity, weida_client_tls_set_identity)
ACT2(nif_server_tls_require_client, server_tls, trust, weida_server_tls_require_client)

NIF(nif_server_tls_require_any_key) {
    struct handle *h = arg(env, argv[0], K_server_tls);
    if (!h)
        return enif_make_badarg(env);
    if (!lock_live(h))
        return closed(env);
    int rc = weida_server_tls_require_any_key(h->ptr);
    enif_mutex_unlock(h->mu);
    return rc < 0 ? fail(env, rc) : A_ok;
}

/* listener and bindings */

NIF(nif_listener_bind_quic) {
    struct handle *l = arg(env, argv[0], K_listener), *tls = arg(env, argv[2], K_server_tls);
    char *addr = NULL;
    weida_listener_bind_quic_op *op = NULL;
    if (!l || !tls || !get_cstr(env, argv[1], &addr))
        return enif_make_badarg(env);
    if (!lock_live(l)) {
        enif_free(addr);
        return closed(env);
    }
    if (!lock_live(tls)) {
        enif_mutex_unlock(l->mu);
        enif_free(addr);
        return closed(env);
    }
    int rc = weida_listener_bind_quic(l->ptr, addr, tls->ptr, &op);
    enif_mutex_unlock(tls->mu);
    enif_mutex_unlock(l->mu);
    enif_free(addr);
    return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_listener_bind_quic_op, op));
}

WAIT1(nif_listener_bind_quic_op_wait, listener_bind_quic_op, weida_binding,
      ok_tuple(env, make_handle(env, K_binding, out)))
FROM_STR(nif_listener_bind_inproc, listener, weida_listener_bind_inproc, local_binding)
FROM_STR(nif_listener_replier, listener, weida_listener_replier, replier)
TEXT(nif_binding_local_addr, binding, local_addr)
TEXT(nif_local_binding_bus, local_binding, bus)

/* replier and incoming request */

TEXT(nif_replier_path, replier, path)

NIF(nif_replier_accept) {
    struct handle *h = arg(env, argv[0], K_replier);
    weida_incoming_request *out = NULL;
    if (!h)
        return enif_make_badarg(env);
    if (!lock_live(h))
        return closed(env);
    int rc = weida_replier_accept(h->ptr, 0, &out);
    enif_mutex_unlock(h->mu);
    return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_incoming_request, out));
}

/* `<t>_meta(H) -> {ok, Meta}` */
#define META(name, t) \
    NIF(name) { \
        struct handle *h = arg(env, argv[0], K_##t); \
        weida_incoming_meta *m = NULL; \
        if (!h) \
            return enif_make_badarg(env); \
        if (!lock_live(h)) \
            return closed(env); \
        int rc = weida_##t##_meta(h->ptr, &m); \
        enif_mutex_unlock(h->mu); \
        return rc < 0 ? fail(env, rc) : ok_tuple(env, meta_value(env, m)); \
    }

META(nif_incoming_request_meta, incoming_request)
FROM1(nif_incoming_request_take_body, incoming_request, weida_incoming_request_take_body,
      incoming_transfer)

/* incoming_request_reply(Q, #{content_type => Binary}): consumes Q. */
NIF(nif_incoming_request_reply) {
    struct handle *h = arg(env, argv[0], K_incoming_request);
    ERL_NIF_TERM v;
    char *content_type = NULL;
    if (!h || !enif_is_map(env, argv[1]))
        return enif_make_badarg(env);
    if (enif_get_map_value(env, argv[1], A_content_type, &v) && !get_cstr(env, v, &content_type))
        return enif_make_badarg(env);
    enif_mutex_lock(h->mu);
    weida_incoming_request *q = detach(env, h);
    enif_mutex_unlock(h->mu);
    if (!q) {
        enif_free(content_type);
        return closed(env);
    }
    weida_transfer_meta *meta = NULL;
    weida_incoming_request_reply_op *op = NULL;
    int rc = 0;
    if (content_type) {
        rc = weida_transfer_meta_new(&meta);
        if (rc == 0)
            rc = weida_transfer_meta_set_content_type(meta, content_type);
        enif_free(content_type);
    }
    if (rc == 0)
        rc = weida_incoming_request_reply(q, meta, &op);
    else
        weida_incoming_request_drop(q); /* consumed on every path */
    ERL_NIF_TERM result =
        rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_incoming_request_reply_op, op));
    weida_transfer_meta_drop(meta);
    return result;
}

WAIT1(nif_incoming_request_reply_op_wait, incoming_request_reply_op, weida_outgoing_transfer,
      ok_tuple(env, make_handle(env, K_outgoing_transfer, out)))

/* requester */

NIF(nif_requester_connect) {
    struct handle *h = arg(env, argv[0], K_requester);
    char *url = NULL;
    weida_requester_connect_op *op = NULL;
    if (!h || !get_cstr(env, argv[1], &url))
        return enif_make_badarg(env);
    if (!lock_live(h)) {
        enif_free(url);
        return closed(env);
    }
    int rc = weida_requester_connect(h->ptr, url, &op);
    enif_mutex_unlock(h->mu);
    enif_free(url);
    return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_requester_connect_op, op));
}

WAIT0(nif_requester_connect_op_wait, requester_connect_op)

NIF(nif_requester_request) {
    struct handle *h = arg(env, argv[0], K_requester);
    ErlNifBinary body;
    weida_requester_request_op *op = NULL;
    if (!h || !enif_inspect_iolist_as_binary(env, argv[1], &body))
        return enif_make_badarg(env);
    if (!lock_live(h))
        return closed(env);
    int rc = weida_requester_request(h->ptr, body.data, body.size, &op);
    enif_mutex_unlock(h->mu);
    return rc < 0 ? fail(env, rc) : ok_tuple(env, make_handle(env, K_requester_request_op, op));
}

WAIT1(nif_requester_request_op_wait, requester_request_op, weida_incoming_transfer,
      ok_tuple(env, make_handle(env, K_incoming_transfer, out)))

/* incoming transfer */

META(nif_incoming_transfer_meta, incoming_transfer)

/* incoming_transfer_read(T, Max) -> {ok, Binary} | eof | again | {error, _} */
NIF(nif_incoming_transfer_read) {
    struct handle *h = arg(env, argv[0], K_incoming_transfer);
    ErlNifUInt64 cap = 0;
    ErlNifBinary buf;
    size_t n = 0;
    if (!h || !enif_get_uint64(env, argv[1], &cap) || cap > SIZE_MAX)
        return enif_make_badarg(env);
    if (!enif_alloc_binary((size_t)cap, &buf))
        return enif_raise_exception(env, A_enomem);
    if (!lock_live(h)) {
        enif_release_binary(&buf);
        return closed(env);
    }
    int rc = weida_incoming_transfer_read(h->ptr, buf.data, buf.size, 0, &n);
    enif_mutex_unlock(h->mu);
    if (rc < 0 || n == 0) {
        enif_release_binary(&buf);
        return rc < 0 ? fail(env, rc) : A_eof;
    }
    if (n < buf.size && !enif_realloc_binary(&buf, n)) {
        enif_release_binary(&buf);
        return enif_raise_exception(env, A_enomem);
    }
    return ok_tuple(env, enif_make_binary(env, &buf));
}

/* incoming_transfer_collect(T, Max): consumes T. */
NIF(nif_incoming_transfer_collect) {
    struct handle *h = arg(env, argv[0], K_incoming_transfer);
    ErlNifUInt64 max = 0;
    weida_incoming_transfer_collect_op *op = NULL;
    if (!h || !enif_get_uint64(env, argv[1], &max))
        return enif_make_badarg(env);
    enif_mutex_lock(h->mu);
    weida_incoming_transfer *t = detach(env, h);
    enif_mutex_unlock(h->mu);
    if (!t)
        return closed(env);
    int rc = weida_incoming_transfer_collect(t, max, &op);
    return rc < 0 ? fail(env, rc)
                  : ok_tuple(env, make_handle(env, K_incoming_transfer_collect_op, op));
}

WAIT1(nif_incoming_transfer_collect_op_wait, incoming_transfer_collect_op, weida_msg,
      msg_value(env, out))

/* outgoing transfer and delivery */

/* outgoing_transfer_write(T, Data) -> {ok, Count} | again | {error, _} */
NIF(nif_outgoing_transfer_write) {
    struct handle *h = arg(env, argv[0], K_outgoing_transfer);
    ErlNifBinary data;
    size_t n = 0;
    if (!h || !enif_inspect_iolist_as_binary(env, argv[1], &data))
        return enif_make_badarg(env);
    if (!lock_live(h))
        return closed(env);
    int rc = weida_outgoing_transfer_write(h->ptr, data.data, data.size, 0, &n);
    enif_mutex_unlock(h->mu);
    return rc < 0 ? fail(env, rc) : ok_tuple(env, enif_make_uint64(env, n));
}

CONSUME1(nif_outgoing_transfer_finish, outgoing_transfer, weida_outgoing_transfer_finish, delivery)
CONSUME1(nif_delivery_delivered, delivery, weida_delivery_delivered, delivery_delivered_op)
WAIT0(nif_delivery_delivered_op_wait, delivery_delivered_op)

/* ---- the library ---- */

static ERL_NIF_TERM atom(ErlNifEnv *env, const char *name) {
    return enif_make_atom(env, name);
}

static int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info) {
    (void)priv;
    (void)info;
    ErlNifResourceTypeInit init = {
        .dtor = handle_dtor,
        .stop = handle_stop,
        .down = handle_down,
    };
    HANDLE = enif_open_resource_type_x(env, "weida_handle", &init, ERL_NIF_RT_CREATE, NULL);
    if (!HANDLE)
        return 1;
    A_ok = atom(env, "ok");
    A_error = atom(env, "error");
    A_again = atom(env, "again");
    A_eof = atom(env, "eof");
    A_undefined = atom(env, "undefined");
    A_select = atom(env, "select");
    A_ready_input = atom(env, "ready_input");
    A_weida_closed = atom(env, "weida_closed");
    A_enomem = atom(env, "enomem");
    A_select_failed = atom(env, "weida_select_failed");
    A_code = atom(env, "code");
    A_message = atom(env, "message");
    A_loss_cause = atom(env, "loss_cause");
    A_fingerprint = atom(env, "fingerprint");
    A_max = atom(env, "max");
    A_os_code = atom(env, "os_code");
    A_endpoint = atom(env, "endpoint");
    A_content_type = atom(env, "content_type");
    A_topic = atom(env, "topic");
    A_peer_fingerprint = atom(env, "peer_fingerprint");
    A_peer_chain = atom(env, "peer_chain");
    A_content_len = atom(env, "content_len");
    A_sequence = atom(env, "sequence");
    A_payload = atom(env, "payload");
    A_meta = atom(env, "meta");
    A_worker_threads = atom(env, "worker_threads");
    A_shutdown_timeout_ms = atom(env, "shutdown_timeout_ms");
    for (size_t i = 0; i < NCODES; i++)
        CODE_ATOMS[i] = atom(env, CODES[i].name);
    for (size_t i = 1; i < NLOSSES; i++)
        LOSS_ATOMS[i] = atom(env, LOSSES[i]);
    return 0;
}

static ErlNifFunc nif_funcs[] = {
    {"version", 0, nif_version, 0},
    {"strerror", 1, nif_strerror, 0},
    {"drop", 1, nif_drop, 0},
    {"select", 2, nif_select, 0},
    {"select_cancel", 2, nif_select_cancel, 0},
    {"runtime_owned", 1, nif_runtime_owned, 0},
    {"runtime_listener", 1, nif_runtime_listener, 0},
    {"runtime_requester", 2, nif_runtime_requester, 0},
    {"runtime_shutdown", 1, nif_runtime_shutdown, 0},
    {"runtime_shutdown_op_wait", 1, nif_runtime_shutdown_op_wait, 0},
    {"trust_new", 0, nif_trust_new, 0},
    {"trust_add_pin", 2, nif_trust_add_pin, 0},
    {"trust_add_anchor", 2, nif_trust_add_anchor, 0},
    {"trust_add_anchor_file", 2, nif_trust_add_anchor_file, 0},
    {"identity_generate", 0, nif_identity_generate, 0},
    {"identity_generate_for", 1, nif_identity_generate_for, 0},
    {"identity_from_pem", 2, nif_identity_from_pem, 0},
    {"identity_from_pem_file", 1, nif_identity_from_pem_file, 0},
    {"identity_from_pem_files", 2, nif_identity_from_pem_files, 0},
    {"identity_certificate_pem", 1, nif_identity_certificate_pem, 0},
    {"identity_to_pem", 1, nif_identity_to_pem, 0},
    {"identity_fingerprint", 1, nif_identity_fingerprint, 0},
    {"client_tls_new", 1, nif_client_tls_new, 0},
    {"server_tls_new", 1, nif_server_tls_new, 0},
    {"client_tls_set_identity", 2, nif_client_tls_set_identity, 0},
    {"server_tls_require_client", 2, nif_server_tls_require_client, 0},
    {"server_tls_require_any_key", 1, nif_server_tls_require_any_key, 0},
    {"listener_bind_quic", 3, nif_listener_bind_quic, 0},
    {"listener_bind_quic_op_wait", 1, nif_listener_bind_quic_op_wait, 0},
    {"listener_bind_inproc", 2, nif_listener_bind_inproc, 0},
    {"listener_replier", 2, nif_listener_replier, 0},
    {"binding_local_addr", 1, nif_binding_local_addr, 0},
    {"local_binding_bus", 1, nif_local_binding_bus, 0},
    {"replier_path", 1, nif_replier_path, 0},
    {"replier_accept", 1, nif_replier_accept, 0},
    {"incoming_request_meta", 1, nif_incoming_request_meta, 0},
    {"incoming_request_take_body", 1, nif_incoming_request_take_body, 0},
    {"incoming_request_reply", 2, nif_incoming_request_reply, 0},
    {"incoming_request_reply_op_wait", 1, nif_incoming_request_reply_op_wait, 0},
    {"requester_connect", 2, nif_requester_connect, 0},
    {"requester_connect_op_wait", 1, nif_requester_connect_op_wait, 0},
    {"requester_request", 2, nif_requester_request, 0},
    {"requester_request_op_wait", 1, nif_requester_request_op_wait, 0},
    {"incoming_transfer_meta", 1, nif_incoming_transfer_meta, 0},
    {"incoming_transfer_read", 2, nif_incoming_transfer_read, 0},
    {"incoming_transfer_collect", 2, nif_incoming_transfer_collect, 0},
    {"incoming_transfer_collect_op_wait", 1, nif_incoming_transfer_collect_op_wait, 0},
    {"outgoing_transfer_write", 2, nif_outgoing_transfer_write, 0},
    {"outgoing_transfer_finish", 1, nif_outgoing_transfer_finish, 0},
    {"delivery_delivered", 1, nif_delivery_delivered, 0},
    {"delivery_delivered_op_wait", 1, nif_delivery_delivered_op_wait, 0},
};

ERL_NIF_INIT(weida_nif, nif_funcs, load, NULL, NULL, NULL)
