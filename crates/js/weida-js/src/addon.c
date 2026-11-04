/*
 * The Node-API module over libweida (docs/libraries/weida-js.md): one JS
 * function per weida.h function of the slice, named camelCase without the
 * `weida_` prefix.
 *
 * Every call passes timeout_ms = 0, so the JS thread never blocks: a try or
 * an op wait that would block returns the frozen `AGAIN` object, and
 * lib/wait.js watches the handle's descriptor through waiter.c. Any other
 * failure throws an instance of the class registered with setErrorClass,
 * carrying the thread-local record read in the same call (R9).
 *
 * A C pointer travels as a type-tagged napi_external around `struct wh`;
 * its finalizer drops the pointer unless `drop()` or a consuming function
 * (R6) already cleared it. A cleared wrapper throws WEIDA_ECLOSED without
 * calling C.
 */
#include "addon.h"

#include <stdlib.h>
#include <string.h>

/* ---- handles ---- */

typedef void (*drop_fn)(void *);

struct wh {
    void *ptr;
    drop_fn drop;
};

static const napi_type_tag WH_TAG = {0x77656964612d6a73ULL, 0x68616e646c650001ULL};

#define DROP(t) \
    static void drop_##t(void *p) { weida_##t##_drop(p); }
DROP(runtime_config)
DROP(runtime)
DROP(runtime_shutdown_op)
DROP(runtime_drain_op)
DROP(listener)
DROP(listener_bind_quic_op)
DROP(listener_bind_quic_socket_op)
DROP(listener_bind_relay_op)
DROP(binding)
DROP(binding_close_op)
DROP(binding_hand_over_op)
DROP(binding_take_over_op)
DROP(handoff)
DROP(local_binding)
DROP(unix_binding)
DROP(pipe_binding)
DROP(replier)
DROP(incoming_request)
DROP(incoming_request_reply_op)
DROP(incoming_request_refuse_op)
DROP(incoming_request_canceled_op)
DROP(relay)
DROP(relay_request)
DROP(relay_request_admit_op)
DROP(relay_request_refuse_op)
DROP(requester)
DROP(requester_connect_op)
DROP(requester_request_op)
DROP(requester_open_op)
DROP(reply_stream)
DROP(reply_stream_recv_op)
DROP(peer_events)
DROP(trust)
DROP(identity)
DROP(client_tls)
DROP(server_tls)
DROP(identity_source)
DROP(weak_identity_source)
DROP(trust_source)
DROP(weak_trust_source)
DROP(identity_events)
DROP(files_options)
DROP(resolver)
DROP(resolved)
DROP(pusher)
DROP(pusher_connect_op)
DROP(pusher_send_op)
DROP(pusher_open_op)
DROP(puller)
DROP(publisher)
DROP(fan_out)
DROP(fan_out_write_within_op)
DROP(subscriber)
DROP(subscriber_connect_op)
DROP(subscriber_subscribe_op)
DROP(subscriber_unsubscribe_op)
DROP(subscriber_grant_op)
DROP(paired)
DROP(paired_connect_op)
DROP(paired_send_op)
DROP(paired_open_op)
DROP(surveyor)
DROP(surveyor_connect_op)
DROP(surveyor_survey_op)
DROP(survey_run)
DROP(respondent)
DROP(bus_member)
DROP(bus_member_connect_op)
DROP(bus_member_send_op)
DROP(radio)
DROP(segment)
DROP(segment_terms)
DROP(join_terms)
DROP(dish)
DROP(dish_connect_op)
DROP(dish_join_op)
DROP(dish_leave_op)
DROP(cursors)
DROP(reporter)
DROP(reporter_report_op)
DROP(reporter_finish_op)
DROP(peer)
DROP(peer_connect_op)
DROP(peer_open_flow_op)
DROP(acceptor)
DROP(flow_meta)
DROP(flow)
DROP(incoming_flow)
DROP(transfer_meta)
DROP(incoming_transfer)
DROP(incoming_transfer_collect_op)
DROP(outgoing_transfer)
DROP(delivery)
DROP(delivery_delivered_op)
DROP(limits)
DROP(guarantee_set)
DROP(reconnect_policy)

static void finalize_wh(napi_env env, void *data, void *hint) {
    (void)env;
    (void)hint;
    struct wh *h = data;
    if (h->ptr)
        h->drop(h->ptr);
    free(h);
}

/* Wraps `ptr`; on failure drops it and throws. */
static napi_value wrap(napi_env env, void *ptr, drop_fn drop) {
    struct wh *h = malloc(sizeof *h);
    napi_value v;
    if (!h) {
        drop(ptr);
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    h->ptr = ptr;
    h->drop = drop;
    if (napi_create_external(env, h, finalize_wh, NULL, &v) != napi_ok) {
        drop(ptr);
        free(h);
        return throw_napi(env);
    }
    /* From here on the finalizer owns `h`. */
    if (napi_type_tag_object(env, v, &WH_TAG) != napi_ok)
        return throw_napi(env);
    return v;
}

/* The wrapper of a handle of the kind `drop` releases (any kind when
 * `drop` is NULL), or NULL with a TypeError thrown. */
static struct wh *unwrap(napi_env env, napi_value v, drop_fn drop) {
    napi_valuetype type;
    bool tagged = false;
    void *data = NULL;
    if (napi_typeof(env, v, &type) != napi_ok || type != napi_external ||
        napi_check_object_type_tag(env, v, &WH_TAG, &tagged) != napi_ok || !tagged ||
        napi_get_value_external(env, v, &data) != napi_ok) {
        throw_type(env, "not a weida handle");
        return NULL;
    }
    struct wh *h = data;
    if (drop && h->drop != drop) {
        throw_type(env, "a weida handle of another kind");
        return NULL;
    }
    return h;
}

static napi_value throw_code(napi_env env, int code);

/* The live pointer of a handle, or NULL with an error thrown. */
static void *live(napi_env env, napi_value v, drop_fn drop) {
    struct wh *h = unwrap(env, v, drop);
    if (!h)
        return NULL;
    if (!h->ptr) {
        throw_code(env, WEIDA_ECLOSED);
        return NULL;
    }
    return h->ptr;
}

/* Takes the pointer out of a handle a consuming function (R6) is given:
 * the wrapper is cleared before C runs, so it is spent on every path. */
static void *take(napi_env env, napi_value v, drop_fn drop) {
    struct wh *h = unwrap(env, v, drop);
    if (!h)
        return NULL;
    void *p = h->ptr;
    h->ptr = NULL;
    if (!p)
        throw_code(env, WEIDA_ECLOSED);
    return p;
}

/* An optional handle: null and undefined are NULL (R7 defaults). */
static bool optional(napi_env env, napi_value v, drop_fn drop, void **out) {
    napi_valuetype type;
    if (napi_typeof(env, v, &type) != napi_ok) {
        throw_napi(env);
        return false;
    }
    if (type == napi_null || type == napi_undefined) {
        *out = NULL;
        return true;
    }
    *out = live(env, v, drop);
    return *out != NULL;
}

/* ---- errors ---- */

struct addon *addon_of(napi_env env) {
    void *data = NULL;
    napi_get_instance_data(env, &data);
    return data;
}

napi_value throw_napi(napi_env env) {
    bool pending = false;
    napi_is_exception_pending(env, &pending);
    if (!pending) {
        const napi_extended_error_info *info = NULL;
        napi_get_last_error_info(env, &info);
        napi_throw_error(env, NULL,
                         info && info->error_message ? info->error_message : "Node-API call failed");
    }
    return NULL;
}

napi_value throw_type(napi_env env, const char *message) {
    napi_throw_type_error(env, NULL, message);
    return NULL;
}

static napi_value throw_range(napi_env env, const char *message) {
    napi_throw_range_error(env, NULL, message);
    return NULL;
}

/* The R9 constant name of a status code (positive), or NULL. */
static const char *code_name(int code) {
    switch (code) {
#define NAME(c) \
    case WEIDA_##c: \
        return "WEIDA_" #c;
        NAME(ERUNTIME)
        NAME(EINVALID_ADDRESS)
        NAME(EINVALID_ENDPOINT_PATH)
        NAME(EINVALID_FINGERPRINT)
        NAME(EALREADY_REGISTERED)
        NAME(ENOT_CONNECTED)
        NAME(ECONNECTION_LOST)
        NAME(ENEGOTIATION)
        NAME(EPROTOCOL)
        NAME(EREJECTED)
        NAME(EUNKNOWN_ENDPOINT)
        NAME(EUNSUPPORTED)
        NAME(ENO_PARKED_CONNECTION)
        NAME(ENO_REPLY)
        NAME(ECANCELED)
        NAME(EEXPIRED)
        NAME(EINDETERMINATE)
        NAME(ELIMIT_EXCEEDED)
        NAME(EDATAGRAMS_UNAVAILABLE)
        NAME(ETOO_LARGE)
        NAME(ETLS)
        NAME(EUNTRUSTED)
        NAME(EIO)
        NAME(ETRANSPORT)
        NAME(EAGAIN)
        NAME(EINTR)
        NAME(EINVAL)
        NAME(ECLOSED)
        NAME(EPANIC)
#undef NAME
    default:
        return NULL;
    }
}

static const char *loss_name(int cause) {
    switch (cause) {
#define NAME(c) \
    case WEIDA_##c: \
        return "WEIDA_" #c;
        NAME(LOSS_IDLE_TIMEOUT)
        NAME(LOSS_PEER_CLOSED)
        NAME(LOSS_PEER_RESTARTING)
        NAME(LOSS_LOCALLY_CLOSED)
        NAME(LOSS_RESET)
        NAME(LOSS_TRANSPORT_ERROR)
#undef NAME
    default:
        return NULL;
    }
}

static const char *peer_event_name(int kind) {
    switch (kind) {
#define NAME(c) \
    case WEIDA_##c: \
        return "WEIDA_" #c;
        NAME(PEER_EVENT_CONNECTED)
        NAME(PEER_EVENT_LOST)
        NAME(PEER_EVENT_RETRYING)
        NAME(PEER_EVENT_GAVE_UP)
        NAME(PEER_EVENT_MISSED)
#undef NAME
    default:
        return NULL;
    }
}

static const char *received_name(int kind) {
    switch (kind) {
#define NAME(c) \
    case WEIDA_##c: \
        return "WEIDA_" #c;
        NAME(RECEIVED_SEGMENT)
        NAME(RECEIVED_DATAGRAM)
#undef NAME
    default:
        return NULL;
    }
}

static const char *give_up_name(int why) {
    switch (why) {
#define NAME(c) \
    case WEIDA_##c: \
        return "WEIDA_" #c;
        NAME(GIVE_UP_POLICY)
        NAME(GIVE_UP_PEER_CHANGED)
        NAME(GIVE_UP_FAILED)
#undef NAME
    default:
        return NULL;
    }
}

static const char *identity_event_name(int kind) {
    switch (kind) {
#define NAME(c) \
    case WEIDA_##c: \
        return "WEIDA_" #c;
        NAME(IDENTITY_EVENT_LOADED)
        NAME(IDENTITY_EVENT_RENEWED)
        NAME(IDENTITY_EVENT_KEY_CHANGED)
        NAME(IDENTITY_EVENT_RENEWAL_FAILED)
        NAME(IDENTITY_EVENT_HANDOFF_STOLEN)
        NAME(IDENTITY_EVENT_TRUST_REFRESHED)
        NAME(IDENTITY_EVENT_MISSED)
#undef NAME
    default:
        return NULL;
    }
}

static const char *acknowledgement_name(int level) {
    switch (level) {
#define NAME(c) \
    case WEIDA_##c: \
        return "WEIDA_" #c;
        NAME(ACKNOWLEDGEMENT_NONE)
        NAME(ACKNOWLEDGEMENT_TRANSPORT_RECEIPT)
        NAME(ACKNOWLEDGEMENT_ACCEPTED)
        NAME(ACKNOWLEDGEMENT_STORED)
        NAME(ACKNOWLEDGEMENT_REPLICATED)
        NAME(ACKNOWLEDGEMENT_PROCESSED)
#undef NAME
    default:
        return NULL;
    }
}

static const char *report_mode_name(int mode) {
    switch (mode) {
#define NAME(c) \
    case WEIDA_##c: \
        return "WEIDA_" #c;
        NAME(REPORT_MODE_PROGRESS)
        NAME(REPORT_MODE_FINAL_ONLY)
#undef NAME
    default:
        return NULL;
    }
}

static const char *incoming_name(int kind) {
    switch (kind) {
#define NAME(c) \
    case WEIDA_##c: \
        return "WEIDA_" #c;
        NAME(INCOMING_STREAM)
        NAME(INCOMING_EXCHANGE)
        NAME(INCOMING_SUBSCRIBED)
        NAME(INCOMING_CREDIT)
        NAME(INCOMING_UNSUBSCRIBED)
        NAME(INCOMING_FLOW)
#undef NAME
    default:
        return NULL;
    }
}

static napi_value js_null(napi_env env) {
    napi_value v;
    napi_get_null(env, &v);
    return v;
}

static napi_value js_undefined(napi_env env) {
    napi_value v;
    napi_get_undefined(env, &v);
    return v;
}

static napi_value js_bool(napi_env env, bool b) {
    napi_value v;
    napi_get_boolean(env, b, &v);
    return v;
}

static napi_value js_string(napi_env env, const char *s, size_t len) {
    napi_value v;
    if (!s)
        return js_null(env);
    if (napi_create_string_utf8(env, s, len, &v) != napi_ok)
        return throw_napi(env);
    return v;
}

static napi_value js_int64(napi_env env, int64_t n) {
    napi_value v;
    if (napi_create_int64(env, n, &v) != napi_ok)
        return throw_napi(env);
    return v;
}

/* The error object of `code` (positive). With `record`, the thread-local
 * record of the call that just failed on this thread fills it. */
static napi_value make_error(napi_env env, int code, bool record) {
    const char *message = NULL, *fingerprint = NULL, *name = code_name(code), *loss = NULL;
    int64_t max = -1;
    int os_code = 0;
    if (record) {
        message = weida_last_error_message();
        loss = loss_name(weida_last_error_loss_cause());
        fingerprint = weida_last_error_fingerprint();
        max = weida_last_error_max();
        os_code = weida_last_error_os_code();
    }
    if (!message)
        message = weida_strerror(code);

    /* Every string becomes a JS value before any JS runs. */
    napi_value msg = js_string(env, message, NAPI_AUTO_LENGTH);
    napi_value v_code = js_string(env, name, NAPI_AUTO_LENGTH);
    napi_value v_loss = js_string(env, loss, NAPI_AUTO_LENGTH);
    napi_value v_fp = js_string(env, fingerprint, NAPI_AUTO_LENGTH);
    napi_value v_max = max < 0 ? js_null(env) : js_int64(env, max);
    napi_value v_os = os_code == 0 ? js_null(env) : js_int64(env, os_code);
    napi_value v_errno = js_int64(env, code);
    if (!msg || !v_code || !v_loss || !v_fp || !v_max || !v_os || !v_errno)
        return NULL;

    napi_value err = NULL, cls = NULL;
    struct addon *a = addon_of(env);
    if (a && a->error_class)
        napi_get_reference_value(env, a->error_class, &cls);
    if (cls) {
        if (napi_new_instance(env, cls, 1, &msg, &err) != napi_ok)
            return throw_napi(env);
    } else if (napi_create_error(env, NULL, msg, &err) != napi_ok) {
        return throw_napi(env);
    }
    if (napi_set_named_property(env, err, "code", v_code) != napi_ok ||
        napi_set_named_property(env, err, "errno", v_errno) != napi_ok ||
        napi_set_named_property(env, err, "lossCause", v_loss) != napi_ok ||
        napi_set_named_property(env, err, "fingerprint", v_fp) != napi_ok ||
        napi_set_named_property(env, err, "max", v_max) != napi_ok ||
        napi_set_named_property(env, err, "osCode", v_os) != napi_ok)
        return throw_napi(env);
    return err;
}

napi_value throw_rc(napi_env env, int rc) {
    napi_value err = make_error(env, -rc, true);
    if (err)
        napi_throw(env, err);
    return NULL;
}

/* An error no C call reported, so without a record. */
static napi_value throw_code(napi_env env, int code) {
    napi_value err = make_error(env, code, false);
    if (err)
        napi_throw(env, err);
    return NULL;
}

static napi_value again(napi_env env) {
    napi_value v;
    if (napi_get_reference_value(env, addon_of(env)->again, &v) != napi_ok)
        return throw_napi(env);
    return v;
}

/* What a try or op wait returned: AGAIN, `value`, or the error. */
static napi_value outcome(napi_env env, int rc, napi_value (*value)(napi_env, void *), void *out) {
    if (rc == -WEIDA_EAGAIN)
        return again(env);
    if (rc < 0)
        return throw_rc(env, rc);
    return value ? value(env, out) : js_bool(env, true);
}

/* ---- values in ---- */

#define ARGS(n) \
    napi_value argv[n]; \
    do { \
        size_t argc = n; \
        if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok) \
            return throw_napi(env); \
    } while (0)

/* A NUL-terminated copy of a JS string (R7), or NULL with an error thrown;
 * the caller frees it. */
static char *get_string(napi_env env, napi_value v) {
    size_t len = 0;
    if (napi_get_value_string_utf8(env, v, NULL, 0, &len) != napi_ok) {
        throw_type(env, "expected a string");
        return NULL;
    }
    char *s = malloc(len + 1);
    if (!s) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    napi_get_value_string_utf8(env, v, s, len + 1, &len);
    if (strlen(s) != len) {
        free(s);
        throw_code(env, WEIDA_EINVAL);
        return NULL;
    }
    return s;
}

/* The bytes of a Uint8Array (a Buffer is one), without a copy. */
static bool get_bytes(napi_env env, napi_value v, const uint8_t **data, size_t *len) {
    bool is = false;
    napi_typedarray_type type;
    void *d = NULL;
    if (napi_is_typedarray(env, v, &is) != napi_ok || !is ||
        napi_get_typedarray_info(env, v, &type, len, &d, NULL, NULL) != napi_ok ||
        type != napi_uint8_array) {
        throw_type(env, "expected a Uint8Array");
        return false;
    }
    *data = d;
    return true;
}

/* A non-negative integer up to `max`, from a number or a BigInt. */
static bool get_u64(napi_env env, napi_value v, uint64_t max, uint64_t *out) {
    napi_valuetype type;
    if (napi_typeof(env, v, &type) != napi_ok) {
        throw_napi(env);
        return false;
    }
    if (type == napi_bigint) {
        bool lossless = false;
        napi_get_value_bigint_uint64(env, v, out, &lossless);
        if (!lossless || *out > max) {
            throw_range(env, "out of range");
            return false;
        }
        return true;
    }
    if (type != napi_number) {
        throw_type(env, "expected a number");
        return false;
    }
    double d = 0;
    napi_get_value_double(env, v, &d);
    if (!(d >= 0 && d <= 9007199254740991.0) || d != (double)(uint64_t)d || (uint64_t)d > max) {
        throw_range(env, "expected a non-negative integer in range");
        return false;
    }
    *out = (uint64_t)d;
    return true;
}

/* An integer number within INT32_MIN..INT32_MAX. */
static bool get_i32(napi_env env, napi_value v, int32_t *out) {
    napi_valuetype type;
    double d = 0;
    if (napi_typeof(env, v, &type) != napi_ok) {
        throw_napi(env);
        return false;
    }
    if (type != napi_number) {
        throw_type(env, "expected a number");
        return false;
    }
    napi_get_value_double(env, v, &d);
    if (!(d >= INT32_MIN && d <= INT32_MAX) || d != (double)(int32_t)d) {
        throw_range(env, "expected a 32-bit integer");
        return false;
    }
    *out = (int32_t)d;
    return true;
}

/* A boolean, as an R7 flag. */
static bool get_flag(napi_env env, napi_value v, int *out) {
    bool b = false;
    if (napi_get_value_bool(env, v, &b) != napi_ok) {
        throw_type(env, "expected a boolean");
        return false;
    }
    *out = b;
    return true;
}

/* ---- values out ---- */

static napi_value set(napi_env env, napi_value obj, const char *key, napi_value v) {
    if (!v || napi_set_named_property(env, obj, key, v) != napi_ok)
        return throw_napi(env);
    return obj;
}

static napi_value optional_u64(napi_env env, int present, uint64_t n) {
    napi_value v;
    if (present != 1)
        return js_null(env);
    if (napi_create_bigint_uint64(env, n, &v) != napi_ok)
        return throw_napi(env);
    return v;
}

static napi_value optional_u32(napi_env env, int present, uint32_t n) {
    return present == 1 ? js_int64(env, n) : js_null(env);
}

/* A new Uint8Array holding a copy of `len` bytes at `data`. */
static napi_value bytes_value(napi_env env, const uint8_t *data, size_t len) {
    napi_value ab, out;
    void *copy = NULL;
    if (napi_create_arraybuffer(env, len, &copy, &ab) != napi_ok ||
        napi_create_typedarray(env, napi_uint8_array, len, ab, 0, &out) != napi_ok)
        return throw_napi(env);
    if (len)
        memcpy(copy, data, len);
    return out;
}

/* The peer's chain as an array of DER Uint8Arrays, leaf first, or null.
 * The certificates live in `m`, so this runs before `m` is dropped. */
static napi_value chain_value(napi_env env, const weida_incoming_meta *m) {
    size_t n = 0;
    napi_value arr;
    if (weida_incoming_meta_peer_chain_len(m, &n) != 1)
        return js_null(env);
    if (napi_create_array_with_length(env, n, &arr) != napi_ok)
        return throw_napi(env);
    for (size_t i = 0; i < n; i++) {
        size_t len = 0;
        const uint8_t *der = weida_incoming_meta_peer_chain_cert(m, i, &len);
        napi_value cert = bytes_value(env, der, len);
        if (!cert)
            return NULL;
        if (napi_set_element(env, arr, (uint32_t)i, cert) != napi_ok)
            return throw_napi(env);
    }
    return arr;
}

/* The local principal the kernel proved: { uid, gid, pid } for an AF_UNIX
 * peer, { sid, pid } for a named-pipe peer, else null. */
static napi_value local_value(napi_env env, const weida_incoming_meta *m) {
    uint32_t uid = 0, gid = 0, pid = 0;
    size_t len = 0;
    napi_value obj;
    int has_pid = weida_incoming_meta_peer_pid(m, &pid);
    if (weida_incoming_meta_peer_uid(m, &uid) == 1) {
        weida_incoming_meta_peer_gid(m, &gid);
        if (napi_create_object(env, &obj) != napi_ok)
            return throw_napi(env);
        if (!set(env, obj, "uid", js_int64(env, uid)) || !set(env, obj, "gid", js_int64(env, gid)) ||
            !set(env, obj, "pid", optional_u32(env, has_pid, pid)))
            return NULL;
        return obj;
    }
    const char *sid = weida_incoming_meta_peer_sid(m, &len);
    if (!sid)
        return js_null(env);
    if (napi_create_object(env, &obj) != napi_ok)
        return throw_napi(env);
    if (!set(env, obj, "sid", js_string(env, sid, len)) ||
        !set(env, obj, "pid", optional_u32(env, has_pid, pid)))
        return NULL;
    return obj;
}

/* What went missing before a transfer: { expected, seen } as BigInts, or
 * null when nothing is missing or ordering is off. */
static napi_value gap_value(napi_env env, const weida_incoming_meta *m) {
    uint64_t expected = 0, seen = 0;
    napi_value obj;
    if (weida_incoming_meta_gap(m, &expected, &seen) != 1)
        return js_null(env);
    if (napi_create_object(env, &obj) != napi_ok)
        return throw_napi(env);
    if (!set(env, obj, "expected", optional_u64(env, 1, expected)) ||
        !set(env, obj, "seen", optional_u64(env, 1, seen)))
        return NULL;
    return obj;
}

/* A cursor level out: weida's own by its `WEIDA_ACKNOWLEDGEMENT_*` name, an
 * application stage as a BigInt. */
static napi_value level_value(napi_env env, uint64_t level) {
    if (level < WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR)
        return js_string(env, acknowledgement_name((int)level), NAPI_AUTO_LENGTH);
    return optional_u64(env, 1, level);
}

/* An array of cursor levels; empty for NULL. */
static napi_value levels_value(napi_env env, const uint64_t *p, size_t n) {
    napi_value arr;
    if (!p)
        n = 0;
    if (napi_create_array_with_length(env, n, &arr) != napi_ok)
        return throw_napi(env);
    for (size_t i = 0; i < n; i++) {
        napi_value v = level_value(env, p[i]);
        if (!v || napi_set_element(env, arr, (uint32_t)i, v) != napi_ok)
            return throw_napi(env);
    }
    return arr;
}

/* Copies a weida_incoming_meta out into a plain object and drops it. */
static napi_value meta_value(napi_env env, void *ptr) {
    weida_incoming_meta *m = ptr;
    napi_value obj = NULL;
    size_t len = 0;
    uint64_t n = 0;
    int present;
    if (napi_create_object(env, &obj) != napi_ok) {
        weida_incoming_meta_drop(m);
        return throw_napi(env);
    }
    const char *s = weida_incoming_meta_endpoint(m, &len);
    bool ok = set(env, obj, "endpoint", js_string(env, s, len));
    s = weida_incoming_meta_content_type(m, &len);
    ok = ok && set(env, obj, "contentType", js_string(env, s, len));
    s = weida_incoming_meta_topic(m, &len);
    ok = ok && set(env, obj, "topic", js_string(env, s, len));
    s = weida_incoming_meta_peer_fingerprint(m, &len);
    ok = ok && set(env, obj, "peerFingerprint", js_string(env, s, len));
    ok = ok && set(env, obj, "peerChain", chain_value(env, m));
    ok = ok && set(env, obj, "peerLocal", local_value(env, m));
    present = weida_incoming_meta_content_len(m, &n);
    ok = ok && set(env, obj, "contentLen", optional_u64(env, present, n));
    present = weida_incoming_meta_sequence(m, &n);
    ok = ok && set(env, obj, "sequence", optional_u64(env, present, n));
    ok = ok && set(env, obj, "gap", gap_value(env, m));
    present = weida_incoming_meta_segment(m, &n);
    ok = ok && set(env, obj, "segment", optional_u64(env, present, n));
    uint8_t layer = 0;
    ok = ok && set(env, obj, "layer",
                   weida_incoming_meta_layer(m, &layer) == 1 ? js_int64(env, layer)
                                                             : js_null(env));
    s = weida_incoming_meta_trace(m, &len);
    ok = ok && set(env, obj, "trace", js_string(env, s, len));
    s = weida_incoming_meta_tracestate(m, &len);
    ok = ok && set(env, obj, "tracestate", js_string(env, s, len));
    int level = 0;
    ok = ok && set(env, obj, "achieved",
                   weida_incoming_meta_achieved(m, &level) == 1
                       ? js_string(env, acknowledgement_name(level), NAPI_AUTO_LENGTH)
                       : js_null(env));
    size_t report_len = 0;
    const uint64_t *report = weida_incoming_meta_report(m, &report_len);
    ok = ok && set(env, obj, "report", levels_value(env, report, report_len));
    ok = ok && set(env, obj, "reportMode",
                   js_string(env, report_mode_name(weida_incoming_meta_report_mode(m)),
                             NAPI_AUTO_LENGTH));
    present = weida_incoming_meta_report_id(m, &n);
    ok = ok && set(env, obj, "reportId", optional_u64(env, present, n));
    weida_incoming_meta_drop(m);
    return ok ? obj : NULL;
}

/* Copies a weida_msg out into `{ payload, meta }` and drops it. */
static napi_value msg_value(napi_env env, void *ptr) {
    weida_msg *msg = ptr;
    napi_value obj = NULL, ab, payload = NULL, meta = NULL;
    void *data = NULL;
    size_t len = weida_msg_len(msg);
    weida_incoming_meta *m = NULL;
    int rc = weida_msg_meta(msg, &m);
    if (rc < 0) {
        weida_msg_drop(msg);
        return throw_rc(env, rc);
    }
    meta = meta_value(env, m);
    if (meta && napi_create_arraybuffer(env, len, &data, &ab) == napi_ok &&
        napi_create_typedarray(env, napi_uint8_array, len, ab, 0, &payload) == napi_ok &&
        napi_create_object(env, &obj) == napi_ok) {
        if (len)
            memcpy(data, weida_msg_data(msg), len);
        obj = set(env, obj, "payload", payload) ? set(env, obj, "meta", meta) : NULL;
    } else {
        obj = meta ? throw_napi(env) : NULL;
    }
    weida_msg_drop(msg);
    return obj;
}

/* Copies a weida_survey_answer out into a Uint8Array and drops it. */
static napi_value answer_value(napi_env env, void *ptr) {
    weida_survey_answer *a = ptr;
    napi_value ab, bytes = NULL;
    void *data = NULL;
    size_t len = weida_survey_answer_len(a);
    if (napi_create_arraybuffer(env, len, &data, &ab) == napi_ok &&
        napi_create_typedarray(env, napi_uint8_array, len, ab, 0, &bytes) == napi_ok) {
        if (len)
            memcpy(data, weida_survey_answer_data(a), len);
    } else {
        bytes = throw_napi(env);
    }
    weida_survey_answer_drop(a);
    return bytes;
}

/* Copies a weida_drained out into `{ delivered, outstanding }` and drops it. */
static napi_value drained_value(napi_env env, void *ptr) {
    weida_drained *d = ptr;
    napi_value obj = NULL;
    if (napi_create_object(env, &obj) != napi_ok) {
        weida_drained_drop(d);
        return throw_napi(env);
    }
    bool ok = set(env, obj, "delivered", optional_u64(env, 1, weida_drained_delivered(d)));
    ok = ok && set(env, obj, "outstanding", optional_u64(env, 1, weida_drained_outstanding(d)));
    weida_drained_drop(d);
    return ok ? obj : NULL;
}

/* A connection record's numbers, each read by one getter (R2). */
typedef int (*stat_fn)(const weida_connection_stats *, size_t, uint64_t *);

struct stat_field {
    const char *key;
    stat_fn get;
};

static const struct stat_field RECORD_FIELDS[] = {
    {"ageUs", weida_connection_stats_age_us},
    {"redials", weida_connection_stats_redials},
};

static const struct stat_field PATH_FIELDS[] = {
    {"rttUs", weida_connection_stats_transport_path_rtt_us},
    {"minRttUs", weida_connection_stats_transport_path_min_rtt_us},
    {"cwnd", weida_connection_stats_transport_path_cwnd},
    {"congestionEvents", weida_connection_stats_transport_path_congestion_events},
    {"lostPackets", weida_connection_stats_transport_path_lost_packets},
    {"lostBytes", weida_connection_stats_transport_path_lost_bytes},
    {"sentPackets", weida_connection_stats_transport_path_sent_packets},
    {"currentMtu", weida_connection_stats_transport_path_current_mtu},
    {"maxDatagramSize", weida_connection_stats_transport_path_max_datagram_size},
};

static const struct stat_field TRANSPORT_TX_FIELDS[] = {
    {"datagrams", weida_connection_stats_transport_tx_datagrams},
    {"bytes", weida_connection_stats_transport_tx_bytes},
};

static const struct stat_field TRANSPORT_RX_FIELDS[] = {
    {"datagrams", weida_connection_stats_transport_rx_datagrams},
    {"bytes", weida_connection_stats_transport_rx_bytes},
};

static const struct stat_field REMOTE_FIELDS[] = {
    {"rttUs", weida_connection_stats_remote_rtt_us},
    {"minRttUs", weida_connection_stats_remote_min_rtt_us},
    {"cwnd", weida_connection_stats_remote_cwnd},
    {"congestionEvents", weida_connection_stats_remote_congestion_events},
    {"lostPackets", weida_connection_stats_remote_lost_packets},
    {"lostBytes", weida_connection_stats_remote_lost_bytes},
    {"sentPackets", weida_connection_stats_remote_sent_packets},
    {"currentMtu", weida_connection_stats_remote_current_mtu},
    {"ageUs", weida_connection_stats_remote_age_us},
};

static const struct stat_field REMOTE_TX_FIELDS[] = {
    {"datagrams", weida_connection_stats_remote_tx_datagrams},
    {"bytes", weida_connection_stats_remote_tx_bytes},
};

static const struct stat_field REMOTE_RX_FIELDS[] = {
    {"datagrams", weida_connection_stats_remote_rx_datagrams},
    {"bytes", weida_connection_stats_remote_rx_bytes},
};

#define FIELDS(t) t, sizeof t / sizeof t[0]

/* Sets each of `fields` of record `i` on `obj`, a BigInt or null. */
static bool set_stats(napi_env env, napi_value obj, const weida_connection_stats *s, size_t i,
                      const struct stat_field *fields, size_t count) {
    for (size_t f = 0; f < count; f++) {
        uint64_t n = 0;
        int present = fields[f].get(s, i, &n);
        if (!set(env, obj, fields[f].key, optional_u64(env, present, n)))
            return false;
    }
    return true;
}

/* A new object holding `fields` of record `i`. */
static napi_value stat_object(napi_env env, const weida_connection_stats *s, size_t i,
                              const struct stat_field *fields, size_t count) {
    napi_value obj;
    if (napi_create_object(env, &obj) != napi_ok)
        return throw_napi(env);
    return set_stats(env, obj, s, i, fields, count) ? obj : NULL;
}

/* `{ url, ageUs, redials, transport, remote }` of record `i`; `transport`
 * and `remote` are null when the record does not carry them. */
static napi_value stats_record(napi_env env, const weida_connection_stats *s, size_t i) {
    napi_value obj, transport, remote;
    size_t len = 0;
    uint64_t n = 0;
    if (napi_create_object(env, &obj) != napi_ok)
        return throw_napi(env);
    const char *url = weida_connection_stats_url(s, i, &len);
    if (!set(env, obj, "url", js_string(env, url, len)) ||
        !set_stats(env, obj, s, i, FIELDS(RECORD_FIELDS)))
        return NULL;
    if (weida_connection_stats_transport_path_rtt_us(s, i, &n) == 1) {
        if (napi_create_object(env, &transport) != napi_ok)
            return throw_napi(env);
        if (!set(env, transport, "path", stat_object(env, s, i, FIELDS(PATH_FIELDS))) ||
            !set(env, transport, "tx", stat_object(env, s, i, FIELDS(TRANSPORT_TX_FIELDS))) ||
            !set(env, transport, "rx", stat_object(env, s, i, FIELDS(TRANSPORT_RX_FIELDS))))
            return NULL;
    } else {
        transport = js_null(env);
    }
    if (weida_connection_stats_remote_rtt_us(s, i, &n) == 1) {
        remote = stat_object(env, s, i, FIELDS(REMOTE_FIELDS));
        if (!remote ||
            !set(env, remote, "tx", stat_object(env, s, i, FIELDS(REMOTE_TX_FIELDS))) ||
            !set(env, remote, "rx", stat_object(env, s, i, FIELDS(REMOTE_RX_FIELDS))))
            return NULL;
    } else {
        remote = js_null(env);
    }
    if (!set(env, obj, "transport", transport) || !set(env, obj, "remote", remote))
        return NULL;
    return obj;
}

/* Copies a weida_peer_event out into a plain object and drops it. */
static napi_value event_value(napi_env env, void *ptr) {
    weida_peer_event *e = ptr;
    napi_value obj = NULL;
    size_t len = 0;
    uint64_t n = 0;
    uint32_t n32 = 0;
    int c = 0;
    int present;
    if (napi_create_object(env, &obj) != napi_ok) {
        weida_peer_event_drop(e);
        return throw_napi(env);
    }
    bool ok = set(env, obj, "kind",
                  js_string(env, peer_event_name(weida_peer_event_kind(e)), NAPI_AUTO_LENGTH));
    const char *s = weida_peer_event_url(e, &len);
    ok = ok && set(env, obj, "url", js_string(env, s, len));
    s = weida_peer_event_peer_fingerprint(e, &len);
    ok = ok && set(env, obj, "peerFingerprint", js_string(env, s, len));
    ok = ok && set(env, obj, "cause",
                   weida_peer_event_cause(e, &c) == 1
                       ? js_string(env, loss_name(c), NAPI_AUTO_LENGTH)
                       : js_null(env));
    present = weida_peer_event_attempt(e, &n32);
    ok = ok && set(env, obj, "attempt", optional_u32(env, present, n32));
    present = weida_peer_event_delay_us(e, &n);
    ok = ok && set(env, obj, "delayUs", optional_u64(env, present, n));
    ok = ok && set(env, obj, "why",
                   weida_peer_event_why(e, &c) == 1
                       ? js_string(env, give_up_name(c), NAPI_AUTO_LENGTH)
                       : js_null(env));
    present = weida_peer_event_why_attempts(e, &n32);
    ok = ok && set(env, obj, "whyAttempts", optional_u32(env, present, n32));
    s = weida_peer_event_why_presented(e, &len);
    ok = ok && set(env, obj, "whyPresented", js_string(env, s, len));
    s = weida_peer_event_why_message(e, &len);
    ok = ok && set(env, obj, "whyMessage", js_string(env, s, len));
    present = weida_peer_event_missed(e, &n);
    ok = ok && set(env, obj, "missed", optional_u64(env, present, n));
    weida_peer_event_drop(e);
    return ok ? obj : NULL;
}

/* Copies a weida_identity_event out into a plain object and drops it. */
static napi_value identity_event_value(napi_env env, void *ptr) {
    weida_identity_event *e = ptr;
    napi_value obj = NULL;
    size_t len = 0;
    uint64_t n = 0;
    if (napi_create_object(env, &obj) != napi_ok) {
        weida_identity_event_drop(e);
        return throw_napi(env);
    }
    bool ok = set(env, obj, "kind",
                  js_string(env, identity_event_name(weida_identity_event_kind(e)),
                            NAPI_AUTO_LENGTH));
    const char *s = weida_identity_event_fingerprint(e, &len);
    ok = ok && set(env, obj, "fingerprint", js_string(env, s, len));
    s = weida_identity_event_from(e, &len);
    ok = ok && set(env, obj, "from", js_string(env, s, len));
    s = weida_identity_event_to(e, &len);
    ok = ok && set(env, obj, "to", js_string(env, s, len));
    s = weida_identity_event_message(e, &len);
    ok = ok && set(env, obj, "message", js_string(env, s, len));
    int present = weida_identity_event_missed(e, &n);
    ok = ok && set(env, obj, "missed", optional_u64(env, present, n));
    weida_identity_event_drop(e);
    return ok ? obj : NULL;
}

/* ---- generated shapes ---- */

#define WRAP_AS(t) \
    static napi_value wrap_##t(napi_env env, void *out) { return wrap(env, out, drop_##t); }
WRAP_AS(runtime)
WRAP_AS(binding)
WRAP_AS(incoming_request)
WRAP_AS(incoming_transfer)
WRAP_AS(outgoing_transfer)
WRAP_AS(handoff)
WRAP_AS(relay_request)
WRAP_AS(resolved)
WRAP_AS(survey_run)

/* `<t>_<x>(h) -> bigint`: an infallible uint64_t accessor (R2). */
#define U64_OUT(js, t, x) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        if (!p) \
            return NULL; \
        return optional_u64(env, 1, weida_##t##_##x(p)); \
    }

/* `<t>Connection(h) -> bigint` */
#define CONNECTION(js, t) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        uint64_t id = 0; \
        if (!p) \
            return NULL; \
        int rc = weida_##t##_connection(p, &id); \
        return rc < 0 ? throw_rc(env, rc) : optional_u64(env, 1, id); \
    }

/* `<t>_fd(h) -> number` */
#define FD(js, t) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        int64_t fd = -1; \
        if (!p) \
            return NULL; \
        int rc = weida_##t##_fd(p, &fd); \
        return rc < 0 ? throw_rc(env, rc) : js_int64(env, fd); \
    }

/* `<op>_wait(op) -> true | AGAIN` */
#define WAIT0(js, t) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        if (!p) \
            return NULL; \
        return outcome(env, weida_##t##_wait(p, 0), NULL, NULL); \
    }

/* `<op>_wait(op) -> value | AGAIN`, `value` made from the out-pointer. */
#define WAIT1(js, t, out_t, value) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        out_t *out = NULL; \
        if (!p) \
            return NULL; \
        int rc = weida_##t##_wait(p, 0, &out); \
        return outcome(env, rc, value, out); \
    }

/* `<t>_<x>(h) -> string | null` (R7 strings out) */
#define STRING_OUT(js, t, x) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        size_t len = 0; \
        if (!p) \
            return NULL; \
        const char *s = weida_##t##_##x(p, &len); \
        return js_string(env, s, len); \
    }

/* `<t>_new() -> handle` */
#define NEW0(js, t) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        (void)info; \
        weida_##t *out = NULL; \
        int rc = weida_##t##_new(&out); \
        return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_##t); \
    }

/* `<t>_set_<x>(h, n)`: an integer up to `max` (R7 counts, durations and
 * optional values; the JS layer never passes null for an optional). */
#define SET_U64(js, t, x, max) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *h = live(env, argv[0], drop_##t); \
        uint64_t v = 0; \
        if (!h || !get_u64(env, argv[1], max, &v)) \
            return NULL; \
        int rc = weida_##t##_set_##x(h, v); \
        return rc < 0 ? throw_rc(env, rc) : js_undefined(env); \
    }

/* `<t>_set_<x>(h, n)`: an integer up to UINT32_MAX for a `uint32_t` setter. */
#define SET_U32(js, t, x) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *h = live(env, argv[0], drop_##t); \
        uint64_t v = 0; \
        if (!h || !get_u64(env, argv[1], UINT32_MAX, &v)) \
            return NULL; \
        int rc = weida_##t##_set_##x(h, (uint32_t)v); \
        return rc < 0 ? throw_rc(env, rc) : js_undefined(env); \
    }

/* `<t>_set_<x>(h, n)`: an enumeration constant, range-checked by C. */
#define SET_INT(js, t, x) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *h = live(env, argv[0], drop_##t); \
        int32_t v = 0; \
        if (!h || !get_i32(env, argv[1], &v)) \
            return NULL; \
        int rc = weida_##t##_set_##x(h, v); \
        return rc < 0 ? throw_rc(env, rc) : js_undefined(env); \
    }

/* `<t>_set_<x>(h, bool)` (R7 flags) */
#define SET_FLAG(js, t, x) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *h = live(env, argv[0], drop_##t); \
        int v = 0; \
        if (!h || !get_flag(env, argv[1], &v)) \
            return NULL; \
        int rc = weida_##t##_set_##x(h, v); \
        return rc < 0 ? throw_rc(env, rc) : js_undefined(env); \
    }

/* `<t>_set_<x>(h, string)` */
#define SET_TEXT(js, t, x) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *h = live(env, argv[0], drop_##t); \
        if (!h) \
            return NULL; \
        char *s = get_string(env, argv[1]); \
        if (!s) \
            return NULL; \
        int rc = weida_##t##_set_##x(h, s); \
        free(s); \
        return rc < 0 ? throw_rc(env, rc) : js_undefined(env); \
    }

/* `<t>_set_<x>(h, handle | null)`: a nested configuration, copied by C. */
#define SET_HANDLE(js, t, x, u) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *h = live(env, argv[0], drop_##t); \
        void *p = NULL; \
        if (!h || !optional(env, argv[1], drop_##u, &p)) \
            return NULL; \
        int rc = weida_##t##_set_##x(h, p); \
        return rc < 0 ? throw_rc(env, rc) : js_undefined(env); \
    }

/* `<t>_<f>(h, u)`: a call that takes a second handle, which it copies or
 * shares, and answers 0. */
#define WITH_HANDLE(js, t, f, u) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *h = live(env, argv[0], drop_##t); \
        if (!h) \
            return NULL; \
        const weida_##u *p = live(env, argv[1], drop_##u); \
        if (!p) \
            return NULL; \
        int rc = weida_##t##_##f(h, p); \
        return rc < 0 ? throw_rc(env, rc) : js_undefined(env); \
    }

/* `weida_<c>(u, &out) -> handle of t`: a constructor from one handle. */
#define FROM_HANDLE(js, c, u, t) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##u *p = live(env, argv[0], drop_##u); \
        weida_##t *out = NULL; \
        if (!p) \
            return NULL; \
        int rc = weida_##c(p, &out); \
        return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_##t); \
    }

/* `<t>_<f>(h) -> boolean`: a call answering 1 or 0. */
#define BOOL_OUT(js, t, f) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *h = live(env, argv[0], drop_##t); \
        if (!h) \
            return NULL; \
        int rc = weida_##t##_##f(h); \
        return rc < 0 ? throw_rc(env, rc) : js_bool(env, rc == 1); \
    }

/* `<t>_<f>(h, string)` */
#define TEXT_ARG(js, t, f) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *h = live(env, argv[0], drop_##t); \
        if (!h) \
            return NULL; \
        char *s = get_string(env, argv[1]); \
        if (!s) \
            return NULL; \
        int rc = weida_##t##_##f(h, s); \
        free(s); \
        return rc < 0 ? throw_rc(env, rc) : js_undefined(env); \
    }

/* `weak_<t>_upgrade(w) -> handle | null`: null once nothing holds the source. */
#define UPGRADE(js, t) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_weak_##t *w = live(env, argv[0], drop_weak_##t); \
        weida_##t *out = NULL; \
        if (!w) \
            return NULL; \
        int rc = weida_weak_##t##_upgrade(w, &out); \
        if (rc < 0) \
            return throw_rc(env, rc); \
        return rc == 1 ? wrap(env, out, drop_##t) : js_null(env); \
    }

/* ---- the functions ---- */

static napi_value version(napi_env env, napi_callback_info info) {
    (void)info;
    return js_string(env, weida_version(), NAPI_AUTO_LENGTH);
}

static napi_value strerror_(napi_env env, napi_callback_info info) {
    ARGS(1);
    int32_t code = 0;
    if (napi_get_value_int32(env, argv[0], &code) != napi_ok)
        return throw_type(env, "expected a number");
    return js_string(env, weida_strerror(code), NAPI_AUTO_LENGTH);
}

/* `error(code)`: the error of a status code (either sign) no call reported,
 * without a record; the JS layer rejects with it, as for WEIDA_ECLOSED. */
static napi_value error_(napi_env env, napi_callback_info info) {
    ARGS(1);
    int32_t code = 0;
    if (napi_get_value_int32(env, argv[0], &code) != napi_ok)
        return throw_type(env, "expected a number");
    return make_error(env, code < 0 ? -code : code, false);
}

static napi_value set_error_class(napi_env env, napi_callback_info info) {
    ARGS(1);
    napi_valuetype type;
    struct addon *a = addon_of(env);
    if (napi_typeof(env, argv[0], &type) != napi_ok || type != napi_function)
        return throw_type(env, "expected a class");
    if (a->error_class)
        napi_delete_reference(env, a->error_class);
    a->error_class = NULL;
    if (napi_create_reference(env, argv[0], 1, &a->error_class) != napi_ok)
        return throw_napi(env);
    return js_undefined(env);
}

static napi_value drop_(napi_env env, napi_callback_info info) {
    ARGS(1);
    struct wh *h = unwrap(env, argv[0], NULL);
    if (!h)
        return NULL;
    void *p = h->ptr;
    h->ptr = NULL;
    if (p)
        h->drop(p);
    return js_undefined(env);
}

/* runtime config and runtime */

NEW0(runtime_config_new, runtime_config)

static napi_value runtime_config_set_worker_threads(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_runtime_config *cfg = live(env, argv[0], drop_runtime_config);
    uint64_t n = 0;
    if (!cfg || !get_u64(env, argv[1], UINT32_MAX, &n))
        return NULL;
    int rc = weida_runtime_config_set_worker_threads(cfg, (uint32_t)n);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

static napi_value runtime_config_set_shutdown_timeout_ms(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_runtime_config *cfg = live(env, argv[0], drop_runtime_config);
    uint64_t ms = 0;
    if (!cfg || !get_u64(env, argv[1], UINT64_MAX, &ms))
        return NULL;
    int rc = weida_runtime_config_set_shutdown_timeout_ms(cfg, ms);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

SET_U64(runtime_config_set_max_connections, runtime_config, max_connections, UINT64_MAX)
SET_U64(runtime_config_set_max_connections_per_peer, runtime_config, max_connections_per_peer,
        UINT64_MAX)
SET_U64(runtime_config_set_endpoint_queue, runtime_config, endpoint_queue, UINT64_MAX)
SET_U64(runtime_config_set_max_resolved_addresses, runtime_config, max_resolved_addresses,
        UINT64_MAX)
SET_U64(runtime_config_set_connect_attempt_timeout_ms, runtime_config, connect_attempt_timeout_ms,
        UINT64_MAX)
SET_U64(runtime_config_set_send_timeout_ms, runtime_config, send_timeout_ms, INT64_MAX)
SET_U64(runtime_config_set_outbox_messages, runtime_config, outbox_messages, UINT64_MAX)
SET_U64(runtime_config_set_outbox_bytes, runtime_config, outbox_bytes, UINT64_MAX)
SET_INT(runtime_config_set_outbox_full, runtime_config, outbox_full)
SET_U64(runtime_config_set_max_handshakes, runtime_config, max_handshakes, UINT64_MAX)
SET_U64(runtime_config_set_retry_above_handshakes, runtime_config, retry_above_handshakes,
        UINT64_MAX)
SET_INT(runtime_config_set_discovery, runtime_config, discovery)
SET_HANDLE(runtime_config_set_limits, runtime_config, limits, limits)
SET_HANDLE(runtime_config_set_guarantees, runtime_config, guarantees, guarantee_set)
SET_HANDLE(runtime_config_set_reconnect, runtime_config, reconnect, reconnect_policy)
SET_HANDLE(runtime_config_set_resolver, runtime_config, resolver, resolver)

/* nested configuration */

NEW0(limits_new, limits)
SET_U64(limits_set_max_header_bytes, limits, max_header_bytes, UINT64_MAX)
SET_U32(limits_set_max_concurrent_uni_streams, limits, max_concurrent_uni_streams)
SET_U32(limits_set_max_concurrent_bidi_streams, limits, max_concurrent_bidi_streams)
SET_U64(limits_set_stream_receive_window, limits, stream_receive_window, UINT64_MAX)
SET_U64(limits_set_connection_receive_window, limits, connection_receive_window, UINT64_MAX)
SET_U64(limits_set_keep_alive_ms, limits, keep_alive_ms, UINT64_MAX)
SET_U64(limits_set_idle_timeout_ms, limits, idle_timeout_ms, UINT64_MAX)
SET_U64(limits_set_hello_timeout_ms, limits, hello_timeout_ms, UINT64_MAX)
SET_U64(limits_set_max_subscriptions, limits, max_subscriptions, UINT64_MAX)
SET_U64(limits_set_subscriber_buffer_bytes, limits, subscriber_buffer_bytes, UINT64_MAX)
SET_U64(limits_set_max_sequence_scopes, limits, max_sequence_scopes, UINT64_MAX)
SET_U64(limits_set_max_reorder_hold, limits, max_reorder_hold, UINT64_MAX)
SET_U64(limits_set_max_local_streams, limits, max_local_streams, UINT64_MAX)
SET_U64(limits_set_max_parked_reverse, limits, max_parked_reverse, UINT64_MAX)
SET_U64(limits_set_max_dedup_entries, limits, max_dedup_entries, UINT64_MAX)
SET_U64(limits_set_datagram_receive_bytes, limits, datagram_receive_bytes, UINT64_MAX)
SET_U64(limits_set_datagram_send_bytes, limits, datagram_send_bytes, UINT64_MAX)
SET_FLAG(limits_set_path_report, limits, path_report)
SET_U64(limits_set_max_flows, limits, max_flows, UINT64_MAX)
SET_U64(limits_set_flow_queue_bytes, limits, flow_queue_bytes, UINT64_MAX)
SET_U64(limits_set_flow_early_bytes, limits, flow_early_bytes, UINT64_MAX)
SET_U64(limits_set_flow_early_hold_ms, limits, flow_early_hold_ms, UINT64_MAX)
SET_INT(limits_set_congestion, limits, congestion)
SET_U64(limits_set_header_timeout_ms, limits, header_timeout_ms, UINT64_MAX)
SET_U64(limits_set_max_header_streams, limits, max_header_streams, UINT64_MAX)

NEW0(guarantee_set_new, guarantee_set)
SET_INT(guarantee_set_set_delivery, guarantee_set, delivery)
SET_INT(guarantee_set_set_acknowledgement, guarantee_set, acknowledgement)
SET_INT(guarantee_set_set_durability, guarantee_set, durability)
SET_U64(guarantee_set_set_replicas, guarantee_set, replicas, INT64_MAX)
SET_INT(guarantee_set_set_ordering, guarantee_set, ordering)
SET_INT(guarantee_set_set_deduplication, guarantee_set, deduplication)
SET_U64(guarantee_set_set_dedup_window_ms, guarantee_set, dedup_window_ms, INT64_MAX)
SET_INT(guarantee_set_set_backpressure, guarantee_set, backpressure)
SET_INT(guarantee_set_set_producer_naming, guarantee_set, producer_naming)
SET_FLAG(guarantee_set_set_control_isolated, guarantee_set, control_isolated)

NEW0(reconnect_policy_new, reconnect_policy)

static napi_value reconnect_policy_never(napi_env env, napi_callback_info info) {
    (void)info;
    weida_reconnect_policy *out = NULL;
    int rc = weida_reconnect_policy_never(&out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_reconnect_policy);
}

SET_U64(reconnect_policy_set_initial_ms, reconnect_policy, initial_ms, UINT64_MAX)
SET_U64(reconnect_policy_set_max_ms, reconnect_policy, max_ms, UINT64_MAX)
SET_FLAG(reconnect_policy_set_jitter, reconnect_policy, jitter)
SET_U64(reconnect_policy_set_max_attempts, reconnect_policy, max_attempts, UINT32_MAX)
SET_FLAG(reconnect_policy_set_stop_on_peer_closed, reconnect_policy, stop_on_peer_closed)

static napi_value runtime_owned(napi_env env, napi_callback_info info) {
    ARGS(1);
    void *cfg = NULL;
    weida_runtime *out = NULL;
    if (!optional(env, argv[0], drop_runtime_config, &cfg))
        return NULL;
    int rc = weida_runtime_owned(cfg, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap_runtime(env, out);
}

static napi_value runtime_listener(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_runtime *rt = live(env, argv[0], drop_runtime);
    weida_listener *out = NULL;
    if (!rt)
        return NULL;
    int rc = weida_runtime_listener(rt, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_listener);
}

static napi_value runtime_requester(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_runtime *rt = live(env, argv[0], drop_runtime);
    if (!rt)
        return NULL;
    const weida_client_tls *tls = live(env, argv[1], drop_client_tls);
    weida_requester *out = NULL;
    if (!tls)
        return NULL;
    int rc = weida_runtime_requester(rt, tls, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_requester);
}

/* consumes the runtime */
static napi_value runtime_shutdown(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_runtime *rt = take(env, argv[0], drop_runtime);
    weida_runtime_shutdown_op *op = NULL;
    if (!rt)
        return NULL;
    int rc = weida_runtime_shutdown(rt, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_runtime_shutdown_op);
}

FD(runtime_shutdown_op_fd, runtime_shutdown_op)
WAIT0(runtime_shutdown_op_wait, runtime_shutdown_op)

/* consumes the runtime; a deadline out of range throws first and consumes nothing */
static napi_value runtime_drain(napi_env env, napi_callback_info info) {
    ARGS(2);
    uint64_t ms = 0;
    if (!get_u64(env, argv[1], UINT64_MAX, &ms))
        return NULL;
    weida_runtime *rt = take(env, argv[0], drop_runtime);
    weida_runtime_drain_op *op = NULL;
    if (!rt)
        return NULL;
    int rc = weida_runtime_drain(rt, ms, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_runtime_drain_op);
}

FD(runtime_drain_op_fd, runtime_drain_op)
WAIT1(runtime_drain_op_wait, runtime_drain_op, weida_drained, drained_value)
U64_OUT(runtime_suppressed_duplicates, runtime, suppressed_duplicates)
U64_OUT(runtime_expired_transfers, runtime, expired_transfers)

/* TLS values */

NEW0(trust_new, trust)

static napi_value trust_add_pin(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_trust *t = live(env, argv[0], drop_trust);
    if (!t)
        return NULL;
    char *fp = get_string(env, argv[1]);
    if (!fp)
        return NULL;
    int rc = weida_trust_add_pin(t, fp);
    free(fp);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

static napi_value trust_add_anchor(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_trust *t = live(env, argv[0], drop_trust);
    const uint8_t *pem = NULL;
    size_t len = 0;
    if (!t || !get_bytes(env, argv[1], &pem, &len))
        return NULL;
    int rc = weida_trust_add_anchor(t, pem, len);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

static napi_value trust_add_anchor_file(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_trust *t = live(env, argv[0], drop_trust);
    if (!t)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    int rc = weida_trust_add_anchor_file(t, path);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

static napi_value identity_generate(napi_env env, napi_callback_info info) {
    (void)info;
    weida_identity *out = NULL;
    int rc = weida_identity_generate(&out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_identity);
}

static void free_strings(char **names, uint32_t n) {
    for (uint32_t i = 0; i < n; i++)
        free(names[i]);
    free(names);
}

/* The strings of an array, copied into `*out` (`*n` of them, freed with
 * `free_strings`); on failure frees what it read and throws. */
static bool string_array(napi_env env, napi_value v, char ***out, uint32_t *n) {
    bool is = false;
    uint32_t len = 0, got = 0;
    if (napi_is_array(env, v, &is) != napi_ok || !is) {
        throw_type(env, "expected an array of strings");
        return false;
    }
    if (napi_get_array_length(env, v, &len) != napi_ok) {
        throw_napi(env);
        return false;
    }
    char **names = calloc(len ? len : 1, sizeof *names);
    if (!names) {
        napi_throw_error(env, NULL, "out of memory");
        return false;
    }
    /* `got` counts the strings to free: every one read before a throw. */
    for (; got < len; got++) {
        napi_value el;
        if (napi_get_element(env, v, got, &el) != napi_ok) {
            throw_napi(env);
            break;
        }
        names[got] = get_string(env, el);
        if (!names[got])
            break;
    }
    if (got < len) {
        free_strings(names, got);
        return false;
    }
    *out = names;
    *n = len;
    return true;
}

static napi_value identity_generate_for(napi_env env, napi_callback_info info) {
    ARGS(1);
    char **names = NULL;
    uint32_t n = 0;
    if (!string_array(env, argv[0], &names, &n))
        return NULL;
    weida_identity *id = NULL;
    int rc = weida_identity_generate_for((const char *const *)names, n, &id);
    free_strings(names, n);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, id, drop_identity);
}

static napi_value identity_from_pem_file(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_identity *out = NULL;
    char *path = get_string(env, argv[0]);
    if (!path)
        return NULL;
    int rc = weida_identity_from_pem_file(path, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_identity);
}

static napi_value identity_from_pem_files(napi_env env, napi_callback_info info) {
    ARGS(2);
    weida_identity *out = NULL;
    char *cert = get_string(env, argv[0]);
    if (!cert)
        return NULL;
    char *key = get_string(env, argv[1]);
    if (!key) {
        free(cert);
        return NULL;
    }
    int rc = weida_identity_from_pem_files(cert, key, &out);
    free(cert);
    free(key);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_identity);
}

static napi_value identity_from_pem(napi_env env, napi_callback_info info) {
    ARGS(2);
    const uint8_t *cert = NULL, *key = NULL;
    size_t cert_len = 0, key_len = 0;
    weida_identity *out = NULL;
    if (!get_bytes(env, argv[0], &cert, &cert_len) || !get_bytes(env, argv[1], &key, &key_len))
        return NULL;
    int rc = weida_identity_from_pem(cert, cert_len, key, key_len, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_identity);
}

static napi_value identity_fingerprint(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_identity *id = live(env, argv[0], drop_identity);
    const char *fp = NULL;
    if (!id)
        return NULL;
    int rc = weida_identity_fingerprint(id, &fp);
    return rc < 0 ? throw_rc(env, rc) : js_string(env, fp, NAPI_AUTO_LENGTH);
}

/* The chain as a string; the weida_pem is dropped at once. */
static napi_value identity_certificate_pem(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_identity *id = live(env, argv[0], drop_identity);
    weida_pem *p = NULL;
    if (!id)
        return NULL;
    int rc = weida_identity_certificate_pem(id, &p);
    if (rc < 0)
        return throw_rc(env, rc);
    napi_value v = js_string(env, (const char *)weida_pem_data(p), weida_pem_len(p));
    weida_pem_drop(p);
    return v;
}

/* Chain and key as a Uint8Array the caller can wipe; the weida_pem is
 * dropped, which wipes the library's copy. */
static napi_value identity_to_pem(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_identity *id = live(env, argv[0], drop_identity);
    weida_pem *p = NULL;
    if (!id)
        return NULL;
    int rc = weida_identity_to_pem(id, &p);
    if (rc < 0)
        return throw_rc(env, rc);
    napi_value v = bytes_value(env, weida_pem_data(p), weida_pem_len(p));
    weida_pem_drop(p);
    return v;
}

static napi_value client_tls_new(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_trust *t = live(env, argv[0], drop_trust);
    weida_client_tls *out = NULL;
    if (!t)
        return NULL;
    int rc = weida_client_tls_new(t, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_client_tls);
}

static napi_value client_tls_set_identity(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_client_tls *tls = live(env, argv[0], drop_client_tls);
    if (!tls)
        return NULL;
    const weida_identity *id = live(env, argv[1], drop_identity);
    if (!id)
        return NULL;
    int rc = weida_client_tls_set_identity(tls, id);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

static napi_value server_tls_new(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_identity *id = live(env, argv[0], drop_identity);
    weida_server_tls *out = NULL;
    if (!id)
        return NULL;
    int rc = weida_server_tls_new(id, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_server_tls);
}

static napi_value server_tls_require_client(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_server_tls *tls = live(env, argv[0], drop_server_tls);
    if (!tls)
        return NULL;
    const weida_trust *t = live(env, argv[1], drop_trust);
    if (!t)
        return NULL;
    int rc = weida_server_tls_require_client(tls, t);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

static napi_value server_tls_require_any_key(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_server_tls *tls = live(env, argv[0], drop_server_tls);
    if (!tls)
        return NULL;
    int rc = weida_server_tls_require_any_key(tls);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

/* identity and trust sources, ALPN */

BOOL_OUT(trust_is_empty, trust, is_empty)
FROM_HANDLE(client_tls_new_source, client_tls_new_source, trust_source, client_tls)
WITH_HANDLE(client_tls_set_identity_source, client_tls, set_identity_source, identity_source)
SET_INT(client_tls_set_alpn, client_tls, alpn)
FROM_HANDLE(server_tls_new_source, server_tls_new_source, identity_source, server_tls)
WITH_HANDLE(server_tls_require_client_source, server_tls, require_client_source, trust_source)
SET_INT(server_tls_set_alpn, server_tls, alpn)

NEW0(files_options_new, files_options)

static napi_value files_options_set_names(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_files_options *o = live(env, argv[0], drop_files_options);
    char **names = NULL;
    uint32_t n = 0;
    if (!o || !string_array(env, argv[1], &names, &n))
        return NULL;
    int rc = weida_files_options_set_names(o, (const char *const *)names, n);
    free_strings(names, n);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

SET_U64(files_options_set_poll_ms, files_options, poll_ms, UINT64_MAX)

FROM_HANDLE(identity_source_from_identity, identity_source_from_identity, identity,
            identity_source)

static napi_value identity_source_ephemeral(napi_env env, napi_callback_info info) {
    ARGS(1);
    char **names = NULL;
    uint32_t n = 0;
    if (!string_array(env, argv[0], &names, &n))
        return NULL;
    weida_identity_source *out = NULL;
    int rc = weida_identity_source_ephemeral((const char *const *)names, n, &out);
    free_strings(names, n);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_identity_source);
}

FROM_HANDLE(identity_source_external, identity_source_external, identity, identity_source)

/* (dir, options): the JS layer always passes live options. */
static napi_value identity_source_files(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_files_options *o = live(env, argv[1], drop_files_options);
    if (!o)
        return NULL;
    char *dir = get_string(env, argv[0]);
    if (!dir)
        return NULL;
    weida_identity_source *out = NULL;
    int rc = weida_identity_source_files(dir, o, &out);
    free(dir);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_identity_source);
}

FROM_HANDLE(identity_source_current, identity_source_current, identity_source, identity)

static napi_value identity_source_fingerprint(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_identity_source *src = live(env, argv[0], drop_identity_source);
    char buf[WEIDA_FINGERPRINT_LEN + 1];
    if (!src)
        return NULL;
    int rc = weida_identity_source_fingerprint(src, buf, sizeof buf);
    return rc < 0 ? throw_rc(env, rc) : js_string(env, buf, WEIDA_FINGERPRINT_LEN);
}

WITH_HANDLE(identity_source_update, identity_source, update, identity)
TEXT_ARG(identity_source_report_failure, identity_source, report_failure)

static napi_value identity_source_report_handoff_stolen(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_identity_source *src = live(env, argv[0], drop_identity_source);
    if (!src)
        return NULL;
    int rc = weida_identity_source_report_handoff_stolen(src);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

BOOL_OUT(identity_source_reload, identity_source, reload)
FROM_HANDLE(identity_source_events, identity_source_events, identity_source, identity_events)
FROM_HANDLE(identity_source_downgrade, identity_source_downgrade, identity_source,
            weak_identity_source)
UPGRADE(weak_identity_source_upgrade, identity_source)

FROM_HANDLE(trust_source_from_trust, trust_source_from_trust, trust, trust_source)
FROM_HANDLE(trust_source_external, trust_source_external, trust, trust_source)
FROM_HANDLE(trust_source_current, trust_source_current, trust_source, trust)
WITH_HANDLE(trust_source_update, trust_source, update, trust)
TEXT_ARG(trust_source_report_failure, trust_source, report_failure)
BOOL_OUT(trust_source_is_empty, trust_source, is_empty)
FROM_HANDLE(trust_source_events, trust_source_events, trust_source, identity_events)
FROM_HANDLE(trust_source_downgrade, trust_source_downgrade, trust_source, weak_trust_source)
UPGRADE(weak_trust_source_upgrade, trust_source)

/* transfer meta */

NEW0(transfer_meta_new, transfer_meta)

static napi_value transfer_meta_set_content_type(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_transfer_meta *m = live(env, argv[0], drop_transfer_meta);
    if (!m)
        return NULL;
    char *s = get_string(env, argv[1]);
    if (!s)
        return NULL;
    int rc = weida_transfer_meta_set_content_type(m, s);
    free(s);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

SET_U64(transfer_meta_set_content_len, transfer_meta, content_len, UINT64_MAX)
SET_TEXT(transfer_meta_set_topic, transfer_meta, topic)
SET_TEXT(transfer_meta_set_trace, transfer_meta, trace)
SET_INT(transfer_meta_set_achieved, transfer_meta, achieved)
SET_INT(transfer_meta_set_report_mode, transfer_meta, report_mode)

/* (meta, levels): an array of cursor levels as wire values */
static napi_value transfer_meta_set_report(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_transfer_meta *m = live(env, argv[0], drop_transfer_meta);
    bool is_array = false;
    uint32_t n = 0;
    if (!m)
        return NULL;
    if (napi_is_array(env, argv[1], &is_array) != napi_ok)
        return throw_napi(env);
    if (!is_array)
        return throw_type(env, "expected an array of cursor levels");
    if (napi_get_array_length(env, argv[1], &n) != napi_ok)
        return throw_napi(env);
    uint64_t *levels = NULL;
    if (n) {
        levels = malloc(n * sizeof *levels);
        if (!levels)
            return throw_range(env, "out of memory");
    }
    for (uint32_t i = 0; i < n; i++) {
        napi_value v;
        if (napi_get_element(env, argv[1], i, &v) != napi_ok) {
            free(levels);
            return throw_napi(env);
        }
        if (!get_u64(env, v, UINT64_MAX, &levels[i])) {
            free(levels);
            return NULL;
        }
    }
    int rc = weida_transfer_meta_set_report(m, levels, n);
    free(levels);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

/* `newTrace() -> string`: a fresh root traceparent. */
static napi_value new_trace(napi_env env, napi_callback_info info) {
    (void)info;
    char buf[WEIDA_TRACEPARENT_LEN + 1];
    int rc = weida_new_trace(buf, sizeof buf);
    return rc < 0 ? throw_rc(env, rc) : js_string(env, buf, WEIDA_TRACEPARENT_LEN);
}

/* listener and bindings */

static napi_value listener_bind_quic(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    const weida_server_tls *tls = live(env, argv[2], drop_server_tls);
    if (!tls)
        return NULL;
    char *addr = get_string(env, argv[1]);
    if (!addr)
        return NULL;
    weida_listener_bind_quic_op *op = NULL;
    int rc = weida_listener_bind_quic(l, addr, tls, &op);
    free(addr);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_listener_bind_quic_op);
}

FD(listener_bind_quic_op_fd, listener_bind_quic_op)
WAIT1(listener_bind_quic_op_wait, listener_bind_quic_op, weida_binding, wrap_binding)

static napi_value listener_bind_quic_socket(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    uint64_t descriptor = 0;
    if (!get_u64(env, argv[1], INT64_MAX, &descriptor))
        return NULL;
    const weida_server_tls *tls = live(env, argv[2], drop_server_tls);
    if (!tls)
        return NULL;
    /* the descriptor is libweida's once the C call is made; a TypeError or
     * RangeError before it leaves it the caller's */
    weida_listener_bind_quic_socket_op *op = NULL;
    int rc = weida_listener_bind_quic_socket(l, (int64_t)descriptor, tls, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_listener_bind_quic_socket_op);
}

FD(listener_bind_quic_socket_op_fd, listener_bind_quic_socket_op)
WAIT1(listener_bind_quic_socket_op_wait, listener_bind_quic_socket_op, weida_binding,
      wrap_binding)

static napi_value listener_bind_relay(napi_env env, napi_callback_info info) {
    ARGS(4);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    const weida_client_tls *via = live(env, argv[2], drop_client_tls);
    if (!via)
        return NULL;
    const weida_server_tls *tls = live(env, argv[3], drop_server_tls);
    if (!tls)
        return NULL;
    char *relay = get_string(env, argv[1]);
    if (!relay)
        return NULL;
    weida_listener_bind_relay_op *op = NULL;
    int rc = weida_listener_bind_relay(l, relay, via, tls, &op);
    free(relay);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_listener_bind_relay_op);
}

FD(listener_bind_relay_op_fd, listener_bind_relay_op)
WAIT1(listener_bind_relay_op_wait, listener_bind_relay_op, weida_binding, wrap_binding)

static napi_value listener_relay(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    weida_relay *out = NULL;
    int rc = weida_listener_relay(l, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_relay);
}

static napi_value listener_bind_inproc(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *bus = get_string(env, argv[1]);
    if (!bus)
        return NULL;
    weida_local_binding *out = NULL;
    int rc = weida_listener_bind_inproc(l, bus, &out);
    free(bus);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_local_binding);
}

static napi_value listener_bind_unix(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    weida_unix_binding *out = NULL;
    int rc = weida_listener_bind_unix(l, path, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_unix_binding);
}

static napi_value listener_bind_pipe(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *name = get_string(env, argv[1]);
    if (!name)
        return NULL;
    weida_pipe_binding *out = NULL;
    int rc = weida_listener_bind_pipe(l, name, &out);
    free(name);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_pipe_binding);
}

static napi_value listener_replier(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    weida_replier *out = NULL;
    int rc = weida_listener_replier(l, path, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_replier);
}

STRING_OUT(binding_local_addr, binding, local_addr)

static napi_value binding_close(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_binding *b = live(env, argv[0], drop_binding);
    if (!b)
        return NULL;
    weida_binding_close_op *op = NULL;
    int rc = weida_binding_close(b, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_binding_close_op);
}

FD(binding_close_op_fd, binding_close_op)
WAIT0(binding_close_op_wait, binding_close_op)

/* consumes the binding */
static napi_value binding_hand_over(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_binding *b = take(env, argv[0], drop_binding);
    if (!b)
        return NULL;
    weida_binding_hand_over_op *op = NULL;
    int rc = weida_binding_hand_over(b, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_binding_hand_over_op);
}

FD(binding_hand_over_op_fd, binding_hand_over_op)
WAIT1(binding_hand_over_op_wait, binding_hand_over_op, weida_handoff, wrap_handoff)

static napi_value binding_take_over(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_binding *b = live(env, argv[0], drop_binding);
    if (!b)
        return NULL;
    const uint8_t *data = NULL;
    size_t len = 0;
    if (!get_bytes(env, argv[1], &data, &len))
        return NULL;
    weida_binding_take_over_op *op = NULL;
    int rc = weida_binding_take_over(b, data, len, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_binding_take_over_op);
}

FD(binding_take_over_op_fd, binding_take_over_op)

/* -> number | AGAIN: how many datagrams were sent */
static napi_value binding_take_over_op_wait(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_binding_take_over_op *p = live(env, argv[0], drop_binding_take_over_op);
    if (!p)
        return NULL;
    size_t n = 0;
    int rc = weida_binding_take_over_op_wait(p, 0, &n);
    return rc == 0 ? js_int64(env, (int64_t)n) : outcome(env, rc, NULL, NULL);
}

static napi_value binding_disconnect(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_binding *b = live(env, argv[0], drop_binding);
    if (!b)
        return NULL;
    char *fingerprint = get_string(env, argv[1]);
    if (!fingerprint)
        return NULL;
    size_t n = 0;
    int rc = weida_binding_disconnect(b, fingerprint, &n);
    free(fingerprint);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

/* -> peer events | null; null for a binding that is not relayed */
static napi_value binding_relay_events(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_binding *b = live(env, argv[0], drop_binding);
    if (!b)
        return NULL;
    weida_peer_events *out = NULL;
    int rc = weida_binding_relay_events(b, &out);
    if (rc < 0)
        return throw_rc(env, rc);
    return rc == 1 ? wrap(env, out, drop_peer_events) : js_null(env);
}

static napi_value handoff_held(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_handoff *h = live(env, argv[0], drop_handoff);
    if (!h)
        return NULL;
    return js_int64(env, (int64_t)weida_handoff_held(h));
}

STRING_OUT(handoff_local_addr, handoff, local_addr)

/* -> a copy of the held datagrams */
static napi_value handoff_bytes(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_handoff *h = live(env, argv[0], drop_handoff);
    if (!h)
        return NULL;
    size_t len = 0;
    const uint8_t *data = weida_handoff_bytes(h, &len);
    return bytes_value(env, data, len);
}

static napi_value handoff_take_socket(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_handoff *h = live(env, argv[0], drop_handoff);
    if (!h)
        return NULL;
    int64_t descriptor = -1;
    int rc = weida_handoff_take_socket(h, &descriptor);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, descriptor);
}

STRING_OUT(local_binding_bus, local_binding, bus)
STRING_OUT(unix_binding_path, unix_binding, path)
STRING_OUT(pipe_binding_name, pipe_binding, name)

/* replier and incoming request */

STRING_OUT(replier_path, replier, path)
FD(replier_fd, replier)

static napi_value replier_accept(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_replier *r = live(env, argv[0], drop_replier);
    weida_incoming_request *out = NULL;
    if (!r)
        return NULL;
    int rc = weida_replier_accept(r, 0, &out);
    return outcome(env, rc, wrap_incoming_request, out);
}

static napi_value incoming_request_meta(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_incoming_request *q = live(env, argv[0], drop_incoming_request);
    weida_incoming_meta *m = NULL;
    if (!q)
        return NULL;
    int rc = weida_incoming_request_meta(q, &m);
    return rc < 0 ? throw_rc(env, rc) : meta_value(env, m);
}

CONNECTION(incoming_request_connection, incoming_request)

static napi_value incoming_request_take_body(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_incoming_request *q = live(env, argv[0], drop_incoming_request);
    weida_incoming_transfer *out = NULL;
    if (!q)
        return NULL;
    int rc = weida_incoming_request_take_body(q, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap_incoming_transfer(env, out);
}

/* consumes the request */
static napi_value incoming_request_reply(napi_env env, napi_callback_info info) {
    ARGS(2);
    void *meta = NULL;
    if (!optional(env, argv[1], drop_transfer_meta, &meta))
        return NULL;
    weida_incoming_request *q = take(env, argv[0], drop_incoming_request);
    weida_incoming_request_reply_op *op = NULL;
    if (!q)
        return NULL;
    int rc = weida_incoming_request_reply(q, meta, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_incoming_request_reply_op);
}

FD(incoming_request_reply_op_fd, incoming_request_reply_op)
WAIT1(incoming_request_reply_op_wait, incoming_request_reply_op, weida_outgoing_transfer,
      wrap_outgoing_transfer)

/* consumes the request */
static napi_value incoming_request_refuse(napi_env env, napi_callback_info info) {
    ARGS(2);
    int32_t code = 0;
    if (!get_i32(env, argv[1], &code))
        return NULL;
    weida_incoming_request *q = take(env, argv[0], drop_incoming_request);
    weida_incoming_request_refuse_op *op = NULL;
    if (!q)
        return NULL;
    int rc = weida_incoming_request_refuse(q, code, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_incoming_request_refuse_op);
}

FD(incoming_request_refuse_op_fd, incoming_request_refuse_op)
WAIT0(incoming_request_refuse_op_wait, incoming_request_refuse_op)

static napi_value incoming_request_canceled(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_incoming_request *q = live(env, argv[0], drop_incoming_request);
    weida_incoming_request_canceled_op *op = NULL;
    if (!q)
        return NULL;
    int rc = weida_incoming_request_canceled(q, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_incoming_request_canceled_op);
}

FD(incoming_request_canceled_op_fd, incoming_request_canceled_op)
WAIT0(incoming_request_canceled_op_wait, incoming_request_canceled_op)

/* requester */

static napi_value requester_connect(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_requester *r = live(env, argv[0], drop_requester);
    if (!r)
        return NULL;
    char *url = get_string(env, argv[1]);
    if (!url)
        return NULL;
    weida_requester_connect_op *op = NULL;
    int rc = weida_requester_connect(r, url, &op);
    free(url);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_requester_connect_op);
}

FD(requester_connect_op_fd, requester_connect_op)
WAIT0(requester_connect_op_wait, requester_connect_op)

static napi_value requester_request(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_requester *r = live(env, argv[0], drop_requester);
    const uint8_t *body = NULL;
    size_t len = 0;
    weida_requester_request_op *op = NULL;
    if (!r || !get_bytes(env, argv[1], &body, &len))
        return NULL;
    int rc = weida_requester_request(r, body, len, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_requester_request_op);
}

static napi_value requester_request_with(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_requester *r = live(env, argv[0], drop_requester);
    void *meta = NULL;
    const uint8_t *body = NULL;
    size_t len = 0;
    weida_requester_request_op *op = NULL;
    if (!r || !optional(env, argv[1], drop_transfer_meta, &meta) ||
        !get_bytes(env, argv[2], &body, &len))
        return NULL;
    int rc = weida_requester_request_with(r, meta, body, len, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_requester_request_op);
}

FD(requester_request_op_fd, requester_request_op)
WAIT1(requester_request_op_wait, requester_request_op, weida_incoming_transfer,
      wrap_incoming_transfer)

static napi_value requester_open(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_requester *r = live(env, argv[0], drop_requester);
    void *meta = NULL;
    weida_requester_open_op *op = NULL;
    if (!r || !optional(env, argv[1], drop_transfer_meta, &meta))
        return NULL;
    int rc = weida_requester_open(r, meta, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_requester_open_op);
}

FD(requester_open_op_fd, requester_open_op)

/* `requesterOpenOpWait(op) -> { body, reply } | AGAIN` */
static napi_value requester_open_op_wait(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_requester_open_op *p = live(env, argv[0], drop_requester_open_op);
    weida_outgoing_transfer *body = NULL;
    weida_reply_stream *reply = NULL;
    napi_value obj, b, s;
    if (!p)
        return NULL;
    int rc = weida_requester_open_op_wait(p, 0, &body, &reply);
    if (rc == -WEIDA_EAGAIN)
        return again(env);
    if (rc < 0)
        return throw_rc(env, rc);
    b = wrap(env, body, drop_outgoing_transfer);
    if (!b) {
        weida_reply_stream_drop(reply);
        return NULL;
    }
    s = wrap(env, reply, drop_reply_stream);
    if (!s)
        return NULL;
    if (napi_create_object(env, &obj) != napi_ok)
        return throw_napi(env);
    return set(env, obj, "body", b) ? set(env, obj, "reply", s) : NULL;
}

/* consumes the reply stream */
static napi_value reply_stream_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_reply_stream *s = take(env, argv[0], drop_reply_stream);
    weida_reply_stream_recv_op *op = NULL;
    if (!s)
        return NULL;
    int rc = weida_reply_stream_recv(s, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_reply_stream_recv_op);
}

FD(reply_stream_recv_op_fd, reply_stream_recv_op)
WAIT1(reply_stream_recv_op_wait, reply_stream_recv_op, weida_incoming_transfer,
      wrap_incoming_transfer)

/* -> record[], copied out of the snapshot a `_connection_stats` call wrote
 * with status `rc`; the snapshot is dropped on every path. */
static napi_value stats_array(napi_env env, int rc, weida_connection_stats *s) {
    napi_value arr = NULL;
    if (rc < 0)
        return throw_rc(env, rc);
    size_t len = weida_connection_stats_len(s);
    if (napi_create_array_with_length(env, len, &arr) != napi_ok) {
        arr = throw_napi(env);
    } else {
        for (size_t i = 0; i < len; i++) {
            napi_value record = stats_record(env, s, i);
            if (!record) {
                arr = NULL;
                break;
            }
            if (napi_set_element(env, arr, (uint32_t)i, record) != napi_ok) {
                arr = throw_napi(env);
                break;
            }
        }
    }
    weida_connection_stats_drop(s);
    return arr;
}

/* -> record[], copied out of one snapshot */
static napi_value requester_connection_stats(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_requester *r = live(env, argv[0], drop_requester);
    weida_connection_stats *s = NULL;
    if (!r)
        return NULL;
    int rc = weida_requester_connection_stats(r, &s);
    return stats_array(env, rc, s);
}

static napi_value requester_events(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_requester *r = live(env, argv[0], drop_requester);
    weida_peer_events *out = NULL;
    if (!r)
        return NULL;
    int rc = weida_requester_events(r, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_peer_events);
}

FD(peer_events_fd, peer_events)

/* -> event | null | AGAIN; null once the requester is gone */
static napi_value peer_events_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_peer_events *e = live(env, argv[0], drop_peer_events);
    weida_peer_event *event = NULL;
    if (!e)
        return NULL;
    int rc = weida_peer_events_recv(e, 0, &event);
    if (rc == 0 && !event)
        return js_null(env);
    return outcome(env, rc, event_value, event);
}

FD(identity_events_fd, identity_events)

/* -> event | null | AGAIN; null once the source is gone */
static napi_value identity_events_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_identity_events *e = live(env, argv[0], drop_identity_events);
    weida_identity_event *event = NULL;
    if (!e)
        return NULL;
    int rc = weida_identity_events_recv(e, 0, &event);
    if (rc == 0 && !event)
        return js_null(env);
    return outcome(env, rc, identity_event_value, event);
}

/* relay */

FD(relay_fd, relay)

static napi_value relay_accept(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_relay *r = live(env, argv[0], drop_relay);
    weida_relay_request *out = NULL;
    if (!r)
        return NULL;
    int rc = weida_relay_accept(r, 0, &out);
    return outcome(env, rc, wrap_relay_request, out);
}

STRING_OUT(relay_request_peer_fingerprint, relay_request, peer_fingerprint)
STRING_OUT(relay_request_target, relay_request, target)

/* consumes the request */
static napi_value relay_request_admit(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_relay_request *q = take(env, argv[0], drop_relay_request);
    if (!q)
        return NULL;
    weida_relay_request_admit_op *op = NULL;
    int rc = weida_relay_request_admit(q, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_relay_request_admit_op);
}

FD(relay_request_admit_op_fd, relay_request_admit_op)
WAIT0(relay_request_admit_op_wait, relay_request_admit_op)

/* consumes the request */
static napi_value relay_request_refuse(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_relay_request *q = take(env, argv[0], drop_relay_request);
    if (!q)
        return NULL;
    weida_relay_request_refuse_op *op = NULL;
    int rc = weida_relay_request_refuse(q, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_relay_request_refuse_op);
}

FD(relay_request_refuse_op_fd, relay_request_refuse_op)
WAIT0(relay_request_refuse_op_wait, relay_request_refuse_op)

/* resolver */

NEW0(resolver_new, resolver)
FD(resolver_fd, resolver)

/* -> lookup | null | AGAIN; null once no configuration or runtime holds it */
static napi_value resolver_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_resolver *r = live(env, argv[0], drop_resolver);
    weida_resolved *q = NULL;
    if (!r)
        return NULL;
    int rc = weida_resolver_recv(r, 0, &q);
    if (rc == 0 && !q)
        return js_null(env);
    return outcome(env, rc, wrap_resolved, q);
}

STRING_OUT(resolved_name, resolved, name)

/* -> number | null; null when the URL wrote no port */
static napi_value resolved_port(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_resolved *q = live(env, argv[0], drop_resolved);
    uint16_t port = 0;
    if (!q)
        return NULL;
    return weida_resolved_port(q, &port) == 1 ? js_int64(env, port) : js_null(env);
}

/* -> number, saturated at Number.MAX_SAFE_INTEGER */
static napi_value resolved_max_addresses(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_resolved *q = live(env, argv[0], drop_resolved);
    if (!q)
        return NULL;
    uint64_t max = weida_resolved_max_addresses(q);
    return js_int64(env, max > 9007199254740991ULL ? 9007199254740991LL : (int64_t)max);
}

/* (lookup, string[]) consumes the lookup; a TypeError consumes nothing */
static napi_value resolved_answer(napi_env env, napi_callback_info info) {
    ARGS(2);
    char **addrs = NULL;
    uint32_t n = 0;
    if (!string_array(env, argv[1], &addrs, &n))
        return NULL;
    weida_resolved *q = take(env, argv[0], drop_resolved);
    if (!q) {
        free_strings(addrs, n);
        return NULL;
    }
    int rc = weida_resolved_answer(q, (const char *const *)addrs, n);
    free_strings(addrs, n);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

/* (lookup, reason) consumes the lookup; a TypeError consumes nothing */
static napi_value resolved_refuse(napi_env env, napi_callback_info info) {
    ARGS(2);
    char *reason = get_string(env, argv[1]);
    if (!reason)
        return NULL;
    weida_resolved *q = take(env, argv[0], drop_resolved);
    if (!q) {
        free(reason);
        return NULL;
    }
    int rc = weida_resolved_refuse(q, reason);
    free(reason);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

/* shapes shared by the pusher and the subscriber */

/* `<t>_<x>(h, string) -> op`: the string copied in */
#define STRING_OP(js, t, x) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        if (!p) \
            return NULL; \
        char *s = get_string(env, argv[1]); \
        if (!s) \
            return NULL; \
        weida_##t##_##x##_op *op = NULL; \
        int rc = weida_##t##_##x(p, s, &op); \
        free(s); \
        return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_##t##_##x##_op); \
    }

/* `<t>_disconnect(h, url) -> boolean`: whether the address was dialled */
#define DISCONNECT(js, t) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(2); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        if (!p) \
            return NULL; \
        char *url = get_string(env, argv[1]); \
        if (!url) \
            return NULL; \
        int rc = weida_##t##_disconnect(p, url); \
        free(url); \
        return rc < 0 ? throw_rc(env, rc) : js_bool(env, rc == 1); \
    }

/* `<t>_<x>(h) -> number`: an infallible size_t accessor (R2) */
#define SIZE_OUT(js, t, x) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        if (!p) \
            return NULL; \
        return js_int64(env, (int64_t)weida_##t##_##x(p)); \
    }

/* `<t>_connection_stats(h) -> record[]` */
#define CONNECTION_STATS(js, t) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        weida_connection_stats *s = NULL; \
        if (!p) \
            return NULL; \
        int rc = weida_##t##_connection_stats(p, &s); \
        return stats_array(env, rc, s); \
    }

/* `<t>_events(h) -> peer events` */
#define EVENTS(js, t) \
    static napi_value js(napi_env env, napi_callback_info info) { \
        ARGS(1); \
        const weida_##t *p = live(env, argv[0], drop_##t); \
        weida_peer_events *out = NULL; \
        if (!p) \
            return NULL; \
        int rc = weida_##t##_events(p, &out); \
        return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_peer_events); \
    }

/* push/pull */

static napi_value runtime_pusher(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_runtime *rt = live(env, argv[0], drop_runtime);
    if (!rt)
        return NULL;
    const weida_client_tls *tls = live(env, argv[1], drop_client_tls);
    weida_pusher *out = NULL;
    if (!tls)
        return NULL;
    int rc = weida_runtime_pusher(rt, tls, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_pusher);
}

STRING_OP(pusher_connect, pusher, connect)
FD(pusher_connect_op_fd, pusher_connect_op)
WAIT0(pusher_connect_op_wait, pusher_connect_op)
DISCONNECT(pusher_disconnect, pusher)
SIZE_OUT(pusher_peer_count, pusher, peer_count)
U64_OUT(pusher_dropped, pusher, dropped)
CONNECTION_STATS(pusher_connection_stats, pusher)
EVENTS(pusher_events, pusher)

static napi_value pusher_open(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_pusher *p = live(env, argv[0], drop_pusher);
    void *meta = NULL;
    weida_pusher_open_op *op = NULL;
    if (!p || !optional(env, argv[1], drop_transfer_meta, &meta))
        return NULL;
    int rc = weida_pusher_open(p, meta, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_pusher_open_op);
}

FD(pusher_open_op_fd, pusher_open_op)
WAIT1(pusher_open_op_wait, pusher_open_op, weida_outgoing_transfer, wrap_outgoing_transfer)

static napi_value pusher_send(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_pusher *p = live(env, argv[0], drop_pusher);
    const uint8_t *body = NULL;
    size_t len = 0;
    weida_pusher_send_op *op = NULL;
    if (!p || !get_bytes(env, argv[1], &body, &len))
        return NULL;
    int rc = weida_pusher_send(p, body, len, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_pusher_send_op);
}

static napi_value pusher_send_with(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_pusher *p = live(env, argv[0], drop_pusher);
    void *meta = NULL;
    const uint8_t *body = NULL;
    size_t len = 0;
    weida_pusher_send_op *op = NULL;
    if (!p || !optional(env, argv[1], drop_transfer_meta, &meta) ||
        !get_bytes(env, argv[2], &body, &len))
        return NULL;
    int rc = weida_pusher_send_with(p, meta, body, len, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_pusher_send_op);
}

FD(pusher_send_op_fd, pusher_send_op)
WAIT0(pusher_send_op_wait, pusher_send_op)

static napi_value listener_puller(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    weida_puller *out = NULL;
    int rc = weida_listener_puller(l, path, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_puller);
}

STRING_OUT(puller_path, puller, path)
FD(puller_fd, puller)

static napi_value puller_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_puller *p = live(env, argv[0], drop_puller);
    weida_incoming_transfer *out = NULL;
    if (!p)
        return NULL;
    int rc = weida_puller_recv(p, 0, &out);
    return outcome(env, rc, wrap_incoming_transfer, out);
}

/* pub/sub */

typedef int (*drops_fn)(const weida_topic_drops *, size_t, uint64_t *);

static const struct {
    const char *key;
    drops_fn get;
} DROPS_FIELDS[] = {
    {"subscriberBudget", weida_topic_drops_subscriber_budget},
    {"subscriberQueue", weida_topic_drops_subscriber_queue},
    {"noParkedConnection", weida_topic_drops_no_parked_connection},
    {"superseded", weida_topic_drops_superseded},
    {"expired", weida_topic_drops_expired},
    {"tooLarge", weida_topic_drops_too_large},
    {"noDatagrams", weida_topic_drops_no_datagrams},
    {"layersCut", weida_topic_drops_layers_cut},
};

/* `{ topic, subscriberBudget, ... }` of record `i`, the counts as BigInts. */
static napi_value drops_record(napi_env env, const weida_topic_drops *d, size_t i) {
    napi_value obj;
    size_t len = 0;
    if (napi_create_object(env, &obj) != napi_ok)
        return throw_napi(env);
    const char *topic = weida_topic_drops_topic(d, i, &len);
    if (!set(env, obj, "topic", js_string(env, topic, len)))
        return NULL;
    for (size_t f = 0; f < sizeof DROPS_FIELDS / sizeof DROPS_FIELDS[0]; f++) {
        uint64_t n = 0;
        int present = DROPS_FIELDS[f].get(d, i, &n);
        if (!set(env, obj, DROPS_FIELDS[f].key, optional_u64(env, present, n)))
            return NULL;
    }
    return obj;
}

/* -> record[], copied out of the snapshot a drops call wrote with status
 * `rc`; the snapshot is dropped on every path. */
static napi_value topic_drops_array(napi_env env, int rc, weida_topic_drops *d) {
    napi_value arr = NULL;
    if (rc < 0)
        return throw_rc(env, rc);
    size_t len = weida_topic_drops_len(d);
    if (napi_create_array_with_length(env, len, &arr) != napi_ok) {
        arr = throw_napi(env);
    } else {
        for (size_t i = 0; i < len; i++) {
            napi_value record = drops_record(env, d, i);
            if (!record) {
                arr = NULL;
                break;
            }
            if (napi_set_element(env, arr, (uint32_t)i, record) != napi_ok) {
                arr = throw_napi(env);
                break;
            }
        }
    }
    weida_topic_drops_drop(d);
    return arr;
}

static napi_value listener_publisher(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    weida_publisher *out = NULL;
    int rc = weida_listener_publisher(l, path, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_publisher);
}

STRING_OUT(publisher_path, publisher, path)

/* (publisher, topic, bytes) -> number: the subscribers it was enqueued to */
static napi_value publisher_publish(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_publisher *p = live(env, argv[0], drop_publisher);
    const uint8_t *data = NULL;
    size_t len = 0, n = 0;
    if (!p || !get_bytes(env, argv[2], &data, &len))
        return NULL;
    char *topic = get_string(env, argv[1]);
    if (!topic)
        return NULL;
    int rc = weida_publisher_publish(p, topic, data, len, &n);
    free(topic);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

/* (publisher, topic, bytes, traceparent) -> number */
static napi_value publisher_publish_with_trace(napi_env env, napi_callback_info info) {
    ARGS(4);
    const weida_publisher *p = live(env, argv[0], drop_publisher);
    const uint8_t *data = NULL;
    size_t len = 0, n = 0;
    if (!p || !get_bytes(env, argv[2], &data, &len))
        return NULL;
    char *topic = get_string(env, argv[1]);
    if (!topic)
        return NULL;
    char *trace = get_string(env, argv[3]);
    if (!trace) {
        free(topic);
        return NULL;
    }
    int rc = weida_publisher_publish_with_trace(p, topic, data, len, trace, &n);
    free(trace);
    free(topic);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

static napi_value publisher_open(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_publisher *p = live(env, argv[0], drop_publisher);
    if (!p)
        return NULL;
    char *topic = get_string(env, argv[1]);
    if (!topic)
        return NULL;
    weida_fan_out *out = NULL;
    int rc = weida_publisher_open(p, topic, &out);
    free(topic);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_fan_out);
}

static napi_value publisher_open_with_trace(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_publisher *p = live(env, argv[0], drop_publisher);
    if (!p)
        return NULL;
    char *topic = get_string(env, argv[1]);
    if (!topic)
        return NULL;
    char *trace = get_string(env, argv[2]);
    if (!trace) {
        free(topic);
        return NULL;
    }
    weida_fan_out *out = NULL;
    int rc = weida_publisher_open_with_trace(p, topic, trace, &out);
    free(trace);
    free(topic);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_fan_out);
}

SIZE_OUT(publisher_subscriber_count, publisher, subscriber_count)
SIZE_OUT(publisher_filter_count, publisher, filter_count)
U64_OUT(publisher_dropped, publisher, dropped)

static napi_value publisher_dropped_on(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_publisher *p = live(env, argv[0], drop_publisher);
    if (!p)
        return NULL;
    char *topic = get_string(env, argv[1]);
    if (!topic)
        return NULL;
    weida_topic_drops *d = NULL;
    int rc = weida_publisher_dropped_on(p, topic, &d);
    free(topic);
    return topic_drops_array(env, rc, d);
}

static napi_value publisher_drops(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_publisher *p = live(env, argv[0], drop_publisher);
    weida_topic_drops *d = NULL;
    if (!p)
        return NULL;
    int rc = weida_publisher_drops(p, &d);
    return topic_drops_array(env, rc, d);
}

STRING_OUT(fan_out_topic, fan_out, topic)

static napi_value fan_out_subscribers(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_fan_out *f = live(env, argv[0], drop_fan_out);
    size_t n = 0;
    if (!f)
        return NULL;
    int rc = weida_fan_out_subscribers(f, &n);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

/* (fan-out, bytes, limitMs) -> op */
static napi_value fan_out_write_within(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_fan_out *f = live(env, argv[0], drop_fan_out);
    const uint8_t *chunk = NULL;
    size_t len = 0;
    uint64_t limit_ms = 0;
    weida_fan_out_write_within_op *op = NULL;
    if (!f || !get_bytes(env, argv[1], &chunk, &len) ||
        !get_u64(env, argv[2], UINT64_MAX, &limit_ms))
        return NULL;
    int rc = weida_fan_out_write_within(f, chunk, len, limit_ms, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_fan_out_write_within_op);
}

FD(fan_out_write_within_op_fd, fan_out_write_within_op)

/* -> number | AGAIN: the subscribers left */
static napi_value fan_out_write_within_op_wait(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_fan_out_write_within_op *p = live(env, argv[0], drop_fan_out_write_within_op);
    if (!p)
        return NULL;
    size_t n = 0;
    int rc = weida_fan_out_write_within_op_wait(p, 0, &n);
    return rc == 0 ? js_int64(env, (int64_t)n) : outcome(env, rc, NULL, NULL);
}

static napi_value fan_out_write_now(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_fan_out *f = live(env, argv[0], drop_fan_out);
    const uint8_t *chunk = NULL;
    size_t len = 0, n = 0;
    if (!f || !get_bytes(env, argv[1], &chunk, &len))
        return NULL;
    int rc = weida_fan_out_write_now(f, chunk, len, &n);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

/* consumes the fan-out -> number: the subscribers that received all of it */
static napi_value fan_out_finish(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_fan_out *f = take(env, argv[0], drop_fan_out);
    size_t n = 0;
    if (!f)
        return NULL;
    int rc = weida_fan_out_finish(f, &n);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

static napi_value runtime_subscriber(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_runtime *rt = live(env, argv[0], drop_runtime);
    if (!rt)
        return NULL;
    const weida_client_tls *tls = live(env, argv[1], drop_client_tls);
    weida_subscriber *out = NULL;
    if (!tls)
        return NULL;
    int rc = weida_runtime_subscriber(rt, tls, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_subscriber);
}

STRING_OP(subscriber_connect, subscriber, connect)
FD(subscriber_connect_op_fd, subscriber_connect_op)
WAIT0(subscriber_connect_op_wait, subscriber_connect_op)
DISCONNECT(subscriber_disconnect, subscriber)
SIZE_OUT(subscriber_peer_count, subscriber, peer_count)
CONNECTION_STATS(subscriber_connection_stats, subscriber)
EVENTS(subscriber_events, subscriber)
STRING_OP(subscriber_subscribe, subscriber, subscribe)
FD(subscriber_subscribe_op_fd, subscriber_subscribe_op)
WAIT0(subscriber_subscribe_op_wait, subscriber_subscribe_op)
STRING_OP(subscriber_unsubscribe, subscriber, unsubscribe)
FD(subscriber_unsubscribe_op_fd, subscriber_unsubscribe_op)
WAIT0(subscriber_unsubscribe_op_wait, subscriber_unsubscribe_op)

/* (subscriber, filter, limit) -> op */
static napi_value subscriber_grant(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_subscriber *s = live(env, argv[0], drop_subscriber);
    uint64_t limit = 0;
    if (!s || !get_u64(env, argv[2], UINT64_MAX, &limit))
        return NULL;
    char *filter = get_string(env, argv[1]);
    if (!filter)
        return NULL;
    weida_subscriber_grant_op *op = NULL;
    int rc = weida_subscriber_grant(s, filter, limit, &op);
    free(filter);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_subscriber_grant_op);
}

FD(subscriber_grant_op_fd, subscriber_grant_op)
WAIT0(subscriber_grant_op_wait, subscriber_grant_op)
SIZE_OUT(subscriber_filter_count, subscriber, filter_count)
FD(subscriber_fd, subscriber)

static napi_value subscriber_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_subscriber *s = live(env, argv[0], drop_subscriber);
    weida_incoming_transfer *out = NULL;
    if (!s)
        return NULL;
    int rc = weida_subscriber_recv(s, 0, &out);
    return outcome(env, rc, wrap_incoming_transfer, out);
}

/* pair */

static napi_value runtime_pair(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_runtime *rt = live(env, argv[0], drop_runtime);
    if (!rt)
        return NULL;
    const weida_client_tls *tls = live(env, argv[1], drop_client_tls);
    weida_paired *out = NULL;
    if (!tls)
        return NULL;
    int rc = weida_runtime_pair(rt, tls, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_paired);
}

static napi_value listener_pair(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    weida_paired *out = NULL;
    int rc = weida_listener_pair(l, path, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_paired);
}

STRING_OUT(paired_path, paired, path)
STRING_OP(paired_connect, paired, connect)
FD(paired_connect_op_fd, paired_connect_op)
WAIT0(paired_connect_op_wait, paired_connect_op)
SIZE_OUT(paired_peer_count, paired, peer_count)
U64_OUT(paired_dropped, paired, dropped)
CONNECTION_STATS(paired_connection_stats, paired)

/* -> peer events | null; null for a bound pair */
static napi_value paired_events(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_paired *p = live(env, argv[0], drop_paired);
    if (!p)
        return NULL;
    weida_peer_events *out = NULL;
    int rc = weida_paired_events(p, &out);
    if (rc < 0)
        return throw_rc(env, rc);
    return rc == 1 ? wrap(env, out, drop_peer_events) : js_null(env);
}

static napi_value paired_open(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_paired *p = live(env, argv[0], drop_paired);
    void *meta = NULL;
    weida_paired_open_op *op = NULL;
    if (!p || !optional(env, argv[1], drop_transfer_meta, &meta))
        return NULL;
    int rc = weida_paired_open(p, meta, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_paired_open_op);
}

FD(paired_open_op_fd, paired_open_op)
WAIT1(paired_open_op_wait, paired_open_op, weida_outgoing_transfer, wrap_outgoing_transfer)

static napi_value paired_send(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_paired *p = live(env, argv[0], drop_paired);
    const uint8_t *body = NULL;
    size_t len = 0;
    weida_paired_send_op *op = NULL;
    if (!p || !get_bytes(env, argv[1], &body, &len))
        return NULL;
    int rc = weida_paired_send(p, body, len, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_paired_send_op);
}

static napi_value paired_send_with(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_paired *p = live(env, argv[0], drop_paired);
    void *meta = NULL;
    const uint8_t *body = NULL;
    size_t len = 0;
    weida_paired_send_op *op = NULL;
    if (!p || !optional(env, argv[1], drop_transfer_meta, &meta) ||
        !get_bytes(env, argv[2], &body, &len))
        return NULL;
    int rc = weida_paired_send_with(p, meta, body, len, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_paired_send_op);
}

FD(paired_send_op_fd, paired_send_op)
WAIT0(paired_send_op_wait, paired_send_op)
FD(paired_fd, paired)

static napi_value paired_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_paired *p = live(env, argv[0], drop_paired);
    weida_incoming_transfer *out = NULL;
    if (!p)
        return NULL;
    int rc = weida_paired_recv(p, 0, &out);
    return outcome(env, rc, wrap_incoming_transfer, out);
}

/* survey */

static napi_value runtime_surveyor(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_runtime *rt = live(env, argv[0], drop_runtime);
    if (!rt)
        return NULL;
    const weida_client_tls *tls = live(env, argv[1], drop_client_tls);
    weida_surveyor *out = NULL;
    if (!tls)
        return NULL;
    int rc = weida_runtime_surveyor(rt, tls, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_surveyor);
}

STRING_OP(surveyor_connect, surveyor, connect)
FD(surveyor_connect_op_fd, surveyor_connect_op)
WAIT0(surveyor_connect_op_wait, surveyor_connect_op)
SIZE_OUT(surveyor_peer_count, surveyor, peer_count)
CONNECTION_STATS(surveyor_connection_stats, surveyor)

/* (surveyor, bytes, deadlineMs) -> op */
static napi_value surveyor_survey(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_surveyor *s = live(env, argv[0], drop_surveyor);
    const uint8_t *body = NULL;
    size_t len = 0;
    uint64_t deadline_ms = 0;
    weida_surveyor_survey_op *op = NULL;
    if (!s || !get_bytes(env, argv[1], &body, &len) ||
        !get_u64(env, argv[2], UINT64_MAX, &deadline_ms))
        return NULL;
    int rc = weida_surveyor_survey(s, body, len, deadline_ms, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_surveyor_survey_op);
}

/* (surveyor, meta | null, bytes, deadlineMs) -> op */
static napi_value surveyor_survey_with(napi_env env, napi_callback_info info) {
    ARGS(4);
    const weida_surveyor *s = live(env, argv[0], drop_surveyor);
    void *meta = NULL;
    const uint8_t *body = NULL;
    size_t len = 0;
    uint64_t deadline_ms = 0;
    weida_surveyor_survey_op *op = NULL;
    if (!s || !optional(env, argv[1], drop_transfer_meta, &meta) ||
        !get_bytes(env, argv[2], &body, &len) ||
        !get_u64(env, argv[3], UINT64_MAX, &deadline_ms))
        return NULL;
    int rc = weida_surveyor_survey_with(s, meta, body, len, deadline_ms, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_surveyor_survey_op);
}

FD(surveyor_survey_op_fd, surveyor_survey_op)
WAIT1(surveyor_survey_op_wait, surveyor_survey_op, weida_survey_run, wrap_survey_run)
FD(survey_run_fd, survey_run)

/* (run, maxBytes) -> Uint8Array | null | AGAIN; null once the run ended */
static napi_value survey_run_next(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_survey_run *r = live(env, argv[0], drop_survey_run);
    uint64_t max = 0;
    weida_survey_answer *a = NULL;
    if (!r || !get_u64(env, argv[1], UINT64_MAX, &max))
        return NULL;
    int rc = weida_survey_run_next(r, max, 0, &a);
    if (rc == 0 && !a)
        return js_null(env);
    return outcome(env, rc, answer_value, a);
}

U64_OUT(survey_run_late, survey_run, late)
SIZE_OUT(survey_run_respondents, survey_run, respondents)

static napi_value listener_respondent(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    weida_respondent *out = NULL;
    int rc = weida_listener_respondent(l, path, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_respondent);
}

STRING_OUT(respondent_path, respondent, path)
FD(respondent_fd, respondent)

static napi_value respondent_accept(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_respondent *r = live(env, argv[0], drop_respondent);
    weida_incoming_request *out = NULL;
    if (!r)
        return NULL;
    int rc = weida_respondent_accept(r, 0, &out);
    return outcome(env, rc, wrap_incoming_request, out);
}

/* bus */

/* (listener, path, clientTls) -> member */
static napi_value listener_bus(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    const weida_client_tls *tls = live(env, argv[2], drop_client_tls);
    if (!tls)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    weida_bus_member *out = NULL;
    int rc = weida_listener_bus(l, path, tls, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_bus_member);
}

STRING_OUT(bus_member_path, bus_member, path)
STRING_OP(bus_member_connect, bus_member, connect)
FD(bus_member_connect_op_fd, bus_member_connect_op)
WAIT0(bus_member_connect_op_wait, bus_member_connect_op)
SIZE_OUT(bus_member_peer_count, bus_member, peer_count)
CONNECTION_STATS(bus_member_connection_stats, bus_member)
U64_OUT(bus_member_dropped, bus_member, dropped)

static napi_value bus_member_send(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_bus_member *m = live(env, argv[0], drop_bus_member);
    const uint8_t *body = NULL;
    size_t len = 0;
    weida_bus_member_send_op *op = NULL;
    if (!m || !get_bytes(env, argv[1], &body, &len))
        return NULL;
    int rc = weida_bus_member_send(m, body, len, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_bus_member_send_op);
}

static napi_value bus_member_send_with(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_bus_member *m = live(env, argv[0], drop_bus_member);
    void *meta = NULL;
    const uint8_t *body = NULL;
    size_t len = 0;
    weida_bus_member_send_op *op = NULL;
    if (!m || !optional(env, argv[1], drop_transfer_meta, &meta) ||
        !get_bytes(env, argv[2], &body, &len))
        return NULL;
    int rc = weida_bus_member_send_with(m, meta, body, len, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_bus_member_send_op);
}

FD(bus_member_send_op_fd, bus_member_send_op)

/* -> number | AGAIN: the members the message reached */
static napi_value bus_member_send_op_wait(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_bus_member_send_op *p = live(env, argv[0], drop_bus_member_send_op);
    if (!p)
        return NULL;
    size_t n = 0;
    int rc = weida_bus_member_send_op_wait(p, 0, &n);
    return rc == 0 ? js_int64(env, (int64_t)n) : outcome(env, rc, NULL, NULL);
}

FD(bus_member_fd, bus_member)

static napi_value bus_member_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_bus_member *m = live(env, argv[0], drop_bus_member);
    weida_incoming_transfer *out = NULL;
    if (!m)
        return NULL;
    int rc = weida_bus_member_recv(m, 0, &out);
    return outcome(env, rc, wrap_incoming_transfer, out);
}

/* radio */

NEW0(join_terms_new, join_terms)
SET_U64(join_terms_set_max_age_ms, join_terms, max_age_ms, INT64_MAX)
SET_U64(join_terms_set_max_layer, join_terms, max_layer, INT64_MAX)
NEW0(segment_terms_new, segment_terms)
SET_U64(segment_terms_set_max_age_ms, segment_terms, max_age_ms, INT64_MAX)
SET_INT(segment_terms_set_priority, segment_terms, priority)
SET_FLAG(segment_terms_set_follows_upstream, segment_terms, follows_upstream)

typedef int (*dish_drops_fn)(const weida_dish_drops *, size_t, uint64_t *);

static const struct {
    const char *key;
    dish_drops_fn get;
} DISH_DROPS_FIELDS[] = {
    {"subscriberBudget", weida_dish_drops_subscriber_budget},
    {"subscriberQueue", weida_dish_drops_subscriber_queue},
    {"superseded", weida_dish_drops_superseded},
    {"expired", weida_dish_drops_expired},
    {"tooLarge", weida_dish_drops_too_large},
    {"noDatagrams", weida_dish_drops_no_datagrams},
    {"layersCut", weida_dish_drops_layers_cut},
};

/* `{ peerFingerprint, subscriberBudget, ... }` of record `i`, the counts as
 * BigInts. */
static napi_value dish_drops_record(napi_env env, const weida_dish_drops *d, size_t i) {
    napi_value obj;
    size_t len = 0;
    if (napi_create_object(env, &obj) != napi_ok)
        return throw_napi(env);
    const char *fp = weida_dish_drops_peer_fingerprint(d, i, &len);
    if (!set(env, obj, "peerFingerprint", js_string(env, fp, len)))
        return NULL;
    for (size_t f = 0; f < sizeof DISH_DROPS_FIELDS / sizeof DISH_DROPS_FIELDS[0]; f++) {
        uint64_t n = 0;
        int present = DISH_DROPS_FIELDS[f].get(d, i, &n);
        if (!set(env, obj, DISH_DROPS_FIELDS[f].key, optional_u64(env, present, n)))
            return NULL;
    }
    return obj;
}

/* -> record[], copied out of the snapshot `weida_radio_dish_drops` wrote
 * with status `rc`; the snapshot is dropped on every path. */
static napi_value dish_drops_array(napi_env env, int rc, weida_dish_drops *d) {
    napi_value arr = NULL;
    if (rc < 0)
        return throw_rc(env, rc);
    size_t len = weida_dish_drops_len(d);
    if (napi_create_array_with_length(env, len, &arr) != napi_ok) {
        arr = throw_napi(env);
    } else {
        for (size_t i = 0; i < len; i++) {
            napi_value record = dish_drops_record(env, d, i);
            if (!record) {
                arr = NULL;
                break;
            }
            if (napi_set_element(env, arr, (uint32_t)i, record) != napi_ok) {
                arr = throw_napi(env);
                break;
            }
        }
    }
    weida_dish_drops_drop(d);
    return arr;
}

/* Copies a weida_received out and drops it: `{ kind, transfer }` for a
 * stream segment, `{ kind, topic, segment, payload }` for a datagram. */
static napi_value received_value(napi_env env, void *ptr) {
    weida_received *r = ptr;
    napi_value obj = NULL;
    size_t len = 0;
    uint64_t n = 0;
    int kind = weida_received_kind(r);
    if (napi_create_object(env, &obj) != napi_ok) {
        weida_received_drop(r);
        return throw_napi(env);
    }
    bool ok = set(env, obj, "kind", js_string(env, received_name(kind), NAPI_AUTO_LENGTH));
    if (kind == WEIDA_RECEIVED_SEGMENT) {
        weida_incoming_transfer *t = NULL;
        int rc = weida_received_take_segment(r, &t);
        if (rc < 0) {
            napi_value e = throw_rc(env, rc);
            weida_received_drop(r);
            return e;
        }
        weida_received_drop(r);
        napi_value transfer = wrap(env, t, drop_incoming_transfer);
        return ok && transfer && set(env, obj, "transfer", transfer) ? obj : NULL;
    }
    const char *s = weida_received_topic(r, &len);
    ok = ok && set(env, obj, "topic", js_string(env, s, len));
    int present = weida_received_segment(r, &n);
    ok = ok && set(env, obj, "segment", optional_u64(env, present, n));
    const uint8_t *payload = weida_received_payload(r, &len);
    ok = ok && set(env, obj, "payload", bytes_value(env, payload, len));
    weida_received_drop(r);
    return ok ? obj : NULL;
}

static napi_value listener_radio(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    weida_radio *out = NULL;
    int rc = weida_listener_radio(l, path, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_radio);
}

STRING_OUT(radio_path, radio, path)

/* (radio, topic, terms | null) -> segment */
static napi_value radio_segment(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_radio *r = live(env, argv[0], drop_radio);
    void *terms = NULL;
    if (!r || !optional(env, argv[2], drop_segment_terms, &terms))
        return NULL;
    char *topic = get_string(env, argv[1]);
    if (!topic)
        return NULL;
    weida_segment *out = NULL;
    int rc = weida_radio_segment(r, topic, terms, &out);
    free(topic);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_segment);
}

/* (radio, topic, bytes) -> number: the dishes it was handed to */
static napi_value radio_datagram(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_radio *r = live(env, argv[0], drop_radio);
    const uint8_t *payload = NULL;
    size_t len = 0, n = 0;
    if (!r || !get_bytes(env, argv[2], &payload, &len))
        return NULL;
    char *topic = get_string(env, argv[1]);
    if (!topic)
        return NULL;
    int rc = weida_radio_datagram(r, topic, payload, len, &n);
    free(topic);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

SIZE_OUT(radio_dish_count, radio, dish_count)
U64_OUT(radio_dropped, radio, dropped)

static napi_value radio_dropped_on(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_radio *r = live(env, argv[0], drop_radio);
    if (!r)
        return NULL;
    char *topic = get_string(env, argv[1]);
    if (!topic)
        return NULL;
    weida_topic_drops *d = NULL;
    int rc = weida_radio_dropped_on(r, topic, &d);
    free(topic);
    return topic_drops_array(env, rc, d);
}

static napi_value radio_drops(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_radio *r = live(env, argv[0], drop_radio);
    weida_topic_drops *d = NULL;
    if (!r)
        return NULL;
    int rc = weida_radio_drops(r, &d);
    return topic_drops_array(env, rc, d);
}

/* (radio, fingerprint, filter) -> number: the joins that went */
static napi_value radio_evict(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_radio *r = live(env, argv[0], drop_radio);
    if (!r)
        return NULL;
    char *peer = get_string(env, argv[1]);
    if (!peer)
        return NULL;
    char *filter = get_string(env, argv[2]);
    if (!filter) {
        free(peer);
        return NULL;
    }
    size_t n = 0;
    int rc = weida_radio_evict(r, peer, filter, &n);
    free(filter);
    free(peer);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

static napi_value radio_dish_drops(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_radio *r = live(env, argv[0], drop_radio);
    weida_dish_drops *d = NULL;
    if (!r)
        return NULL;
    int rc = weida_radio_dish_drops(r, &d);
    return dish_drops_array(env, rc, d);
}

U64_OUT(segment_number, segment, number)
STRING_OUT(segment_topic, segment, topic)

/* (segment, bytes) -> number: the copies that took the chunk */
static napi_value segment_write(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_segment *s = live(env, argv[0], drop_segment);
    const uint8_t *chunk = NULL;
    size_t len = 0, n = 0;
    if (!s || !get_bytes(env, argv[1], &chunk, &len))
        return NULL;
    int rc = weida_segment_write(s, chunk, len, &n);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

/* (segment, layer, bytes) -> number: the copies that took the chunk */
static napi_value segment_write_layer(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_segment *s = live(env, argv[0], drop_segment);
    uint64_t layer = 0;
    const uint8_t *chunk = NULL;
    size_t len = 0, n = 0;
    if (!s || !get_u64(env, argv[1], UINT8_MAX, &layer) ||
        !get_bytes(env, argv[2], &chunk, &len))
        return NULL;
    int rc = weida_segment_write_layer(s, (uint8_t)layer, chunk, len, &n);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

static napi_value segment_finish_layer(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_segment *s = live(env, argv[0], drop_segment);
    uint64_t layer = 0;
    if (!s || !get_u64(env, argv[1], UINT8_MAX, &layer))
        return NULL;
    int rc = weida_segment_finish_layer(s, (uint8_t)layer);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

/* consumes the segment -> number: the copies still holding layer 0 */
static napi_value segment_finish(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_segment *s = take(env, argv[0], drop_segment);
    size_t n = 0;
    if (!s)
        return NULL;
    int rc = weida_segment_finish(s, &n);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

static napi_value runtime_dish(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_runtime *rt = live(env, argv[0], drop_runtime);
    if (!rt)
        return NULL;
    const weida_client_tls *tls = live(env, argv[1], drop_client_tls);
    weida_dish *out = NULL;
    if (!tls)
        return NULL;
    int rc = weida_runtime_dish(rt, tls, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_dish);
}

STRING_OP(dish_connect, dish, connect)
FD(dish_connect_op_fd, dish_connect_op)
WAIT0(dish_connect_op_wait, dish_connect_op)
SIZE_OUT(dish_peer_count, dish, peer_count)
CONNECTION_STATS(dish_connection_stats, dish)
EVENTS(dish_events, dish)

/* (dish, filter, terms | null) -> op */
static napi_value dish_join(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_dish *d = live(env, argv[0], drop_dish);
    void *terms = NULL;
    if (!d || !optional(env, argv[2], drop_join_terms, &terms))
        return NULL;
    char *filter = get_string(env, argv[1]);
    if (!filter)
        return NULL;
    weida_dish_join_op *op = NULL;
    int rc = weida_dish_join(d, filter, terms, &op);
    free(filter);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_dish_join_op);
}

FD(dish_join_op_fd, dish_join_op)
WAIT0(dish_join_op_wait, dish_join_op)
STRING_OP(dish_leave, dish, leave)
FD(dish_leave_op_fd, dish_leave_op)
WAIT0(dish_leave_op_wait, dish_leave_op)
FD(dish_fd, dish)
U64_OUT(dish_stale, dish, stale)
U64_OUT(dish_overflow, dish, overflow)

static napi_value dish_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_dish *d = live(env, argv[0], drop_dish);
    weida_received *out = NULL;
    if (!d)
        return NULL;
    int rc = weida_dish_recv(d, 0, &out);
    return outcome(env, rc, received_value, out);
}

/* cursors */

/* -> Cursors handle | null; null once taken and when nothing was ordered */
static napi_value outgoing_transfer_cursors(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_outgoing_transfer *t = live(env, argv[0], drop_outgoing_transfer);
    weida_cursors *out = NULL;
    if (!t)
        return NULL;
    int rc = weida_outgoing_transfer_cursors(t, &out);
    if (rc < 0)
        return throw_rc(env, rc);
    return rc == 0 ? js_null(env) : wrap(env, out, drop_cursors);
}

/* -> Reporter handle | null; null when nothing was ordered */
static napi_value incoming_transfer_reporter(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_incoming_transfer *t = live(env, argv[0], drop_incoming_transfer);
    weida_reporter *out = NULL;
    if (!t)
        return NULL;
    int rc = weida_incoming_transfer_reporter(t, &out);
    if (rc < 0)
        return throw_rc(env, rc);
    return rc == 0 ? js_null(env) : wrap(env, out, drop_reporter);
}

FD(cursors_fd, cursors)

/* Copies a weida_cursor_set out into an array of `{ level, offset }` in
 * ascending level order and drops it. */
static napi_value cursor_set_value(napi_env env, void *ptr) {
    weida_cursor_set *s = ptr;
    size_t n = weida_cursor_set_len(s);
    napi_value arr = NULL;
    bool ok = napi_create_array_with_length(env, n, &arr) == napi_ok;
    if (!ok)
        throw_napi(env);
    for (size_t i = 0; ok && i < n; i++) {
        uint64_t level = 0, offset = 0;
        napi_value entry = NULL;
        weida_cursor_set_level(s, i, &level, &offset);
        ok = napi_create_object(env, &entry) == napi_ok;
        if (!ok) {
            throw_napi(env);
            break;
        }
        ok = set(env, entry, "level", level_value(env, level)) &&
             set(env, entry, "offset", optional_u64(env, 1, offset));
        if (ok && napi_set_element(env, arr, (uint32_t)i, entry) != napi_ok) {
            throw_napi(env);
            ok = false;
        }
    }
    weida_cursor_set_drop(s);
    return ok ? arr : NULL;
}

/* -> set | null | AGAIN; null once no more cursors come */
static napi_value cursors_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_cursors *c = live(env, argv[0], drop_cursors);
    weida_cursor_set *s = NULL;
    if (!c)
        return NULL;
    int rc = weida_cursors_recv(c, 0, &s);
    if (rc == 0 && !s)
        return js_null(env);
    return outcome(env, rc, cursor_set_value, s);
}

static napi_value reporter_levels(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_reporter *r = live(env, argv[0], drop_reporter);
    size_t len = 0;
    if (!r)
        return NULL;
    const uint64_t *levels = weida_reporter_levels(r, &len);
    return levels_value(env, levels, len);
}

static napi_value reporter_mode(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_reporter *r = live(env, argv[0], drop_reporter);
    if (!r)
        return NULL;
    int mode = weida_reporter_mode(r);
    if (mode < 0)
        return throw_rc(env, mode);
    return js_string(env, report_mode_name(mode), NAPI_AUTO_LENGTH);
}

/* (reporter, bytes, intervalMs) */
static napi_value reporter_set_granularity(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_reporter *r = live(env, argv[0], drop_reporter);
    uint64_t bytes = 0, interval_ms = 0;
    if (!r || !get_u64(env, argv[1], UINT64_MAX, &bytes) ||
        !get_u64(env, argv[2], UINT64_MAX, &interval_ms))
        return NULL;
    int rc = weida_reporter_set_granularity(r, bytes, interval_ms);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

/* (reporter, level, offset) -> op */
static napi_value reporter_report(napi_env env, napi_callback_info info) {
    ARGS(3);
    const weida_reporter *r = live(env, argv[0], drop_reporter);
    uint64_t level = 0, offset = 0;
    if (!r || !get_u64(env, argv[1], UINT64_MAX, &level) ||
        !get_u64(env, argv[2], UINT64_MAX, &offset))
        return NULL;
    weida_reporter_report_op *op = NULL;
    int rc = weida_reporter_report(r, level, offset, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_reporter_report_op);
}

FD(reporter_report_op_fd, reporter_report_op)
WAIT0(reporter_report_op_wait, reporter_report_op)

/* consumes the reporter */
static napi_value reporter_finish(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_reporter *r = take(env, argv[0], drop_reporter);
    weida_reporter_finish_op *op = NULL;
    if (!r)
        return NULL;
    int rc = weida_reporter_finish(r, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_reporter_finish_op);
}

FD(reporter_finish_op_fd, reporter_finish_op)
WAIT0(reporter_finish_op_wait, reporter_finish_op)

/* flows */

static napi_value runtime_peer(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_runtime *rt = live(env, argv[0], drop_runtime);
    if (!rt)
        return NULL;
    const weida_client_tls *tls = live(env, argv[1], drop_client_tls);
    weida_peer *out = NULL;
    if (!tls)
        return NULL;
    int rc = weida_runtime_peer(rt, tls, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_peer);
}

STRING_OP(peer_connect, peer, connect)
FD(peer_connect_op_fd, peer_connect_op)
WAIT0(peer_connect_op_wait, peer_connect_op)

/* (peer, meta | null) -> op */
static napi_value peer_open_flow(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_peer *p = live(env, argv[0], drop_peer);
    void *meta = NULL;
    weida_peer_open_flow_op *op = NULL;
    if (!p || !optional(env, argv[1], drop_flow_meta, &meta))
        return NULL;
    int rc = weida_peer_open_flow(p, meta, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_peer_open_flow_op);
}

static napi_value wrap_flow(napi_env env, void *p) { return wrap(env, p, drop_flow); }

FD(peer_open_flow_op_fd, peer_open_flow_op)
WAIT1(peer_open_flow_op_wait, peer_open_flow_op, weida_flow, wrap_flow)
NEW0(flow_meta_new, flow_meta)
SET_TEXT(flow_meta_set_content_type, flow_meta, content_type)
SET_TEXT(flow_meta_set_trace, flow_meta, trace)
SET_TEXT(flow_meta_set_topic, flow_meta, topic)

static napi_value listener_acceptor(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_listener *l = live(env, argv[0], drop_listener);
    if (!l)
        return NULL;
    char *path = get_string(env, argv[1]);
    if (!path)
        return NULL;
    weida_acceptor *out = NULL;
    int rc = weida_listener_acceptor(l, path, &out);
    free(path);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_acceptor);
}

STRING_OUT(acceptor_path, acceptor, path)
FD(acceptor_fd, acceptor)

/* Copies a weida_incoming out and drops it: `{ kind, flow }` for a flow,
 * `{ kind }` for the kinds not mapped yet, whose payload the drop drops. */
static napi_value incoming_value(napi_env env, void *ptr) {
    weida_incoming *i = ptr;
    napi_value obj = NULL;
    int kind = weida_incoming_kind(i);
    if (napi_create_object(env, &obj) != napi_ok) {
        weida_incoming_drop(i);
        return throw_napi(env);
    }
    bool ok = set(env, obj, "kind", js_string(env, incoming_name(kind), NAPI_AUTO_LENGTH));
    if (kind == WEIDA_INCOMING_FLOW) {
        weida_incoming_flow *f = NULL;
        int rc = weida_incoming_take_flow(i, &f);
        if (rc < 0) {
            napi_value e = throw_rc(env, rc);
            weida_incoming_drop(i);
            return e;
        }
        weida_incoming_drop(i);
        napi_value flow = wrap(env, f, drop_incoming_flow);
        return ok && flow && set(env, obj, "flow", flow) ? obj : NULL;
    }
    weida_incoming_drop(i);
    return ok ? obj : NULL;
}

static napi_value acceptor_accept(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_acceptor *a = live(env, argv[0], drop_acceptor);
    weida_incoming *out = NULL;
    if (!a)
        return NULL;
    int rc = weida_acceptor_accept(a, 0, &out);
    return outcome(env, rc, incoming_value, out);
}

static napi_value flow_send(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_flow *f = live(env, argv[0], drop_flow);
    const uint8_t *payload = NULL;
    size_t len = 0;
    if (!f || !get_bytes(env, argv[1], &payload, &len))
        return NULL;
    int rc = weida_flow_send(f, payload, len);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

/* (flow) -> number | null */
static napi_value flow_max_payload(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_flow *f = live(env, argv[0], drop_flow);
    size_t n = 0;
    if (!f)
        return NULL;
    return weida_flow_max_payload(f, &n) == 1 ? js_int64(env, (int64_t)n) : js_null(env);
}

U64_OUT(flow_sent, flow, sent)
U64_OUT(flow_too_large, flow, too_large)
U64_OUT(flow_discarded, flow, discarded)
U64_OUT(flow_not_live, flow, not_live)

/* Copies a weida_path_stats out into a plain object of BigInts and drops it. */
static napi_value path_stats_value(napi_env env, weida_path_stats *s) {
    napi_value obj = NULL;
    uint64_t n = 0;
    if (napi_create_object(env, &obj) != napi_ok) {
        weida_path_stats_drop(s);
        return throw_napi(env);
    }
    bool ok = set(env, obj, "rttUs", optional_u64(env, 1, weida_path_stats_rtt_us(s)));
    ok = ok && set(env, obj, "minRttUs", optional_u64(env, 1, weida_path_stats_min_rtt_us(s)));
    ok = ok && set(env, obj, "cwnd", optional_u64(env, 1, weida_path_stats_cwnd(s)));
    ok = ok && set(env, obj, "congestionEvents",
                   optional_u64(env, 1, weida_path_stats_congestion_events(s)));
    ok = ok &&
         set(env, obj, "lostPackets", optional_u64(env, 1, weida_path_stats_lost_packets(s)));
    ok = ok && set(env, obj, "lostBytes", optional_u64(env, 1, weida_path_stats_lost_bytes(s)));
    ok = ok &&
         set(env, obj, "sentPackets", optional_u64(env, 1, weida_path_stats_sent_packets(s)));
    ok = ok && set(env, obj, "currentMtu", optional_u64(env, 1, weida_path_stats_current_mtu(s)));
    int present = weida_path_stats_max_datagram_size(s, &n);
    ok = ok && set(env, obj, "maxDatagramSize", optional_u64(env, present, n));
    weida_path_stats_drop(s);
    return ok ? obj : NULL;
}

/* What a path_stats call returned: the error, null, or the snapshot. */
static napi_value path_stats_outcome(napi_env env, int rc, weida_path_stats *s) {
    if (rc < 0)
        return throw_rc(env, rc);
    return rc == 1 ? path_stats_value(env, s) : js_null(env);
}

static napi_value flow_path_stats(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_flow *f = live(env, argv[0], drop_flow);
    weida_path_stats *s = NULL;
    if (!f)
        return NULL;
    return path_stats_outcome(env, weida_flow_path_stats(f, &s), s);
}

/* consumes the flow */
static napi_value flow_abort(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_flow *f = take(env, argv[0], drop_flow);
    if (!f)
        return NULL;
    weida_flow_abort(f);
    return js_undefined(env);
}

static napi_value incoming_flow_info(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_incoming_flow *f = live(env, argv[0], drop_incoming_flow);
    weida_incoming_meta *m = NULL;
    if (!f)
        return NULL;
    int rc = weida_incoming_flow_info(f, &m);
    return rc < 0 ? throw_rc(env, rc) : meta_value(env, m);
}

FD(incoming_flow_fd, incoming_flow)

/* Copies a weida_datagram out into a Uint8Array and drops it. */
static napi_value datagram_value(napi_env env, void *ptr) {
    weida_datagram *d = ptr;
    napi_value v = bytes_value(env, weida_datagram_data(d), weida_datagram_len(d));
    weida_datagram_drop(d);
    return v;
}

/* (flow) -> Uint8Array | null | AGAIN; null once the flow ended */
static napi_value incoming_flow_recv(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_incoming_flow *f = live(env, argv[0], drop_incoming_flow);
    weida_datagram *d = NULL;
    if (!f)
        return NULL;
    int rc = weida_incoming_flow_recv(f, 0, &d);
    if (rc == 0 && !d)
        return js_null(env);
    return outcome(env, rc, datagram_value, d);
}

U64_OUT(incoming_flow_received, incoming_flow, received)
U64_OUT(incoming_flow_overflow, incoming_flow, overflow)

static napi_value incoming_flow_path_stats(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_incoming_flow *f = live(env, argv[0], drop_incoming_flow);
    weida_path_stats *s = NULL;
    if (!f)
        return NULL;
    return path_stats_outcome(env, weida_incoming_flow_path_stats(f, &s), s);
}

/* consumes the incoming flow */
static napi_value incoming_flow_refuse(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_incoming_flow *f = take(env, argv[0], drop_incoming_flow);
    if (!f)
        return NULL;
    weida_incoming_flow_refuse(f);
    return js_undefined(env);
}

/* incoming transfer */

static napi_value incoming_transfer_meta(napi_env env, napi_callback_info info) {
    ARGS(1);
    const weida_incoming_transfer *t = live(env, argv[0], drop_incoming_transfer);
    weida_incoming_meta *m = NULL;
    if (!t)
        return NULL;
    int rc = weida_incoming_transfer_meta(t, &m);
    return rc < 0 ? throw_rc(env, rc) : meta_value(env, m);
}

CONNECTION(incoming_transfer_connection, incoming_transfer)

FD(incoming_transfer_fd, incoming_transfer)

/* `incomingTransferRead(h, cap) -> Uint8Array | AGAIN`: a fresh buffer of
 * `cap` bytes, viewed over the bytes read; empty at the end of the payload. */
static napi_value incoming_transfer_read(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_incoming_transfer *t = live(env, argv[0], drop_incoming_transfer);
    uint64_t cap = 0;
    napi_value ab, view;
    void *buf = NULL;
    size_t n = 0;
    if (!t || !get_u64(env, argv[1], INT32_MAX, &cap))
        return NULL;
    if (napi_create_arraybuffer(env, cap, &buf, &ab) != napi_ok)
        return throw_napi(env);
    int rc = weida_incoming_transfer_read(t, buf, cap, 0, &n);
    if (rc == -WEIDA_EAGAIN)
        return again(env);
    if (rc < 0)
        return throw_rc(env, rc);
    if (napi_create_typedarray(env, napi_uint8_array, n, ab, 0, &view) != napi_ok)
        return throw_napi(env);
    return view;
}

/* consumes the transfer */
static napi_value incoming_transfer_collect(napi_env env, napi_callback_info info) {
    ARGS(2);
    uint64_t max = 0;
    if (!get_u64(env, argv[1], UINT64_MAX, &max))
        return NULL;
    weida_incoming_transfer *t = take(env, argv[0], drop_incoming_transfer);
    weida_incoming_transfer_collect_op *op = NULL;
    if (!t)
        return NULL;
    int rc = weida_incoming_transfer_collect(t, max, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_incoming_transfer_collect_op);
}

FD(incoming_transfer_collect_op_fd, incoming_transfer_collect_op)
WAIT1(incoming_transfer_collect_op_wait, incoming_transfer_collect_op, weida_msg, msg_value)

/* outgoing transfer and delivery */

FD(outgoing_transfer_fd, outgoing_transfer)

static napi_value outgoing_transfer_write(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_outgoing_transfer *t = live(env, argv[0], drop_outgoing_transfer);
    const uint8_t *data = NULL;
    size_t len = 0, n = 0;
    if (!t || !get_bytes(env, argv[1], &data, &len))
        return NULL;
    int rc = weida_outgoing_transfer_write(t, data, len, 0, &n);
    if (rc == -WEIDA_EAGAIN)
        return again(env);
    return rc < 0 ? throw_rc(env, rc) : js_int64(env, (int64_t)n);
}

STRING_OUT(outgoing_transfer_trace, outgoing_transfer, trace)

static napi_value outgoing_transfer_set_priority(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_outgoing_transfer *t = live(env, argv[0], drop_outgoing_transfer);
    int32_t priority = 0;
    if (!t || !get_i32(env, argv[1], &priority))
        return NULL;
    int rc = weida_outgoing_transfer_set_priority(t, priority);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

static napi_value outgoing_transfer_expire_after_ms(napi_env env, napi_callback_info info) {
    ARGS(2);
    const weida_outgoing_transfer *t = live(env, argv[0], drop_outgoing_transfer);
    uint64_t ms = 0;
    if (!t || !get_u64(env, argv[1], UINT64_MAX, &ms))
        return NULL;
    int rc = weida_outgoing_transfer_expire_after_ms(t, ms);
    return rc < 0 ? throw_rc(env, rc) : js_undefined(env);
}

/* consumes the transfer */
static napi_value outgoing_transfer_finish(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_outgoing_transfer *t = take(env, argv[0], drop_outgoing_transfer);
    weida_delivery *out = NULL;
    if (!t)
        return NULL;
    int rc = weida_outgoing_transfer_finish(t, &out);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, out, drop_delivery);
}

/* consumes the receipt */
static napi_value delivery_delivered(napi_env env, napi_callback_info info) {
    ARGS(1);
    weida_delivery *d = take(env, argv[0], drop_delivery);
    weida_delivery_delivered_op *op = NULL;
    if (!d)
        return NULL;
    int rc = weida_delivery_delivered(d, &op);
    return rc < 0 ? throw_rc(env, rc) : wrap(env, op, drop_delivery_delivered_op);
}

FD(delivery_delivered_op_fd, delivery_delivered_op)
WAIT0(delivery_delivered_op_wait, delivery_delivered_op)

/* `CONSTANTS`: the enumeration constants of weida.h by name (R7), frozen. */
static napi_value constants(napi_env env) {
    napi_value obj;
    if (napi_create_object(env, &obj) != napi_ok)
        return throw_napi(env);
#define CONSTANT(c) \
    if (!set(env, obj, "WEIDA_" #c, js_int64(env, WEIDA_##c))) \
        return NULL;
    CONSTANT(CONGESTION_CUBIC)
    CONSTANT(CONGESTION_NEW_RENO)
    CONSTANT(CONGESTION_BBR)
    CONSTANT(DISCOVERY_AWARE)
    CONSTANT(DISCOVERY_SINGLE)
    CONSTANT(OUTBOX_FULL_BLOCK)
    CONSTANT(OUTBOX_FULL_DROP)
    CONSTANT(OUTBOX_FULL_REJECT)
    CONSTANT(DELIVERY_LEVEL_BEST_EFFORT)
    CONSTANT(DELIVERY_LEVEL_AT_MOST_ONCE)
    CONSTANT(DELIVERY_LEVEL_AT_LEAST_ONCE)
    CONSTANT(ACKNOWLEDGEMENT_NONE)
    CONSTANT(ACKNOWLEDGEMENT_TRANSPORT_RECEIPT)
    CONSTANT(ACKNOWLEDGEMENT_ACCEPTED)
    CONSTANT(ACKNOWLEDGEMENT_STORED)
    CONSTANT(ACKNOWLEDGEMENT_REPLICATED)
    CONSTANT(ACKNOWLEDGEMENT_PROCESSED)
    CONSTANT(DURABILITY_WRITTEN)
    CONSTANT(DURABILITY_FLUSHED)
    CONSTANT(ORDERING_MODE_NONE)
    CONSTANT(ORDERING_MODE_PER_PRODUCER_DETECT)
    CONSTANT(ORDERING_MODE_PER_PRODUCER_REASSEMBLE)
    CONSTANT(ORDERING_MODE_PER_KEY)
    CONSTANT(ORDERING_MODE_TOTAL)
    CONSTANT(DEDUPLICATION_NONE)
    CONSTANT(DEDUPLICATION_BOUNDED)
    CONSTANT(DEDUPLICATION_DURABLE)
    CONSTANT(BACKPRESSURE_BLOCK)
    CONSTANT(BACKPRESSURE_REJECT)
    CONSTANT(BACKPRESSURE_DROP)
    CONSTANT(BACKPRESSURE_SPILL)
    CONSTANT(BACKPRESSURE_COALESCE)
    CONSTANT(PRODUCER_NAMING_FINGERPRINT)
    CONSTANT(PRODUCER_NAMING_STABLE)
    CONSTANT(ERROR_CODE_UNKNOWN_ENDPOINT)
    CONSTANT(ERROR_CODE_REJECTED)
    CONSTANT(ERROR_CODE_UNSUPPORTED)
    CONSTANT(ERROR_CODE_INTERNAL)
    CONSTANT(ERROR_CODE_NO_REPLY)
    CONSTANT(PEER_EVENT_CONNECTED)
    CONSTANT(PEER_EVENT_LOST)
    CONSTANT(PEER_EVENT_RETRYING)
    CONSTANT(PEER_EVENT_GAVE_UP)
    CONSTANT(PEER_EVENT_MISSED)
    CONSTANT(GIVE_UP_POLICY)
    CONSTANT(GIVE_UP_PEER_CHANGED)
    CONSTANT(GIVE_UP_FAILED)
    CONSTANT(ALPN_WEIDA)
    CONSTANT(ALPN_RAFT)
    CONSTANT(IDENTITY_EVENT_LOADED)
    CONSTANT(IDENTITY_EVENT_RENEWED)
    CONSTANT(IDENTITY_EVENT_KEY_CHANGED)
    CONSTANT(IDENTITY_EVENT_RENEWAL_FAILED)
    CONSTANT(IDENTITY_EVENT_HANDOFF_STOLEN)
    CONSTANT(IDENTITY_EVENT_TRUST_REFRESHED)
    CONSTANT(IDENTITY_EVENT_MISSED)
    CONSTANT(RECEIVED_SEGMENT)
    CONSTANT(RECEIVED_DATAGRAM)
    CONSTANT(MAX_SEGMENT_LAYERS)
    CONSTANT(CURSOR_LEVEL_APPLICATION_FLOOR)
    CONSTANT(REPORT_MODE_PROGRESS)
    CONSTANT(REPORT_MODE_FINAL_ONLY)
    CONSTANT(REPORTER_DEFAULT_BYTES)
    CONSTANT(REPORTER_DEFAULT_INTERVAL_MS)
    CONSTANT(INCOMING_STREAM)
    CONSTANT(INCOMING_EXCHANGE)
    CONSTANT(INCOMING_SUBSCRIBED)
    CONSTANT(INCOMING_CREDIT)
    CONSTANT(INCOMING_UNSUBSCRIBED)
    CONSTANT(INCOMING_FLOW)
    CONSTANT(DEFAULT_DATAGRAM_RECEIVE_BYTES)
#undef CONSTANT
    if (napi_object_freeze(env, obj) != napi_ok)
        return throw_napi(env);
    return obj;
}

/* ---- the module ---- */

void addon_release(struct addon *a) {
    if (--a->owners > 0)
        return;
    waiter_free(&a->waiter);
    free(a);
}

static void finalize_addon(napi_env env, void *data, void *hint) {
    (void)env;
    (void)hint;
    addon_release(data);
}

#define FN(js, c) {js, NULL, c, NULL, NULL, NULL, napi_enumerable, NULL}

NAPI_MODULE_INIT() {
#ifdef _WIN32
    if (!load_libweida()) {
        napi_throw_error(env, NULL, "weida: weida.dll beside weida.node could not be loaded");
        return NULL;
    }
#endif
    struct addon *a = calloc(1, sizeof *a);
    napi_value again_obj, constants_obj;
    if (!a) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    a->owners = 1;
    waiter_init(&a->waiter);
    if (napi_set_instance_data(env, a, finalize_addon, NULL) != napi_ok) {
        waiter_free(&a->waiter);
        free(a);
        return throw_napi(env);
    }
    if (napi_create_object(env, &again_obj) != napi_ok ||
        napi_object_freeze(env, again_obj) != napi_ok ||
        napi_create_reference(env, again_obj, 1, &a->again) != napi_ok)
        return throw_napi(env);
    constants_obj = constants(env);
    if (!constants_obj)
        return NULL;

    napi_property_descriptor props[] = {
        {"AGAIN", NULL, NULL, NULL, NULL, again_obj, napi_enumerable, NULL},
        {"CONSTANTS", NULL, NULL, NULL, NULL, constants_obj, napi_enumerable, NULL},
        FN("version", version),
        FN("strerror", strerror_),
        FN("error", error_),
        FN("setErrorClass", set_error_class),
        FN("drop", drop_),
        FN("watch", waiter_watch),
        FN("unwatch", waiter_unwatch),
        FN("newTrace", new_trace),
        FN("runtimeConfigNew", runtime_config_new),
        FN("runtimeConfigSetWorkerThreads", runtime_config_set_worker_threads),
        FN("runtimeConfigSetShutdownTimeoutMs", runtime_config_set_shutdown_timeout_ms),
        FN("runtimeConfigSetMaxConnections", runtime_config_set_max_connections),
        FN("runtimeConfigSetMaxConnectionsPerPeer", runtime_config_set_max_connections_per_peer),
        FN("runtimeConfigSetEndpointQueue", runtime_config_set_endpoint_queue),
        FN("runtimeConfigSetMaxResolvedAddresses", runtime_config_set_max_resolved_addresses),
        FN("runtimeConfigSetConnectAttemptTimeoutMs",
           runtime_config_set_connect_attempt_timeout_ms),
        FN("runtimeConfigSetSendTimeoutMs", runtime_config_set_send_timeout_ms),
        FN("runtimeConfigSetOutboxMessages", runtime_config_set_outbox_messages),
        FN("runtimeConfigSetOutboxBytes", runtime_config_set_outbox_bytes),
        FN("runtimeConfigSetOutboxFull", runtime_config_set_outbox_full),
        FN("runtimeConfigSetMaxHandshakes", runtime_config_set_max_handshakes),
        FN("runtimeConfigSetRetryAboveHandshakes", runtime_config_set_retry_above_handshakes),
        FN("runtimeConfigSetDiscovery", runtime_config_set_discovery),
        FN("runtimeConfigSetLimits", runtime_config_set_limits),
        FN("runtimeConfigSetGuarantees", runtime_config_set_guarantees),
        FN("runtimeConfigSetReconnect", runtime_config_set_reconnect),
        FN("runtimeConfigSetResolver", runtime_config_set_resolver),
        FN("limitsNew", limits_new),
        FN("limitsSetMaxHeaderBytes", limits_set_max_header_bytes),
        FN("limitsSetMaxConcurrentUniStreams", limits_set_max_concurrent_uni_streams),
        FN("limitsSetMaxConcurrentBidiStreams", limits_set_max_concurrent_bidi_streams),
        FN("limitsSetStreamReceiveWindow", limits_set_stream_receive_window),
        FN("limitsSetConnectionReceiveWindow", limits_set_connection_receive_window),
        FN("limitsSetKeepAliveMs", limits_set_keep_alive_ms),
        FN("limitsSetIdleTimeoutMs", limits_set_idle_timeout_ms),
        FN("limitsSetHelloTimeoutMs", limits_set_hello_timeout_ms),
        FN("limitsSetMaxSubscriptions", limits_set_max_subscriptions),
        FN("limitsSetSubscriberBufferBytes", limits_set_subscriber_buffer_bytes),
        FN("limitsSetMaxSequenceScopes", limits_set_max_sequence_scopes),
        FN("limitsSetMaxReorderHold", limits_set_max_reorder_hold),
        FN("limitsSetMaxLocalStreams", limits_set_max_local_streams),
        FN("limitsSetMaxParkedReverse", limits_set_max_parked_reverse),
        FN("limitsSetMaxDedupEntries", limits_set_max_dedup_entries),
        FN("limitsSetDatagramReceiveBytes", limits_set_datagram_receive_bytes),
        FN("limitsSetDatagramSendBytes", limits_set_datagram_send_bytes),
        FN("limitsSetPathReport", limits_set_path_report),
        FN("limitsSetMaxFlows", limits_set_max_flows),
        FN("limitsSetFlowQueueBytes", limits_set_flow_queue_bytes),
        FN("limitsSetFlowEarlyBytes", limits_set_flow_early_bytes),
        FN("limitsSetFlowEarlyHoldMs", limits_set_flow_early_hold_ms),
        FN("limitsSetCongestion", limits_set_congestion),
        FN("limitsSetHeaderTimeoutMs", limits_set_header_timeout_ms),
        FN("limitsSetMaxHeaderStreams", limits_set_max_header_streams),
        FN("guaranteeSetNew", guarantee_set_new),
        FN("guaranteeSetSetDelivery", guarantee_set_set_delivery),
        FN("guaranteeSetSetAcknowledgement", guarantee_set_set_acknowledgement),
        FN("guaranteeSetSetDurability", guarantee_set_set_durability),
        FN("guaranteeSetSetReplicas", guarantee_set_set_replicas),
        FN("guaranteeSetSetOrdering", guarantee_set_set_ordering),
        FN("guaranteeSetSetDeduplication", guarantee_set_set_deduplication),
        FN("guaranteeSetSetDedupWindowMs", guarantee_set_set_dedup_window_ms),
        FN("guaranteeSetSetBackpressure", guarantee_set_set_backpressure),
        FN("guaranteeSetSetProducerNaming", guarantee_set_set_producer_naming),
        FN("guaranteeSetSetControlIsolated", guarantee_set_set_control_isolated),
        FN("reconnectPolicyNew", reconnect_policy_new),
        FN("reconnectPolicyNever", reconnect_policy_never),
        FN("reconnectPolicySetInitialMs", reconnect_policy_set_initial_ms),
        FN("reconnectPolicySetMaxMs", reconnect_policy_set_max_ms),
        FN("reconnectPolicySetJitter", reconnect_policy_set_jitter),
        FN("reconnectPolicySetMaxAttempts", reconnect_policy_set_max_attempts),
        FN("reconnectPolicySetStopOnPeerClosed", reconnect_policy_set_stop_on_peer_closed),
        FN("runtimeOwned", runtime_owned),
        FN("runtimeListener", runtime_listener),
        FN("runtimeRequester", runtime_requester),
        FN("runtimeShutdown", runtime_shutdown),
        FN("runtimeShutdownOpFd", runtime_shutdown_op_fd),
        FN("runtimeShutdownOpWait", runtime_shutdown_op_wait),
        FN("runtimeDrain", runtime_drain),
        FN("runtimeDrainOpFd", runtime_drain_op_fd),
        FN("runtimeDrainOpWait", runtime_drain_op_wait),
        FN("runtimeSuppressedDuplicates", runtime_suppressed_duplicates),
        FN("runtimeExpiredTransfers", runtime_expired_transfers),
        FN("trustNew", trust_new),
        FN("trustAddPin", trust_add_pin),
        FN("trustAddAnchor", trust_add_anchor),
        FN("trustAddAnchorFile", trust_add_anchor_file),
        FN("identityGenerate", identity_generate),
        FN("identityGenerateFor", identity_generate_for),
        FN("identityFromPem", identity_from_pem),
        FN("identityFromPemFile", identity_from_pem_file),
        FN("identityFromPemFiles", identity_from_pem_files),
        FN("identityFingerprint", identity_fingerprint),
        FN("identityCertificatePem", identity_certificate_pem),
        FN("identityToPem", identity_to_pem),
        FN("clientTlsNew", client_tls_new),
        FN("clientTlsSetIdentity", client_tls_set_identity),
        FN("serverTlsNew", server_tls_new),
        FN("serverTlsRequireClient", server_tls_require_client),
        FN("serverTlsRequireAnyKey", server_tls_require_any_key),
        FN("trustIsEmpty", trust_is_empty),
        FN("clientTlsNewSource", client_tls_new_source),
        FN("clientTlsSetIdentitySource", client_tls_set_identity_source),
        FN("clientTlsSetAlpn", client_tls_set_alpn),
        FN("serverTlsNewSource", server_tls_new_source),
        FN("serverTlsRequireClientSource", server_tls_require_client_source),
        FN("serverTlsSetAlpn", server_tls_set_alpn),
        FN("filesOptionsNew", files_options_new),
        FN("filesOptionsSetNames", files_options_set_names),
        FN("filesOptionsSetPollMs", files_options_set_poll_ms),
        FN("identitySourceFromIdentity", identity_source_from_identity),
        FN("identitySourceEphemeral", identity_source_ephemeral),
        FN("identitySourceExternal", identity_source_external),
        FN("identitySourceFiles", identity_source_files),
        FN("identitySourceCurrent", identity_source_current),
        FN("identitySourceFingerprint", identity_source_fingerprint),
        FN("identitySourceUpdate", identity_source_update),
        FN("identitySourceReportFailure", identity_source_report_failure),
        FN("identitySourceReportHandoffStolen", identity_source_report_handoff_stolen),
        FN("identitySourceReload", identity_source_reload),
        FN("identitySourceEvents", identity_source_events),
        FN("identitySourceDowngrade", identity_source_downgrade),
        FN("weakIdentitySourceUpgrade", weak_identity_source_upgrade),
        FN("trustSourceFromTrust", trust_source_from_trust),
        FN("trustSourceExternal", trust_source_external),
        FN("trustSourceCurrent", trust_source_current),
        FN("trustSourceUpdate", trust_source_update),
        FN("trustSourceReportFailure", trust_source_report_failure),
        FN("trustSourceIsEmpty", trust_source_is_empty),
        FN("trustSourceEvents", trust_source_events),
        FN("trustSourceDowngrade", trust_source_downgrade),
        FN("weakTrustSourceUpgrade", weak_trust_source_upgrade),
        FN("transferMetaNew", transfer_meta_new),
        FN("transferMetaSetContentType", transfer_meta_set_content_type),
        FN("transferMetaSetContentLen", transfer_meta_set_content_len),
        FN("transferMetaSetTopic", transfer_meta_set_topic),
        FN("transferMetaSetTrace", transfer_meta_set_trace),
        FN("transferMetaSetAchieved", transfer_meta_set_achieved),
        FN("transferMetaSetReport", transfer_meta_set_report),
        FN("transferMetaSetReportMode", transfer_meta_set_report_mode),
        FN("listenerBindQuic", listener_bind_quic),
        FN("listenerBindQuicOpFd", listener_bind_quic_op_fd),
        FN("listenerBindQuicOpWait", listener_bind_quic_op_wait),
        FN("listenerBindQuicSocket", listener_bind_quic_socket),
        FN("listenerBindQuicSocketOpFd", listener_bind_quic_socket_op_fd),
        FN("listenerBindQuicSocketOpWait", listener_bind_quic_socket_op_wait),
        FN("listenerBindRelay", listener_bind_relay),
        FN("listenerBindRelayOpFd", listener_bind_relay_op_fd),
        FN("listenerBindRelayOpWait", listener_bind_relay_op_wait),
        FN("listenerRelay", listener_relay),
        FN("listenerBindInproc", listener_bind_inproc),
        FN("listenerBindUnix", listener_bind_unix),
        FN("listenerBindPipe", listener_bind_pipe),
        FN("listenerReplier", listener_replier),
        FN("bindingLocalAddr", binding_local_addr),
        FN("bindingClose", binding_close),
        FN("bindingCloseOpFd", binding_close_op_fd),
        FN("bindingCloseOpWait", binding_close_op_wait),
        FN("bindingHandOver", binding_hand_over),
        FN("bindingHandOverOpFd", binding_hand_over_op_fd),
        FN("bindingHandOverOpWait", binding_hand_over_op_wait),
        FN("bindingTakeOver", binding_take_over),
        FN("bindingTakeOverOpFd", binding_take_over_op_fd),
        FN("bindingTakeOverOpWait", binding_take_over_op_wait),
        FN("bindingDisconnect", binding_disconnect),
        FN("bindingRelayEvents", binding_relay_events),
        FN("handoffHeld", handoff_held),
        FN("handoffLocalAddr", handoff_local_addr),
        FN("handoffBytes", handoff_bytes),
        FN("handoffTakeSocket", handoff_take_socket),
        FN("localBindingBus", local_binding_bus),
        FN("unixBindingPath", unix_binding_path),
        FN("pipeBindingName", pipe_binding_name),
        FN("replierPath", replier_path),
        FN("replierFd", replier_fd),
        FN("replierAccept", replier_accept),
        FN("incomingRequestMeta", incoming_request_meta),
        FN("incomingRequestConnection", incoming_request_connection),
        FN("incomingRequestTakeBody", incoming_request_take_body),
        FN("incomingRequestReply", incoming_request_reply),
        FN("incomingRequestReplyOpFd", incoming_request_reply_op_fd),
        FN("incomingRequestReplyOpWait", incoming_request_reply_op_wait),
        FN("incomingRequestRefuse", incoming_request_refuse),
        FN("incomingRequestRefuseOpFd", incoming_request_refuse_op_fd),
        FN("incomingRequestRefuseOpWait", incoming_request_refuse_op_wait),
        FN("incomingRequestCanceled", incoming_request_canceled),
        FN("incomingRequestCanceledOpFd", incoming_request_canceled_op_fd),
        FN("incomingRequestCanceledOpWait", incoming_request_canceled_op_wait),
        FN("requesterConnect", requester_connect),
        FN("requesterConnectOpFd", requester_connect_op_fd),
        FN("requesterConnectOpWait", requester_connect_op_wait),
        FN("requesterRequest", requester_request),
        FN("requesterRequestWith", requester_request_with),
        FN("requesterRequestOpFd", requester_request_op_fd),
        FN("requesterRequestOpWait", requester_request_op_wait),
        FN("requesterOpen", requester_open),
        FN("requesterOpenOpFd", requester_open_op_fd),
        FN("requesterOpenOpWait", requester_open_op_wait),
        FN("replyStreamRecv", reply_stream_recv),
        FN("replyStreamRecvOpFd", reply_stream_recv_op_fd),
        FN("replyStreamRecvOpWait", reply_stream_recv_op_wait),
        FN("requesterConnectionStats", requester_connection_stats),
        FN("requesterEvents", requester_events),
        FN("peerEventsFd", peer_events_fd),
        FN("peerEventsRecv", peer_events_recv),
        FN("identityEventsFd", identity_events_fd),
        FN("identityEventsRecv", identity_events_recv),
        FN("relayFd", relay_fd),
        FN("relayAccept", relay_accept),
        FN("relayRequestPeerFingerprint", relay_request_peer_fingerprint),
        FN("relayRequestTarget", relay_request_target),
        FN("relayRequestAdmit", relay_request_admit),
        FN("relayRequestAdmitOpFd", relay_request_admit_op_fd),
        FN("relayRequestAdmitOpWait", relay_request_admit_op_wait),
        FN("relayRequestRefuse", relay_request_refuse),
        FN("relayRequestRefuseOpFd", relay_request_refuse_op_fd),
        FN("relayRequestRefuseOpWait", relay_request_refuse_op_wait),
        FN("resolverNew", resolver_new),
        FN("resolverFd", resolver_fd),
        FN("resolverRecv", resolver_recv),
        FN("resolvedName", resolved_name),
        FN("resolvedPort", resolved_port),
        FN("resolvedMaxAddresses", resolved_max_addresses),
        FN("resolvedAnswer", resolved_answer),
        FN("resolvedRefuse", resolved_refuse),
        FN("runtimePusher", runtime_pusher),
        FN("pusherConnect", pusher_connect),
        FN("pusherConnectOpFd", pusher_connect_op_fd),
        FN("pusherConnectOpWait", pusher_connect_op_wait),
        FN("pusherDisconnect", pusher_disconnect),
        FN("pusherPeerCount", pusher_peer_count),
        FN("pusherDropped", pusher_dropped),
        FN("pusherConnectionStats", pusher_connection_stats),
        FN("pusherEvents", pusher_events),
        FN("pusherOpen", pusher_open),
        FN("pusherOpenOpFd", pusher_open_op_fd),
        FN("pusherOpenOpWait", pusher_open_op_wait),
        FN("pusherSend", pusher_send),
        FN("pusherSendWith", pusher_send_with),
        FN("pusherSendOpFd", pusher_send_op_fd),
        FN("pusherSendOpWait", pusher_send_op_wait),
        FN("listenerPuller", listener_puller),
        FN("pullerPath", puller_path),
        FN("pullerFd", puller_fd),
        FN("pullerRecv", puller_recv),
        FN("listenerPublisher", listener_publisher),
        FN("publisherPath", publisher_path),
        FN("publisherPublish", publisher_publish),
        FN("publisherPublishWithTrace", publisher_publish_with_trace),
        FN("publisherOpen", publisher_open),
        FN("publisherOpenWithTrace", publisher_open_with_trace),
        FN("publisherSubscriberCount", publisher_subscriber_count),
        FN("publisherFilterCount", publisher_filter_count),
        FN("publisherDropped", publisher_dropped),
        FN("publisherDroppedOn", publisher_dropped_on),
        FN("publisherDrops", publisher_drops),
        FN("fanOutTopic", fan_out_topic),
        FN("fanOutSubscribers", fan_out_subscribers),
        FN("fanOutWriteWithin", fan_out_write_within),
        FN("fanOutWriteWithinOpFd", fan_out_write_within_op_fd),
        FN("fanOutWriteWithinOpWait", fan_out_write_within_op_wait),
        FN("fanOutWriteNow", fan_out_write_now),
        FN("fanOutFinish", fan_out_finish),
        FN("runtimeSubscriber", runtime_subscriber),
        FN("subscriberConnect", subscriber_connect),
        FN("subscriberConnectOpFd", subscriber_connect_op_fd),
        FN("subscriberConnectOpWait", subscriber_connect_op_wait),
        FN("subscriberDisconnect", subscriber_disconnect),
        FN("subscriberPeerCount", subscriber_peer_count),
        FN("subscriberConnectionStats", subscriber_connection_stats),
        FN("subscriberEvents", subscriber_events),
        FN("subscriberSubscribe", subscriber_subscribe),
        FN("subscriberSubscribeOpFd", subscriber_subscribe_op_fd),
        FN("subscriberSubscribeOpWait", subscriber_subscribe_op_wait),
        FN("subscriberUnsubscribe", subscriber_unsubscribe),
        FN("subscriberUnsubscribeOpFd", subscriber_unsubscribe_op_fd),
        FN("subscriberUnsubscribeOpWait", subscriber_unsubscribe_op_wait),
        FN("subscriberGrant", subscriber_grant),
        FN("subscriberGrantOpFd", subscriber_grant_op_fd),
        FN("subscriberGrantOpWait", subscriber_grant_op_wait),
        FN("subscriberFilterCount", subscriber_filter_count),
        FN("subscriberFd", subscriber_fd),
        FN("subscriberRecv", subscriber_recv),
        FN("runtimePair", runtime_pair),
        FN("listenerPair", listener_pair),
        FN("pairedPath", paired_path),
        FN("pairedConnect", paired_connect),
        FN("pairedConnectOpFd", paired_connect_op_fd),
        FN("pairedConnectOpWait", paired_connect_op_wait),
        FN("pairedPeerCount", paired_peer_count),
        FN("pairedDropped", paired_dropped),
        FN("pairedConnectionStats", paired_connection_stats),
        FN("pairedEvents", paired_events),
        FN("pairedOpen", paired_open),
        FN("pairedOpenOpFd", paired_open_op_fd),
        FN("pairedOpenOpWait", paired_open_op_wait),
        FN("pairedSend", paired_send),
        FN("pairedSendWith", paired_send_with),
        FN("pairedSendOpFd", paired_send_op_fd),
        FN("pairedSendOpWait", paired_send_op_wait),
        FN("pairedFd", paired_fd),
        FN("pairedRecv", paired_recv),
        FN("runtimeSurveyor", runtime_surveyor),
        FN("surveyorConnect", surveyor_connect),
        FN("surveyorConnectOpFd", surveyor_connect_op_fd),
        FN("surveyorConnectOpWait", surveyor_connect_op_wait),
        FN("surveyorPeerCount", surveyor_peer_count),
        FN("surveyorConnectionStats", surveyor_connection_stats),
        FN("surveyorSurvey", surveyor_survey),
        FN("surveyorSurveyWith", surveyor_survey_with),
        FN("surveyorSurveyOpFd", surveyor_survey_op_fd),
        FN("surveyorSurveyOpWait", surveyor_survey_op_wait),
        FN("surveyRunFd", survey_run_fd),
        FN("surveyRunNext", survey_run_next),
        FN("surveyRunLate", survey_run_late),
        FN("surveyRunRespondents", survey_run_respondents),
        FN("listenerRespondent", listener_respondent),
        FN("respondentPath", respondent_path),
        FN("respondentFd", respondent_fd),
        FN("respondentAccept", respondent_accept),
        FN("listenerBus", listener_bus),
        FN("busMemberPath", bus_member_path),
        FN("busMemberConnect", bus_member_connect),
        FN("busMemberConnectOpFd", bus_member_connect_op_fd),
        FN("busMemberConnectOpWait", bus_member_connect_op_wait),
        FN("busMemberPeerCount", bus_member_peer_count),
        FN("busMemberConnectionStats", bus_member_connection_stats),
        FN("busMemberDropped", bus_member_dropped),
        FN("busMemberSend", bus_member_send),
        FN("busMemberSendWith", bus_member_send_with),
        FN("busMemberSendOpFd", bus_member_send_op_fd),
        FN("busMemberSendOpWait", bus_member_send_op_wait),
        FN("busMemberFd", bus_member_fd),
        FN("busMemberRecv", bus_member_recv),
        FN("joinTermsNew", join_terms_new),
        FN("joinTermsSetMaxAgeMs", join_terms_set_max_age_ms),
        FN("joinTermsSetMaxLayer", join_terms_set_max_layer),
        FN("segmentTermsNew", segment_terms_new),
        FN("segmentTermsSetMaxAgeMs", segment_terms_set_max_age_ms),
        FN("segmentTermsSetPriority", segment_terms_set_priority),
        FN("segmentTermsSetFollowsUpstream", segment_terms_set_follows_upstream),
        FN("listenerRadio", listener_radio),
        FN("radioPath", radio_path),
        FN("radioSegment", radio_segment),
        FN("radioDatagram", radio_datagram),
        FN("radioDishCount", radio_dish_count),
        FN("radioDropped", radio_dropped),
        FN("radioDroppedOn", radio_dropped_on),
        FN("radioDrops", radio_drops),
        FN("radioEvict", radio_evict),
        FN("radioDishDrops", radio_dish_drops),
        FN("segmentNumber", segment_number),
        FN("segmentTopic", segment_topic),
        FN("segmentWrite", segment_write),
        FN("segmentWriteLayer", segment_write_layer),
        FN("segmentFinishLayer", segment_finish_layer),
        FN("segmentFinish", segment_finish),
        FN("runtimeDish", runtime_dish),
        FN("dishConnect", dish_connect),
        FN("dishConnectOpFd", dish_connect_op_fd),
        FN("dishConnectOpWait", dish_connect_op_wait),
        FN("dishPeerCount", dish_peer_count),
        FN("dishConnectionStats", dish_connection_stats),
        FN("dishEvents", dish_events),
        FN("dishJoin", dish_join),
        FN("dishJoinOpFd", dish_join_op_fd),
        FN("dishJoinOpWait", dish_join_op_wait),
        FN("dishLeave", dish_leave),
        FN("dishLeaveOpFd", dish_leave_op_fd),
        FN("dishLeaveOpWait", dish_leave_op_wait),
        FN("dishFd", dish_fd),
        FN("dishRecv", dish_recv),
        FN("dishStale", dish_stale),
        FN("dishOverflow", dish_overflow),
        FN("outgoingTransferCursors", outgoing_transfer_cursors),
        FN("incomingTransferReporter", incoming_transfer_reporter),
        FN("cursorsFd", cursors_fd),
        FN("cursorsRecv", cursors_recv),
        FN("reporterLevels", reporter_levels),
        FN("reporterMode", reporter_mode),
        FN("reporterSetGranularity", reporter_set_granularity),
        FN("reporterReport", reporter_report),
        FN("reporterReportOpFd", reporter_report_op_fd),
        FN("reporterReportOpWait", reporter_report_op_wait),
        FN("reporterFinish", reporter_finish),
        FN("reporterFinishOpFd", reporter_finish_op_fd),
        FN("reporterFinishOpWait", reporter_finish_op_wait),
        FN("runtimePeer", runtime_peer),
        FN("peerConnect", peer_connect),
        FN("peerConnectOpFd", peer_connect_op_fd),
        FN("peerConnectOpWait", peer_connect_op_wait),
        FN("peerOpenFlow", peer_open_flow),
        FN("peerOpenFlowOpFd", peer_open_flow_op_fd),
        FN("peerOpenFlowOpWait", peer_open_flow_op_wait),
        FN("flowMetaNew", flow_meta_new),
        FN("flowMetaSetContentType", flow_meta_set_content_type),
        FN("flowMetaSetTrace", flow_meta_set_trace),
        FN("flowMetaSetTopic", flow_meta_set_topic),
        FN("listenerAcceptor", listener_acceptor),
        FN("acceptorPath", acceptor_path),
        FN("acceptorFd", acceptor_fd),
        FN("acceptorAccept", acceptor_accept),
        FN("flowSend", flow_send),
        FN("flowMaxPayload", flow_max_payload),
        FN("flowSent", flow_sent),
        FN("flowTooLarge", flow_too_large),
        FN("flowDiscarded", flow_discarded),
        FN("flowNotLive", flow_not_live),
        FN("flowPathStats", flow_path_stats),
        FN("flowAbort", flow_abort),
        FN("incomingFlowInfo", incoming_flow_info),
        FN("incomingFlowFd", incoming_flow_fd),
        FN("incomingFlowRecv", incoming_flow_recv),
        FN("incomingFlowReceived", incoming_flow_received),
        FN("incomingFlowOverflow", incoming_flow_overflow),
        FN("incomingFlowPathStats", incoming_flow_path_stats),
        FN("incomingFlowRefuse", incoming_flow_refuse),
        FN("incomingTransferMeta", incoming_transfer_meta),
        FN("incomingTransferConnection", incoming_transfer_connection),
        FN("incomingTransferFd", incoming_transfer_fd),
        FN("incomingTransferRead", incoming_transfer_read),
        FN("incomingTransferCollect", incoming_transfer_collect),
        FN("incomingTransferCollectOpFd", incoming_transfer_collect_op_fd),
        FN("incomingTransferCollectOpWait", incoming_transfer_collect_op_wait),
        FN("outgoingTransferFd", outgoing_transfer_fd),
        FN("outgoingTransferWrite", outgoing_transfer_write),
        FN("outgoingTransferTrace", outgoing_transfer_trace),
        FN("outgoingTransferSetPriority", outgoing_transfer_set_priority),
        FN("outgoingTransferExpireAfterMs", outgoing_transfer_expire_after_ms),
        FN("outgoingTransferFinish", outgoing_transfer_finish),
        FN("deliveryDelivered", delivery_delivered),
        FN("deliveryDeliveredOpFd", delivery_delivered_op_fd),
        FN("deliveryDeliveredOpWait", delivery_delivered_op_wait),
    };
    if (napi_define_properties(env, exports, sizeof props / sizeof props[0], props) != napi_ok)
        return throw_napi(env);
    return exports;
}
