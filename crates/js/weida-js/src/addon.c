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
DROP(listener)
DROP(listener_bind_quic_op)
DROP(binding)
DROP(local_binding)
DROP(replier)
DROP(incoming_request)
DROP(incoming_request_reply_op)
DROP(requester)
DROP(requester_connect_op)
DROP(requester_request_op)
DROP(trust)
DROP(identity)
DROP(client_tls)
DROP(server_tls)
DROP(transfer_meta)
DROP(incoming_transfer)
DROP(incoming_transfer_collect_op)
DROP(outgoing_transfer)
DROP(delivery)
DROP(delivery_delivered_op)

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
    present = weida_incoming_meta_content_len(m, &n);
    ok = ok && set(env, obj, "contentLen", optional_u64(env, present, n));
    present = weida_incoming_meta_sequence(m, &n);
    ok = ok && set(env, obj, "sequence", optional_u64(env, present, n));
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

/* ---- generated shapes ---- */

#define WRAP_AS(t) \
    static napi_value wrap_##t(napi_env env, void *out) { return wrap(env, out, drop_##t); }
WRAP_AS(runtime)
WRAP_AS(binding)
WRAP_AS(incoming_request)
WRAP_AS(incoming_transfer)
WRAP_AS(outgoing_transfer)

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
#define TEXT(js, t, x) \
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

static napi_value identity_generate_for(napi_env env, napi_callback_info info) {
    ARGS(1);
    bool is = false;
    uint32_t n = 0, got = 0;
    napi_value result = NULL;
    if (napi_is_array(env, argv[0], &is) != napi_ok || !is)
        return throw_type(env, "expected an array of strings");
    if (napi_get_array_length(env, argv[0], &n) != napi_ok)
        return throw_napi(env);
    char **names = calloc(n ? n : 1, sizeof *names);
    if (!names) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    /* `got` counts the strings to free: every one read before a throw. */
    for (; got < n; got++) {
        napi_value el;
        if (napi_get_element(env, argv[0], got, &el) != napi_ok) {
            throw_napi(env);
            break;
        }
        names[got] = get_string(env, el);
        if (!names[got])
            break;
    }
    if (got == n) {
        weida_identity *id = NULL;
        int rc = weida_identity_generate_for((const char *const *)names, n, &id);
        result = rc < 0 ? throw_rc(env, rc) : wrap(env, id, drop_identity);
    }
    for (uint32_t i = 0; i < got; i++)
        free(names[i]);
    free(names);
    return result;
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

TEXT(binding_local_addr, binding, local_addr)
TEXT(local_binding_bus, local_binding, bus)

/* replier and incoming request */

TEXT(replier_path, replier, path)
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

FD(requester_request_op_fd, requester_request_op)
WAIT1(requester_request_op_wait, requester_request_op, weida_incoming_transfer,
      wrap_incoming_transfer)

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
    struct addon *a = calloc(1, sizeof *a);
    napi_value again_obj;
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

    napi_property_descriptor props[] = {
        {"AGAIN", NULL, NULL, NULL, NULL, again_obj, napi_enumerable, NULL},
        FN("version", version),
        FN("strerror", strerror_),
        FN("error", error_),
        FN("setErrorClass", set_error_class),
        FN("drop", drop_),
        FN("watch", waiter_watch),
        FN("unwatch", waiter_unwatch),
        FN("runtimeConfigNew", runtime_config_new),
        FN("runtimeConfigSetWorkerThreads", runtime_config_set_worker_threads),
        FN("runtimeConfigSetShutdownTimeoutMs", runtime_config_set_shutdown_timeout_ms),
        FN("runtimeOwned", runtime_owned),
        FN("runtimeListener", runtime_listener),
        FN("runtimeRequester", runtime_requester),
        FN("runtimeShutdown", runtime_shutdown),
        FN("runtimeShutdownOpFd", runtime_shutdown_op_fd),
        FN("runtimeShutdownOpWait", runtime_shutdown_op_wait),
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
        FN("transferMetaNew", transfer_meta_new),
        FN("transferMetaSetContentType", transfer_meta_set_content_type),
        FN("listenerBindQuic", listener_bind_quic),
        FN("listenerBindQuicOpFd", listener_bind_quic_op_fd),
        FN("listenerBindQuicOpWait", listener_bind_quic_op_wait),
        FN("listenerBindInproc", listener_bind_inproc),
        FN("listenerReplier", listener_replier),
        FN("bindingLocalAddr", binding_local_addr),
        FN("localBindingBus", local_binding_bus),
        FN("replierPath", replier_path),
        FN("replierFd", replier_fd),
        FN("replierAccept", replier_accept),
        FN("incomingRequestMeta", incoming_request_meta),
        FN("incomingRequestTakeBody", incoming_request_take_body),
        FN("incomingRequestReply", incoming_request_reply),
        FN("incomingRequestReplyOpFd", incoming_request_reply_op_fd),
        FN("incomingRequestReplyOpWait", incoming_request_reply_op_wait),
        FN("requesterConnect", requester_connect),
        FN("requesterConnectOpFd", requester_connect_op_fd),
        FN("requesterConnectOpWait", requester_connect_op_wait),
        FN("requesterRequest", requester_request),
        FN("requesterRequestOpFd", requester_request_op_fd),
        FN("requesterRequestOpWait", requester_request_op_wait),
        FN("incomingTransferMeta", incoming_transfer_meta),
        FN("incomingTransferFd", incoming_transfer_fd),
        FN("incomingTransferRead", incoming_transfer_read),
        FN("incomingTransferCollect", incoming_transfer_collect),
        FN("incomingTransferCollectOpFd", incoming_transfer_collect_op_fd),
        FN("incomingTransferCollectOpWait", incoming_transfer_collect_op_wait),
        FN("outgoingTransferFd", outgoing_transfer_fd),
        FN("outgoingTransferWrite", outgoing_transfer_write),
        FN("outgoingTransferFinish", outgoing_transfer_finish),
        FN("deliveryDelivered", delivery_delivered),
        FN("deliveryDeliveredOpFd", delivery_delivered_op_fd),
        FN("deliveryDeliveredOpWait", delivery_delivered_op_wait),
    };
    if (napi_define_properties(env, exports, sizeof props / sizeof props[0], props) != napi_ok)
        return throw_napi(env);
    return exports;
}
