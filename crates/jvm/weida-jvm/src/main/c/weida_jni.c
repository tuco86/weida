/*
 * weida_jni.c: the JNI glue of weida for the JVM and Android
 * (docs/decisions/0046, docs/libraries/weida-jvm.md). Every static native
 * method of net.doodleshnookie.weida.Native is one function here, over one
 * or a few functions of weida.h. The same source builds against a JDK's
 * jni.h (build.sh) and the NDK's (.zeughaus-ci/android-libweida.sh).
 *
 * Handles cross as jlong, the C pointer. No call blocks: every try and op
 * wait passes timeout_ms = 0, and the Java layer waits on the readiness
 * descriptors with Native.poll on its waiter thread. -WEIDA_EAGAIN is
 * returned, not thrown: a native returning a handle returns 0, one
 * returning a count -1, one returning nothing 0 (again) or 1 (done). Any
 * other failure throws a WeidaException built from the thread-local record
 * (docs/c/README.md R9), read in the same native call that failed.
 */
#include <jni.h>

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "weida.h"

#define UNUSED __attribute__((unused))
#define NATIVE(ret, name) JNIEXPORT ret JNICALL Java_net_doodleshnookie_weida_Native_##name
#define ENV JNIEnv *env UNUSED, jclass cls UNUSED
#define PTR(type, h) ((type *)(intptr_t)(h))
#define HANDLE(p) ((jlong)(intptr_t)(p))

/* The handle kinds of Native.drop and Native.fd; Native.java names the same
 * numbers. */
enum kind {
    K_RUNTIME_CONFIG = 1,
    K_RUNTIME = 2,
    K_SHUTDOWN_OP = 3,
    K_TRUST = 4,
    K_IDENTITY = 5,
    K_CLIENT_TLS = 6,
    K_SERVER_TLS = 7,
    K_TRANSFER_META = 8,
    K_LISTENER = 9,
    K_BIND_QUIC_OP = 10,
    K_BINDING = 11,
    K_LOCAL_BINDING = 12,
    K_REPLIER = 13,
    K_INCOMING_REQUEST = 14,
    K_REPLY_OP = 15,
    K_REQUESTER = 16,
    K_CONNECT_OP = 17,
    K_REQUEST_OP = 18,
    K_INCOMING_META = 19,
    K_INCOMING_TRANSFER = 20,
    K_COLLECT_OP = 21,
    K_OUTGOING_TRANSFER = 22,
    K_DELIVERY = 23,
    K_DELIVERED_OP = 24,
};

static jclass exception_class;
static jmethodID exception_init;

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved UNUSED) {
    JNIEnv *env;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_6) != JNI_OK)
        return JNI_ERR;
    jclass local = (*env)->FindClass(env, "net/doodleshnookie/weida/WeidaException");
    if (!local)
        return JNI_ERR;
    exception_class = (*env)->NewGlobalRef(env, local);
    (*env)->DeleteLocalRef(env, local);
    if (!exception_class)
        return JNI_ERR;
    /* (int errno, String message, int lossCause, String fingerprint,
     *  long max, int osCode) */
    exception_init = (*env)->GetMethodID(env, exception_class, "<init>",
                                         "(ILjava/lang/String;ILjava/lang/String;JI)V");
    if (!exception_init)
        return JNI_ERR;
    return JNI_VERSION_1_6;
}

static void throw_oom(JNIEnv *env) {
    jclass oom = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
    if (oom)
        (*env)->ThrowNew(env, oom, "weida: out of native memory");
}

/* `len` bytes of UTF-8 as a Java string, decoded here because NewStringUTF
 * takes modified UTF-8: a supplementary character becomes a surrogate pair,
 * an interior NUL stays U+0000, a malformed sequence becomes U+FFFD. NULL
 * for NULL, and NULL with a pending exception when memory runs out. */
static jstring string_of(JNIEnv *env, const char *text, size_t len) {
    if (!text)
        return NULL;
    if (len > INT32_MAX) {
        throw_oom(env);
        return NULL;
    }
    /* Every byte yields at most one UTF-16 unit. The first unit is set so
     * that GCC sees an initialized buffer for an empty string too. */
    jchar stack[256];
    stack[0] = 0;
    jchar *buf = stack;
    if (len > sizeof stack / sizeof *stack) {
        buf = malloc(len * sizeof *buf);
        if (!buf) {
            throw_oom(env);
            return NULL;
        }
    }
    const unsigned char *s = (const unsigned char *)text;
    size_t n = 0, i = 0;
    while (i < len) {
        unsigned char c = s[i];
        uint32_t cp;
        size_t need;
        if (c < 0x80) {
            buf[n++] = c;
            i++;
            continue;
        } else if ((c & 0xE0) == 0xC0) {
            cp = c & 0x1F;
            need = 1;
        } else if ((c & 0xF0) == 0xE0) {
            cp = c & 0x0F;
            need = 2;
        } else if ((c & 0xF8) == 0xF0) {
            cp = c & 0x07;
            need = 3;
        } else {
            buf[n++] = 0xFFFD;
            i++;
            continue;
        }
        size_t j = 1;
        for (; j <= need && i + j < len && (s[i + j] & 0xC0) == 0x80; j++)
            cp = (cp << 6) | (s[i + j] & 0x3F);
        i += j;
        bool overlong = (need == 1 && cp < 0x80) || (need == 2 && cp < 0x800) ||
                        (need == 3 && cp < 0x10000);
        if (j <= need || overlong || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
            buf[n++] = 0xFFFD;
        } else if (cp >= 0x10000) {
            cp -= 0x10000;
            buf[n++] = (jchar)(0xD800 | (cp >> 10));
            buf[n++] = (jchar)(0xDC00 | (cp & 0x3FF));
        } else {
            buf[n++] = (jchar)cp;
        }
    }
    jstring out = (*env)->NewString(env, buf, (jsize)n);
    if (buf != stack)
        free(buf);
    return out;
}

static jstring cstring(JNIEnv *env, const char *text) {
    return text ? string_of(env, text, strlen(text)) : NULL;
}

/* Throws the WeidaException of the failure `rc` (either sign). With
 * `message` NULL it carries the thread-local record of the call that just
 * failed on this thread; otherwise `message` and no record. */
static void throw_status(JNIEnv *env, int rc, const char *message) {
    int code = rc < 0 ? -rc : rc;
    const char *fingerprint = NULL;
    int loss = 0, os_code = 0;
    int64_t max = -1;
    if (!message) {
        message = weida_last_error_message();
        loss = weida_last_error_loss_cause();
        fingerprint = weida_last_error_fingerprint();
        max = weida_last_error_max();
        os_code = weida_last_error_os_code();
    }
    if (!message)
        message = weida_strerror(code);
    /* Both strings become Java values before anything else can fail on this
     * thread and replace the record they point into. */
    jstring jmessage = cstring(env, message);
    if ((*env)->ExceptionCheck(env))
        return;
    jstring jfingerprint = cstring(env, fingerprint);
    if ((*env)->ExceptionCheck(env))
        return;
    jobject e = (*env)->NewObject(env, exception_class, exception_init, (jint)code, jmessage,
                                  (jint)loss, jfingerprint, (jlong)max, (jint)os_code);
    if (e)
        (*env)->Throw(env, (jthrowable)e);
}

/* true for a non-negative status; otherwise throws and returns false. */
static bool check(JNIEnv *env, int rc) {
    if (rc >= 0)
        return true;
    throw_status(env, rc, NULL);
    return false;
}

/* A Java string as NUL-terminated UTF-8 in `*out`, to be freed with free().
 * NULL stays NULL (libweida refuses it with -WEIDA_EINVAL). false with a
 * pending exception when the string holds a NUL, which a C string cannot
 * carry, or memory runs out. A lone surrogate becomes U+FFFD. */
static bool utf8_of(JNIEnv *env, jstring str, char **out) {
    *out = NULL;
    if (!str)
        return true;
    jsize n = (*env)->GetStringLength(env, str);
    const jchar *chars = (*env)->GetStringChars(env, str, NULL);
    if (!chars)
        return false;
    /* A UTF-16 unit takes at most 3 bytes; a pair of them 4. */
    char *buf = malloc((size_t)n * 3 + 1);
    if (!buf) {
        (*env)->ReleaseStringChars(env, str, chars);
        throw_oom(env);
        return false;
    }
    size_t len = 0;
    for (jsize i = 0; i < n; i++) {
        uint32_t cp = chars[i];
        if (cp == 0) {
            (*env)->ReleaseStringChars(env, str, chars);
            free(buf);
            throw_status(env, -WEIDA_EINVAL, "a string passed to weida holds a NUL character");
            return false;
        }
        if (cp >= 0xD800 && cp <= 0xDBFF && i + 1 < n && chars[i + 1] >= 0xDC00 &&
            chars[i + 1] <= 0xDFFF) {
            cp = 0x10000 + ((cp - 0xD800) << 10) + (chars[i + 1] - 0xDC00);
            i++;
        } else if (cp >= 0xD800 && cp <= 0xDFFF) {
            cp = 0xFFFD;
        }
        if (cp < 0x80) {
            buf[len++] = (char)cp;
        } else if (cp < 0x800) {
            buf[len++] = (char)(0xC0 | (cp >> 6));
            buf[len++] = (char)(0x80 | (cp & 0x3F));
        } else if (cp < 0x10000) {
            buf[len++] = (char)(0xE0 | (cp >> 12));
            buf[len++] = (char)(0x80 | ((cp >> 6) & 0x3F));
            buf[len++] = (char)(0x80 | (cp & 0x3F));
        } else {
            buf[len++] = (char)(0xF0 | (cp >> 18));
            buf[len++] = (char)(0x80 | ((cp >> 12) & 0x3F));
            buf[len++] = (char)(0x80 | ((cp >> 6) & 0x3F));
            buf[len++] = (char)(0x80 | (cp & 0x3F));
        }
    }
    buf[len] = '\0';
    (*env)->ReleaseStringChars(env, str, chars);
    *out = buf;
    return true;
}

/* ---- the library and its codes -------------------------------------- */

NATIVE(jstring, version)(ENV) {
    return cstring(env, weida_version());
}

NATIVE(jstring, strerror)(ENV, jint code) {
    return cstring(env, weida_strerror(code));
}

/* ---- every handle kind: release and readiness ------------------------ */

NATIVE(void, drop)(ENV, jint kind, jlong h) {
    switch (kind) {
    case K_RUNTIME_CONFIG: weida_runtime_config_drop(PTR(weida_runtime_config, h)); break;
    case K_RUNTIME: weida_runtime_drop(PTR(weida_runtime, h)); break;
    case K_SHUTDOWN_OP: weida_runtime_shutdown_op_drop(PTR(weida_runtime_shutdown_op, h)); break;
    case K_TRUST: weida_trust_drop(PTR(weida_trust, h)); break;
    case K_IDENTITY: weida_identity_drop(PTR(weida_identity, h)); break;
    case K_CLIENT_TLS: weida_client_tls_drop(PTR(weida_client_tls, h)); break;
    case K_SERVER_TLS: weida_server_tls_drop(PTR(weida_server_tls, h)); break;
    case K_TRANSFER_META: weida_transfer_meta_drop(PTR(weida_transfer_meta, h)); break;
    case K_LISTENER: weida_listener_drop(PTR(weida_listener, h)); break;
    case K_BIND_QUIC_OP: weida_listener_bind_quic_op_drop(PTR(weida_listener_bind_quic_op, h)); break;
    case K_BINDING: weida_binding_drop(PTR(weida_binding, h)); break;
    case K_LOCAL_BINDING: weida_local_binding_drop(PTR(weida_local_binding, h)); break;
    case K_REPLIER: weida_replier_drop(PTR(weida_replier, h)); break;
    case K_INCOMING_REQUEST: weida_incoming_request_drop(PTR(weida_incoming_request, h)); break;
    case K_REPLY_OP:
        weida_incoming_request_reply_op_drop(PTR(weida_incoming_request_reply_op, h));
        break;
    case K_REQUESTER: weida_requester_drop(PTR(weida_requester, h)); break;
    case K_CONNECT_OP: weida_requester_connect_op_drop(PTR(weida_requester_connect_op, h)); break;
    case K_REQUEST_OP: weida_requester_request_op_drop(PTR(weida_requester_request_op, h)); break;
    case K_INCOMING_META: weida_incoming_meta_drop(PTR(weida_incoming_meta, h)); break;
    case K_INCOMING_TRANSFER: weida_incoming_transfer_drop(PTR(weida_incoming_transfer, h)); break;
    case K_COLLECT_OP:
        weida_incoming_transfer_collect_op_drop(PTR(weida_incoming_transfer_collect_op, h));
        break;
    case K_OUTGOING_TRANSFER: weida_outgoing_transfer_drop(PTR(weida_outgoing_transfer, h)); break;
    case K_DELIVERY: weida_delivery_drop(PTR(weida_delivery, h)); break;
    case K_DELIVERED_OP:
        weida_delivery_delivered_op_drop(PTR(weida_delivery_delivered_op, h));
        break;
    default: break;
    }
}

NATIVE(jlong, fd)(ENV, jint kind, jlong h) {
    int64_t fd = -1;
    int rc;
    switch (kind) {
    case K_SHUTDOWN_OP: rc = weida_runtime_shutdown_op_fd(PTR(weida_runtime_shutdown_op, h), &fd); break;
    case K_BIND_QUIC_OP:
        rc = weida_listener_bind_quic_op_fd(PTR(weida_listener_bind_quic_op, h), &fd);
        break;
    case K_REPLIER: rc = weida_replier_fd(PTR(weida_replier, h), &fd); break;
    case K_REPLY_OP:
        rc = weida_incoming_request_reply_op_fd(PTR(weida_incoming_request_reply_op, h), &fd);
        break;
    case K_CONNECT_OP: rc = weida_requester_connect_op_fd(PTR(weida_requester_connect_op, h), &fd); break;
    case K_REQUEST_OP: rc = weida_requester_request_op_fd(PTR(weida_requester_request_op, h), &fd); break;
    case K_INCOMING_TRANSFER: rc = weida_incoming_transfer_fd(PTR(weida_incoming_transfer, h), &fd); break;
    case K_COLLECT_OP:
        rc = weida_incoming_transfer_collect_op_fd(PTR(weida_incoming_transfer_collect_op, h), &fd);
        break;
    case K_OUTGOING_TRANSFER: rc = weida_outgoing_transfer_fd(PTR(weida_outgoing_transfer, h), &fd); break;
    case K_DELIVERED_OP:
        rc = weida_delivery_delivered_op_fd(PTR(weida_delivery_delivered_op, h), &fd);
        break;
    default:
        throw_status(env, -WEIDA_EINVAL, "this handle has no readiness descriptor");
        return -1;
    }
    return check(env, rc) ? (jlong)fd : -1;
}

/* ---- runtime --------------------------------------------------------- */

NATIVE(jlong, runtimeConfigNew)(ENV) {
    weida_runtime_config *out = NULL;
    return check(env, weida_runtime_config_new(&out)) ? HANDLE(out) : 0;
}

NATIVE(void, runtimeConfigSetWorkerThreads)(ENV, jlong cfg, jint threads) {
    check(env, weida_runtime_config_set_worker_threads(PTR(weida_runtime_config, cfg),
                                                       (uint32_t)threads));
}

NATIVE(void, runtimeConfigSetShutdownTimeoutMs)(ENV, jlong cfg, jlong ms) {
    check(env, weida_runtime_config_set_shutdown_timeout_ms(PTR(weida_runtime_config, cfg),
                                                            (uint64_t)ms));
}

NATIVE(jlong, runtimeOwned)(ENV, jlong cfg) {
    weida_runtime *out = NULL;
    return check(env, weida_runtime_owned(PTR(weida_runtime_config, cfg), &out)) ? HANDLE(out) : 0;
}

NATIVE(jlong, runtimeListener)(ENV, jlong rt) {
    weida_listener *out = NULL;
    return check(env, weida_runtime_listener(PTR(weida_runtime, rt), &out)) ? HANDLE(out) : 0;
}

NATIVE(jlong, runtimeRequester)(ENV, jlong rt, jlong tls) {
    weida_requester *out = NULL;
    int rc = weida_runtime_requester(PTR(weida_runtime, rt), PTR(weida_client_tls, tls), &out);
    return check(env, rc) ? HANDLE(out) : 0;
}

/* Consumes `rt`. */
NATIVE(jlong, runtimeShutdown)(ENV, jlong rt) {
    weida_runtime_shutdown_op *out = NULL;
    return check(env, weida_runtime_shutdown(PTR(weida_runtime, rt), &out)) ? HANDLE(out) : 0;
}

NATIVE(jint, runtimeShutdownOpWait)(ENV, jlong op) {
    int rc = weida_runtime_shutdown_op_wait(PTR(weida_runtime_shutdown_op, op), 0);
    if (rc == -WEIDA_EAGAIN)
        return 0;
    return check(env, rc) ? 1 : 0;
}

/* ---- TLS values ------------------------------------------------------ */

NATIVE(jlong, trustNew)(ENV) {
    weida_trust *out = NULL;
    return check(env, weida_trust_new(&out)) ? HANDLE(out) : 0;
}

NATIVE(void, trustAddPin)(ENV, jlong t, jstring fingerprint) {
    char *fp;
    if (!utf8_of(env, fingerprint, &fp))
        return;
    int rc = weida_trust_add_pin(PTR(weida_trust, t), fp);
    free(fp);
    check(env, rc);
}

NATIVE(jlong, identityGenerate)(ENV) {
    weida_identity *out = NULL;
    return check(env, weida_identity_generate(&out)) ? HANDLE(out) : 0;
}

NATIVE(jlong, identityFromPem)(ENV, jbyteArray cert, jbyteArray key) {
    jsize cert_len = (*env)->GetArrayLength(env, cert);
    jsize key_len = (*env)->GetArrayLength(env, key);
    /* Non-blocking, and both are copied before it returns. */
    jbyte *c = (*env)->GetPrimitiveArrayCritical(env, cert, NULL);
    if (!c)
        return 0;
    jbyte *k = (*env)->GetPrimitiveArrayCritical(env, key, NULL);
    if (!k) {
        (*env)->ReleasePrimitiveArrayCritical(env, cert, c, JNI_ABORT);
        return 0;
    }
    weida_identity *out = NULL;
    int rc = weida_identity_from_pem((const uint8_t *)c, (size_t)cert_len, (const uint8_t *)k,
                                     (size_t)key_len, &out);
    (*env)->ReleasePrimitiveArrayCritical(env, key, k, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, cert, c, JNI_ABORT);
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jstring, identityFingerprint)(ENV, jlong id) {
    const char *out = NULL;
    if (!check(env, weida_identity_fingerprint(PTR(weida_identity, id), &out)))
        return NULL;
    return cstring(env, out);
}

NATIVE(jlong, clientTlsNew)(ENV, jlong trust) {
    weida_client_tls *out = NULL;
    return check(env, weida_client_tls_new(PTR(weida_trust, trust), &out)) ? HANDLE(out) : 0;
}

NATIVE(jlong, serverTlsNew)(ENV, jlong identity) {
    weida_server_tls *out = NULL;
    return check(env, weida_server_tls_new(PTR(weida_identity, identity), &out)) ? HANDLE(out) : 0;
}

NATIVE(jlong, transferMetaNew)(ENV) {
    weida_transfer_meta *out = NULL;
    return check(env, weida_transfer_meta_new(&out)) ? HANDLE(out) : 0;
}

NATIVE(void, transferMetaSetContentType)(ENV, jlong m, jstring content_type) {
    char *ct;
    if (!utf8_of(env, content_type, &ct))
        return;
    int rc = weida_transfer_meta_set_content_type(PTR(weida_transfer_meta, m), ct);
    free(ct);
    check(env, rc);
}

/* ---- listener and bindings ------------------------------------------- */

NATIVE(jlong, listenerBindQuic)(ENV, jlong l, jstring addr, jlong tls) {
    char *a;
    if (!utf8_of(env, addr, &a))
        return 0;
    weida_listener_bind_quic_op *out = NULL;
    int rc = weida_listener_bind_quic(PTR(weida_listener, l), a, PTR(weida_server_tls, tls), &out);
    free(a);
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jlong, listenerBindQuicOpWait)(ENV, jlong op) {
    weida_binding *out = NULL;
    int rc = weida_listener_bind_quic_op_wait(PTR(weida_listener_bind_quic_op, op), 0, &out);
    if (rc == -WEIDA_EAGAIN)
        return 0;
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jlong, listenerBindInproc)(ENV, jlong l, jstring bus) {
    char *b;
    if (!utf8_of(env, bus, &b))
        return 0;
    weida_local_binding *out = NULL;
    int rc = weida_listener_bind_inproc(PTR(weida_listener, l), b, &out);
    free(b);
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jlong, listenerReplier)(ENV, jlong l, jstring path) {
    char *p;
    if (!utf8_of(env, path, &p))
        return 0;
    weida_replier *out = NULL;
    int rc = weida_listener_replier(PTR(weida_listener, l), p, &out);
    free(p);
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jstring, bindingLocalAddr)(ENV, jlong b) {
    size_t len = 0;
    const char *s = weida_binding_local_addr(PTR(weida_binding, b), &len);
    return string_of(env, s, len);
}

NATIVE(jstring, localBindingBus)(ENV, jlong b) {
    size_t len = 0;
    const char *s = weida_local_binding_bus(PTR(weida_local_binding, b), &len);
    return string_of(env, s, len);
}

/* ---- replier and requests -------------------------------------------- */

NATIVE(jstring, replierPath)(ENV, jlong r) {
    size_t len = 0;
    const char *s = weida_replier_path(PTR(weida_replier, r), &len);
    return string_of(env, s, len);
}

NATIVE(jlong, replierAccept)(ENV, jlong r) {
    weida_incoming_request *out = NULL;
    int rc = weida_replier_accept(PTR(weida_replier, r), 0, &out);
    if (rc == -WEIDA_EAGAIN)
        return 0;
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jlong, incomingRequestMeta)(ENV, jlong q) {
    weida_incoming_meta *out = NULL;
    int rc = weida_incoming_request_meta(PTR(weida_incoming_request, q), &out);
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jlong, incomingRequestTakeBody)(ENV, jlong q) {
    weida_incoming_transfer *out = NULL;
    int rc = weida_incoming_request_take_body(PTR(weida_incoming_request, q), &out);
    return check(env, rc) ? HANDLE(out) : 0;
}

/* Consumes `q`; `meta` 0 is empty metadata. */
NATIVE(jlong, incomingRequestReply)(ENV, jlong q, jlong meta) {
    weida_incoming_request_reply_op *out = NULL;
    int rc = weida_incoming_request_reply(PTR(weida_incoming_request, q),
                                          PTR(weida_transfer_meta, meta), &out);
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jlong, incomingRequestReplyOpWait)(ENV, jlong op) {
    weida_outgoing_transfer *out = NULL;
    int rc = weida_incoming_request_reply_op_wait(PTR(weida_incoming_request_reply_op, op), 0, &out);
    if (rc == -WEIDA_EAGAIN)
        return 0;
    return check(env, rc) ? HANDLE(out) : 0;
}

/* ---- requester ------------------------------------------------------- */

NATIVE(jlong, requesterConnect)(ENV, jlong r, jstring url) {
    char *u;
    if (!utf8_of(env, url, &u))
        return 0;
    weida_requester_connect_op *out = NULL;
    int rc = weida_requester_connect(PTR(weida_requester, r), u, &out);
    free(u);
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jint, requesterConnectOpWait)(ENV, jlong op) {
    int rc = weida_requester_connect_op_wait(PTR(weida_requester_connect_op, op), 0);
    if (rc == -WEIDA_EAGAIN)
        return 0;
    return check(env, rc) ? 1 : 0;
}

NATIVE(jlong, requesterRequest)(ENV, jlong r, jbyteArray body) {
    jsize len = (*env)->GetArrayLength(env, body);
    /* Non-blocking, and the body is copied before it returns. */
    jbyte *data = (*env)->GetPrimitiveArrayCritical(env, body, NULL);
    if (!data)
        return 0;
    weida_requester_request_op *out = NULL;
    int rc = weida_requester_request(PTR(weida_requester, r), (const uint8_t *)data, (size_t)len, &out);
    (*env)->ReleasePrimitiveArrayCritical(env, body, data, JNI_ABORT);
    return check(env, rc) ? HANDLE(out) : 0;
}

NATIVE(jlong, requesterRequestOpWait)(ENV, jlong op) {
    weida_incoming_transfer *out = NULL;
    int rc = weida_requester_request_op_wait(PTR(weida_requester_request_op, op), 0, &out);
    if (rc == -WEIDA_EAGAIN)
        return 0;
    return check(env, rc) ? HANDLE(out) : 0;
}

/* ---- incoming metadata ----------------------------------------------- */

/* `which`: 0 endpoint, 1 content type, 2 topic, 3 peer fingerprint. */
NATIVE(jstring, incomingMetaString)(ENV, jlong m, jint which) {
    const weida_incoming_meta *meta = PTR(const weida_incoming_meta, m);
    size_t len = 0;
    const char *s = NULL;
    switch (which) {
    case 0: s = weida_incoming_meta_endpoint(meta, &len); break;
    case 1: s = weida_incoming_meta_content_type(meta, &len); break;
    case 2: s = weida_incoming_meta_topic(meta, &len); break;
    case 3: s = weida_incoming_meta_peer_fingerprint(meta, &len); break;
    default: break;
    }
    return string_of(env, s, len);
}

/* Writes content_len to out[0] and sequence to out[1]; returns which are
 * present: bit 0 content_len, bit 1 sequence. */
NATIVE(jint, incomingMetaNumbers)(ENV, jlong m, jlongArray out) {
    const weida_incoming_meta *meta = PTR(const weida_incoming_meta, m);
    uint64_t content_len = 0, sequence = 0;
    jint present = 0;
    if (weida_incoming_meta_content_len(meta, &content_len) == 1)
        present |= 1;
    if (weida_incoming_meta_sequence(meta, &sequence) == 1)
        present |= 2;
    jlong values[2] = {(jlong)content_len, (jlong)sequence};
    (*env)->SetLongArrayRegion(env, out, 0, 2, values);
    return present;
}

/* ---- incoming transfer ----------------------------------------------- */

NATIVE(jlong, incomingTransferMeta)(ENV, jlong t) {
    weida_incoming_meta *out = NULL;
    int rc = weida_incoming_transfer_meta(PTR(weida_incoming_transfer, t), &out);
    return check(env, rc) ? HANDLE(out) : 0;
}

/* Reads into buf[off, off + len), len > 0: the count, 0 at the end of the
 * payload, -1 for again. */
NATIVE(jint, incomingTransferRead)(ENV, jlong t, jbyteArray buf, jint off, jint len) {
    /* Safe as a critical region: the read never blocks. */
    jbyte *data = (*env)->GetPrimitiveArrayCritical(env, buf, NULL);
    if (!data)
        return 0;
    size_t got = 0;
    int rc = weida_incoming_transfer_read(PTR(weida_incoming_transfer, t), (uint8_t *)data + off,
                                          (size_t)len, 0, &got);
    (*env)->ReleasePrimitiveArrayCritical(env, buf, data, 0);
    if (rc == -WEIDA_EAGAIN)
        return -1;
    return check(env, rc) ? (jint)got : 0;
}

/* Consumes `t`. */
NATIVE(jlong, incomingTransferCollect)(ENV, jlong t, jlong max_bytes) {
    weida_incoming_transfer_collect_op *out = NULL;
    int rc = weida_incoming_transfer_collect(PTR(weida_incoming_transfer, t), (uint64_t)max_bytes,
                                             &out);
    return check(env, rc) ? HANDLE(out) : 0;
}

/* The payload, its meta handle written to meta_out[0]; NULL for again. The
 * weida_msg is dropped here. */
NATIVE(jbyteArray, incomingTransferCollectOpWait)(ENV, jlong op, jlongArray meta_out) {
    weida_msg *msg = NULL;
    int rc = weida_incoming_transfer_collect_op_wait(PTR(weida_incoming_transfer_collect_op, op), 0,
                                                     &msg);
    if (rc == -WEIDA_EAGAIN || !check(env, rc))
        return NULL;
    size_t len = weida_msg_len(msg);
    weida_incoming_meta *meta = NULL;
    rc = weida_msg_meta(msg, &meta);
    jbyteArray payload = NULL;
    if (rc >= 0 && len <= INT32_MAX) {
        payload = (*env)->NewByteArray(env, (jsize)len);
        if (payload)
            (*env)->SetByteArrayRegion(env, payload, 0, (jsize)len, (const jbyte *)weida_msg_data(msg));
    }
    weida_msg_drop(msg);
    if (rc < 0) {
        throw_status(env, rc, NULL);
        return NULL;
    }
    if (!payload) {
        weida_incoming_meta_drop(meta);
        if (!(*env)->ExceptionCheck(env))
            throw_oom(env);
        return NULL;
    }
    jlong handle = HANDLE(meta);
    (*env)->SetLongArrayRegion(env, meta_out, 0, 1, &handle);
    return payload;
}

/* ---- outgoing transfer and delivery ---------------------------------- */

/* Writes from buf[off, off + len): the count taken, -1 for again. */
NATIVE(jint, outgoingTransferWrite)(ENV, jlong t, jbyteArray buf, jint off, jint len) {
    /* Safe as a critical region: the write never blocks. */
    jbyte *data = (*env)->GetPrimitiveArrayCritical(env, buf, NULL);
    if (!data)
        return 0;
    size_t took = 0;
    int rc = weida_outgoing_transfer_write(PTR(weida_outgoing_transfer, t),
                                           (const uint8_t *)data + off, (size_t)len, 0, &took);
    (*env)->ReleasePrimitiveArrayCritical(env, buf, data, JNI_ABORT);
    if (rc == -WEIDA_EAGAIN)
        return -1;
    return check(env, rc) ? (jint)took : 0;
}

/* Consumes `t`. */
NATIVE(jlong, outgoingTransferFinish)(ENV, jlong t) {
    weida_delivery *out = NULL;
    return check(env, weida_outgoing_transfer_finish(PTR(weida_outgoing_transfer, t), &out))
               ? HANDLE(out)
               : 0;
}

/* Consumes `d`. */
NATIVE(jlong, deliveryDelivered)(ENV, jlong d) {
    weida_delivery_delivered_op *out = NULL;
    return check(env, weida_delivery_delivered(PTR(weida_delivery, d), &out)) ? HANDLE(out) : 0;
}

NATIVE(jint, deliveryDeliveredOpWait)(ENV, jlong op) {
    int rc = weida_delivery_delivered_op_wait(PTR(weida_delivery_delivered_op, op), 0);
    if (rc == -WEIDA_EAGAIN)
        return 0;
    return check(env, rc) ? 1 : 0;
}

/* ---- the waiter thread ----------------------------------------------- */

static void throw_os(JNIEnv *env, const char *what) {
    char message[160];
    snprintf(message, sizeof message, "%s: %s", what, strerror(errno));
    throw_status(env, -WEIDA_EIO, message);
}

/* A non-blocking, close-on-exec pipe: {read end, write end}. */
NATIVE(jlongArray, wakeCreate)(ENV) {
    int fds[2];
    if (pipe(fds) != 0) {
        throw_os(env, "pipe");
        return NULL;
    }
    for (int i = 0; i < 2; i++) {
        if (fcntl(fds[i], F_SETFL, fcntl(fds[i], F_GETFL) | O_NONBLOCK) != 0 ||
            fcntl(fds[i], F_SETFD, FD_CLOEXEC) != 0) {
            throw_os(env, "fcntl");
            close(fds[0]);
            close(fds[1]);
            return NULL;
        }
    }
    jlongArray out = (*env)->NewLongArray(env, 2);
    if (!out) {
        close(fds[0]);
        close(fds[1]);
        return NULL;
    }
    jlong values[2] = {fds[0], fds[1]};
    (*env)->SetLongArrayRegion(env, out, 0, 2, values);
    return out;
}

NATIVE(void, wake)(ENV, jlong fd) {
    char byte = 1;
    ssize_t r;
    do {
        r = write((int)fd, &byte, 1);
    } while (r < 0 && errno == EINTR);
    /* EAGAIN: the pipe is full, so the waiter wakes anyway. */
}

NATIVE(void, drain)(ENV, jlong fd) {
    char buf[64];
    ssize_t r;
    do {
        r = read((int)fd, buf, sizeof buf);
    } while (r > 0 || (r < 0 && errno == EINTR));
}

/* weida_poll over fds[0, n), up to `timeout_ms` (below 0 forever): the
 * number of ready descriptors, each marked in `ready`; -1 on
 * -WEIDA_EINTR. */
NATIVE(jint, poll)(ENV, jlongArray fds, jint n, jlong timeout_ms, jbooleanArray ready) {
    weida_poll_item stack[64];
    weida_poll_item *items = stack;
    if ((size_t)n > sizeof stack / sizeof *stack) {
        items = malloc((size_t)n * sizeof *items);
        if (!items) {
            throw_oom(env);
            return 0;
        }
    }
    /* Copied out first: nothing of the Java arrays is held while polling. */
    jlong *fdv = (*env)->GetLongArrayElements(env, fds, NULL);
    if (!fdv) {
        if (items != stack)
            free(items);
        return 0;
    }
    for (jint i = 0; i < n; i++)
        items[i] = (weida_poll_item){fdv[i], WEIDA_POLLIN, 0};
    (*env)->ReleaseLongArrayElements(env, fds, fdv, JNI_ABORT);

    int rc = weida_poll(items, (size_t)n, timeout_ms);
    if (rc == -WEIDA_EINTR || rc < 0) {
        if (items != stack)
            free(items);
        if (rc == -WEIDA_EINTR)
            return -1;
        throw_status(env, rc, NULL);
        return 0;
    }
    jboolean *rv = (*env)->GetBooleanArrayElements(env, ready, NULL);
    if (rv) {
        for (jint i = 0; i < n; i++)
            rv[i] = items[i].revents ? JNI_TRUE : JNI_FALSE;
        (*env)->ReleaseBooleanArrayElements(env, ready, rv, 0);
    }
    if (items != stack)
        free(items);
    return rc;
}
