/*
 * The waiter: one thread per napi_env that waits on the readiness
 * descriptors (R11) of every pending JS wait with weida_poll, and reports
 * each one that turns readable to the JS thread through one
 * napi_threadsafe_function.
 *
 * A watch is one-shot: the thread removes a registration when it reports it.
 * The tsfn is referenced while at least one callback that is not weak is
 * pending and unreferenced otherwise, so an idle process exits, and so does
 * one whose only pending watches are weak. A wake pipe (on Windows
 * a loopback UDP socket connected to itself, since `WSAPoll` takes only
 * sockets) interrupts the thread's poll whenever the registrations change.
 * On Windows there are no signals for the thread to block.
 */
#include "addon.h"

#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <process.h>
#else
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#endif

static void run(struct waiter *w);

#ifdef _WIN32

#define WAKE_FAILED "weida waiter: wake socket failed"

static void waiter_lock(struct waiter *w) { AcquireSRWLockExclusive(&w->lock); }

static void waiter_unlock(struct waiter *w) { ReleaseSRWLockExclusive(&w->lock); }

static bool wake_open(struct waiter *w) {
    WSADATA data;
    struct sockaddr_in addr;
    int len = (int)sizeof addr;
    u_long on = 1;
    SOCKET s = INVALID_SOCKET;
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
        w->wake = INVALID_SOCKET;
        return false;
    }
    /* Overlapped, as socket() makes them: on a socket without it, I/O is
     * serialized per handle, so send() on the JS thread would wait for the
     * waiter thread's WSAPoll to return. */
    s = WSASocketW(AF_INET, SOCK_DGRAM, IPPROTO_UDP, NULL, 0,
                   WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT);
    if (s == INVALID_SOCKET)
        goto fail;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(s, (struct sockaddr *)&addr, len) != 0 ||
        getsockname(s, (struct sockaddr *)&addr, &len) != 0 ||
        connect(s, (struct sockaddr *)&addr, len) != 0 || ioctlsocket(s, FIONBIO, &on) != 0)
        goto fail;
    w->wake = s;
    return true;

fail:
    if (s != INVALID_SOCKET)
        closesocket(s);
    WSACleanup();
    w->wake = INVALID_SOCKET;
    return false;
}

static int64_t wake_fd(const struct waiter *w) { return (int64_t)w->wake; }

static void wake(struct waiter *w) {
    /* WSAEWOULDBLOCK: a datagram is already queued, so the thread wakes
     * anyway. */
    send(w->wake, "\1", 1, 0);
}

static void drain(struct waiter *w) {
    char buf[64];
    while (recv(w->wake, buf, (int)sizeof buf, 0) > 0) {
    }
}

static void wake_close(struct waiter *w) {
    closesocket(w->wake);
    w->wake = INVALID_SOCKET;
    WSACleanup();
}

static void pause_briefly(void) { Sleep(10); }

static unsigned __stdcall thread_main(void *arg) {
    run(arg);
    return 0;
}

static bool thread_start(struct waiter *w) {
    uintptr_t h = _beginthreadex(NULL, 0, thread_main, w, 0, NULL);
    if (h == 0)
        return false;
    w->thread = (HANDLE)h;
    return true;
}

static void thread_join(struct waiter *w) {
    WaitForSingleObject(w->thread, INFINITE);
    CloseHandle(w->thread);
}

#else

#define WAKE_FAILED "weida waiter: pipe failed"

static void waiter_lock(struct waiter *w) { pthread_mutex_lock(&w->lock); }

static void waiter_unlock(struct waiter *w) { pthread_mutex_unlock(&w->lock); }

static bool nonblocking(int fd) {
    int fl = fcntl(fd, F_GETFL);
    int fd_fl = fcntl(fd, F_GETFD);
    return fl >= 0 && fd_fl >= 0 && fcntl(fd, F_SETFL, fl | O_NONBLOCK) == 0 &&
           fcntl(fd, F_SETFD, fd_fl | FD_CLOEXEC) == 0;
}

static bool wake_open(struct waiter *w) {
    if (pipe(w->wake) != 0) {
        w->wake[0] = w->wake[1] = -1;
        return false;
    }
    if (!nonblocking(w->wake[0]) || !nonblocking(w->wake[1])) {
        close(w->wake[0]);
        close(w->wake[1]);
        w->wake[0] = w->wake[1] = -1;
        return false;
    }
    return true;
}

static int64_t wake_fd(const struct waiter *w) { return w->wake[0]; }

static void wake(struct waiter *w) {
    char byte = 1;
    ssize_t r;
    do {
        r = write(w->wake[1], &byte, 1);
    } while (r < 0 && errno == EINTR);
    /* EAGAIN: the pipe is full, so the thread wakes anyway. */
}

static void drain(struct waiter *w) {
    char buf[64];
    ssize_t r;
    do {
        r = read(w->wake[0], buf, sizeof buf);
    } while (r > 0 || (r < 0 && errno == EINTR));
}

static void wake_close(struct waiter *w) {
    close(w->wake[0]);
    close(w->wake[1]);
    w->wake[0] = w->wake[1] = -1;
}

static void pause_briefly(void) {
    struct timespec pause = {0, 10 * 1000 * 1000};
    nanosleep(&pause, NULL);
}

static void *thread_main(void *arg) {
    run(arg);
    return NULL;
}

static bool thread_start(struct waiter *w) {
    /* The thread blocks every signal, so a process-directed one reaches
     * the JS thread, whose runtime handles it. */
    sigset_t all, old;
    sigfillset(&all);
    pthread_sigmask(SIG_SETMASK, &all, &old);
    int rc = pthread_create(&w->thread, NULL, thread_main, w);
    pthread_sigmask(SIG_SETMASK, &old, NULL);
    return rc == 0;
}

static void thread_join(struct waiter *w) { pthread_join(w->thread, NULL); }

#endif

void waiter_init(struct waiter *w) {
    memset(w, 0, sizeof *w);
#ifdef _WIN32
    InitializeSRWLock(&w->lock);
    w->wake = INVALID_SOCKET;
#else
    pthread_mutex_init(&w->lock, NULL);
    w->wake[0] = w->wake[1] = -1;
#endif
}

/* Under `lock`. */
static bool remove_registration(struct waiter *w, uint32_t id) {
    for (size_t i = 0; i < w->nregs; i++) {
        if (w->regs[i].id == id) {
            w->regs[i] = w->regs[--w->nregs];
            return true;
        }
    }
    return false;
}

static void run(struct waiter *w) {
    weida_poll_item *items = NULL;
    uint32_t *ids = NULL, *fired = NULL;
    size_t cap = 0;

    for (;;) {
        waiter_lock(w);
        if (w->stop) {
            waiter_unlock(w);
            break;
        }
        size_t n = w->nregs + 1;
        if (n > cap) {
            size_t want = n * 2;
            weida_poll_item *ni = realloc(items, want * sizeof *items);
            if (ni)
                items = ni;
            uint32_t *nids = realloc(ids, want * sizeof *ids);
            if (nids)
                ids = nids;
            uint32_t *nf = realloc(fired, want * sizeof *fired);
            if (nf)
                fired = nf;
            if (!ni || !nids || !nf)
                abort();
            cap = want;
        }
        items[0] = (weida_poll_item){wake_fd(w), WEIDA_POLLIN, 0};
        ids[0] = 0;
        for (size_t i = 0; i < w->nregs; i++) {
            items[i + 1] = (weida_poll_item){w->regs[i].fd, WEIDA_POLLIN, 0};
            ids[i + 1] = w->regs[i].id;
        }
        waiter_unlock(w);

        int rc = weida_poll(items, n, -1);
        if (rc == -WEIDA_EINTR)
            continue;
        if (rc < 0) {
            /* poll(2) itself failed (EIO): back off instead of spinning;
             * the next round polls a fresh copy of the registrations. */
            pause_briefly();
            continue;
        }
        if (items[0].revents)
            drain(w);

        size_t nfired = 0;
        waiter_lock(w);
        for (size_t i = 1; i < n; i++) {
            /* Only a registration still present fires: one the JS thread
             * removed meanwhile may name a descriptor already closed. */
            if (items[i].revents && remove_registration(w, ids[i]))
                fired[nfired++] = ids[i];
        }
        waiter_unlock(w);
        for (size_t i = 0; i < nfired; i++)
            napi_call_threadsafe_function(w->tsfn, (void *)(uintptr_t)fired[i],
                                          napi_tsfn_nonblocking);
    }

    free(items);
    free(ids);
    free(fired);
}

/* JS thread: takes the pending callback of `id` out of the table, and
 * unreferences the tsfn when it was the last one that is not weak. */
static bool take_callback(napi_env env, struct waiter *w, uint32_t id, napi_ref *out) {
    for (size_t i = 0; i < w->ncbs; i++) {
        if (w->cbs[i].id == id) {
            bool weak = w->cbs[i].weak;
            *out = w->cbs[i].fn;
            w->cbs[i] = w->cbs[--w->ncbs];
            if (!weak && --w->nstrong == 0)
                napi_unref_threadsafe_function(env, w->tsfn);
            return true;
        }
    }
    return false;
}

static void call_js(napi_env env, napi_value js_cb, void *context, void *data) {
    (void)js_cb;
    struct waiter *w = context;
    napi_ref ref;
    napi_value fn = NULL, undefined, result;
    if (!env || !take_callback(env, w, (uint32_t)(uintptr_t)data, &ref))
        return; /* tearing down, or unwatched after the thread fired it */
    napi_get_reference_value(env, ref, &fn);
    napi_delete_reference(env, ref);
    if (fn && napi_get_undefined(env, &undefined) == napi_ok)
        napi_call_function(env, undefined, fn, 0, NULL, &result);
}

void waiter_stop(struct waiter *w) {
    if (!w->started)
        return;
    waiter_lock(w);
    w->stop = true;
    waiter_unlock(w);
    wake(w);
    thread_join(w);
    wake_close(w);
    w->started = false;
}

void waiter_free(struct waiter *w) {
    waiter_stop(w);
    free(w->regs);
    free(w->cbs);
#ifndef _WIN32
    pthread_mutex_destroy(&w->lock);
#endif
}

/* The env is going away: stop the thread before Node closes the tsfn it
 * calls. Registered after the tsfn was created, so it runs before the tsfn's
 * own cleanup (cleanup hooks run in reverse order). */
static void cleanup(void *arg) {
    struct addon *a = arg;
    waiter_stop(&a->waiter);
    addon_release(a);
}

static bool start(napi_env env, struct addon *a) {
    struct waiter *w = &a->waiter;
    napi_value name;
    if (w->started)
        return true;
    if (!wake_open(w)) {
        napi_throw_error(env, NULL, WAKE_FAILED);
        return false;
    }
    if (napi_create_string_utf8(env, "weida waiter", NAPI_AUTO_LENGTH, &name) != napi_ok ||
        napi_create_threadsafe_function(env, NULL, NULL, name, 0, 1, NULL, NULL, w, call_js,
                                        &w->tsfn) != napi_ok)
        goto fail_wake;
    napi_unref_threadsafe_function(env, w->tsfn);

    if (!thread_start(w)) {
        napi_release_threadsafe_function(w->tsfn, napi_tsfn_abort);
        goto fail_wake;
    }
    w->started = true;
    a->owners++;
    napi_add_env_cleanup_hook(env, cleanup, a);
    return true;

fail_wake:
    wake_close(w);
    throw_napi(env);
    return false;
}

/* `watch(fd, callback, weak?) -> id`: calls `callback()` once on the JS
 * thread when `fd` turns readable, unless `unwatch(id)` came first. A weak
 * watch (`weak === true`) is reported like any other but does not keep the
 * process alive. */
napi_value waiter_watch(napi_env env, napi_callback_info info) {
    napi_value argv[3], result;
    size_t argc = 3;
    napi_valuetype type;
    int64_t fd = -1;
    bool weak = false;
    napi_ref ref;
    struct addon *a = addon_of(env);
    struct waiter *w = &a->waiter;

    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok)
        return throw_napi(env);
    if (napi_typeof(env, argv[0], &type) != napi_ok || type != napi_number ||
        napi_get_value_int64(env, argv[0], &fd) != napi_ok || fd < 0)
        return throw_type(env, "expected a descriptor");
    if (napi_typeof(env, argv[1], &type) != napi_ok || type != napi_function)
        return throw_type(env, "expected a function");
    if (napi_typeof(env, argv[2], &type) == napi_ok && type == napi_boolean &&
        napi_get_value_bool(env, argv[2], &weak) != napi_ok)
        return throw_napi(env);
    if (!start(env, a))
        return NULL;

    if (w->ncbs == w->capcbs) {
        size_t want = w->capcbs ? w->capcbs * 2 : 8;
        struct callback *n = realloc(w->cbs, want * sizeof *n);
        if (!n)
            goto oom;
        w->cbs = n;
        w->capcbs = want;
    }
    waiter_lock(w);
    if (w->nregs == w->capregs) {
        size_t want = w->capregs ? w->capregs * 2 : 8;
        struct registration *n = realloc(w->regs, want * sizeof *n);
        if (!n) {
            waiter_unlock(w);
            goto oom;
        }
        w->regs = n;
        w->capregs = want;
    }
    waiter_unlock(w);

    if (napi_create_reference(env, argv[1], 1, &ref) != napi_ok)
        return throw_napi(env);
    uint32_t id = ++w->next_id;
    if (id == 0)
        id = ++w->next_id;
    w->cbs[w->ncbs++] = (struct callback){id, ref, weak};
    if (!weak && w->nstrong++ == 0)
        napi_ref_threadsafe_function(env, w->tsfn);

    waiter_lock(w);
    w->regs[w->nregs++] = (struct registration){id, fd};
    waiter_unlock(w);
    wake(w);

    if (napi_create_uint32(env, id, &result) != napi_ok)
        return throw_napi(env);
    return result;

oom:
    napi_throw_error(env, NULL, "out of memory");
    return NULL;
}

/* `unwatch(id) -> boolean`: whether the watch was still pending. */
napi_value waiter_unwatch(napi_env env, napi_callback_info info) {
    napi_value argv[1], result;
    size_t argc = 1;
    uint32_t id = 0;
    napi_ref ref;
    struct waiter *w = &addon_of(env)->waiter;

    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok)
        return throw_napi(env);
    if (napi_get_value_uint32(env, argv[0], &id) != napi_ok)
        return throw_type(env, "expected a watch id");
    bool pending = take_callback(env, w, id, &ref);
    if (pending)
        napi_delete_reference(env, ref);
    if (w->started) {
        waiter_lock(w);
        bool removed = remove_registration(w, id);
        waiter_unlock(w);
        /* The thread may be polling the descriptor its owner closes next. */
        if (removed)
            wake(w);
    }
    if (napi_get_boolean(env, pending, &result) != napi_ok)
        return throw_napi(env);
    return result;
}
