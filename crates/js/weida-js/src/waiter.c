/*
 * The waiter: one thread per napi_env that waits on the readiness
 * descriptors (R11) of every pending JS wait with weida_poll, and reports
 * each one that turns readable to the JS thread through one
 * napi_threadsafe_function.
 *
 * A watch is one-shot: the thread removes a registration when it reports it.
 * The tsfn is referenced while at least one JS callback is pending and
 * unreferenced otherwise, so an idle process exits. A wake pipe interrupts
 * the thread's poll whenever the registrations change.
 */
#include "addon.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

void waiter_init(struct waiter *w) {
    memset(w, 0, sizeof *w);
    pthread_mutex_init(&w->lock, NULL);
    w->wake[0] = w->wake[1] = -1;
}

static void wake(struct waiter *w) {
    char byte = 1;
    ssize_t r;
    do {
        r = write(w->wake[1], &byte, 1);
    } while (r < 0 && errno == EINTR);
    /* EAGAIN: the pipe is full, so the thread wakes anyway. */
}

static void drain(int fd) {
    char buf[64];
    ssize_t r;
    do {
        r = read(fd, buf, sizeof buf);
    } while (r > 0 || (r < 0 && errno == EINTR));
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

static void *run(void *arg) {
    struct waiter *w = arg;
    weida_poll_item *items = NULL;
    uint32_t *ids = NULL, *fired = NULL;
    size_t cap = 0;

    for (;;) {
        pthread_mutex_lock(&w->lock);
        if (w->stop) {
            pthread_mutex_unlock(&w->lock);
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
        items[0] = (weida_poll_item){w->wake[0], WEIDA_POLLIN, 0};
        ids[0] = 0;
        for (size_t i = 0; i < w->nregs; i++) {
            items[i + 1] = (weida_poll_item){w->regs[i].fd, WEIDA_POLLIN, 0};
            ids[i + 1] = w->regs[i].id;
        }
        pthread_mutex_unlock(&w->lock);

        int rc = weida_poll(items, n, -1);
        if (rc == -WEIDA_EINTR)
            continue;
        if (rc < 0) {
            /* poll(2) itself failed (EIO): back off instead of spinning;
             * the next round polls a fresh copy of the registrations. */
            struct timespec pause = {0, 10 * 1000 * 1000};
            nanosleep(&pause, NULL);
            continue;
        }
        if (items[0].revents)
            drain(w->wake[0]);

        size_t nfired = 0;
        pthread_mutex_lock(&w->lock);
        for (size_t i = 1; i < n; i++) {
            /* Only a registration still present fires: one the JS thread
             * removed meanwhile may name a descriptor already closed. */
            if (items[i].revents && remove_registration(w, ids[i]))
                fired[nfired++] = ids[i];
        }
        pthread_mutex_unlock(&w->lock);
        for (size_t i = 0; i < nfired; i++)
            napi_call_threadsafe_function(w->tsfn, (void *)(uintptr_t)fired[i],
                                          napi_tsfn_nonblocking);
    }

    free(items);
    free(ids);
    free(fired);
    return NULL;
}

/* JS thread: takes the pending callback of `id` out of the table, and
 * unreferences the tsfn when it was the last one. */
static bool take_callback(napi_env env, struct waiter *w, uint32_t id, napi_ref *out) {
    for (size_t i = 0; i < w->ncbs; i++) {
        if (w->cbs[i].id == id) {
            *out = w->cbs[i].fn;
            w->cbs[i] = w->cbs[--w->ncbs];
            if (w->ncbs == 0)
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
    pthread_mutex_lock(&w->lock);
    w->stop = true;
    pthread_mutex_unlock(&w->lock);
    wake(w);
    pthread_join(w->thread, NULL);
    close(w->wake[0]);
    close(w->wake[1]);
    w->wake[0] = w->wake[1] = -1;
    w->started = false;
}

void waiter_free(struct waiter *w) {
    waiter_stop(w);
    free(w->regs);
    free(w->cbs);
    pthread_mutex_destroy(&w->lock);
}

/* The env is going away: stop the thread before Node closes the tsfn it
 * calls. Registered after the tsfn was created, so it runs before the tsfn's
 * own cleanup (cleanup hooks run in reverse order). */
static void cleanup(void *arg) {
    struct addon *a = arg;
    waiter_stop(&a->waiter);
    addon_release(a);
}

static bool nonblocking(int fd) {
    int fl = fcntl(fd, F_GETFL);
    int fd_fl = fcntl(fd, F_GETFD);
    return fl >= 0 && fd_fl >= 0 && fcntl(fd, F_SETFL, fl | O_NONBLOCK) == 0 &&
           fcntl(fd, F_SETFD, fd_fl | FD_CLOEXEC) == 0;
}

static bool start(napi_env env, struct addon *a) {
    struct waiter *w = &a->waiter;
    napi_value name;
    sigset_t all, old;
    if (w->started)
        return true;
    if (pipe(w->wake) != 0) {
        napi_throw_error(env, NULL, "weida waiter: pipe failed");
        return false;
    }
    if (!nonblocking(w->wake[0]) || !nonblocking(w->wake[1]))
        goto fail_pipe;
    if (napi_create_string_utf8(env, "weida waiter", NAPI_AUTO_LENGTH, &name) != napi_ok ||
        napi_create_threadsafe_function(env, NULL, NULL, name, 0, 1, NULL, NULL, w, call_js,
                                        &w->tsfn) != napi_ok)
        goto fail_pipe;
    napi_unref_threadsafe_function(env, w->tsfn);

    /* The thread blocks every signal, so a process-directed one reaches
     * the JS thread, whose runtime handles it. */
    sigfillset(&all);
    pthread_sigmask(SIG_SETMASK, &all, &old);
    int rc = pthread_create(&w->thread, NULL, run, w);
    pthread_sigmask(SIG_SETMASK, &old, NULL);
    if (rc != 0) {
        napi_release_threadsafe_function(w->tsfn, napi_tsfn_abort);
        goto fail_pipe;
    }
    w->started = true;
    a->owners++;
    napi_add_env_cleanup_hook(env, cleanup, a);
    return true;

fail_pipe:
    close(w->wake[0]);
    close(w->wake[1]);
    w->wake[0] = w->wake[1] = -1;
    throw_napi(env);
    return false;
}

/* `watch(fd, callback) -> id`: calls `callback()` once on the JS thread when
 * `fd` turns readable, unless `unwatch(id)` came first. */
napi_value waiter_watch(napi_env env, napi_callback_info info) {
    napi_value argv[2], result;
    size_t argc = 2;
    napi_valuetype type;
    int64_t fd = -1;
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
    pthread_mutex_lock(&w->lock);
    if (w->nregs == w->capregs) {
        size_t want = w->capregs ? w->capregs * 2 : 8;
        struct registration *n = realloc(w->regs, want * sizeof *n);
        if (!n) {
            pthread_mutex_unlock(&w->lock);
            goto oom;
        }
        w->regs = n;
        w->capregs = want;
    }
    pthread_mutex_unlock(&w->lock);

    if (napi_create_reference(env, argv[1], 1, &ref) != napi_ok)
        return throw_napi(env);
    uint32_t id = ++w->next_id;
    if (id == 0)
        id = ++w->next_id;
    w->cbs[w->ncbs++] = (struct callback){id, ref};
    if (w->ncbs == 1)
        napi_ref_threadsafe_function(env, w->tsfn);

    pthread_mutex_lock(&w->lock);
    w->regs[w->nregs++] = (struct registration){id, fd};
    pthread_mutex_unlock(&w->lock);
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
        pthread_mutex_lock(&w->lock);
        bool removed = remove_registration(w, id);
        pthread_mutex_unlock(&w->lock);
        /* The thread may be polling the descriptor its owner closes next. */
        if (removed)
            wake(w);
    }
    if (napi_get_boolean(env, pending, &result) != napi_ok)
        return throw_napi(env);
    return result;
}
