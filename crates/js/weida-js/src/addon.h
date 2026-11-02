/*
 * The Node-API addon over libweida: state shared by addon.c (one function
 * per weida.h function) and waiter.c (the readiness thread).
 */
#ifndef WEIDA_JS_ADDON_H
#define WEIDA_JS_ADDON_H

#define _POSIX_C_SOURCE 200809L
#define NAPI_VERSION 8
#include <node_api.h>

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "weida.h"

/* One watched descriptor; the waiter thread reads these under `lock`. */
struct registration {
    uint32_t id;
    int64_t fd;
};

/* The JS callback of one watch; touched on the JS thread only. */
struct callback {
    uint32_t id;
    napi_ref fn;
};

struct waiter {
    /* Guarded by `lock`: the registrations and the stop flag. */
    pthread_mutex_t lock;
    struct registration *regs;
    size_t nregs, capregs;
    bool stop;

    /* Set on the first watch and cleared by waiter_stop, on the JS thread. */
    bool started;
    int wake[2];
    pthread_t thread;
    napi_threadsafe_function tsfn;

    /* JS thread only. */
    struct callback *cbs;
    size_t ncbs, capcbs;
    uint32_t next_id;
};

/* The instance data of one napi_env. It has two owners: the env's instance
 * data finalizer and, once the waiter started, the env cleanup hook that
 * stops it. The last of the two to run frees it; both run on the JS thread. */
struct addon {
    napi_ref error_class;
    napi_ref again;
    int owners;
    struct waiter waiter;
};

struct addon *addon_of(napi_env env);

/* Releases one owner of `a`, freeing it with the last. */
void addon_release(struct addon *a);

/* Throws the error a failed C call returned (`rc` negative), with the
 * thread-local record; returns NULL for the caller to return. */
napi_value throw_rc(napi_env env, int rc);

/* Throws a TypeError, or the Node-API failure that is pending; both return
 * NULL. */
napi_value throw_type(napi_env env, const char *message);
napi_value throw_napi(napi_env env);

void waiter_init(struct waiter *w);
/* Stops and joins the thread and closes the wake pipe; idempotent. */
void waiter_stop(struct waiter *w);
/* Frees what waiter_init and the watches allocated; after waiter_stop. */
void waiter_free(struct waiter *w);
napi_value waiter_watch(napi_env env, napi_callback_info info);
napi_value waiter_unwatch(napi_env env, napi_callback_info info);

#endif
