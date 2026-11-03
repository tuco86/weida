# The easy tier: weida_easy.h

`crates/weida-c/include/weida_easy.h` is `static inline` C over the exports of `weida.h`. It is
not part of the ABI and adds no export, so the rules R1 to R13 of [README.md](README.md) bind
only the functions it calls. Each helper fuses the start, the wait and the release of the ops of
one Req/Rep step, and the TLS setup, into one blocking call with one deadline, much like
libcurl's easy interface over its multi interface. Its names carry the `weida_easy_` prefix;
names ending in `_` are internal.

Include it instead of `weida.h`, which it includes. On POSIX it needs
`clock_gettime(CLOCK_MONOTONIC)`: under strict `-std=c11` on glibc or musl, define
`_POSIX_C_SOURCE 200809L` before the first `#include` (or compile with `-std=gnu11`), otherwise
the header stops with an `#error`. On Windows it uses `GetTickCount64` from `<windows.h>`.

```c
#define _POSIX_C_SOURCE 200809L
#include "weida_easy.h"
```

## Semantics

- **One deadline.** `timeout_ms` bounds the whole helper, not each step: the helper takes a
  monotonic deadline on entry and hands every wait the time left. Otherwise R4 applies: below 0
  waits forever, 0 only tries, and a wait that runs out returns `-WEIDA_EAGAIN`.
- **Errors.** A helper returns 0 on success and otherwise the first failing call's code,
  unchanged. That call's thread-local record (`weida_last_error_message` and the other
  `weida_last_error_*`) stays readable. A NULL out-parameter is `-WEIDA_EINVAL` without a
  record, because a header cannot store one.
- **No resumption.** On any failure, `-WEIDA_EAGAIN` and `-WEIDA_EINTR` included, the helper
  drops every op and handle it created: an unfinished op's task is aborted (R3b), and the
  out-parameters stay unwritten (R2). The one exception is the accept step of
  `weida_easy_accept`, a try (R3a) whose in-flight take stays in the replier, so a later accept
  loses nothing. A failure after that step drops the request, and the requester sees
  `NO_REPLY`.
- **A retry may send twice.** When a helper gives up, its request may already have reached the
  peer; calling it again sends the request again.
- **Who it is for.** Programs without their own signal handling. A program that installs
  handlers and wants to resume a wait after `-WEIDA_EINTR` uses the primitives, as in
  [The EINTR retry loop](README.md#the-eintr-retry-loop).

## weida_easy_client_tls_pinned

```c
int weida_easy_client_tls_pinned(const char *fingerprint, weida_client_tls **out);
```

Client terms that trust exactly the pinned `fingerprint` (`sha256:` plus 64 hex digits). Calls
`weida_trust_new`, `weida_trust_add_pin` and `weida_client_tls_new`, and drops the trust in any
case. Release the terms with `weida_client_tls_drop`.

Returns 0, `-WEIDA_EINVAL` for a NULL `out` (no record), or the failing call's code: a NULL or
malformed `fingerprint` fails in `weida_trust_add_pin` with `-WEIDA_EINVAL` or
`-WEIDA_EINVALID_FINGERPRINT`.

## weida_easy_connect

```c
int weida_easy_connect(const weida_requester *r, const char *url, int64_t timeout_ms);
```

Dials `url` and waits until the connection is up. Calls `weida_requester_connect`,
`weida_requester_connect_op_wait` and `weida_requester_connect_op_drop`, which aborts an
unfinished dial.

Returns 0 or the failing call's code; a NULL `r` or `url` is `-WEIDA_EINVAL` from
`weida_requester_connect`.

## weida_easy_call

```c
int weida_easy_call(const weida_requester *r, const uint8_t *body, size_t len,
                    uint64_t max_bytes, int64_t timeout_ms, weida_msg **out);
```

Sends `len` bytes of `body` as one request and collects the reply, at most `max_bytes` of it,
into `*out`. Calls `weida_requester_request`, `weida_requester_request_op_wait`,
`weida_incoming_transfer_collect` and `weida_incoming_transfer_collect_op_wait`, dropping each
op after its wait. The reply's metadata travels in the message (`weida_msg_meta`). Release the
message with `weida_msg_drop`.

Returns 0, `-WEIDA_EINVAL` for a NULL `out` (no record), or the failing call's code, for
example a refusal of the request or `-WEIDA_ELIMIT_EXCEEDED` for a reply over `max_bytes`.

## weida_easy_accept

```c
int weida_easy_accept(const weida_replier *rep, uint64_t max_bytes, int64_t timeout_ms,
                      weida_incoming_request **out_request, weida_msg **out_body);
```

Accepts the next request and collects its body, at most `max_bytes` of it. Calls
`weida_replier_accept`, `weida_incoming_request_take_body`, `weida_incoming_transfer_collect`
and `weida_incoming_transfer_collect_op_wait`, dropping the collect op after its wait. On
success the caller owns both: it answers the request with `weida_easy_reply` (or the
primitives) or drops it, and releases the body with `weida_msg_drop`.

Returns 0, `-WEIDA_EINVAL` for a NULL out-parameter (no record), `-WEIDA_EAGAIN` when no
request arrived in time, or the failing call's code. A failure after the accept drops the
request (`NO_REPLY` to the requester).

## weida_easy_reply

```c
int weida_easy_reply(weida_incoming_request *q, const weida_transfer_meta *meta,
                     const uint8_t *data, size_t len, int64_t timeout_ms);
```

Answers `q` with `len` bytes of `data` under `meta` (NULL: default metadata) and waits until
the requester's transport has acknowledged the whole payload. **Consumes** `q` on every return
path. Calls `weida_incoming_request_reply`, `weida_incoming_request_reply_op_wait`,
`weida_outgoing_transfer_write` until all of `data` is written, `weida_outgoing_transfer_finish`,
`weida_delivery_delivered` and `weida_delivery_delivered_op_wait`. A failed write drops the
outgoing transfer, which resets it `CANCELED`. `meta` stays the caller's.

Returns 0 or the failing call's code. A reply the requester refused returns the refusal, for
example `-WEIDA_EREJECTED`. A NULL `data` with `len > 0` is `-WEIDA_EINVAL` from the write.

## weida_easy_shutdown

```c
int weida_easy_shutdown(weida_runtime *rt, int64_t timeout_ms);
```

Shuts the runtime down and waits until it stopped. **Consumes** `rt`. Calls
`weida_runtime_shutdown`, `weida_runtime_shutdown_op_wait` and `weida_runtime_shutdown_op_drop`.

Returns 0 or the failing call's code.

## A minimal client

```c
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include "weida_easy.h"

/* Sends "ping" to `url`, whose server presents `fingerprint`. */
int ping(const char *url, const char *fingerprint) {
    weida_runtime *rt = NULL;
    weida_client_tls *tls = NULL;
    weida_requester *requester = NULL;
    weida_msg *reply = NULL;
    int rc = weida_runtime_owned(NULL, &rt);
    if (rc < 0)
        return rc;
    if ((rc = weida_easy_client_tls_pinned(fingerprint, &tls)) >= 0) {
        rc = weida_runtime_requester(rt, tls, &requester);
        weida_client_tls_drop(tls);
    }
    if (rc >= 0)
        rc = weida_easy_connect(requester, url, 5000);
    if (rc >= 0)
        rc = weida_easy_call(requester, (const uint8_t *)"ping", 4, 1 << 20, 5000, &reply);
    if (rc >= 0) {
        printf("%.*s\n", (int)weida_msg_len(reply), (const char *)weida_msg_data(reply));
        weida_msg_drop(reply);
    }
    weida_requester_drop(requester);
    int down = weida_easy_shutdown(rt, 5000);
    return rc < 0 ? rc : down;
}
```

`crates/weida-c/tests/c/easy.c` runs both sides, the replier on its own thread.
