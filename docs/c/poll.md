# Readiness and `weida_poll` (R11)

Every handle that can become ready has one descriptor: a try handle (replier, incoming and
outgoing transfer) and every op. It is a Unix file descriptor, or a socket on Windows, created on
the first `_fd` call or the first blocking wait, and it lives as long as its handle.

**The contract.** The descriptor is readable when the next try or `_wait` on its handle will not
block. That call may still return `-WEIDA_EAGAIN` once, after a wake that turned out stale, and
an `EAGAIN` leaves the descriptor unreadable until there is news. An op's descriptor is readable
exactly while its result waits.

**Try first.** A try handle's descriptor is raised by a wake, and only a pending try registers
for one. An event loop therefore calls the try with `timeout_ms = 0`, and only on `EAGAIN`
watches the descriptor; when it turns readable, it tries again. A blocking caller passes a
timeout instead and the library does exactly this itself.

```c
int64_t fd;
weida_incoming_request *q;
int rc = weida_replier_accept(r, 0, &q);
if (rc == -WEIDA_EAGAIN && weida_replier_fd(r, &fd) == 0) {
    weida_poll_item item = {fd, WEIDA_POLLIN, 0};
    if (weida_poll(&item, 1, 5000) > 0)
        rc = weida_replier_accept(r, 0, &q);
}
```

## `weida_poll`

`int weida_poll(weida_poll_item *items, size_t n, int64_t timeout_ms)`

```c
typedef struct {
    int64_t fd;      /* from a _fd function; negative: skipped */
    int16_t events;  /* WEIDA_POLLIN */
    int16_t revents; /* written: WEIDA_POLLIN or 0 */
} weida_poll_item;
```

| Returns | When |
| --- | --- |
| `n > 0` | `n` items have `revents != 0` |
| 0 | `timeout_ms` passed (below 0 waits forever, 0 only looks) |
| `-WEIDA_EINTR` | a signal reached this thread (never on Windows) |
| `-WEIDA_EINVAL` | `n > 0` and `items` is NULL, or a descriptor out of range |
| `-WEIDA_EIO` | `poll` or `WSAPoll` failed |

It blocks the calling thread for up to `timeout_ms` and holds no lock of any handle while it
does. The descriptors are ordinary ones: an application may hand them to its own `epoll`,
`kqueue`, `select` or event loop instead (Python's `loop.add_reader`), watching for readability.
On Windows they are sockets, which `WSAPoll` and `select` accept and a Proactor loop does not.
