# Errors (R9, R10)

The codes and their values are the tables of [README.md](README.md) R9. This page is what a
caller does with them. Every function here is thread-safe and reads or writes only the calling
thread's record; none blocks.

| Function | Returns |
| --- | --- |
| `const char *weida_strerror(int code)` | a static description of a code of either sign; `"unknown error"` for one the library does not define |
| `const char *weida_last_error_message(void)` | the `Display` text of this thread's last failure, interior NULs replaced by U+FFFD; NULL when no call on this thread has failed |
| `int weida_last_error_loss_cause(void)` | a `WEIDA_LOSS_*` when the last failure was `WEIDA_ECONNECTION_LOST`, else 0 |
| `const char *weida_last_error_fingerprint(void)` | the key the peer presented when the last failure was `WEIDA_EUNTRUSTED`, else NULL |
| `int64_t weida_last_error_max(void)` | the largest datagram the connection carries when the last failure was `WEIDA_ETOO_LARGE`, else -1 |
| `int weida_last_error_os_code(void)` | the raw OS error when the last failure was `WEIDA_EIO` and had one, else 0 |

**The record.** Every failing `int` function stores a record on the calling thread; a success
leaves it alone. The strings it hands out stay valid until the next failure on that thread. Read
it right after the failing call, on the same thread: an op's failure is recorded by the `_wait`
that takes it, on the waiting thread, not on the reactor thread that ran the operation.

**What a code means for the next step.** `WEIDA_EAGAIN` and `WEIDA_EINTR` are not failures of
the operation: nothing was lost, and the same call continues it (R3a, R4). `WEIDA_ECLOSED`
means the handle cannot be used again. `WEIDA_EPANIC` reports a bug in the library and leaves
the handle in whatever state the panic left it; drop it. The codes 1 to 24 are weida's own
outcomes, with the meanings of [FAILURE_MODEL.md](../FAILURE_MODEL.md): `WEIDA_ECONNECTION_LOST`
and the refusals (`WEIDA_EREJECTED`, `WEIDA_EUNKNOWN_ENDPOINT`, ...) prove the transfer had no
effect at the peer, `WEIDA_EINDETERMINATE` and `WEIDA_ENO_REPLY` do not.

**Panics.** Every export runs in `catch_unwind` (R10): an `int` function returns
`-WEIDA_EPANIC` with `panic: <payload>` as its message, a pointer getter returns NULL, and a
`_drop` swallows it. A panic never unwinds into the caller's frames.
