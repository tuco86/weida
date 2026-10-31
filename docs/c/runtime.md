# Runtime and configuration

The rules are in [README.md](README.md); signatures are those of `weida.h`. Every function here
is callable from any thread (R5). "Never blocks" means the call returns without waiting on the
network or another thread; a lock it takes is held only for the call's own bookkeeping.

## `weida_runtime_config`

A builder for `weida::RuntimeConfig`, one setter per field this stage maps. Unset fields keep
weida's defaults.

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_config_new(weida_runtime_config **out)` | never | 0; `-WEIDA_EINVAL` for a NULL `out` |
| `int weida_runtime_config_set_worker_threads(const weida_runtime_config *cfg, uint32_t threads)` | never | 0; `-WEIDA_EINVAL`. `0` is accepted here and refused by `weida_runtime_owned` |
| `int weida_runtime_config_set_shutdown_timeout_ms(const weida_runtime_config *cfg, uint64_t ms)` | never | 0; `-WEIDA_EINVAL` |
| `void weida_runtime_config_drop(weida_runtime_config *cfg)` | never | — |

## `weida_runtime`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_owned(const weida_runtime_config *cfg_or_null, weida_runtime **out)` | never; starts the reactor's threads | 0; `-WEIDA_ERUNTIME` when `worker_threads` is 0 or the OS refuses threads; `-WEIDA_EINVAL` |
| `int weida_runtime_listener(const weida_runtime *rt, weida_listener **out)` | never | 0; `-WEIDA_EINVAL` ([listener.md](listener.md)) |
| `int weida_runtime_requester(const weida_runtime *rt, const weida_client_tls *tls, weida_requester **out)` | never | 0; `-WEIDA_EINVAL` ([requester.md](requester.md)) |
| `int weida_runtime_shutdown(weida_runtime *rt, weida_runtime_shutdown_op **out)` | never; **consumes** `rt` | 0; `-WEIDA_EINVAL` |
| `void weida_runtime_drop(weida_runtime *rt)` | never | — |

The reactor's threads, its blocking pool included, block every signal except SIGSEGV, SIGBUS,
SIGILL and SIGFPE (R12), so a process signal lands on a caller thread.

`weida_runtime_drop` releases the caller's reference only. Listeners, requesters and everything
made from them keep the reactor alive; once the last of them is gone it shuts down in the
background (R13). `weida_runtime_shutdown` closes every endpoint at once, whatever else is still
held, and waits for the connections to go idle, bounded by `shutdown_timeout`.

## `weida_runtime_shutdown_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_shutdown_op_fd(const weida_runtime_shutdown_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` when the descriptor pair cannot be made; `-WEIDA_EINVAL` |
| `int weida_runtime_shutdown_op_wait(const weida_runtime_shutdown_op *op, int64_t timeout_ms)` | up to `timeout_ms` (R4) | 0 when the shutdown finished; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` (taken or dropped) |
| `void weida_runtime_shutdown_op_drop(weida_runtime_shutdown_op *op)` | never | — |

Dropping the op without waiting does not stop the shutdown it started: the endpoints are already
closed.

## `weida_version`

`const char *weida_version(void)`: the library's version, e.g. `"0.1.0-alpha.11-dev"`, a
static string.
