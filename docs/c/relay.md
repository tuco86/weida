# The relay and serving through one

Every function is callable from any thread (R5). Every `int` function can also return
`-WEIDA_EINVAL` (a NULL or a non-UTF-8 string) and `-WEIDA_EPANIC` (R10); the tables list the
rest.

A hidden peer that can only dial out serves its listener's endpoints through a relay with
`weida_listener_bind_relay` ([listener.md](listener.md)); diallers reach them as
`weida+relay://<peer-key>@[relay-key@]host[:port]/path` through `weida_requester_connect`, with
QUIC and TLS end to end (PROTOCOL §9.6). A relay serves `/.weida/relay` on a listener with
`weida_listener_relay`; what it serves is the application's decision, one `weida_relay_request`
at a time.

## `weida_listener_bind_relay_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_bind_relay_op_fd(const weida_listener_bind_relay_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_listener_bind_relay_op_wait(const weida_listener_bind_relay_op *op, int64_t timeout_ms, weida_binding **out)` | up to `timeout_ms` | 0 and a relayed binding; `-WEIDA_EREJECTED` (the relay refused the registration), `-WEIDA_EINVALID_ADDRESS` (not `/.weida/relay`), `-WEIDA_EUNTRUSTED`, `-WEIDA_ETLS`, `-WEIDA_ECONNECTION_LOST`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_listener_bind_relay_op_drop(weida_listener_bind_relay_op *op)` | never | — |

`relay` is `weida://[relay-key@]host[:port]/.weida/relay`. The peer registers under the key
`via` presents, so `via` needs an identity; `tls` is its server identity and client trust, as
for `weida_listener_bind_quic`, and every endpoint sees the dialler's own key, not the relay's.
The registration is kept like a dialled address: a lost relay is redialled and registered again
under the runtime's `reconnect` policy, reported on `weida_binding_relay_events`. A relayed
binding cannot be handed over.

## `weida_relay` (a try handle, R3a)

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_relay_fd(const weida_relay *r, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_relay_accept(const weida_relay *r, int64_t timeout_ms, weida_relay_request **out)` | up to `timeout_ms` | 0; `-WEIDA_ENOT_CONNECTED` once the relay stopped serving; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_relay_drop(weida_relay *r)` | never | — |

The accept in flight is kept in the relay between tries: a timeout or `EINTR` loses no request,
and the next call continues it. Readiness follows R11: **try first**. The descriptor is raised
by a wake, and only a pending try registers for one, so a caller polls the descriptor after an
`EAGAIN`, not before its first try.

An anonymous registration, a tunnel to a key nobody registered, and an unknown preamble are
refused before they reach `accept`. Dropping the relay unregisters `/.weida/relay`, closes every
registration's connection with `SHUTDOWN`, so the hidden peers redial, and ends every wait on
it with `-WEIDA_ECLOSED`.

## `weida_relay_request`

| Function | Blocks | Returns |
| --- | --- | --- |
| `const char *weida_relay_request_peer_fingerprint(const weida_relay_request *q, size_t *len_out)` | never | the `sha256:` key the asker proved; NULL for an anonymous dialler, always a key for a registration |
| `const char *weida_relay_request_target(const weida_relay_request *q, size_t *len_out)` | never | NULL for a registration; for a tunnel, the `sha256:` key of the peer it reaches |
| `int weida_relay_request_admit(weida_relay_request *q, weida_relay_request_admit_op **out)` | never; **consumes** `q` | 0; `-WEIDA_ECLOSED` |
| `int weida_relay_request_refuse(weida_relay_request *q, weida_relay_request_refuse_op **out)` | never; **consumes** `q` | 0; `-WEIDA_ECLOSED` |
| `void weida_relay_request_drop(weida_relay_request *q)` | never | — |

Admitting serves the registration or the tunnel. A registration replaces one of the same key
on another connection, and closes that connection. Refusing fails the asker with
`-WEIDA_EREJECTED`: a hidden peer's `weida_listener_bind_relay` op, a dialler's connect op. A
request dropped unanswered answers `NO_REPLY`.

## `weida_relay_request_admit_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_relay_request_admit_op_fd(const weida_relay_request_admit_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_relay_request_admit_op_wait(const weida_relay_request_admit_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 once served; `-WEIDA_EALREADY_REGISTERED` (a second registration on one connection), `-WEIDA_EUNKNOWN_ENDPOINT` (a tunnel whose target went away), a connection error when the asker is gone; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_relay_request_admit_op_drop(weida_relay_request_admit_op *op)` | never | — |

Dropped unfinished, an admit op aborts the admission, and the asker may see `-WEIDA_ENO_REPLY`.

## `weida_relay_request_refuse_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_relay_request_refuse_op_fd(const weida_relay_request_refuse_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_relay_request_refuse_op_wait(const weida_relay_request_refuse_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 once the refusal was written, or could not be (the asker is gone); `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_relay_request_refuse_op_drop(weida_relay_request_refuse_op *op)` | never | — |

Dropped unfinished, a refuse op aborts the refusal, and the asker may see `-WEIDA_ENO_REPLY`.
