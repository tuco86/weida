# Listener, bindings, replier, requests

Every function is callable from any thread (R5). Every `int` function can also return
`-WEIDA_EINVAL` (a NULL or a non-UTF-8 string) and `-WEIDA_EPANIC` (R10); the tables list the
rest.

## `weida_listener`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_bind_quic(const weida_listener *l, const char *addr, const weida_server_tls *tls, weida_listener_bind_quic_op **out)` | never (R3b) | 0; `-WEIDA_EINVALID_ADDRESS` when `addr` is not an IP and port |
| `int weida_listener_bind_inproc(const weida_listener *l, const char *bus, weida_local_binding **out)` | never | 0; `-WEIDA_EALREADY_REGISTERED` for a bus in use |
| `int weida_listener_replier(const weida_listener *l, const char *path, weida_replier **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `void weida_listener_drop(weida_listener *l)` | never | — |

## `weida_listener_bind_quic_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_bind_quic_op_fd(const weida_listener_bind_quic_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_listener_bind_quic_op_wait(const weida_listener_bind_quic_op *op, int64_t timeout_ms, weida_binding **out)` | up to `timeout_ms` | 0; `-WEIDA_EIO` (address in use), `-WEIDA_ETLS`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_listener_bind_quic_op_drop(weida_listener_bind_quic_op *op)` | never | — |

## `weida_binding`, `weida_local_binding`

| Function | Returns |
| --- | --- |
| `const char *weida_binding_local_addr(const weida_binding *b, size_t *len_out)` | the bound address with its port, e.g. `"127.0.0.1:40123"`; lives as long as `b` |
| `void weida_binding_drop(weida_binding *b)` | stops serving on the socket |
| `const char *weida_local_binding_bus(const weida_local_binding *b, size_t *len_out)` | the bus name |
| `void weida_local_binding_drop(weida_local_binding *b)` | frees the bus name |

## `weida_replier` (a try handle, R3a)

| Function | Blocks | Returns |
| --- | --- | --- |
| `const char *weida_replier_path(const weida_replier *r, size_t *len_out)` | never | the path |
| `int weida_replier_fd(const weida_replier *r, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_replier_accept(const weida_replier *r, int64_t timeout_ms, weida_incoming_request **out)` | up to `timeout_ms` | 0; `-WEIDA_ENOT_CONNECTED` once the listener's runtime closed the endpoint; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_replier_drop(weida_replier *r)` | never | — |

The accept in flight is kept in the replier between tries: a timeout or `EINTR` loses no
request, and the next call continues it. Readiness follows R11: **try first**. The descriptor
is raised by a wake, and only a pending try registers for one, so a caller polls the descriptor
after an `EAGAIN`, not before its first try. After a successful accept the descriptor is
readable again (there may be more); the next `EAGAIN` clears it.

Dropping the replier unregisters its path and ends every wait on it with `-WEIDA_ECLOSED`.

## `weida_incoming_request`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_incoming_request_meta(const weida_incoming_request *q, weida_incoming_meta **out)` | never | 0 and a new metadata reference ([msg.md](msg.md)); `-WEIDA_ECLOSED` |
| `int weida_incoming_request_take_body(const weida_incoming_request *q, weida_incoming_transfer **out)` | never | 0; `-WEIDA_ECLOSED` on a second call |
| `int weida_incoming_request_reply(weida_incoming_request *q, const weida_transfer_meta *meta_or_null, weida_incoming_request_reply_op **out)` | never; **consumes** `q` | 0; `-WEIDA_ECLOSED` |
| `void weida_incoming_request_drop(weida_incoming_request *q)` | never | — |

A request dropped without a reply answers the requester with `NO_REPLY`. A body not taken
before the reply is refused with `REJECTED`. `IncomingRequest::body` (a lent borrow) is not
mapped (R1); `take_body` is its owning twin.

## `weida_incoming_request_reply_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_incoming_request_reply_op_fd(const weida_incoming_request_reply_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_incoming_request_reply_op_wait(const weida_incoming_request_reply_op *op, int64_t timeout_ms, weida_outgoing_transfer **out)` | up to `timeout_ms` | 0 and the reply stream ([transfer.md](transfer.md)); `-WEIDA_ECONNECTION_LOST`, `-WEIDA_ECANCELED` (the requester gave up); `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_incoming_request_reply_op_drop(weida_incoming_request_reply_op *op)` | never | — |
