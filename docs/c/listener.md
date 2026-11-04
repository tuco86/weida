# Listener, bindings, replier, requests

Every function is callable from any thread (R5). Every `int` function can also return
`-WEIDA_EINVAL` (a NULL or a non-UTF-8 string) and `-WEIDA_EPANIC` (R10); the tables list the
rest.

## `weida_listener`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_bind_quic(const weida_listener *l, const char *addr, const weida_server_tls *tls, weida_listener_bind_quic_op **out)` | never (R3b) | 0; `-WEIDA_EINVALID_ADDRESS` when `addr` is not an IP and port |
| `int weida_listener_bind_quic_socket(const weida_listener *l, int64_t socket, const weida_server_tls *tls, weida_listener_bind_quic_socket_op **out)` | never (R3b); **takes ownership of `socket`** | 0; `-WEIDA_EINVAL` for a negative or out-of-range number (nothing taken) |
| `int weida_listener_bind_relay(const weida_listener *l, const char *relay, const weida_client_tls *via, const weida_server_tls *tls, weida_listener_bind_relay_op **out)` | never (R3b) | 0 ([relay.md](relay.md)) |
| `int weida_listener_relay(const weida_listener *l, weida_relay **out)` | never | 0; `-WEIDA_EALREADY_REGISTERED` ([relay.md](relay.md)) |
| `int weida_listener_bind_inproc(const weida_listener *l, const char *bus, weida_local_binding **out)` | never | 0; `-WEIDA_EALREADY_REGISTERED` for a bus in use |
| `int weida_listener_bind_unix(const weida_listener *l, const char *path, weida_unix_binding **out)` | never | 0; serves the endpoints on the `AF_UNIX` socket `path`, reached at `weida+unix://<percent-encoded path>/<endpoint>`, no TLS, the kernel proves the peer; `path` MUST be in a directory this process owns and no other user may write; a stale socket file is replaced; `-WEIDA_EIO` (a live listener on the path), `-WEIDA_EINVALID_ADDRESS` (over the `sun_path` budget, or not a socket at the path), `-WEIDA_EUNSUPPORTED` off Unix |
| `int weida_listener_bind_pipe(const weida_listener *l, const char *name, weida_pipe_binding **out)` | never | 0; binds `\\.\pipe\<name>` owner-only and local-only, only if the name does not exist yet, reached at `weida+pipe://<name>/<endpoint>`; `-WEIDA_EINVALID_ADDRESS` (an invalid name, or one that exists already), `-WEIDA_EIO`, `-WEIDA_EUNSUPPORTED` off Windows |
| `int weida_listener_replier(const weida_listener *l, const char *path, weida_replier **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `int weida_listener_puller(const weida_listener *l, const char *path, weida_puller **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` ([push.md](push.md)) |
| `int weida_listener_publisher(const weida_listener *l, const char *path, weida_publisher **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` ([pubsub.md](pubsub.md)) |
| `int weida_listener_pair(const weida_listener *l, const char *path, weida_paired **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` ([pair.md](pair.md)) |
| `int weida_listener_respondent(const weida_listener *l, const char *path, weida_respondent **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` ([survey.md](survey.md)) |
| `int weida_listener_bus(const weida_listener *l, const char *path, const weida_client_tls *tls, weida_bus_member **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` ([bus.md](bus.md)) |
| `int weida_listener_radio(const weida_listener *l, const char *path, weida_radio **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED`; the path is released when the radio is dropped ([radio.md](radio.md)) |
| `int weida_listener_acceptor(const weida_listener *l, const char *path, weida_acceptor **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` ([flow.md](flow.md)) |
| `void weida_listener_drop(weida_listener *l)` | never | — |

## `weida_listener_bind_quic_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_bind_quic_op_fd(const weida_listener_bind_quic_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_listener_bind_quic_op_wait(const weida_listener_bind_quic_op *op, int64_t timeout_ms, weida_binding **out)` | up to `timeout_ms` | 0; `-WEIDA_EIO` (address in use), `-WEIDA_ETLS`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_listener_bind_quic_op_drop(weida_listener_bind_quic_op *op)` | never | — |

## `weida_listener_bind_quic_socket_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_bind_quic_socket_op_fd(const weida_listener_bind_quic_socket_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_listener_bind_quic_socket_op_wait(const weida_listener_bind_quic_socket_op *op, int64_t timeout_ms, weida_binding **out)` | up to `timeout_ms` | 0 and the binding; `-WEIDA_EIO` (not a UDP socket), `-WEIDA_ETLS`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_listener_bind_quic_socket_op_drop(weida_listener_bind_quic_socket_op *op)` | never | — |

A socket is its descriptor as `int64_t`: a file descriptor on Unix, a `SOCKET` on Windows.
Ownership passes to libweida at the call; weida neither binds it nor sets options on it.

## `weida_binding`, `weida_local_binding`, `weida_unix_binding`, `weida_pipe_binding`

| Function | Blocks | Returns |
| --- | --- | --- |
| `const char *weida_binding_local_addr(const weida_binding *b, size_t *len_out)` | never | the bound address with its port, e.g. `"127.0.0.1:40123"`; lives as long as `b` |
| `int weida_binding_close(const weida_binding *b, weida_binding_close_op **out)` | never (R3b) | 0; `-WEIDA_ECLOSED` once handed over |
| `int weida_binding_hand_over(weida_binding *b, weida_binding_hand_over_op **out)` | never; **consumes** `b` | 0; `-WEIDA_EUNSUPPORTED` (relayed), `-WEIDA_ECLOSED` (a close was started), `-WEIDA_EINVAL` (a take-over of it is unfinished) |
| `int weida_binding_take_over(const weida_binding *b, const uint8_t *held, size_t len, weida_binding_take_over_op **out)` | never (R3b) | 0; `-WEIDA_ECLOSED` |
| `int weida_binding_disconnect(const weida_binding *b, const char *peer, size_t *out)` | never | 0 and how many connections it closed; `-WEIDA_EINVALID_FINGERPRINT`, `-WEIDA_ECLOSED` |
| `int weida_binding_relay_events(const weida_binding *b, weida_peer_events **out)` | never | 1 and a new reader ([stats.md](stats.md)) for a relayed binding; 0 for any other or once its registration ended; `-WEIDA_ECLOSED` |
| `void weida_binding_drop(weida_binding *b)` | never | releases the handle; the binding keeps serving until `weida_binding_close`, a hand-over or the runtime's shutdown |
| `const char *weida_local_binding_bus(const weida_local_binding *b, size_t *len_out)` | never | the bus name |
| `void weida_local_binding_drop(weida_local_binding *b)` | never | frees the bus name |
| `const char *weida_unix_binding_path(const weida_unix_binding *b, size_t *len_out)` | never | the socket path bound; lives as long as `b` |
| `void weida_unix_binding_drop(weida_unix_binding *b)` | never | ends the binding and removes the socket file, so a new dial finds nobody |
| `const char *weida_pipe_binding_name(const weida_pipe_binding *b, size_t *len_out)` | never | the pipe name without `\\.\pipe\`; lives as long as `b` |
| `void weida_pipe_binding_drop(weida_pipe_binding *b)` | never | ends the accept loop and releases the name |

`weida_binding_close` stops accepting and closes the binding's connections with `SHUTDOWN`. Its
op finishes once the socket went idle, bounded by `shutdown_timeout`, and the port is free to
bind again. A client then sees `WEIDA_LOSS_PEER_CLOSED`, and a new dial finds nobody.
`weida_binding_disconnect` closes one peer's connections with `REJECTED`; it is not a ban, and
the peer redials under its own policy.

`weida_listener_bind_unix` and `weida_listener_bind_pipe` are plain functions (R3:
`Listener::bind_unix` and `bind_pipe` are sync in Rust). Both exist on every platform; the
other platform's answers `-WEIDA_EUNSUPPORTED`. Unlike `weida_binding_drop`, dropping a
`weida_unix_binding` or `weida_pipe_binding` ends the binding, as dropping the Rust value does.

A hand-over (decision 0039) closes every connection with `RESTARTING` but holds the close
packets in the `weida_handoff` its op yields. The successor process serves the socket with
`weida_listener_bind_quic_socket`, registers its endpoints, then calls
`weida_binding_take_over` with `weida_handoff_bytes`. Only then do the clients see
`WEIDA_LOSS_PEER_RESTARTING` and redial at once into the successor. The bytes need the
integrity of the channel that carries the descriptor: whoever can write them decides what the
successor sends from its port.

## `weida_binding_close_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_binding_close_op_fd(const weida_binding_close_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_binding_close_op_wait(const weida_binding_close_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 once the port is free; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED`, and `-WEIDA_ERUNTIME` once the runtime shut down |
| `void weida_binding_close_op_drop(weida_binding_close_op *op)` | never | — |

Dropped unfinished, a close op aborts the close where it is: the connections may already be
closed, and the port is then freed only by the runtime's shutdown.

## `weida_binding_hand_over_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_binding_hand_over_op_fd(const weida_binding_hand_over_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_binding_hand_over_op_wait(const weida_binding_hand_over_op *op, int64_t timeout_ms, weida_handoff **out)` | up to `timeout_ms` | 0 and the handoff; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED`, and `-WEIDA_ERUNTIME` once the runtime shut down |
| `void weida_binding_hand_over_op_drop(weida_binding_hand_over_op *op)` | never | — |

## `weida_binding_take_over_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_binding_take_over_op_fd(const weida_binding_take_over_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_binding_take_over_op_wait(const weida_binding_take_over_op *op, int64_t timeout_ms, size_t *out)` | up to `timeout_ms` | 0 and how many datagrams were sent; `-WEIDA_EIO` for bytes that are not a handoff or are another address's; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_binding_take_over_op_drop(weida_binding_take_over_op *op)` | never | — |

## `weida_handoff`

| Function | Returns |
| --- | --- |
| `size_t weida_handoff_held(const weida_handoff *h)` | the close packets held; 0 for NULL |
| `const char *weida_handoff_local_addr(const weida_handoff *h, size_t *len_out)` | the socket's address; lives as long as `h` |
| `const uint8_t *weida_handoff_bytes(const weida_handoff *h, size_t *len_out)` | the held packets for `weida_binding_take_over`; lives as long as `h` |
| `int weida_handoff_take_socket(const weida_handoff *h, int64_t *out)` | 0 and the descriptor, the caller's from then on; `-WEIDA_ECLOSED` on a second call |
| `void weida_handoff_drop(weida_handoff *h)` | closes a socket not taken |

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
| `int weida_incoming_request_refuse(weida_incoming_request *q, int code, weida_incoming_request_refuse_op **out)` | never; **consumes** `q` | 0; `-WEIDA_EINVAL` for a `code` that is not a `WEIDA_ERROR_CODE_*` (the request is consumed all the same and answers `NO_REPLY`); `-WEIDA_ECLOSED` |
| `int weida_incoming_request_canceled(const weida_incoming_request *q, weida_incoming_request_canceled_op **out)` | never | 0; `-WEIDA_ECLOSED` |
| `int weida_incoming_request_connection(const weida_incoming_request *q, uint64_t *out)` | never | 0 and the number of its connection (`ConnectionId`): equal for one connection, never shared by two of this process, not an address; `-WEIDA_ECLOSED` |
| `void weida_incoming_request_drop(weida_incoming_request *q)` | never | — |

A request dropped without a reply answers the requester with `NO_REPLY`. A body not taken
before the reply is refused with `REJECTED`. `IncomingRequest::body` (a lent borrow) is not
mapped (R1); `take_body` is its owning twin.

A refusal answers the requester with an ERROR instead of a reply; its op ends with the code of
[README](README.md) R9's `ErrorCode` table. A body still attached is stopped with `REJECTED`.
`canceled` maps a sync method returning a future, so it is an op too (R3b). It is independent
of the request: start it before `reply` or `refuse` consumes `q`. It settles when any of these
happened: the requester dropped its request op or its reply; a finished reply was read whole;
the connection is gone.

## `weida_incoming_request_reply_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_incoming_request_reply_op_fd(const weida_incoming_request_reply_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_incoming_request_reply_op_wait(const weida_incoming_request_reply_op *op, int64_t timeout_ms, weida_outgoing_transfer **out)` | up to `timeout_ms` | 0 and the reply stream ([transfer.md](transfer.md)); `-WEIDA_ECONNECTION_LOST`, `-WEIDA_ECANCELED` (the requester gave up); `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_incoming_request_reply_op_drop(weida_incoming_request_reply_op *op)` | never | — |

## `weida_incoming_request_refuse_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_incoming_request_refuse_op_fd(const weida_incoming_request_refuse_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_incoming_request_refuse_op_wait(const weida_incoming_request_refuse_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 once the refusal was written, or could not be (the connection is gone); `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED`, and `-WEIDA_ERUNTIME` once the runtime shut down |
| `void weida_incoming_request_refuse_op_drop(weida_incoming_request_refuse_op *op)` | never | — |

Dropped before its wait returned 0, a refuse op aborts the refusal, and the requester may see
`-WEIDA_ENO_REPLY`.

## `weida_incoming_request_canceled_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_incoming_request_canceled_op_fd(const weida_incoming_request_canceled_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_incoming_request_canceled_op_wait(const weida_incoming_request_canceled_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 once the requester gave up; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED`, and `-WEIDA_ERUNTIME` once the runtime shut down |
| `void weida_incoming_request_canceled_op_drop(weida_incoming_request_canceled_op *op)` | never | — |

Dropping a canceled op ends the watch.
