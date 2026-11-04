# BUS

A bus member serves one endpoint path of a listener and dials the other members; each message
it sends goes to every member it dialled. Every function is callable from any thread (R5).
Every `int` function can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## `weida_bus_member`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_bus(const weida_listener *l, const char *path, const weida_client_tls *tls, weida_bus_member **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `const char *weida_bus_member_path(const weida_bus_member *m, size_t *len_out)` | never | the path |
| `int weida_bus_member_connect(const weida_bus_member *m, const char *url, weida_bus_member_connect_op **out)` | never (R3b) | 0; the URL is parsed by the op |
| `int weida_bus_member_connect_op_fd(const weida_bus_member_connect_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_bus_member_connect_op_wait(const weida_bus_member_connect_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 when connected; `-WEIDA_EINVALID_ADDRESS`, `-WEIDA_EUNTRUSTED` (with `weida_last_error_fingerprint`), `-WEIDA_ETLS`, `-WEIDA_ENEGOTIATION`, `-WEIDA_EIO`, `-WEIDA_ECONNECTION_LOST`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_bus_member_connect_op_drop(weida_bus_member_connect_op *op)` | never | — |
| `size_t weida_bus_member_peer_count(const weida_bus_member *m)` | never | the members this one joined; 0 for NULL |
| `int weida_bus_member_connection_stats(const weida_bus_member *m, weida_connection_stats **out)` | never; a snapshot | 0 ([stats.md](stats.md)) |
| `uint64_t weida_bus_member_dropped(const weida_bus_member *m)` | never | the copies that never reached a member; 0 for NULL |
| `int weida_bus_member_send(const weida_bus_member *m, const uint8_t *body, size_t len, weida_bus_member_send_op **out)` | never; the body is copied | 0 |
| `int weida_bus_member_send_with(const weida_bus_member *m, const weida_transfer_meta *meta, const uint8_t *body, size_t len, weida_bus_member_send_op **out)` | never; `meta` and the body are copied | 0. `weida_bus_member_send` with metadata ([tls.md](tls.md)); NULL `meta` is `TransferMeta::default()` |
| `int weida_bus_member_send_op_fd(const weida_bus_member_send_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_bus_member_send_op_wait(const weida_bus_member_send_op *op, int64_t timeout_ms, size_t *out)` | up to `timeout_ms` | 0 and how many members the message reached; `-WEIDA_ELIMIT_EXCEEDED` for a body above `Limits::subscriber_buffer_bytes`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_bus_member_send_op_drop(weida_bus_member_send_op *op)` | never | — |
| `int weida_bus_member_fd(const weida_bus_member *m, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_bus_member_recv(const weida_bus_member *m, int64_t timeout_ms, weida_incoming_transfer **out)` | up to `timeout_ms` | 0 and the transfer as soon as its header arrives; `-WEIDA_ENOT_CONNECTED` once the queue ended; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_bus_member_drop(weida_bus_member *m)` | never | — |

A member is bound and dialling at once: joining another member is an ordinary connect, and
there is no membership protocol. A send goes to every member this one dialled and never to
itself, and nothing is relayed: a bus of *n* members needs every member to dial the *n* − 1
others. Delivery is best effort per member: "reached" counts the members a copy was queued for,
and a copy a member cannot take, its writer queue full in messages or bytes, is lost and
counted in `weida_bus_member_dropped` rather than failing the send.

`weida_bus_member_recv` is a try (R3a): the take in flight is kept in the member between tries,
so a timeout or `EINTR` loses no transfer. Readiness follows R11: **try first**. Dropping the
member ends a waiting `weida_bus_member_recv` with `-WEIDA_ECLOSED` and drops its connections;
the path stays registered, as a dropped Rust `BusMember` leaves it.
