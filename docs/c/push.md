# Push/Pull

A pusher dials pullers and sends each message to the next one, round-robin; a puller serves one
endpoint path of a listener and takes the transfers pushed to it. Every function is callable
from any thread (R5). Every `int` function can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## `weida_pusher`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_pusher(const weida_runtime *rt, const weida_client_tls *tls, weida_pusher **out)` | never | 0 |
| `int weida_pusher_connect(const weida_pusher *p, const char *url, weida_pusher_connect_op **out)` | never (R3b) | 0; the URL is parsed by the op |
| `int weida_pusher_connect_op_fd(const weida_pusher_connect_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_pusher_connect_op_wait(const weida_pusher_connect_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 when connected; `-WEIDA_EINVALID_ADDRESS`, `-WEIDA_EUNTRUSTED` (with `weida_last_error_fingerprint`), `-WEIDA_ETLS`, `-WEIDA_ENEGOTIATION`, `-WEIDA_EIO`, `-WEIDA_ECONNECTION_LOST`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_pusher_connect_op_drop(weida_pusher_connect_op *op)` | never | — |
| `int weida_pusher_disconnect(const weida_pusher *p, const char *url)` | never | 1 when `url` was dialled and is now forgotten, its connection closed and no longer redialled; 0 when it was not |
| `size_t weida_pusher_peer_count(const weida_pusher *p)` | never | the connected peers; 0 for NULL |
| `uint64_t weida_pusher_dropped(const weida_pusher *p)` | never | the bodies the outbox discarded; 0 for NULL |
| `int weida_pusher_connection_stats(const weida_pusher *p, weida_connection_stats **out)` | never; a snapshot | 0 ([stats.md](stats.md)) |
| `int weida_pusher_events(const weida_pusher *p, weida_peer_events **out)` | never | 0, a new reader ([stats.md](stats.md)) |
| `int weida_pusher_open(const weida_pusher *p, const weida_transfer_meta *meta, weida_pusher_open_op **out)` | never; `meta` is copied | 0. NULL `meta` is `TransferMeta::default()` |
| `int weida_pusher_open_op_fd(const weida_pusher_open_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_pusher_open_op_wait(const weida_pusher_open_op *op, int64_t timeout_ms, weida_outgoing_transfer **out)` | up to `timeout_ms` | 0 and the transfer ([transfer.md](transfer.md)); `-WEIDA_ENOT_CONNECTED`, `-WEIDA_ECONNECTION_LOST`, `-WEIDA_ELIMIT_EXCEEDED` (a topic over 256 bytes); `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_pusher_open_op_drop(weida_pusher_open_op *op)` | never | — |
| `int weida_pusher_send(const weida_pusher *p, const uint8_t *body, size_t len, weida_pusher_send_op **out)` | never; the body is copied | 0 |
| `int weida_pusher_send_with(const weida_pusher *p, const weida_transfer_meta *meta, const uint8_t *body, size_t len, weida_pusher_send_op **out)` | never; `meta` and the body are copied | 0. `weida_pusher_send` with metadata ([tls.md](tls.md)); NULL `meta` is `TransferMeta::default()` |
| `int weida_pusher_send_op_fd(const weida_pusher_send_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_pusher_send_op_wait(const weida_pusher_send_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 once the body is written or held in the outbox, or dropped by a full outbox under `OutboxFull::Drop`; `-WEIDA_ELIMIT_EXCEEDED` (a full outbox under `OutboxFull::Reject`, or a topic over 256 bytes), `-WEIDA_ECONNECTION_LOST`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_pusher_send_op_drop(weida_pusher_send_op *op)` | never | — |
| `void weida_pusher_drop(weida_pusher *p)` | never | — |

A connected address is redialled by the runtime when its connection is lost. A send owns its
body from the call: with a live peer it is written at once, with none it waits in the outbox
and is written once a peer is back, in order, and a full outbox applies
`RuntimeConfig::outbox_full` (the send waits under `Block`). The send discards the delivery
receipt, so it costs no round trip; a caller that wants the receipt opens the transfer with
`weida_pusher_open`, and its `weida_outgoing_transfer_finish` hands back a `weida_delivery`.
With every peer down an open waits for the redial. Dropping an open op whose transfer no wait
took resets that transfer `CANCELED`.

## `weida_puller`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_puller(const weida_listener *l, const char *path, weida_puller **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `const char *weida_puller_path(const weida_puller *p, size_t *len_out)` | never | the path |
| `int weida_puller_fd(const weida_puller *p, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_puller_recv(const weida_puller *p, int64_t timeout_ms, weida_incoming_transfer **out)` | up to `timeout_ms` | 0 and the transfer as soon as its header arrives; `-WEIDA_ENOT_CONNECTED` once the listener's runtime closed the endpoint; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_puller_drop(weida_puller *p)` | never | — |

`weida_puller_recv` is a try (R3a): the take in flight is kept in the puller between tries, so
a timeout or `EINTR` loses no transfer. Readiness follows R11: **try first**. The queue is
bounded by `Limits::endpoint_queue`; a full queue stalls the pushers through flow control.
Dropping the puller ends a waiting `weida_puller_recv` with `-WEIDA_ECLOSED`; the path stays
registered, as a dropped Rust `Puller` leaves it.
