# PAIR

A pair has exactly one peer and sends one-way transfers to it in both directions. A dialling
pair (`weida_runtime_pair`) dials its peer; a bound pair (`weida_listener_pair`) serves one
endpoint path of a listener and is dialled. Every function is callable from any thread (R5).
Every `int` function can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## `weida_paired`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_pair(const weida_runtime *rt, const weida_client_tls *tls, weida_paired **out)` | never | 0, a dialling pair |
| `int weida_listener_pair(const weida_listener *l, const char *path, weida_paired **out)` | never | 0, a bound pair; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `const char *weida_paired_path(const weida_paired *p, size_t *len_out)` | never | the bound pair's path; the empty string on a dialling pair |
| `int weida_paired_connect(const weida_paired *p, const char *url, weida_paired_connect_op **out)` | never (R3b) | 0; the URL is parsed by the op |
| `int weida_paired_connect_op_fd(const weida_paired_connect_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_paired_connect_op_wait(const weida_paired_connect_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 when connected; `-WEIDA_EUNSUPPORTED` on a bound pair, `-WEIDA_ELIMIT_EXCEEDED` once an address is dialled; `-WEIDA_EINVALID_ADDRESS`, `-WEIDA_EUNTRUSTED` (with `weida_last_error_fingerprint`), `-WEIDA_ETLS`, `-WEIDA_ENEGOTIATION`, `-WEIDA_EIO`, `-WEIDA_ECONNECTION_LOST`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_paired_connect_op_drop(weida_paired_connect_op *op)` | never | — |
| `size_t weida_paired_peer_count(const weida_paired *p)` | never | the connected peers, 0 or 1; 0 on a bound pair and for NULL |
| `uint64_t weida_paired_dropped(const weida_paired *p)` | never | the bodies a dialling pair's outbox discarded; 0 on a bound pair and for NULL |
| `int weida_paired_connection_stats(const weida_paired *p, weida_connection_stats **out)` | never; a snapshot | 0 ([stats.md](stats.md)); no records on a bound pair |
| `int weida_paired_events(const weida_paired *p, weida_peer_events **out)` | never | 1 and a new reader ([stats.md](stats.md)); 0 on a bound pair, `out` untouched |
| `int weida_paired_open(const weida_paired *p, const weida_transfer_meta *meta, weida_paired_open_op **out)` | never; `meta` is copied | 0. NULL `meta` is `TransferMeta::default()` |
| `int weida_paired_open_op_fd(const weida_paired_open_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_paired_open_op_wait(const weida_paired_open_op *op, int64_t timeout_ms, weida_outgoing_transfer **out)` | up to `timeout_ms` | 0 and the transfer ([transfer.md](transfer.md)); `-WEIDA_ENOT_CONNECTED`, `-WEIDA_ECONNECTION_LOST`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_paired_open_op_drop(weida_paired_open_op *op)` | never | — |
| `int weida_paired_send(const weida_paired *p, const uint8_t *body, size_t len, weida_paired_send_op **out)` | never; the body is copied | 0 |
| `int weida_paired_send_with(const weida_paired *p, const weida_transfer_meta *meta, const uint8_t *body, size_t len, weida_paired_send_op **out)` | never; `meta` and the body are copied | 0. `weida_paired_send` with metadata ([tls.md](tls.md)); NULL `meta` is `TransferMeta::default()` |
| `int weida_paired_send_op_fd(const weida_paired_send_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_paired_send_op_wait(const weida_paired_send_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 once the body is written or held in the outbox; `-WEIDA_ELIMIT_EXCEEDED` (a full outbox under `OutboxFull::Reject`), `-WEIDA_ENOT_CONNECTED`, `-WEIDA_ECONNECTION_LOST`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_paired_send_op_drop(weida_paired_send_op *op)` | never | — |
| `int weida_paired_fd(const weida_paired *p, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_paired_recv(const weida_paired *p, int64_t timeout_ms, weida_incoming_transfer **out)` | up to `timeout_ms` | 0 and the transfer as soon as its header arrives; `-WEIDA_ENOT_CONNECTED` once the queue ended; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_paired_drop(weida_paired *p)` | never | — |

A pair has one peer. A dialling pair's path is empty, and its second dial is
`-WEIDA_ELIMIT_EXCEEDED` whether the first address is live or being redialled: two peers on
one endpoint would need a selection policy, and PAIR has none. A bound pair dials nothing: its
connect is `-WEIDA_EUNSUPPORTED`, its events return 0 and its connection stats are empty.

The two sides reach their peer differently. A dialling pair sends on the connection it opened,
written at once while the peer is live and held in the outbox otherwise, as a pusher's
([push.md](push.md)). A bound pair sends on the connection its peer dialled, so its first send
or open waits until the peer has sent first; nothing is buffered meanwhile. A send discards the
delivery receipt; `weida_paired_open` hands it back through `weida_outgoing_transfer_finish`.

`weida_paired_recv` is a try (R3a): the take in flight is kept in the pair between tries, so a
timeout or `EINTR` loses no transfer. Readiness follows R11: **try first**. Dropping the pair
ends a waiting `weida_paired_recv` with `-WEIDA_ECLOSED`; a bound pair's path stays registered,
as a dropped Rust `Paired` leaves it, and a dialling pair's route goes with the last op holding
the pair.
