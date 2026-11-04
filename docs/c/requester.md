# Requester

Every function is callable from any thread (R5), and one requester carries any number of
exchanges at once. Every `int` function can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_requester_connect(const weida_requester *r, const char *url, weida_requester_connect_op **out)` | never (R3b) | 0; the URL is parsed by the op |
| `int weida_requester_connect_op_fd(const weida_requester_connect_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_requester_connect_op_wait(const weida_requester_connect_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 when connected; `-WEIDA_EINVALID_ADDRESS`, `-WEIDA_EUNTRUSTED` (with `weida_last_error_fingerprint`), `-WEIDA_ETLS`, `-WEIDA_ENEGOTIATION`, `-WEIDA_EIO`, `-WEIDA_ECONNECTION_LOST`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_requester_connect_op_drop(weida_requester_connect_op *op)` | never | — |
| `int weida_requester_request(const weida_requester *r, const uint8_t *body, size_t len, weida_requester_request_op **out)` | never; the body is copied | 0 |
| `int weida_requester_request_with(const weida_requester *r, const weida_transfer_meta *meta, const uint8_t *body, size_t len, weida_requester_request_op **out)` | never; `meta` and the body are copied | 0. `weida_requester_request` with metadata ([tls.md](tls.md)); NULL `meta` is `TransferMeta::default()`. The op is a request op as above |
| `int weida_requester_request_op_fd(const weida_requester_request_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_requester_request_op_wait(const weida_requester_request_op *op, int64_t timeout_ms, weida_incoming_transfer **out)` | up to `timeout_ms` | 0 and the reply as an incoming transfer, handed over as soon as its header arrives; `-WEIDA_ENOT_CONNECTED`, `-WEIDA_EUNKNOWN_ENDPOINT`, `-WEIDA_EREJECTED`, `-WEIDA_ENO_REPLY`, `-WEIDA_ECONNECTION_LOST`, `-WEIDA_EINDETERMINATE`, `-WEIDA_EUNTRUSTED`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_requester_request_op_drop(weida_requester_request_op *op)` | never | — |
| `int weida_requester_open(const weida_requester *r, const weida_transfer_meta *meta, weida_requester_open_op **out)` | never; `meta` is copied | 0. NULL `meta` is `TransferMeta::default()` |
| `int weida_requester_open_op_fd(const weida_requester_open_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_requester_open_op_wait(const weida_requester_open_op *op, int64_t timeout_ms, weida_outgoing_transfer **out_body, weida_reply_stream **out_reply)` | up to `timeout_ms` | 0, the request body ([transfer.md](transfer.md)) and the reply stream, both written; `-WEIDA_ENOT_CONNECTED`, `-WEIDA_ECONNECTION_LOST`, `-WEIDA_ELIMIT_EXCEEDED` (a topic over 256 bytes), `-WEIDA_EUNTRUSTED`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_requester_open_op_drop(weida_requester_open_op *op)` | never | — |
| `void weida_requester_drop(weida_requester *r)` | never | — |

A connected address is redialled by the runtime when its connection is lost. Dropping an
unfinished request op abandons the exchange: its task is aborted, and the replier's reply, if
one comes, finds nobody reading.

`weida_requester_open` opens one exchange whose request body is written like a socket and ended
with `weida_outgoing_transfer_finish`, and whose reply is awaited on its own handle. The halves
are independent: the reply can be awaited and read while the body is still open. A replier that
refuses the request may fail a body write with the refusal's error before the reply's wait
reports it. Dropping an open op whose result no wait took abandons the exchange.

## `weida_reply_stream`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_reply_stream_recv(weida_reply_stream *s, weida_reply_stream_recv_op **out)` | never; **consumes** `s` | 0; `-WEIDA_ECLOSED` |
| `void weida_reply_stream_drop(weida_reply_stream *s)` | never | — |

A reply stream dropped before `weida_reply_stream_recv` stops the reply half with `CANCELED`:
the replier's `weida_incoming_request_canceled` op settles, and its reply fails with
`-WEIDA_ECANCELED`.

## `weida_reply_stream_recv_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_reply_stream_recv_op_fd(const weida_reply_stream_recv_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_reply_stream_recv_op_wait(const weida_reply_stream_recv_op *op, int64_t timeout_ms, weida_incoming_transfer **out)` | up to `timeout_ms` | 0 and the reply as an incoming transfer, handed over as soon as its header arrives; `-WEIDA_EUNKNOWN_ENDPOINT`, `-WEIDA_EREJECTED`, `-WEIDA_EUNSUPPORTED`, `-WEIDA_ENO_REPLY`, `-WEIDA_ETRANSPORT` (README R9's `ErrorCode` table), `-WEIDA_EINDETERMINATE` (the connection was lost while waiting), `-WEIDA_EPROTOCOL`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_reply_stream_recv_op_drop(weida_reply_stream_recv_op *op)` | never | — |

Dropped unfinished, a recv op aborts the wait, and the replier sees the requester gone.
