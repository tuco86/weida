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
| `int weida_requester_request_op_fd(const weida_requester_request_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_requester_request_op_wait(const weida_requester_request_op *op, int64_t timeout_ms, weida_incoming_transfer **out)` | up to `timeout_ms` | 0 and the reply as an incoming transfer, handed over as soon as its header arrives; `-WEIDA_ENOT_CONNECTED`, `-WEIDA_EUNKNOWN_ENDPOINT`, `-WEIDA_EREJECTED`, `-WEIDA_ENO_REPLY`, `-WEIDA_ECONNECTION_LOST`, `-WEIDA_EINDETERMINATE`, `-WEIDA_EUNTRUSTED`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_requester_request_op_drop(weida_requester_request_op *op)` | never | — |
| `void weida_requester_drop(weida_requester *r)` | never | — |

A connected address is redialled by the runtime when its connection is lost. Dropping an
unfinished request op abandons the exchange: its task is aborted, and the replier's reply, if
one comes, finds nobody reading.

`Requester::open` (a request body written as a stream, with `ReplyStream`) is not mapped yet
([0045](../decisions/0045-the-c-abi-is-the-boundary.md) §5, *streams*).
