# Transfers and delivery receipts

A transfer is read and written like a socket (R3a): the read and the write are tries on the
caller's thread that poll weida's stream straight into and out of the caller's buffer. Every
function is callable from any thread (R5); two threads reading one transfer get disjoint pieces
of it in an order they cannot know, so a caller that cares reads from one. Every `int` function
can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## `weida_incoming_transfer`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_incoming_transfer_meta(const weida_incoming_transfer *t, weida_incoming_meta **out)` | never | 0 and a new metadata reference ([msg.md](msg.md)) |
| `int weida_incoming_transfer_fd(const weida_incoming_transfer *t, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_incoming_transfer_read(const weida_incoming_transfer *t, uint8_t *buf, size_t cap, int64_t timeout_ms, size_t *out_n)` | up to `timeout_ms` | 0 and `*out_n > 0` bytes, or `*out_n == 0` at the end of the payload; `-WEIDA_ECANCELED` (the sender canceled), `-WEIDA_EREJECTED`, `-WEIDA_ECONNECTION_LOST`, `-WEIDA_ETRANSPORT`; `-WEIDA_EINVAL` for `cap == 0`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `int weida_incoming_transfer_collect(weida_incoming_transfer *t, uint64_t max_bytes, weida_incoming_transfer_collect_op **out)` | never; **consumes** `t` | 0; `-WEIDA_ECLOSED` |
| `void weida_incoming_transfer_drop(weida_incoming_transfer *t)` | never | — |

A canceled transfer never reads as an end: the read fails with the sender's outcome. Dropping a
transfer whose payload was not read to its end refuses the rest with `REJECTED`.

`weida_incoming_transfer_collect_op`: `_fd` as above; `int
weida_incoming_transfer_collect_op_wait(op, int64_t timeout_ms, weida_msg **out)` returns 0
and the whole payload ([msg.md](msg.md)), `-WEIDA_ELIMIT_EXCEEDED` when it exceeds `max_bytes`
(the rest is refused), or the read failures above; `_drop` aborts an unfinished collect.

## `weida_outgoing_transfer`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_outgoing_transfer_fd(const weida_outgoing_transfer *t, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_outgoing_transfer_write(const weida_outgoing_transfer *t, const uint8_t *buf, size_t len, int64_t timeout_ms, size_t *out_n)` | up to `timeout_ms` for flow control | 0 and `*out_n` bytes taken (at least 1 when `len > 0`); `len == 0` returns 0 at once; `-WEIDA_EREJECTED`, `-WEIDA_ECANCELED`, `-WEIDA_EUNKNOWN_ENDPOINT` (the peer's refusal), `-WEIDA_ECONNECTION_LOST`, `-WEIDA_EEXPIRED`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `int weida_outgoing_transfer_finish(weida_outgoing_transfer *t, weida_delivery **out)` | never; **consumes** `t` | 0 and the receipt; `-WEIDA_EEXPIRED` is its only weida failure |
| `void weida_outgoing_transfer_drop(weida_outgoing_transfer *t)` | never | — |

A write takes what flow control allows; the caller loops for the rest (R3c). An unfinished
transfer dropped is reset with `CANCELED`, which the reader sees as `-WEIDA_ECANCELED`.

## `weida_delivery`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_delivery_delivered(weida_delivery *d, weida_delivery_delivered_op **out)` | never; **consumes** `d` | 0 |
| `int weida_delivery_delivered_op_fd(const weida_delivery_delivered_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_delivery_delivered_op_wait(const weida_delivery_delivered_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 when the peer's transport acknowledged every byte and the end; `-WEIDA_EREJECTED`, `-WEIDA_ECANCELED`, `-WEIDA_ECONNECTION_LOST`, `-WEIDA_EINDETERMINATE`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_delivery_delivered_op_drop(weida_delivery_delivered_op *op)` | never | — |
| `void weida_delivery_drop(weida_delivery *d)` | never | — |

A receipt dropped without waiting is parked where `Runtime::drain` finds it, as in Rust.
