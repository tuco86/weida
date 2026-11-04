# Flows

A datagram flow is registered once on a stream of its own; its datagrams are then sent without
waiting and received as a queue. Flows need `datagram_receive_bytes` above 0 on both runtimes
(`WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES`, [runtime.md](runtime.md)). Every function is callable
from any thread (R5). Every `int` function can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## `weida_peer`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_peer(const weida_runtime *rt, const weida_client_tls *tls, weida_peer **out)` | never | 0 |
| `int weida_peer_connect(const weida_peer *p, const char *url, weida_peer_connect_op **out)` | never (R3b) | 0; the wait returns 0 when connected, `-WEIDA_EUNTRUSTED` for a key not trusted; the runtime redials a lost address |
| `int weida_peer_open_flow(const weida_peer *p, const weida_flow_meta *meta, weida_peer_open_flow_op **out)` | never (R3b); `meta` is copied, NULL is no keys | 0; registers a flow to the next peer, round-robin |
| `int weida_peer_open_flow_op_wait(const weida_peer_open_flow_op *op, int64_t timeout_ms, weida_flow **out)` | up to `timeout_ms` | 0 and the flow once its header is written; `-WEIDA_EDATAGRAMS_UNAVAILABLE` when the connection did not agree datagrams, `-WEIDA_ENOT_CONNECTED` for a peer never connected, `-WEIDA_ELIMIT_EXCEEDED` for a label above 256 bytes; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_peer_drop(weida_peer *p)` | never | — |

Each op type, `weida_peer_connect_op` and `weida_peer_open_flow_op`, has `_op_fd(op, int64_t
*out)` and `_op_drop(op)`; `weida_peer_connect_op_wait(op, int64_t timeout_ms)` returns 0 when
done, `-WEIDA_EAGAIN`, `-WEIDA_EINTR` or `-WEIDA_ECLOSED`. Dropping an unfinished op aborts it;
a flow an open registered that no wait took is closed.

## `weida_flow_meta`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_flow_meta_new(weida_flow_meta **out)` | never | 0; no key set |
| `int weida_flow_meta_set_content_type(const weida_flow_meta *m, const char *content_type)` | never | 0; above 256 bytes fails the open with `-WEIDA_ELIMIT_EXCEEDED` |
| `int weida_flow_meta_set_topic(const weida_flow_meta *m, const char *topic)` | never | 0; above 256 bytes fails the open with `-WEIDA_ELIMIT_EXCEEDED` |
| `int weida_flow_meta_set_trace(const weida_flow_meta *m, const char *traceparent)` | never | 0; `-WEIDA_EINVAL` for anything but a W3C version-00 traceparent |
| `void weida_flow_meta_drop(weida_flow_meta *m)` | never | — |

## `weida_acceptor` and `weida_incoming`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_acceptor(const weida_listener *l, const char *path, weida_acceptor **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `const char *weida_acceptor_path(const weida_acceptor *a, size_t *len_out)` | never | the path, valid as long as `a`; NULL for NULL |
| `int weida_acceptor_fd(const weida_acceptor *a, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_acceptor_accept(const weida_acceptor *a, int64_t timeout_ms, weida_incoming **out)` | up to `timeout_ms` | 0 and what arrived next, tagged; `-WEIDA_ENOT_CONNECTED` once the queue ended; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_acceptor_drop(weida_acceptor *a)` | never | —; the path stays registered, a waiting accept returns `-WEIDA_ECLOSED` |
| `int weida_incoming_kind(const weida_incoming *i)` | never | a `WEIDA_INCOMING_*` constant |
| `int weida_incoming_take_flow(const weida_incoming *i, weida_incoming_flow **out)` | never | 0 and the flow, once; `-WEIDA_ECLOSED` after that, `-WEIDA_EINVAL` for another kind |
| `void weida_incoming_drop(weida_incoming *i)` | never | —; drops what was not taken: a stream is refused `REJECTED`, an exchange answers `NO_REPLY`, a flow is stopped `CANCELED` |

| Constant | Value |
| --- | --- |
| `WEIDA_INCOMING_STREAM` | 0, a unidirectional stream |
| `WEIDA_INCOMING_EXCHANGE` | 1, a bidirectional stream that owes a reply |
| `WEIDA_INCOMING_SUBSCRIBED` | 2, a consumer subscribed with one filter |
| `WEIDA_INCOMING_CREDIT` | 3, a consumer stated a subscription's delivery limit |
| `WEIDA_INCOMING_UNSUBSCRIBED` | 4, a subscription ended |
| `WEIDA_INCOMING_FLOW` | 5, a datagram flow |

`weida_acceptor_accept` is a try (R3a): the accept in flight is kept between tries, and
`timeout_ms` 0 is the non-blocking read.

## `weida_flow`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_flow_send(const weida_flow *f, const uint8_t *payload, size_t len)` | never; the payload is copied | 0 once handed to the connection, not arrived; `-WEIDA_ETOO_LARGE` above the largest payload (`weida_last_error_max` names it), `-WEIDA_EDATAGRAMS_UNAVAILABLE`; on an ended flow the error that ended it: `-WEIDA_EREJECTED`, `-WEIDA_EUNKNOWN_ENDPOINT`, `-WEIDA_EUNSUPPORTED`, `-WEIDA_ELIMIT_EXCEEDED`, `-WEIDA_ECANCELED`, `-WEIDA_ECONNECTION_LOST`, `-WEIDA_ENOT_CONNECTED`; `-WEIDA_ECLOSED` after an abort |
| `int weida_flow_max_payload(const weida_flow *f, size_t *out)` | never | 1 and the largest payload a send accepts now; 0 when the connection carries no datagrams, for a NULL argument and after an abort |
| `uint64_t weida_flow_sent(const weida_flow *f)` | never | datagrams handed to the transport; 0 for NULL |
| `uint64_t weida_flow_too_large(const weida_flow *f)` | never | sends refused with `-WEIDA_ETOO_LARGE`; 0 for NULL |
| `uint64_t weida_flow_discarded(const weida_flow *f)` | never | queued datagrams discarded oldest-first, local transports only; 0 for NULL |
| `uint64_t weida_flow_not_live(const weida_flow *f)` | never | datagrams dropped while no connection was live; 0 for NULL |
| `int weida_flow_path_stats(const weida_flow *f, weida_path_stats **out)` | never | 1 and a snapshot of the path; 0 with NULL on a local transport; `-WEIDA_ECLOSED` after an abort |
| `void weida_flow_abort(weida_flow *f)` | never; **consumes** `f` | —; resets the flow `CANCELED` |
| `void weida_flow_drop(weida_flow *f)` | never | —; closes the flow with FIN |

## `weida_incoming_flow`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_incoming_flow_info(const weida_incoming_flow *f, weida_incoming_meta **out)` | never | 0; the endpoint, topic, content type, trace, peer and chain, read with the `weida_incoming_meta_*` getters ([msg.md](msg.md)); every other getter reads absent |
| `int weida_incoming_flow_fd(const weida_incoming_flow *f, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_incoming_flow_recv(const weida_incoming_flow *f, int64_t timeout_ms, weida_datagram **out)` | up to `timeout_ms` | 0 and the next datagram, oldest first; 0 with NULL once the flow ended and every datagram that arrived before its end was taken, and on every recv after that; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `uint64_t weida_incoming_flow_received(const weida_incoming_flow *f)` | never | datagrams that arrived; 0 for NULL |
| `uint64_t weida_incoming_flow_overflow(const weida_incoming_flow *f)` | never | arrived datagrams dropped oldest-first because `flow_queue_bytes` were unread; 0 for NULL |
| `int weida_incoming_flow_path_stats(const weida_incoming_flow *f, weida_path_stats **out)` | never | 1 and a snapshot of the path; 0 with NULL on a local transport |
| `void weida_incoming_flow_refuse(weida_incoming_flow *f)` | never; **consumes** `f` | —; stops the flow `REJECTED`, which the sender's next send reports as `-WEIDA_EREJECTED`; a waiting recv returns `-WEIDA_ECLOSED` |
| `void weida_incoming_flow_drop(weida_incoming_flow *f)` | never | —; stops the flow `CANCELED`, which the sender's next send reports as `-WEIDA_ECANCELED` |

`weida_incoming_flow_recv` is a try (R3a): the recv in flight is kept between tries, and
`timeout_ms` 0 is the non-blocking read. It tells "nothing yet" (`-WEIDA_EAGAIN`) from the end
(NULL).

## `weida_datagram`

| Function | Returns |
| --- | --- |
| `const uint8_t *weida_datagram_data(const weida_datagram *d)` | the bytes, valid as long as `d`; NULL for NULL |
| `size_t weida_datagram_len(const weida_datagram *d)` | the length; 0 for NULL |
| `void weida_datagram_drop(weida_datagram *d)` | — |

## `weida_path_stats`

One snapshot of the path under a flow, read-only. Each `uint64_t` getter returns 0 for NULL.

| Function | Returns |
| --- | --- |
| `uint64_t weida_path_stats_rtt_us(const weida_path_stats *s)` | the smoothed round-trip time, in microseconds |
| `uint64_t weida_path_stats_min_rtt_us(const weida_path_stats *s)` | the smallest round-trip time seen, in microseconds |
| `uint64_t weida_path_stats_cwnd(const weida_path_stats *s)` | the congestion window, in bytes |
| `uint64_t weida_path_stats_congestion_events(const weida_path_stats *s)` | congestion events the controller reacted to |
| `uint64_t weida_path_stats_lost_packets(const weida_path_stats *s)` | packets sent and declared lost |
| `uint64_t weida_path_stats_lost_bytes(const weida_path_stats *s)` | bytes sent and declared lost |
| `uint64_t weida_path_stats_sent_packets(const weida_path_stats *s)` | packets sent |
| `uint64_t weida_path_stats_current_mtu(const weida_path_stats *s)` | the largest UDP payload the path carries now |
| `int weida_path_stats_max_datagram_size(const weida_path_stats *s, uint64_t *out)` | 1 and the largest datagram the connection carries now, the flow's prefix included; 0 when the peer accepts none and for a NULL argument |
| `void weida_path_stats_drop(weida_path_stats *s)` | — |

## Rules

- Late is lost: a send never waits and never retries, and a datagram that cannot go now is
  dropped and counted.
- An open does not wait for an answer, so a refusal or a missing path arrives as the error of a
  later send.
- A flow through a peer belongs to the dialled address: after a loss it is registered again on
  the redialled connection, and sends in between count `not_live`.
- The queue's end comes after the datagrams that arrived before it.
- On a local transport the datagrams travel on the flow's stream, `weida_flow_max_payload` is
  1200 and there is no path.
- `weida_flow_drop` closes with FIN and `weida_flow_abort` resets; dropping an incoming flow
  stops it `CANCELED` and `weida_incoming_flow_refuse` stops it `REJECTED`.
- The rest of `weida_peer`, `weida_acceptor` and `weida_incoming` is the *raw peers* area
  ([libraries/weida-c.md](../libraries/weida-c.md)); until it lands, the kinds other than a flow
  can only be dropped.
