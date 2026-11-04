# Flows

A datagram flow is registered once on a stream of its own; its datagrams are then sent without
waiting and received as a queue. Flows need `datagram_receive_bytes` above 0 on both runtimes
(`WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES`, [runtime.md](runtime.md)). Every function is callable
from any thread (R5). Every `int` function can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.
A raw peer opens a flow (`weida_peer_open_flow`) and an acceptor hands it over
(`weida_incoming_take_flow`), both in [peer.md](peer.md).

## `weida_flow_meta`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_flow_meta_new(weida_flow_meta **out)` | never | 0; no key set |
| `int weida_flow_meta_set_content_type(const weida_flow_meta *m, const char *content_type)` | never | 0; above 256 bytes fails the open with `-WEIDA_ELIMIT_EXCEEDED` |
| `int weida_flow_meta_set_topic(const weida_flow_meta *m, const char *topic)` | never | 0; above 256 bytes fails the open with `-WEIDA_ELIMIT_EXCEEDED` |
| `int weida_flow_meta_set_trace(const weida_flow_meta *m, const char *traceparent)` | never | 0; `-WEIDA_EINVAL` for anything but a W3C version-00 traceparent |
| `void weida_flow_meta_drop(weida_flow_meta *m)` | never | — |

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
