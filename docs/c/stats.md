# Connection statistics and peer events

The *stats* area (0045 §5): what a requester knows about its connections, read as values, and the
transitions of its dialled addresses, read from a queue (decision 10). Every function is callable
from any thread (R5); every `int` function can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.
Durations are microseconds (R7).

## `weida_connection_stats`

One snapshot of `weida_requester_connection_stats` ([requester.md](requester.md)): one record per
live dialled address, in dial order, copied at the call; a record is read by its index, 0 to
`weida_connection_stats_len` - 1. No record carries an address: the dialled URL is the only label
(0036 §4.4).

| Function | Returns |
| --- | --- |
| `size_t weida_connection_stats_len(const weida_connection_stats *s)` | the number of records; 0 for NULL |
| `const char *weida_connection_stats_url(const weida_connection_stats *s, size_t index, size_t *len_out)` | the URL the record was dialled as, exactly as given to `weida_requester_connect`; NULL past the end. Lives as long as `s` |
| `int weida_connection_stats_<field>(const weida_connection_stats *s, size_t index, uint64_t *out)` | 1 and `*out` when present; 0 past the end, for a NULL argument, or when the record does not carry it (R2) |
| `void weida_connection_stats_drop(weida_connection_stats *s)` | — |

| `<field>` | Rust field | Absent when |
| --- | --- | --- |
| `age_us` | `age` (µs) | never, within `len` |
| `redials` | `redials` | never, within `len` |
| `transport_path_rtt_us` | `transport.path.rtt` (µs) | the transport is local |
| `transport_path_min_rtt_us` | `transport.path.min_rtt` (µs) | the transport is local |
| `transport_path_cwnd` | `transport.path.cwnd` | the transport is local |
| `transport_path_congestion_events` | `transport.path.congestion_events` | the transport is local |
| `transport_path_lost_packets` | `transport.path.lost_packets` | the transport is local |
| `transport_path_lost_bytes` | `transport.path.lost_bytes` | the transport is local |
| `transport_path_sent_packets` | `transport.path.sent_packets` | the transport is local |
| `transport_path_current_mtu` | `transport.path.current_mtu` | the transport is local |
| `transport_path_max_datagram_size` | `transport.path.max_datagram_size` | the transport is local, or the peer accepts no datagram |
| `transport_tx_datagrams` | `transport.tx.datagrams` | the transport is local |
| `transport_tx_bytes` | `transport.tx.bytes` | the transport is local |
| `transport_rx_datagrams` | `transport.rx.datagrams` | the transport is local |
| `transport_rx_bytes` | `transport.rx.bytes` | the transport is local |
| `remote_rtt_us` | `remote.rtt` (µs) | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_min_rtt_us` | `remote.min_rtt` (µs) | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_cwnd` | `remote.cwnd` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_congestion_events` | `remote.congestion_events` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_lost_packets` | `remote.lost_packets` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_lost_bytes` | `remote.lost_bytes` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_sent_packets` | `remote.sent_packets` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_current_mtu` | `remote.current_mtu` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_tx_datagrams` | `remote.tx.datagrams` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_tx_bytes` | `remote.tx.bytes` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_rx_datagrams` | `remote.rx.datagrams` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_rx_bytes` | `remote.rx.bytes` | no record from the peer yet, a local transport, or `path_report` off on either side |
| `remote_age_us` | `remote.age` (µs) | no record from the peer yet, a local transport, or `path_report` off on either side |

## `weida_peer_events` (a try handle, R3a)

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_peer_events_fd(const weida_peer_events *e, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_peer_events_recv(const weida_peer_events *e, int64_t timeout_ms, weida_peer_event **out)` | up to `timeout_ms` | 0 and the next event; 0 with `*out == NULL` once the requester, or the relayed binding's registration, is gone; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_peer_events_drop(weida_peer_events *e)` | never | — |

Each `weida_requester_events` or `weida_binding_relay_events` call is an independent reader
that sees every event from then on. The queue holds 64 events; a reader that falls further
behind gets one `WEIDA_PEER_EVENT_MISSED` event carrying how many it lost. Dropping a reader
ends a thread waiting in `recv` with `-WEIDA_ECLOSED`.

## `weida_peer_event`

| Function | Returns |
| --- | --- |
| `int weida_peer_event_kind(const weida_peer_event *e)` | the kind, a `WEIDA_PEER_EVENT_*` constant |
| `const char *weida_peer_event_url(const weida_peer_event *e, size_t *len_out)` | the address as dialled; NULL for `MISSED`. Lives as long as `e` |
| `const char *weida_peer_event_peer_fingerprint(const weida_peer_event *e, size_t *len_out)` | `CONNECTED`: the fingerprint (`sha256:` text) the peer proved; NULL for an anonymous peer or a local principal |
| `int weida_peer_event_cause(const weida_peer_event *e, int *out)` | `LOST`: 1 and the `WEIDA_LOSS_*` cause |
| `int weida_peer_event_attempt(const weida_peer_event *e, uint32_t *out)` | `RETRYING`: 1 and which attempt since the last connection that stayed up, from 1 |
| `int weida_peer_event_delay_us(const weida_peer_event *e, uint64_t *out)` | `RETRYING`: 1 and the wait before it, in microseconds |
| `int weida_peer_event_why(const weida_peer_event *e, int *out)` | `GAVE_UP`: 1 and why, a `WEIDA_GIVE_UP_*` constant |
| `int weida_peer_event_why_attempts(const weida_peer_event *e, uint32_t *out)` | `GAVE_UP` for `POLICY`: 1 and the redials attempted after the loss |
| `const char *weida_peer_event_why_presented(const weida_peer_event *e, size_t *len_out)` | `GAVE_UP` for `PEER_CHANGED`: the fingerprint that answered instead; NULL when it proved none |
| `const char *weida_peer_event_why_message(const weida_peer_event *e, size_t *len_out)` | `GAVE_UP` for `FAILED`: why the redial could not be used |
| `int weida_peer_event_missed(const weida_peer_event *e, uint64_t *out)` | `MISSED`: 1 and how many events this reader lost |
| `void weida_peer_event_drop(weida_peer_event *e)` | — |

A getter of another kind's field reads absent: 0, or NULL.

## Constants

| Rust type | Constants, value |
| --- | --- |
| `PeerEvent` | `WEIDA_PEER_EVENT_CONNECTED` 0, `_LOST` 1, `_RETRYING` 2, `_GAVE_UP` 3, `_MISSED` 4 |
| `GiveUp` | `WEIDA_GIVE_UP_POLICY` 0, `_PEER_CHANGED` 1, `_FAILED` 2 |
