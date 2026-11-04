# RADIO/DISH

A radio serves one endpoint path of a listener and fans segments out to every dish joined to
their topic, written in layers and never waiting; a dish dials radios, joins topics and
receives the segments. Every function is callable from any thread (R5). Every `int` function
can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## `weida_radio`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_radio(const weida_listener *l, const char *path, weida_radio **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `const char *weida_radio_path(const weida_radio *r, size_t *len_out)` | never | the path |
| `int weida_radio_segment(const weida_radio *r, const char *topic, const weida_segment_terms *terms, weida_segment **out)` | never | 0, the topic's next segment to the dishes joined now; NULL `terms` are the defaults; `-WEIDA_ELIMIT_EXCEEDED` for a topic above 256 bytes or when `max_sequence_scopes` topics all have a copy in flight |
| `int weida_radio_datagram(const weida_radio *r, const char *topic, const uint8_t *payload, size_t len, size_t *out)` | never; the payload is copied | 0 and how many dishes the datagram segment was handed to; `-WEIDA_ELIMIT_EXCEEDED` for a topic above 256 bytes |
| `size_t weida_radio_dish_count(const weida_radio *r)` | never | the dishes joined; 0 for NULL |
| `uint64_t weida_radio_dropped(const weida_radio *r)` | never | the copies dropped, summed over topics and causes; 0 for NULL |
| `int weida_radio_dropped_on(const weida_radio *r, const char *topic, weida_topic_drops **out)` | never; a snapshot | 0, one record, or none when nothing was dropped on `topic` ([pubsub.md](pubsub.md)) |
| `int weida_radio_drops(const weida_radio *r, weida_topic_drops **out)` | never; a snapshot | 0, one record per topic that lost a copy ([pubsub.md](pubsub.md)) |
| `int weida_radio_evict(const weida_radio *r, const char *peer, const char *filter, size_t *out)` | never | 0 and how many joins on `filter` of the dishes whose key is `peer` (`sha256:` text) went; `-WEIDA_EINVALID_FINGERPRINT` |
| `int weida_radio_dish_drops(const weida_radio *r, weida_dish_drops **out)` | never; a snapshot | 0, one record per joined dish connection |
| `void weida_radio_drop(weida_radio *r)` | never | — |

## `weida_segment`

| Function | Blocks | Returns |
| --- | --- | --- |
| `uint64_t weida_segment_number(const weida_segment *s)` | never | the segment's number on its topic, 0 for the first; 0 for NULL |
| `const char *weida_segment_topic(const weida_segment *s, size_t *len_out)` | never | the topic |
| `int weida_segment_write(const weida_segment *s, const uint8_t *chunk, size_t len, size_t *out)` | never; the chunk is copied | as `weida_segment_write_layer` on layer 0 |
| `int weida_segment_write_layer(const weida_segment *s, uint8_t layer, const uint8_t *chunk, size_t len, size_t *out)` | never; the chunk is copied | 0 and how many copies took the chunk; `-WEIDA_ELIMIT_EXCEEDED` for a layer of `WEIDA_MAX_SEGMENT_LAYERS` (16) or above or a chunk above 4 GiB, `-WEIDA_ERUNTIME` for a finished layer |
| `int weida_segment_finish_layer(const weida_segment *s, uint8_t layer)` | never | 0; a layer of 16 or above is ignored |
| `int weida_segment_finish(weida_segment *s, size_t *out)` | never; **consumes** `s` | 0 and how many copies still hold layer 0 |
| `void weida_segment_drop(weida_segment *s)` | never | — |

A segment's calls take its lock in turn, where Rust's `&mut self` forbids concurrent use. A
segment dropped without `weida_segment_finish` resets every unfinished layer on every copy.

## `weida_segment_terms` and `weida_join_terms`

| Function | Returns |
| --- | --- |
| `int weida_segment_terms_new(weida_segment_terms **out)` | 0, the defaults: no `max_age`, priority 0, not following an upstream |
| `int weida_segment_terms_set_max_age_ms(const weida_segment_terms *t, int64_t v)` | 0; a negative value is none |
| `int weida_segment_terms_set_priority(const weida_segment_terms *t, int32_t v)` | 0; `-WEIDA_EINVAL` outside `int16_t`, the value unchanged |
| `int weida_segment_terms_set_follows_upstream(const weida_segment_terms *t, int v)` | 0; a flag |
| `void weida_segment_terms_drop(weida_segment_terms *t)` | — |
| `int weida_join_terms_new(weida_join_terms **out)` | 0, the defaults: no `max_age`, every layer |
| `int weida_join_terms_set_max_age_ms(const weida_join_terms *t, int64_t v)` | 0; a negative value is none |
| `int weida_join_terms_set_max_layer(const weida_join_terms *t, int64_t v)` | 0; a negative value is none; `-WEIDA_EINVAL` above 255, the value unchanged; 16 to 255 fail the join |
| `void weida_join_terms_drop(weida_join_terms *t)` | — |

Terms are copied by the call that takes them.

## `weida_dish`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_dish(const weida_runtime *rt, const weida_client_tls *tls, weida_dish **out)` | never | 0 |
| `int weida_dish_connect(const weida_dish *d, const char *url, weida_dish_connect_op **out)` | never (R3b) | 0; the URL is parsed by the op |
| `size_t weida_dish_peer_count(const weida_dish *d)` | never | the connected radios; 0 for NULL |
| `int weida_dish_connection_stats(const weida_dish *d, weida_connection_stats **out)` | never; a snapshot | 0 ([stats.md](stats.md)) |
| `int weida_dish_events(const weida_dish *d, weida_peer_events **out)` | never | 0, a new reader ([stats.md](stats.md)) |
| `int weida_dish_join(const weida_dish *d, const char *filter, const weida_join_terms *terms, weida_dish_join_op **out)` | never (R3b); the filter and the terms are copied | 0; NULL `terms` are the defaults |
| `int weida_dish_leave(const weida_dish *d, const char *filter, weida_dish_leave_op **out)` | never (R3b); the filter is copied | 0 |
| `int weida_dish_fd(const weida_dish *d, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_dish_recv(const weida_dish *d, int64_t timeout_ms, weida_received **out)` | up to `timeout_ms` | 0 and the next segment; `-WEIDA_ENOT_CONNECTED` once the queue ended; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `uint64_t weida_dish_stale(const weida_dish *d)` | never | segments discarded because a newer one on their topic was already delivered; 0 for NULL |
| `uint64_t weida_dish_overflow(const weida_dish *d)` | never | segments discarded because the receive queue was full; 0 for NULL |
| `void weida_dish_drop(weida_dish *d)` | never | — |

Each op type, `weida_dish_connect_op`, `_join_op` and `_leave_op`, has
`_op_fd(op, int64_t *out)`, `_op_drop(op)` and `int _op_wait(op, int64_t timeout_ms)`, which
returns 0 when done, `-WEIDA_EAGAIN`, `-WEIDA_EINTR` or `-WEIDA_ECLOSED`, and the operation's
own errors: for connect those of `weida_pusher_connect_op_wait` ([push.md](push.md)); for join
`-WEIDA_ELIMIT_EXCEEDED` for a `max_layer` of 16 or above and `-WEIDA_EPROTOCOL` for a filter
outside the grammar ([pubsub.md](pubsub.md)). `weida_dish_recv` is a try (R3a) like
`weida_subscriber_recv`: the take in flight is kept between tries.

## `weida_received`

One thing a dish received, a tagged handle.

| Function | Returns |
| --- | --- |
| `int weida_received_kind(const weida_received *r)` | `WEIDA_RECEIVED_SEGMENT` (a stream segment) or `WEIDA_RECEIVED_DATAGRAM` |
| `int weida_received_take_segment(const weida_received *r, weida_incoming_transfer **out)` | 0 and a stream segment's transfer, once; its topic, number and layer are on its metadata ([msg.md](msg.md)); `-WEIDA_ECLOSED` on a second call, `-WEIDA_EINVAL` on a datagram |
| `const char *weida_received_topic(const weida_received *r, size_t *len_out)` | a datagram's topic; NULL on a stream segment |
| `int weida_received_segment(const weida_received *r, uint64_t *out)` | 1 and a datagram's segment number; 0 on a stream segment |
| `const uint8_t *weida_received_payload(const weida_received *r, size_t *len_out)` | a datagram's payload, its length through `len_out` (NULL allowed), valid until `r` is dropped; NULL on a stream segment |
| `void weida_received_drop(weida_received *r)` | —; a transfer not taken is refused with `REJECTED` |

## `weida_dish_drops`

What a radio dropped toward each joined dish connection, copied at the call. A record goes when
its connection closes.

| Function | Returns |
| --- | --- |
| `size_t weida_dish_drops_len(const weida_dish_drops *d)` | the number of records; 0 for NULL |
| `const char *weida_dish_drops_peer_fingerprint(const weida_dish_drops *d, size_t index, size_t *len_out)` | the `sha256:` key the dish of record `index` proved; NULL past the end and for a dish that proved no key |
| `int weida_dish_drops_subscriber_budget(const weida_dish_drops *d, size_t index, uint64_t *out)` | 1 and the copies dropped for an exhausted byte budget; 0 past the end |
| `int weida_dish_drops_subscriber_queue(const weida_dish_drops *d, size_t index, uint64_t *out)` | 1 and the copies dropped for a full queue; 0 past the end |
| `int weida_dish_drops_superseded(const weida_dish_drops *d, size_t index, uint64_t *out)` | 1 and the copies reset by a newer segment; 0 past the end |
| `int weida_dish_drops_expired(const weida_dish_drops *d, size_t index, uint64_t *out)` | 1 and the copies reset after the `max_age`; 0 past the end |
| `int weida_dish_drops_too_large(const weida_dish_drops *d, size_t index, uint64_t *out)` | 1 and the datagram segments larger than the dish's connection carries; 0 past the end |
| `int weida_dish_drops_no_datagrams(const weida_dish_drops *d, size_t index, uint64_t *out)` | 1 and the datagram segments for a dish that carries no datagrams; 0 past the end |
| `int weida_dish_drops_layers_cut(const weida_dish_drops *d, size_t index, uint64_t *out)` | 1 and the segment copies that lost layers above 0 and kept layer 0; 0 past the end |
| `void weida_dish_drops_drop(weida_dish_drops *d)` | — |

## Rules

- A segment goes to the dishes joined when it opens. A write never waits: a copy without room
  loses that layer and every layer above it, and loses the segment if the layer is 0.
- Opening segment n+1 resets every unfinished layer of segment n on that topic, counted
  `superseded`. On a local transport a finished layer gets no grace.
- A layer above a dish's `max_layer` is never sent to that dish, and that is not a drop.
- A dish discards a segment older than the newest it delivered on its topic, counted in
  `weida_dish_stale`, and discards on a full queue, counted in `weida_dish_overflow`.
- Two dishes on one runtime collide on the path. Use one runtime per dish.
- A datagram segment needs `datagram_receive_bytes` on both ends. Otherwise it is a
  `no_datagrams` drop, never a stream.
- A radio's path is released when its handle is dropped.
- Admission (`Radio::with_admission`) is not mapped. `weida_radio_evict` withdraws a join by
  key and frees its subscription slot.
