# Pub/Sub

A publisher serves one endpoint path of a listener and fans each message out to every subscriber
whose filter matches its topic; a subscriber dials publishers and receives the copies. Every
function is callable from any thread (R5). Every `int` function can also return
`-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## `weida_publisher`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_publisher(const weida_listener *l, const char *path, weida_publisher **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `const char *weida_publisher_path(const weida_publisher *p, size_t *len_out)` | never | the path |
| `int weida_publisher_publish(const weida_publisher *p, const char *topic, const uint8_t *data, size_t len, size_t *out)` | never; the data is copied | 0 and how many subscribers the copy was enqueued to; `-WEIDA_ELIMIT_EXCEEDED` for `len` above `subscriber_buffer_bytes` or a topic above 256 bytes |
| `int weida_publisher_publish_with_trace(const weida_publisher *p, const char *topic, const uint8_t *data, size_t len, const char *traceparent, size_t *out)` | never; the data is copied | as `weida_publisher_publish`, propagating the trace; `-WEIDA_EINVAL` for anything but a W3C version-00 `traceparent` |
| `int weida_publisher_open(const weida_publisher *p, const char *topic, weida_fan_out **out)` | never | 0, a streamed publish to the subscribers matched now |
| `int weida_publisher_open_with_trace(const weida_publisher *p, const char *topic, const char *traceparent, weida_fan_out **out)` | never | as `weida_publisher_open`, propagating the trace; `-WEIDA_EINVAL` for anything but a W3C version-00 `traceparent` |
| `size_t weida_publisher_subscriber_count(const weida_publisher *p)` | never | the subscribed connections; 0 for NULL |
| `size_t weida_publisher_filter_count(const weida_publisher *p)` | never | the filters registered, summed over subscribers; 0 for NULL |
| `uint64_t weida_publisher_dropped(const weida_publisher *p)` | never | the copies dropped because a subscriber could not take them, summed over topics and causes; 0 for NULL |
| `int weida_publisher_dropped_on(const weida_publisher *p, const char *topic, weida_topic_drops **out)` | never; a snapshot | 0, one record, or none when nothing was dropped on `topic` |
| `int weida_publisher_drops(const weida_publisher *p, weida_topic_drops **out)` | never; a snapshot | 0, one record per topic that lost a copy |
| `void weida_publisher_drop(weida_publisher *p)` | never | — |

`weida_publisher_publish` never waits on a subscriber: one whose byte budget
(`Limits::subscriber_buffer_bytes`) is exhausted loses this copy, counted by
`weida_publisher_dropped` and by topic and cause in `weida_publisher_drops`. Zero subscribers
is not an error. Subscriptions that arrived before the publisher was registered are already
recorded. A dropped publisher leaves its path registered, as a dropped Rust `Publisher` does.

## `weida_fan_out`

A publish in progress: one payload written chunk by chunk, a stream per subscriber matched at
the open. The budget bounds a **chunk**, so the payload has no ceiling.

| Function | Blocks | Returns |
| --- | --- | --- |
| `const char *weida_fan_out_topic(const weida_fan_out *f, size_t *len_out)` | never | the topic |
| `int weida_fan_out_subscribers(const weida_fan_out *f, size_t *out)` | never | 0 and the subscribers still receiving; it only falls |
| `int weida_fan_out_write_within(const weida_fan_out *f, const uint8_t *chunk, size_t len, uint64_t limit_ms, weida_fan_out_write_within_op **out)` | never (R3b); the chunk is copied | 0 |
| `int weida_fan_out_write_now(const weida_fan_out *f, const uint8_t *chunk, size_t len, size_t *out)` | never; the chunk is copied | 0 and the subscribers left: one with no room right now loses the transfer; `-WEIDA_ELIMIT_EXCEEDED` for a chunk above `subscriber_buffer_bytes`, `-WEIDA_ECANCELED` after an abandoned write |
| `int weida_fan_out_finish(weida_fan_out *f, size_t *out)` | never; **consumes** `f` | 0 and the subscribers that received all of it as far as this side can tell; 0 of them after an abandoned write, every copy aborted |
| `void weida_fan_out_drop(weida_fan_out *f)` | never | — |

`write_within`'s `limit_ms` bounds the whole call, not each subscriber: those with no room are
waited for together, and one that frees none in time loses the transfer, and only it. While a
`write_within` op runs, every other call on the fan-out is `-WEIDA_EINVAL` (Rust's borrow rules
forbid it there): wait for the op first. A fan-out dropped, or finished, while its write runs
aborts its copies once the write ends; one dropped without `weida_fan_out_finish` aborts every
copy, so no subscriber mistakes a partial payload for a whole one.

## `weida_fan_out_write_within_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_fan_out_write_within_op_fd(const weida_fan_out_write_within_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_fan_out_write_within_op_wait(const weida_fan_out_write_within_op *op, int64_t timeout_ms, size_t *out)` | up to `timeout_ms` | 0 and the subscribers left; `-WEIDA_ELIMIT_EXCEEDED` for a chunk above `subscriber_buffer_bytes` (nothing written or aborted), `-WEIDA_ECANCELED` after an abandoned write; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_fan_out_write_within_op_drop(weida_fan_out_write_within_op *op)` | never | — |

A write op dropped unfinished abandons the transfer: the chunk reached some subscribers and not
the rest, so later writes fail with `-WEIDA_ECANCELED` and `weida_fan_out_finish` counts 0,
aborting every copy.

## `weida_topic_drops`

What a publisher dropped, one record per topic, copied at the call.

| Function | Returns |
| --- | --- |
| `size_t weida_topic_drops_len(const weida_topic_drops *d)` | the number of records; 0 for NULL |
| `const char *weida_topic_drops_topic(const weida_topic_drops *d, size_t index, size_t *len_out)` | the topic of record `index`; NULL past the end |
| `int weida_topic_drops_subscriber_budget(const weida_topic_drops *d, size_t index, uint64_t *out)` | 1 and the copies dropped for an exhausted subscriber byte budget; 0 past the end |
| `int weida_topic_drops_subscriber_queue(const weida_topic_drops *d, size_t index, uint64_t *out)` | 1 and the copies dropped for a full subscriber queue; 0 past the end |
| `int weida_topic_drops_no_parked_connection(const weida_topic_drops *d, size_t index, uint64_t *out)` | 1 and the copies dropped because the subscriber had parked no connection; 0 past the end |
| `int weida_topic_drops_superseded(const weida_topic_drops *d, size_t index, uint64_t *out)` | 1 and the RADIO copies reset by a newer segment; 0 past the end |
| `int weida_topic_drops_expired(const weida_topic_drops *d, size_t index, uint64_t *out)` | 1 and the RADIO copies reset after the dish's `max_age`; 0 past the end |
| `int weida_topic_drops_too_large(const weida_topic_drops *d, size_t index, uint64_t *out)` | 1 and the RADIO datagram segments larger than the dish's connection carries; 0 past the end |
| `int weida_topic_drops_no_datagrams(const weida_topic_drops *d, size_t index, uint64_t *out)` | 1 and the RADIO datagram segments for a dish that carries no datagrams; 0 past the end |
| `int weida_topic_drops_layers_cut(const weida_topic_drops *d, size_t index, uint64_t *out)` | 1 and the segment copies that lost layers above 0 and kept layer 0; 0 past the end |
| `void weida_topic_drops_drop(weida_topic_drops *d)` | — |

The table holds at most `Limits::max_sequence_scopes` topics; a topic beyond it counts in
`weida_publisher_dropped` only. `TopicDrops::total` is the sum of the eight counts.

## `weida_subscriber`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_subscriber(const weida_runtime *rt, const weida_client_tls *tls, weida_subscriber **out)` | never | 0 |
| `int weida_subscriber_connect(const weida_subscriber *s, const char *url, weida_subscriber_connect_op **out)` | never (R3b) | 0; the URL is parsed by the op |
| `int weida_subscriber_disconnect(const weida_subscriber *s, const char *url)` | never | 1 when `url` was dialled and is now forgotten; 0 when it was not |
| `size_t weida_subscriber_peer_count(const weida_subscriber *s)` | never | the connected peers; 0 for NULL |
| `int weida_subscriber_connection_stats(const weida_subscriber *s, weida_connection_stats **out)` | never; a snapshot | 0 ([stats.md](stats.md)) |
| `int weida_subscriber_events(const weida_subscriber *s, weida_peer_events **out)` | never | 0, a new reader ([stats.md](stats.md)) |
| `int weida_subscriber_subscribe(const weida_subscriber *s, const char *filter, weida_subscriber_subscribe_op **out)` | never (R3b); the filter is copied | 0 |
| `int weida_subscriber_unsubscribe(const weida_subscriber *s, const char *filter, weida_subscriber_unsubscribe_op **out)` | never (R3b); the filter is copied | 0 |
| `int weida_subscriber_grant(const weida_subscriber *s, const char *filter, uint64_t limit, weida_subscriber_grant_op **out)` | never (R3b); the filter is copied | 0 |
| `size_t weida_subscriber_filter_count(const weida_subscriber *s)` | never | the filters registered; 0 for NULL |
| `int weida_subscriber_fd(const weida_subscriber *s, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_subscriber_recv(const weida_subscriber *s, int64_t timeout_ms, weida_incoming_transfer **out)` | up to `timeout_ms` | 0 and the next copy as soon as its header arrives, its topic in `weida_incoming_meta_topic`; `-WEIDA_ENOT_CONNECTED` once the queue ended; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_subscriber_drop(weida_subscriber *s)` | never | — |

Each op type, `weida_subscriber_connect_op`, `_subscribe_op`, `_unsubscribe_op` and
`_grant_op`, has `_op_fd(op, int64_t *out)`, `_op_drop(op)` and
`int _op_wait(op, int64_t timeout_ms)`, which returns 0 when done, `-WEIDA_EAGAIN`,
`-WEIDA_EINTR` or `-WEIDA_ECLOSED`, and the operation's own errors: for connect those of
`weida_pusher_connect_op_wait` ([push.md](push.md)); for subscribe and grant
`-WEIDA_EPROTOCOL` for a filter outside the grammar, which never travels to the publisher;
for grant `-WEIDA_ELIMIT_EXCEEDED` for a credit header a peer would not read.

A filter is a segmented pattern ([PROTOCOL.md](../PROTOCOL.md) §6.4): segments are split on
`.`, `*` matches exactly one whole segment, a trailing `#` matches zero or more, and every other
byte is literal; `""` and `#` receive everything. Subscribing twice is idempotent, and an
unknown filter is ignored by unsubscribe. A connect subscribes there to every filter registered
so far, and a redial does it again; what was published in between is gone, and
`weida_subscriber_events` says when the gap began and ended. A grant is the L2 credit a queue
serving the path consults (absolute, cumulative, never lowered); a publisher consults none.

Two subscribers on one runtime share its pooled connection and collide on the path: the second
dial fails with `-WEIDA_EALREADY_REGISTERED`. Use one runtime per subscriber of a path.
`weida_subscriber_recv` is a try (R3a) like `weida_puller_recv`: the take in flight is kept
between tries.
