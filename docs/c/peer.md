# Raw peers

A raw peer dials addresses and opens one-way streams, exchanges, segments and datagram flows to
them, round-robin; an acceptor takes whatever arrives on one endpoint path as a tagged
`weida_incoming`. A queue serving such a path learns of its consumers and the credit they grant
through the same acceptor, and delivers to them itself. Every function is callable from any
thread (R5). Every `int` function can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## `weida_peer`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_peer(const weida_runtime *rt, const weida_client_tls *tls, weida_peer **out)` | never | 0 |
| `int weida_peer_connect(const weida_peer *p, const char *url, weida_peer_connect_op **out)` | never (R3b) | 0; the wait returns 0 when connected, `-WEIDA_EUNTRUSTED` for a key not trusted; the runtime redials a lost address |
| `int weida_peer_disconnect(const weida_peer *p, const char *url)` | never | 1 when `url` was dialled and is now forgotten, its connection closed and no longer redialled; 0 when it was not |
| `size_t weida_peer_peer_count(const weida_peer *p)` | never | the connected peers; 0 for NULL |
| `uint64_t weida_peer_dropped(const weida_peer *p)` | never | the bodies the outbox discarded; 0 for NULL |
| `int weida_peer_connection_stats(const weida_peer *p, weida_connection_stats **out)` | never; a snapshot | 0 ([stats.md](stats.md)) |
| `int weida_peer_peer_events(const weida_peer *p, weida_peer_events **out)` | never | 0, a new reader ([stats.md](stats.md)); `_peer_events` because `weida_peer_events` is the reader's type |
| `int weida_peer_open(const weida_peer *p, const weida_transfer_meta *meta, weida_peer_open_op **out)` | never (R3b); `meta` is copied, NULL is `TransferMeta::default()` | 0; opens a one-way stream to the next peer |
| `int weida_peer_open_op_wait(const weida_peer_open_op *op, int64_t timeout_ms, weida_outgoing_transfer **out)` | up to `timeout_ms` | 0 and the transfer ([transfer.md](transfer.md)); `-WEIDA_ENOT_CONNECTED` for a peer never connected, `-WEIDA_ECONNECTION_LOST` when no peer came back within `send_timeout`, `-WEIDA_ELIMIT_EXCEEDED` (a topic over 256 bytes); `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `int weida_peer_open_bi(const weida_peer *p, const weida_transfer_meta *meta, weida_peer_open_bi_op **out)` | never (R3b); `meta` is copied, NULL is `TransferMeta::default()` | 0; opens an exchange with the next peer |
| `int weida_peer_open_bi_op_wait(const weida_peer_open_bi_op *op, int64_t timeout_ms, weida_outgoing_transfer **out_body, weida_reply_stream **out_reply)` | up to `timeout_ms` | 0, the request body and the reply stream ([requester.md](requester.md)), both or neither; the codes of `weida_peer_open_op_wait` |
| `int weida_peer_send(const weida_peer *p, const weida_transfer_meta *meta, const uint8_t *body, size_t len, weida_peer_send_op **out)` | never (R3b); `meta` and the body are copied, NULL `meta` is `TransferMeta::default()` | 0; one message through the outbox |
| `int weida_peer_send_op_wait(const weida_peer_send_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 once the body is written or held in the outbox; `-WEIDA_ELIMIT_EXCEEDED` (a full outbox under `OutboxFull::Reject`, a body larger than the outbox, or a topic over 256 bytes), `-WEIDA_ECONNECTION_LOST` once every address is given up, `-WEIDA_ENOT_CONNECTED` for a peer never connected; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `int weida_peer_segment(const weida_peer *p, const char *topic, const weida_segment_terms *terms, weida_peer_segment_op **out)` | never (R3b); `topic` and `terms` are copied, NULL `terms` are `SegmentTerms::default()` | 0; opens segment n+1 on `topic` to the next peer |
| `int weida_peer_segment_op_wait(const weida_peer_segment_op *op, int64_t timeout_ms, weida_segment **out)` | up to `timeout_ms` | 0 and the segment, written with the `weida_segment_*` calls ([radio.md](radio.md)); `-WEIDA_ELIMIT_EXCEEDED` for a topic over 256 bytes or when `max_sequence_scopes` topics all have a copy in flight, and the codes of `weida_peer_open_op_wait` |
| `int weida_peer_segment_drops(const weida_peer *p, const char *topic, weida_topic_drops **out)` | never; a snapshot | 0; one record when a segment on `topic` lost a copy, none otherwise ([pubsub.md](pubsub.md)) |
| `int weida_peer_open_flow(const weida_peer *p, const weida_flow_meta *meta, weida_peer_open_flow_op **out)` | never (R3b); `meta` is copied, NULL is no keys | 0; registers a flow to the next peer ([flow.md](flow.md)) |
| `int weida_peer_open_flow_op_wait(const weida_peer_open_flow_op *op, int64_t timeout_ms, weida_flow **out)` | up to `timeout_ms` | 0 and the flow once its header is written; `-WEIDA_EDATAGRAMS_UNAVAILABLE` when the connection did not agree datagrams, `-WEIDA_ENOT_CONNECTED` for a peer never connected, `-WEIDA_ELIMIT_EXCEEDED` for a label above 256 bytes; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_peer_drop(weida_peer *p)` | never | — |

Each op type, `weida_peer_connect_op`, `weida_peer_open_op`, `weida_peer_open_bi_op`,
`weida_peer_send_op`, `weida_peer_segment_op` and `weida_peer_open_flow_op`, has `_op_fd(op,
int64_t *out)` (0; `-WEIDA_EIO`) and `_op_drop(op)`; `weida_peer_connect_op_wait(op, int64_t
timeout_ms)` returns 0 when connected, `-WEIDA_EAGAIN`, `-WEIDA_EINTR` or `-WEIDA_ECLOSED`.
Dropping an unfinished op aborts it. What an open produced that no wait took is dropped: a
transfer is reset `CANCELED`, an exchange abandoned (its body reset and its reply half stopped
`CANCELED`), a segment's unfinished layers reset, a flow closed.

## `weida_acceptor` and `weida_incoming`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_acceptor(const weida_listener *l, const char *path, weida_acceptor **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `const char *weida_acceptor_path(const weida_acceptor *a, size_t *len_out)` | never | the path, valid as long as `a`; NULL for NULL |
| `int weida_acceptor_fd(const weida_acceptor *a, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_acceptor_accept(const weida_acceptor *a, int64_t timeout_ms, weida_incoming **out)` | up to `timeout_ms` | 0 and what arrived next, tagged; `-WEIDA_ENOT_CONNECTED` once the queue ended; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_acceptor_drop(weida_acceptor *a)` | never | —; the path stays registered, a waiting accept returns `-WEIDA_ECLOSED` |
| `int weida_incoming_kind(const weida_incoming *i)` | never | a `WEIDA_INCOMING_*` constant |
| `int weida_incoming_take_stream(const weida_incoming *i, weida_incoming_transfer **out)` | never | 0 and the transfer ([transfer.md](transfer.md)), once; `-WEIDA_ECLOSED` after that, `-WEIDA_EINVAL` for another kind |
| `int weida_incoming_take_exchange(const weida_incoming *i, weida_incoming_request **out)` | never | 0 and the request, which owes a reply ([listener.md](listener.md)), once; `-WEIDA_ECLOSED` after that, `-WEIDA_EINVAL` for another kind |
| `int weida_incoming_take_subscribed(const weida_incoming *i, weida_consumer **out)` | never | 0 and the consumer, once; `-WEIDA_ECLOSED` after that, `-WEIDA_EINVAL` for another kind |
| `int weida_incoming_take_credit(const weida_incoming *i, weida_credit_grant **out)` | never | 0 and the grant, once; `-WEIDA_ECLOSED` after that, `-WEIDA_EINVAL` for another kind |
| `int weida_incoming_take_flow(const weida_incoming *i, weida_incoming_flow **out)` | never | 0 and the flow ([flow.md](flow.md)), once; `-WEIDA_ECLOSED` after that, `-WEIDA_EINVAL` for another kind |
| `int weida_incoming_unsubscribed_id(const weida_incoming *i, uint64_t *out)` | never | 1 and the consumer number of an unsubscription; 0 for another kind and a NULL argument |
| `const char *weida_incoming_unsubscribed_filter(const weida_incoming *i, size_t *len_out)` | never | the filter of an unsubscription, valid as long as `i`; NULL when the whole connection went away, for another kind and for NULL |
| `void weida_incoming_drop(weida_incoming *i)` | never | —; drops what was not taken: a stream is refused `REJECTED`, an exchange answers `NO_REPLY`, a flow is stopped `CANCELED`, a subscription, credit grant or unsubscription is forgotten |

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

## `weida_consumer`

| Function | Blocks | Returns |
| --- | --- | --- |
| `uint64_t weida_consumer_id(const weida_consumer *c)` | never | the number of the connection it subscribed on, shared by every subscription of that connection; 0 for NULL |
| `const char *weida_consumer_filter(const weida_consumer *c, size_t *len_out)` | never | the filter the subscription was created with, valid as long as `c`; NULL for NULL |
| `const char *weida_consumer_path(const weida_consumer *c, size_t *len_out)` | never | the endpoint path it subscribed on, valid as long as `c`; NULL for NULL |
| `int weida_consumer_open(const weida_consumer *c, const weida_transfer_meta *meta, weida_consumer_open_op **out)` | never (R3b); `meta` is copied, NULL is `TransferMeta::default()` | 0; one delivery addressed to the subscribed path, arriving at the consumer's subscriber; the topic it filters on travels in `meta` |
| `int weida_consumer_open_op_wait(const weida_consumer_open_op *op, int64_t timeout_ms, weida_outgoing_transfer **out)` | up to `timeout_ms` | 0 and the transfer ([transfer.md](transfer.md)); `-WEIDA_ECONNECTION_LOST` once its connection is gone; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `int weida_consumer_deliver(const weida_consumer *c, const weida_transfer_meta *meta, const uint8_t *body, size_t len, weida_consumer_deliver_op **out)` | never (R3b); `meta` and the body are copied, NULL `meta` is `TransferMeta::default()` | 0; one whole delivery |
| `int weida_consumer_deliver_op_wait(const weida_consumer_deliver_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 once the body is written and finished; `-WEIDA_ECONNECTION_LOST` once its connection is gone; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `int weida_consumer_close(const weida_consumer *c, uint64_t code, const char *reason)` | never | 0; closes the connection the consumer subscribed on, ending every subscription it holds on every path. `code` is one of [PROTOCOL.md](../PROTOCOL.md) §7's (e.g. 5 `LIMIT_EXCEEDED`, 6 `SHUTDOWN`); above `2^62-1` it is sent as 1 `INTERNAL`. Closing a closed connection does nothing |
| `void weida_consumer_drop(weida_consumer *c)` | never | —; the subscription stays |

Each op type, `weida_consumer_open_op` and `weida_consumer_deliver_op`, has `_op_fd(op, int64_t
*out)` (0; `-WEIDA_EIO`) and `_op_drop(op)`. Dropping an unfinished op aborts it; a transfer an
open produced that no wait took is reset `CANCELED`. A consumer has no clone: one handle serves
every thread (R5).

## `weida_credit_grant`

A consumer's absolute delivery limit for one subscription, read-only.

| Function | Returns |
| --- | --- |
| `uint64_t weida_credit_grant_id(const weida_credit_grant *g)` | the number of the consumer that granted it, as `weida_consumer_id` reads it; 0 for NULL |
| `const char *weida_credit_grant_filter(const weida_credit_grant *g, size_t *len_out)` | the filter of the subscription it was granted for, valid as long as `g`; NULL for NULL |
| `uint64_t weida_credit_grant_limit(const weida_credit_grant *g)` | the messages the subscription accepts in total since its creation; 0 for NULL |
| `void weida_credit_grant_drop(weida_credit_grant *g)` | — |

## Rules

- A peer's opens go to the next live address, round-robin; with every address down they wait for
  a redial up to `send_timeout`.
- `weida_peer_send` owns its body from the call: with a live peer and an empty outbox it is
  written at once, otherwise it waits in the outbox under `RuntimeConfig::outbox_full`
  ([runtime.md](runtime.md)).
- A segment supersedes the peer's previous one on its topic as a radio's does
  ([radio.md](radio.md)): opening segment n+1 resets the unfinished layers of segment n, which
  the acceptor reads as `-WEIDA_ECANCELED`, counted `superseded` in `weida_peer_segment_drops`.
- Subscriptions, credit and unsubscriptions arrive in the order the peer caused them, per
  connection, so a grant always follows the subscription it names.
- Credit is absolute and cumulative, and it is only reported: nothing is delivered by itself. A
  queue keeps the highest limit per subscription and delivers while what it delivered is below
  it. A fresh subscription's limit is 0.
- A closed consumer connection, by either side, reports `WEIDA_INCOMING_UNSUBSCRIBED` with a NULL
  filter once per path it consumed on. A subscriber that redials subscribes again on a new
  connection, under a new number.
- The consumer number equals the connection number (`ConnectionId`) of its transfers and is never
  reused while the process lives.
