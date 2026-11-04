# Runtime and configuration

The rules are in [README.md](README.md); signatures are those of `weida.h`. Every function here
is callable from any thread (R5). "Never blocks" means the call returns without waiting on the
network or another thread; a lock it takes is held only for the call's own bookkeeping.

## `weida_runtime_config`

A builder for `weida::RuntimeConfig`, one setter per field but `resolver` (absent, *resolver*)
and `network` (test-only). Unset fields keep weida's defaults. Values follow R7: counts are
`uint64_t` and saturate at `SIZE_MAX`, durations are milliseconds, an optional value is
negative for none, an enumeration is one of the constants below, and a nested configuration is
its own builder, copied by the setter that takes it (NULL is its default).

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_config_new(weida_runtime_config **out)` | never | 0; `-WEIDA_EINVAL` for a NULL `out` |
| `int weida_runtime_config_set_worker_threads(const weida_runtime_config *cfg, uint32_t threads)` | never | 0; `-WEIDA_EINVAL`. `0` is accepted here and refused by `weida_runtime_owned` |
| `int weida_runtime_config_set_shutdown_timeout_ms(const weida_runtime_config *cfg, uint64_t ms)` | never | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_config_set_max_connections(const weida_runtime_config *cfg, uint64_t v)` | never | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_config_set_max_connections_per_peer(const weida_runtime_config *cfg, uint64_t v)` | never | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_config_set_endpoint_queue(const weida_runtime_config *cfg, uint64_t v)` | never | 0; `-WEIDA_EINVAL`, also for `v == 0` (Rust panics at the first queue) |
| `int weida_runtime_config_set_max_resolved_addresses(const weida_runtime_config *cfg, uint64_t v)` | never | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_config_set_connect_attempt_timeout_ms(const weida_runtime_config *cfg, uint64_t v)` | never | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_config_set_send_timeout_ms(const weida_runtime_config *cfg, int64_t v)` | never | 0; `-WEIDA_EINVAL`. Negative is none: an `open` waits as long as the redial does |
| `int weida_runtime_config_set_outbox_messages(const weida_runtime_config *cfg, uint64_t v)` | never | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_config_set_outbox_bytes(const weida_runtime_config *cfg, uint64_t v)` | never | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_config_set_outbox_full(const weida_runtime_config *cfg, int v)` | never | 0; `-WEIDA_EINVAL`, also for a value not `WEIDA_OUTBOX_FULL_*` |
| `int weida_runtime_config_set_max_handshakes(const weida_runtime_config *cfg, uint64_t v)` | never | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_config_set_retry_above_handshakes(const weida_runtime_config *cfg, uint64_t v)` | never | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_config_set_discovery(const weida_runtime_config *cfg, int v)` | never | 0; `-WEIDA_EINVAL`, also for a value not `WEIDA_DISCOVERY_*` |
| `int weida_runtime_config_set_limits(const weida_runtime_config *cfg, const weida_limits *v)` | never | 0; `-WEIDA_EINVAL`. Copies `v`; NULL is `Limits::default()` |
| `int weida_runtime_config_set_guarantees(const weida_runtime_config *cfg, const weida_guarantee_set *v)` | never | 0; `-WEIDA_EINVAL`. Copies `v`; NULL is `GuaranteeSet::CORE` |
| `int weida_runtime_config_set_reconnect(const weida_runtime_config *cfg, const weida_reconnect_policy *v)` | never | 0; `-WEIDA_EINVAL`. Copies `v`; NULL is `ReconnectPolicy::default()` |
| `void weida_runtime_config_drop(weida_runtime_config *cfg)` | never | — |

## `weida_limits`

A builder for `weida::Limits`, the per-connection profile, one setter
`int weida_limits_set_<field>(const weida_limits *l, <type> v)` per field. Every setter returns 0,
or `-WEIDA_EINVAL` for a NULL handle. No setter validates more than its type: a receive window
above 2^62-1 or an idle timeout QUIC cannot carry fails where Rust fails it, in the op of the
first QUIC bind or dial, with `-WEIDA_ERUNTIME`.

| Function or field | C type | Rust field |
| --- | --- | --- |
| `int weida_limits_new(weida_limits **out)` | — | `Limits::default()`; `-WEIDA_EINVAL` for a NULL `out` |
| `void weida_limits_drop(weida_limits *l)` | — | — |
| `max_header_bytes`, `stream_receive_window`, `connection_receive_window`, `hello_timeout_ms`, `header_timeout_ms` | `uint64_t` | the same name |
| `max_concurrent_uni_streams`, `max_concurrent_bidi_streams` | `uint32_t` | the same name |
| `keep_alive_ms`, `idle_timeout_ms`, `flow_early_hold_ms` | `uint64_t`, milliseconds | `keep_alive`, `idle_timeout`, `flow_early_hold` |
| `max_subscriptions`, `subscriber_buffer_bytes`, `max_sequence_scopes`, `max_reorder_hold`, `max_local_streams`, `max_parked_reverse`, `max_dedup_entries`, `datagram_receive_bytes`, `datagram_send_bytes`, `max_flows`, `flow_queue_bytes`, `flow_early_bytes`, `max_header_streams` | `uint64_t`, a count | the same name |
| `path_report` | `int`, a flag | `path_report` |
| `congestion` | `int`, `WEIDA_CONGESTION_*`; any other value is `-WEIDA_EINVAL` | `congestion` |

`WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES` is `weida::DEFAULT_DATAGRAM_RECEIVE_BYTES`, the documented
`datagram_receive_bytes` for turning datagram flows on.

## `weida_guarantee_set`

A builder for `weida::GuaranteeSet`, the set a runtime offers and requires, one setter
`int weida_guarantee_set_set_<field>(const weida_guarantee_set *g, <type> v)` per dimension.
Every setter returns 0, or `-WEIDA_EINVAL` for a NULL handle or a value outside its constants.
No combination is checked, as in Rust: a set a peer cannot meet fails the connect or the first
request with `-WEIDA_ENEGOTIATION`.

| Function or field | C type | Rust field |
| --- | --- | --- |
| `int weida_guarantee_set_new(weida_guarantee_set **out)` | — | `GuaranteeSet::CORE`; `-WEIDA_EINVAL` for a NULL `out` |
| `void weida_guarantee_set_drop(weida_guarantee_set *g)` | — | — |
| `delivery` | `int`, `WEIDA_DELIVERY_LEVEL_*` | `delivery` |
| `acknowledgement` | `int`, `WEIDA_ACKNOWLEDGEMENT_*` | `acknowledgement` |
| `durability` | `int`, `WEIDA_DURABILITY_*`; negative is none | `durability` |
| `replicas` | `int64_t`; negative is none | `replicas` |
| `ordering` | `int`, `WEIDA_ORDERING_MODE_*` | `ordering` |
| `deduplication` | `int`, `WEIDA_DEDUPLICATION_*` | `deduplication` |
| `dedup_window_ms` | `int64_t`; negative is none | `dedup_window_ms` |
| `backpressure` | `int`, `WEIDA_BACKPRESSURE_*` | `backpressure` |
| `producer_naming` | `int`, `WEIDA_PRODUCER_NAMING_*` | `producer_naming` |
| `control_isolated` | `int`, a flag | `control_isolated` |

## `weida_reconnect_policy`

A builder for `weida::ReconnectPolicy`, how a dialling endpoint redials a lost address, one
setter `int weida_reconnect_policy_set_<field>(const weida_reconnect_policy *p, <type> v)` per
field. Every setter returns 0, or `-WEIDA_EINVAL` for a NULL handle.

| Function or field | C type | Rust field |
| --- | --- | --- |
| `int weida_reconnect_policy_new(weida_reconnect_policy **out)` | — | `ReconnectPolicy::default()`; `-WEIDA_EINVAL` for a NULL `out` |
| `int weida_reconnect_policy_never(weida_reconnect_policy **out)` | — | `ReconnectPolicy::never()`; `-WEIDA_EINVAL` for a NULL `out` |
| `void weida_reconnect_policy_drop(weida_reconnect_policy *p)` | — | — |
| `initial_ms`, `max_ms` | `uint64_t`, milliseconds | `initial`, `max` |
| `jitter`, `stop_on_peer_closed` | `int`, a flag | the same name |
| `max_attempts` | `int64_t`; negative is none, above `UINT32_MAX` is `-WEIDA_EINVAL` | `max_attempts` |

## Constants

Each enumeration is a set of `#define`s, `WEIDA_<TYPE>_<VARIANT>` (R7). The value is the wire
value of `docs/PROTOCOL.md` §6.5 for the guarantee dimensions, and the declaration order from 0
for the rest.

| Rust type | Constants, value |
| --- | --- |
| `Congestion` | `WEIDA_CONGESTION_CUBIC` 0, `_NEW_RENO` 1, `_BBR` 2 |
| `Discovery` | `WEIDA_DISCOVERY_AWARE` 0, `_SINGLE` 1 |
| `OutboxFull` | `WEIDA_OUTBOX_FULL_BLOCK` 0, `_DROP` 1, `_REJECT` 2 |
| `DeliveryLevel` | `WEIDA_DELIVERY_LEVEL_BEST_EFFORT` 0, `_AT_MOST_ONCE` 1, `_AT_LEAST_ONCE` 2 |
| `Acknowledgement` | `WEIDA_ACKNOWLEDGEMENT_NONE` 0, `_TRANSPORT_RECEIPT` 1, `_ACCEPTED` 2, `_STORED` 3, `_REPLICATED` 4, `_PROCESSED` 5 |
| `Durability` | `WEIDA_DURABILITY_WRITTEN` 0, `_FLUSHED` 1 |
| `OrderingMode` | `WEIDA_ORDERING_MODE_NONE` 0, `_PER_PRODUCER_DETECT` 1, `_PER_PRODUCER_REASSEMBLE` 2, `_PER_KEY` 3, `_TOTAL` 4 |
| `Deduplication` | `WEIDA_DEDUPLICATION_NONE` 0, `_BOUNDED` 1, `_DURABLE` 2 |
| `Backpressure` | `WEIDA_BACKPRESSURE_BLOCK` 0, `_REJECT` 1, `_DROP` 2, `_SPILL` 3, `_COALESCE` 4 |
| `ProducerNaming` | `WEIDA_PRODUCER_NAMING_FINGERPRINT` 0, `_STABLE` 1 |

## `weida_runtime`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_owned(const weida_runtime_config *cfg_or_null, weida_runtime **out)` | never; starts the reactor's threads | 0; `-WEIDA_ERUNTIME` when `worker_threads` is 0 or the OS refuses threads; `-WEIDA_EINVAL` |
| `int weida_runtime_listener(const weida_runtime *rt, weida_listener **out)` | never | 0; `-WEIDA_EINVAL` ([listener.md](listener.md)) |
| `int weida_runtime_requester(const weida_runtime *rt, const weida_client_tls *tls, weida_requester **out)` | never | 0; `-WEIDA_EINVAL` ([requester.md](requester.md)) |
| `int weida_runtime_shutdown(weida_runtime *rt, weida_runtime_shutdown_op **out)` | never; **consumes** `rt` | 0; `-WEIDA_EINVAL` |
| `int weida_runtime_drain(weida_runtime *rt, uint64_t deadline_ms, weida_runtime_drain_op **out)` | never; **consumes** `rt` | 0; `-WEIDA_EINVAL` |
| `uint64_t weida_runtime_suppressed_duplicates(const weida_runtime *rt)` | never | duplicates discarded inside a negotiated `Deduplication::Bounded` window; 0 for NULL |
| `uint64_t weida_runtime_expired_transfers(const weida_runtime *rt)` | never | outgoing transfers reset because their deadline passed; 0 for NULL |
| `void weida_runtime_drop(weida_runtime *rt)` | never | — |

The reactor's threads, its blocking pool included, block every signal except SIGSEGV, SIGBUS,
SIGILL and SIGFPE (R12), so a process signal lands on a caller thread.

`weida_runtime_drop` releases the caller's reference only. Listeners, requesters and everything
made from them keep the reactor alive; once the last of them is gone it shuts down in the
background (R13). `weida_runtime_shutdown` closes every endpoint at once, whatever else is still
held, and waits for the connections to go idle, bounded by `shutdown_timeout`.

`weida_runtime_drain` is the counterpart of that abortive close (`Runtime::drain`, decision 0009).
First admission stops for good: no new connection, and a new inbound stream is refused with
`SHUTDOWN`. Then every finished transfer whose receipt was dropped unwaited gets until
`deadline_ms` to reach the peer's transport. Last comes the same close, within what is left of that
one deadline. It never waits for a reply or a peer application.

## `weida_runtime_shutdown_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_shutdown_op_fd(const weida_runtime_shutdown_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` when the descriptor pair cannot be made; `-WEIDA_EINVAL` |
| `int weida_runtime_shutdown_op_wait(const weida_runtime_shutdown_op *op, int64_t timeout_ms)` | up to `timeout_ms` (R4) | 0 when the shutdown finished; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` (taken or dropped) |
| `void weida_runtime_shutdown_op_drop(weida_runtime_shutdown_op *op)` | never | — |

Dropping the op without waiting does not stop the shutdown it started: the endpoints are already
closed.

## `weida_runtime_drain_op`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_drain_op_fd(const weida_runtime_drain_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` when the descriptor pair cannot be made; `-WEIDA_EINVAL` |
| `int weida_runtime_drain_op_wait(const weida_runtime_drain_op *op, int64_t timeout_ms, weida_drained **out)` | up to `timeout_ms` (R4) | 0 and the report; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` (taken or dropped) |
| `void weida_runtime_drain_op_drop(weida_runtime_drain_op *op)` | never | — |

Dropping the op unfinished aborts the drain where it is: admission, once stopped, stays stopped,
and no close runs.

## `weida_drained`

| Function | Returns |
| --- | --- |
| `uint64_t weida_drained_delivered(const weida_drained *d)` | transfers the peer's transport acknowledged before the deadline; 0 for NULL |
| `uint64_t weida_drained_outstanding(const weida_drained *d)` | transfers unacknowledged at the deadline, refused, or lost with their connection; not an error; 0 for NULL |
| `void weida_drained_drop(weida_drained *d)` | — |

Both counts are about the transfers that were still unacknowledged when the drain started.

## `weida_version`

`const char *weida_version(void)`: the library's version, e.g. `"0.1.0-alpha.11-dev"`, a
static string.
