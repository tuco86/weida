# Core NATS for Python (nats-py 2.15.0) - feature parity

The parity document of `weida-nats-py`, the Python binding of the `weida-nats` library. Its
reference implementation is **nats-py**, because that is what a Python program that speaks
NATS uses today; the protocol-level parity against the client protocol reference is
[`nats.md`](nats.md) and is not repeated here. This document answers one question per row:
*what does a Python caller who knows nats-py get, and what does that caller not get.*

## 1. What a row means

Three verdicts and no others:

- **present** - implemented, with the module or test that proves it;
- **refused** - the call exists and fails at configuration time with a reason the code gives;
- **absent** - not implemented, with what is missing named.

"Partial" is not a verdict ([0013](../decisions/0013-competitor-libraries.md) §4.7 clause 6).
Where this binding's shape differs from nats-py's the row says so and §8 states why.

Measured against: nats-py 2.15.0 (the `nats.aio.client`, `nats.aio.subscription`,
`nats.aio.msg` and `nats.errors` modules of nats-io/nats.py), CPython 3.13.14, PyO3 0.29.2,
`abi3` from CPython 3.9, Linux x86-64. **Interop against a real `nats-server` is written and not run**, so
every row below is checked against the scripted server of
`crates/nats/weida-nats-py/tests/scripted.py` and against the protocol reference - not against
a running server. The two interop tests that need one are skipped with their install command,
which is the same honesty [`nats.md`](nats.md) §10 records for the Rust side.

## 2. Getting a connection

| nats-py | This binding | Verdict |
| --- | --- | --- |
| `await nats.connect("nats://host:4222")` | `await weida_nats.connect(host, port, **options)` | present - `connection.rs`, `tests/test_asyncio.py` |
| `nats.aio.client.Client()` then `await nc.connect(...)` | - | absent: there is no unconnected connection object. Connecting reads `INFO` and answers it, so a constructor that dialled would block an event loop and one that did not would hand back something with no `max_payload` to enforce |
| a synchronous client | `weida_nats.sync.connect(host, port, **options)` | present, and nats-py has **no** synchronous surface at all: it is asyncio-only. `sync.rs`, `tests/test_sync.py` |
| `servers=[...]` with a pool and `dont_randomize` | - | absent: one host and one port. `INFO.connect_urls` is retained and readable (`info().connect_urls`), which is the whole client-visible surface of a cluster, and choosing the next address is the caller's |
| `allow_reconnect`, `reconnect_time_wait`, `max_reconnect_attempts`, `Backoff` | - | absent, in the library too: "client reconnection policy is a client-library policy, not a Core NATS wire guarantee" ([`nats.md`](nats.md) §1). What a policy needs is exposed - `closed()`, `info().connect_urls`, `lame_duck_notice()` |
| `error_cb`, `disconnected_cb`, `reconnected_cb`, `closed_cb`, `discovered_server_cb` | `await connection.closed()`, `state()`, `lame_duck_notice()` | present as values rather than callbacks: the two things a callback would report are "it ended, here is why" and "the server is draining", and both are awaitable |
| `name`, `verbose`, `pedantic`, `no_echo`, `ping_interval`, `max_outstanding_pings`, `inbox_prefix` | `name=`, `verbose=`, `pedantic=`, `echo=`, `ping_interval=`, `max_pings_out=`, `inbox_prefix=` | present, by their `CONNECT` field names. **29 keyword options** in all - every bound of the library, among them `max_queued_bytes`, `max_inbound_payload`, `max_inbound_control_line`, `max_info_line` and `write_timeout` - and an unknown one is **refused** with `weida_nats.Configuration` naming it - `tests/test_asyncio.py::test_an_unknown_connection_option_is_refused` |
| `user`/`password`/`token` | same three | present |
| `nkeys_seed`, `user_credentials`, `user_jwt_cb`, `signature_cb` | `jwt=`, `signer=`, `nkey_signer=` | present as **closures**: the nonce arrives unchanged and the caller returns the signature. No key file is read and no algorithm is chosen here, for the same reason the TLS trust store is not chosen here |
| `tls=ssl.SSLContext`, `tls_hostname`, `tls_handshake_first` | - | absent from **this binding**: `weida-nats` completes TLS where `INFO` demands it and takes the caller's `rustls::ClientConfig`, which has no Python spelling. A server whose `INFO` says `tls_required` arrives as `weida_nats.TlsRequired` rather than as a silent cleartext `CONNECT`. §8.5 says what adding it would need |
| `nc.max_payload`, `client_id`, `connected_url`, `servers`, `discovered_servers` | `await connection.max_payload()`, `info()` -> `RemoteInfo` with all fifteen `INFO` fields | present, and `INFO` is one object rather than five accessors |
| `nc.is_connected`, `is_closed`, `is_reconnecting`, `is_draining` | `await connection.state()` -> `State` with `name`, `is_usable`, `why` | present as one question with three answers (`connected`, `closed`, `failed`). The reconnecting and draining states do not exist here because neither reconnection nor drain does |
| - | `Connection.connect_current`, `Connection.connect_sharing`, `sync.connect_sharing` | present, and nats-py has no counterpart: a second connection on the first one's reactor, so ten connections are not ten thread pools |

## 3. Publish and subscribe

| nats-py | This binding | Verdict |
| --- | --- | --- |
| `await nc.publish(subject, payload, reply=None, headers=None)` | `await connection.publish(subject, payload)` and `publish_with(subject, *, reply_to=None, headers=None, payload=None)` | present, split in two because the short form is the common one |
| `await nc.subscribe(subject)` returning a `Subscription` | `await connection.subscribe(subject)` | present - `subscription.rs` |
| `await nc.subscribe(subject, queue="workers")` | `await connection.subscribe_with_queue_group(subject, queue_group)` | present, and the test asserts the group **on the wire** in the `SUB` line rather than in the object - `tests/test_asyncio.py::test_a_queue_group_travels_in_the_sub_line` |
| `cb=` push callbacks, `pending_msgs_limit`, `pending_bytes_limit`, `SlowConsumerError` | - | absent as an API shape: a subscription is pulled from, never pushed into. The queue behind it is bounded by `subscription_queue` messages and, over the whole connection, by `max_queued_bytes` octets, and a copy for a full queue is dropped and counted, which is what the server does to a client that will not read ([`nats.md`](nats.md) §5, §7). The count is `Subscription::dropped()` in Rust and is not exposed here |
| `async for msg in sub` | `async for message in subscription`, `await subscription.next()`, `subscription.try_next()` | present, all three - `tests/test_asyncio.py::test_a_subscription_is_an_async_iterator` |
| `await sub.next_msg(timeout=1.0)` | `subscription.next_msg(timeout)` on the **synchronous** surface | present. On the asyncio surface the answer to "how do I stop waiting" is cancellation, and a cancelled `next()` leaves the subscription usable - `tests/test_asyncio.py::test_a_cancelled_next_leaves_the_subscription_usable` |
| `await sub.unsubscribe(limit=0)` | `await subscription.unsubscribe()`, `unsubscribe_after(max_msgs)` | present, two methods for the two meanings, and the count is honoured by this client and not only by the server - `test_unsubscribe_after_one_ends_the_subscription` |
| `sub.subject`, `sub.queue`, `sub.delivered`, `sub.pending_msgs`, `sub.pending_bytes` | `subscription.subject()`, `queue_group()`, `sid()` | present for the first two plus the `sid` the protocol actually carries; the three counters are absent - what is delivered is what the iterator yielded, and a counter beside it would be a second source of truth |
| `await sub.drain()`, `await nc.drain()`, `drain_timeout` | - | absent: drain is a client-library convenience over `UNSUB` plus a flush, and the two pieces are here (`unsubscribe`, `flush`). What a drain adds is a policy about how long to wait for in-flight callbacks, and there are no callbacks |
| `msg.subject`, `msg.data`, `msg.reply`, `msg.headers`, `msg.sid` | `message.subject` (bytes), `subject_str`, `payload`, `reply_to`, `headers`, `sid`, `status`, `is_no_responders` | present, and richer: the `NATS/1.0` status is readable and `headers` is an **ordered list of pairs** rather than a dict, because a header name may repeat ([`nats.md`](nats.md) §3) |
| `msg.subject` as `str` | `message.subject` as `bytes`, with `subject_str` for the text | present, deliberately: a subject is a name of octets on the wire, and this binding does not decide an encoding for it. §8.6 |
| `await nc.flush(timeout)`, `nc.rtt()` | `await connection.flush()` | present. `flush` is `PING` and its `PONG`, which is the only round trip the protocol has; timing it is the caller's stopwatch |
| `msg.ack()`, `nak()`, `term()`, `in_progress()`, `metadata` | - | absent: those are JetStream acknowledgements sent as payloads to a `$JS.ACK` reply subject, and JetStream is absent (§6) |

## 4. Request-reply

| nats-py | This binding | Verdict |
| --- | --- | --- |
| `await nc.request(subject, payload, timeout=0.5)` | `await connection.request(subject, payload, timeout)` | present, and the timeout is **mandatory**: there is no default window, because the right one is the caller's and a hidden 0.5 s is a hidden failure - `tests/test_asyncio.py::test_request_is_answered` |
| `nats.errors.NoRespondersError` | `weida_nats.NoResponders`, and `weida_nats.NO_RESPONDERS == 503` | present, and kept distinct from the timeout class in the tests rather than only in the code - `test_a_503_is_no_responders_and_not_the_timeout_class` |
| `nats.errors.TimeoutError` | `weida_nats.RequestTimeout`, carrying the window that elapsed | present |
| scatter-gather | `await connection.request_many(subject, payload, timeout, max_replies)` | present, and nats-py has no counterpart: it collects what arrived inside one window, bounded by `max_replies` - `test_request_many_collects_what_arrived_in_the_window` |
| `old_style=True` request (one inbox per request) | - | absent: the new style is a single wildcard inbox subscription per connection, which is what both the reference and nats-py's default do. There is no second implementation of the pattern to keep |
| `nc.new_inbox()` | `weida_nats.INBOX_PREFIX` | present as the prefix; the token after it is built by the library and is unguessable. §8.7 says what that buys |
| `headers=` on a request | `publish_with(headers=...)` plus a subscription, and `request` carries the same block | present - `test_headers_cross_in_both_directions` |
| `no_responders` negotiation | automatic, and **refused** where the server did not offer headers: `weida_nats.HeadersUnsupported` | present - `test_no_responders_needs_headers` |

## 5. Failures

| nats-py | This binding | Verdict |
| --- | --- | --- |
| 28 classes under `nats.errors.Error` | **25 classes under `weida_nats.NatsError`**, one per `weida_nats::Error` variant, each carrying `errno` | present - `errors.rs`, `tests/test_asyncio.py::test_the_exception_family_is_the_librarys_enum` |
| `NoRespondersError`, `TimeoutError`, `StaleConnectionError`, `MaxPayloadError`, `BadSubjectError`, `AuthorizationError`, `ProtocolError`, `ConnectionClosedError`, `SecureConnRequiredError` | `NoResponders`, `RequestTimeout`, `StaleConnection`, `PayloadTooLarge`, `InvalidSubject`, `AuthenticationRequired`, `Protocol`, `ConnectionGone`, `TlsRequired` | present, name for name |
| `SlowConsumerError`, `OutboundBufferLimitError`, `DrainTimeoutError`, `ConnectionDrainingError`, `ConnectionReconnectingError`, `NotJSMessageError`, `MsgAlreadyAckdError` | - | absent because the mechanism is: no push callbacks, no drain, no reconnect loop, no JetStream |
| `InvalidCallbackTypeError`, `BadTimeoutError`, `NoServersError`, `ServerNotInPoolError` | `weida_nats.Configuration` at the call that configured it | present as one class for one kind of mistake: a value this client will not send, refused where it was given and naming the value - `tests/test_sync.py::test_a_bad_timeout_is_refused_where_it_was_given` |
| a payload above `max_payload` reaching the server | refused **before the wire**, `PayloadTooLarge` | present: the server would answer `-ERR 'Maximum Payload Violation'` and close, and one comparison keeps the connection - `test_a_payload_above_max_payload_is_refused_before_the_wire` |
| a subject the server would refuse | refused at the call, `InvalidSubject`, by the rules of [`nats.md`](nats.md) §4 | present - `test_a_subject_this_client_will_not_send_is_refused` |
| the exception a new library variant would raise | - | present by construction: the variant table is `match`ed **exhaustively** in Rust, so a variant added to `weida_nats::Error` and not to this table is a compile error rather than a failure that quietly arrives as the base class. `weida_nats::Error` is not `#[non_exhaustive]` for exactly this reason |

## 6. What has no Python spelling here

| nats-py | This binding | Verdict |
| --- | --- | --- |
| `nc.jetstream()`, `js.publish`, `js.subscribe`, `js.pull_subscribe`, the KV and object stores | - | absent: JetStream is `$JS.API` request-reply over the same twelve verbs plus acknowledgements as payloads ([`nats.md`](nats.md) §13), so it adds no verb - but it is a large API and none of it is here |
| `nats.micro` services | - | absent: a service framework over request-reply |
| WebSocket transport (`ws://`, `wss://`) | - | absent, in the library too |
| a server pool, `discovered_server_cb`, cluster failover | - | absent as behaviour, present as data: `info().connect_urls` is retained and updated by later `INFO` frames |
| `Msg.respond(payload)` | - | absent: a responder publishes to `message.reply_to`, which is one call and no hidden client reference on the message |

## 7. Interop evidence

`crates/nats/weida-nats-py/tests/` - **38 tests**, 36 of which run anywhere:

- **21 on the asyncio surface** and **15 on the synchronous one**, against the scripted server
  of `tests/scripted.py`, which *routes*: it records `SUB`, delivers a publication to every
  matching subscription by that subscription's own `sid`, answers a request nobody subscribed
  to with `NATS/1.0 503`, and honours `UNSUB <sid> <max>`. That is what makes
  "a wildcard subscription received it" an assertion about matching rather than about a
  hand-written `MSG`.
- **2 against a real `nats-server`** (`tests/test_interop.py`), **skipped**: the binary is
  absent where the suite runs and the skip carries its four install routes. Until one runs, nothing
  here is evidence that a server accepts what this client writes - only that the bytes are
  what the reference describes.
- The wheel is proved separately: `crates/nats/weida-nats-py/package.sh` (a wrapper over
  `crates/py/package.sh`) builds the `abi3`
  release wheel, installs it into a fresh virtualenv, and runs a publish/subscribe round trip,
  a request-reply over an inbox and the 503 with `PATH` scrubbed of `cargo`, `rustc` and
  `maturin`.

## 8. Deliberate deviations

1. **No callbacks.** nats-py's connection takes five of them and its subscriptions take a
   sixth. Everything a callback here would report is the value of a call: `closed()` for the
   end, `state()` for where it is, `lame_duck_notice()` for a draining server, and the
   iterator for messages. A program's state stays the program's.
2. **The timeout on `request` is mandatory.** nats-py defaults it to 0.5 s. A default window
   is a policy about somebody else's service, and the failure it produces - a `TimeoutError`
   nobody chose - looks like the network's fault.
3. **Nothing reconnects.** Neither the library nor the binding has a reconnect loop, because
   the protocol has no client session to restore: core subscriptions are connection-local and
   a new connection must re-subscribe. The three things a policy needs are exposed, and
   re-subscribing is the caller's because only the caller knows whether its subscriptions are
   still wanted.
4. **The synchronous surface is smaller, not different.** `sync` has the same methods with
   `next_msg(timeout)` in place of the async iterator, and implements no protocol behaviour of
   its own ([0013](../decisions/0013-competitor-libraries.md) §4.4 item 3). nats-py has no
   synchronous surface, so this row has no reference implementation to differ from;
   `tests/test_sync.py::test_the_two_surfaces_agree` is what keeps the two honest.
5. **TLS is absent from the binding, and that is a defect rather than a decision.**
   `weida-nats` completes TLS where `INFO.tls_required` demands it; its API takes the caller's
   `rustls::ClientConfig`, because trust anchors are the application's decision and a library
   that picks them cannot be audited. A Python caller has no `ClientConfig`, so what this
   binding needs is a Python spelling of "these roots, this client certificate, this server
   name" - filed rather than guessed at.
6. **A subject is `bytes`.** The protocol calls it a name and never a string; `subject_str`
   decodes it where it is UTF-8. The same rule makes a payload `bytes` with no `send_string`
   and no JSON helper.
7. **The inbox token is unguessable, not merely unique.** It is 128 random bits from the
   operating system's generator, hex encoded, under `inbox_prefix`, with a per-request counter
   after it. nats-py's `new_inbox()` is a random token too, and for the same reason: the reply
   subject is the only thing that decides who may answer a request, and any client in the
   account that can publish to `_INBOX.>` - in the default account, every client - could
   otherwise forge the reply to a guessable one ([`nats.md`](nats.md) §5). An application that
   wants its inboxes under a prefix of its own, so that a server permission can be written
   against it, sets `inbox_prefix`.
8. **`try_next` raises `BlockingIOError` when another coroutine holds the subscription**,
   rather than returning `None`: "no message" and "somebody else is reading" are different
   facts, and `BlockingIOError` is Python's own `EAGAIN`.

## 9. The definition of done

| Clause | Verdict | Where |
| --- | --- | --- |
| Connect, publish with optional headers, subscribe | yes | §2, §3 |
| An async iterator of messages with subject, `sid`, reply-to and payload | yes, plus the status and the header list | §3 |
| Queue-group membership, asserted on the wire | yes | §3 |
| `unsubscribe` with a message count | yes, honoured client-side | §3 |
| `request` with a mandatory timeout that raises rather than hangs | yes, and the 503 is its own class | §4 |
| The no-responder status among the exception classes | yes, `NoResponders` beside `RequestTimeout` | §4, §5 |
| The synchronous surface over the same client, no second implementation | yes, `next_msg(timeout)` where the other has cancellation | §8.4 |
| The round trip against a supervised `nats-server` | **not met**: `nats-server` is not run, the two interop tests skip with their install command, and nothing here claims a server accepted it | §7 |
| No row says "partial" | yes: §6 and §8.5 name what a caller does not get | §6, §8 |

## 10. Sources

- Code: `crates/nats/weida-nats-py/src/` (`connection.rs`, `subscription.rs`, `values.rs`,
  `options.rs`, `sync.rs`, `errors.rs`, `lease.rs`), `crates/py/weida-py-core/src/`.
- Tests: `crates/nats/weida-nats-py/tests/` - `test_asyncio.py`, `test_sync.py`,
  `test_interop.py`, with `scripted.py` as the routing server; `smoke.py` for the wheel.
- nats-py's surface: `src/nats/aio/client.py`, `src/nats/aio/subscription.py`,
  `src/nats/aio/msg.py` and `src/nats/errors.py` of nats-io/nats.py, and nats-py 2.15.0 on
  PyPI.
- Decisions: [0014](../decisions/0014-parallel-libraries.md) §2 (one shared PyO3 foundation,
  asyncio first, the bytes boundary), [0013](../decisions/0013-competitor-libraries.md) §4.4
  (options honoured or refused, no second implementation in the binding).
- The library's own parity table: [`nats.md`](nats.md). The protocol sheet it cites:
  [`../research/nats.md`](../research/nats.md).
