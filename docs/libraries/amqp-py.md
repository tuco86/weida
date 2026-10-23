# AMQP 1.0 for Python (python-qpid-proton 0.40.0) - feature parity

The parity document of `weida-amqp-py`, the Python binding of the `weida-amqp` library. Its
reference implementation is **python-qpid-proton**, because that is what a Python program
that speaks AMQP 1.0 uses today; the protocol-level parity against the OASIS standard is
[`amqp.md`](amqp.md) and is not repeated here. This document answers one question per row:
*what does a Python caller who knows Proton get, and what does that caller not get.*

## 1. What a row means

Three verdicts and no others:

- **present** - implemented, with the module or test that proves it;
- **refused** - the call exists and fails at configuration time with a reason the code gives;
- **absent** - not implemented, with what is missing named.

"Partial" is not a verdict ([0013](../decisions/0013-competitor-libraries.md) §4.7 clause 6).
Where this binding's shape differs from Proton's the row says so and §9 states why.

Measured against: python-qpid-proton 0.40.0 (the `proton`, `proton.reactor`,
`proton.handlers` and `proton.utils` modules of the Apache Qpid Proton source), CPython
3.13.14, PyO3 0.29.2, `abi3` from CPython 3.9, Linux x86-64. The interop evidence of §8 is
against `fe2o3-amqp` 0.18.4, run by
`crates/amqp/weida-amqp-py/tests/test_interop.py`.

## 2. Getting a connection

| Proton | This binding | Verdict |
| --- | --- | --- |
| `proton.reactor.Container(handler).run()` | - | absent as a shape: there is no container, no reactor object and no handler class. The reactor is a Tokio runtime this connection owns, created inside `connect` and dropped with the last object that needs it |
| `Container.connect(url, ...)` | `await weida_amqp.connect(host, port, ...)` | present - `connection.rs`, `tests/test_interop.py` |
| `proton.utils.BlockingConnection(url)` | `weida_amqp.sync.connect(host, port, ...)` | present, the same client with no event loop in the process - `sync.rs`, `tests/test_sync.py` |
| `MessagingHandler.on_connection_opened` and the other 30 `on_*` callbacks | - | absent: a callback whose only job is to resume a caller is an `await` here. What arrives as an event in Proton arrives as the value of the call that asked for it |
| `Container(handler, url, reconnect=Backoff())` | - | absent: no reconnect loop, in the library or the binding ([`amqp.md`](amqp.md) §10). A connection that closes is reported, and re-dialling is the caller's |
| `proton.Url` | - | absent: `host` and `port` are two arguments. An AMQP URL carries a scheme, credentials and a path, and parsing one would decide the SASL mechanism and the TLS trust for the caller |
| `SSLDomain`, `Container(ssl_domain=...)` | - | absent from **this binding**: `weida-amqp`'s `connect_tls` takes the caller's `rustls::ClientConfig`, which is a Rust value with no Python spelling. A Python caller therefore gets TCP and SASL only, and §9.5 says what adding it would need |
| `SASL` (`sasl.allow_insecure_mechs`, `sasl.user`) | `sasl_user=`/`sasl_password=` (PLAIN), `sasl_anonymous=True` | present. EXTERNAL is absent because it means "the identity is in the TLS certificate" and there is no TLS here |
| `Container.container_id` | `container_id=` | present, and it defaults to `"weida-amqp-py"` rather than a UUID |
| `Transport.max_frame_size`, `channel_max`, `idle_timeout` | `max_frame_size=`, `channel_max=`, `idle_time_out=` | present; `idle_time_out` is **seconds or `None`**, and `None` means "advertise none" rather than "use a default" |
| - | `handshake_timeout=`, `worker_threads=` | present, and neither has a Proton counterpart: the protocol gives the handshake no deadline ([`amqp.md`](amqp.md) §8), and a reactor has to be sized by somebody |
| `connection.remote_container`, `remote_offered_capabilities` | `await connection.remote()` | present as one dict: `container_id`, `hostname`, `max_frame_size`, `channel_max`, `idle_time_out`, `offered_capabilities` |
| `Endpoint.state` bit flags (`LOCAL_ACTIVE`, `REMOTE_ACTIVE` and the products of the two) | `await connection.state()`, `await connection.is_usable()` | present as a string and a boolean. The nine local/remote combinations are not reproduced: a caller asks "can I still use this", and the answer to that is one question |

## 3. Sessions and links

| Proton | This binding | Verdict |
| --- | --- | --- |
| a session per connection, created implicitly (`SessionPerConnection`, `GlobalOverrides`) | `session = await connection.begin()` | present, and **explicit**: the session carries the frame-level window, and a client that cannot see it cannot reason about it |
| `Container.create_sender(conn, target)` | `await session.attach(name, "sender", address)` | present - `session.rs`, `link.rs` |
| `Container.create_receiver(conn, source)` | `await session.attach(name, "receiver", address)` | present |
| `AtMostOnce()`, `AtLeastOnce()` link options | `snd_settle_mode=`/`rcv_settle_mode=` by their specification names (`"settled"`, `"unsettled"`, `"mixed"`, `"first"`, `"second"`) | present, and a name outside the set is **refused** at the call with the four names in the message |
| `DynamicNodeProperties()`, `receiver(dynamic=True)` | `dynamic=True` on the asyncio surface | present; the address the peer chose is read from `await link.negotiated()`. Absent from the synchronous surface, which has no accessor for it (§9.4) |
| `Terminus.durability`, `expiry_policy`, `timeout` | `await link.negotiated()` reports what the peer said | present to read. Not settable from Python: the library takes a `Source`/`Target` value and the binding offers the address, the settle modes and `max-message-size` |
| `Filter(...)`, `Selector("...")`, `DurableSubscription()` | - | absent: the three filter families are not a standard registry ([`amqp.md`](amqp.md) §7), and the field is carried by the codec rather than named by the binding |
| `link.credit`, `link.queued`, `link.drained()` | `await link.credit()` -> `Credit(link_credit, delivery_count, available, drain)` | present, all four variables of Part 2 §2.6.7 |
| `MessagingHandler(prefetch=10)`, `FlowController` | `await link.grant_credit(n)` | present, and **explicit**: no automatic prefetch. Nothing arrives before credit is granted, which `tests/test_asyncio.py::test_nothing_arrives_before_credit_is_granted` asserts |
| `receiver.drain()` | `await link.drain()`, `await link.stop()` | present: drain asks the sender to consume the credit and report, stop withdraws it |
| `link.detach()` / `link.close()` | `await link.detach(closed=True, condition=..., description=...)` | present. `closed=False` is the detach that keeps the terminus, which is the distinction Part 2 §2.6.4 makes and Proton's two methods make too |
| `session.close()`, `connection.close()` | `await session.end()`, `await connection.close()` | present |
| `session.incoming_window`, `outgoing_window` | `await session.windows()` -> `Windows`, `await session.flow(echo=True)` | present, all six variables. Proton exposes two of them and no `flow` |
| `link.name`, `link.handle`, `link.is_sender` | `await link.name()`, `output_handle()`, `input_handle()`, `role()` | present. Both handles are readable, because the two ends number independently and a log that shows one number is ambiguous |

## 4. Messages

| Proton | This binding | Verdict |
| --- | --- | --- |
| `Message(body=..., subject=..., id=..., correlation_id=..., content_type=..., reply_to=..., durable=...)` then `sender.send(msg)` | keyword arguments of `await link.send(body, text=False, subject=..., message_id=..., correlation_id=..., content_type=..., reply_to=..., durable=...)` | present for those fields. There is no `Message` class to construct and mutate: a message is the argument list of the call that sends it |
| `Message.body` as any Python object, encoded by type guessing | `body` is `bytes` -> a **`data`** section; `text=True` makes a `str` an `amqp-value` | present, and the guessing is refused: a `str` without `text=True` is a `TypeError`, because choosing a section for somebody else's wire format is not this library's to choose |
| the other 13 `properties` fields (`user_id`, `group_id`, `absolute_expiry_time`, ...) | - | absent **to write**, present **to read**: `delivery.properties()` returns every field the peer sent. The send-side list is the seven fields a client actually sets; the codec carries all thirteen ([`amqp.md`](amqp.md) §3) |
| `Message.annotations`, `instructions`, `properties` (application) | `delivery.message_annotations()`, `delivery_annotations()`, `application_properties()`, `footer()`, `header()` | present to read, one accessor per section, decoded on demand |
| `Message.encode()` / `decode()` | `delivery.payload` | present: the octets of every section, exactly as they arrived, beside the decoded views |
| `Delivery.tag`, `.settled`, `.remote_state` | `delivery.delivery_tag`, `.settled`, `.delivery_id`, `.message_format`, `.handle` | present |
| `MessagingHandler.on_message(event)` | `await link.next_delivery()`, or `async for delivery in link` | present, and the iterator is the ordinary shape - `tests/test_asyncio.py::test_the_delivery_stream_is_an_async_iterator` |
| `BlockingReceiver.receive(timeout)` | `link.next_delivery(timeout=None)` on the synchronous surface | present, and the timeout is the synchronous surface's answer to what cancellation does on the asyncio one |

## 5. Dispositions and outcomes

| Proton | This binding | Verdict |
| --- | --- | --- |
| `sender.send(msg)` returning a `Delivery`, with `on_accepted`/`on_rejected`/`on_released` later | `outcome = await link.send(...)` | present, and this is the point: **`await send` returns the delivery's terminal state**, not a boolean and not a callback |
| `Delivery.ACCEPTED`, `REJECTED`, `RELEASED`, `MODIFIED` as integers | `Outcome` with `.name` (`"accepted"`, `"rejected"`, `"released"`, `"modified"`), comparing equal to that string | present, all four |
| `Disposition.condition` on a rejection | `outcome.error` -> `(condition, description)` | present - `tests/test_asyncio.py::test_a_rejection_arrives_as_an_outcome_with_its_reason` |
| `ModifiedDisposition(delivery_failed=..., undeliverable_here=...)` | `outcome.delivery_failed`, `.undeliverable_here`, and `await link.modify(id, ...)` to send one | present, with `None` where the peer left the flag unset rather than a defaulted `False` |
| - | `outcome.may_be_redelivered`, `outcome.increments_delivery_count` | present, and Proton has no counterpart: the two questions an application actually asks of an outcome, answered from Part 3's table rather than by the caller re-deriving them |
| `delivery.update(state)` + `delivery.settle()` | `await link.accept(id)`, `reject(id, condition, description)`, `release(id)`, `modify(id, ...)` | present, one method per terminal state |
| `Acking.accept/reject/release` over a batch | `await link.settle_range(first, last, outcome, ...)` | present: one `disposition` covering a contiguous range of delivery-ids, which is what the range is for |
| `ReceivedDisposition` (`0x23`) | - | absent as something to send, present in the codec. It is the one non-terminal state; an application that treated it as an answer would be acting on a progress report ([`amqp.md`](amqp.md) §5) |
| `sender.unsettled`, `sender.settled` counters | `await link.unsettled()`, `await link.settled(delivery_id)`, `await link.send_nowait(...)` | present: `send_nowait` returns `(delivery_id, delivery_tag, frames)` and `settled` waits for one delivery's outcome, which is the pipelined form of §5 row 1 |
| `Transaction`, `TransactionalDisposition`, `DeclaredDisposition`, `TransactionHandler` | - | absent: Part 4 transactions are not in the library ([`amqp.md`](amqp.md) §7), so there is nothing to bind |

## 6. Failures

| Proton | This binding | Verdict |
| --- | --- | --- |
| `ProtonException`, `Timeout`, `LinkDetached`, `ConnectionClosed`, `SendException` | 16 client-failure classes under `weida_amqp.AmqpError`, one per `weida_amqp::Error` variant, each carrying `errno` | present - `errors.rs`, `tests/test_sync.py::test_the_two_surfaces_raise_the_same_classes` |
| `Condition("amqp:resource-limit-exceeded", "...")` as a value to inspect | **28 exception classes**, one per condition of Part 2 §2.8.15-§2.8.18, under the same base, plus `weida_amqp.CONDITIONS` mapping each class name to its symbol | present, and this is the other point: `except weida_amqp.ResourceLimitExceeded` rather than `if condition.name == "..."` - `tests/test_asyncio.py::test_a_condition_is_its_own_exception_class` |
| a refused configuration raising at the first I/O | `weida_amqp.Configuration` at the call that configured it, naming the value | present: `max_frame_size=511` says `MIN-MAX-FRAME-SIZE`, and two SASL mechanisms at once says so, before a socket is opened |
| `proton.Interrupt`, `proton.Timeout` for a blocking call | `weida_amqp.HandshakeTimeout`, `IdleTimeout`, `WriteTimeout` (the peer stopped reading and the connection failed), and `None` from `next_delivery(timeout=...)` | present. An empty receive is a **value**, not an exception, so a caller that polls does not pay for a traceback |
| the exception a new library variant would raise | - | present by construction: the variant table is `match`ed **exhaustively** in Rust, so a variant added to `weida_amqp::Error` and not to this table is a compile error rather than a failure that quietly arrives as the base class. `weida_amqp::Error` is not `#[non_exhaustive]` for exactly this reason |

## 7. What has no Python spelling here

| Proton | This binding | Verdict |
| --- | --- | --- |
| `proton.Data`, `Described`, `Array`, `symbol`, `ulong`, `char`, `timestamp`, `decimal32` and the rest of the AMQP type wrappers | - | absent as Python types: the binding's boundary is `bytes` plus the seven message fields, and the type system stays in `weida-amqp-codec`. A caller that needs a described array builds the octets |
| `Container.listen(url)`, `Acceptor` | - | absent: `weida-amqp` is a client and holds no nodes ([`amqp.md`](amqp.md) §2). The foreign peer in §8 is a server *because* it is foreign |
| `SyncRequestResponse`, `Fetcher` | - | absent: request-reply over AMQP is a `reply_to` and a correlation-id, both of which are arguments of `send`, plus a receiver the caller attaches. A helper here would be policy |
| `Container.schedule(delay, task)`, `EventInjector` | - | absent: `asyncio.sleep` and a task are the Python spellings, and this binding does not own the loop |
| WebSocket transport | - | absent, in the library too |
| one reactor shared by several connections | - | absent: each `connect` owns a reactor sized by `worker_threads`. A client normally holds one connection; ten connections would be ten runtimes, and the sharing constructors `weida-nats-py` has are not filed here |

## 8. Interop evidence

`crates/amqp/weida-amqp-py/tests/test_interop.py`, against **`fe2o3-amqp` 0.18.4** - the peer of
the Rust interop suite, driven as a child process by `crates/amqp/weida-amqp/examples/fe2o3_peer.rs`. It needs
no broker and no C toolchain, so the tests are not skipped on a normal run; where the example
has not been built they skip with the one command that builds it.

- **A send awaited to `accepted`** against that peer's receiver, on the asyncio surface: the
  outcome object is a foreign implementation's disposition.
- **The same send on the synchronous surface**, with no event loop in the process.
- **A delivery settled from Python**: credit granted explicitly, the peer's message read with
  its sections, `accept` reaching it - the peer's own `send` returns only once that outcome
  has landed, so the assertion is the peer's, not ours.
- **14 further tests**: twelve against a scripted AMQP peer written in Python
  (`tests/broker.py`), which is where the shapes a foreign client would not produce on
  demand live - a rejection with a reason, a session's six window variables, a `close`
  carrying a condition - and two that open no socket at all, because a refused
  configuration is refused before one is opened.
- **Nothing disagreed.** The scripted test peer follows two rules the specification fixes:
  `role` is a boolean where `true` means receiver, and an answering `attach` echoes the
  client's link **name**, because that is what correlates it.

The wheel is proved separately: `crates/amqp/weida-amqp-py/package.sh` (a wrapper over
`crates/py/package.sh`) builds the `abi3`
release wheel, installs it into a fresh virtualenv, and runs one accepted transfer with
`PATH` scrubbed of `cargo`, `rustc` and `maturin`. An AMQP 1.0 client for Python that needs
no C library at install time is the one claim Proton cannot make.

## 9. Deliberate deviations

1. **No container and no handler class.** Proton's API is an event loop you hand a
   `MessagingHandler` to: `on_sendable`, `on_message`, `on_accepted` and 28 more, each a
   method on a class whose instance is the program's state. Here every one of those that
   answers a question is the return value of the call that asked it. What is lost is the
   Proton idiom; what is gained is that the state a program keeps is its own.
2. **Credit is never automatic.** Proton's `MessagingHandler(prefetch=10)` and `FlowController`
   grant credit behind the application. Nothing arrives here before `grant_credit`, because
   link credit is the receiver's statement of what it can handle
   ([0003](../decisions/0003-credit-unit.md)) and a default guesses it.
3. **The reactor is built synchronously and the handshake is awaited.** `connect` creates the
   runtime on the calling thread - nothing can be awaited before it exists - and then awaits
   the header exchange, the SASL dialog and `open` on it. A blocking connect would park the
   caller's event loop, which in a test means parking the peer it is connecting to.
4. **The synchronous surface is smaller, not different.** `sync` has no `state()`,
   `windows()`, `negotiated()`, `send_nowait`, `settled`, `modify`, `stop`, `unsettled` or
   `dynamic=`: it is the same client with the same behaviour and fewer accessors, and it
   implements no protocol behaviour of its own ([0013](../decisions/0013-competitor-libraries.md)
   §4.4 item 3). A caller that needs those reads them on the asyncio surface. `next_delivery`
   is the one place the two differ on purpose: it takes a timeout, because cancellation is
   the asyncio answer and a blocking call has none.
5. **TLS is absent from the binding, and that is a defect rather than a decision.**
   `weida-amqp` speaks TLS; its API takes the caller's `rustls::ClientConfig`, because
   trust anchors are the application's decision and a library that picks them cannot be
   audited ([`amqp.md`](amqp.md) §6). That reasoning does not survive translation: a Python
   caller has no `ClientConfig`. What it needs is a Python spelling of "these roots, this
   client certificate, this server name" - filed rather than guessed at here.
6. **Seconds, not milliseconds**, for every duration (`idle_time_out`, `handshake_timeout`,
   `timeout=`), with `None` for "none" rather than a sentinel number.
7. **A payload is `bytes`.** No `send_string`, no `send_json`, no type-guessed body: `text=True`
   is the one conversion, and it is the one the specification names (`amqp-value` of a string).

## 10. The definition of done

| Clause | Verdict | Where |
| --- | --- | --- |
| Connection, session and link are Python objects | yes, three classes plus `Delivery`, `Outcome`, `Credit`, `Negotiated`, `Windows` | §2, §3 |
| `await send` returns the delivery's terminal state | yes, an `Outcome` with four names and both redelivery questions | §5 |
| An async iterator of deliveries with sections and annotations | yes, `async for` plus seven section accessors | §4 |
| Credit granted explicitly from Python | yes, and nothing arrives before it | §3 |
| AMQP conditions as distinct exception classes | yes, 28 conditions plus 16 client failures, exhaustively matched | §6 |
| Send-to-`accepted` and a receive settled, against the `fe2o3-amqp` peer | yes, `fe2o3-amqp` 0.18.4 in both roles | §8 |
| The synchronous surface over the same client, no second implementation | yes, and the asyncio surface's send runs unchanged in it | §9.4 |
| No row says "partial" | yes: §7 and §9.5 name what a caller does not get | §7, §9 |

## 11. Sources

- Code: `crates/amqp/weida-amqp-py/src/` (`connection.rs`, `session.rs`, `link.rs`,
  `values.rs`, `sync.rs`, `errors.rs`, `lease.rs`), `crates/py/weida-py-core/src/`.
- Tests: `crates/amqp/weida-amqp-py/tests/` - `test_asyncio.py`, `test_sync.py`,
  `test_interop.py`, with `broker.py` as the scripted peer; `smoke.py` for the wheel.
- The peer: `crates/amqp/weida-amqp/examples/fe2o3_peer.rs`, and
  `crates/amqp/weida-amqp/tests/interop_fe2o3.rs` for the same peer at the Rust level.
- Proton's surface: the `proton`, `proton/_reactor.py`, `proton/_handlers.py` and
  `proton/_utils.py` modules of apache/qpid-proton, and python-qpid-proton 0.40.0 on PyPI.
- Decisions: [0014](../decisions/0014-parallel-libraries.md) §2 (one shared PyO3 foundation,
  asyncio first, the bytes boundary), [0013](../decisions/0013-competitor-libraries.md) §4.4
  (options honoured or refused, no second implementation in the binding).
- The library's own parity table: [`amqp.md`](amqp.md). The protocol sheet it cites:
  [`../research/amqp10.md`](../research/amqp10.md).
