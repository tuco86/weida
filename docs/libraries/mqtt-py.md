# MQTT 5 for Python (paho-mqtt 2.x) — feature parity

The parity document of `weida-mqtt-py`, the Python binding of the `weida-mqtt` MQTT 5.0
client, built on `weida-py-core`. Its reference implementation is **paho-mqtt**, because that
is what a Python program that speaks MQTT uses today; the protocol-level parity against MQTT
5.0 itself is [`mqtt.md`](mqtt.md) and is not repeated here — its §2 (the fifteen packet
types), §3 (the 27 properties), §4 (QoS), §5 (filters and subscription options), §6 (retained
messages and the Will) and §7 (the reason-code groups) are the rows a *protocol* reader wants.
This document answers one question per row: *what does a Python caller who knows paho-mqtt
get, and what does that caller not get.*

## 0. Why there is no server column

The same reason [`mqtt.md`](mqtt.md) §0 gives, one layer up: **MQTT's topology is strictly
asymmetric and this is a client**. Retained storage, the session store, subscription routing,
shared-subscription selection and the Will's delay timer are all the server's, a server is a
broker and so outside this library ([0014](../decisions/0014-parallel-libraries.md) §2), and
there is no `weida_mqtt.Broker`, as the module documentation of
`crates/mqtt/weida-mqtt-py/src/lib.rs` says. paho-mqtt is a client too, so the two columns below line up: neither has a
server half, and a row about server behaviour belongs in [`mqtt.md`](mqtt.md) §6 and §10.

## 1. What a row means

Three verdicts and no others:

- **present** — implemented, with the module or test that proves it;
- **refused** — the call exists and fails at configuration time with a reason the code gives;
- **absent** — not implemented, with what is missing named.

"Partial" is not a verdict ([0013](../decisions/0013-competitor-libraries.md) §4.7 clause 6).
Where this binding's shape differs from paho's the row says so and §9 states why.

**What this side was read from.** Every verdict below names a module under
`crates/mqtt/weida-mqtt-py/src/` or a test function under `crates/mqtt/weida-mqtt-py/tests/`,
and was read there. CPython 3.9 and up through PyO3 0.29 with `abi3-py39`
(the `pyo3` entry of the workspace `Cargo.toml`, `requires-python` in
`crates/mqtt/weida-mqtt-py/pyproject.toml`), the
library entered with `default-features = false` plus `blocking`
(the `weida-mqtt` dependency of `crates/mqtt/weida-mqtt-py/Cargo.toml`), Linux x86-64.

**Which broker the tests run against.** One: **`rumqttd` 0.20.0** on `127.0.0.1:21884`, the
listener of `crates/mqtt/weida-mqtt/tests/interop/rumqttd.toml`. `tests/conftest.py`
probes that port once per session and skips with the `cargo install rumqttd --version 0.20.0`
command where nothing answers — the Python half of what `#[ignore]` does for the Rust suites.
The port is deliberately not 1883, so a developer's own broker cannot be measured by accident.
The second broker of [`mqtt.md`](mqtt.md) §1, `rmqtt` 0.23.1, is **not** used by the Python
suite: no test in `crates/mqtt/weida-mqtt-py/tests/` names it.

**Which paho version.** None. `paho` does not occur anywhere under `crates/`, nothing in
`pyproject.toml`, `develop.sh` (a wrapper over `crates/py/develop.sh` that installs `maturin`,
`pytest` and `pytest-asyncio` and nothing else), `tests/conftest.py` or `tests/test_interop.py`
pins or imports it, and no test in this repository runs against it. The paho column below is
therefore **the documented public API of paho-mqtt 2.x, not a measurement**, and it is the one
column of this document that cannot be checked out of the repository — see §8 and the open
question in §9.7.
Where a claim about paho's *behaviour* rather than its API names appears, it is one the
research sheet already carries with a citation:
[`../research/mqtt5.md`](../research/mqtt5.md) §13 and its references [37] and [38].

## 2. The client surface

paho is one `Client` object with a callback for every event. This binding is a context, a
client and a delivery stream — the three handles §9.2 measures the two surfaces by — plus a
`Session` the caller holds and six value classes. **13 `#[pyclass]`es** in all: ten in
`weida_mqtt` and three in `weida_mqtt.sync`.

| paho-mqtt 2.x | This binding | Verdict |
| --- | --- | --- |
| `mqtt.Client(CallbackAPIVersion.VERSION2, client_id, protocol=MQTTv5)` | `weida_mqtt.Context()` then `await context.connect(address, options)` → `(client, events)` | present — `client.rs:46-166`. There is no protocol argument: 5.0 or nothing (`mqtt.md` §2) |
| `client.connect(host, port, keepalive)` / `connect_async` | `await context.connect("host:port", options)` | present — `tests/test_interop.py::test_the_whole_round_trip_from_python`. One address string, and the coroutine *is* the async form, so there is no second `_async` spelling |
| `client.reconnect()`, `reconnect_on_failure`, `reconnect_delay_set` | — | absent: no reconnect loop anywhere. A reconnect is a new `context.connect_session(address, session, options)` with the `Session` the caller holds, which is what makes it a *resumption* — `client.rs:125-161`, `tests/test_interop.py::test_a_session_survives_a_reconnect`. §9.5 |
| the client object as the session | `weida_mqtt.Session`, a separate object | present, and it is the deviation: 4.1's client-side session state outlives every connection, so it is its own class with `client_id`, `in_flight`, `send_quota`, `receive_maximum`, `subscriptions` and `matching(topic)` — `client.rs:206-305` |
| `client.publish(topic, payload, qos, retain, properties)` → `MQTTMessageInfo` | `await client.publish(weida_mqtt.Message(...))` → `Completion` | present — `client.rs:332-342`. The await *is* `MQTTMessageInfo.wait_for_publish()`, and the QoS 2 case resolves on the PUBCOMP: `tests/test_interop.py::test_a_qos_2_publish_is_awaited_to_its_pubcomp` |
| `MQTTMessageInfo.rc`, `.mid`, `.is_published()`, `.wait_for_publish()` | `Completion.kind` ∈ `sent`/`acknowledged`/`complete`/`refused`, `.reason_code`, `.accepted` | present, and **narrower on purpose**: the three QoS levels certify three different things, so the kind is a string a caller compares rather than a boolean — `values.rs:431-496`. `mid` is not on it; a resumed exchange's identifier arrives with `Events.next_event` |
| `client.subscribe(topic, options=SubscribeOptions(...))` → `(rc, mid)` | `await client.subscribe([Subscription(filter, qos, ...)])` → list of granted codes | present — `client.rs:352-371`. The granted codes come back from the SUBACK itself, one per filter in order, rather than arriving later in `on_subscribe`; `granted == [2]` is the assertion in `test_the_whole_round_trip_from_python` |
| `paho.mqtt.subscribeoptions.SubscribeOptions(qos, noLocal, retainAsPublished, retainHandling)` | `weida_mqtt.Subscription(filter, qos, *, no_local, retain_as_published, retain_handling)` | present, all four options of 3.8.3.1 — `values.rs:348-393`, `tests/test_values.py::test_the_four_options_of_3_8_3_1`. Retain Handling 3 is **refused** with "it is a Protocol Error to send" rather than being a value — `test_retain_handling_three_is_a_protocol_error_and_not_a_value` |
| the Subscription Identifier as a `Properties` field on `subscribe` | `subscribe(..., subscription_id=42)` | present — `client.rs:352-358`, `tests/test_interop.py::test_a_subscription_identifier_names_the_filter_that_matched` |
| `client.unsubscribe(topic)` | `await client.unsubscribe([filter, ...])` → list of codes | present — `client.rs:377-390`. A count that disagrees with the filters sent raises rather than being zipped short: §8's measured disagreement |
| `client.disconnect(reasoncode, properties)` | `await client.disconnect(reason_code=0x00, *, session_expiry=None)` | present — `client.rs:400-425`, with `weida_mqtt.NORMAL_DISCONNECTION` and `DISCONNECT_WITH_WILL_MESSAGE` named rather than left as bytes (`lib.rs:125-130`). A byte that is not a DISCONNECT code is a `ValueError` at the call |
| `client.on_message` callback | `async for delivery in events:` and `await events.next_event()` | present as an iterator — `client.rs:594-686`, `tests/test_interop.py::test_the_async_iterator_yields_deliveries`. The synchronous mirror is `for delivery in deliveries:` and `deliveries.recv(timeout=...)` — `sync.rs:276-341` |
| `MQTTMessage` (`.topic`, `.payload`, `.qos`, `.retain`, `.dup`, `.mid`, `.properties`, `.timestamp`, `.state`) | `weida_mqtt.Delivery` (`.topic`, `.payload`, `.qos`, `.retain`, `.dup`, `.packet_id`, `.subscription_ids`, `.origin()`, `.properties`) | present — `values.rs:196-333`. `.timestamp` and `.state` are absent: the first is `time.time()` at the caller's line, the second is paho's own queue bookkeeping. `.origin()` has no paho counterpart at all and is the honest answer to "was this from the cache", including `"unknowable"` under Retain As Published 1 |
| a **publish** and a **delivery** being the same `MQTTMessage` class | `Message` for what a publisher sets, `Delivery` for what a subscriber reads | present as two classes, deliberately: `dup` and `packet_id` are on no publish, and `retain` is an instruction on one and an observation on the other — `values.rs:1-13` |
| `paho.mqtt.properties.Properties(PacketTypes.PUBLISH)` with `.MessageExpiryInterval = 60` | keyword arguments on `Message` — ten of them, `values.rs:89-101` | present: the ten fields of an Application Message are named arguments checked at construction, not attributes set on a bag that is validated at publish time. What arrives is `delivery.properties`, a **dict of eight keys**, the last of which, `truncated`, says that the PUBLISH carried more `User Property` pairs or Subscription Identifiers than the library keeps (the first are kept and the delivery arrives) — `values.rs:291-321` |
| `Properties` on CONNECT / DISCONNECT / SUBSCRIBE | keyword arguments on `ConnectOptions` (§5) and `disconnect(session_expiry=...)`; SUBSCRIBE properties are `subscription_id` | present for every property this client sends, refused where the protocol forbids the combination — §5 |
| `paho.mqtt.reasoncodes.ReasonCode` as a value with `.value`, `.getName()`, `.is_failure` | reason codes are **exception classes** — 50 of them under `weida_mqtt.MqttError`, each with `errno`, `cause` and `reason_code` | present as classes, which is the largest single difference in this table. §9.3 |
| `paho.mqtt.enums.MQTTErrorCode`, `MQTTProtocolVersion`, `CallbackAPIVersion` | — | absent: there is one protocol version, no callbacks and therefore no callback-API version, and a failure is a class rather than an `rc` |
| `client.user_data_set(...)`, `userdata` on every callback | — | absent: there are no callbacks to hand state to. A coroutine that owns the `Events` already owns its own state |
| a `str` payload | refused | `payload_of` takes `bytes` and refuses a `str`, because Payload Format Indicator is a hint a publisher sets and not an encoding the protocol applies — `values.rs:15-22`, `tests/test_values.py::test_a_str_payload_is_refused` |
| — | `weida_mqtt.sync.Context`, `sync.Client`, `sync.Deliveries` | present, and paho has no counterpart shape: this is the same client with no event loop in the process, over the library's own `blocking` module — `sync.rs`, `tests/test_sync.py::test_no_event_loop_exists_in_this_process`. §9.2 |

## 3. Transports

| paho-mqtt 2.x | This binding | Verdict |
| --- | --- | --- |
| `transport="tcp"`, port 1883 | `context.connect("127.0.0.1:1883", options)` | present — `client.rs:97-123`, and every interop test runs over it |
| `client.tls_set(ca_certs=..., certfile=..., keyfile=...)`, `tls_set_context(ssl.SSLContext)`, `tls_insecure_set` | — | **absent by construction.** The library has TLS behind a default-on `tls` feature and this binding turns it off (the `weida-mqtt` dependency in `Cargo.toml`), because `weida_mqtt::ConnectOptions::tls` takes the caller's `rustls::ClientConfig` (`crates/mqtt/weida-mqtt/src/options.rs:124-133`) and this surface has no way to accept one from Python. A binding that linked a TLS stack it could not configure would be linking it for nothing. §9.1 |
| `client.socket()` after a TLS connect, to ask what protected the credentials | `client.is_encrypted` → **always `False`**, and says so | present as an honest answer rather than an absent attribute — `client.rs:459-471`, `sync.rs:259-264`, asserted at `tests/test_interop.py::test_the_connack_reaches_python` (`assert client.is_encrypted is False`). The User Name, the Password and every byte of Authentication Data travel in the CONNECT, so this is the only question a client can ask about what protected them, and the answer here is "nothing" |
| `transport="websockets"`, `ws_set_options(path, headers)`, `wss` | — | absent, in the library too: an HTTP upgrade and a framing layer `weida-mqtt` has no dependency for ([`mqtt.md`](mqtt.md) §8). A caller who needs it wants a WebSocket client plus `weida-mqtt-codec`, which is sans-I/O so that the composition is possible |
| `transport="unix"` | — | absent: not in the specification, and brokers that offer one differ on the address syntax ([`mqtt.md`](mqtt.md) §8) |
| `client.proxy_set(proxy_type=...)` | — | absent: SOCKS/HTTP proxying is `PySocks` under paho's socket, and there is no socket to swap here — the transport is the library's |
| MQTT over QUIC | — | absent: not a standard, one vendor's extension ([`mqtt.md`](mqtt.md) §8) |

## 4. Mechanisms and authentication

| paho-mqtt 2.x | This binding | Verdict |
| --- | --- | --- |
| `client.username_pw_set(username, password)` | `ConnectOptions(user_name=..., password=...)` | present — `options.rs:180-204`. The password is `bytes` through `payload_of`, and 5.0 permits one with no user name |
| `Properties.AuthenticationMethod` / `AuthenticationData` on CONNECT | `ConnectOptions(authentication_method=..., authentication_data=...)` | present, and the pair is checked where it is written: data without a method is a Protocol Error ([MQTT-3.1.2-33]) refused as a `ValueError` naming the option — `options.rs:234-240`, `tests/test_values.py::test_an_unusable_option_is_refused_where_it_is_written` |
| a multi-step 4.12 AUTH exchange (paho reports the server's AUTH to `on_connect`/callbacks and the application answers) | — | **absent from Python.** The library takes an `Arc<dyn Authenticator>` through `Client::connect_with` (`crates/mqtt/weida-mqtt/src/client.rs:303-311`), and this binding never passes one: both constructors go through `Client::connect`/`connect_session`, which install `NoAuthenticator` (`crates/mqtt/weida-mqtt/src/client.rs:289-290`). A server that answers the CONNECT with `Continue authentication` therefore gets `UnexpectedPacket` rather than a reply, and a CONNACK that carries final `Authentication Data` is refused as `AuthenticationFailed`, because nothing here can check a server's proof (`crates/mqtt/weida-mqtt/src/connection.rs:145-164`). What is missing is a Python spelling of the challenge callback — a `challenge(data: bytes | None) -> bytes` handed to `connect` |
| — | `await client.reauthenticate()` | present as a call and **unusable without the callback above**: with no `Authentication Method` on the connection it raises `Configuration` (`crates/mqtt/weida-mqtt/src/client.rs:517-520`, surfaced at `client.rs:439-444`), and with one it reaches `NoAuthenticator.challenge` and raises `UnexpectedPacket`. Named here rather than left for a caller to discover |
| TLS client certificates as an identity | — | absent with §3's reason, and there would be nothing to read it back with: MQTT has no per-message identity, so no accessor on `Delivery` returns one ([`mqtt.md`](mqtt.md) §8) |
| `client.enable_bridge_mode()` | — | absent: bridge mode is 3.1.1's `0x80|version` handshake plus broker-side loop prevention, and this client speaks 5.0 only |

## 5. Options

Everything a CONNECT declares is one frozen `weida_mqtt.ConnectOptions`, built with **19
keyword arguments** (`options.rs:152-173`), and every refusal happens **at construction**
rather than on the wire: `ConnectOptions.new` ends in `inner.validate()` and turns the
library's message into a `ValueError` that names the option, "because a `ValueError` at the
line that built the options names the option, and a Protocol Error from the broker names a
byte" (`options.rs:1-8`, `234-240`).

| paho-mqtt 2.x | This binding | Verdict |
| --- | --- | --- |
| `client_id=` on the constructor | `ConnectOptions(client_id)`, empty asks the server to assign one | present — `options.rs:137-141`, read back as `Client.client_id` (`client.rs:446-451`) |
| `clean_session=` (3.1.1) / `clean_start=` on `connect` | `clean_start=True` by default | present — `options.rs:249-254`. There is no `clean_session`: this client speaks 5.0 |
| `keepalive=60` seconds | `keep_alive=60.0` seconds, `0` disables the mechanism | present — `options.rs:143-145`, `tests/test_values.py::test_keep_alive_zero_is_legal_and_means_off`. What is *in force* after CONNACK is `Client.keep_alive`, which is the server's `Server Keep Alive` where it sent one |
| `Properties.SessionExpiryInterval` on CONNECT and on DISCONNECT | `session_expiry=` seconds, and `disconnect(session_expiry=...)` | present — `options.rs:199`, `client.rs:400-420`. Seconds as a float everywhere, and a negative one is refused: `test_a_negative_interval_is_refused` |
| `client.will_set(topic, payload, qos, retain, properties)` | `weida_mqtt.Will(topic, payload, *, qos, retain, delay, message_expiry, content_type, response_topic, correlation_data)` passed as `will=` | present, nine arguments — `options.rs:42-96`. A Will Topic is a Topic **Name**: `tests/test_values.py::test_a_will_topic_is_a_topic_name` |
| `client.will_clear()` | `will=None`, which is the default | present: the Will lives in the CONNECT, so there is nothing to clear on a live connection — clearing it is not sending it |
| `Properties.ReceiveMaximum` | `receive_maximum=` | present — `options.rs:214-216`, and it is **this client's own** declaration, a different number from the server's: `tests/test_interop.py::test_the_send_quota_is_the_servers_receive_maximum` asserts both side by side ([`mqtt.md`](mqtt.md) §9.2) |
| `Properties.MaximumPacketSize` | `maximum_packet_size=` | present — `options.rs:217-219` |
| `Properties.TopicAliasMaximum`; paho's `client.publish` never sends an alias | `topic_alias_maximum=`, and the library keeps both alias tables | present — `options.rs:220-222`; aliases are resolved before a delivery reaches Python, so `Delivery.topic` "is never empty and never a number" (`values.rs:210-215`) |
| `Properties.RequestResponseInformation` / `RequestProblemInformation` | `request_response_information=False`, `request_problem_information=True` | present, with the protocol's own defaults — `options.rs:169-170` |
| `Properties.UserProperty` on CONNECT | `user_properties=[(name, value), ...]` | present, a list of pairs so repeats and order survive — `options.rs:206` |
| `client.max_inflight_messages_set(n)` | — | absent as a keyword: the in-flight bound is the smaller of the **server's** `Receive Maximum`, read from CONNACK, and the library's own `Limits::max_inflight` (1024, with no keyword; the `Limits` row below), and exhausting it stalls the publish rather than queueing it locally (`client.rs:328-331`). `Session.send_quota` is what is left |
| `client.max_queued_messages_set(n)` (outbound queue) | — | absent: nothing is queued for a disconnected client. A publish with no connection raises `NotConnected` — `tests/test_interop.py::test_a_use_after_disconnect_is_not_connected` |
| `client.message_retry_set(seconds)` | — | absent, and the absence is the library's rule: retransmission happens once, just after a CONNACK with `Session Present` 1, and there is no retry timer anywhere ([`mqtt.md`](mqtt.md) §4) |
| — | `max_subscription_ids=`, `incoming_queue=` | present, and paho has no counterpart: the two resources MQTT bounds nowhere ([`mqtt.md`](mqtt.md) §9.1) are caller arguments — `options.rs:223-228`. `incoming_queue` is also held to a byte budget of 8 MiB that has no keyword (the `Limits` row below) |
| — | `connect_timeout=`, `ping_timeout=` | present, and paho has neither as a number the caller sets: the specification puts no deadline on the CONNACK and none on a missing PINGRESP, and an unbounded wait is a hang — `options.rs:229-232` |
| `Limits::max_addresses` (the library's cap on a resolver's answer), `Limits::max_user_properties` (its cap on `User Property` pairs per delivery), `Limits::incoming_queue_bytes` (the delivery queue's byte budget), `Limits::max_inflight` and `Limits::max_inflight_bytes` (the unacknowledged QoS 1 and 2 messages, counted and in bytes), `Limits::max_alias_bytes` (the Topic Alias tables) | — | absent from the Python surface: these are the six `Limits` fields with no keyword argument (`crates/mqtt/weida-mqtt/src/limits.rs` against `options.rs:214-228`), so a Python caller gets the library's defaults of 8, 64, 8 MiB, 1024, 8 MiB and 256 KiB and cannot change any of them. All stay out of the pyo3 signature deliberately |
| `client.enable_logger()`, `on_log` | — | absent — §6 |
| `client.manual_ack=True`, `client.ack(mid, qos)` | — | absent: acknowledgements are the connection task's, and a delivery reaching the iterator has already been acknowledged at its QoS. What paho's manual ack buys — backpressure — is `incoming_queue`, which stops the reader rather than delaying an ack |

## 6. Observability

| paho-mqtt 2.x | This binding | Verdict |
| --- | --- | --- |
| `on_connect(client, userdata, flags, reason_code, properties)` | the CONNACK's contents as `Client.server`, a **dict of 14 keys** | present — `client.rs:502-553`, `tests/test_interop.py::test_the_connack_reaches_python`. Every declaration of 3.2.2 with §11's defaults applied to what the server left out, which is what makes an absent property safe to read |
| `flags["session present"]` | `Client.session_present` | present — `client.rs:453-457` |
| the assigned Client Identifier out of `properties` | `Client.client_id`, and `server["assigned_client_identifier"]` | present — `client.rs:446-451` |
| `Properties.ResponseInformation` on CONNACK | `Client.response_information`, `Client.response_topic(suffix)` | present, and `None` is an answer rather than an omission: without the property there is no protocol-level way to learn which topics this client may reply on — `client.rs:483-500` |
| `on_disconnect(..., reason_code, ...)` | the failure the iterator raises | present, and it is the distinction 5.0 exists to make: a server DISCONNECT arrives as the class named for its reason code, an orderly end as `StopAsyncIteration` — `client.rs:606-637`, `errors.rs:253-282` |
| `on_publish`, `on_subscribe`, `on_unsubscribe` | the awaited return values | present as values: `Completion` from `publish`, the granted codes from `subscribe`, the codes from `unsubscribe`. A resumed exchange completing has no caller left to resolve and arrives at `await events.next_event()` as `("completed", (packet_id, Completion))`; a stored message that the server's `Maximum Packet Size` no longer admits is released and arrives as `("abandoned", (packet_id, cause))` — `client.rs:639-681` |
| `on_log(client, userdata, level, buf)`, `enable_logger(logger)` | — | absent: no logging hook and no `tracing` bridge into Python's `logging`. What a log line would carry — the reason a call failed — is on the exception (`errno`, `cause`, `reason_code`), and what a wire trace would carry is not exposed at all |
| `client.is_connected()`, `socket()`, `want_write()` | — | absent: they are paho's network-loop API, and the loop is the library's. The question "is it still up" is answered by the next call raising `NotConnected` or by the iterator ending |
| — | `Session.in_flight`, `send_quota`, `receive_maximum`, `subscriptions`, `matching(topic)` | present, and paho has no counterpart: the client-side session state of 4.1 is readable rather than internal — `client.rs:236-295` |
| — | `Delivery.origin(retain_as_published)` → `"retained"`/`"live"`/`"unknowable"` | present, and the third value is the point: under Retain As Published 1 the flag is the publisher's, so a cached copy is indistinguishable from a live one and the type says so — `values.rs:266-283`, asserted both ways in `tests/test_interop.py::test_a_retained_value_is_stored_and_then_deleted` |

## 7. Helpers

| paho-mqtt 2.x | This binding | Verdict |
| --- | --- | --- |
| `paho.mqtt.publish.single(topic, payload, hostname=...)` | — | absent: connect, publish, disconnect is three awaited lines here, and the helper's value in paho is hiding the network loop. There is no loop to hide |
| `paho.mqtt.publish.multiple(msgs, hostname=...)` | — | absent, same reason; a list of `await client.publish(...)` calls on one connection is the shape, and `asyncio.gather` runs them together |
| `paho.mqtt.subscribe.simple(topics, hostname=..., msgCount=1)` | — | absent: `async for delivery in events: break` is it, and `tests/test_interop.py::test_the_async_iterator_yields_deliveries` is that loop |
| `paho.mqtt.subscribe.callback(callback, topics, ...)` | — | absent as an API shape: deliveries are pulled from an iterator, never pushed into a callback (§9.4) |
| `client.loop()`, `loop_start()`, `loop_stop()`, `loop_forever()`, `loop_read/write/misc` | — | absent **by construction**: the connection is driven by a Rust task on the reactor the `Context` owns, so there is no network loop for Python to run, start or forget to start. This is the single largest simplification of the surface |
| `paho.mqtt.client.topic_matches_sub(sub, topic)` | `weida_mqtt.matches(filter, topic)` | present — `lib.rs:161-164`, `tests/test_values.py::test_the_specifications_own_worked_examples`, including `sport/#` matching bare `sport` and [MQTT-4.7.2-1]'s `$` rule |
| — | `weida_mqtt.check_topic_filter(filter)`, `check_topic_name(topic)` | present, and paho has no counterpart: the grammar of 4.7.1 and [MQTT-3.3.2-2] as two functions that raise `ValueError` naming the rule — `lib.rs:173-188`, `tests/test_values.py::test_the_grammar_of_4_7_1`, `test_a_topic_name_is_not_a_pattern` |
| — | `weida_mqtt.Message.delete_retained(topic, qos=...)` | present: the RETAIN 1 zero-byte delete as a named constructor, "because `Message(topic, b"", retain=True)` reads like an oversight" — `values.rs:139-157` |
| — | six module constants: `NORMAL_DISCONNECTION`, `DISCONNECT_WITH_WILL_MESSAGE`, `DEFAULT_RECEIVE_MAXIMUM`, `DEFAULT_TOPIC_ALIAS_MAXIMUM`, `MAX_TOPIC_BYTES`, `SHARE_PREFIX` | present — `lib.rs:125-143` |
| paho's persistence (`MQTTClientPersistence`-style stores in the C and Java families) | — | absent: `Session` is in memory, as paho Python's is ([`../research/mqtt5.md`](../research/mqtt5.md) §13 [38]). The consequence is not hidden: a restart that reconnects with `clean_start=False` against a broker that kept the session raises `SessionPresentWithoutState`, and the answer is `clean_start=True` — `client.rs:199-205` |

## 8. Interop evidence

**45 pytest functions** in `crates/mqtt/weida-mqtt-py/tests/`: 12 in `test_interop.py`, 12 in
`test_sync.py` and 21 in `test_values.py`. **23** of them ask for the `broker` fixture and
skip with its install command where the port does not answer (`conftest.py`); the other
**22** — the 21 in `test_values.py` and `test_sync.py::test_no_event_loop_exists_in_this_process`,
which asserts the condition every other test in that file runs under — need nothing.

**Against `rumqttd` 0.20.0, this client on both sides of the broker** (`test_interop.py`):

- the QoS 2 publish awaited to its **PUBCOMP**, with QoS 0 and QoS 1 beside it so the three
  completions are distinguishable rather than asserted one at a time
  (`test_a_qos_2_publish_is_awaited_to_its_pubcomp`);
- the round trip with every PUBLISH property read back off the delivery — content type,
  response topic, correlation data and a User Property pair
  (`test_the_whole_round_trip_from_python`);
- the async iterator over three deliveries (`test_the_async_iterator_yields_deliveries`);
- a Subscription Identifier reported on the delivery it caused, plus the local-matcher
  fallback for a server that declares none
  (`test_a_subscription_identifier_names_the_filter_that_matched`);
- a retained topic through all three states including the zero-byte delete, and the
  `origin()` of a cached copy against a live one
  (`test_a_retained_value_is_stored_and_then_deleted`);
- session resumption across two connections on one `Session`
  (`test_a_session_survives_a_reconnect`), and the send quota being the *server's* number
  (`test_the_send_quota_is_the_servers_receive_maximum`);
- the CONNACK's declarations in Python, including `is_encrypted is False`
  (`test_the_connack_reaches_python`);
- three refusals that never reach the wire — an illegal filter, Subscription Identifier 0, an
  empty SUBSCRIBE (`test_a_refusal_carries_the_code_the_server_would_have_sent`);
- `NotConnected` after a DISCONNECT, and a **cancelled publish leaving the client usable**
  (`test_a_cancelled_publish_leaves_the_client_usable`).

**The synchronous surface against the same broker** (`test_sync.py`): the QoS 2 round trip
with the `await`s removed and nothing else, the blocking iterator, the receive deadline where
cancellation goes, the GIL released while blocked (asserted by a *count* a second thread
raises), one thread publishing while another consumes, and the same refusal class arriving
from both surfaces (`test_a_refusal_is_the_asynchronous_surfaces_refusal`).

**One measured disagreement.** rumqttd 0.20.0 answers a
two-filter UNSUBSCRIBE with **one** reason code where [MQTT-3.11.3-1] requires one per filter
in order. Position is the only thing binding a code to a filter, so a count that disagrees is
unreadable rather than merely odd, and this client raises `AcknowledgementLengthMismatch`
instead of zipping short and telling the caller the second filter was removed with the first
filter's code. One filter per UNSUBSCRIBE interoperates, and the second half of
`test_an_unsubscribe_length_mismatch_is_reported_and_not_guessed` shows it. This is the sixth
entry of [`mqtt.md`](mqtt.md) §10's disagreement list and the only one raised by the Python
suite.

**The wheel, with no Rust toolchain.** `package.sh` (a wrapper over `crates/py/package.sh`)
builds the `abi3` release wheel, refuses
one whose name is not `abi3`, installs it into a fresh temporary virtualenv and runs
`smoke.py` with `PATH` scrubbed to `/usr/bin:/bin` — so "the wheel needs no Rust toolchain" is
established rather than expected. `smoke.py` checks the eleven names of
the module surface, both import spellings of `weida_mqtt.sync`, eleven exception classes under
one base, the matcher and the grammar, the two refusals of Retain Handling 3 and QoS 3, and a
connect to a closed port raising a *named* failure rather than hanging.

**Paho is not run.** No test in this repository imports paho-mqtt, and no
pairing between this binding and paho is measured — in either role. Everything in the
paho column of this document is that library's documented API, and nothing here is evidence
that paho and this client agree on the wire. What would close it is the shape
[`zmq-py.md`](zmq-py.md) §8 has: a `test_interop_paho.py` publishing from one and subscribing
with the other, both ways, skipped with `pip install paho-mqtt` where the package is absent.

**The numbers are counted, not run.** The counts above were counted out of the tree with the
commands in §11; the last recorded execution passed 45 tests with rumqttd under the
supervisor, which matches the 45 functions the tree holds.

## 9. Deliberate deviations and bounds

### 9.1 TLS is absent by construction, not unfinished

The library implements TLS behind a default-on `tls` feature, and this binding enters it with
`default-features = false` (the `weida-mqtt` dependency in `Cargo.toml`). The reason is one line of the library's API:
`ConnectOptions::tls` takes the caller's `rustls::ClientConfig`
(`crates/mqtt/weida-mqtt/src/options.rs:124-133`), because which certificates an application
trusts is the application's decision and a messaging library that picked for it is one that
cannot be audited. **A `ClientConfig` has no Python spelling**, so this surface has no way to
accept one, and a binding that linked a TLS stack it could not configure would be linking it
for nothing.

The consequence is stated rather than hidden: `Client.is_encrypted` and
`sync.Client.is_encrypted` exist and are **always `False`** (`client.rs:459-471`,
`sync.rs:259-264`), asserted as such by `test_the_connack_reaches_python`. That matters
because the User Name, the Password and every byte of `Authentication Data` travel inside the
CONNECT: on this transport they travel in the clear, and the attribute is how a caller finds
out. What adding TLS needs is a Python spelling of "these roots, this client certificate, this
server name" — the same gap [`nats-py.md`](nats-py.md) §8.5 and [`amqp-py.md`](amqp-py.md)
record, and it is filed rather than guessed at.

### 9.2 Two surfaces, one set of values, three handles

`weida_mqtt` (asyncio) and `weida_mqtt.sync` (blocking) are the same client. Every value class
is **the same object** in both — `ConnectOptions`, `Will`, `Session`, `Message`,
`Subscription`, `Delivery`, `Completion` — and so is the whole exception family; a `Session`
built for one surface is passed to the other unchanged
(`tests/test_sync.py::test_the_session_is_the_same_object_in_both_surfaces`).

**The three handles are the only difference**: `Context`, `Client` and the delivery stream,
which is `Events` on the asyncio surface and `sync.Deliveries` on the blocking one
(`sync.rs`, and `smoke.py` asserts the three names). They differ
because only they can: an awaitable and a blocked thread are not the same object.

`weida_mqtt.sync` implements **no protocol behaviour**. It is Python argument conversion over
`weida-mqtt`'s own `blocking` module, which is a `block_on` around each asynchronous method,
so the QoS state machines, the session, the send quota, the topic aliases, the keep-alive
timer and every refusal are decided exactly once and the two surfaces cannot disagree
(`sync.rs:20-31`; [0013](../decisions/0013-competitor-libraries.md) §4.4 item 3).
`test_a_refusal_is_the_asynchronous_surfaces_refusal` is what keeps that honest. The GIL is
released while blocked, through `Python::detach` on every call, so the usual shape of a
synchronous MQTT program — one thread publishing, one consuming — is two threads and not one
(`tests/test_sync.py::test_the_gil_is_released_while_blocked`).

One honest rider. The blocking `Client` carries **fewer accessors** than the asyncio one:
`server`, `keep_alive`, `response_information`, `response_topic` and `session` have no
synchronous counterpart (`sync.rs:247-264` against `client.rs:446-569`), and `Deliveries` has
no `next_event`, so a resumed exchange's completion is not reachable from the blocking
surface.

### 9.3 50 exception classes, where paho has one errno

Every acknowledgement in 5.0 carries a reason code, which is the protocol's largest single
improvement over 3.1.1. paho hands that back as a value: an `MQTTErrorCode` return, or a
`ReasonCode` object a caller inspects with `.value` and `.is_failure`. Here it is the class:

- **36 classes named for MQTT reason codes**, de-duplicated across packets — `NotAuthorized`
  is 0x87 in CONNACK, PUBACK, SUBACK, UNSUBACK and DISCONNECT alike, and appears once, so
  `except NotAuthorized` catches it wherever it arrived (`errors.rs:65-102`);
- **13 classes for failures MQTT gives no byte** — `NotConnected`, `Timeout`, `InvalidTopic`,
  `AcknowledgementLengthMismatch`, `SessionPresentWithoutState` and eight more
  (`errors.rs:104-118`);
- **the base, `MqttError`**, created by the shared foundation alongside the subclasses
  (`crates/py/weida-py-core/src/errors.rs:117-133`), so `except MqttError` catches the lot.

36 + 13 + 1 = **50 classes**, each carrying `errno` (the name), `cause` (why) and
`reason_code` (the byte, or `None`) — `tests/test_values.py::test_every_reason_code_is_its_own_class_under_one_base`
and `test_the_failures_with_no_reason_code_are_classes_too`. A count of "36 coded plus 14
uncoded" gives the same total by counting the base among the uncoded; the code has 13
uncoded, so 50 is the right total only when the base class is counted, and that is how this
document counts it (§11 gives the commands).

Two properties of the table are structural rather than tested. A duplicated name is refused at
**import** by the foundation (`errors.rs:27-31`). And `connect_name` and `disconnect_name` are **exhaustive
matches** over the codec's own enums (`errors.rs:137-201`), so a reason code added to the
library that is not added here is a compile error in this file rather than a code that
silently arrives as the base class.

One name in the table is not a failure at all: `StopAsyncIteration`, which is how the end of a
delivery stream is spelled, because an iteration's end is not an error (`errors.rs:253-282`).

### 9.4 `publish` has no timeout argument on the synchronous surface, deliberately

`sync.Deliveries.recv` takes `timeout=` — that is where the asynchronous surface's
cancellation goes, since a blocking caller has no task to cancel (`sync.rs:283-305`). `publish`
does **not**, and the omission is the argued one (`sync.rs:179-186`):

> A publish abandoned mid-exchange is still session state on both sides, so a deadline here
> would return control while the exchange continued.

That is exactly what cancelling the asyncio publish does — and that surface is honest about
it, because "the caller stops waiting" is what cancellation means and the exchange survives to
be retransmitted on the next connection (`client.rs:12-20`,
`tests/test_interop.py::test_a_cancelled_publish_leaves_the_client_usable`). An argument named
`timeout` on a blocking publish would read as "give up on the publish", which is not something
either end can do. A caller who wants the asyncio semantics runs the publish on its own
thread. `tests/test_sync.py::test_publish_has_no_timeout_argument_and_the_omission_is_deliberate`
pins the absence: passing `timeout=1.0` is a `TypeError`.

### 9.5 What a Python caller does not get

- **No automatic reconnect and no backoff**, where paho defaults `reconnect_on_failure` to
  true with a 1..120 s doubling ([`../research/mqtt5.md`](../research/mqtt5.md) §1 [37]). A
  loop that reconnected invisibly would decide the Clean Start flag on the caller's behalf,
  and that flag decides whether messages are lost. A reconnect is
  `context.connect_session(address, session, options)`.
- **No republish after a reconnect.** paho's Python client republishes QoS > 0 messages after
  a network reconnect even with `clean_session=True`, calls that non-compliant itself and
  warns that QoS 2 messages can therefore arrive twice (sheet §13 [38]). Here retransmission
  happens once, just after a CONNACK with `Session Present` 1 ([MQTT-4.4.0-1]), and there is
  no retry timer at all ([`mqtt.md`](mqtt.md) §4).
- **No durable session store.** `Session` is in memory (`client.rs:199-205`), as paho's is.
- **No acting on `Server Reference`**: it is readable as `server["server_reference"]` and
  following a redirection is the application's.
- **No broker**, per §0.

### 9.6 Bounds this binding inherits and exposes

The ceilings [`mqtt.md`](mqtt.md) §9.1 adds because the protocol has none are keyword
arguments here rather than constants: `max_subscription_ids` (32), `incoming_queue` (1024),
`connect_timeout` (10 s) and `ping_timeout` (the Keep Alive). Two have no Python spelling —
`max_addresses` (8) and `max_user_properties` (64), §5. Every duration in this surface is **seconds as a
float**, and a negative or non-finite one is a `ValueError` at the call that wrote it
(`options.rs:18-29`, `values.rs:55-66`, `sync.rs:64-75`).

### 9.7 Open questions

1. **The paho column is unmeasured.** No paho version is pinned anywhere under `crates/`, and
   no test imports it (§1, §8). The API names in the left-hand column are paho-mqtt 2.x's
   documented surface; a reader who needs them load-bearing should run the interop suite §8
   describes rather than trust this column.

## 10. The definition of done

The clauses the asyncio surface, the synchronous surface and this document were built against, each with its verdict and the section that proves it.

| Clause | Verdict | Where |
| --- | --- | --- |
| Connect, publish, subscribe, and an async iterator of deliveries | yes | §2, `tests/test_interop.py` |
| A delivery carrying topic, QoS, the retain flag, the properties and the subscription identifiers | yes, plus `dup`, `packet_id` and `origin()` | §2, §6 |
| Reason codes as distinct exception classes | yes, 50 under one base, each with `reason_code` | §9.3 |
| A QoS 2 publish awaited to its PUBCOMP from Python | yes | §8, `test_a_qos_2_publish_is_awaited_to_its_pubcomp` |
| The whole round trip against a real broker | yes, rumqttd 0.20.0, this client on both sides | §8 |
| A synchronous surface over the same client, no loop in the process | yes | §9.2, `test_no_event_loop_exists_in_this_process` |
| No second implementation of any protocol behaviour in the binding | yes, `weida_mqtt.sync` over the library's `blocking` module | §9.2 |
| A receive deadline where the asynchronous surface has cancellation | yes, and `publish` deliberately has none | §9.4 |
| Options honoured under a name or refused at configuration time | yes, 19 CONNECT keywords, `validate()` at construction; six library fields (`max_addresses`, `max_user_properties`, `incoming_queue_bytes`, `max_inflight`, `max_inflight_bytes`, `max_alias_bytes`) have no keyword and the row says so | §5 |
| TLS absent by construction, with `is_encrypted` saying so | yes | §3, §9.1 |
| The wheel builds and runs with no Rust toolchain | yes: `package.sh` (a wrapper over `crates/py/package.sh`) builds the `abi3` wheel, installs it into a fresh virtualenv and runs `smoke.py` with `PATH` scrubbed to `/usr/bin:/bin`; both files exist and the last recorded runs passed. This document does not re-run them | §8, `package.sh`, `smoke.py` |
| Interop against the Python reference implementation | **not met**: paho-mqtt is not pinned, not installed and never run against this binding, and §8 says what would close it | §8, §9.7 |
| No row says "partial" | yes: §3, §4 and §9.5 name what a caller does not get | §3, §4, §9 |

## 11. Sources, and the commands behind every count

Counts, each reproducible from the repository root:

| Count | Command | Result |
| --- | --- | --- |
| pytest functions | `grep -c 'def test_' crates/mqtt/weida-mqtt-py/tests/*.py` | `conftest.py` 0, `test_interop.py` 12, `test_sync.py` 12, `test_values.py` 21 — **45** |
| reason-code exception classes | `grep -cE '^    \([A-Za-z]+, 0x[0-9A-F]{2}\),$' crates/mqtt/weida-mqtt-py/src/errors.rs` | **36** |
| classes for failures with no byte | `grep -cE '^    \([A-Za-z]+\),$' crates/mqtt/weida-mqtt-py/src/errors.rs` | **13** |
| exception classes in all | the two above plus the base `MqttError`, created by `ErrorFamily::new` (`crates/py/weida-py-core/src/errors.rs:117-133`) | **50** |
| Python classes | `grep -c '#\[pyclass' crates/mqtt/weida-mqtt-py/src/*.rs` | `client.rs` 4, `options.rs` 2, `values.rs` 4, `sync.rs` 3 — **13**, ten in `weida_mqtt` and three in `weida_mqtt.sync` |
| module-level functions | `grep -c '#\[pyfunction\]' crates/mqtt/weida-mqtt-py/src/lib.rs` | **3** (`matches`, `check_topic_filter`, `check_topic_name`) |
| `ConnectOptions` keywords | the `#[pyo3(signature = ...)]` block at `crates/mqtt/weida-mqtt-py/src/options.rs:152-173` | **19** |
| `Will` keywords | `crates/mqtt/weida-mqtt-py/src/options.rs:51-62` | **9** |
| `Message` keywords | `crates/mqtt/weida-mqtt-py/src/values.rs:89-101` | **10** |
| keys in `Client.server` | `crates/mqtt/weida-mqtt-py/src/client.rs:510-553`, one per field of `ServerLimits` | **14** |
| keys in `Delivery.properties` | `crates/mqtt/weida-mqtt-py/src/values.rs:291-321` | **8** |
| module constants | `crates/mqtt/weida-mqtt-py/src/lib.rs:129-143` | **6** |

Where the verdicts were read:

- Code: `crates/mqtt/weida-mqtt-py/src/` — `lib.rs` (the module, the three functions, the TLS
  paragraph), `client.rs` (`Context`, `Session`, `Client`, `Events`), `options.rs`
  (`ConnectOptions`, `Will`), `values.rs` (`Message`, `Delivery`, `Subscription`,
  `Completion`), `sync.rs` (the three blocking handles), `errors.rs` (the 50 classes);
  `crates/mqtt/weida-mqtt-py/Cargo.toml` for the feature set and
  `crates/mqtt/weida-mqtt-py/pyproject.toml` for the wheel's metadata; `crates/py/weida-py-core/src/`
  for the exception family, the asyncio bridge and the bytes boundary. Where a verdict depends
  on the library rather than the binding, the row names the file under
  `crates/mqtt/weida-mqtt/src/`.
- Tests: `crates/mqtt/weida-mqtt-py/tests/` — `test_interop.py`, `test_sync.py`,
  `test_values.py`, with `conftest.py` for the broker fixture; `smoke.py` for the wheel;
  `develop.sh` and `package.sh` (wrappers over `crates/py/develop.sh` and `crates/py/package.sh`) for how both are run.
- The library's own parity table: [`mqtt.md`](mqtt.md) — §0 (why there is no server column),
  §2, §3, §4, §5, §6, §7, §8 (transports and the TLS trust anchors), §9 (the bounds), §10 (the
  broker disagreements). The protocol sheet it cites:
  [`../research/mqtt5.md`](../research/mqtt5.md), whose §1, §11 and §13 carry the paho
  behaviour claims used above, with references [37] and [38].
- Decisions: [0014](../decisions/0014-parallel-libraries.md) §2 (one shared PyO3 foundation,
  asyncio first, the bytes boundary, the client/broker line),
  [0013](../decisions/0013-competitor-libraries.md) §4.4 (the three constructors, options
  honoured or refused, no second implementation in the binding) and §4.7 (the six clauses).
- paho-mqtt's surface: `paho.mqtt.client`, `paho.mqtt.properties`, `paho.mqtt.reasoncodes`,
  `paho.mqtt.subscribeoptions`, `paho.mqtt.enums`, `paho.mqtt.publish` and
  `paho.mqtt.subscribe` of eclipse-paho/paho.mqtt.python 2.x — **read from that project's
  documentation, not from this repository and not from a run** (§1, §9.7).
