# MQTT 5.0 (OASIS Standard 2019-03-07, measured against rumqttd 0.20.0 and rmqtt 0.23.1) — feature parity

Status: complete against the six clauses of
[0013](../decisions/0013-competitor-libraries.md) §4.7, read for MQTT. This document is the
sixth clause itself — the row-by-row table — and §10 gives the other five their verdicts.

The library is two crates: `weida-mqtt-codec`, the sans-I/O MQTT 5.0 codec whose
`[dependencies]` is empty, and `weida-mqtt`, the client on top of it. Neither depends on
`weida`; a caller who wants an MQTT client and nothing else uses `weida-mqtt` alone
([0013](../decisions/0013-competitor-libraries.md) §4.3, §4.4). It exposes MQTT's own client,
session, subscription and QoS semantics. No crate assigns them global weida counterparts.

## 0. Why there is no server column

**MQTT's topology is strictly asymmetric, and this is a client.** There is no MQTT
peer-to-peer: the server binds and accepts, the client connects, and every one of the
protocol's central mechanisms — retained messages, the session store, subscription routing,
shared-subscription selection, the Will's delay timer — lives in the server. A client cannot
implement half of them and does not try.

[0014](../decisions/0014-parallel-libraries.md) §2 draws the line, and a broker owns the other
side. So every row below answers **"does the client have it"**, and a mechanism whose whole
implementation is the server's is marked `server-side` rather than absent: it is not missing
from this library, it is not this library's to have. Where a client must *say* something for
a server-side mechanism to work — the Will in CONNECT, Retain Handling in SUBSCRIBE, the
`$share/` prefix in a filter — that saying is a row and it is present.

The consequence a reader should take away: a row marked `server-side` is the one place a
parity table can hide a gap, so each of them names what the client sends and which interop
test measured what the server then did.

## 1. What a row means

Three verdicts and no fourth:

- **present** — implemented, with the module or test that carries it named. A present row may
  still say where the behaviour differs, and then the difference is the row.
- **refused with a reason** — the packet, property or option exists in MQTT 5.0 and fails
  **here, at configuration time**, with a message naming why (0013 §4.4 item 4) and, where
  the protocol has one, the reason code the server would have sent. Nothing is silently
  ignored.
- **absent with a reason** — not implemented and not configurable, with what is missing
  named.

Plus one that belongs to this protocol and not to ZeroMQ's or SP's:

- **server-side** — the mechanism is the server's, per §0. The row says what the client sends
  and which interop run measured the server's half.

"Partial" is not a verdict. Where something is present in part, the row says which part is
missing (0013 §4.7 clause 6).

**Versions every measured claim was measured against.** The specification is *MQTT Version
5.0*, OASIS Standard, 07 March 2019. Claims about what MQTT *specifies* cite
[`../research/mqtt5.md`](../research/mqtt5.md) by section and the conformance statements by
number. Claims about what an implementation *does* were measured on Linux x86-64 on
2026-09-12, rustc 1.98, against two brokers over loopback TCP:

| Broker | Version | Suite | Why it is here |
| --- | --- | --- | --- |
| `rumqttd` | 0.20.0 | `crates/mqtt/weida-mqtt/tests/interop_rumqttd.rs`, 12 tests | a broker that declares almost nothing, so every §11 default is exercised as a default |
| `rmqtt` | 0.23.1 | `crates/mqtt/weida-mqtt/tests/interop_mqtt5.rs`, 12 tests | a broker that **declares** its capabilities, which is the only way a client's refusal paths are reachable |

Mosquitto is the broker [0014](../decisions/0014-parallel-libraries.md) and the backlog name,
and it is a **system package**; it is absent on the machine this was measured on and
installing one is outside [LOOP.md](../LOOP.md) §2. Its commands are in
`interop_mqtt5.rs`'s module doc so the named broker can be measured where it exists. Nothing
in this table is inferred from a version other than the two above.

## 2. Packet types

All fifteen of table 2-1, encoded and decoded by `weida-mqtt-codec`, with the direction rule
of 2.1.2 enforced: a client-to-server-only packet arriving at the client is
`Error::UnexpectedPacket` naming the type, and the codec's `the_direction_table_matches_the_specification`
test asserts the whole table rather than the packets this client happens to send.

| Packet | Value | Codec | Client | Notes |
| --- | --- | --- | --- | --- |
| CONNECT | 1 | present (`connect.rs`) | present (`connection.rs`) | the Will boxed inside it, because a Will holds a second property set and inlining it cost every `Packet` 336 bytes |
| CONNACK | 2 | present (`connack.rs`) | present (`limits.rs`) | every declaration read into `ServerLimits` with §11's defaults applied to what is absent |
| PUBLISH | 3 | present (`publish.rs`) | present (`client.rs`, `message.rs`) | the only packet carrying an Application Message and the only one using its fixed-header flags |
| PUBACK | 4 | present (`ack.rs`) | present | QoS 1's acknowledgement |
| PUBREC | 5 | present (`ack.rs`) | present | a code ≥ 0x80 ends the exchange early ([MQTT-4.4.0-2]) |
| PUBREL | 6 | present (`ack.rs`) | present | the only resend a session performs besides the PUBLISH |
| PUBCOMP | 7 | present (`ack.rs`) | present | 0x92 on recovery is not an error (3.6.2.1) |
| SUBSCRIBE | 8 | present (`subscribe.rs`) | present (`Client::subscribe`) | per-filter options; one Subscription Identifier per packet, not per filter |
| SUBACK | 9 | present (`subscribe.rs`) | present | one code per filter, in order; a count mismatch is `Error::AcknowledgementLengthMismatch` |
| UNSUBSCRIBE | 10 | present (`unsubscribe.rs`) | present (`Client::unsubscribe`) | bare filters, no options |
| UNSUBACK | 11 | present (`unsubscribe.rs`) | present | 0x11 `No subscription existed` is a **success** |
| PINGREQ | 12 | present (`control.rs`) | present (`client.rs`) | sent by the keep-alive timer or by `Client::ping` |
| PINGRESP | 13 | present (`control.rs`) | present | its absence is bounded by `ConnectOptions::effective_ping_timeout`, a number the specification does not give |
| DISCONNECT | 14 | present (`control.rs`) | present (`Client::disconnect_with`) | in **both** directions: a server DISCONNECT is `Error::ServerDisconnected` carrying its code |
| AUTH | 15 | present (`control.rs`) | present (`connection.rs`, `Client::reauthenticate`) | in the handshake and on the live connection |

## 3. The property framework

Table 2-4 assigns 27 identifiers, each to a set of packets. All 27 are encoded and decoded;
`the_twenty_seven_identifiers_are_distinct_in_both_numberings` asserts the table rather than
the properties this client happens to use, and `a_property_in_the_wrong_packet_is_refused_and_right_where_it_belongs`
asserts the packet membership both ways.

| Identifier | Dec | Codec | Client surface | Notes |
| --- | --- | --- | --- | --- |
| Payload Format Indicator | 1 | present | `Message::payload_format_indicator` | |
| Message Expiry Interval | 2 | present | `Message::message_expiry` | a server rewrites it downwards; the interop assertion is a bound, not an equality |
| Content Type | 3 | present | `Message::content_type` | |
| Response Topic | 8 | present | `Message::response_topic` | checked as a Topic Name where `Client::response_topic` builds it |
| Correlation Data | 9 | present | `Message::correlation_data` | the only thing pairing a reply with its request; the server keeps no table |
| Subscription Identifier | 11 | present, **repeatable** | `Client::subscribe_with`, `DeliveryProperties::subscription_identifiers` | bounded by `Limits::max_subscription_identifiers` because the protocol bounds it nowhere |
| Session Expiry Interval | 17 | present | `ConnectOptions::session_expiry`, `Client::disconnect_with` | the zero-then-non-zero Protocol Error is refused at configuration time |
| Assigned Client Identifier | 18 | present | `Client::client_id` | a zero-length Client Identifier asks for one |
| Server Keep Alive | 19 | present | `Client::keep_alive` | the one property that **overrides** the client ([MQTT-3.2.2-21]) |
| Authentication Method | 21 | present | `ConnectOptions::authentication_method` | repeated by every AUTH and checked, not trusted |
| Authentication Data | 22 | present | `ConnectOptions::authentication_data`, `Authenticator::challenge` | |
| Request Problem Information | 23 | present | `ConnectOptions::request_problem_information` | default 1, so only the non-default reaches the wire |
| Will Delay Interval | 24 | present | `WillMessage::delay` | client-side: sent and asserted; the timer is `server-side` |
| Request Response Information | 25 | present | `ConnectOptions::request_response_information` | |
| Response Information | 26 | present | `Client::response_information`, `Client::response_topic` | offered by neither measured broker; `response_topic` returns `None` and says why |
| Server Reference | 28 | present | `ServerLimits::server_reference` | read and reported; **acting** on a redirection is absent, §9.4 |
| Reason String | 31 | present | `ServerLimits::reason_string` | "SHOULD NOT be parsed", so it is carried and never matched on |
| Receive Maximum | 33 | present | `Limits::receive_maximum`, `Session::send_quota` | two different numbers, §9.2 |
| Topic Alias Maximum | 34 | present | `Limits::topic_alias_maximum`, `ServerLimits::topic_alias_maximum` | two tables, two bounds (`alias.rs`) |
| Topic Alias | 35 | present | `alias.rs`, resolved before the application sees a delivery | |
| Maximum QoS | 36 | present | `ServerLimits::maximum_qos` | a higher publish is refused locally with 0x9B |
| Retain Available | 37 | present | `ServerLimits::retain_available` | refused locally with 0x9A — except on the Will, §9.3 |
| User Property | 38 | present, **repeatable** | `Message::user_properties`, `ConnectOptions::user_properties` | order and repeats preserved, measured end to end |
| Maximum Packet Size | 39 | present | `Limits::maximum_packet_size`, `ServerLimits::send_packet_ceiling` | checked from the fixed header *before* a body is allocated |
| Wildcard Subscription Available | 40 | present | `ServerLimits::wildcard_subscription_available` | refused locally with 0xA2 |
| Subscription Identifier Available | 41 | present | `ServerLimits::subscription_identifiers_available` | refused locally with 0xA1 |
| Shared Subscription Available | 42 | present | `ServerLimits::shared_subscription_available` | refused locally with 0x9E |

The two repeatable properties are not collected into a `Vec` on decode: a decoded
`Properties` re-walks the wire bytes on demand, so a peer sending a thousand User Properties
costs the same as none. That is the allocation claim `weida-mqtt-codec`'s module doc makes and
its fuzz targets check.

## 4. QoS and the delivery state machines

| Level | Verdict | What it certifies here |
| --- | --- | --- |
| QoS 0 | present | `Completion::Sent` — the bytes left. Not counted against the send quota and never stalled |
| QoS 1 | present | `Completion::Acknowledged(code)` on the PUBACK |
| QoS 2 | present | `Completion::Complete(code)` on the PUBCOMP, or `Completion::Refused` early on a PUBREC ≥ 0x80 |

Both directions are implemented. Outbound state lives in `session.rs` (`Stage::AwaitingPuback`,
`AwaitingPubrec`, `AwaitingPubcomp`); inbound QoS 2 duplicate suppression is one set of
identifiers awaiting their PUBREL, and a repeat of one already held is answered with another
PUBREC and **not** delivered again ([MQTT-4.3.3-10]).

**Retransmission happens in exactly one place**: just after a CONNACK with `Session Present`
1 ([MQTT-4.4.0-1]). There is no retry timer anywhere in this crate, and `lib.rs` says so,
because the ecosystem disagrees loudly enough to make the absence worth stating — EMQX's
30-second `retry_interval`, Paho Python's self-declared non-compliant republish (sheet §6).
`tests/qos.rs` drives each of the four points a QoS 2 handshake can be interrupted at.

The send quota is the server's `Receive Maximum` counted in QoS 1 and 2 PUBLISH packets and
nothing else, and exhausting it **stalls** the sender rather than exceeding it
([MQTT-4.9.0-2]): the caller's future stays pending and the packet stays off the wire.

## 5. Topic filters and subscription options

| Construct | Verdict | Where |
| --- | --- | --- |
| `/` levels, `+` one level, `#` the rest including the parent | present | `filter::matches`, asserted against the specification's own worked examples |
| `$`-prefixed topics excluded from a leading wildcard ([MQTT-4.7.2-1]) | present | `filter::matches`; the rule is about the filter's first character, and `$SYS/+` is fine |
| The grammar's refusals (`sport/tennis#`, `sport/#/ranking`, `sp+rt`, empty) | refused with a reason | `filter::check_topic_filter`, each naming the conformance statement |
| A Topic Name containing `+` or `#` | refused with a reason | `filter::check_topic_name` ([MQTT-3.3.2-2]) |
| 65,535-byte ceiling on a name or filter | present | the two-byte length prefix is the bound |
| Maximum QoS per filter | present | `SubscriptionOptions::maximum_qos` |
| No Local | present | `Subscription::no_local`; on a `$share/` filter it is **refused** ([MQTT-3.8.3-4]) |
| Retain As Published | present | `Subscription::retain_as_published`; its cost is `RetainedOrigin::Unknowable`, §6 |
| Retain Handling 0, 1, 2 | present | `RetainHandling`; 1 separated from 0 against rmqtt |
| Retain Handling 3 | refused with a reason | unrepresentable in `RetainHandling`, and `DecodeError::InvalidRetainHandling` off the wire ("a Protocol Error to send", 3.8.3.1) |
| The two reserved option bits | present | asserted clear on every encode, refused non-zero on decode |
| `$share/{ShareName}/{filter}` as a subscriber | present | `filter::split_shared`; the three ShareName rules refused at configuration time |
| Shared-subscription **selection** between members | server-side | the client sends the filter; `a_shared_subscription_splits_deliveries_between_its_members` measures one publication producing one delivery to the group. The sheet records that implementations disagree substantively about strategy (§13), so no client may assume one |
| Subscription Identifier reported on a delivery | present | measured against both brokers |
| Telling which filter matched **without** an identifier | present | `Subscriptions::matching`, for a server declaring `Subscription Identifiers Available` 0 |

## 6. Retained messages and the Will

| Construct | Verdict | Where |
| --- | --- | --- |
| Publishing with RETAIN | present | `Message::retained` |
| The zero-byte delete that is itself not stored | present | `Message::delete_retained`, measured against rumqttd through all three states |
| Storing, and sending at subscribe time | server-side | measured against both brokers |
| Telling a retained delivery from a live one | present, **with a named loss** | `Delivery::origin` → `RetainedOrigin::{Retained, Live, Unknowable}`. Under Retain As Published 1 the RETAIN flag reports the publisher's intent rather than this copy's origin ([MQTT-3.3.1-13]), so the two are indistinguishable and the type says so rather than guessing |
| Retained messages never sent to a shared subscription (4.8.2) | server-side | **rmqtt 0.23.1 sends one**, recorded as a measured disagreement |
| The Will in CONNECT: topic, payload, QoS, retain, and its four properties | present | `WillMessage`, asserted field by field on the wire |
| Will Delay Interval | present (sent) / server-side (timed) | the client declares it; the "a reconnect inside the delay cancels it" rule is the server's |
| Will discarded by DISCONNECT 0x00 ([MQTT-3.14.4-3]) | present | measured: it does not fire |
| Will requested by DISCONNECT 0x04 | present (sent) / server-side (honoured) | the client sends the code; **rumqttd 0.20.0 publishes nothing**, a measured disagreement |
| The Will fires on an abnormal close | present (caused) / server-side | dropping the handle closes the transport with **nothing written**, which is what makes it abnormal; the connection task deliberately does not invent an orderly close, because that would suppress the Will |
| Will Retain against `Retain Available` 0 | **cannot** be refused locally, §9.3 | the Will travels in the CONNECT, before the CONNACK that declares it exists |

## 7. Reason codes

Every acknowledgement in 5.0 carries one, which is the protocol's largest single improvement
over 3.1.1 — where "a client learned why a server objected only by watching it close the
socket" (sheet §1.9). The codec gives each packet its **own closed subset** rather than one
flat enum, so a code outside a packet's permitted set is `DecodeError::InvalidReasonCode`
naming the packet and not an opaque byte a caller has to interpret.

| Group | Codes | Type | Client surface |
| --- | --- | --- | --- |
| CONNACK | 22 | `ConnectReasonCode` | `Error::ConnectionRefused` |
| PUBACK / PUBREC | 9 (shared) | `PubackReasonCode`, `PubrecReasonCode` | `Completion::Acknowledged`, `Completion::Refused` |
| PUBREL / PUBCOMP | 2 (shared) | `PubrelReasonCode`, `PubcompReasonCode` | `Completion::Complete` |
| SUBACK | 12 | `SubackReasonCode` | the `Vec` `Client::subscribe` returns |
| UNSUBACK | 7 | `UnsubackReasonCode` | the `Vec` `Client::unsubscribe` returns |
| DISCONNECT | 29 | `DisconnectReasonCode` | `Error::ServerDisconnected`, and the argument to `Client::disconnect` |
| AUTH | 3 | `AuthReasonCode` | the re-authentication exchange |

`the_successes_that_look_like_failures` asserts the 0x80 line's three exceptions — "a value of 0x80 or greater
indicates a failure" — across every group, and `every_subset_is_closed_and_has_the_documented_size` asserts each group's count with
`assert_closed!`, so a code added without a row fails the test. Locally-refused configurations carry the code the
server *would* have sent, which is what `Error::reason_code` returns: 0x9A, 0x9B, 0x9E, 0xA1,
0xA2, 0x94, 0x93.

## 8. Transports

| Transport | Verdict | Where |
| --- | --- | --- |
| TCP, port 1883 | present | `transport::Stream::Plain` |
| TLS, port 8883 | present, behind the default-on `tls` feature | `transport::Stream::Tls`, `TlsOptions`; a real handshake against an `rcgen` certificate in `tests/tls_auth.rs` |
| WebSocket (`ws`, `wss`) | absent with a reason | the specification's own transport section names it, and it needs an HTTP upgrade and a framing layer this crate has no dependency for. A caller who needs it wants a WebSocket client and this codec, which is sans-I/O precisely so that composition is possible |
| MQTT over QUIC | absent with a reason | **not a standard**: an EMQX transport and a NanoMQ bridge to EMQX, not interoperable across brokers in any standard sense (sheet §13). Implementing it would be implementing one vendor's extension |
| Unix domain sockets | absent with a reason | not in the specification; brokers that offer one differ on the address syntax |

The trust anchors are **not** in this crate: `TlsOptions` takes the caller's
`rustls::ClientConfig`, because which certificates an application trusts is the application's
decision and a messaging library that picked for it is one that cannot be audited. There is
no conversion from the TLS peer to any identity type and no accessor on `Delivery` that
returns one, because MQTT has no per-message identity at all.

## 9. Deliberate deviations and bounds the protocol lacks

### 9.1 Bounds this library adds, each because the protocol has none

MQTT 5.0's §11 is a table of limits and a list of four resources the protocol does **not**
bound at all. A client is on the exposed side of each, so `Limits` names them and each is a
**caller argument** rather than a constant:

| Bound | Default | Why the protocol has none |
| --- | --- | --- |
| `Limits::max_subscription_identifiers` | 32 | a server MAY send one copy carrying every matching identifier ([MQTT-3.3.4-4]) and the only limit on the wire is the packet size |
| `Limits::max_user_properties` | 64 | `User Property` is repeatable per delivery (3.3.2.3.7) and a pair's minimal wire form is five octets, so even the default 1 MiB `Maximum Packet Size` admits over 200 000 of them and a 268 MB one over 50 million |
| `Limits::incoming_queue` | 1024 | deliveries held for the application; QoS 0 has no flow control at all |
| `Limits::max_addresses` | 8 | a resolver's answer is remote input |
| `ConnectOptions::effective_ping_timeout` | the Keep Alive | "a Client SHOULD close the connection within a reasonable amount of time" with **no number** (sheet §1); an unbounded wait is a hang with a rationale ([LOOP.md](../LOOP.md) §2) |
| `ConnectOptions::connect_timeout` | 10 s | the specification puts no deadline on the CONNACK either |

Every decode entry point takes the packet-size cap as an **argument**, and the declared
length is checked from the fixed header before a body is allocated — so a peer announcing a
quarter-gigabyte packet against a 1 KiB ceiling allocates nothing.

### 9.2 `Receive Maximum` is two numbers and they are not interchangeable

The server's, from CONNACK, bounds what **this client** may have in flight toward it —
`Session::send_quota`. The client's own, from CONNECT, bounds what the **server** may have in
flight toward this client — `Session::receive_maximum`, and a server exceeding it earns 0x93.
One field serving both is a bug that dissolves the inbound cap whenever a CONNACK merely
omits the property; it was one, and the split is `session.rs`'s.

### 9.3 Will Retain cannot be refused at configuration time, and this is structural

Every other availability flag refuses locally. The Will is the exception and the reason is
ordering: it travels inside the CONNECT, before the CONNACK that carries `Retain Available`
exists. The only honest answer is the server's CONNACK 0x9A, surfaced as
`Error::ConnectionRefused`. `will_retain_against_a_server_without_retain_is_the_connacks_code`
asserts both halves side by side so the asymmetry reads as a fact rather than an omission.

### 9.4 What a caller does not get

- **Automatic reconnect.** There is no reconnect loop and no backoff. A reconnect is a new
  `Client::connect_session` with the session the application holds, which is what makes it a
  *resumption*; a loop that did it invisibly would decide the Clean Start flag on the
  caller's behalf, and that flag decides whether messages are lost.
- **Acting on `Server Reference`.** The property is read and reported; following a
  redirection is the application's, because it means trusting a server to choose the next
  server.
- **A durable session store.** `Session` is in memory. §9.5 is the consequence.
- **Broker behaviour.** Per §0.

### 9.5 [MQTT-3.2.2-4] read literally is unusable, and what this client does instead

4.1's client-side Session State is *only* the unacknowledged QoS 1 and 2 exchanges. Read
literally, a client that connected with Clean Start 0 and a non-zero Session Expiry Interval,
published nothing, and reconnected would receive `Session Present` 1 with "no Session State"
and be obliged to close — on its own correct resumption.

So `Session` tracks a third thing 4.1's list omits: whether a session exists on the server at
all, decided by whether the last CONNECT declared a non-zero Session Expiry Interval. An idle
session resumes; a session that declared no expiry ends with its connection and a
`Session Present` 1 afterwards really is the forbidden row.

The practical half is measured: **a process that restarts with no persisted state and
reconnects Clean Start 0 must close**, because the broker legitimately still holds its session
and this process has nothing to match it against. That is not a corner case — it is what every
restart of a client without a durable store does, and the answer is Clean Start 1.
`a_restart_without_persisted_state_must_close_and_then_clean_start` asserts the refusal by
name and then takes that answer.

## 10. Interop evidence

Our client on **both** sides of each broker, so publisher and subscriber roles are both ours
and the broker is the only thing in the middle that is not.

**rumqttd 0.20.0** (12 tests): all three QoS levels; the granted-maximum downgrade; retained
messages through all three states including the zero-byte delete; Retain Handling; the Will on
an abnormal close and its discard on an orderly one; session resumption and Clean Start
discard; the `+` and `#` wildcards including `#` matching the parent level; the CONNACK's
declarations; Subscription Identifiers; and the restart case of §9.5.

**rmqtt 0.23.1** (12 tests): the declarations themselves; the send quota from a declared
`Receive Maximum` of 8 against a client-declared 200; shared subscriptions measured by the
count that separates a group from a filter; topic aliases both directions against a declared
16; Retain Handling 1 separated from 0; Session Expiry as a timer, proved by a pairing;
every PUBLISH property forwarded unaltered and in order; SUBACK and UNSUBACK codes; and the
availability flags as **refusals** against a second listener declaring less.

### Five disagreements, each measured against a named version

| # | Broker | What the specification says | What was measured |
| --- | --- | --- | --- |
| 1 | rumqttd 0.20.0 | the delivery QoS is the minimum of the publish's and the granted maximum ([MQTT-3.8.4-8]) | the granted maximum in both directions, so a QoS 0 publish into a QoS 2 subscription arrives at QoS 2 — an **upgrade** the specification forbids. Proved by a *pair* of tests: the downgrade direction is correct |
| 2 | rumqttd 0.20.0 | Retain Handling 2 does not send retained messages at subscribe (3.8.3.1) | they are sent |
| 3 | rumqttd 0.20.0 | DISCONNECT 0x04 asks the server to publish the Will | nothing is published; 0x00's discard works, so it implements one half |
| 4 | rmqtt 0.23.1 | retained messages are never sent to a shared subscription (4.8.2) | one is sent, with RETAIN 1 |
| 5 | rmqtt 0.23.1 | a Client Identifier takeover closes the older connection ([MQTT-3.1.4-3]) **saying why**, DISCONNECT 0x8E | the transport closes with nothing sent — the 3.1.1 behaviour 5.0 exists to replace, from a broker claiming full 5.0 |

Two facts about installing these brokers, recorded because they cost time: `cargo install
rumqttd --version 0.20.0` fails **with** `--locked` (its pinned `metrics` fails borrow
checking on rustc 1.98) and `cargo install rmqttd --version 0.23.1` fails **without** it (a
resolved `pulsar` gained a field its egress-bridge plugin does not set). And rmqtt reports
`Retain Available` 0 and `Shared Subscription Available` 0 unless the `rmqtt-retainer` and
`rmqtt-shared-subscription` plugins are started, because both capabilities are plugin state
rather than listener settings — against which this client correctly refuses a retained publish
and a shared subscription before the wire, which looks exactly like a broken test.

### What no broker in this repository could exercise

- **AUTH.** Neither offers an enhanced-authentication mechanism; rmqtt's JWT and HTTP auth
  plugins authenticate the CONNECT's credentials rather than running 4.12's exchange. So 4.12
  is measured against the scripted server of `tests/tls_auth.rs` — a real exchange against
  our own codec, which is a weaker claim than a foreign one and is stated as such.
- **`Response Information`.** Asked for and offered by neither broker.

## 11. The definition of done

[0013](../decisions/0013-competitor-libraries.md) §4.7's six clauses, each with its verdict
and where it is proved.

| Clause | Verdict | Proof |
| --- | --- | --- |
| 1. A typed surface, not one handle with a runtime tag | met | `Client`, `Session`, `Message`, `Delivery`, `Subscription` are distinct types; `Completion` distinguishes what each QoS certifies; `RetainedOrigin` names the case the protocol cannot answer |
| 2. Async first, on `weida-runtime` | met | `Context` mirrors `weida-zmq`'s three constructors, including `Context::owned` for a caller with no reactor of its own (`client.rs`) |
| 3. Options honoured or refused, never ignored | met | §3, §5, §8 and §9; every refusal names the conformance statement and, where one exists, the reason code the server would have sent |
| 4. Identity types apart | met | §8: no conversion from a TLS peer to an identity, and no accessor on `Delivery` returning one, because MQTT has none |
| 5. A sans-I/O codec with an empty `[dependencies]` | met | `cargo tree -p weida-mqtt-codec` prints exactly one node; every decode takes its cap as an argument; golden vectors in `crates/mqtt/weida-mqtt-codec/tests/golden_vectors.rs` and six fuzz targets in its own workspace |
| 6. This table | met | §2 through §8, with no row saying "partial" |

## 12. Sources

- [`../research/mqtt5.md`](../research/mqtt5.md): §1 connection and sessions, §1.9 the 3.1.1
  differences, §2 packets, §3 QoS, §4.1 topics and filters, §4.2 shared subscriptions, §4.3
  request/response, §4.4 subscription options, §4.5 the Will, §4.6 the bridge options, §5–§6
  flow control and guarantees, §8 retained, §10 security and AUTH, §11 limits, §13 the
  ecosystem and the known incompatibilities.
- Code and tests: `crates/mqtt/weida-mqtt-codec/tests/golden_vectors.rs` asserts the codec's
  accepted and rejected wire vectors in both directions.
- [0013](../decisions/0013-competitor-libraries.md) §4.1–§4.4, §4.7;
  [0014](../decisions/0014-parallel-libraries.md) §2 for the client/broker line of §0.
- Code: `crates/mqtt/weida-mqtt-codec/src/` (§2, §3, §7) and `crates/mqtt/weida-mqtt/src/`
  — `client.rs`, `session.rs`, `filter.rs`, `alias.rs`, `message.rs`, `limits.rs`,
  `options.rs`, `transport.rs` — plus `tests/interop_rumqttd.rs` and `tests/interop_mqtt5.rs`
  (§10).
