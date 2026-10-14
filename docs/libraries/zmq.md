# ZeroMQ (libzmq 4.3.5) — feature parity

Status: complete against the six clauses of
[0013](../decisions/0013-competitor-libraries.md) §4.7. This document is the sixth clause
itself — the row-by-row table — and §10 gives the other five their verdicts.

The library is two crates: `weida-zmtp`, the sans-I/O ZMTP 3.1 codec whose `[dependencies]`
is empty, and `weida-zmq`, the socket implementation on top of it. Neither depends on
`weida`; a caller who wants a ZeroMQ and nothing else uses `weida-zmq` alone
([0013](../decisions/0013-competitor-libraries.md) §4.3, §4.4). The standard queue,
forwarder and streamer devices are `weida_zmq::proxy` helpers inside this library; they connect
ZeroMQ sockets to ZeroMQ sockets. No crate assigns ZeroMQ socket types global weida
counterparts.

## 1. What a row means

Three verdicts and no fourth:

- **present** — implemented, with the module or test that carries it named. A present row may
  still say where the behaviour differs, and then the difference is the row.
- **refused with a reason** — the configuration exists in libzmq and fails **here, at
  configuration time**, with a message naming why (0013 §4.4 item 4). Nothing is silently
  ignored.
- **absent with a reason** — not implemented and not configurable, with what is missing named.

"Partial" is not a verdict. Where something is present in part, the row says which part is
missing (0013 §4.7 clause 6).

**Versions every measured claim was measured against.** libzmq **4.3.5**, `pkg-config
--modversion libzmq` = 4.3.5, linked against libsodium 26, reached through the `zmq` crate
0.9.2; and the pure-Rust `zeromq` crate **0.6.0**. Both were run against this library over
loopback TCP on 2026-09-11 (`docs/research/zeromq.md` §14 sources [40] and [41]). Claims about
what libzmq *specifies* cite that sheet's section numbers, which cite libzmq's manual pages,
the ZeroMQ RFCs and the zguide. Nothing in this document is inferred from a version other than
those two.

## 2. Socket types

`zmq_socket(3)`'s own table is reproduced in the sheet's §4.1 with seven columns; the
compatible-peers, direction, routing and mute-state rows are the ones an implementation can be
held to. Here they are, with the verdict and where the behaviour lives. "Mute action" is the
socket's `PipeConfig`, which is the one place the choice is made, and the compatible-peers row
is enforced at the handshake by `SocketType::accepts` in the codec, called from
`session.rs` — a socket paired with a type its table does not list is refused with
`ENOCOMPATPROTO` rather than left to misbehave later.

| `zmq_socket(3)` type | Verdict | Type here | Mute action, send/receive and routing as implemented |
| --- | --- | --- | --- |
| `ZMQ_REQ` | present | `ReqSocket` (`reqrep.rs`) | block both ways; strict send/receive alternation with `EFSM` on any other order; round-robin out, last peer in; the empty delimiter prepended and stripped; `ZMQ_REQ_CORRELATE` and `ZMQ_REQ_RELAXED` |
| `ZMQ_REP` | present | `RepSocket` (`reqrep.rs`) | outgoing **drop**, incoming block — the manual omits REP's mute row and 28/REQREP says the reply to a vanished originator is discarded and sending never blocks; receive/send alternation, envelope stored and restored, fair-queued in |
| `ZMQ_DEALER` | present | `DealerSocket` (`dealerrouter.rs`) | block both ways; unrestricted; round-robin out, fair-queued in; never discards |
| `ZMQ_ROUTER` | present | `RouterSocket` (`dealerrouter.rs`) | outgoing **drop**, incoming block; routing-id frame prepended and stripped, peer-chosen `Identity` honoured; `ZMQ_ROUTER_MANDATORY` turns the drop into `EHOSTUNREACH`, `ZMQ_ROUTER_HANDOVER` evicts an incumbent, `ZMQ_PROBE_ROUTER` sends the empty message |
| `ZMQ_PUB` | present | `PubSocket` (`pubsub.rs`) | outgoing **drop**; send only; publisher-side binary prefix match on the first frame; fan out |
| `ZMQ_SUB` | present | `SubSocket` (`pubsub.rs`) | incoming **drop** — 29/PUBSUB's receiving-side rule, which the manual's table omits for SUB; receive only, fair-queued; subscriptions additive and non-idempotent; both wire forms accepted, the sent one configurable |
| `ZMQ_XPUB` | present | `XPubSocket` (`xpubxsub.rs`) | outgoing **drop**; subscriptions delivered to the application in the `%x01`/`%x00` form, a departure synthesized as an unsubscribe; `ZMQ_XPUB_VERBOSE`, `_VERBOSER`, `_MANUAL`, `_WELCOME_MSG` |
| `ZMQ_XSUB` | present | `XSubSocket` (`xpubxsub.rs`) | drop both ways; subscriptions forwarded upstream and re-sent at every handshake, with their counts |
| `ZMQ_PUSH` | present | `PushSocket` (`pipeline.rs`) | outgoing block; send only; round-robin over peers whose queue has room; never discards |
| `ZMQ_PULL` | present | `PullSocket` (`pipeline.rs`) | incoming block; receive only, fair-queued |
| `ZMQ_PAIR` | present | `PairSocket` (`pair.rs`) | block both ways; at most one peer, no auto-reconnect, further inbound connections terminated while one is live |
| `ZMQ_STREAM` | **absent** — what is missing: raw TCP with no ZMTP at all. Its peers column is "none": a STREAM socket talks to a non-ZeroMQ client, prepends a routing id to every part, and signals connect and disconnect with a zero-length message. This library is a ZMTP implementation; a socket that speaks no ZMTP shares no code with it, and no caller has asked. | — | — |
| `ZMQ_CLIENT`, `ZMQ_SERVER` | **absent** — libzmq **DRAFT** (41/CLIENTSERVER): "in DRAFT state, not yet available in stable releases", so a stable-build peer cannot use them either. What is missing: the thread-safe socket family and its single-frame-message rule | — | — |
| `ZMQ_RADIO`, `ZMQ_DISH` | **absent** — DRAFT (48/RADIO-DISH), and additionally the group family is defined over `udp`, which §3 lists as absent. What is missing: groups, `zmq_join`/`zmq_leave`, and the datagram transport under them | — | — |
| `ZMQ_SCATTER`, `ZMQ_GATHER` | **absent** — DRAFT (49/SCATTERGATHER). What is missing: the thread-safe pipeline pair | — | — |
| `ZMQ_PEER` | **absent** — DRAFT (51/P2P). What is missing: `zmq_connect_peer` and the caller-visible routing id it returns | — | — |
| `ZMQ_CHANNEL` | **absent** — DRAFT (52/CHANNEL). What is missing: the thread-safe exclusive pair | — | — |

Eleven of the twelve stable types are present; `ZMQ_STREAM` is the one stable type that is
not, and its row says what a caller who wants it does not get. The eight draft types are
absent as a family, for the one reason that they are draft — `weida-zmtp`'s metadata table
still *parses* their `Socket-Type` names, because a peer may announce one and a decoder that
refused the name could not report the pairing refusal properly.

## 3. Transports

The sheet's §12 inventories libzmq's transports. `tcp`, `ipc` and `inproc` are present; every
other name in that inventory is refused by `Endpoint::parse` with `EPROTONOSUPPORT` and the
reason below, from `endpoint.rs`'s `ABSENT_TRANSPORTS` table — a name it does not know at all
is refused too, with "this library speaks tcp, ipc and inproc".

| Transport | Verdict |
| --- | --- |
| `tcp://` | present — wildcard, IP literal and name forms kept apart; `ZMQ_LAST_ENDPOINT` readable after a wildcard bind; `DEFAULT_MAX_RESOLVED_ADDRESSES` = 8 caps what a name may expand to |
| `ipc://` | present on Unix — `AF_UNIX` with unlink-then-bind, an explicit mode, a socket-type check before unlinking, the 113-octet Linux path budget (`MAX_IPC_ENDPOINT_BYTES`), and peer credentials available to authorization; the endpoint-stealing hazard is documented rather than papered over |
| `inproc://` | present — context-scoped namespace, libzmq's 256-character budget (`MAX_INPROC_NAME_BYTES`), connect-before-bind working as libzmq 4.0 fixed it, and two contexts in one process never meeting |
| `pgm://`, `epgm://` | refused with a reason: reliable multicast is out of scope until a user asks. What is missing: the multicast family, the datagram-alignment offset field and subscriber-side filtering |
| `udp://` | refused with a reason: the multicast/datagram family is out of scope. What is missing: RADIO/DISH, which is the only pattern libzmq allows on it |
| `ws://`, `wss://` | refused with a reason: WebSockets are a draft-gated libzmq feature ("disabled by default if DRAFT APIs are disabled") and out of scope. What is missing: 45/ZWS's `Sec-WebSocket-Protocol` negotiation, its flag-byte framing and, for `wss`, the TLS stack |
| `vmci://`, `vsock://` | refused with a reason: hypervisor transports, out of scope |
| `tipc://` | refused with a reason: cluster transport, out of scope |

## 4. Mechanisms and authentication

One mechanism per socket, announced in the greeting and never negotiated, which is 37/ZMTP's
"security in ZMTP is *assertive*".

| Mechanism | Verdict |
| --- | --- |
| NULL (37/ZMTP) | present — greeting with the 3.0 downgrade, `READY` metadata with `Socket-Type` and `Identity`, `PING`/`PONG` gated on the negotiated version, `ERROR` sent and understood |
| PLAIN (24/ZMTP-PLAIN) | present — `HELLO`/`WELCOME`/`INITIATE`/`READY` with username and password, either side, and the ZAP dialog behind it |
| CURVE (25/ZMTP-CURVE, 26/CURVEZMQ) | present — the four keys; `HELLO` 200, `WELCOME` 168, `INITIATE` 257+, `READY` 30+ octets; the cookie discarded by a valid `INITIATE` or by `COOKIE_LIFETIME` (60 s); the nonce prefixes and short counters, which never repeat within a connection; session keys destroyed when the connection closes; the three security models; the peer's long-term key handed to the ZAP handler as the credential. One cryptographic dependency, `crypto_box` (RustCrypto's NaCl `crypto_box`: XSalsa20-Poly1305 over X25519), and `unsafe_code = "forbid"` |
| ZAP (27/ZAP) | present — request and reply framing over `inproc://zeromq.zap.01`, one handler per context, status 200/300/400/500, the user-id field, `ZMQ_ZAP_DOMAIN` as the switch that turns authorization on, `ZMQ_ZAP_ENFORCE_DOMAIN` checked at configuration time; a 400 refuses the connection before any message flows, and the user id never becomes a weida identity |
| GSSAPI | **absent** — what is missing: the mechanism itself and a Kerberos stack under it. Its six options are refused by name (§5), because a mechanism this library cannot speak must not be announced in a greeting |
| Private mechanisms | **absent** — 37/ZMTP permits them and this library has none; a peer announcing an unknown mechanism is refused at the greeting with `ENOCOMPATPROTO` |

## 5. Options

Every `zmq_setsockopt` and `zmq_ctx_set` option of the sheet's inventory, from
`weida-zmq`'s `optiontable.rs`, which is the code this table is generated from and which a
test walks in full: **98 rows — 38 honoured, 60 refused** (24 absent with what is missing
named, 14 for a transport this library does not implement, 9 replaced by a `weida-runtime`
construct, 9 libzmq DRAFT options, 4 deprecated by libzmq itself in favour of ZAP). Eleven are
`zmq_ctx_set`, 87 `zmq_setsockopt`.

Read-only options are deliberately not rows: `ZMQ_EVENTS`, `ZMQ_FD`, `ZMQ_LAST_ENDPOINT`,
`ZMQ_SOCKET_LIMIT`, `ZMQ_MSG_T_SIZE` and `ZMQ_MECHANISM` are `zmq_getsockopt`/`zmq_ctx_get`
only, so there is no configuration-time decision to record. `ZMQ_LAST_ENDPOINT` is answered by
`Engine`'s own accessor; `ZMQ_FD` has no counterpart at all, because this library's readiness
is a future rather than a descriptor.

| Option | Call | Verdict |
| --- | --- | --- |
| `ZMQ_IO_THREADS` | `zmq_ctx_set` | **refused** — replaced by a weida-runtime construct: ContextConfig::worker_threads sizes the reactor Context::owned creates, and Context::new joins a reactor somebody else sized; the I/O thread pool is a Tokio runtime here and there is no second pool to count |
| `ZMQ_MAX_SOCKETS` | `zmq_ctx_set` | **honoured** as `ContextConfig::max_sockets` — default 1023 as in libzmq |
| `ZMQ_MAX_MSGSZ` | `zmq_ctx_set` | **refused** — absent: a context-wide message ceiling on top of the per-socket one; ZMQ_MAXMSGSIZE is where a message is bounded here, and two ceilings for one question is how a message gets refused for a reason nobody can find |
| `ZMQ_BLOCKY` | `zmq_ctx_set` | **refused** — replaced by a weida-runtime construct: ContextConfig::close_budget is finite by default, so there is no block-forever to switch off; ZMQ_BLOCKY exists because libzmq's default is the other way |
| `ZMQ_ZERO_COPY_RECV` | `zmq_ctx_set` | **refused** — draft only: it is a libzmq DRAFT option, absent from stable builds, so no peer can rely on it |
| `ZMQ_BUSY_POLL` | `zmq_ctx_set` | **refused** — replaced by a weida-runtime construct: how the reactor waits is the reactor's business; a Tokio runtime is not busy-pollable from here |
| `ZMQ_THREAD_SCHED_POLICY` | `zmq_ctx_set` | **refused** — replaced by a weida-runtime construct: the reactor owns its threads: Context::owned creates and names them, Context::new and Context::with_handle borrow somebody else's |
| `ZMQ_THREAD_PRIORITY` | `zmq_ctx_set` | **refused** — replaced by a weida-runtime construct: the reactor owns its threads |
| `ZMQ_THREAD_AFFINITY_CPU_ADD` | `zmq_ctx_set` | **refused** — replaced by a weida-runtime construct: the reactor owns its threads |
| `ZMQ_THREAD_AFFINITY_CPU_REMOVE` | `zmq_ctx_set` | **refused** — replaced by a weida-runtime construct: the reactor owns its threads |
| `ZMQ_THREAD_NAME_PREFIX` | `zmq_ctx_set` | **refused** — replaced by a weida-runtime construct: Context::owned names its reactor's threads "weida-zmq" |
| `ZMQ_SNDHWM` | `zmq_setsockopt` | **honoured** as `SocketOptions::pipe.outgoing.hwm` — in messages, default 1000 |
| `ZMQ_RCVHWM` | `zmq_setsockopt` | **honoured** as `SocketOptions::pipe.incoming.hwm` — in messages, default 1000 |
| `ZMQ_MAXMSGSIZE` | `zmq_setsockopt` | **honoured** as `SocketOptions::max_message_size` — bounded by default where libzmq has no limit, which is 0013 §4.4 item 5's second deliberate deviation (DEFAULT_MAX_MESSAGE_SIZE) |
| `ZMQ_LINGER` | `zmq_setsockopt` | **honoured** as `ContextConfig::close_budget` — finite by default where libzmq's is infinite, which is 0013 §4.4 item 5's first deliberate deviation (DEFAULT_CLOSE_BUDGET) |
| `ZMQ_SNDTIMEO` | `zmq_setsockopt` | **honoured** as `SocketOptions::send_timeout` |
| `ZMQ_RCVTIMEO` | `zmq_setsockopt` | **honoured** as `SocketOptions::recv_timeout` |
| `ZMQ_SNDBUF` | `zmq_setsockopt` | **refused** — absent: kernel socket buffers are left at the OS default; what this library bounds is the message queue, in messages, under ZMQ_SNDHWM |
| `ZMQ_RCVBUF` | `zmq_setsockopt` | **refused** — absent: kernel socket buffers are left at the OS default; ZMQ_RCVHWM is the queue this library bounds |
| `ZMQ_CONFLATE` | `zmq_setsockopt` | **refused** — absent: keep-only-the-last is a queue this library does not have, and §11 names it as one of the four unbounded-resource shapes: "the queue and memory will grow with each message received" on an inbound socket nobody reads |
| `ZMQ_AFFINITY` | `zmq_setsockopt` | **refused** — replaced by a weida-runtime construct: which I/O thread serves a connection: there is one reactor and it schedules its own tasks |
| `ZMQ_RECONNECT_IVL` | `zmq_setsockopt` | **honoured** as `SocketOptions::reconnect_ivl` — None for libzmq's -1 |
| `ZMQ_RECONNECT_IVL_MAX` | `zmq_setsockopt` | **honoured** as `SocketOptions::reconnect_ivl_max` — None for libzmq's 0 |
| `ZMQ_RECONNECT_STOP` | `zmq_setsockopt` | **refused** — draft only: it is a libzmq DRAFT option, absent from stable builds, so no peer can rely on it |
| `ZMQ_HANDSHAKE_IVL` | `zmq_setsockopt` | **honoured** as `SocketOptions::handshake_ivl` — finite by default |
| `ZMQ_CONNECT_TIMEOUT` | `zmq_setsockopt` | **honoured** as `SocketOptions::connect_timeout` |
| `ZMQ_IMMEDIATE` | `zmq_setsockopt` | **honoured** as `SocketOptions::immediate` |
| `ZMQ_BACKLOG` | `zmq_setsockopt` | **honoured** as `SocketOptions::backlog` — default 100 |
| `ZMQ_HEARTBEAT_IVL` | `zmq_setsockopt` | **honoured** as `SocketOptions::heartbeat_ivl` |
| `ZMQ_HEARTBEAT_TIMEOUT` | `zmq_setsockopt` | **honoured** as `SocketOptions::heartbeat_timeout` |
| `ZMQ_HEARTBEAT_TTL` | `zmq_setsockopt` | **honoured** as `SocketOptions::heartbeat_ttl` — deciseconds on the wire |
| `ZMQ_TCP_KEEPALIVE` | `zmq_setsockopt` | **refused** — absent: no TCP keepalive is configured; ZMQ_HEARTBEAT_IVL is the liveness this library offers, and it works on every transport rather than on TCP alone |
| `ZMQ_TCP_KEEPALIVE_IDLE` | `zmq_setsockopt` | **refused** — absent: no TCP keepalive is configured; see ZMQ_TCP_KEEPALIVE |
| `ZMQ_TCP_KEEPALIVE_CNT` | `zmq_setsockopt` | **refused** — absent: no TCP keepalive is configured; see ZMQ_TCP_KEEPALIVE |
| `ZMQ_TCP_KEEPALIVE_INTVL` | `zmq_setsockopt` | **refused** — absent: no TCP keepalive is configured; see ZMQ_TCP_KEEPALIVE |
| `ZMQ_TCP_MAXRT` | `zmq_setsockopt` | **refused** — absent: TCP_USER_TIMEOUT is not set; ZMQ_HANDSHAKE_IVL bounds a handshake and ZMQ_HEARTBEAT_IVL bounds a silence |
| `ZMQ_TOS` | `zmq_setsockopt` | **refused** — absent: no IP type-of-service is set; the OS default applies |
| `ZMQ_BINDTODEVICE` | `zmq_setsockopt` | **refused** — absent: SO_BINDTODEVICE is not set; an endpoint's interface is chosen by the address it names |
| `ZMQ_IPV6` | `zmq_setsockopt` | **refused** — absent: IPv6 is never disabled: every address a name resolves to is dialled whatever its family, so there is no ZMQ_IPV6=0 to honour |
| `ZMQ_USE_FD` | `zmq_setsockopt` | **refused** — absent: binding a file descriptor this library did not open; weida-runtime's bind hygiene is what the ipc transport relies on and an adopted descriptor would bypass it |
| `ZMQ_ROUTING_ID` | `zmq_setsockopt` | **honoured** as `SocketOptions::routing_id` — 1-255 octets with a nonzero first one |
| `ZMQ_IDENTITY` | `zmq_setsockopt` | **honoured** as `SocketOptions::routing_id` — libzmq's own deprecated name for ZMQ_ROUTING_ID, and the wire property is still called Identity |
| `ZMQ_ROUTER_MANDATORY` | `zmq_setsockopt` | **honoured** as `SocketOptions::router_mandatory` |
| `ZMQ_ROUTER_HANDOVER` | `zmq_setsockopt` | **honoured** as `SocketOptions::router_handover` |
| `ZMQ_PROBE_ROUTER` | `zmq_setsockopt` | **honoured** as `SocketOptions::probe_router` — refused on other socket types |
| `ZMQ_ROUTER_NOTIFY` | `zmq_setsockopt` | **refused** — draft only: it is a libzmq DRAFT option, absent from stable builds, so no peer can rely on it |
| `ZMQ_CONNECT_ROUTING_ID` | `zmq_setsockopt` | **refused** — absent: assigning the next peer's routing id from the connecting side; a ROUTER here addresses a peer by the identity it announced or by one this library gave it |
| `ZMQ_REQ_CORRELATE` | `zmq_setsockopt` | **honoured** as `SocketOptions::req_correlate` |
| `ZMQ_REQ_RELAXED` | `zmq_setsockopt` | **honoured** as `SocketOptions::req_relaxed` — which requires req_correlate |
| `ZMQ_SUBSCRIBE` | `zmq_setsockopt` | **honoured** as `SubSocket::subscribe` — and XSubSocket::subscribe |
| `ZMQ_UNSUBSCRIBE` | `zmq_setsockopt` | **honoured** as `SubSocket::unsubscribe` — and XSubSocket::unsubscribe |
| `ZMQ_XPUB_VERBOSE` | `zmq_setsockopt` | **honoured** as `SocketOptions::xpub_verbose` |
| `ZMQ_XPUB_VERBOSER` | `zmq_setsockopt` | **honoured** as `SocketOptions::xpub_verboser` |
| `ZMQ_XPUB_MANUAL` | `zmq_setsockopt` | **honoured** as `SocketOptions::xpub_manual` — with XPubSocket::subscribe applying one |
| `ZMQ_XPUB_WELCOME_MSG` | `zmq_setsockopt` | **honoured** as `SocketOptions::xpub_welcome_msg` — sent on connect and reconnect |
| `ZMQ_XPUB_MANUAL_LAST_VALUE` | `zmq_setsockopt` | **refused** — draft only: it is a libzmq DRAFT option, absent from stable builds, so no peer can rely on it |
| `ZMQ_XPUB_NODROP` | `zmq_setsockopt` | **refused** — absent: a publisher drops at the high-water mark, which 29/PUBSUB requires - "SHALL silently drop the message if the queue for a subscriber is full"; reporting EAGAIN to the publisher instead is not implemented |
| `ZMQ_INVERT_MATCHING` | `zmq_setsockopt` | **refused** — absent: sending to everything except the matching subscribers, which must be set on both ends to work; the matcher here matches prefixes and does not invert |
| `ZMQ_STREAM_NOTIFY` | `zmq_setsockopt` | **refused** — absent: ZMQ_STREAM is not a socket type here, so there are no connect and disconnect messages to switch on |
| `ZMQ_HELLO_MSG` | `zmq_setsockopt` | **refused** — draft only: it is a libzmq DRAFT option, absent from stable builds, so no peer can rely on it |
| `ZMQ_DISCONNECT_MSG` | `zmq_setsockopt` | **refused** — draft only: it is a libzmq DRAFT option, absent from stable builds, so no peer can rely on it |
| `ZMQ_HICCUP_MSG` | `zmq_setsockopt` | **refused** — draft only: it is a libzmq DRAFT option, absent from stable builds, so no peer can rely on it |
| `ZMQ_METADATA` | `zmq_setsockopt` | **refused** — absent: application metadata properties in the handshake; what this library sends is Socket-Type and Identity, and a peer's other properties are readable rather than settable |
| `ZMQ_IN_BATCH_SIZE` | `zmq_setsockopt` | **refused** — draft only: it is a libzmq DRAFT option, absent from stable builds, so no peer can rely on it |
| `ZMQ_OUT_BATCH_SIZE` | `zmq_setsockopt` | **refused** — draft only: it is a libzmq DRAFT option, absent from stable builds, so no peer can rely on it |
| `ZMQ_PLAIN_SERVER` | `zmq_setsockopt` | **honoured** as `SocketOptions::plain_server` |
| `ZMQ_PLAIN_USERNAME` | `zmq_setsockopt` | **honoured** as `SocketOptions::plain_username` — which selects PLAIN as the client |
| `ZMQ_PLAIN_PASSWORD` | `zmq_setsockopt` | **honoured** as `SocketOptions::plain_password` |
| `ZMQ_CURVE_SERVER` | `zmq_setsockopt` | **honoured** as `SocketOptions::curve_server` |
| `ZMQ_CURVE_PUBLICKEY` | `zmq_setsockopt` | **honoured** as `SocketOptions::curve_publickey` — 32 octets or 40 characters of Z85 |
| `ZMQ_CURVE_SECRETKEY` | `zmq_setsockopt` | **honoured** as `SocketOptions::curve_secretkey` — 32 octets or 40 characters of Z85 |
| `ZMQ_CURVE_SERVERKEY` | `zmq_setsockopt` | **honoured** as `SocketOptions::curve_serverkey` — which selects CURVE as the client |
| `ZMQ_ZAP_DOMAIN` | `zmq_setsockopt` | **honoured** as `SocketOptions::zap_domain` — the switch that turns authorization on |
| `ZMQ_ZAP_ENFORCE_DOMAIN` | `zmq_setsockopt` | **honoured** as `SocketOptions::zap_enforce_domain` — checked at configuration time |
| `ZMQ_GSSAPI_SERVER` | `zmq_setsockopt` | **refused** — absent: the GSSAPI mechanism; this library announces NULL, PLAIN or CURVE in the greeting and a mechanism it cannot speak must not be announced |
| `ZMQ_GSSAPI_PLAINTEXT` | `zmq_setsockopt` | **refused** — absent: the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER |
| `ZMQ_GSSAPI_PRINCIPAL` | `zmq_setsockopt` | **refused** — absent: the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER |
| `ZMQ_GSSAPI_SERVICE_PRINCIPAL` | `zmq_setsockopt` | **refused** — absent: the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER |
| `ZMQ_GSSAPI_PRINCIPAL_NAMETYPE` | `zmq_setsockopt` | **refused** — absent: the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER |
| `ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE` | `zmq_setsockopt` | **refused** — absent: the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER |
| `ZMQ_TCP_ACCEPT_FILTER` | `zmq_setsockopt` | **refused** — deprecated in favour of ZAP: libzmq's own manual says so, and the ZAP dialog is implemented here |
| `ZMQ_IPC_FILTER_UID` | `zmq_setsockopt` | **refused** — deprecated in favour of ZAP: libzmq's own manual says so, and the ZAP dialog is implemented here |
| `ZMQ_IPC_FILTER_GID` | `zmq_setsockopt` | **refused** — deprecated in favour of ZAP: libzmq's own manual says so, and the ZAP dialog is implemented here |
| `ZMQ_IPC_FILTER_PID` | `zmq_setsockopt` | **refused** — deprecated in favour of ZAP: libzmq's own manual says so, and the ZAP dialog is implemented here |
| `ZMQ_RATE` | `zmq_setsockopt` | **refused** — no transport: this library does not implement pgm, epgm or norm multicast |
| `ZMQ_RECOVERY_IVL` | `zmq_setsockopt` | **refused** — no transport: this library does not implement pgm, epgm or norm multicast |
| `ZMQ_MULTICAST_HOPS` | `zmq_setsockopt` | **refused** — no transport: this library does not implement pgm, epgm or udp multicast |
| `ZMQ_MULTICAST_MAXTPDU` | `zmq_setsockopt` | **refused** — no transport: this library does not implement pgm or epgm multicast |
| `ZMQ_SOCKS_PROXY` | `zmq_setsockopt` | **refused** — no transport: this library does not implement a SOCKS5 client for outbound tcp |
| `ZMQ_WSS_KEY_PEM` | `zmq_setsockopt` | **refused** — no transport: this library does not implement ws or wss, which is DRAFT in libzmq and needs GnuTLS |
| `ZMQ_WSS_CERT_PEM` | `zmq_setsockopt` | **refused** — no transport: this library does not implement ws or wss |
| `ZMQ_WSS_TRUST_PEM` | `zmq_setsockopt` | **refused** — no transport: this library does not implement ws or wss |
| `ZMQ_WSS_HOSTNAME` | `zmq_setsockopt` | **refused** — no transport: this library does not implement ws or wss |
| `ZMQ_WSS_TRUST_SYSTEM` | `zmq_setsockopt` | **refused** — no transport: this library does not implement ws or wss |
| `ZMQ_VMCI_BUFFER_SIZE` | `zmq_setsockopt` | **refused** — no transport: this library does not implement vmci |
| `ZMQ_VMCI_BUFFER_MIN_SIZE` | `zmq_setsockopt` | **refused** — no transport: this library does not implement vmci |
| `ZMQ_VMCI_BUFFER_MAX_SIZE` | `zmq_setsockopt` | **refused** — no transport: this library does not implement vmci |
| `ZMQ_VMCI_CONNECT_TIMEOUT` | `zmq_setsockopt` | **refused** — no transport: this library does not implement vmci |

## 6. Observability

`zmq_socket_monitor`'s event set, from `monitor.rs`. Two renderings of one publisher: a typed
stream, and the two-frame `inproc://` PAIR form the Espresso recipe reads, produced by
`MonitorEvent::encode` and read back by `decode`, so the octets are asserted in both
directions.

| `ZMQ_EVENT_*` | Id | Verdict |
| --- | --- | --- |
| `CONNECTED` | `0x0001` | present |
| `CONNECT_DELAYED` | `0x0002` | present — the state `ZMQ_IMMEDIATE` is about |
| `CONNECT_RETRIED` | `0x0004` | present, with the recalculated interval in the value field |
| `LISTENING` | `0x0008` | present, carrying the endpoint actually bound with the wildcard port resolved |
| `BIND_FAILED` | `0x0010` | present |
| `ACCEPTED` | `0x0020` | present |
| `ACCEPT_FAILED` | `0x0040` | present — including a refusal by this socket's own peer ceiling, which libzmq has no equivalent of (§9) |
| `CLOSED` | `0x0080` | present |
| `CLOSE_FAILED` | `0x0100` | **present as a mask bit, never emitted.** What is missing: a close that reports a failure. Closing here drops a pipe and lets the session end; there is no path that can fail and be reported, so no event is synthesized to fill the gap |
| `DISCONNECTED` | `0x0200` | present |
| `MONITOR_STOPPED` | `0x0400` | present, with an empty endpoint, because the socket rather than one connection is what stopped |
| `HANDSHAKE_FAILED_NO_DETAIL` | `0x0800` | present — a closed connection, a timeout, an I/O error |
| `HANDSHAKE_SUCCEEDED` | `0x1000` | present |
| `HANDSHAKE_FAILED_PROTOCOL` | `0x2000` | present, with the difference in the next paragraph |
| `HANDSHAKE_FAILED_AUTH` | `0x4000` | present — a ZAP handler's refusal or a mechanism's own check |
| `ZMQ_EVENT_ALL` | `0xFFFF` | present as `MonitorEvents::ALL`; libzmq's own warning that a strict and fixed sequence of events must not be read from it applies here too |

Two differences, both deliberate and both visible to a reader of the wire form:

1. **A failure carries a sentence, not a number.** libzmq's value field holds a
   `ZMQ_PROTOCOL_ERROR_*` code for `HANDSHAKE_FAILED_PROTOCOL` and the ZAP status for
   `HANDSHAKE_FAILED_AUTH`; here the value field is zero for every event except
   `CONNECT_RETRIED`, and a **third frame** carries this library's own reason text. What is
   missing for a caller matching on numbers: the `ZMQ_PROTOCOL_ERROR_*` enumeration. What is
   gained is a reason a human can read in a log, which is what those events are for; the
   two-frame form is preserved for the events that have no reason.
2. **No transport restriction by construction.** libzmq's page says the monitor "supports only
   connection-oriented transports, that is, TCP, IPC, and TIPC". Here the events are published
   at the engine's lifecycle points, which are the same for every transport this library has.
   The tests cover `tcp`; a monitor on `inproc` is therefore [inference] rather than measured.

`zmq_proxy`'s capture socket is the message-level half of observability and is in §7.
`MONITOR_CAPACITY` = 1024 events per monitor, oldest dropped first — a bound libzmq does not
have (§9).

## 7. Devices and helpers

| libzmq | Verdict |
| --- | --- |
| `zmq_proxy(frontend, backend, capture)` | present — `proxy()`, over the typed socket surfaces behind the `Device` trait (PAIR, DEALER, ROUTER, XSUB, XPUB, PUB, SUB, PUSH, PULL). Both directions, one message held at a time, and a copy of every message to the capture socket including the subscription control frames. Returns the counters it reached rather than only an error code |
| `zmq_proxy_steerable(frontend, backend, capture, control)` | present — `proxy_steerable()`, with `PAUSE`, `RESUME`, `TERMINATE` and `STATISTICS`; `STATISTICS` replies with libzmq's eight `u64` counters, and `PAUSE` stops reading both data sockets so their queues fill and their peers feel it |
| The three canonical configurations — shared queue (ROUTER/DEALER), forwarder (XSUB/XPUB), streamer (PULL/PUSH) | present — as socket-type pairings of `proxy()`, not as separate functions; libzmq ships them the same way |
| `zmq_device(3)` | **absent** — removed from libzmq itself in favour of `zmq_proxy`. What is missing: nothing a 4.x caller can reach |
| `zmq_poll`, `zmq_poller` | **absent by construction** — what is missing: a descriptor-set poller. Readiness here is a future, and `tokio::select!` over socket futures is what a `zmq_poll` loop becomes; every zguide recipe in `examples/` is such a loop, which is the evidence that the shape survives the translation |

## 8. Interop evidence

Both directions of clause 4, each pairing with our socket as the **connecting** side and as
the **bound** side, in the ordinary test suite (no supervised process, no script to remember).

| Against | How | Tests | Result |
| --- | --- | --- | --- |
| `zeromq` 0.6.0 (pure Rust) | dev-dependency, always runs | 13 (`tests/interop_zmqrs.rs`) | all pass; ten pairings — REQ/REP, DEALER/ROUTER, PUSH/PULL, PUB/SUB, XPUB/XSUB, each with our socket bound *and* connecting — plus a test that PAIR is absent from that crate, one for the suppressed heartbeat against a 3.0 peer, and one for the subscription form it does not understand |
| libzmq 4.3.5 through `zmq` 0.9.2 | optional dev-dependency behind the `libzmq-interop` feature, `#[ignore]` with the install command in the doc comment | 18 (`tests/interop_libzmq.rs`) | all pass; the same ten pairings plus PAIR both ways, PLAIN in both roles, CURVE in both roles, a ZAP refusal that stops a libzmq client, our heartbeat answered, and the `as-server` measurement of fact 1 below |

**Three measured skews against `zeromq` 0.6.0**, exercised rather than assumed: it announces a
3.0 greeting; it recognizes only the `READY` command name; and it sends and reads only ZMTP
2.0's one-frame subscription form. Two further disagreements were measured and recorded as
that crate's behaviour rather than ours: its XPUB applies a subscription only when the
application receives it, and its DEALER announces no `Identity`.

**Four measured facts about libzmq 4.3.5**, each observed in one exchange over loopback TCP and
none of them stated by its documentation (sheet §13, source [41]) — three of them determine
what our side sends or accepts:

1. The greeting's `as-server` octet is **0 even on a socket configured as the PLAIN or CURVE
   server**, against 24/ZMTP-PLAIN and 25/ZMTP-CURVE, and libzmq never reads the peer's octet
   either. Our guard is narrower accordingly: two peers that both claim the server role are
   refused, and the octet alone does not decide.
2. Every REQ, DEALER and ROUTER socket with no `ZMQ_ROUTING_ID` announces an `Identity`
   property with an **empty value**. 37/ZMTP's `identity = 0*255OCTET` permits it, so an empty
   property is the *absence* of an identity and is accepted; refusing it would fail half the
   matrix at the handshake.
3. A CURVE `MESSAGE` travels behind a **message frame header, not a command frame header**,
   although 26/CURVEZMQ calls it a command and its body is a command body — and a
   command-framed `MESSAGE` closes the connection. The handshake commands are command frames
   as specified, so both forms are mixed within one connection; `session.rs` sends and accepts
   the measured form.
4. Its PLAIN and CURVE servers refuse every connection without a ZAP handler bound on
   `inproc://zeromq.zap.01` in the same context — which 27/ZAP's "the handler SHALL start
   before any server starts" requires but no libzmq page states as a refusal.

Clause 5's evidence is `examples/` with `tests/zguide_*.rs`: nine recipes — lazy, simple and
paranoid pirate, the load-balancing broker inside Simple Pirate, Majordomo with `mmi.service`,
Freelance models one and two, Clone/CHP, Binary Star, the last-value cache and Espresso — each
asserting the guarantee its original claims, with the test files including the example files as
modules so that what is asserted is the code a reader runs.

## 9. Deliberate deviations and bounds libzmq does not have

**Two deliberate default changes** (0013 §4.4 item 5), both settable back to anything the type
allows and both rows in §5:

| | libzmq | here |
| --- | --- | --- |
| `ZMQ_LINGER` | infinite: `zmq_ctx_term` waits forever for pending messages | `DEFAULT_CLOSE_BUDGET` = 1 s, finite, because "termination waits forever" is how a ZeroMQ process hangs at shutdown |
| `ZMQ_MAXMSGSIZE` | "no limit" | `DEFAULT_MAX_MESSAGE_SIZE` = 1 MiB, because a frame may declare 2^63-1 octets and this option is the only defence against a peer that does |

**One behavioural difference found by a test rather than by reading**: `ZMQ_LINGER` maps to the
context's close budget at `Context::shutdown` and **not** to socket drop, so a socket dropped
before its session flushed loses its queued messages. Found by the blocking facade's first
REQ/REP test, and the reason its `hello_world` example loops like `hwserver.c` instead of
returning.

**Two errnos libzmq has no name for**, because it has nowhere to report them: `EACCES` for a
peer a ZAP handler refused, and `ENOCOMPATPROTO` for a dialog that malfunctioned — a peer
announcing a mechanism or socket type this side cannot pair with, or a malformed handshake.

**One API libzmq does not have, because C does not need it**: `split(self)` on DEALER,
ROUTER, PAIR, XPUB and XSUB, returning a sending and a receiving half onto the same
connections. libzmq's socket is a pointer any thread may use one at a time; a Rust socket is
a `Send`, non-`Sync` handle, and one task parked in `recv()` would otherwise hold the whole
socket — so the concurrency libzmq permits needs a type to say it. REQ and REP have
no `split`: 28/REQREP alternates, so two independent halves would promise what the protocol
forbids. Each half is still one operation at a time, and a ROUTER's two halves share one
routing table behind a lock that is never held across an `await`.

**Seven bounds that are not libzmq options at all.** libzmq bounds a message count per queue and
nothing else; every number below exists because no remote input may cause unbounded allocation
([INVARIANTS.md](../INVARIANTS.md)), and each is a named constant rather than a magic value:

| Bound | Default | What it bounds, and why libzmq has no option for it |
| --- | --- | --- |
| `SocketOptions::max_peers` | 1024 | how many peers one socket admits. libzmq bounds sockets per context (`ZMQ_MAX_SOCKETS`) but not connections per socket, so a stranger decides the multiplier on every per-peer queue |
| `QueueConfig::max_bytes` | 8 MiB | the payload one direction of one peer's queue holds. `ZMQ_SNDHWM`/`ZMQ_RCVHWM` count **messages**, so at the defaults one peer could hold 1000 × 1 MiB per direction — a product, not a bound. The message count keeps libzmq's meaning exactly; this refuses the peer that actually occupies the memory. A queue that is *empty* accepts one message of any size, so the real exposure is `max_bytes - 1 + max_message_size`, just under 9 MiB, and never a wait for room that cannot appear |
| `Subscriptions::max_subscriptions` | 1024 | distinct prefixes one peer may hold. 37/ZMTP makes subscriptions additive and non-idempotent, so N `SUBSCRIBE` commands cost N entries and the count is a peer's choice |
| `Subscriptions::max_subscription_bytes` | 256 | one prefix's length. 37/ZMTP's grammar is `subscription = *OCTET`, unbounded; a count alone is not a bound, because 1024 prefixes of a megabyte each is a megabyte times 1024 |
| `MessageLimits::max_message_frames` | 1024 | frames in one message. ZMTP bounds a frame's size through `ZMQ_MAXMSGSIZE` and the frame *count* not at all, so a peer could send a million empty frames |
| `MAX_ZAP_DOMAIN_BYTES` | 256 | the ZAP domain a caller may configure, which travels in every ZAP request |
| `MONITOR_CAPACITY` | 1024 | events one monitor holds before the oldest are dropped, which is a drop and not a silence |

`INPROC_BUFFER_BYTES` (64 KiB) and `DEFAULT_MAX_RESOLVED_ADDRESSES` (8) are the same kind of
number for the `inproc` transport and the resolver.

**One capability libzmq's API does not have.** `XPubSocket::refuse` sends 37/ZMTP's `ERROR`
command to the subscriber whose subscription arrived last, and
`Pipe::refuse` is the channel underneath it. The protocol has always had the command — "the
peer SHALL treat an incoming ERROR command as fatal" — and libzmq's API exposes no way to send
one: an XPUB application can decline to *apply* a subscription (`ZMQ_XPUB_MANUAL`) and cannot
say why. That silence is unusable for a program that relays subscriptions, because a subscription silently ignored is a
subscriber waiting forever for messages nobody will send, so this library has the command and
the row is here rather than hidden. It is additive: nothing sends an `ERROR` unless an
application asks, and the connection is left to the peer's own rule.

**What replaces an option rather than refusing it.** Nine rows of §5 are
`RuntimeInstead`: libzmq's I/O-thread pool, thread affinity, priority, scheduling policy and
name prefix are a Tokio runtime here, sized by `ContextConfig::worker_threads`, and
`ZMQ_BLOCKY` has nothing to switch off because the close budget is already finite.

## 10. The definition of done

0013 §4.7's six clauses, each with its verdict and where this document proves it.

| Clause | Verdict |
| --- | --- |
| 1. Every socket type of `zmq_socket(3)`'s table implemented or absent with a reason, with the manual's rows asserted rather than described | **met** — §2: eleven of twelve stable types present with their mute, routing and alternation rows in the module tests; `ZMQ_STREAM` and the eight draft types absent with what is missing named |
| 2. `tcp`, `ipc` and `inproc`, with the rest named absent | **met** — §3 |
| 3. NULL, PLAIN and CURVE, with ZAP | **met** — §4; GSSAPI is the one mechanism absent, with its options refused by name |
| 4. Interop in both roles against libzmq and against zmq.rs, every pairing each supports | **met** — §8: 13 + 18 tests, both roles, four measured libzmq facts and three measured skews |
| 5. The zguide's canonical patterns as examples that execute in `cargo test`, each asserting the guarantee the guide claims | **met** — §8's last paragraph; nine recipes in `examples/`, asserted by `tests/zguide_*.rs` |
| 6. A feature-parity table, row by row, where no row says "partial" without saying what is missing | **met** — this document |

What a caller who compares this library with libzmq 4.3.5 does **not** get, in one list:
`ZMQ_STREAM`; the draft socket families and their transports; GSSAPI; the multicast, datagram,
WebSocket, hypervisor and cluster transports; `ZMQ_PROTOCOL_ERROR_*` numbers in monitor events
; a descriptor-based poller; and every option §5 marks
refused. Nothing else.

## 11. Sources

- [`../research/zeromq.md`](../research/zeromq.md) — §2 (context, monitor, proxy), §4.1 (the
  socket-type tables), §4.2-§4.6 (the pattern RFCs and the draft families), §5 (flow control
  and the mute state), §9 (the zguide recipes), §10 (security, ZAP, the CURVE octet counts),
  §11 (options and limits), §12 (transports), §13 (the measured libzmq and zmq.rs facts),
  §14 sources [17], [18], [22], [23], [40], [41].
- [0013](../decisions/0013-competitor-libraries.md) §4.1 (crate layout), §4.3 (`weida-zmtp`
  stays sans-I/O), §4.4 (the shape of `weida-zmq`, the option rule and the two default
  changes), §4.7 (the six clauses), §5.5 (the `kind` column).
- [INVARIANTS.md](../INVARIANTS.md) — the allocation invariant §9's bounds answer.
- Code the verdicts were read from, all under `crates/zmq/weida-zmq/src/`: `optiontable.rs`
  (§5, generated from `OPTIONS`), `endpoint.rs` (§3), `options.rs` and `curve.rs` and `zap.rs`
  (§4), `reqrep.rs`, `dealerrouter.rs`, `pubsub.rs`, `xpubxsub.rs`, `pipeline.rs`, `pair.rs`
  (§2), `monitor.rs` (§6), `proxy.rs` (§7), `session.rs` (the handshake refusals), plus
  `tests/interop_zmqrs.rs`, `tests/interop_libzmq.rs` and `tests/zguide_*.rs` (§8).
