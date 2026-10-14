# nanomsg/NNG (SP v1, measured against NNG 1.4.0-rc.0) — feature parity

Status: complete against the six clauses of
[0013](../decisions/0013-competitor-libraries.md) §4.7, read for SP rather than for ZMTP.
This document is the sixth clause itself — the row-by-row table — and §10 gives the other
five their verdicts.

The library is two crates: `weida-sp`, the sans-I/O SP codec whose `[dependencies]` is empty,
and `weida-nng`, the socket implementation on top of it. Neither depends on `weida`; a caller
who wants an SP implementation and nothing else uses `weida-nng` alone
([0013](../decisions/0013-competitor-libraries.md) §4.3, §4.4). It exposes SP's own protocols
and semantics. No crate assigns those protocols global weida counterparts.

## 1. What a row means

Three verdicts and no fourth:

- **present** — implemented, with the module or test that carries it named. A present row may
  still say where the behaviour differs, and then the difference is the row.
- **refused with a reason** — the option or URL exists in NNG and fails **here, at
  configuration time**, with a message naming why (0013 §4.4 item 4). Nothing is silently
  ignored.
- **absent with a reason** — not implemented and not configurable, with what is missing named.

"Partial" is not a verdict. Where something is present in part, the row says which part is
missing (0013 §4.7 clause 6).

**Versions every measured claim was measured against.** NNG **1.4.0-rc.0**, vendored and
built from source by `nng-sys` 1.4.0-rc.0 under the `nng` crate **1.0.1**, exercised over
loopback TCP on Linux x86-64 on 2026-09-12 by `crates/nng/weida-nng/tests/interop_nng.rs`.
Claims about what SP *specifies* cite [`../research/nanomsg-nng.md`](../research/nanomsg-nng.md),
which cites the nanomsg RFCs and the NNG manual; claims about what the *implementation* does
cite that sheet's §13 and the interop suite. Nothing here is inferred from a version other
than that one.

## 2. Protocols, cooked

SP v1 has eleven protocol numbers and the manual gives each a pattern, a peer and a direction
(`nng(7)`, sheet §4). All eleven are implemented. The pairing rule — a socket only talks to
the one protocol its row names as its peer — is enforced in the codec by
`ProtocolHeader::accepts`, called from the handshake in `session.rs`, so a wrong pairing is
refused as `NNG_EPROTO` when the 8-octet header arrives rather than misbehaving later. The
full-queue behaviour is the socket's `PipeConfig` and is asserted row by row in
`protocol.rs`'s tests against the sheet's sentence: PUSH and PAIR block, BUS, PUB and
SURVEYOR drop, and an incoming queue always blocks.

| `nng(7)` protocol | Verdict | Type here | Peer, direction and the behaviour as implemented |
| --- | --- | --- | --- |
| `nng_req0` | present | `ReqSocket`, `ReqCtx` (`reqrep.rs`) | peer REP; send then receive per context; a 31-bit request id with the terminal bit set, seeded at random and incremented per context (sheet §3); resend on `NNG_OPT_REQ_RESENDTIME`, checked at the `NNG_OPT_REQ_RESENDTICK` granularity and re-sent on a new pipe when one arrives; `NNG_ESTATE` for a receive with no request outstanding; buffer options refused by name because a context is one transaction |
| `nng_rep0` | present | `RepSocket`, `RepCtx` (`reqrep.rs`, on `replier.rs`) | peer REQ; receive then send per context; the tag stack of the received message remembered and written back in front of the reply; a reply with nothing received is `NNG_ESTATE`; a malformed tag stack makes the message unusable, so the message is dropped and the pipe closed |
| `nng_push0` | present | `PushSocket` (`pipeline.rs`) | peer PULL; send only; round-robin over the peers that can accept now; never discards — a send with no eligible peer waits and then reports `NNG_ETIMEDOUT` (sheet §4, §12) |
| `nng_pull0` | present | `PullSocket` (`pipeline.rs`) | peer PUSH; receive only, fair-queued over the pipes |
| `nng_pub0` | present | `PubSocket` (`pubsub.rs`) | peer SUB; send only; every connected peer gets a copy and a peer that cannot take one has that copy discarded, counted rather than silent (`Broadcast::dropped`) |
| `nng_sub0` | present | `SubSocket` (`pubsub.rs`) | peer PUB; receive only; **receiver-side** prefix filtering, which is where SP puts it (sheet §4); `NNG_OPT_SUB_SUBSCRIBE`/`_UNSUBSCRIBE` as byte prefixes, the empty prefix admitting everything, and `NNG_OPT_SUB_PREFNEW` choosing which end of a full queue is lost |
| `nng_surveyor0` | present | `SurveyorSocket`, `SurveyorCtx` (`survey.rs`, on `replier.rs`) | peer RESPONDENT; broadcast then collect; the deadline starts **at the send** and runs for `NNG_OPT_SURVEYOR_SURVEYTIME`; a survey id in the same tag-stack form as REQ's request id; answers to an expired survey discarded, and the expiry reported as `NNG_ETIMEDOUT` — indistinguishable from silence, which is what SP gives a surveyor |
| `nng_respondent0` | present | `RespondentSocket`, `RespondentCtx` (`survey.rs`, on `replier.rs`) | peer SURVEYOR; receive then answer, the stack returned in front of the answer; answering nothing is `NNG_ESTATE` |
| `nng_pair0` | present | `Pair0Socket` (`pair.rs`) | peer PAIR v0; one peer at a time, the second pairing refused at the `AddPre` hook and its connection closed; no per-protocol header at all, so the body is the payload |
| `nng_pair1` | present | `Pair1Socket` (`pair.rs`) | peer PAIR v1; one peer at a time; a 32-bit hop-count word in front of the body, compared against `NNG_OPT_MAXTTL` and dropped silently past it, the pipe kept (sheet §3, §11) |
| `nng_bus0` | present | `BusSocket` (`bus.rs`) | peer BUS; one hop and best effort; every directly connected peer and nobody else, a discarded copy still a successful send, and the pipe a message arrived on excluded from the re-broadcast |

**Contexts** (`nng_ctx`) exist for the four protocols NNG gives them to: `ReqCtx`, `RepCtx`,
`SurveyorCtx` and `RespondentCtx`, each with its own state, its own id and its own deadline,
so several transactions run in parallel over one socket (sheet §4). The `NNG_ESTATE` rule is per
context, and a test drives two contexts through overlapping surveys with independent
deadlines.

What a caller does **not** get from this section: `NNG_OPT_PAIR1_POLY`. PAIR v1 polyamorous
mode — one PAIR v1 socket with many peers, where a received message carries the pipe it came
from and a send may name one — is **refused with a reason** rather than implemented: NNG
deprecated it in 1.x, the API is a pipe handle rather than a routable identity, and a
polyamorous send to a departed peer decays into a silent discard with no way to tell it from
a delivery. A caller who wants many peers over one socket uses BUS, which is the pattern SP
defines for it.

## 3. Transports

`nng(7)` exposes seven transports in 1.10 and SP's RFCs define one more. Four are present;
the rest are refused by `Endpoint::parse` with `NNG_ENOTSUP` and the reason below, from
`endpoint.rs`'s `ABSENT_TRANSPORTS` table. A name the table does not know is refused too,
with "this library speaks tcp, tls+tcp, ipc and inproc". Every URL is bounded by
`NNG_MAXADDRLEN` = 128 before anything else is parsed.

| Transport | Verdict |
| --- | --- |
| `tcp://` | present — wildcard, IPv4/IPv6 literal and name forms kept apart; the bound port readable after a wildcard listen (`NNG_OPT_TCP_BOUND_PORT`, `NNG_OPT_URL`); `DEFAULT_MAX_ADDRESSES` = 8 caps what one name may expand to; Nagle off by default, as NNG's `NNG_OPT_TCP_NODELAY` is |
| `tls+tcp://` | present — TLS 1.2 and 1.3 over TCP through `tokio-rustls`, pure Rust, so `unsafe_code = "forbid"` survives; the four `NNG_OPT_TLS_AUTH_MODE` values, a CA file, a certificate/key file, `NNG_OPT_TLS_SERVER_NAME`, and the peer's verification state, common name and subject alternative names readable afterwards |
| `ipc://` | present on Unix — `AF_UNIX` with unlink-then-bind, a socket-type check before unlinking, both path budgets checked (`MAX_LEGACY_IPC_PATH_BYTES` = 121 for the legacy form, and the platform's `sun_path`), and the kernel's `SO_PEERCRED` answer available as uid, gid and pid |
| `inproc://` | present — context-scoped namespace on `weida-runtime`'s `NameRegistry`, a name bounded by `MAX_INPROC_NAME_BYTES` (`NNG_MAXADDRLEN` minus `inproc://`), a dial that waits for the bind rather than failing outright, as NNG's does, and two contexts in one process never meeting |
| `ws://`, `wss://` | refused with a reason: the WebSocket mapping needs an HTTP server and is out of scope until a user asks. What is missing: the `Sec-WebSocket-Protocol` negotiation of the SP WebSocket mapping, its binary framing, and the four `NNG_OPT_WS*` header options (§5 here) |
| `zt://` | refused with a reason: ZeroTier is experimental in NNG itself and its connection setup can take a minute. What is missing: the ZeroTier transport and its five options |
| `udp://` | refused with a reason: SP's UDP mapping is a datagram mapping with no pipe, and every pattern here is written on a pipe. What is missing: the datagram mapping |
| `socket://` | refused with a reason: NNG's inherited-file-descriptor form has no address to parse. What is missing: `nng_listener`/`nng_dialer` over a pre-opened socket pair |
| `abstract://` | refused with a reason: the Linux abstract `AF_UNIX` namespace has no filesystem permissions to check, so the one authorization `ipc` offers is gone. What is missing: the abstract-namespace form |
| `tcp4://`, `tcp6://`, `tls+tcp4://`, `tls+tcp6://` | refused with a reason: the address-family-pinned forms are absent; write a literal of the family you want, or a name whose resolved addresses this library tries in order |

## 4. Mechanisms and authentication

The first row is the one a reader should take away, and it is a property of SP rather than a
gap in this library.

| Question | Verdict |
| --- | --- |
| Message-level authentication | **absent in SP itself**: SP defines no authentication of any kind on a message. There is no mechanism negotiation, no handshake beyond the 8-octet protocol header, no credential frame and no signature. This library implements SP, so it has none either, and no option turns one on |
| Authorization | **absent in SP itself**: nothing in the protocol says who may send what to whom. What a deployment gets is the transport's answer (below) and the `AddPre` hook, which can refuse a connection before it becomes a pipe |
| Sender identity | **absent in SP itself**: a message carries a tag stack of device-local pipe ids and a request or survey id (sheet §3). None of those is an identity, none is authenticated, and none survives a hop through a device as anything but a routing breadcrumb. A caller who needs "who sent this" must carry it in the body and authenticate it themselves |
| TLS over TCP | present (§3 here) — and it authenticates **the transport peer only**: a certificate proves who terminated the TLS connection, for as long as that connection lasts. It says nothing about the origin of any message that arrived through it, and a device between two TLS connections breaks even that chain |
| IPC peer credentials | present (§3 here) — uid, gid and pid from the kernel at connection time, non-forgeable, and again **the transport peer only**: the process at the other end of this socket, not the author of a message it forwarded |
| A ZAP-like authorization dialog | **absent with a reason**: SP has nothing of the kind and inventing one would be a private protocol no NNG peer could speak. What is missing: per-connection authorization beyond the transport's answer and the `AddPre` refusal |

## 5. Options

Every option of `nng_options(5)` and the transport pages, from `weida-nng`'s
`optiontable.rs`, which is the code this table is generated from and which a test walks in
full: **50 rows — 33 honoured, 17 refused** (5 absent with what is missing named, 5 for a
transport NNG itself calls experimental, 4 for a transport this library does not implement,
3 replaced by a `weida-runtime` construct). By scope: 22 socket, 16 transport, 9 pipe, 2
listener, 1 dialer.

The five honoured rows a caller is most likely to want are `NNG_OPT_RECVMAXSZ`,
`NNG_OPT_SENDBUF`/`NNG_OPT_RECVBUF`, `NNG_OPT_SENDTIMEO`/`NNG_OPT_RECVTIMEO`,
`NNG_OPT_RECONNMINT`/`NNG_OPT_RECONNMAXT` and `NNG_OPT_MAXTTL`; `NNG_OPT_RECVMAXSZ` is also
settable **per endpoint**, before a dialer or listener starts, which is where NNG's manual
says it belongs (sheet §3).

| Option | Scope | Verdict |
| --- | --- | --- |
| `NNG_OPT_RECVMAXSZ` | socket, and per dialer/listener | honoured — the declared 64-bit length is checked before a body is allocated; `0` is NNG's "unlimited"; `inproc` accepts the option and deliberately ignores it, because peers there share an address space |
| `NNG_OPT_SENDBUF`, `NNG_OPT_RECVBUF` | socket | honoured — message depths `0..=8192` per pipe and per direction; zero is a rendezvous, which is PUSH's documented default; the protocols that permit neither refuse them by name |
| `NNG_OPT_SENDTIMEO`, `NNG_OPT_RECVTIMEO` | socket | honoured — `None` is NNG's `NNG_DURATION_INFINITE`; expiry is `NNG_ETIMEDOUT` |
| `NNG_OPT_RECONNMINT`, `NNG_OPT_RECONNMAXT` | socket, dialer | honoured — the first delay is the minimum and doubles to the maximum; `NNG_OPT_RECONNMAXT` = 0 is NNG's "no exponential backoff", every delay the minimum |
| `NNG_OPT_MAXTTL` | socket | honoured — `1..=255` as the manual documents, with a note that a real NNG node refuses a stack of 16 and publishes `NNG_MAX_TTL` = 15 beside it; both numbers are named constants here rather than one chosen silently |
| `NNG_OPT_PROTO`, `NNG_OPT_PROTONAME`, `NNG_OPT_PEER`, `NNG_OPT_PEERNAME` | socket | honoured — the protocol id and the peer's, with the names from the same registry the 12-bit header field uses |
| `NNG_OPT_URL`, `NNG_OPT_LOCADDR`, `NNG_OPT_REMADDR` | listener/dialer, pipe | honoured — read-only, as NNG's are; `NNG_OPT_URL` is what a wildcard listen is read back through |
| `NNG_OPT_REQ_RESENDTIME`, `NNG_OPT_REQ_RESENDTICK` | socket | honoured — the resend delay and the clock granularity that checks it |
| `NNG_OPT_SURVEYOR_SURVEYTIME` | socket | honoured — one second by default, as NNG's is |
| `NNG_OPT_SUB_SUBSCRIBE`, `NNG_OPT_SUB_UNSUBSCRIBE`, `NNG_OPT_SUB_PREFNEW` | socket | honoured — write-only prefixes as in NNG, and the full-queue end that `PREFNEW` chooses |
| `NNG_OPT_TCP_NODELAY`, `NNG_OPT_TCP_BOUND_PORT` | transport, listener | honoured |
| `NNG_OPT_TLS_AUTH_MODE`, `_CA_FILE`, `_CERT_KEY_FILE`, `_SERVER_NAME`, `_VERIFIED`, `_PEER_CN`, `_PEER_ALT_NAMES` | transport, pipe | honoured — the four auth modes, PEM material from files, and the three read-only answers about who the peer proved to be |
| `NNG_OPT_IPC_PEER_UID`, `_GID`, `_PID` | pipe | honoured — the kernel's answer at connection time, non-forgeable, read-only |
| `NNG_OPT_SOCKNAME` | socket | **refused** — absent: a mutable free-text name on a socket, whose only use in NNG is logging. What is missing: the name; `tracing` spans carry the endpoint and the pipe id instead |
| `NNG_OPT_RECVFD`, `NNG_OPT_SENDFD` | socket | **refused** — replaced by a weida-runtime construct: readiness here is a future, not a descriptor. What is missing: integration with an external `poll` loop, which a caller gets by awaiting instead |
| `NNG_OPT_PAIR1_POLY` | socket | **refused** — absent, with §2's four reasons; deprecated by NNG itself |
| `NNG_OPT_TCP_KEEPALIVE` | transport | **refused** — absent: TCP keepalive is a liveness probe SP has no use for, because SP has no heartbeat and a dead peer is discovered by the write that fails. What is missing: the socket-level `SO_KEEPALIVE` toggle |
| `NNG_OPT_TLS_CONFIG` | transport | **refused** — absent: NNG's option hands over an opaque `nng_tls_config` pointer, which is a C lifetime discipline and not a value. What is missing: nothing a caller can express — `TlsConfig` is the typed form and is set directly |
| `NNG_OPT_IPC_PEER_ZONEID` | pipe | **refused** — absent: illumos/Solaris zones, which this build does not target. What is missing: the zone id; uid, gid and pid are present |
| `NNG_OPT_IPC_PERMISSIONS` | transport | **refused** — replaced by a weida-runtime construct: the listener's mode is set by `weida-runtime`'s `BoundUnixSocket`, which creates the node with its permissions rather than chmod-ing it afterwards |
| `NNG_OPT_WS_REQUEST_HEADERS`, `NNG_OPT_WS_RESPONSE_HEADERS`, `NNG_OPT_WS_PROTOCOL`, `NNG_OPT_WSS_REQUEST_HEADERS` | transport | **refused** — for a transport this library does not implement (§3 here) |
| `NNG_OPT_ZT_HOME`, `_NWID`, `_PING_TIME`, `_PING_TRIES`, `_MTU` | transport | **refused** — for a transport NNG itself calls experimental (§3 here) |

## 6. Observability: pipes, events and what is counted

| Object | Verdict |
| --- | --- |
| `nng_dialer` | present — a named object created by `dial`, with its own `NNG_OPT_RECVMAXSZ` and reconnect times; it exists whether or not the first connection succeeds and retries with the backoff |
| `nng_listener` | present — a named object created by `listen`, accepting as long as it lives; closing it closes what it created |
| `nng_dial` blocking until the handshake | present, and improved: `dial` returns once the peer's protocol header has arrived, as NNG's synchronous form does, so a dial that succeeded means a pipe exists. Unlike NNG's, that wait costs no thread and is bounded by `SocketOptions::handshake_timeout`; `dial_nonblocking` is `NNG_FLAG_NONBLOCK`, returning at once and retrying in the background |
| `nng_pipe` | present — `PipeId`, one per connection, with the two queues it owns and the endpoint it came from |
| `NNG_PIPE_EV_ADD_PRE` | present — and it is where a connection can still be **rejected**: the callback returns `Admission`, which is how PAIR refuses a second peer |
| `NNG_PIPE_EV_ADD_POST` | present — fired after the pipe is admitted and visible to the socket |
| `NNG_PIPE_EV_REM_POST` | present — fired when the pipe is retired, once per admitted pipe |
| `nng_pipe_notify` per event | **absent with a reason**: NNG registers one callback per event number; here one callback sees every event and switches on it, because three registrations for three variants of one enum is an API that can be half-configured. What is missing: nothing observable — every event is delivered |
| `nng_stats` | **absent with a reason**: NNG's statistics tree is a snapshot API over counters this library does not keep. What is missing: the counters and the tree walk. `Broadcast::dropped` and `Discarded` report the two numbers a caller actually acts on — what a publication cost and what a close threw away |

## 7. Devices and helpers: raw mode and `nng_device`

Raw sockets are the second half of every protocol, not a twelfth protocol: `RawSocket`
(`raw.rs`) takes any of the eleven protocol numbers and bypasses the pattern's state machine
and header handling, leaving both to the application (`nng(7)`'s raw mode, sheet §4).

| Capability | Verdict |
| --- | --- |
| Raw send and receive with the protocol header preserved verbatim | present — `RawSocket::send`/`recv`; no REQ resend timer, no REQ/REP alternation, no SUB filtering, no PAIR v1 hop check on send |
| The pairing rule still applied in raw mode | present — a raw REQ socket still refuses a REQ peer: raw is about the state machine, not about who may connect |
| Contexts on a raw socket | **absent with a reason**, and refused in the type rather than at runtime: `nng_ctx_open` on a raw socket is `NNG_ENOTSUP` in NNG too, because a context *is* the state a raw socket does not hold. `RawSocket` has a `context()` that returns `Infallible`, so the refusal is in the signature |
| `nng_device` between two raw sockets | present — `device()` (`raw.rs`), forwarding in both directions between two raw sockets of a legal pairing, with the tag stack passed through untouched and the PAIR v1 hop count bounded by `NNG_OPT_MAXTTL` — a message arriving at hop 1 leaves as hop 2, and one at the ceiling is dropped with the pipe kept |
| `nng_device` with one socket (a loopback reflector) | **absent with a reason**: NNG allows a single-socket device that reflects messages back, which is only meaningful for PAIR and BUS raw sockets, and no caller has asked. What is missing: the one-argument form; the two-argument form covers the broker shapes |

Raw sockets and the device are the reason the tag stack is a public type
(`weida_sp::backtrace`): a device that could not read a stack could not bound its hop count.

## 8. Interop evidence

`tests/interop_nng.rs`, eleven tests, against the versions §1 names. The `nng` crate is an
optional dependency behind the `nng-interop` feature and the tests are `#[ignore]`d on top of
that, because the C library is a build condition rather than a runtime one
([LOOP.md](../LOOP.md) §2); the command is
`cargo test -p weida-nng --features nng-interop --test interop_nng -- --ignored`, and all
eleven pass.

Every pairing runs in **both roles** — this library bound with NNG dialling, and this library
dialling an NNG listener: REQ/REP, PUSH/PULL, PUB/SUB, PAIR v0, PAIR v1, SURVEYOR/RESPONDENT
and BUS. Two tests are about the wire rather than a pattern: `the_role_nibble_is_nngs_registry`
compares all eleven protocol ids and both role nibbles against what `weida_sp::ProtocolHeader`
encodes, and `the_tag_stack_survives_a_round_trip_through_nng` sends a stack through a real
NNG REP socket and back. One is about the device: `the_device_forwards_between_two_nng_sockets`
puts this library's `device()` between two NNG sockets.

**What interop settled that reading had not:**

1. **PAIR v1's initial hop count.** The RFC says the counter is "initialized to one and
   incremented at each node"; a reading of NNG's source suggested zero. A running NNG
   1.4.0-rc.0 originates **one** — the RFC's value, so `weida_sp::pair::INITIAL_HOPS` is `1`,
   and both readings are accepted on receipt, because the difference is a count and not a
   format.
2. **Every other pairing agrees on both sides**, in both roles, including
   the tag stack's byte order and the terminal-bit rule.

**Two deliberate divergences, visible on the wire and kept:**

1. **`NNG_OPT_RECVMAXSZ` defaults to 1 MiB here; NNG's default is unlimited.** A 64-bit
   declared length with no cap is the whole allocation hazard
   ([INVARIANTS.md](../INVARIANTS.md)), so the default is finite and the option sets it back
   to `0` for callers who want NNG's.
2. **An oversize declaration closes the pipe.** SP gives a reader no way to skip a body it
   declined — the size field is all there is — so the connection ends where NNG's would also
   end, but this side ends it on the *declaration* rather than after reading the octets.

## 9. Deliberate deviations and bounds NNG does not have

**Two deliberate default changes** (0013 §4.4 item 5), both settable back and both rows in §5
or §8: `NNG_OPT_RECVMAXSZ` = 1 MiB against NNG's unlimited, and `ContextConfig::close_budget`
= 1 s, a finite close where NNG's `nng_close` waits for its own internal drain.

**Bounds that are not NNG options at all.** Every number below exists because no remote input
may cause unbounded allocation ([INVARIANTS.md](../INVARIANTS.md)), and each is a named
constant rather than a magic value:

| Bound | Default | What it bounds, and why NNG has no option for it |
| --- | --- | --- |
| `SocketOptions::max_pipes` | 1024 | how many pipes one socket admits. NNG bounds neither pipes per socket nor sockets per process, so a stranger would otherwise decide the multiplier on every per-pipe queue |
| `SocketOptions::max_addresses` | 8 | how many addresses one name may resolve to before a dial gives up trying them |
| `ContextConfig::max_sockets` | 1023 | sockets one context may hold at once. NNG has no ceiling; the number is libzmq's, for the one reason that a process that runs out of sockets should say so rather than fail at an accept |
| `MAX_QUEUE_DEPTH` | 8192 | the largest `NNG_OPT_SENDBUF`/`RECVBUF` a caller may ask for |
| `NNG_MAXADDRLEN` | 128 | one URL, which NNG does publish, applied here before any parsing |
| `MAX_INPROC_NAME_BYTES` | 119 | one `inproc` name, derived from `NNG_MAXADDRLEN` because NNG publishes no name limit |
| `MAX_LEGACY_IPC_PATH_BYTES` | 121 | the legacy `ipc` path form, beside the platform's own `sun_path` budget; both are checked and the error says which failed |
| `DEFAULT_BROADCAST_SEND_DEPTH` | 128 | the per-pipe outgoing depth for the protocols that drop rather than wait — PUB, BUS and SURVEYOR. NNG documents no depth for them; a zero depth would make a broadcast discard nearly everything, so the number is named rather than hidden |

**One thing this library refuses to offer**, and it is a refusal rather than a gap: a sender
identity (§4 here). Adding one would be a private extension no NNG peer could read.

## 10. The definition of done

0013 §4.7's six clauses, read for SP, each with its verdict.

| Clause | Verdict |
| --- | --- |
| 1. Every protocol of `nng(7)`'s table implemented or absent with a reason, with the manual's rows asserted rather than described | **met** — §2: all eleven present, with their peer, direction, alternation and full-queue rows asserted in the module tests and in `protocol.rs`'s table test; `NNG_OPT_PAIR1_POLY` is the one behaviour refused, with what is missing named |
| 2. `tcp`, `ipc` and `inproc`, with the rest named absent | **met** — §3, and `tls+tcp` beyond the clause |
| 3. The security surface, honestly stated | **met** — §4: SP has no message-level authentication, authorization or sender identity, and the two transport-level answers say plainly that they authenticate a transport peer only |
| 4. Interop in both roles against the reference implementation, every pairing it supports | **met** — §8: eleven tests against NNG 1.4.0-rc.0 through the `nng` crate, both roles, one measured correction and two kept divergences |
| 5. The patterns exercised as the manual describes them, asserting the guarantee each claims | **met** — the seven suites `reqrep`, `pipeline`, `pubsub`, `pair`, `survey`, `bus`, `raw_device`, plus `local_transports`, `tls`, `options` and the compile-fail `directions` harness: 77 unit tests and 54 integration tests run by default, and the eleven interop tests behind the feature; each asserts the sheet's sentence rather than merely running |
| 6. A feature-parity table, row by row, where no row says "partial" without saying what is missing | **met** — this document |

What a caller who compares this library with NNG 1.10 does **not** get, in one list: PAIR v1
polyamorous mode; contexts on raw sockets and the one-socket device; the `ws`, `wss`, `zt`,
`udp`, `socket` and `abstract` transports and the address-family-pinned URL forms;
`nng_stats`; per-event pipe callbacks; `NNG_OPT_SOCKNAME`, `NNG_OPT_RECVFD`/`SENDFD`,
`NNG_OPT_TCP_KEEPALIVE`, `NNG_OPT_TLS_CONFIG`, `NNG_OPT_IPC_PEER_ZONEID` and
`NNG_OPT_IPC_PERMISSIONS`. Nothing else.

## 11. Sources

- [`../research/nanomsg-nng.md`](../research/nanomsg-nng.md) — §1 (handshake and dial), §2
  (sockets, contexts, pipes, dialers, listeners), §3 (the wire layouts and the tag stack),
  §4 (the eleven protocols and their rows), §5 (buffers and `NNG_OPT_RECVMAXSZ`), §10 (TLS),
  §11 (limits and the open questions), §13 (the measured `nng`-crate facts).
- [0013](../decisions/0013-competitor-libraries.md) §4.1 (crate layout), §4.3 (`weida-sp`
  stays sans-I/O), §4.4 (the shape of a library, the option rule and the default changes),
  §4.7 (the six clauses).
- [INVARIANTS.md](../INVARIANTS.md) — the allocation invariant §9's bounds answer.
- Code the verdicts were read from, all under `crates/nng/weida-nng/src/`: `optiontable.rs`
  (§5, generated from `OPTIONS` and walked by a test), `endpoint.rs` (§3), `protocol.rs` and
  `pipe.rs` (§2's full-queue rows), `reqrep.rs`, `replier.rs`, `survey.rs`, `pipeline.rs`,
  `pubsub.rs`, `pair.rs`, `bus.rs`, `raw.rs` (§2, §7), `engine.rs` (§6), `tls.rs` and `ipc.rs`
  (§4), plus `tests/interop_nng.rs` (§8).
