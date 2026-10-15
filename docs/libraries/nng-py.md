# NNG for Python (pynng 0.9.0 over NNG 1.11.0) — feature parity

The parity document of `weida-nng-py`, the Python binding of the `weida-nng` library. Its
reference implementation is **pynng**, because that is what a Python program that speaks the
Scalability Protocols uses today; the protocol-level parity against NNG itself is
[`nng.md`](nng.md) and is not repeated here — this document cites its sections instead. One
question per row: *what does a Python caller who knows pynng get, and what does that caller
not get.*

## 1. What a row means

Three verdicts and no others:

- **present** — implemented, with the module or the test that proves it;
- **refused** — the call exists and fails at configuration time with a reason the code gives;
- **absent** — not implemented, with what is missing named.

"Partial" is not a verdict ([0013](../decisions/0013-competitor-libraries.md) §4.7 clause 6).
Where this binding's shape differs from pynng's the row says so and §9 states why.

**Two C releases, and every row says which one it was measured against.** This library is
measured against two different builds of the NNG C implementation, which is worth more than
either alone:

| Suite | C release | How it is reached |
| --- | --- | --- |
| `crates/nng/weida-nng/tests/interop_nng.rs` (Rust) | **NNG 1.4.0-rc.0** | vendored by `nng-sys` under the `nng` crate 1.0.1 ([`nng.md`](nng.md) §1) |
| `crates/nng/weida-nng-py/tests/test_interop.py` (this binding) | **NNG 1.11.0** | bundled and built by **pynng 0.9.0**, which is the peer in §8 |

The 1.11.0 figure is not assumed: `pynng.ffi.string(pynng.lib.nng_version())` answers
`1.11.0` in the repository virtualenv, and pynng reports `0.9.0` (§11 names the
command). **The pynng version is not pinned anywhere**: `develop.sh` (a wrapper over
`crates/py/develop.sh`) installs `pynng` unpinned into the repository virtualenv,
`pyproject.toml` declares no dependency on it, and
`tests/test_interop.py::test_the_c_library_is_the_one_this_test_claims` asserts only that the
C library is an NNG 1.x. A later `develop.sh` run may therefore measure a different 1.x, and
the test will still pass — which is the honest bound on every claim in §8.

Everything else was measured against: CPython 3.13 with the `abi3` wheel built from CPython
3.9, PyO3 0.29 (`abi3-py39`, workspace `Cargo.toml`), Linux x86-64. Rows about the Rust side
cite `crates/nng/weida-nng/src/`; rows about SP itself cite
[`nng.md`](nng.md) and, through it, [`../research/nanomsg-nng.md`](../research/nanomsg-nng.md).

## 2. Socket and protocol types

Eleven protocol classes in pynng, eleven here — `src/sockets.rs`, one `python_socket!`
invocation each, and eleven more in `src/sync.rs` for the synchronous surface (§9.5).

| pynng | This binding | Verdict |
| --- | --- | --- |
| `pynng.Req0` | `ReqSocket` | present — `sockets.rs:356`, `tests/test_sockets.py::test_a_request_reaches_a_replier_and_the_reply_comes_back` |
| `pynng.Rep0` | `RepSocket` | present, and an answer nobody asked for is `ESTATE` — `test_a_reply_nobody_asked_for_is_refused_by_name` |
| `pynng.Push0` | `PushSocket` | present, `send`/`send_nowait`; a send with no eligible peer waits and then reports `ETIMEDOUT` — `test_work_reaches_a_puller_and_a_push_without_one_times_out` |
| `pynng.Pull0` | `PullSocket` | present, `recv`/`recv_nowait` |
| `pynng.Pub0` | `PubSocket` | present, and `send` answers a `Broadcast(queued, dropped)` — the count of lost copies, which pynng has nowhere (§6) |
| `pynng.Sub0` | `SubSocket` | present, with `subscribe`, `unsubscribe`, the `subscriptions` list and the `discarded` counter — `test_a_subscriber_is_sent_only_what_it_asked_for` |
| `pynng.Pair0` | `Pair0Socket` | present, one peer at a time |
| `pynng.Pair1` | `Pair1Socket` | present, with the hop count — `test_a_pair_talks_both_ways` |
| `pynng.Surveyor0` | `SurveyorSocket` | present, deadline from `survey_time` — `test_a_survey_reaches_every_respondent_and_the_deadline_ends_it` |
| `pynng.Respondent0` | `RespondentSocket` | present |
| `pynng.Bus0` | `BusSocket` | present, one hop — `test_a_bus_message_reaches_every_directly_connected_peer` |
| `pynng.Socket(opener=...)`, the instantiable base class | — | absent, deliberately: there is no base socket class to construct. A PUB has no `recv` here and a PULL has no `send`, so `nng(7)`'s own direction table is a `AttributeError` at the call instead of `NNG_ENOTSUP` on the wire |
| `Socket(raw=True)` | — | **absent**, and §7 carries the row: the library has `RawSocket` in Rust ([`nng.md`](nng.md) §7); no Python class opens one. pynng 0.9.0 does not have it either — `pynng/nng.py:217` says "pynng does not support `raw` mode sockets" |
| `sock.new_context()` → `pynng.Context` | `socket.context()` on REQ, REP, SURVEYOR, RESPONDENT | present as **three** classes, `ReqContext`, `ReplyContext`, `SurveyContext` (`contexts.rs:120-149`) — REP and RESPONDENT share one because `RepCtx` is one library type for both. Two contexts have two transactions outstanding at once: `tests/test_contexts.py::test_two_contexts_have_two_requests_outstanding_at_once` |
| `ctx.close()` | — | absent as a method: a context is closed by dropping it, which is `nng_ctx_close` (`contexts.rs:12-13`) |
| `Pair1(polyamorous=True)` | — | refused: `NNG_OPT_PAIR1_POLY` is refused by the library itself, deprecated by NNG's own manual ([`nng.md`](nng.md) §5), and the binding offers no keyword for it |
| `sock.send_msg`, `recv_msg`, `asend_msg`, `arecv_msg`, `pynng.Message` | — | absent: a message is `bytes` in and `bytes` out (`values.rs`, `Body`). What `Message` adds in pynng is the owning pipe, and pipes have no Python object here (§6) |
| `sock.send(data)` / `recv()` blocking, `asend`/`arecv` on the same object | `await socket.send(...)` / `await socket.recv()`, and `weida_nng.sync.<Class>.send`/`recv` | present, as **two surfaces rather than two method names**: asyncio in `weida_nng`, blocking in `weida_nng.sync`, over one set of sockets (§9.5) |
| `sock.send(data, block=False)` | `send_nowait` / `recv_nowait` | present where the protocol permits it — `NNG_FLAG_NONBLOCK`, `EAGAIN` for an empty receive |
| `str` passed as a payload | — | refused: `payload_of` accepts `bytes` and `bytearray` and answers a `TypeError` naming what it takes (`crates/py/weida-py-core/src/bytes.rs:84-94`); pynng refuses the same mistake with a `ValueError` (`pynng/nng.py:54-57`). Neither guesses an encoding |
| — | `weida_nng.Context`, with `Context()`, `Context.current()`, `Context.sharing(other)` | present, and pynng has **no counterpart**: NNG's reactor, `inproc` namespace and resource ceiling are process-global. All three are explicit here (`context.rs:9-13`) |
| `sock.protocol`, `sock.protocol_name` | `socket.protocol` | present as one string, and it **differs from NNG's spelling for PAIR v0**: NNG's `protocol_name` answers `pair` (measured through pynng, §11), this binding answers `"pair0"` (`sockets.rs:423`). The other ten agree — `req`, `rep`, `push`, `pull`, `pub`, `sub`, `pair1`, `surveyor`, `respondent`, `bus` |
| `sock.peer`, `sock.peer_name` | — | absent: the pairing rule is enforced by the library at handshake ([`nng.md`](nng.md) §2), and the peer's protocol number is not read back into Python |

## 3. Transports

The binding exposes what the library parses; a URL is bounded by `NNG_MAXADDRLEN` = 128 before
anything else happens (`tests/test_options.py::test_a_url_past_nng_maxaddrlen_is_refused_before_it_is_parsed`).
The pynng column is measured against its bundled NNG 1.11.0 by attempting a
listen on each scheme (§11).

| pynng / NNG 1.11.0 | This binding | Verdict |
| --- | --- | --- |
| `tcp://` | `tcp://` | present — `test_a_request_reaches_a_replier_and_the_reply_comes_back[tcp]`, and every interop test of §8 |
| `ipc://` | `ipc://` | present on Unix — `[ipc]` of the same parametrized test |
| `inproc://` | `inproc://` | present, and **context-scoped** rather than process-global, which is the `Context` object of §2 — `[inproc]` |
| `tls+tcp://`, configured by `pynng.TLSConfig(mode, ...)` and `sock.tls_config` | — | **refused**, and this is the first of the binding's two named absences. The library *has* the transport — TLS 1.2 and 1.3 through `tokio-rustls`, the four auth modes, CA and certificate/key material, the peer's verified state ([`nng.md`](nng.md) §3, §5) — and takes it as `SocketOptions::tls: Option<TlsConfig>` (`crates/nng/weida-nng/src/options.rs`). **This surface has no certificate API**: `weida_nng.SocketOptions` has nineteen keywords and none of them is `tls` (`src/options.rs:49-70`). A `tls+tcp://` URL therefore reaches `engine.rs` and fails with `EINVAL` — "a tls+tcp endpoint needs `SocketOptions::tls`; a TLS transport with no configuration is a TCP transport with a longer name". Asserted by `tests/test_options.py::test_a_tls_endpoint_is_refused_because_this_binding_configures_none`. What is missing: a Python spelling of "these roots, this certificate and key, this server name, this auth mode" |
| `ws://`, `wss://` — both accepted a listen in pynng's bundled build | — | refused with a reason by the library: the WebSocket mapping needs an HTTP server ([`nng.md`](nng.md) §3). From Python the refusal is `ENOTSUP` naming the transport — `test_an_unknown_transport_is_refused_with_what_this_library_speaks` |
| `abstract://` — accepted a listen in pynng's bundled build | — | refused with a reason: the Linux abstract `AF_UNIX` namespace has no filesystem permissions, so the one authorization `ipc://` offers is gone ([`nng.md`](nng.md) §3) |
| `udp://` — **`NotSupported` in pynng's bundled build too**, measured | — | refused with a reason: SP's UDP mapping has no pipe and every pattern here is written on one ([`nng.md`](nng.md) §3) |
| `zt://`, `socket://`, `tcp4://`, `tcp6://`, `tls+tcp4://`, `tls+tcp6://` | — | refused with a reason each, from the library's `ABSENT_TRANSPORTS` table ([`nng.md`](nng.md) §3). A scheme the table does not know is refused too, naming what this library speaks |

## 4. Mechanisms and authentication

SP has **no** authentication of its own, and that is a property of the protocol rather than a
gap in either implementation: no mechanism negotiation, no credential frame, no signature
([`nng.md`](nng.md) §4). What remains is what the transport can prove.

| pynng | This binding | Verdict |
| --- | --- | --- |
| Message-level authentication | — | **absent in SP itself**; no option in pynng turns one on either ([`nng.md`](nng.md) §4) |
| Sender identity on a message | — | **absent in SP itself**: the tag stack is a routing breadcrumb, not an identity |
| TLS, the only mechanism SP has: `pynng.TLSConfig`, `NNG_OPT_TLS_*` | — | **absent from this binding**, exactly as §3's row says, and it is the same absence counted once: the library authenticates the transport peer, the Python surface cannot configure it |
| IPC peer credentials (uid, gid, pid) | — | absent from this binding: the library reads the kernel's answer at connection time ([`nng.md`](nng.md) §5, `NNG_OPT_IPC_PEER_UID`/`_GID`/`_PID`), and Python has no pipe object to read it from (§6). What is missing: a pipe surface carrying the credentials |
| A ZAP-like authorization dialog | — | **absent in SP itself**: inventing one would be a private protocol no NNG peer could speak |

## 5. Options

Two surfaces, answering two questions, and both are the library's own table rather than a
retyped copy.

- **`weida_nng.OPTIONS` and `weida_nng.option(name)`** are all **50 rows** of `nng_options(5)`
  as `weida-nng`'s `optiontable.rs` answers them — **34 honoured, 16 refused** (§11 gives the
  commands). Each row is an `NngOption` with `name`, `scope`, `honoured`, `note` and
  `refusal`; a refused row always carries the reason and an honoured one never does, which a
  test walks over the whole table (`tests/test_options.py::test_the_option_table_answers_for_every_name_it_carries`).
  A name NNG does not have answers `None`.
- **`weida_nng.SocketOptions(**kwargs)`** is what a socket is opened with: **nineteen keyword
  arguments** (`src/options.rs:49-70`), frozen, validated at construction.

The refusal vocabulary a Python caller sees is the library's errno classes, not a string:

| What is wrong | What is raised | Evidence |
| --- | --- | --- |
| A value no configuration can hold — `max_ttl` 0 or 256, a depth past `MAX_QUEUE_DEPTH`, `max_pipes` 0, `max_pipes_per_peer` 0, `max_tls_handshakes` 0, `handshake_timeout`, `message_timeout` or `idle_timeout` 0, `max_addresses` 0, a negative duration | `EINVAL` at the `SocketOptions(...)` call | `test_an_impossible_configuration_is_refused_at_configuration_time` (11 parametrized rows), `test_a_negative_duration_is_not_a_duration` |
| An option this protocol does not have — a `send_depth` on REQ | `ENOTSUP` **naming `NNG_OPT_SENDBUF`**, at the socket constructor | `test_a_buffer_a_protocol_does_not_have_is_refused_when_the_socket_opens` |
| A subscription on a socket with no subscriptions | `ENOTSUP` **naming `NNG_OPT_SUB_SUBSCRIBE`**, at the socket constructor (`subscriptions.rs`) | `test_a_subscription_on_a_socket_without_subscriptions_is_refused_by_name` |
| A transport the library does not implement | `ENOTSUP` naming what it does speak | `test_an_unknown_transport_is_refused_with_what_this_library_speaks` |
| A `tls+tcp://` endpoint | `EINVAL` naming the missing configuration | §3 |

Twelve of the nineteen keywords are NNG options under NNG's own semantics — `recv_max_size`
(`NNG_OPT_RECVMAXSZ`), `send_depth` (`SENDBUF`), `recv_depth` (`RECVBUF`), `send_timeout`
(`SENDTIMEO`), `recv_timeout` (`RECVTIMEO`), `reconnect_min`/`reconnect_max`
(`RECONNMINT`/`RECONNMAXT`), `max_ttl` (`MAXTTL`), `resend_time` (`REQ_RESENDTIME`),
`survey_time` (`SURVEYOR_SURVEYTIME`), `sub_prefer_new` (`SUB_PREFNEW`), `subscribe`
(`SUB_SUBSCRIBE`). The other seven — `max_pipes`, `max_pipes_per_peer`, `max_tls_handshakes`,
`max_addresses`, `handshake_timeout`, `message_timeout`, `idle_timeout` — are bounds NNG has no
option for (§9.8); the three timeouts follow the same rule as `send_timeout`: seconds as floats,
and `None` for no limit.

Differences from pynng's option surface:

| | pynng | This binding |
| --- | --- | --- |
| Where | descriptor properties on a live socket: `sock.recv_timeout = 500`, or keywords on the constructor | one frozen `SocketOptions` object passed to the constructor. An option that cannot be delivered fails before the socket exists |
| Durations | **milliseconds**, as integers | **seconds**, as floats, with `None` for `NNG_DURATION_INFINITE`. NNG's default is to wait forever, which is why every test helper sets both timeouts |
| Reading back | every option is a readable property | thirteen of the nineteen have getters (`recv_max_size`, `send_depth`, `recv_depth`, `send_timeout`, `recv_timeout`, `max_ttl`, `survey_time`, `resend_time`, `sub_prefer_new`, `max_pipes`, `max_pipes_per_peer`, `message_timeout`, `idle_timeout`, `src/options.rs`). `reconnect_min`, `reconnect_max`, `handshake_timeout`, `max_addresses`, `max_tls_handshakes` and `subscribe` have none: absent, and what is missing is a getter per field |
| Per-endpoint options | `dialer`/`listener` objects carry their own | absent: `dial` and `listen` take a URL and answer a URL. The library allows `NNG_OPT_RECVMAXSZ` per endpoint ([`nng.md`](nng.md) §5); Python cannot reach it |
| `sock.name` (`NNG_OPT_SOCKNAME`) | present | refused by the library — absent, with `tracing` spans carrying the endpoint and pipe id instead ([`nng.md`](nng.md) §5) |
| `sock.recv_fd`, `sock.send_fd` | present, for `select`/`poll` | refused by the library — replaced by a weida-runtime construct: readiness here is a future, and a caller awaits instead ([`nng.md`](nng.md) §5) |
| `sock.tcp_nodelay` | settable | honoured as **always on**: SP's patterns are round-trip shaped ([`nng.md`](nng.md) §5) |
| `sock.tcp_keepalive` | settable | honoured as **always on**: SP has no heartbeat, so the kernel's probes are how a dead peer is found ([`nng.md`](nng.md) §5) |
| `sock.tls_config` | present | absent (§3, §4) |
| Module constants to compare a configuration against | — | present, and pynng has none: `DEFAULT_MAX_SOCKETS`, `DEFAULT_MAX_PIPES`, `DEFAULT_RECV_MAX_SIZE`, `MAX_QUEUE_DEPTH`, `NNG_MAX_TTL`, `SPEC_MAX_TTL`, `NNG_MAXADDRLEN`, `DEFAULT_CLOSE_BUDGET`, `DEFAULT_SURVEY_TIME`, `DEFAULT_RESEND_TIME`, all taken from the library (`lib.rs:125-143`) and asserted by `test_the_defaults_are_the_librarys_own_numbers` |

## 6. Observability

| pynng | This binding | Verdict |
| --- | --- | --- |
| `sock.add_pre_pipe_connect_cb`, `add_post_pipe_connect_cb`, `add_post_pipe_remove_cb` and their `remove_*` counterparts | — | **absent**: the library delivers all three `NNG_PIPE_EV_*` events to one callback and lets `AddPre` reject a connection ([`nng.md`](nng.md) §6), and no `#[pyclass]` or method here exposes it — `grep -n 'Admission\|pipe_notify\|notify' crates/nng/weida-nng-py/src` finds nothing. What is missing: a Python callback or event stream, and with it the ability to refuse a pipe |
| `pynng.Pipe`, `sock.pipes`, `msg.pipe`, `pipe.local_address`, `pipe.remote_address`, `pipe.close()` | `socket.pipe_count` | present as a **number, not a collection**: how many pipes this socket can talk to right now (`sockets.rs:332-336`). What is missing: the per-pipe objects, their addresses and the ability to close one |
| `pynng.Dialer`, `pynng.Listener`, `sock.dialers`, `sock.listeners`, `dialer.close()` | `await socket.dial(url)` / `listen(url)` returning the URL actually used | present as the one fact a caller needs — a wildcard listen reports its port (`test_a_wildcard_listen_reports_the_port_it_got`), which is `NNG_OPT_URL`/`NNG_OPT_TCP_BOUND_PORT`. What is missing: the endpoint objects and closing one without closing the socket |
| `sock.dial(addr, block=False)` | `socket.dial_nowait(url)` | present, `NNG_FLAG_NONBLOCK`, retrying in the background with the reconnect backoff |
| `nng_stats` | — | absent in **both**: NNG's statistics tree is a snapshot API over counters this library does not keep ([`nng.md`](nng.md) §6), and pynng exports nothing for it either |
| — | `SubSocket.discarded` | present, and pynng has no counterpart: publications this subscriber threw away, filtered out or dropped for a full queue. Asserted across implementations — `tests/test_interop.py` checks `subscriber.discarded == 1` after the C library published one non-matching topic |
| — | `Broadcast(queued, dropped)` from `PubSocket.send`, `BusSocket.send` | present, and pynng has no counterpart: PUB, BUS and SURVEYOR are best effort and NNG's own send reports success either way. This is the only place the second number exists (`values.rs:25-37`) |
| — | `Context.socket_count`, `max_sockets`, `worker_threads`, `close_budget`, `closed`, and `await ctx.shutdown()` returning the sockets still open when the budget ran out | present, no counterpart in pynng, which has no context object at all (`context.rs:155-197`) |
| — | `ReqContext.id` / `ReplyContext.id` / `SurveyContext.id` | present — `nng_ctx_id()` as a property (`contexts.rs:102-106`) |

## 7. Devices and helpers

| NNG / pynng | This binding | Verdict |
| --- | --- | --- |
| **Raw sockets** (`NNG_OPT_RAW`) | — | **absent**, the first half of the binding's second named absence. The library has `RawSocket` in Rust over all eleven protocol numbers ([`nng.md`](nng.md) §7); no Python class opens one, and `src/sockets.rs:22-28` says so in the module documentation. What is missing: a Python surface for a message whose protocol header is the application's — which is a byte-slicing API over the header these eleven classes deliberately keep. **pynng 0.9.0 does not offer it either**: `pynng/nng.py:217` documents `raw` as unsupported, and its `Socket.raw` property is read-only |
| **`nng_device()`** between two raw sockets | — | **absent**, the second half. The library has `device()` in `raw.rs`, forwarding in both directions with the tag stack passed through and the PAIR v1 hop count bounded by `NNG_OPT_MAXTTL` ([`nng.md`](nng.md) §7). What is missing: the forwarder, because it needs the raw sockets above. **pynng 0.9.0 has no device either** — `pynng/nng.py:270` reads "We're not supporting `nng_device` yet", and `pynng/__init__.py` exports no such name. So a Python program that wants an SP broker has it from neither implementation today |
| the single-socket loopback form of `nng_device` | — | absent in the library too, with its reason ([`nng.md`](nng.md) §7) |
| `pynng.Message`, `msg.bytes`, `msg.pipe` | — | absent: a payload is `bytes` (§2), and there is no pipe to attach it to (§6) |
| a `send_string`/`send_json` convenience | — | absent, as in pynng: neither library guesses an encoding or a serialization for somebody else's wire format |
| `pynng`'s Trio backend (`async_backend=`) | — | absent: this binding drives asyncio futures from the context's reactor (`weida-py-core`'s `Bridge`). What is missing: any second event-loop flavour |

A note on the manifest rather than on the API: `weida-sp` is a declared dependency of
`crates/nng/weida-nng-py`, commented as the endpoint-type enum that "travels no further than
the raw socket's constructor" (its `Cargo.toml`), and
`grep -rn 'weida_sp' crates/nng/weida-nng-py/src` finds no use of it — consistent with the two
absences above.

## 8. Interop evidence

`crates/nng/weida-nng-py/tests/test_interop.py` — **six tests**, against **pynng** over the NNG
C library it bundles, which is **1.11.0** in the repository virtualenv (§1). Where pynng is absent every
test skips with its install command in the skip message. Every pynng call is blocking and runs
in a worker thread through `asyncio.to_thread`, because a blocking C call on the event-loop
thread would deadlock against the coroutine it waits for.

| Test | Pairing | Which side is this binding | Transport |
| --- | --- | --- | --- |
| `test_the_c_library_is_the_one_this_test_claims` | — | — | asserts `nng_version()` is 1.x, so the peer is named rather than assumed |
| `test_a_request_from_the_c_library_is_answered_by_this_binding` | REQ/REP | REP, **bound** (the C library dials) | `tcp://127.0.0.1:0` |
| `test_a_request_from_this_binding_is_answered_by_the_c_library` | REQ/REP | REQ, **dialling** (the C library listens) | loopback `tcp` |
| `test_a_publication_from_the_c_library_is_filtered_by_this_subscriber` | PUB/SUB | SUB, dialling — and the prefix filter *and* the `discarded` count are asserted, not just arrival | loopback `tcp` |
| `test_work_pushed_by_this_binding_is_pulled_by_the_c_library` | PUSH/PULL | PUSH, dialling | loopback `tcp` |
| `test_a_survey_from_the_c_library_is_answered_by_this_respondent` | SURVEYOR/RESPONDENT | RESPONDENT, dialling | loopback `tcp` |

**REQ/REP is exercised in both roles and on both sides of the bind**; PUB/SUB, PUSH/PULL and
SURVEYOR/RESPONDENT are exercised in one role each. **Nothing disagreed** in the recorded
run: all six pass. The suite's 51 Python test cases are counted from the source rather than
re-run (§11).

What the interop does **not** cover, named rather than implied:

- **PAIR v0, PAIR v1 and BUS against the C library.** They are covered only between two
  sockets of this binding (`tests/test_sockets.py`). A hop-count or PAIR-v0 header
  disagreement with NNG would not be caught here.
- **`ipc://` and `inproc://` against the C library.** All six tests use loopback TCP.
  `inproc://` cannot cross implementations by construction; `ipc://` could and is not tested.
- **The synchronous surface against pynng.** `tests/test_sync.py::test_the_two_surfaces_talk_to_each_other`
  crosses `weida_nng.sync` with `weida_nng`, which proves one implementation underneath, not
  the C library's agreement.
- **Any cost measurement.** There is no `roundtrip_cost.py` for this binding, so unlike
  [`zmq-py.md`](zmq-py.md) §8 this document quotes no microseconds.

## 9. Deliberate deviations, and bounds NNG does not have

1. **Two coroutines may use one socket at once** — *unlike* this workspace's ZeroMQ binding
   ([`zmq-py.md`](zmq-py.md) §9.1), and not a limitation here. A `weida-nng` socket's `send`
   and `recv` take `&self` and the socket is `Sync`, so a receive parked on an empty socket
   does not hold it against a send: `tests/test_sockets.py::test_two_coroutines_may_use_one_socket_at_once`.
   Where a protocol *does* need one transaction at a time it says so per **context**, with
   `ESTATE`, which is SP's own answer — `tests/test_contexts.py::test_a_context_that_answers_out_of_turn_is_refused_and_its_neighbour_is_not`.
2. **Every call that can wait is a coroutine**, including `dial`, `listen` and
   `Context.shutdown`. `dial_nowait`, `send_nowait` and `recv_nowait` are synchronous because
   they never wait. A cancelled task leaves the socket usable —
   `test_a_cancelled_receive_leaves_the_socket_usable`.
3. **Seconds, not milliseconds**, for every duration, with `None` where NNG writes
   `NNG_DURATION_INFINITE` (§5). pynng takes milliseconds as integers.
4. **A payload is `bytes`**, both ways, and a `str` is refused rather than encoded (§2).
5. **The synchronous surface is a facade over a facade and implements nothing.**
   `weida_nng.sync` is the same eleven protocols and the same three context classes over
   `weida-nng`'s own `blocking` module, which the binding enables as a Cargo feature
   (the `blocking` feature of `Cargo.toml`). No protocol behaviour exists twice, which is why the two surfaces
   cannot disagree and why a `sync` socket and an asyncio socket exchange a message in the
   suite (`test_the_two_surfaces_talk_to_each_other`). **The GIL is released** — `py.detach`
   around every blocking `send`, `recv`, `dial`, `listen` and `shutdown` (`sync.rs:146`, `190`,
   `299`, `311`, `115`) — so one thread parked in `recv` does not stop another
   (`test_a_blocked_thread_does_not_hold_the_gil`), and no asyncio loop exists in such a
   process (`test_no_event_loop_is_running_while_this_suite_runs`).
6. **`weida_nng.sync.Context` is the owned constructor only.** The asyncio `Context` has three
   (`Context()`, `current()`, `sharing(other)`) and six properties; the synchronous one has
   the default constructor, `max_sockets`, `socket_count` and `shutdown` (`sync.rs:67-126`).
   What is missing on the synchronous side: the ambient and shared reactors, and the
   `closed`/`worker_threads`/`close_budget` properties.
7. **Failures are classes, not numbers** — **23** exception classes under `weida_nng.NngError`,
   one per `weida_nng::Error` variant, each carrying `errno` and `cause` (`errors.rs:40-64`).
   The table cannot drift: the name list and the exhaustive `match` come from one macro, so a
   variant added to the library and not here is a compile error. pynng has **31** classes under
   `pynng.NNGException`; the nine it has that this does not are `ENOMEM`, `EBUSY`, `ENOSPC`,
   `EEXIST`, `ECRYPTO`, `ENOARG`, `EAMBIGUOUS`, `EBADTYPE` and `EINTERNAL` — conditions of the
   C implementation the Rust library has no variant for — and the one this has that pynng does
   not is `ESYSERR`.
8. **Bounds NNG does not have**, all inherited from the library, all settable, all readable
   from Python as module constants (§5): pipes per socket (`max_pipes`, 1024), addresses one
   name may resolve to (`max_addresses`, 8), sockets per context (`max_sockets`, 1023), queue
   depth (`MAX_QUEUE_DEPTH`, 8192), and the handshake timeout, which NNG has no option for at
   all. [`nng.md`](nng.md) §9 lists them with their reasons.
9. **Two defaults differ from NNG**, both inherited and both settable back:
   `NNG_OPT_RECVMAXSZ` is 1 MiB where NNG's is unlimited, and a context's close budget is
   finite (1 s, `DEFAULT_CLOSE_BUDGET`) where `nng_close` waits for its own drain
   ([`nng.md`](nng.md) §9).
10. **`shutdown` reports rather than raises.** `await ctx.shutdown()` answers how many sockets
    were still open when the budget ran out — a number to log or retry against, because a peer
    that will not go away is not that call's failure (`context.rs:185-197`).

## 10. The definition of done

The acceptance clauses of the asyncio surface, the synchronous surface and this document, each with its verdict.

| Clause | Verdict | Where |
| --- | --- | --- |
| One Python class per SP protocol with `dial`, `listen`, `close`, `await send`, `await recv` | yes, eleven asyncio classes and eleven synchronous ones | §2 |
| Contexts as their own Python objects, so concurrent requests are concurrent | yes, three classes for the four protocols that have one, two transactions outstanding at once in a test | §2, §6 |
| `NNG_E*` names as distinct exception classes | yes, 23 under one base, checked against the library's enum by the compiler | §9.7 |
| Options refused at configuration time exactly as the Rust table refuses them | yes, `EINVAL` for an impossible value and `ENOTSUP` naming the NNG option | §5 |
| An asyncio REQ/REP round trip and a SUB prefix match, from Python | yes, over all three transports for the round trip | §2, §3 |
| One exchange against the C library, proving the binding on the wire | yes — six tests, REQ/REP in both roles, against NNG 1.11.0 | §8 |
| A synchronous surface with no event loop in the process and no second implementation | yes, over the library's `blocking` facade, with the GIL released | §9.5 |
| Send and receive timeouts turning a stalled exchange into an exception | yes, both asserted within two seconds | §9.5 |
| The two absences stated as rows rather than as prose | yes — `tls+tcp` in §3 and §4, raw sockets with `nng_device` in §7 | §3, §7 |
| Which C release each claim was measured against | yes, and the pynng version is recorded as **unpinned** | §1 |
| The wheel needs no Rust toolchain | yes: `package.sh` (a wrapper over `crates/py/package.sh`) builds the `abi3` release wheel (`weida_nng-0.1.0a6-cp39-abi3-manylinux_2_34_x86_64.whl` here), installs it into a fresh virtualenv and runs `smoke.py` — one REQ/REP round trip on each surface, both ends this module, no `pynng` installed — with `PATH` scrubbed to `/usr/bin:/bin` | `package.sh`, `smoke.py` |
| No row says "partial" | yes: §3, §6 and §7 name what a caller does not get | §3, §6, §7 |

## 11. Sources, and the commands behind every count

- Code: `crates/nng/weida-nng-py/src/` — `lib.rs`, `sockets.rs`, `contexts.rs`, `context.rs`,
  `options.rs`, `subscriptions.rs`, `values.rs`, `errors.rs`, `sync.rs` — and
  `crates/py/weida-py-core/src/`. The library: `crates/nng/weida-nng/src/`
  (`optiontable.rs`, `options.rs`, `engine.rs`, `raw.rs`).
- Tests: `crates/nng/weida-nng-py/tests/` — `test_sockets.py`, `test_options.py`,
  `test_contexts.py`, `test_sync.py`, `test_interop.py`. Build and run: `./develop.sh` in
  `crates/nng/weida-nng-py`, a wrapper over `crates/py/develop.sh` (`uv` virtualenv,
  `maturin develop`, `pytest`), which also installs `pynng`.
- pynng's surface is read from the copy in the repository virtualenv:
  `.venv/lib/python3.13/site-packages/pynng/` — `nng.py`, `exceptions.py`, `tls.py`,
  `__init__.py`.

Every count in this document, with the command that produces it:

| Claim | Command | Result |
| --- | --- | --- |
| The peer's versions (§1, §8) | `.venv/bin/python -c "import pynng; print(pynng.__version__, pynng.ffi.string(pynng.lib.nng_version()).decode())"` | `0.9.0 1.11.0` |
| The suite's size (§8) | `grep -c 'def test_' crates/nng/weida-nng-py/tests/*.py` | `test_contexts.py:4`, `test_interop.py:6`, `test_options.py:12`, `test_sockets.py:12`, `test_sync.py:9` — **43 test functions** |
| Collected test cases | the two `@pytest.mark.parametrize` rows (`grep -n 'parametrize' crates/nng/weida-nng-py/tests/*.py`) expand one function to 7 and one to 3, so 41 + 7 + 3 = **51** | 51 |
| Eleven socket classes (§2) | `grep -c '^python_socket!($' crates/nng/weida-nng-py/src/sockets.rs` and `grep -c '^sync_socket!($' crates/nng/weida-nng-py/src/sync.rs` | 11 and 11 |
| Three context classes (§2) | `grep -c '^python_context!($' crates/nng/weida-nng-py/src/contexts.rs` and `grep -c '^sync_context!($' crates/nng/weida-nng-py/src/sync.rs` | 3 and 3 |
| 23 exception classes (§9.7) | `grep -cE '^    E[A-Z]*,$' crates/nng/weida-nng-py/src/errors.rs` | 23 |
| 31 pynng exception classes (§9.7) | `grep -c '^class [A-Za-z]*(NNGException):' .venv/lib/python3.13/site-packages/pynng/exceptions.py` | 31 |
| Nineteen option keywords (§5) | `grep -cE '^        [a-z_]*=None,$' crates/nng/weida-nng-py/src/options.rs` | 19 |
| 50 option rows, 34 honoured, 16 refused (§5) | `grep -c 'name: "NNG_OPT_' crates/nng/weida-nng/src/optiontable.rs`, then the same file with `'        disposition: Disposition::Honoured'` and `'        disposition: Disposition::Refused'` | 50, 34, 16 |
| `package.sh` and `smoke.py` present (§10) | `glob 'crates/**/package.sh; crates/**/smoke.py'` | every binding has both, each `package.sh` a wrapper over `crates/py/package.sh` |
| pynng's transports (§3) | a listen attempt per scheme through `.venv/bin/python` against `pynng.Pull0` | `ws`, `wss`, `tls+tcp`, `inproc`, `ipc`, `abstract` accepted; `udp` raised `NotSupported` |
| NNG's protocol names (§2) | `protocol_name` read from each of pynng's eleven classes | `pair` for `Pair0`, where this binding answers `pair0`; the other ten agree |

- Decisions: [0014](../decisions/0014-parallel-libraries.md) §2 (one shared PyO3 foundation,
  asyncio first, the synchronous surface second and over the same code, the bytes boundary),
  [0013](../decisions/0013-competitor-libraries.md) §4.4 (options honoured or refused at
  configuration time, no second implementation in the binding) and §4.7 clause 6 (no "partial").
- The library's own parity table: [`nng.md`](nng.md) — §2 protocols, §3 transports, §4
  mechanisms, §5 the 50 option rows, §6 pipes and events, §7 raw mode and `nng_device`, §9
  bounds. The protocol sheet it cites:
  [`../research/nanomsg-nng.md`](../research/nanomsg-nng.md).

**What this document does not check, stated rather than guessed:**

1. **The pynng version is unpinned.** `0.9.0` over NNG `1.11.0` is what the repository
   virtualenv holds; nothing in the repository fixes it, and the suite asserts only
   `1.x`. Any §8 claim is a claim about that build.
2. **The suite's size is counted, not run.** The 43 test functions and 51 cases are
   counted from the source; the last recorded green run reports 51.
3. **Whether `weida-sp` in the binding's manifest is reserved for a raw-socket surface or is a
   leftover** is not decided by the manifest's own comment, which names the raw socket's
   constructor; §7 records that nothing under `src/` refers to it.
4. **The `ws://`, `wss://` and `tls+tcp://` probes in §3 record that pynng's bundled build
   accepted a `listen` call**, not that a handshake completes on them. Only `udp://`'s refusal
   is a completed negative.
