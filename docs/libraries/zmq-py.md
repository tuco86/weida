# ZeroMQ for Python (pyzmq 27.2.0 over libzmq 4.3.5) — feature parity

The parity document of `weida-zmq-py`, the Python binding of the `weida-zmq` library. Its
reference implementation is **pyzmq**, because that is what a Python program that speaks
ZeroMQ uses today; the protocol-level parity against libzmq itself is [`zmq.md`](zmq.md) and
is not repeated here. This document answers one question per row: *what does a Python caller
who knows pyzmq get, and what does that caller not get.*

## 1. What a row means

Three verdicts and no others:

- **present** — implemented, with the module or test that proves it;
- **refused** — the call exists and fails at configuration time with a reason the code gives;
- **absent** — not implemented, with what is missing named.

"Partial" is not a verdict (0013 §4.7 clause 6). Where this binding's shape differs from
pyzmq's the row says so and §9 states why.

Measured against: pyzmq 27.2.0 over libzmq 4.3.5 with libsodium (`zmq.has("curve")` true),
CPython 3.13.14, PyO3 0.29.2, `abi3` from CPython 3.9, Linux x86-64. Every number in §8 is
measured by a program in the repository against that configuration.

## 2. Socket types

| pyzmq | This binding | Verdict |
| --- | --- | --- |
| `zmq.REQ` | `ReqSocket` | present — `sockets.rs`, `tests/test_messages.py::test_req_rep` |
| `zmq.REP` | `RepSocket` | present |
| `zmq.DEALER` | `DealerSocket` | present |
| `zmq.ROUTER` | `RouterSocket` | present, routing id as the first frame |
| `zmq.PUB` | `PubSocket` | present, `send` returns a `Published` report |
| `zmq.SUB` | `SubSocket` | present, with `subscribe`/`unsubscribe` |
| `zmq.XPUB` | `XPubSocket` | present, subscriptions delivered to the application |
| `zmq.XSUB` | `XSubSocket` | present |
| `zmq.PUSH` | `PushSocket` | present |
| `zmq.PULL` | `PullSocket` | present |
| `zmq.PAIR` | `PairSocket` | present, one peer |
| `zmq.STREAM` | — | absent: `weida-zmq` implements no raw-TCP socket type, so there is nothing to bind ([`zmq.md`](zmq.md) §2) |
| `zmq.SERVER`/`CLIENT`/`RADIO`/`DISH`/`SCATTER`/`GATHER`/`PEER`/`CHANNEL` | — | absent: libzmq DRAFT socket types, absent from stable libzmq builds too, so no peer can rely on them |

**One class per type, not one `Socket` plus a constant.** `weida_zmq.ReqSocket(context)` where
pyzmq writes `context.socket(zmq.REQ)`. A PUB has no `recv` here and a PULL has no `send`:
`zmq_socket(3)`'s own table as a type error at construction instead of `ENOTSUP` at the first
call.

## 3. Transports

| pyzmq | This binding | Verdict |
| --- | --- | --- |
| `tcp://` | `tcp://` | present |
| `ipc://` | `ipc://` | present, `AF_UNIX` with bind hygiene and peer credentials |
| `inproc://` | `inproc://` | present, context-scoped |
| `udp://`, `pgm://`, `epgm://`, `ws://`, `wss://`, `vmci://`, `tipc://`, `vsock://` | — | absent, and their options are **refused by name** with "no transport" rather than ignored (§5) |

## 4. Mechanisms and authentication

| pyzmq | This binding | Verdict |
| --- | --- | --- |
| NULL | default | present |
| PLAIN (`plain_server`, `plain_username`, `plain_password`) | same options by libzmq name | present — `tests/test_security.py`, `tests/test_interop.py` |
| CURVE (`curve_server`, `curve_publickey`, `curve_secretkey`, `curve_serverkey`) | same, keys as 32 bytes or 40-character Z85 | present, interoperating with libsodium's boxes in both directions |
| `zmq.curve_keypair()` | `weida_zmq.curve_keypair()` | present |
| GSSAPI | — | absent: libzmq's own manual deprecates it in favour of ZAP, and the options are refused with that reason |
| `zmq.auth`, `zmq.auth.thread.ThreadAuthenticator` | — | absent as a *framework*: ZAP here is a REP socket the application binds to `inproc://zeromq.zap.01`, which is what 27/ZAP is. A handler is ~10 lines of Python (`tests/test_security.py::ZapHandler`) and needs no thread of its own. What pyzmq's authenticator adds — an allow/deny list, a password file, a certificate directory — is policy this binding does not ship. |
| `CurveKey`, `RoutingId`, `ZapUserId` as types | present | distinct Python types with no conversion between them and none to any weida identity. `CurveKey` is one type for the public and the secret half, so it treats every key as a secret: its `repr` names the type and prints no octets, `hash()` is keyed per process, and `==` compares every octet (`test_security.py::test_a_curve_key_never_shows_its_octets_in_a_repr_or_a_str`) |
| `zmq.Socket.close()` from another thread while `recv()` is parked | `sync` sockets: the parked call raises `ENOTSOCK` and `close()` returns | present: a wait with no deadline ends on `close()` from any thread and on Ctrl-C on the main one (`test_sync.py::test_close_from_another_thread_ends_a_receive_with_no_deadline`, `test_ctrl_c_ends_a_receive_with_no_peer_and_no_deadline`); every other call on the socket queues behind a parked receive, as two coroutines do on the asyncio surface |

## 5. Options

Every one of `weida-zmq`'s 98 option rows is reachable from Python: `weida_zmq.OPTIONS`,
`weida_zmq.option(name)`, and `SocketOptions.set(name, value)` / `ContextOptions.set(...)`.
An option this library does not implement **raises `EINVAL` at the `set` call** with one of
five reasons — no transport, draft only, deprecated in favour of ZAP, replaced by a
`weida-runtime` construct, absent — and never silently does nothing.
`tests/test_options.py` walks all 98 rows.

Differences from pyzmq's option surface:

| | pyzmq | This binding |
| --- | --- | --- |
| Naming | `sock.setsockopt(zmq.SNDHWM, 1000)`, or the `sock.sndhwm` property | `options.set("ZMQ_SNDHWM", 1000)` — libzmq's own name as a string, because a refusal has to be able to name the option it refused |
| When | on a live socket, at any time | at construction: `Socket(context, options)`. An option that cannot be delivered fails before the socket exists |
| Durations | milliseconds, with `-1` and `0` as three different sentinels | **seconds**, with `None` for the sentinel, and each row's documentation names which libzmq value the `None` is |
| Reading back | `sock.getsockopt(...)` | absent: no getter. The table's `binding` field says where a value lives; the values a caller set are the caller's |
| `zmq.Context` options (`zmq.IO_THREADS`, `zmq.MAX_SOCKETS`) | `ctx.set(...)` | `ContextOptions` plus keyword arguments; `ZMQ_IO_THREADS` is refused and replaced by `worker_threads` |

## 6. Observability

| pyzmq | This binding | Verdict |
| --- | --- | --- |
| `sock.get_monitor_socket()` | `await sock.monitor()` → async iterator of typed `MonitorEvent` | present, and typed: `name`, `id`, `endpoint`, `reason`, `value`. The failure *reason* is words here where libzmq's wire form carries a number |
| the two-frame `inproc` monitor message | `await monitor.serve_pair(pair)` | present, the same octets, for a port that reads the wire form |
| `zmq.utils.monitor.recv_monitor_message` | — | absent: there is nothing to decode, because the events arrive typed. The wire form is available through `serve_pair` for code that wants to decode it anyway |
| `sock.events`, `sock.fd`, `zmq.Poller` | — | absent: they are the edge-triggered polling API an asyncio binding exists to replace. A caller that wants to wait on several sockets uses `asyncio.gather`, `asyncio.wait` or a `TaskGroup` |

## 7. Devices and helpers

| pyzmq | This binding | Verdict |
| --- | --- | --- |
| `zmq.proxy(frontend, backend, capture=None)` | `await weida_zmq.proxy(...)` | present, returning the counters it accumulated |
| `zmq.proxy_steerable(...)` | `await weida_zmq.proxy_steerable(frontend, backend, control, capture=None)` | present, with `CONTROL_PAUSE`/`RESUME`/`TERMINATE`/`STATISTICS` and a `ProxyStatistics` of libzmq's eight counters |
| `zmq.devices.ThreadProxy`, `ProcessDevice`, `ThreadDevice` | — | absent: they are `threading`/`multiprocessing` wrappers around the proxy. `asyncio.create_task(weida_zmq.proxy(...))` is the same thing in this surface, and a cancelled task ends it |
| a proxy between REQ and REP | — | refused: `TypeError` at the call. 28/REQREP's alternation is the application's to drive, `weida_zmq::Device` is not implemented for those two, and libzmq's devices do not accept them either |
| a PULL→PUSH streamer device | `weida_zmq.proxy(pull, push)` | present: `weida_zmq::proxy` reads only the sides that receive (`Device::RECEIVES`), so an end that cannot receive is a side that never delivers rather than an `ENOTSUP` at the first poll; a device whose two ends both never deliver is refused. Proved in the library by `a_streamer_forwards_through_a_side_that_never_receives` and from Python by `test_monitor.py::test_a_streamer_device_forwards` |
| `zmq.asyncio.Context`/`Socket` | — | absent as an API shape: this binding is asyncio-first, so there is no second "asyncio flavour" of it to import. The synchronous surface is `weida_zmq.sync` |
| `send_string`, `recv_string`, `send_json`, `send_pyobj` | — | absent: a payload is `bytes`, and a `str` is refused with a message saying to encode it. Guessing an encoding or a serialization for somebody else's wire format is not this library's to guess |
| `zmq.Frame`, `copy_threshold`, `sock.send(..., copy=False)` | — | absent: a message is a `Multipart` value of `bytes` frames. What the copy costs is measured, in [IMPLEMENTATION.md](../IMPLEMENTATION.md) §4.14, rather than made configurable |

## 8. Interop evidence

`crates/zmq/weida-zmq-py/tests/test_interop.py`, against pyzmq 27.2.0 over libzmq 4.3.5.
Skipped with its install command where pyzmq is absent.

- **32 pairing runs**: eight pairings both implementations support — REQ/REP, DEALER/ROUTER,
  DEALER/REP, REQ/ROUTER, PUSH/PULL, PUB/SUB, XPUB/XSUB, PAIR/PAIR — each run with this
  binding in **either role** of the pattern and as **both** the bound and the connecting side.
- **PLAIN and CURVE in both directions**: this binding as the security server, authorized by a
  ZAP handler written in Python, with a libzmq client; and libzmq as the server behind pyzmq's
  `ThreadAuthenticator`, with this binding as the client.
- **The asyncio surface** against libzmq as well as the synchronous one.
- **Nothing disagreed.** No pairing needed a workaround, and no frame layout differed.

Cost, measured by `crates/zmq/weida-zmq/examples/roundtrip_cost.rs` and
`crates/zmq/weida-zmq-py/roundtrip_cost.py` and recorded in
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §4.15: a REQ/REP round trip is **16.5 µs**
in Rust, **67.7 µs** through `weida_zmq.sync`, **330 µs** through the asyncio surface and
**5.7 µs** through pyzmq, over `inproc://`; over loopback `tcp://` the four are 32.4, 111,
343 and 45.8 µs. The asyncio figure is one event-loop wakeup per `await`, and the fast path
that would remove it for an operation that does not wait is filed rather than hinted at.

## 9. Deliberate deviations

1. **One operation at a time per socket object, and `split()` where that is not enough.** A
   task parked in `await sock.recv()` holds the socket, so a concurrent
   `await sock.send(...)` on that *same object* queues behind it: a `weida-zmq` socket's
   operations take `&self` on a handle that is not `Sync`, which is libzmq's thread rule as a
   type. pyzmq allows the overlap, so the five socket types whose two directions are
   independent have `await sock.split()`: it returns `(send_half, recv_half)` onto the
   same connections, usable from two coroutines at once, and retires the object it was called
   on (`ENOTSOCK` naming the split). DEALER, ROUTER, PAIR, XPUB and XSUB have it; REQ and REP
   do not, because 28/REQREP's alternation is a single sequence and two halves would promise
   an independence the protocol forbids. Each half is itself one operation at a time, for the
   same reason.
2. **Every awaiting call is a coroutine**, including `connect`, `unbind`, `last_endpoint` and
   `close`, which `weida-zmq` answers synchronously. They need the socket, the socket may be
   held by a parked receive, and a synchronous Python method that waited for it would block
   the event-loop thread. The two `_nowait` methods are synchronous, because they never wait.
3. **The work starts at the first `await`, not at the call.** `sock.recv()` builds a coroutine
   and starts nothing; a coroutine never awaited does nothing at all. Starting at the call
   would lose a message to a task cancelled before it ran.
4. **Seconds, not milliseconds**, for every duration, with `None` for libzmq's sentinels (§5).
5. **Two defaults differ from libzmq**, inherited from the library and readable from Python:
   `ZMQ_LINGER` is finite (`weida_zmq.DEFAULT_CLOSE_BUDGET`, 1.0 s) where libzmq's is
   infinite, and `ZMQ_MAXMSGSIZE` is bounded (`DEFAULT_MAX_MESSAGE_SIZE`, 1 MiB) where
   libzmq's is "no limit".
6. **Failures are classes, not numbers.** 19 exception classes, one per errno name, under
   `weida_zmq.ZmqError`, each carrying `errno` and `cause`. pyzmq raises `zmq.ZMQError` with
   an `errno` attribute; `except EAGAIN` is the difference.
7. **Ceilings libzmq does not have**, all inherited from the library and all settable:
   a frame count per message, a subscription count and length per peer, and a peer count per
   socket ([`zmq.md`](zmq.md) §9).

## 10. The definition of done

| Clause | Verdict | Where |
| --- | --- | --- |
| Every socket type of the library is bound | yes, all eleven | §2 |
| Every option row reachable, refusals at configuration time | yes, 98 rows walked by a test | §5 |
| The errno vocabulary survives as branchable classes | yes, 19 classes under one base | §9.6 |
| Asyncio first, synchronous second and over the same code | yes, `weida_zmq.sync` over the library's `blocking` facade | §7, §9.2 |
| Security in both directions, ZAP handler in the binding's language | yes | §4, §8 |
| Interop in both roles against the reference implementation | yes, 32 pairings plus security both ways | §8 |
| Payload boundary measured rather than assumed | yes | §8, IMPLEMENTATION.md §4.14 and §4.15 |
| No row says "partial" | yes: §7 and §9.1 name what a caller does not get | §7, §9 |

## 11. Sources

- Code: `crates/zmq/weida-zmq-py/src/` (`sockets.rs`, `ops.rs`, `options.rs`, `identity.rs`,
  `monitor.rs`, `devices.rs`, `sync.rs`, `errors.rs`), `crates/py/weida-py-core/src/`.
- Tests: `crates/zmq/weida-zmq-py/tests/` — `test_sockets.py`, `test_messages.py`,
  `test_options.py`, `test_security.py`, `test_monitor.py`, `test_sync.py`, `test_interop.py`.
- Measurements: [IMPLEMENTATION.md](../IMPLEMENTATION.md) §4.14 and §4.15.
- Decisions: [0014](../decisions/0014-parallel-libraries.md) §2 (one shared PyO3 foundation,
  asyncio first, the bytes boundary), [0013](../decisions/0013-competitor-libraries.md) §4.4
  (the three constructors, options honoured or refused, the two deliberate defaults, identity
  types apart).
- The library's own parity table: [`zmq.md`](zmq.md). The protocol sheet it cites:
  [`../research/zeromq.md`](../research/zeromq.md).
