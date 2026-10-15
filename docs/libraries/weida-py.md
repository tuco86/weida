# weida for Python (the Rust API of `crates/weida`) — API parity

The parity document of `weida-py`, the Python binding of **weida itself**, built on
`weida-py-core`. It answers one question per row: *what does a Python
caller get of the API this binding binds, and what does that caller not get.*

## 1. What a row means, and what the other column is

Three verdicts and no others:

- **present** — implemented, with the module or test that proves it;
- **refused** — the call exists and fails with a reason the code gives;
- **absent** — not implemented, with what is missing named.

"Partial" is not a verdict ([0013](../decisions/0013-competitor-libraries.md) §4.7 clause 6),
and neither is "mostly", "planned" or "good coverage" ([README.md](README.md) rule 2).

**The one way this document differs from the other five.** [`zmq-py.md`](zmq-py.md),
[`nng-py.md`](nng-py.md), [`mqtt-py.md`](mqtt-py.md), [`amqp-py.md`](amqp-py.md) and
[`nats-py.md`](nats-py.md) are parity documents against a **foreign reference
implementation** — pyzmq, pynng, paho-mqtt, python-qpid-proton, nats-py — because a Python
program that speaks those protocols already has one to compare against. This binding has no
foreign reference: weida has no other Python client, and no second implementation of the
protocol exists in any language. The reference here is therefore **the Rust API this module
binds**: every public surface of `crates/weida` is present in Python, refused with a reason,
or absent with a reason, and the left-hand column of every table below is a Rust item read out
of `crates/weida/src/`. That makes this the one document in this directory whose two columns
are the same protocol at two layers rather than two implementations of one protocol; nothing
here is an interop claim about a foreign peer, because there is no foreign peer to make one
about (§2, last paragraph).

**What each side was read from.** The Python column is `crates/py/weida-py/src/` — `lib.rs`,
`runtime.rs`, `endpoints.rs`, `pubsub.rs`, `patterns.rs`, `streams.rs`, `sync.rs`, `values.rs`,
`errors.rs` —
and the tests under `crates/py/weida-py/tests/`. The Rust column is `crates/weida/src/`:
`lib.rs`'s re-exports, `config.rs`, `runtime.rs`, `listener.rs`,
`endpoint.rs`, `transfer.rs`, `stream.rs` and `blocking.rs`. CPython 3.9 and up through PyO3
0.29 with `abi3-py39` (the `pyo3` entry of the workspace `Cargo.toml` and `requires-python` in
`crates/py/weida-py/pyproject.toml`), the library entered with the `generate` and `blocking`
features (the `weida` dependency of `crates/py/weida-py/Cargo.toml`), Linux x86-64.

**How the numbers were counted.** Every count in this document was counted out of the tree,
not estimated:

| Number | Value | Counted from |
| --- | --- | --- |
| `#[pyclass]`es | **44** | 26 `add_class` calls in `lib.rs` plus 18 in `sync.rs`'s `install` |
| `weida.__all__` | **62** names | 33 literals in `every_name` (which include `sync`, `VERSION`, `ALPN`, `WeidaError` and the five cursor names), the five named levels appended after them, plus the 24 of `errors::NAMES` |
| `weida.sync.__all__` | **18** names | the list in `sync.rs`'s `install`, one per class registered above it |
| Exception classes | **25** | the 24 entries of the `failures!` invocation in `errors.rs`, **plus the base** `WeidaError` (`errors.rs:33`), which is not in that list |
| Test functions | **34** | 8 in `tests/test_asyncio.py`, 10 in `tests/test_patterns.py`, 9 in `tests/test_sync.py`, 7 in `tests/test_cursors.py` |
| `IncomingMeta` attributes | **12** of the Rust struct's 15 fields | `values.rs:147-185` against `crates/weida/src/transfer.rs`; `tracestate`, `achieved` and `peer_chain` are absent (§5) |

## 2. How it is built, tested and packaged

| Rust-side fact | This binding | Verdict |
| --- | --- | --- |
| the crate is `weida_py`, the module is `weida` | one `#[pymodule] fn weida` in `lib.rs`; the crate cannot share the name because rustdoc writes one directory per lib name | present, and the reason is in the `[lib]` comment of `Cargo.toml` |
| `cargo test` links libpython, a wheel must not | `extension-module` is off by default in the crate and turned on by maturin (the `extension-module` feature in `Cargo.toml`, `[tool.maturin] features` in `pyproject.toml`) | present |
| — | `develop.sh` | present: a wrapper that calls `crates/py/develop.sh` with this directory, the one script every binding shares — a `uv` virtualenv at the repository root, `maturin` and `pytest` into **that** virtualenv and nothing into a system interpreter, `maturin develop`, then pytest with the caller's arguments |
| — | `package.sh` | present: a wrapper that calls `crates/py/package.sh` with this directory and the wheel prefix `weida` — `maturin build --release`, a wheel whose name is **checked to contain `abi3`** or the script exits 1, a fresh temporary virtualenv holding only that wheel, and `smoke.py` run with `PATH=/usr/bin:/bin` — so "the wheel needs no Rust toolchain" is established rather than expected |
| `abi3-py39`, one wheel for CPython 3.9 and up | `requires-python = ">=3.9"` (in `pyproject.toml`), one `abi3` wheel | present, and the two agree **for GIL-enabled CPython** only. Read out of PyO3 0.29.2's own selector (`pyo3-build-config-0.29.2/src/impl_.rs:96-132`): a **free-threaded** interpreter below 3.15 gets `None` from `applicable_stable_abi` and therefore a version-specific build, because the free-threaded stable ABI (`abi3t`) starts at 3.15 — so `cp39-abi3` does not serve 3.13t or 3.14t and a second, version-specific wheel would be needed for them. PyPy and GraalPy take the same branch from the other side: a stable ABI is selected but pinned to the interpreter's own version, so their artifact is their own too (§10) |
| — | what `smoke.py` claims | present, and it is five claims: the installed distribution's name and version (the version is the workspace version that `crates/py/package.sh` passes in `WEIDA_WORKSPACE_VERSION`, in PEP 440 spelling), every name of `weida.__all__` resolving plus `RuntimeFailure` under `WeidaError` and `weida.Runtime` **not** a `BaseException`, a Req/Rep round trip, a Push/Pull transfer and a Pub/Sub message over real QUIC in one process, a **10 MiB streamed fan-out against an 8 MiB subscriber budget**, and a wrong fingerprint refused with the key that answered in the message |
| the whole thing under one wall-clock bound | `asyncio.run(asyncio.wait_for(round_trip(), 30.0))` in `smoke.py`, and each test file's `run()` helper wraps its coroutine in `asyncio.wait_for(..., 15.0)` | present: a binding that deadlocks fails the suite instead of hanging it |
| a broker, a server or a C library to test against | none needed | present: both halves of every test are this library (the module docstring of `test_asyncio.py` says so) |

**There is no interop section in this document, and that is not an omission.** The other five
Python parity documents have one because a foreign implementation of their protocol exists to
run against. weida's protocol has exactly one implementation, `crates/weida`, and this binding
is a thin wrapper over it, so a "pairing" here would be this library against itself — which is
what every test in `crates/py/weida-py/tests/` and `smoke.py` already is. The claim a real
interop section would carry is therefore unavailable, and no number in this document is an
interop number. The counts above were counted out of the tree with the sources named in §1
**and re-measured by a run**: 32 tests passed and
`smoke.py` resolved all 56 names through a freshly built wheel.

## 3. The runtime, the binding and the reactor

| `crates/weida` | This binding | Verdict |
| --- | --- | --- |
| `Runtime::owned(config)` (`runtime.rs:220`) | `weida.Runtime(worker_threads=1)` | present — `runtime.rs:53-66`, used by every test |
| `Runtime::new(config)` (needs an ambient Tokio reactor), `Runtime::with_handle(handle, config)` | — | absent, and the reason is that they mean nothing here: a plain Python process has no ambient Tokio runtime and no Python caller can hold a `tokio::runtime::Handle` (`runtime.rs:3-11`). `owned` is the only constructor that can serve this surface, so it is the only one offered |
| `RuntimeConfig`'s thirteen fields (`config.rs`) | `worker_threads`, and nothing else | present for one field — absent for `limits`, `max_connections`, `max_connections_per_peer`, `endpoint_queue`, `max_resolved_addresses`, `shutdown_timeout`, `guarantees`, `connect_attempt_timeout` and the five of [0031](../decisions/0031-transparent-redial-and-the-sender-outbox.md), `reconnect`, `send_timeout`, `outbox_messages`, `outbox_bytes` and `outbox_full`: a Python caller gets `RuntimeConfig::default()` for all of them (`runtime.rs:56-59`) and cannot change them. The redial therefore runs with its defaults from Python, and the `PeerEvent` stream of 0031 §4.8 has no Python surface. §10 |
| `Runtime::exec()` (`runtime.rs:431`) | not a method; the bridge is built on it | present as the *reason that accessor exists*: `Bridge::new(runtime.exec().clone(), ...)` at `runtime.rs:61` puts this crate's futures on the runtime's own executor, so one process holds **one** Tokio runtime rather than two (`runtime.rs:13-18`) |
| `Runtime::listener()` then `Listener::bind_quic(addr, identity)` | `await runtime.bind("host:port", identity)` → `Binding` | present as one call — `runtime.rs:78-109`. `bind` is a coroutine because it creates a socket and a QUIC endpoint, and a `__new__` that did that would block the event loop (`runtime.rs:20-25`) |
| `Listener::bind_inproc(bus)` | `runtime.bind_inproc(bus)` → `Binding`, on both surfaces | present, synchronous because nothing is created that could block; the binding's `url(path)` is `weida+inproc://<bus><path>`, `fingerprint()` is `None` and `local_addr()` the bus name. A client in the same process dials it |
| `Listener::bind_unix`, `bind_pipe` | — | absent: a Python **server** on a socket transport is what is missing; the dialling side's address grammar accepts `weida+unix://` and `weida+pipe://` (`endpoints.rs:59-62`). §10 |
| `Listener::replier/puller/publisher(path)` | `binding.replier(path)`, `.puller(path)`, `.publisher(path)` | present, synchronous, raising `weida.AlreadyRegistered` or `weida.InvalidEndpointPath` — `runtime.rs:225-261`. `pair`, `respondent` and `bus` join them at `runtime.rs:263-311` (§4.4) |
| `Listener::acceptor(path)` (`listener.rs:350`) | — | absent: the raw L0 surface. §9.2 |
| `Binding::local_addr()` | `binding.local_addr()` → `str` | present — `runtime.rs:205-207`, and port `0` resolved is what every test dials |
| `Binding::close()` (`listener.rs:378`) | — | absent: dropping the Python `Binding` unbinds, because it holds the `Arc<weida::Binding>` (`runtime.rs:193-195`). What is missing is an awaited close that waits for the endpoint to go idle |
| `Identity::fingerprint()` on the serving side | `binding.fingerprint()`, `binding.url(path)` | present, and both are Python-only conveniences with no single Rust counterpart: `url` formats `weida://{fingerprint}@{local}{path}`, which is the whole of a client's configuration — `runtime.rs:209-218`, asserted at `test_sync.py:136` |
| `Runtime::drain(deadline: Duration)` → `Drained` | `await runtime.drain(seconds: float)` → `(delivered, outstanding)` | present — `runtime.rs:167-182`, `test_asyncio.py::test_a_drain_reports_what_it_achieved`. Seconds as a float, mandatory and finite. §9.4 |
| `Runtime::drain` consumes `self` | the Python `Runtime` stays usable | present, deliberately: `weida::Runtime` is `Clone` and a clone shares the pool, the bindings and the drain state, so the binding drains a clone (`runtime.rs:174-177`) |
| `Runtime::shutdown()` (`runtime.rs:323`) | — | absent from the asyncio surface: `drain` is the only end there, and the reactor otherwise dies with the last handle that can cause work on it (`endpoints.rs:10-12`). `weida.sync.Runtime.shutdown()` exists (`sync.rs:203-209`), so the gap is the asyncio surface's alone. §10 |
| `Runtime::suppressed_duplicates()` (`runtime.rs:251`), `Runtime::config()` (`runtime.rs:416`) | — | absent: two read-only accessors with no Python spelling, and `runtime.rs:47-187` registers neither |
| `Runtime::peer(tls)` → `Peer` (`runtime.rs:272`) | — | absent: the raw dialling core. §9.2 |

## 4. The asyncio surface, class by class

Twenty-six classes in `weida` (§1). Every call that waits is a coroutine and the waiting happens
on the reactor with the GIL released; the Rust future starts at the **first `await`**, not at
the call, so a coroutine that is never awaited does nothing at all
(`crates/py/weida-py-core/src/bridge.rs:131-153`).

### 4.1 Req/Rep

| `crates/weida` | This binding | Verdict |
| --- | --- | --- |
| `Runtime::requester(tls)` (`runtime.rs:282`) | `runtime.requester(trust)` → `Requester` | present — `runtime.rs:112-118` |
| `Requester::connect(url)` | `await requester.connect(url)` | present — `endpoints.rs:79-85`; the address forms and the failures are written once at `endpoints.rs:53-68` |
| `Requester::request(body)` / `request_with(meta, body)` | `await requester.request(payload, max_reply_bytes)` → `bytes` | present — `endpoints.rs:101-119`. Two differences: the ceiling is mandatory (§8) and the binding sets `content_len` for the caller (`endpoints.rs:112`, asserted as `meta.content_len == 4` at `test_asyncio.py:48`). Proved end to end by `test_asyncio.py::test_a_request_is_answered_over_quic` |
| `Requester::open(meta)` → `(OutgoingTransfer, ReplyStream)` | `await requester.open()` → `(OutgoingStream, Reply)` | present — `endpoints.rs:127-141`, `test_patterns.py::test_a_streamed_exchange_reads_the_reply_while_writing`, which reads the reply's `meta.endpoint is None` and collects `b"streamed request"` |
| `Requester::peer_count()` | — | absent on `Requester` and `Pusher`; `Subscriber.peer_count()` has it (`pubsub.rs:336-338`). The Rust method exists on all three (`endpoint.rs:131`, `225`, `490`) |
| `Requester::connection_stats()` and the same method on every dialling endpoint, `ConnectionStats`, `TransportStats`, `PathStats`, `UdpCounts`, `RemoteStats` ([0036](../decisions/0036-connection-statistics.md)) | `connection_stats()` → list of `weida.ConnectionStats` on `Requester`, `Pusher`, `Subscriber`, `Paired`, `Surveyor`, `BusMember` and `Dish` of both surfaces; `url`, `age` (seconds), `redials`, `transport` (`path`, `tx`, `rx`) or `None`, `remote` or `None` | present, synchronous on both surfaces; times are `float` seconds. `Runtime(path_report=True)` turns the peer's view on; both ends must set it. Asserted by `test_asyncio.py::test_connection_stats_over_quic_and_in_process` and its `test_sync.py` twin: an RTT over QUIC, the remote view when both report, `transport is None` over `weida+inproc` |
| `Listener::replier(path)`, `Replier::path()` | `Replier.path()` | present — `endpoints.rs:232-235` |
| `Replier::accept()` → `IncomingRequest` (payload still a stream) | `await replier.accept(max_bytes)` → `Request` (payload already `bytes`) | present, and the shape differs on purpose: the accept and the `take_body().collect(max_bytes)` are one call (`endpoints.rs:247-267`), because a Python object is materialized anyway |
| `IncomingRequest::meta()`, its body | `Request.payload` → `bytes`, `Request.meta` → `IncomingMeta` | present as getters — `endpoints.rs:295-304` |
| `IncomingRequest::reply(meta)` → `OutgoingTransfer` (streamed) | `await request.reply(payload)` — whole payload, `finish` included | present — `endpoints.rs:314-330` |
| `IncomingRequest::refuse(code)` for **any** `ErrorCode` | `await request.refuse()` — always `ErrorCode::Rejected` | present as one refusal, absent as a choice: there is no `weida.ErrorCode`, so the code is hard-wired (`endpoints.rs:341`, `sync.rs:412`) where `crates/weida/src/transfer.rs:626` takes one. §10 |
| an exchange carries one reply | the second `reply` raises `weida.NoReply` | present, and enforced rather than documented: the request is a `Mutex<Option<...>>` taken once (`endpoints.rs:281-290`, `351-369`), proved by `test_asyncio.py::test_a_request_carries_one_reply` |
| dropping an unanswered request causes ERROR `{NO_REPLY}` | same, inherited | present — `endpoints.rs:274-279`, the rule [PROTOCOL.md](../PROTOCOL.md) §9.4 makes mandatory |
| `IncomingRequest::canceled()` (`transfer.rs:563`) | — | absent: a handler cannot see the requester walk away before it answers. It learns at `reply`, as `weida.Canceled` (`endpoints.rs:310-311`). §10 |

### 4.2 Push/Pull

| `crates/weida` | This binding | Verdict |
| --- | --- | --- |
| `Runtime::pusher(tls)`, `Pusher::connect(url)` | `runtime.pusher(trust)`, `await pusher.connect(url)` | present — `runtime.rs:121-127`, `endpoints.rs:157-163` |
| `Pusher::send(body)` / `send_with(meta, body)`, the receipt through `Delivery::delivered()` | `await pusher.send(payload)` | present as one call — `endpoints.rs:179-199`: open, `write_all`, `finish`, `delivered`. The receipt is QUIC's fin-acknowledgement and **not** an application acknowledgement, and the docstring says so ([GUARANTEES.md](../GUARANTEES.md) §1) |
| `Pusher::open(meta)` → `OutgoingTransfer` | `await pusher.open()` → `OutgoingStream` | present — `endpoints.rs:205-216` |
| `Listener::puller`, `Puller::path()`, `Puller::recv()` | `Puller.path()`, `await puller.recv(max_bytes)` → `(bytes, IncomingMeta)` | present — `endpoints.rs:381-399`, `test_asyncio.py::test_a_push_reaches_a_puller_with_its_metadata`, which asserts `meta.endpoint == "/ingest"` and `meta.content_len == 8` |
| — | `await puller.recv_stream()` → `(IncomingStream, IncomingMeta)` | present, and it has no single Rust counterpart because `Puller::recv` already hands back a stream: this is the call that does *not* collect (`endpoints.rs:403-412`) |

### 4.3 Pub/Sub

| `crates/weida` | This binding | Verdict |
| --- | --- | --- |
| `Publisher::publish(topic, payload)` → `usize` | `publisher.publish(topic, payload)` → `int`, **not** a coroutine | present — `pubsub.rs:78-81`. A publish never waits for a subscriber, so making it awaitable would promise a wait that does not happen (`pubsub.rs:68-70`); `test_patterns.py::test_a_published_message_reaches_the_matching_filter_only` asserts `1` for a match and `0` for a non-match |
| a payload above `Limits::subscriber_buffer_bytes` | `weida.LimitExceeded` | refused at the call — `pubsub.rs:74-77`, asserted against the 8 MiB budget at `test_patterns.py:115-119` |
| `Publisher::open(topic)` → `FanOut` | `publisher.open(topic)` → `FanOut`, not a coroutine | present — `pubsub.rs:89-96` |
| `FanOut::write_within(chunk, limit)` | `await fan.write_within(chunk, seconds)` → subscribers left | present — `pubsub.rs:189-208`; the budget bounds a **chunk**, so the payload has no ceiling (`pubsub.rs:186-188`) |
| `FanOut::write_now(chunk)` | `await fan.write_now(chunk)` | present, and a coroutine although it never waits on a subscriber: two coroutines may hold the same `FanOut` and the lock is the one thing it can wait for (`pubsub.rs:24-28`, `212-224`) |
| both write forms, neither a default | both, and the reason is in the module's own table | present — `pubsub.rs:4-22`: only the first would make a video publisher wait on its slowest viewer, only the second would make a payload larger than the budget undeliverable to everybody |
| `FanOut::subscribers()`, `FanOut::finish()` | `await fan.subscribers()`, `await fan.finish()` | present — `pubsub.rs:173-179`, `232-238`. `finish` returns how many received all of it "as far as this side can tell", because a fan-out copy carries no receipt |
| a transfer ends once | a second `finish` or a later `write_now` raises `weida.NoReply` | present — `pubsub.rs:154-160`, proved by `test_patterns.py::test_a_finished_transfer_is_finished` |
| `Publisher::subscriber_count()`, `filter_count()`, `dropped()`, `dropped_on(topic)` | all four | present — `pubsub.rs:99-135`. `dropped_on` returns `(total, subscriber_budget, subscriber_queue, no_parked_connection)`: three causes rather than one number, and `test_patterns.py::test_a_slow_subscriber_loses_copies_and_the_publisher_says_which_topic` asserts they sum to the total and that an untouched topic is `None` |
| `Publisher::drops()` → `Vec<TopicDrops>` (`endpoint.rs:416`) | — | absent: there is no call that enumerates every topic that lost a copy, only `dropped_on` per topic |
| `Publisher::publish_with_trace`, `open_with_trace` (`endpoint.rs:333`, `384`) | — | absent: a Python caller cannot propagate an inbound trace into an outbound publish. It can *read* one, as `IncomingMeta.traceparent` (§5) |
| `Runtime::subscriber(tls)`, `Subscriber::connect/subscribe/unsubscribe/recv` | `runtime.subscriber(trust)`, then all four | present — `runtime.rs:130-136`, `pubsub.rs:270-317`; the segmented filter grammar of [PROTOCOL.md](../PROTOCOL.md) §6.4 is the library's, and a filter it rejects is `weida.Protocol` |
| — | `await subscriber.recv_stream()` | present — `pubsub.rs:321-333` |
| `Subscriber::filter_count()` (`endpoint.rs:542`) | — | absent on the Python `Subscriber`; the *publisher* side's `filter_count` is what the tests wait on (`test_patterns.py:42-53`) |

### 4.4 PAIR, SURVEY and BUS

Present on both surfaces: `crates/py/weida-py/src/patterns.rs`, and a `weida.Survey` in `values.rs`.

| `crates/weida` | This binding | Verdict |
| --- | --- | --- |
| `Runtime::pair(tls)`, `Listener::pair(path)` — one type, two roles | `runtime.pair(trust)` and `binding.pair(path)`, both → `Paired` | present — `runtime.rs:139-149`, `runtime.rs:263-278`; what distinguishes the roles is which of them calls `connect`, as in Rust |
| `Paired::connect(url)` refusing a second call with `LimitExceeded` | same, and the class is the library's | present — `patterns.rs:67-73`, asserted by `test_patterns.py::test_a_pair_carries_both_directions_and_keeps_its_first_peer`, which dials twice on one endpoint |
| a **second peer** is refused per transfer, and the first keeps working | same, inherited | present, and it is the rule the test spends its bytes on: the newcomer's `connect` succeeds, its `send` raises `weida.LimitExceeded`, and the first peer's next `send` is still received — written past the 1 MiB stream window so the refusal cannot lose the race to the receipt ([0005](../decisions/0005-refusal-race.md)) |
| `Paired::send/send_with`, `recv()`, `path()`, `peer_count()` | `await paired.send(payload)`, `await paired.recv(max_bytes)` → `(bytes, IncomingMeta)`, `paired.path()`, `paired.peer_count()` | present — `patterns.rs:56-133`. `send` sets `content_len` for the caller, as Req/Rep does |
| `Paired::open(meta)` → `OutgoingTransfer` | — | absent: a pair has no streamed form here, for the reason §9.3 gives for `sync` — the whole-payload call is the one a Python caller reaches for, and the Rust API is the streamed one |
| `Runtime::surveyor(tls)`, `Surveyor::connect/peer_count` | `runtime.surveyor(trust)`, `await surveyor.connect(url)`, `surveyor.peer_count()` | present — `runtime.rs:151-158`, `patterns.rs:142-153`; a surveyor accumulates peers on purpose, and the test connects two respondents on one binding |
| `Surveyor::survey(body, deadline)` → `SurveyRun`, then `next(max_bytes)` per answer | `await surveyor.survey(payload, deadline_seconds, max_reply_bytes)` → **`weida.Survey`**, a value | present as a **collected value**, and this is the one shape that is a decision rather than a translation: the only channel out of a bridged future here is the errno family (`crates/py/weida-py-core/src/bridge.rs:154-158`), so an `async for` over answers would need a second error channel invented for `StopAsyncIteration` alone. The Rust `SurveyRun` stays the place answers arrive one at a time (`values.rs:206-216`, `patterns.rs:169-206`) |
| `SurveyRun::respondents()`, `late()`, and a failed answer among the good ones | `Survey.asked`, `Survey.late`, `Survey.failed`, `Survey.replies`, `Survey.silent()` | present — `values.rs:217-250`. `silent()` is `asked` minus what answered either way, so **nobody answering is a number and never an exception**; `test_patterns.py::test_a_survey_collects_what_answers_and_counts_the_silence` asserts `asked == 2`, one reply, `failed == 0` and `silent() == 1` against a respondent that accepts the question and never answers |
| the deadline is the **caller's** and nothing on the wire carries it | same, and mandatory: a non-finite or negative float is `weida.RuntimeFailure` | present — `patterns.rs:35-39`. The 0.5 s deadline is load-bearing in the test: with it ignored, the case fails on its own 15 s bound rather than passing |
| `Listener::respondent(path)`, `Respondent::accept()` | `binding.respondent(path)`, `await respondent.accept(max_bytes)` → **`weida.Request`** | present, and the **same class a replier hands out** — `runtime.rs:280-292`, `patterns.rs:238-246`: a survey question is an exchange, byte for byte, so `reply`, `refuse` and the answered-once rule are the ones from §4.1 (`endpoints.rs:341-366` builds the object for both) |
| `Listener::bus(path, tls)` — bound and dialling at once | `binding.bus(path, trust)` → `BusMember`, the one registration that takes a trust | present — `runtime.rs:294-311`, and the reason is in the docstring: a member accepts here and joins others with `connect` |
| `BusMember::send(body)` → how many it reached, **never the sender** | `await member.send(payload)` → `int` | present — `patterns.rs:298-315`, asserted by `test_patterns.py::test_a_bus_message_reaches_every_other_member_and_never_the_sender`: `send` returns 1 for one other member, and a receive on the sender after everything is delivered times out rather than handing back its own message |
| `BusMember::recv()`, `peer_count()`, `dropped()`, `path()` | all four | present — `patterns.rs:263-331`; `dropped` is the fan-out's counted loss, and the test asserts it is `0` for a bus both members read |
| `BusMember::send_with(meta, body)` as a separate call | — | absent as a name: the binding sets `content_len` and there is no `weida.TransferMeta` to pass, exactly as for `Pusher.send` |

### 4.4a RADIO/DISH

Present on both surfaces ([0034](../decisions/0034-late-is-lost.md) §4.6):
`crates/py/weida-py/src/patterns.rs` for asyncio, `sync.rs` for `weida.sync`. A runtime that
carries datagram segments is built with `weida.Runtime(datagrams=True)`, which sets
`Limits::datagram_receive_bytes` to its documented 64 KiB; both ends need it.

| `crates/weida` | This binding | Verdict |
| --- | --- | --- |
| `Listener::radio(path)` | `binding.radio(path)` → `Radio` | present |
| `Radio::segment(topic, SegmentTerms)` → `Segment`, `write`, `write_layer`, `finish_layer`, `finish` | `radio.segment(topic, max_age=None, priority=0, follows_upstream=False)` → `Segment`; `segment.write(chunk)` and `segment.write_layer(layer, chunk)` → `int`; `segment.finish_layer(layer)`; `segment.finish()` → `int` | present, and **none of them is a coroutine**: a radio never waits for a dish, so the calls are plain on both surfaces. `SegmentTerms` is keywords, not a class |
| `Radio::datagram(topic, bytes)` | `radio.datagram(topic, payload)` → `int` | present, plain |
| `Radio::dish_count()`, `dropped()`, `dish_drops()` | all three; `dish_drops()` → list of `weida.DishDrops` (`peer` as `IncomingMeta.peer` formats it, and the seven counters) | present; `dropped_on` and `drops` stay Rust-only, as `publisher.dropped_on` does in `sync` |
| `Radio::with_admission`, `Radio::evict`, `Join` | — | absent: admission and eviction decide on the dish's proved key ([0035](../decisions/0035-keys-proved-not-judged.md) §4.3), and a Python binding accepts anonymous clients only (`bind_quic(addr, identity)`, `runtime.rs:98`, `sync.rs:133`), so every dish it serves has `peer: None` and cannot be evicted by name ([0035](../decisions/0035-keys-proved-not-judged.md) §4.7) |
| `Binding::disconnect(fingerprint)` | — | absent for the same reason: a Python binding never learns a client's fingerprint, so there is nobody to name |
| `Runtime::dish(tls)`, `Dish::connect`, `join(filter, JoinTerms)`, `leave`, `recv`, `peer_count` | the same names; `join(filter, max_age=None, max_layer=None)`, `max_age` in seconds as a `float` or `None` | present; a `max_layer` above 15 raises `weida.LimitExceeded` from Rust's check |
| `Received::Segment(IncomingTransfer)` / `Received::Datagram { topic, segment, payload }` | `("segment", payload, meta)` / `("datagram", topic, segment, payload)` | present, a tagged tuple, and the segment read whole under the caller's ceiling as every receive here is; `meta.segment` carries the number and `meta.layer` the layer, each layer arriving as its own tuple. Asserted by `test_patterns.py::test_a_radio_segment_and_datagram_reach_a_dish`, `test_patterns.py::test_a_layered_segment_reaches_a_dish_layer_by_layer` and their twins in `test_sync.py` |
| `Peer::open_flow`, `Flow`, `IncomingFlow` | — | absent, for §9.2's reason: the flow is L0, and this binding binds no `Peer` or `Acceptor` |

### 4.5 The streamed transfer classes

| `crates/weida` | This binding | Verdict |
| --- | --- | --- |
| `OutgoingTransfer::write_all(buf)` | `await stream.write(chunk)` | present — `streams.rs:84-96` |
| `OutgoingTransfer::finish()` → `Delivery`, then `Delivery::delivered()` | `await stream.finish()` — both steps | present — `streams.rs:105-120`; the receipt is the transport's, not the application's |
| `OutgoingTransfer::cancel()` | `await stream.cancel()` | present — `streams.rs:124-135` |
| dropping an unfinished transfer resets it | same, inherited | present — `streams.rs:26-28` |
| `IncomingTransfer` as an `AsyncRead` | `await stream.read(max_bytes)` → `bytes`, `b""` at the end | present — `streams.rs:175-201`, over `tokio::io::AsyncReadExt::read` precisely so a streaming caller does not go through `read_capped`; a loop over it is `test_patterns.py::test_a_streamed_transfer_is_written_and_read_in_pieces`, which asserts more than one piece |
| `IncomingTransfer::collect(max_bytes)` | `await stream.collect(max_bytes)` | present — `streams.rs:205-215` |
| `IncomingTransfer::read_capped(max_bytes)` (`crates/weida/src/transfer.rs:390`), non-consuming beside the consuming `collect` (`transfer.rs:417`) | — | absent as a separate name: on a Python `IncomingStream`, `read` is the non-consuming call (`streams.rs:175-201`) and `collect` the consuming one (`streams.rs:205-215`), so `read_capped`'s "whole payload, handle still usable" shape has no third spelling |
| `ReplyStream::recv()` | `await reply.recv()` → `(IncomingStream, IncomingMeta)` | present — `streams.rs:256-266`. A lost connection on this half is `weida.Indeterminate` and never `ConnectionLost`, because after the request's FIN the outcome is genuinely unknown ([FAILURE_MODEL.md](../FAILURE_MODEL.md)) |
| the request and reply halves are independent streams | two objects, `OutgoingStream` and `Reply` | present, and deliberately two: a caller may read the reply while still writing the request, which is what keeps flow control live in both directions (`streams.rs:11-17`) |

### 4.6 The cursor surface

How a **one-way** transfer gets a verdict: a Push has no reply to carry one, so `Accepted`,
`Stored` and `Processed` arrive on a stream of their own, after the payload's FIN
([0023](../decisions/0023-completion-is-a-cursor.md)). Present on both surfaces;
`crates/py/weida-py/src/cursors.rs` is the asyncio half.

| `crates/weida` | This binding | Verdict |
| --- | --- | --- |
| `TransferMeta::with_report(levels)`, `with_report_mode(mode)` | `report=` and `mode=` on `pusher.send`, `pusher.open` and `paired.send` | present as keyword arguments — `cursors.rs:107-131`, `endpoints.rs:185-233`, `patterns.rs:96-116`. There is no `weida.TransferMeta`, so the order rides the call that makes the transfer; the levels reach the DATA header, which is what makes a report the **sender's** request rather than a convention |
| `CursorLevel::Known(Acknowledgement)` and `CursorLevel::Application(u64)` | an **integer**: `weida.TRANSPORT_RECEIPT`, `ACCEPTED`, `STORED`, `REPLICATED`, `PROCESSED`, or any value at or above `weida.APPLICATION_FLOOR` | present as module constants rather than a class — `lib.rs:128-141`, `cursors.rs:47-58`. The level space is **open** (0023 §4.4), and a class would close what the protocol leaves open. `test_cursors.py::test_an_application_level_is_carried_and_never_interpreted` orders `APPLICATION_FLOOR + 3` and reads back an offset the library cannot check against the payload |
| an undefined value **below** the floor is a protocol violation | `weida.Protocol`, naming the floor | refused at the boundary — `cursors.rs:60-76`, asserted by `test_cursors.py::test_a_value_the_protocol_reserves_is_refused` for the level `7` and for the mode `9`, with `APPLICATION_FLOOR` itself accepted in the same case so the line is a line |
| `ReportMode::Progress`, `FinalOnly` | `weida.PROGRESS`, `weida.FINAL_ONLY` | present as the same two integers on both surfaces (`cursors.rs:60`, `sync.rs`), and `Progress` is the default because it is the library's |
| `OutgoingTransfer::cursors()` → `Option<Cursors>`, once | `await pusher.send(...)` → `weida.Cursors` or `None`; `await stream.cursors()` on the streamed form | present — `endpoints.rs:198-211`, `streams.rs:122-140`. `None` means nothing was ordered, which is every ordinary send; the second `cursors()` on one stream is `None`, asserted by `test_cursors.py::test_a_streamed_transfer_orders_a_report_too` |
| `Cursors::snapshot()`, `offset(level)`, `changed()` | `cursors.snapshot()` → `{level: offset}`, `cursors.offset(level)`, `await cursors.changed()` | present — `cursors.rs:141-205`. A set is a **dict** rather than a class: a `CursorSet` is exactly the latest absolute offset per level, and a class would add a vocabulary without adding a fact |
| `Cursors::changed()` → `None` at the end | the same `None`, and **no `async for`** | present, with the reason `weida.Survey` gives: the only channel out of a bridged future is the exception family, so `StopAsyncIteration` would need a second one. The loop is `while (set := await cursors.changed()) is not None:` |
| `Cursors::changed_within(deadline)` → `Reported` | `sync.Cursors.changed(seconds)` → dict, `None`, or **`TimeoutError`** | present on the synchronous surface **only**, and that asymmetry is the point (`sync.rs`): `asyncio.wait_for` bounds the asyncio call and composes, while a thread parked in `changed` is bounded by its argument alone — a peer that never reports opens no stream and a connection both sides keep alive never closes. The deadline is mandatory there for the reason [0009](../decisions/0009-drain.md) §4.4 gives for `drain`, and a deadline that passes raises rather than returning `None`, because it is **not** the end of the report |
| `IncomingTransfer::reporter()` → `Option<Reporter>` | `await puller.recv_reporting(max_bytes)` → `(payload, meta, reporter)`, `await paired.recv_reporting(...)`, `await stream.reporter()` | present — `endpoints.rs:433-464`, `patterns.rs:135-157`, `streams.rs:237-254`. `recv` stays a two-tuple and forgets the transfer: a receiver that reports is a receiver with stages, and it says so at the call it makes |
| `Reporter::levels()`, `mode()`, `report(level, offset)`, `finish()` | all four — `reporter.levels`, `reporter.mode`, `await reporter.report(...)`, `await reporter.finish()` | present — `cursors.rs:207-293`. A level the sender did not order is **ignored rather than refused**, which `test_cursors.py` asserts by reporting `STORED` on a report that ordered only `ACCEPTED` and `PROCESSED` and then finding it absent from the sender's snapshot |
| `Reporter::with_granularity(bytes, interval)` | — | absent: the granularity is the reporter's own number and never negotiated (0023 §4.5), and this binding takes the library's default. A Python reporter that wants coarser records reports less often |
| a cursor on a **fan-out** or an **exchange** | — | absent, and not an omission: a published copy is one transfer per subscriber with no single handle to read, and an exchange's **reply is** its verdict. The senders that can order are the one-way ones — `Pusher` and `Paired` — which is what `crates/weida` itself allows (`BusMember::send_with` opens one transfer per member and keeps none) |

## 5. The value classes

| `crates/weida` | This binding | Verdict |
| --- | --- | --- |
| `Trust::by_address()`, `Trust::pin(fingerprint)`, `Trust::anchor_file(path)` | `Trust.by_address()`, `Trust.pin(text)`, `Trust.anchor_file(path)` | present as three classmethods with **no default** — `values.rs:29-71`. `pin` parses the `sha256:` + 64 hex text and raises `weida.InvalidFingerprint` otherwise (`values.rs:47-48`) |
| `Trust::anchor(pem_bytes)`, `and_pin`, `and_anchor`, `and_anchor_file`, `is_empty` | — | absent: a `Trust` holds *lists* in Rust (`config.rs:290-295`), and Python can build only the single-entry forms. Two pins, or a pin plus an anchor, has no spelling here |
| `Identity::generate()`, `generate_for(names)`, `from_pem_file(path)`, `fingerprint()` | all four | present — `values.rs:84-136`. `fingerprint()` is a method and not a field because a `from_pem_file` identity is read lazily, so a corrupt file fails there with the path in hand |
| `Identity::from_pem(chain, key)`, `from_pem_files(chain, key)`, `certificate_pem()`, `to_pem()` | — | absent: PEM **in memory** cannot enter (a key from a secret store has no Python route in, `config.rs:221-232`) and a generated identity cannot be persisted from Python (`config.rs:270`). §10 |
| `Pem`, `ClientTls`, `ServerTls` as types | — | absent: `Trust` and `Identity` are converted into them by the calls that take them (`runtime.rs:92`, `112-136`), so the two combining types never appear in Python |
| `IncomingMeta`'s 16 fields | 13 attributes: `endpoint`, `content_len`, `content_type`, `topic`, `peer`, `sequence`, `missed`, `report`, `report_mode`, `report_id`, `traceparent`, `segment`, `layer` | present — `values.rs:147-216`, and the shape is flattened: `gap` becomes `missed` as a number, `trace` becomes the W3C `traceparent` string, and `report` becomes a list of wire values (§4.6). `tracestate`, `achieved` and `peer_chain` are absent |
| `IncomingMeta::tracestate` | — | absent: forwarded unmodified by the library and not surfaced here. A caller that propagates a full trace context gets the `traceparent` and loses the vendor state |
| `IncomingMeta::peer_chain`, `FlowInfo::peer_chain`, `PeerChain` | — | absent: the chain's use is admitting and deriving rights from a client's key ([0035](../decisions/0035-keys-proved-not-judged.md) §4.2), and a Python binding accepts anonymous clients only, so it never receives one; on the dialling side the server's key is already named by the address ([0035](../decisions/0035-keys-proved-not-judged.md) §4.7) |
| `IncomingMeta::{report, report_mode, report_id}` | `meta.report`, `meta.report_mode`, `meta.report_id` | present — `values.rs:168-179`, `200-207`. The metadata of a report a caller could not act on would be worse than nothing; a receiver acts on it through the cursor surface (§4.6) |
| `PeerIdentity` as a type | `meta.peer` as `str` or `None` | present as text — `values.rs:157-161`: `sha256:…` over QUIC, `uid=…` over a local socket, `None` for an anonymous or in-process peer. Never from a header, so it can be authorized on rather than claimed ([0015](../decisions/0015-peer-authorization.md)); `test_asyncio.py:47` asserts the `None` for a client that presented no identity |
| a survey's outcome: `SurveyRun`'s answers, `respondents()` and `late()` | `weida.Survey` — `replies`, `asked`, `failed`, `late`, `silent()` | present as a shared value class, on both surfaces (§4.4) — `values.rs:206-250` |
| `TransferMeta::with_content_len` | set by the binding on `request`, `send`, `reply` | present implicitly — `endpoints.rs:112`, `188`, `323` |
| `TransferMeta::with_content_type`, `with_trace` (`transfer.rs:44`, `56`) | — | absent: an outbound transfer cannot declare a content type or carry a trace from Python, although both are readable on arrival (§5 above) |
| `weida::VERSION`, `weida::ALPN` | `weida.VERSION`, `weida.ALPN` | present as module constants — `lib.rs:102-103`, asserted at `test_asyncio.py:186-187` |
| `Limits`, `GuaranteeSet`, `codes`, `ErrorCode`, `Address`/`EndpointAddr`, `Fingerprint`, `LossCause`, `StopReason`, `Gap`, `TraceContext` as types (`lib.rs:67-72`) | — | absent: none of them is a Python class. Where their values matter they arrive as `str`, `int` or an exception class |

## 6. The `sync` surface, and exactly how it differs

`weida.sync` is a **real submodule** placed in `sys.modules`, so `import weida.sync` works and
a traceback names `weida.sync.Runtime` (`sync.rs:916-956`, asserted by
`test_sync.py::test_the_submodule_is_importable_and_shares_its_values`). It implements no
protocol behaviour: it is `weida::blocking` — where the owned reactor and the refusal of a call
from inside a Tokio runtime live (`crates/weida/src/blocking.rs:32-47`) — with argument
conversion around it (`sync.rs:17-34`), **except for the calls that wait on a peer** (`accept`,
`recv`, `recv_reporting`, `request`, and the `reply` and report calls). The facade's `block_on`
cannot be interrupted, so those are driven in this crate, on the asynchronous endpoint the facade
hands out, by `weida_py_core::interruptible`: Ctrl-C ends them on the main thread
(`test_sync.py::test_ctrl_c_ends_a_wait_with_no_peer_and_no_deadline`) and an interrupted call
consumes nothing (`test_an_interrupted_accept_leaves_the_next_request_for_the_next_accept`).
The GIL is released on every waiting
call through `Python::detach` (`sync.rs:36-42`, and every `py.detach(...)` in that file), which
is what makes one thread per endpoint the shape it is supposed to be
(`test_sync.py::test_a_request_is_answered_with_no_event_loop` runs the serving half on a
`threading.Thread`).

Eighteen classes: `Runtime`, `Binding`, `Requester`, `Pusher`, `Subscriber`, `Replier`,
`Request`, `Puller`, `Publisher`, `Paired`, `Surveyor`, `Respondent`, `BusMember`, `Radio`,
`Segment`, `Dish`, `Cursors`, `Reporter` — every pattern of §4 and the cursor surface of §4.6,
each present on both surfaces. The value classes are
**shared, not copied** —
`weida.Trust`, `weida.Identity`, `weida.IncomingMeta`, `weida.DishDrops` and the five statistics classes (`ConnectionStats`, `TransportStats`, `PathStats`, `UdpCounts`, `RemoteStats`) are the same objects on both surfaces
(`sync.rs:50-52`, `sync.rs:62`) — and so is the whole exception family
(`test_sync.py::test_the_failure_classes_are_the_same_on_both_surfaces`).

What differs, row by row:

| Asyncio surface | `weida.sync` | Verdict |
| --- | --- | --- |
| every waiting call is a coroutine | the same call, blocking | present — `sync.rs:267-715` |
| `Runtime.drain(seconds)` leaves the runtime usable | `drain` **consumes** the facade's runtime | present as a difference, and it is the facade's: `blocking::Runtime::drain` takes `self` (`crates/weida/src/blocking.rs:157`), so the second call raises `weida.RuntimeFailure` — `sync.rs:186-200`, asserted by `test_sync.py::test_a_runtime_is_shut_down_once` |
| no `shutdown` | `runtime.shutdown()` | present here and absent there (§3) — `sync.rs:203-209` |
| `requester.open()`, `pusher.open()`, `publisher.open(topic)`, `puller.recv_stream()`, `subscriber.recv_stream()` | — | absent from `sync`, with the reason at `sync.rs:44-52`: the blocking facade takes whole payloads by design, a synchronous caller that wants to stream wants the asyncio surface or the Rust API, and a `sync` module that invented its own streaming would be inventing a second facade. §9.3 |
| `OutgoingStream`, `IncomingStream`, `Reply`, `FanOut` | — | absent as classes, following from the row above: none of the eighteen classes `sync.rs` registers is a stream |
| `publisher.filter_count()` | — | absent: the synchronous publisher has `path`, `publish`, `subscriber_count` and `dropped` (`sync.rs:504-529`), so `test_sync.py:66-72` waits on `subscriber_count` where the asyncio tests wait on `filter_count` |
| `publisher.dropped_on(topic)` | — | absent: the per-cause breakdown is asyncio-only, and so it is in the facade (`crates/weida/src/blocking.rs:569-606`) |
| `subscriber.peer_count()` (`pubsub.rs:336-338`) | — | absent: `sync.rs:352-386` gives the synchronous subscriber `connect`, `subscribe`, `unsubscribe` and `recv`, and the facade has no such accessor either (`crates/weida/src/blocking.rs:383-426`) |
| `request.payload` / `request.meta` after answering | `weida.RuntimeFailure`, naming the spent request | present as a refusal — `sync.rs:65-72`, `422-440`: the blocking `Request` holds the payload inside the request it consumes, so reading it after `reply` is refused rather than stale |
| `publish` is not a coroutine | `publish` does not block either | present, and it is the one call in the facade that waits for nothing (`crates/weida/src/blocking.rs:578-581`, `sync.rs:512-515`) |
| — | `blocking::Requester::endpoint()` and the other `endpoint()` accessors, which hand out the async type | absent from Python: the escape hatch the facade offers a Rust caller (`crates/weida/src/blocking.rs:459-462`) has no Python spelling, because the asyncio surface *is* the escape hatch here |
| `await surveyor.survey(...)` → `weida.Survey` | `surveyor.survey(...)` → the **same** `weida.Survey` | present, and the same value class rather than a synchronous twin of it (`sync.rs:594-628`): the collected shape is the right one on a surface with no event loop for the reason §4.4 gives, and it happens to be the right one on both |
| `paired.peer_count()`, `member.dropped()`, `respondent.accept()` | all three | present — `sync.rs:532-715`; the synchronous `Respondent.accept` hands back `weida.sync.Request`, the class `Replier.accept` hands back, exactly as on the asyncio surface |
| `await cursors.changed()`, unbounded | `cursors.changed(seconds)`, **mandatory** deadline, `TimeoutError` when it passes | present as the one deliberate signature difference in this module (§4.6) — `sync.rs`. `asyncio.wait_for` bounds the asyncio call and composes; the synchronous call has no loop to compose with, so the facade takes the deadline that the asyncio surface gets from its loop (`crates/weida/src/blocking.rs:336-353`, `crates/weida/src/cursor.rs:280-296`) |
| `await stream.reporter()`, `await stream.cursors()` | — | absent as stream calls, because `sync` has no streams (§9.3). The handles arrive from `pusher.send(report=…)` and `puller.recv_reporting(...)` instead, which is the whole-payload shape of the same thing |

## 7. The exception family

**25 classes: 24 named failures plus the base**, counted as §1 states. Every failure of the
library is a class under `weida.WeidaError`, each instance carrying `errno` and `cause`
(`errors.rs:1-23`), and the base is what lets a caller catch the family
(`test_asyncio.py:184-185`).

| Rust | Python | Verdict |
| --- | --- | --- |
| `Error::Runtime` | `weida.RuntimeFailure` | present, and **the one rename**: `weida.Runtime` is the runtime, a module cannot have one name for two things, and the module with the collision silently keeps whichever was added last. So the class is `RuntimeFailure`, its `errno` says the same, and the variant it comes from is written beside it (`errors.rs:45-53`, `72-73`). Both `smoke.py` and `test_sync.py` assert that `weida.Runtime` is not a `BaseException` |
| the other 23 variants of `weida_core::Error` | the same names: `InvalidAddress`, `InvalidEndpointPath`, `InvalidFingerprint`, `AlreadyRegistered`, `NotConnected`, `ConnectionLost`, `Negotiation`, `Protocol`, `Rejected`, `UnknownEndpoint`, `Unsupported`, `NoParkedConnection`, `NoReply`, `Canceled`, `Expired`, `Indeterminate`, `LimitExceeded`, `DatagramsUnavailable`, `TooLarge`, `Tls`, `Untrusted`, `Io`, `Transport` | present — `errors.rs:74-97`; the three of 0034 (`Expired`, `DatagramsUnavailable`, `TooLarge`) are included, because the exhaustive match below does not compile without them |
| a variant added to the library | a **compile error in this file** | present: `name_of` is an exhaustive `match` written by the same macro that writes the name list (`errors.rs:54-70`), so a new variant cannot silently arrive as the base class |
| `Error::Display` | `cause` | present, and no second vocabulary: the library's own `Display` is the wording, because a binding that rephrased it would be a second one to keep in step (`errors.rs:101-109`) |
| `Error::is_definite_failure()` keeping `Indeterminate` out of the definite set | `weida.Indeterminate` as a **sibling** of `ConnectionLost`, not a kind of it | present — `errors.rs:18-23`, and it is the one class worth reading twice: the transfer may or may not have arrived, and a caller that treats it as a definite failure is wrong ([FAILURE_MODEL.md](../FAILURE_MODEL.md)) |
| distinguishable failures at a call site | `Rejected` vs `UnknownEndpoint` vs `LimitExceeded` vs `Untrusted` as separate branches | present — `test_asyncio.py::test_an_unknown_path_and_a_refusal_are_distinct_classes`, `::test_a_payload_above_the_ceiling_is_refused_rather_than_held`, `::test_the_wrong_key_is_untrusted_and_says_which_one_answered`, which also asserts the fingerprint that answered is inside `cause` |

## 8. The byte boundary and the explicit receive ceiling

| Rule | Where | Verdict |
| --- | --- | --- |
| a payload is `bytes` or `bytearray`; a `str` is a `TypeError` | `payload_of` (`crates/py/weida-py-core/src/bytes.rs:64-72`) | refused by type: guessing an encoding for somebody else's wire format is how mojibake gets sent (`bytes.rs:49-55`) |
| one copy in, one copy out, and no second one | `payload_of` copies straight into the `Vec` the library keeps; `py_bytes` uses `PyBytes::new` (`bytes.rs:62-80`) | present, with the reason zero-copy out is not available: the buffer protocol is not in the limited API before CPython 3.11 and the wheel is `abi3` from 3.9, so a zero-copy path would exist only on some interpreters (`bytes.rs:29-37`) |
| **every receive takes a ceiling in bytes** | `Replier.accept(max_bytes)`, `Requester.request(payload, max_reply_bytes)`, `Puller.recv(max_bytes)`, `Subscriber.recv(max_bytes)`, `IncomingStream.read(max_bytes)`, `IncomingStream.collect(max_bytes)`, and the same four on `weida.sync` | present on all of them — `endpoints.rs:247`, `101`, `391`, `pubsub.rs:309`, `streams.rs:175`, `205`, `sync.rs:259`, `327`, `358`, `437` |
| there is no default ceiling | none anywhere | present, and this is the deliberate one: weida's payloads are streams and [INVARIANTS.md](../INVARIANTS.md) forbids the core from materializing them, while **a Python object is materialized** — so the caller who wants the bytes in memory is the one who has to say how many of them there may be. A binding that chose a default would be choosing how much memory a stranger may make a Python process allocate (`lib.rs:38-46`, `endpoints.rs:6-8`, `tests/test_asyncio.py:19-22`) |
| a payload above the ceiling | `weida.LimitExceeded` | refused — `test_asyncio.py::test_a_payload_above_the_ceiling_is_refused_rather_than_held`, where the reply is 4096 bytes and the caller allowed 16 |
| the ceiling on a *stream* bounds one piece, not the payload | `IncomingStream.read(max)` | present — `streams.rs:22-24`, `166-169`: a loop over `read` holds one piece at a time and the payload has no size limit at all |

## 9. Deliberate deviations from the Rust API

### 9.1 No per-call timeouts, anywhere

No call in this module takes a timeout argument, and the one deadline that exists is
`Runtime.drain(seconds)` (§9.4) and `FanOut.write_within(chunk, seconds)`, which is a
subscriber's room rather than a call's patience. The reason is that `asyncio.wait_for` already
is a timeout and **a cancelled weida coroutine resets its streams**, so the peer learns rather
than waits (`endpoints.rs:88-91`, `README.md:87-92`). A per-call timeout would be a second
cancellation mechanism with the same effect and a worse composition: `asyncio.wait_for`,
`asyncio.timeout` and a `TaskGroup` all compose, a keyword argument does not. The synchronous
surface has no timeout argument either; there Ctrl-C ends a blocked call on the main thread, and
a thread that is not the main one waits for its peer.

### 9.2 No raw L0 surface: `Peer` and `Acceptor` are not bound

`crates/weida/src/stream.rs` is the stream core — the socket replacement, one
`Peer` for dialling and one `Acceptor` for whatever arrives (`stream.rs:117-126`, `275-285`) —
and `Runtime::peer(tls)` (`runtime.rs:272`) and `Listener::acceptor(path)`
(`listener.rs:350`) are the two doors into it. Neither is bound. What a Python caller does not
get is the ability to take a stream without a pattern deciding what it is: an `Acceptor` hands
over `Incoming::Transfer` and `Incoming::Exchange` and lets the application choose
(`stream.rs:266-285`), where a `Replier` accepts exchanges only and a `Puller` one-way
transfers only. The binding's own module documentation names this as the absence
(`lib.rs:55-62`), and §10 says where a follow-up would add it.

### 9.3 No streamed form in `weida.sync`

The five streaming entry points of the asyncio surface have no synchronous counterpart (§6),
because `weida::blocking` has none to wrap: it takes whole payloads by design and hands out
the asynchronous endpoint for anything else (`crates/weida/src/blocking.rs:27-30`). A `sync`
module that built streaming itself would be the second implementation
[0013](../decisions/0013-competitor-libraries.md) §4.4 exists to prevent, and the order the
other five bindings used — library facade first, binding `sync` second — is what avoids it
(`sync.rs:30-34`).

### 9.4 `drain` takes seconds as a float, and the deadline is mandatory

`weida::Runtime::drain` takes a `Duration`; the Python call takes a `float` and converts with
`Duration::try_from_secs_f64`, so a negative or non-finite value is `weida.RuntimeFailure`
naming the argument (`runtime.rs:145-151`, `sync.rs:164-169`). Seconds as a float is the
Python convention — `asyncio.wait_for`, `threading.Event.wait` and `socket.settimeout` all
take one — and there is no `None`: the deadline is mandatory and finite for the reason
[0009](../decisions/0009-drain.md) §4.4 gives, that waiting on a peer without one is how a
process hangs at shutdown. `FanOut.write_within(chunk, seconds)` converts identically
(`pubsub.rs:196-201`).

### 9.5 One reply, one finish, and a spent handle says so

`Request`, `FanOut`, `OutgoingStream`, `IncomingStream` and `Reply` are all `frozen` classes
two coroutines may hold, so "answered at most once" and "a transfer ends once" are enforced by
an `Option` behind a mutex and a named failure, not by a panic: `weida.NoReply` with the
sentence that says why (`endpoints.rs:351-369`, `pubsub.rs:154-160`, `streams.rs:42-48`).
`FanOut` and the stream classes use a `tokio::sync::Mutex` because the guard is held across an
await, which `std`'s may not be (the `tokio` dependency comment in `Cargo.toml`, `pubsub.rs:146-149`).

### 9.6 Each object holds its runtime alive

Every endpoint, stream and request holds an `Arc<Runtime>` (`endpoints.rs:37-38`,
`streams.rs:57`, `pubsub.rs:44-45`), so a program that keeps a `Replier` and drops the
`Runtime` object is a program whose requests keep arriving: the reactor dies with the last
handle, not with the first (`endpoints.rs:10-12`). The Rust API needs no such rule, because a
Rust caller holds the `Runtime` for as long as its endpoints borrow from it.

## 10. What is absent, with a reason and where a follow-up adds it

| Absent | What a caller does not get | Where it would be added |
| --- | --- | --- |
| the raw L0 surface, `Peer` and `Acceptor` (§9.2) | taking a stream without a pattern interpreting it; `Incoming::Transfer` vs `Incoming::Exchange` as the application's choice | a slice binding `Runtime.peer(trust)` and `Binding.acceptor(path)` with an `Incoming` the two arms of which are distinguishable in Python |
| a **streamed** form for PAIR (§4.4) | writing or reading a pair's payload in pieces; `Paired::open(meta)` is the Rust call | the same shape Push/Pull has here — one `paired.open()` returning an `OutgoingStream` — if a caller ever needs it |
| `Reporter::with_granularity(bytes, interval)` (§4.6) | choosing how often a reporter's records reach the wire; the library's default stands | one method on `weida.Reporter` taking bytes and seconds, if a Python reporter ever needs coarser records. Nothing is lost by the default: a cursor is absolute, so coarser records end at the same numbers |
| a Python **server** on a socket transport | `bind_unix`, `bind_pipe` (`listener.rs`); the client side already dials both (`endpoints.rs`), and `bind_inproc` is present (§3) | two calls on `Runtime` beside `bind`, plus the `LocalPrincipal`/`WindowsPrincipal` reading that makes `uid=…` useful |
| eight of `RuntimeConfig`'s nine fields, and all of `Limits` (§3) | every resource ceiling weida bounds — queue depths, connection counts, the subscriber budget, the guarantee set — is the library's default and cannot be moved from Python | a `weida.Limits` and keyword arguments on `Runtime`, in the shape [`mqtt-py.md`](mqtt-py.md) §5 uses for the two limits that surface there |
| streaming in `weida.sync` (§9.3) | a synchronous caller cannot write or read a payload in pieces | `weida::blocking` gaining a streamed form first; building it in the binding is what 0013 §4.4 forbids |
| `await runtime.shutdown()` on the asyncio surface (§3) | a close that does not wait for anything in flight; `drain` is the only end | one coroutine wrapping `weida::Runtime::shutdown`, beside `drain` |
| `await binding.close()` (§3) | an awaited unbind that waits for the endpoint to go idle; dropping the object is the only way to stop accepting | one coroutine wrapping `weida::Binding::close` (`crates/weida/src/listener.rs:378`) |
| `weida.ErrorCode` (§4.1) | refusing a request with any code but `Rejected` | an `ErrorCode` enum or a string argument on `refuse`, with the protocol's codes named |
| `Identity.from_pem`, `from_pem_files`, `certificate_pem`, `to_pem` (§5) | a key from a secret store entering without touching the filesystem, and a generated identity being persisted so an address survives a restart | four methods on `Identity`; `to_pem` returns a private key, so it needs the same "write it owner-only" wording the Rust doc has |
| multi-entry `Trust` (§5) | two pins, or a pin plus an anchor, on one dialling endpoint | `and_pin`/`and_anchor_file` as chainable methods, or list arguments on the three constructors |
| `IncomingRequest::canceled()` (§4.1) | seeing the requester walk away *before* answering, rather than at `reply` | an awaitable on `Request`, which is what the Rust method is |
| `TransferMeta::with_content_type`, `with_trace`; `publish_with_trace`, `open_with_trace` (§4.3, §5) | declaring a content type on an outbound payload, and propagating an inbound trace outward — the read side of both already works | keyword arguments on `request`, `send`, `reply` and `publish` |
| `IncomingMeta.tracestate` (§5) | the vendor half of a W3C trace context | one more field in `PyIncomingMeta::of` |
| `Publisher.drops()`, `Subscriber.filter_count()`, `Requester/Pusher.peer_count()` (§4.3, §4.1) | three read-only accessors the Rust API has | one method each |
| type stubs | a `.pyi` or a `py.typed` marker: there is none anywhere under `crates/py/`, so a type checker sees an untyped extension module, and `pyproject.toml` carries no `Typing :: Typed` classifier, because a classifier is a claim | a stub file per module, generated or written, and the classifier with it |
| **a client for PyPy** | `import weida` on PyPy, GraalPy, or a free-threaded CPython below 3.15 | **nothing, and the answer is "no" rather than "later"** — decided on the number that already exists. The cost of this binding is **~78 µs per operation dominated by one event-loop wakeup per `await`** ([IMPLEMENTATION.md](../IMPLEMENTATION.md) §4.15), not by interpreting bytecode, so the part PyPy replaces is not the part that costs. Against that stands a second artifact per interpreter (§2: PyPy and GraalPy pin the stable ABI to their own version, and a free-threaded build below 3.15 gets no stable ABI at all), plus a second Python surface to keep in step with this one — which is what [0013](../decisions/0013-competitor-libraries.md) §4.4 exists to prevent. If it is ever wanted, the route is a `cffi` shim over the **C ABI** of [0030](../decisions/0030-which-runtimes-and-in-which-order.md) §3.4, which makes it a consumer of that row rather than a binding of its own: a few hundred lines of Python over a stable header, with the surface list taken from this document. Free-threaded CPython is the case worth revisiting first, and it revisits itself: when `abi3t` (3.15) is the floor, one wheel serves both builds again |
| an interop section (§2) | nothing measurable is missing: weida has one implementation, so there is no second peer to pair against | a second implementation of the protocol, which is not a binding slice |

## 11. Sources

- This binding: `crates/py/weida-py/src/` (`lib.rs`, `runtime.rs`, `endpoints.rs`,
  `pubsub.rs`, `patterns.rs`, `cursors.rs`, `streams.rs`, `sync.rs`, `values.rs`,
  `errors.rs`),
  `crates/py/weida-py/Cargo.toml`, `pyproject.toml`, `smoke.py`, `README.md`, and the wrappers
  `develop.sh` and `package.sh`, which delegate to `crates/py/develop.sh` and `crates/py/package.sh`.
- The shared foundation: `crates/py/weida-py-core/src/` — `bridge.rs` (the coroutine and its
  lazy start), `bytes.rs` (the payload boundary), the exception-family machinery `errors.rs`
  builds on.
- The API this document is the parity table of: `crates/weida/src/` — `lib.rs`'s re-exports,
  `config.rs`, `runtime.rs`, `listener.rs`, `endpoint.rs`, `transfer.rs`, `stream.rs`,
  `cursor.rs`, `blocking.rs`.
- Tests: `crates/py/weida-py/tests/` — `test_asyncio.py` (8), `test_patterns.py` (9),
  `test_sync.py` (8), `test_cursors.py` (7). A run of them passed 32, and `smoke.py`
  resolved all 56 names through a freshly built wheel.
- Normative documents the rows cite: [PROTOCOL.md](../PROTOCOL.md) §6.4 and §9.4,
  [PATTERNS.md](../PATTERNS.md) §4.1, [GUARANTEES.md](../GUARANTEES.md) §1,
  [INVARIANTS.md](../INVARIANTS.md), [FAILURE_MODEL.md](../FAILURE_MODEL.md).
- Decisions: [0014](../decisions/0014-parallel-libraries.md) §2 (one shared PyO3 foundation,
  asyncio first, the bytes boundary), [0013](../decisions/0013-competitor-libraries.md) §4.4
  (nothing decided twice) and §4.7 (the verdicts),
  [0009](../decisions/0009-drain.md) §4.4 (the mandatory finite deadline),
  [0010](../decisions/0010-local-transport.md) §4.8 (the local address forms),
  [0015](../decisions/0015-peer-authorization.md) (the proved peer).
