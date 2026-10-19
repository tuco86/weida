# 0042 — A simulated network under real QUIC

- **Status:** accepted
- **Date:** 2026-10-19
- **Items:** B-348, B-349, B-350
- **Related:** [0010](0010-local-transport.md) §4.4, [0031](0031-transparent-redial-and-the-sender-outbox.md)

## 1. The question

zeughaus tiers its tests by I/O: the unit tier may not touch the file system, the network,
processes or real time. Every zeughaus test that stands up a weida endpoint landed in the
integration tier, because the only endpoint weida offers a test is a real QUIC listener on
loopback. The code under test is request handling that decides by the peer's key
(`IncomingMeta.peer = PeerIdentity::Key(fp)`, fingerprint-pinned `EndpointAddr`s, mTLS refusing
an anonymous client), and its reconnect tests (`PeerEvent::{Connected, Retrying, Lost, GaveUp}`,
`GiveUp::PeerChanged`) wait out real backoff.

`weida+inproc://` does not cover it: it is a transport, not a test double. It carries no peer
identity and no TLS, a pinned address is refused on it, and it offers no control over time or
faults.

zeughaus asked for test doubles behind a non-default feature `test-util`:

- in-process endpoints that carry a real `PeerIdentity` and fingerprint, so `IncomingMeta.peer`
  and pinned `EndpointAddr`s behave as over QUIC;
- refusal the way mTLS refuses (anonymous client, wrong fingerprint);
- close and rebind with the same or another identity, driving the `PeerEvent`s;
- no real timers inside, so tests run under `#[tokio::test(start_paused = true)]`;
- fault injection: delay, drop, disconnect.

The API shape is weida's. So the question is: **what lets a test run weida's endpoints, their
identities and their failure paths with no socket and on a paused clock.**

## 2. The evidence

- QUIC sockets are created in exactly two places: `Listener::bind_quic` (`listener.rs`,
  `std::net::UdpSocket::bind`) and the client endpoint of `ClientPool::connect` (`pool.rs`,
  `quinn::Endpoint::client`).
- `serve_quic` already builds the server endpoint with `quinn::Endpoint::new_with_abstract_socket`
  over `HandoverSocket` (`handoff.rs`), a decorator of `quinn::AsyncUdpSocket`. The socket is
  already a seam; only its constructor is fixed.
- `quinn::TokioRuntime::now()` returns `tokio::time::Instant::now().into_std()` and its timers
  use `sleep_until` (quinn 0.11.12 `src/runtime/tokio.rs`), so QUIC's own timers (idle timeout,
  loss detection, keep-alive) follow a paused clock.
- A v6 quinn endpoint stores v4 remotes as v4-mapped v6 (`ensure_ipv6`, quinn `endpoint.rs`).
- `Exec::sleep` and `Exec::within` use `tokio::time` (`weida-runtime` `exec.rs`), so weida's
  waits follow a paused clock too.
- weida read `std::time::Instant::now()` in about 40 places, several of which compare against a
  later `now` after a Tokio sleep. Under a paused clock such a comparison never becomes true:
  `swarm/gather.rs` waits for `next_refresh` that way, and `stream.rs` `watch_slot` decides with
  `up_since.elapsed()` whether a connection stayed up long enough to restart the redial
  schedule.

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — inproc links with an identity | `Link::Local` carrying a chosen `PeerIdentity` | production code must bind and dial other addresses than it does in production; TLS refusal and a redial against a new key go untested; no packet-level fault model |
| **B — a simulated UDP network under real QUIC and TLS** | **`RuntimeConfig::network` picks the OS's UDP or a `SimNet`; QUIC, TLS, identities and redials run unchanged** | **chosen** |
| C — a separate crate | `weida-test` owning the doubles | it cannot reach the socket seam without making that seam public API |
| D — mock endpoints behind traits | `Requester`, `Listener` and friends as traits with fakes | every pattern gets a second implementation that drifts from the first |

## 4. Decision

**Option B.** A runtime's QUIC sockets come from `RuntimeConfig::network`. The default is the
operating system's UDP. With the cargo feature `test-util`, `Network::sim(&net)` puts a runtime
on a `SimNet`: an in-memory UDP network that every runtime of one test shares. QUIC and TLS run
over it unchanged, so identities, pinned addresses, mTLS refusals, redials and their `PeerEvent`s
are the real ones, and the test's fixture code stays what it is over loopback
(`Runtime::new`, `bind_quic`, `connect("weida://fp@127.0.0.1:port/...")`): only the config
differs.

### 4.1 The runtime's clock (B-348)

`Exec::now()` reads the runtime's clock: `tokio::time::Instant::now().into_std()` under the
runtime's handle, which is the paused time under `tokio::time::pause` and
`std::time::Instant::now()` otherwise. Every clock read in `weida` that gates a wait (a redial
schedule, a deadline, a refresh, a report's age, a flow's or a dedup window's expiry) goes
through it, so a deadline measured against it ends when the timers say it does.

Reads that stay on `std::time::Instant`, each with its reason:

- identity file polling (`identity.rs`): the file system's clock;
- the listener's log throttle: a log rate;
- `OutgoingTransfer::expire_at` and the SWARM seeder's `expire_at` deadlines: the public deadline
  is on the caller's `std` clock; it is converted to a duration at once, so it stays correct
  under a paused clock;
- `weida-runtime` `CloseBudget`: under a paused clock it can at most double a bounded close
  wait.

### 4.2 `Network` and `SimNet` (B-349)

```rust
pub struct Network { .. }                      // Default: the OS's UDP
impl Network {
    #[cfg(feature = "test-util")]
    pub fn sim(net: &SimNet) -> Network;
}
pub struct RuntimeConfig { .., pub network: Network, .. }

#[cfg(feature = "test-util")]
pub struct SimNet { .. }                       // Clone to share
impl SimNet {
    pub fn new() -> SimNet;
    pub fn set_delay(&self, addr: SocketAddr, one_way: Duration);
    pub fn drop_next(&self, addr: SocketAddr, datagrams: u64);
    pub fn partition(&self, addr: SocketAddr);
    pub fn heal(&self, addr: SocketAddr);
    pub fn stats(&self) -> SimStats;           // delivered, dropped
}
```

- **Addressing.** A port identifies one socket in a `SimNet`, whatever its IP. Port 0 takes the
  next free port from 49152 upward. A port in use is `Error::Io(AddrInUse)` until its binding
  closes (`Binding::close`, or the runtime's `shutdown`/`drain`). A socket bound to an
  unspecified IP is seen from `127.0.0.1` or `::1`; a v4 source reaches a v6 socket v4-mapped,
  as quinn stores it.
- **Faults**, keyed by port and applied to every datagram from or to it: `set_delay` holds
  datagrams for a one-way delay (the larger of source and destination); `drop_next` loses the
  next n datagrams, which QUIC retransmits; `partition` loses everything until `heal`, an
  unannounced disconnect that QUIC notices at `Limits::idle_timeout`
  (`LossCause::IdleTimeout`). An announced disconnect is `Binding::close` or
  `Binding::disconnect`.
- **Bounds.** A socket's receive buffer holds `INBOX_DATAGRAMS` (4096) datagrams; a datagram
  arriving at a full buffer is dropped, as UDP drops past its receive buffer.
- **Time.** A simulated socket never sleeps on its own: delayed datagrams are released by a
  Tokio timer, so a delay costs paused time, not wall time.
- `Listener::bind_quic_socket` takes an OS socket, so a binding made there is never simulated.
  `Binding::hand_over` needs an OS socket and panics on a simulated binding.

### 4.3 The normative documents (B-350)

ARCHITECTURE §7 (the API above), INVARIANTS (the receive buffer bound), STATUS §1, and a
proposal to zeughaus.

## 5. Backlog

### B-348 — weida reads its clocks through Exec::now
kind: code | size: 60 | status: ready | needs: []
acceptance: [0042](0042-simulated-network.md) §4.1: `Exec::now` with the paused-clock test `now_reads_the_runtime_clock`; every clock read in `crates/weida/src` that gates a wait goes through it; `git grep -n 'Instant::now()\|\.elapsed()' -- crates/weida/src` lists only the exempt sites (`identity.rs`, the listener's log throttle, `transfer.rs` `expire_at`, `swarm/seed.rs`) and lines inside `#[cfg(test)]` modules; the full gate passes.

### B-349 — test-util: SimNet, a simulated network under real QUIC
kind: code | size: 90 | status: ready | needs: [B-348]
acceptance: [0042](0042-simulated-network.md) §4.2: `RuntimeConfig::network` and `Network`; feature `test-util` with `SimNet`/`SimStats`; three unit tests in `sim.rs` (addressing and the v4-mapped source, partition and drop count, a port taken until released); eight integration tests in `tests/sim.rs` (pinned client identity, mTLS and pin refusal, close and rebind events, partition and heal, delay, drop, the redial schedule restarting under a paused clock, an address free again after close and shutdown); the LOOP §6 `test-util` row; the full gate and `cargo doc -p weida --no-default-features` pass.

### B-350 — 0042 into the normative documents, zeughaus proposal
kind: spec | size: 45 | status: ready | needs: [B-349]
acceptance: [0042](0042-simulated-network.md) §4.3: ARCHITECTURE §7 `RuntimeConfig::network`, `Network`, `SimNet`, `SimStats`; the INVARIANTS row for `INBOX_DATAGRAMS`; STATUS §1 "Tests need no network"; `git grep -n 'bind_client_endpoint' -- crates docs` empty; `PROPOSAL-weida-test-doubles.md` in zeughaus's checkout.

## 6. What this note does not decide

- **`hand_over` on a simulated binding.** A handover passes an OS socket to a successor process;
  a simulated network lives in one process. Unsupported, and it panics.
- **A name table inside the simulation.** Names go through `RuntimeConfig::resolver`; fixtures
  dial IP literals or bring a resolver.

## 7. Sources

quinn 0.11.12 `src/runtime/tokio.rs` (`TokioRuntime::now`, timers), `src/endpoint.rs`
(`ensure_ipv6`, `Endpoint::new_with_abstract_socket`). weida: `listener.rs` (`bind_quic`,
`serve_quic`, `Binding`), `pool.rs` (`ClientPool::connect`), `handoff.rs` (`HandoverSocket`),
`stream.rs` (`watch_slot`), `swarm/gather.rs`, `weida-runtime` `exec.rs`. zeughaus's request is
its proposal `PROPOSAL-zeughaus-test-doubles.md`, consumed by this note.
