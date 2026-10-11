# 0039 — A handover says when

- **Status:** accepted
- **Date:** 2026-10-11
- **Items:** B-320, B-321
- **Amends:** [0038](0038-restart-handoff.md) §4.6 (`restart` is replaced)

## 1. The question

[0038](0038-restart-handoff.md) gave weida `Binding::restart`: it closes every connection with
`RESTARTING`, diallers see `LossCause::PeerRestarting` and redial at once, and the successor
serves the same UDP socket through `Listener::bind_quic_socket`. The flaw is the timing. The
clients learn of the restart **before** the successor serves; the predecessor's closed endpoint
keeps reading the socket for its closing period, and quinn refuses every new connection on a
closed endpoint ([0038](0038-restart-handoff.md) §2.6). So the immediate redial is refused, and
the client falls back to its backoff schedule.

The owner wants the clean pattern: the server tells its clients "I have restarted, connect now"
at the moment the successor already serves, and each client reconnects exactly once, with no
backoff. The constraints: no socket virtualization beyond a thin decorator, no client change, and
timeouts and normal redials stay untouched and heal anything that goes wrong.

So the question is: **how does a close reach the clients only once the successor serves.**

## 2. The evidence, condensed

Verified at `61e872e`, against quinn 0.11.12, quinn-proto 0.11.19 (the locked version) and
quinn-udp 0.5.16:

**2.1 An endpoint can sit on any socket.** `Endpoint::new_with_abstract_socket(EndpointConfig,
Option<ServerConfig>, Arc<dyn AsyncUdpSocket>, Arc<dyn Runtime>)`
(`quinn-0.11.12/src/endpoint.rs:136-141`).

**2.2 The socket trait is small.** `quinn::AsyncUdpSocket` (`quinn-0.11.12/src/runtime.rs:42-89`)
is `create_io_poller(self: Arc<Self>)`, `try_send(&self, &Transmit)`, `poll_recv(&self, cx, bufs,
meta)`, `local_addr`, and the defaulted `max_transmit_segments`, `max_receive_segments` and
`may_fragment`. `quinn::udp::Transmit` is `{ destination, ecn, contents, segment_size, src_ip }`
(`quinn-udp-0.5.16/src/lib.rs:134-147`). quinn re-exports `AsyncUdpSocket`, `UdpPoller` and
`udp` (`quinn-0.11.12/src/lib.rs:75,90`).

**2.3 A closed connection has passed its close to the socket.** `Endpoint::close` reaches each
connection as a `ConnectionEvent::Close` (`quinn-0.11.12/src/endpoint.rs:299-308`).
`ConnectionDriver::poll` takes the connection's state lock, applies the event, which sets the
close reason, and then, still under that lock, `drive_transmit` calls `poll_transmit` and
`socket.try_send` (`quinn-0.11.12/src/connection.rs:259-267, 1016-1060, 1107-1108, 1228-1229`).
`Connection::closed()` reads the reason under the same lock (`:376-395`). So once `closed()`
returns for a connection that was open before `Endpoint::close`, the datagram carrying its close
has been passed to `try_send`, unless the socket was not writable and quinn buffered it. The
counter that would say so directly, `stats().frame_tx.connection_close`, is never incremented on
send by the locked `quinn-proto` 0.11.19: `FrameStats::record` is called for received frames only
(`quinn-proto-0.11.19/src/connection/mod.rs:2460, 2706, 2764`).

**2.4 An endpoint cannot be told to stop reading.** `Endpoint::rebind` keeps reading the old
socket until a connection packet arrives on the new one (`quinn-0.11.12/src/endpoint.rs:475-518`),
and a closed endpoint reads on and refuses (§1). Only the socket under the endpoint can stop the
reading.

**2.5 A lost close heals on its own.** `Limits::idle_timeout` defaults to 30 s and `keep_alive`
to 10 s (`crates/core/src/limits.rs:173-174`). A client that never hears the close loses its
connection to the idle timeout and redials on its normal schedule.

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — 0038's `restart` | close, then the successor serves | the immediate redial is refused, backoff follows. Replaced |
| B — A stops reading right after its closes are sent | the redial waits in the kernel receive buffer until B reads | the buffer overflows with many clients, and B's readiness is not awaited. Refused |
| **C — hold the closes, the successor replays them** | A's close packets are captured, B sends them once it serves | one datagram per connection, no retransmit; a lost one heals through `idle_timeout` (§2.5). Chosen |
| D — mvfst-style forwarding | B forwards packets of A's connection IDs to A | zero reconnects, but needs a connection ID generator, demultiplexing and a forwarding channel between the processes. Refused |

## 4. Decision

**Option C: the old process A holds its socket, closes every connection with `RESTARTING` into a
`Handoff`, and the new process B replays the held datagrams once it serves.** The held datagrams
are ordinary QUIC packets of each client's still-live connection, encrypted under that
connection's keys; they leave from the same IP and port, so they pass the client's NAT mapping.
The client reads `RESTARTING`, gets `PeerRestarting`, and redials at once into B, which already
reads. No client change.

### 4.1 What is decided

1. `HandoverSocket`, a decorator under every QUIC binding, and `Handoff` with its byte format
   (§4.2).
2. Every QUIC binding sits on a `HandoverSocket` and tracks its open connections (§4.3).
3. `Binding::hand_over` replaces `Binding::restart` (§4.4).
4. `Binding::take_over` replays the held datagrams (§4.5).
5. The rustdoc and tests that named `restart` (§4.6).

### 4.2 `HandoverSocket` and `Handoff` (B-320)

A new module `crates/weida/src/handoff.rs`; `Handoff` is exported from the crate root.

`HandoverSocket` wraps the runtime's `Arc<dyn quinn::AsyncUdpSocket>` and has three modes:
`LIVE`, `HOLDING`, `DISCARDING`. `hold` sets `HOLDING`; `release` sets `DISCARDING` and takes
the held datagrams. Under `LIVE` it delegates everything. Under `HOLDING`, `try_send` keeps each
datagram (split by `segment_size` when GSO batched several) with its `destination` and `src_ip`;
under `DISCARDING` it drops them. Both return `Ok(())`, never `WouldBlock`. `poll_recv` in any
mode but `LIVE` returns `Pending` without touching the inner socket: a held socket is never read
again, and the kernel keeps its datagrams for the successor. `local_addr`,
`max_transmit_segments`, `max_receive_segments` and `may_fragment` delegate, because the trait
defaults would turn off GSO and GRO and change the MTU behaviour. `send_raw` sends one held
datagram on the inner socket, waiting for writability on `WouldBlock`.

`Handoff { socket, local_addr, held }` carries a duplicate of the bound socket and the held
datagrams. `held()` counts them, `local_addr()` names the address, and `into_parts()` returns the
socket, which goes to the successor as a descriptor (`SCM_RIGHTS`, or inherited across `exec`),
and the encoded bytes, which go alongside it in any channel. The bytes are QUIC packets, in the
clear only as far as their headers go, and need no secrecy.

The byte format, all integers big-endian:

```
magic   b"WDHO"
version u8 = 1
addr    local_addr
u32     count
count × { addr destination; u8 src tag (0 none | 4 + 4 bytes | 6 + 16 bytes); u16 len; len bytes }
addr := u8 tag (4 → 4-byte IPv4 | 6 → 16-byte IPv6) + u16 port
```

`decode` refuses, with `Error::Io` of kind `InvalidData`: a wrong magic ("handoff: not a weida
handoff"), a version other than 1 ("handoff: unknown version {v}"), and an unknown address or
source tag, truncation, a zero length or trailing bytes ("handoff: malformed"). It does not
pre-allocate from `count`.

### 4.3 Every QUIC binding sits on a `HandoverSocket` (B-320)

`serve_quic` duplicates the socket (`try_clone`) for the handoff, wraps the original through
`quinn::Runtime::wrap_udp_socket` in a `HandoverSocket`, and builds the endpoint with
`Endpoint::new_with_abstract_socket`. `Binding` keeps the decorator, the duplicate and an
`OpenConns`: every handshaken connection of the binding, anonymous ones included, keyed by
`stable_id()` and bounded by `max_connections`. `accept_connections` inserts a connection as its
handshake completes and removes it when its task ends, on the per-peer-limit path and the served
path alike. `PeerConns` cannot serve here: it holds proved peers only.

### 4.4 `Binding::hand_over` (B-320)

`pub async fn hand_over(self) -> Handoff`:

1. The decorator holds: from here on this process neither reads nor sends on the socket.
2. The open connections not yet closed are snapshotted.
3. The endpoint closes with `RESTARTING` and the reason `server restarting`.
4. It awaits `closed()` of every snapshotted connection (§2.3), for at most
   `RuntimeConfig::shutdown_timeout`. A timeout is logged at `debug` and is not an error.
5. The decorator releases: the held datagrams are taken, and anything sent later is dropped.
6. It returns the `Handoff`.

The successor may serve the socket as soon as `hand_over` returns, and this process may keep its
runtime as long as it likes; 0038 §4.6's ordering rule and its "drop the runtime at once" are
gone. A connection still in its handshake is not waited for. If the successor never calls
`take_over`, or a datagram is lost, a client notices through `Limits::idle_timeout` and redials on
its normal schedule.

`restart` and the private `close_with` are removed: `restart` was never published, and a restart
without a serving successor cannot say "now". `close` keeps its bounded idle wait with `SHUTDOWN`.

### 4.5 `Binding::take_over` (B-320)

`pub async fn take_over(&self, held: &[u8]) -> Result<usize, Error>` decodes the bytes, refuses a
handoff from another address with `Error::Io` of kind `InvalidInput`, and sends every held
datagram once through the decorator's `send_raw`. A datagram that fails to send is logged at
`debug` and skipped. It returns how many were sent. The rustdoc says: call it after the
Listener's endpoints the clients use are registered, because a client redials within one round
trip and a path not yet registered refuses it.

### 4.6 Rustdoc and tests that named `restart` (B-320)

`RuntimeConfig::shutdown_timeout` caps `Binding::close` and `Binding::hand_over`.
`ReconnectPolicy::stop_on_peer_closed` names `hand_over` as what sends `PeerRestarting`.
`bind_quic_socket`'s rustdoc names `hand_over` as the predecessor's step and `take_over` as the
successor's, once its endpoints are registered.

`crates/weida/tests/reconnect.rs` replaces 0038's two tests:

- `a_handed_over_binding_tells_its_clients_once_the_successor_serves`: a pusher on an old runtime
  is `Connected` and delivers; `hand_over` returns in less than half its 10 s bound and holds at
  least one datagram; the client hears nothing for 300 ms; a new runtime registers the puller,
  serves the socket at the same address and `take_over` sends at least one datagram; the raw
  event stream is `Lost { PeerRestarting }`, `Retrying { attempt: 1, delay: ZERO }`, and the very
  next event `Connected`; the next send is pulled by the new puller while the old runtime is still
  alive.
- `a_send_after_a_handover_names_the_restart`: under `ReconnectPolicy::never()`, after
  `hand_over` and the successor's `take_over`, `Lost { PeerRestarting }`,
  `GaveUp { Policy { attempts: 0 } }`, and the next send fails with
  `ConnectionLost(PeerRestarting)`.
- `a_handoff_is_refused_where_it_does_not_belong`: a handoff of a binding without connections
  holds nothing, another binding refuses it with `InvalidInput`, and garbage with `InvalidData`.

`handoff.rs` unit tests: a round trip of an IPv4 datagram without source address, an IPv6 one
with, and of an empty list; `decode` refuses a wrong magic, version 2, every truncated prefix of
a valid encoding, one trailing byte and a zero-length datagram.

## 5. Consequences and follow-ups

A handover costs every client one reconnect, at the moment the successor serves, and no backoff.
A lost replay costs that client the idle timeout. Documents (B-321):
[ARCHITECTURE.md](../ARCHITECTURE.md)'s API sketch names `hand_over` and `take_over` and its
`Binding` bullet says the predecessor holds its closes and the successor replays them;
[PATTERNS.md](../PATTERNS.md) §1's loss paragraph says `RESTARTING` is sent by the successor
through `take_over`; [IMPLEMENTATION.md](../IMPLEMENTATION.md)'s `shutdown_timeout` row names
`hand_over` and 0039; the requirement points at what shipped. Backlog:

### B-320 — Hand a QUIC binding over: hold the closes, replay them in the successor
kind: code | size: 90 | status: ready | needs: []
acceptance: [0039](0039-handover-says-when.md) §4.2-§4.6: `crates/weida/src/handoff.rs` with `HandoverSocket` (`LIVE`/`HOLDING`/`DISCARDING`, held datagrams split by `segment_size`, `poll_recv` pending once held, the four capability methods delegated, `send_raw` waiting for writability), the exported `Handoff` (`held`, `local_addr`, `into_parts`) and the `WDHO` version-1 byte format with its `InvalidData` refusals; `serve_quic` on `Endpoint::new_with_abstract_socket` over a `HandoverSocket`, `Binding` holding the decorator, a `try_clone` of the socket and an `OpenConns` that `accept_connections` fills and empties; `Binding::hand_over(self) -> Handoff` bounded by `shutdown_timeout`, `Binding::take_over(&self, &[u8]) -> Result<usize, Error>` refusing another address with `InvalidInput`; `restart` and `close_with` removed, `close` bounded as before; the rustdoc of `shutdown_timeout`, `stop_on_peer_closed` and `bind_quic_socket` names `hand_over` and `take_over`. Tests: `a_handed_over_binding_tells_its_clients_once_the_successor_serves` (`Lost { PeerRestarting }`, `Retrying { attempt: 1, delay: ZERO }`, then at once `Connected`, passing 15 runs in a row), `a_send_after_a_handover_names_the_restart`, `a_handoff_is_refused_where_it_does_not_belong` in `crates/weida/tests/reconnect.rs`, and the `handoff.rs` round-trip and refusal unit tests.

### B-321 — Documents for 0039
kind: spec | size: 15 | status: ready | needs: [B-320]
acceptance: [0039](0039-handover-says-when.md) §5's edits: [ARCHITECTURE.md](../ARCHITECTURE.md)'s API sketch names `Binding::hand_over` and `Binding::take_over` instead of `Binding::restart`, and its `Binding` bullet says the predecessor holds its closes and the successor replays them; [PATTERNS.md](../PATTERNS.md) §1's loss paragraph says `RESTARTING` is sent by the successor through `Binding::take_over`; [IMPLEMENTATION.md](../IMPLEMENTATION.md)'s `shutdown_timeout` row names `Binding::hand_over` and cites 0039; [requirements/griasdi-restart-handoff.md](../requirements/griasdi-restart-handoff.md)'s (b) is `Binding::hand_over` plus `Binding::take_over` per 0039, shipped in B-320. None of these four documents names `Binding::restart` any more.

## 6. What this note does not decide

- **The `weida::blocking` and Python surfaces.** `hand_over` and `take_over` are not mirrored, as
  0038 §6 left `restart` unmirrored; filed when a caller needs it.
- **Stateless-reset key sharing between A and B.** B still gets the socket, not A's reset key.
- **Local bindings.** `LocalBinding`, `UnixBinding` and `PipeBinding` have no handover; nothing
  local sends `RESTARTING`.

## 7. Sources

weida documents: decision [0038](0038-restart-handoff.md);
[requirements/griasdi-restart-handoff.md](../requirements/griasdi-restart-handoff.md).

Code read for this note, at `61e872e`: `crates/weida/src/listener.rs:344-406, 670-751,
1102-1198`; `crates/weida/src/config.rs:113-115`; `crates/weida/src/reconnect.rs:44-47`;
`crates/core/src/limits.rs:173-174`; `crates/weida/tests/reconnect.rs:849-1001`. `quinn` 0.11.12
`src/endpoint.rs:136-141, 299-308, 475-518`, `src/runtime.rs:24, 42-89`, `src/connection.rs:259-267,
376-395, 1016-1060, 1107-1108, 1228-1229`, `src/lib.rs:75,90`; `quinn-proto` 0.11.19
`src/connection/mod.rs:491-507, 794-852`; `quinn-udp` 0.5.16 `src/lib.rs:134-147`.
