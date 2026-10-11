# 0038 — A restart is said, and its socket is handed over

- **Status:** accepted
- **Date:** 2026-10-11
- **Items:** B-316 to B-319
- **Answers:** [requirements/griasdi-restart-handoff.md](../requirements/griasdi-restart-handoff.md)
  (a), (b), (c)
- **Related:** [0009](0009-drain.md) §4.4 (bounded wait),
  [0031](0031-transparent-redial-and-the-sender-outbox.md) (redial),
  [0035](0035-keys-proved-not-judged.md) §4.4 (`disconnect` closes with `REJECTED`)
- **Amended by:** [0039](0039-handover-says-when.md) (`Binding::restart` replaced by
  `Binding::hand_over` and `Binding::take_over`)

## 1. The question

griasdi ships every push to `main`: its server installs the new build and restarts, and every
desktop follows. It wants to restart by handing over to a parallel process. The old process A
passes its bound UDP socket to the new process B over `SCM_RIGHTS`, closes every QUIC connection
with a signal that means "restart, redial now", and exits. QUIC connections cannot move between
processes, so every client still redials once; the gain is that it can do so at once and into a
server that is already listening [griasdi-restart-handoff, Why griasdi asks]. That needs three
things from weida:

| Need | Shape |
| --- | --- |
| (a) A QUIC binding on a socket the process received | `bind_quic` creates the socket itself |
| (b) A close of every connection with that signal, with a bounded idle wait | `Binding::close` waits without a bound |
| (c) The dialler tells that signal from a plain shutdown | every unmapped application code reads as the same deliberate close |

So the question is: **who owns the meaning of "restart", how does it reach the dialler and its
redial schedule, and what is the smallest change that lets a process take over another's
address.**

## 2. The evidence, condensed

Verified at `e8a6403`:

**2.1 `bind_quic` binds the socket itself.** `Listener::bind_quic` hands an address to
`quinn::Endpoint::server`, which creates and binds the socket
(`crates/weida/src/listener.rs:333-365`). A socket received by `SCM_RIGHTS` cannot be passed in.

**2.2 `Binding::close` has no bound.** It closes the endpoint with `SHUTDOWN` and then awaits
`endpoint.wait_idle()` with no timeout (`listener.rs:644-647`). griasdi measured 3.08 s once and
now wraps the call itself. [0009](0009-drain.md) §4.4 already calls an unbounded `wait_idle` "a
defect, not a drain"; `Binding::close` is a path it did not name.

**2.3 Every unmapped code is `PeerClosed`.** `conn_error` maps `NEGOTIATION_FAILED`,
`PROTOCOL_VIOLATION` and `LIMIT_EXCEEDED` and sends every other application close code to
`LossCause::PeerClosed`, which carries no code (`crates/weida/src/conn.rs:294-307`). A client
cannot tell a restart from a shutdown.

**2.4 The ambiguity is documented.** `stop_on_peer_closed` is off by default "because a server
that restarts closes deliberately on its way down" (`crates/weida/src/reconnect.rs:42-46`): a
policy that gives a slot up on a deliberate close would also give it up on a restart.

**2.5 No code is free except `10`, and no application range exists.** Codes `0..=9` are used
(`crates/protocol/src/codes.rs`, [PROTOCOL.md](../PROTOCOL.md) §7). The table has no range
reserved for the application.

**2.6 A closed quinn endpoint outlives its close.** Once an endpoint is closed its driver keeps
reading the socket and answers every new connection with a refusal
(`quinn-0.11.12/src/endpoint.rs:810-817`). It does so until the endpoint and its runtime are
gone.

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — application close codes | an application `u32` code with its own wire range | weida's own redial cannot act on a meaning it does not know. Refused |
| **B — weida `RESTARTING`** | this note | no channel for other application close reasons. Chosen |
| C — both | A and B together | two signals for one event. Refused |

For the handoff itself:

| Option | Shape | Named loss |
| --- | --- | --- |
| D — `SO_REUSEPORT` on both processes | A and B bind the same address | the kernel spreads A's existing 4-tuples across both processes, so packets of A's live connections reach B, which cannot decrypt them (griasdi's reason). Refused |
| **E — pass the bound socket** | `bind_quic_socket` on a `std::net::UdpSocket` | for a moment two descriptors refer to one socket and each datagram reaches only one reader (§4.6's ordering rule). Chosen |

## 4. Decision

**Option B with E: weida owns the restart semantics. A protocol code `RESTARTING`, a
`Binding::restart`, a `LossCause::PeerRestarting` the dialler sees, a first redial that waits
for nothing, and `Listener::bind_quic_socket` for the successor.** No application close codes:
no `close_with(code, ..)` and no `LossCause::Application`.

### 4.1 What is decided

1. Protocol code `10` `RESTARTING` (§4.2).
2. `LossCause::PeerRestarting` and its mapping (§4.3).
3. An immediate first redial after a restart (§4.4).
4. `Listener::bind_quic_socket`, sharing one serve path with `bind_quic` (§4.5).
5. `Binding::restart` and a bounded `Binding::close` (§4.6).

### 4.2 Protocol code `RESTARTING` (B-316)

`codes::RESTARTING = 10`, named `"RESTARTING"` by `codes::name`. [PROTOCOL.md](../PROTOCOL.md) §7
gains the row `10` `RESTARTING` — "binding closed because its server is being replaced; redial at
once" — and says it is used only in `CONNECTION_CLOSE`. A peer that does not know code `10`
treats it as any other close code it does not map, a deliberate close, so **no capability
negotiates it**. `stop_reason` is unchanged: `RESTARTING` is a connection close code and never a
stop code, so it falls to `StopReason::Other(10)`.

### 4.3 `LossCause::PeerRestarting` (B-318)

`LossCause` gains `PeerRestarting`, after `PeerClosed`, displayed as "the peer is restarting".
It stays `Copy`, `PartialEq`, `Eq` and exhaustive; the only exhaustive match on it in the
workspace is its `Display`. `conn_error` maps `codes::RESTARTING` to
`Error::ConnectionLost(LossCause::PeerRestarting)`, before its catch-all.

Everything downstream passes the cause through: `stream.rs::loss_cause` forwards
`ConnectionLost(cause)`, so `PeerEvent::Lost` and every operation that fails with
`ConnectionLost` carry the new cause. That is proposal (c): the cause reaches an operation, not
only the event stream. `transfer.rs::indeterminate_on_loss` is unchanged: a request whose FIN
was sent stays `Indeterminate`, because the outcome comes first, and its cause still reaches
`PeerEvent::Lost`. The local transports' close mapping is unchanged; nothing local sends
`RESTARTING`.

### 4.4 The first redial waits for nothing (B-318)

`ReconnectPolicy::delay_after(cause, attempt)` replaces `delay(attempt)` in `watch_slot`:

| Cause | Attempt 1 | Attempt `n > 1` |
| --- | --- | --- |
| `PeerRestarting` | zero | `delay(n - 1)` |
| anything else | `delay(1)` | `delay(n)` |

A restart skips the first wait and only that one: the successor is expected to serve already,
and if it does not, the schedule continues from its own first step. `PeerEvent::Retrying`
reports `delay: Duration::ZERO` for that attempt.

`redials_after` is unchanged. It already redials `PeerRestarting` under `stop_on_peer_closed`,
because it compares with `PeerClosed` only, and `ReconnectPolicy::never()` still stops it
because `redials()` is false. The documentation of `stop_on_peer_closed` says so: a restart that
says so is redialled regardless, and the field stays off by default because a server that
restarts by closing and binding again still looks like a deliberate close.

### 4.5 `bind_quic_socket` (B-317)

`Listener::bind_quic_socket(socket: std::net::UdpSocket, tls: impl Into<ServerTls>) ->
Result<Binding, Error>` is `bind_quic` from the point where the socket exists. `bind_quic` is
split into its TLS validation, its bind and a private `serve_quic(socket, server_config)`; the
order is today's, so TLS material is validated before any socket is used. `serve_quic` builds
the endpoint with `quinn::Endpoint::new(EndpointConfig::default(), Some(server_config), socket,
runtime)`, which is what `Endpoint::server` does internally, and leaves everything after it as
it is. quinn-udp sets the socket non-blocking itself, so weida does not.

The rustdoc says: the method is for a process that received a bound socket, from its
predecessor over `SCM_RIGHTS` or through socket activation, and is otherwise exactly
`bind_quic`; the socket is taken as it is — weida neither binds it nor sets options — and
`local_addr` reports what it is bound to.

### 4.6 `restart` and a bounded `close` (B-318)

`Binding` keeps the runtime's executor and `RuntimeConfig::shutdown_timeout`. `close` closes the
endpoint with `SHUTDOWN` and waits for it to go idle for at most that timeout; `restart` does the
same with `RESTARTING`, and a private `close_with(code, reason)` is the one body. A timeout is
logged at `debug` and is not an error: [0009](0009-drain.md) §4.4 says the wait is bounded, not
that it succeeds. `shutdown_timeout`'s rustdoc says the same bound caps `Binding::close` and
`Binding::restart`.

**The handoff, in order.** This is the rule `restart`'s rustdoc carries:

1. While another process still reads the same socket, each datagram reaches only one of the two.
   A successor should therefore start serving, through `bind_quic_socket`, **only once its
   predecessor's `restart` has returned**.
2. A closed endpoint keeps reading its socket and refuses new connections until the binding and
   its runtime are gone (§2.6). The predecessor **drops the runtime, or exits, as soon as
   `restart` returns**.
3. A client whose immediate redial lands in that window and is refused retries on its normal
   schedule (§4.4): attempt 2 waits `delay(1)`.

### 4.7 Tests

`crates/weida/tests/reconnect.rs`: `a_restart_hands_the_socket_over_and_is_redialled_at_once` —
the old runtime serves a socket, `restart` closes it, the raw event stream shows
`Lost { cause: PeerRestarting }` and then `Retrying { attempt: 1, delay: ZERO }`, a new runtime
serves a clone of the same socket through `bind_quic_socket`, and under
`stop_on_peer_closed: true` the pusher is `Connected` again and delivers.
`a_send_after_a_restart_names_the_restart` — under `ReconnectPolicy::never()` the next send
fails with `ConnectionLost(PeerRestarting)`.
`reconnect::tests::a_restart_skips_the_first_wait_and_only_that` and
`runtime::tests::a_binding_close_is_bounded_by_the_shutdown_timeout`. `weida-protocol`'s two
`codes` tests are extended to `10`. The first test calls `restart`, so B-317 lands after B-318.

## 5. Consequences and follow-ups

`close` keeps reporting `PeerClosed` to diallers; only `restart` says `PeerRestarting`.
Documents (B-319): [FAILURE_MODEL.md](../FAILURE_MODEL.md)'s idle-timeout and
reset row and [PATTERNS.md](../PATTERNS.md) §1's loss paragraph list `PeerRestarting`;
[ARCHITECTURE.md](../ARCHITECTURE.md)'s API sketch names `Listener::bind_quic_socket` and
`Binding::restart`; [IMPLEMENTATION.md](../IMPLEMENTATION.md)'s loss-cause row lists the sixth
cause; the requirement points at what shipped. Backlog:

### B-316 — PROTOCOL: code 10 RESTARTING
kind: spec | size: 15 | status: ready | needs: []
acceptance: [0038](0038-restart-handoff.md) §4.2: [PROTOCOL.md](../PROTOCOL.md) §7's table has the row `10` `RESTARTING` and a paragraph saying it is used only in `CONNECTION_CLOSE` and that no capability negotiates it; `weida_protocol::codes::RESTARTING` is `10` and `codes::name` names it; `stop_reason` is unchanged. `codes_match_the_protocol_document` covers `10`, and `names_are_defined_for_known_codes` asserts `name(RESTARTING) == "RESTARTING"` and `name(11) == "UNKNOWN"`.

### B-317 — Serve QUIC on a socket the process received
kind: code | size: 30 | status: ready | needs: [B-318]
acceptance: [0038](0038-restart-handoff.md) §4.5: `Listener::bind_quic_socket(socket: std::net::UdpSocket, tls)` and `bind_quic` share one private `serve_quic`, built on `quinn::Endpoint::new`, and TLS material is still validated before any socket is used. Test in `crates/weida/tests/reconnect.rs`: `a_restart_hands_the_socket_over_and_is_redialled_at_once` — a second runtime serves `try_clone` of the first runtime's socket after the first one's `restart` returned, `local_addr()` is the same address, and a pusher under `stop_on_peer_closed: true` is `Connected` again and its next send is pulled.

### B-318 — Binding::restart, a bounded close, PeerRestarting and the immediate redial
kind: code | size: 60 | status: ready | needs: [B-316]
acceptance: [0038](0038-restart-handoff.md) §4.3, §4.4 and §4.6: `LossCause::PeerRestarting` with its `Display`, mapped from `codes::RESTARTING` in `conn_error`; `ReconnectPolicy::delay_after`, used by `watch_slot`, and the rewritten documentation of `stop_on_peer_closed`; `Binding::restart`, and `Binding::close` bounded by `RuntimeConfig::shutdown_timeout` through one `close_with`, `shutdown_code` removed, the `shutdown_timeout` documentation extended; `restart`'s rustdoc carries §4.6's handoff order. Tests: `a_send_after_a_restart_names_the_restart` in `crates/weida/tests/reconnect.rs` (`Lost { cause: PeerRestarting }`, `GaveUp { why: GiveUp::Policy { attempts: 0 } }` under `never()`, and the next send fails with `ConnectionLost(PeerRestarting)`), `a_restart_skips_the_first_wait_and_only_that` in `crates/weida/src/reconnect.rs`, and `a_binding_close_is_bounded_by_the_shutdown_timeout` in `crates/weida/src/runtime.rs` (a 1 ms cap returns in less than half the time of a 10 s cap against a client that stays connected). `a_pusher_delivers_again_after_the_server_restarts` and `the_never_policy_reports_the_loss_and_stops` stay green: `close` still reports `PeerClosed`.

### B-319 — Documents for 0038
kind: spec | size: 30 | status: ready | needs: [B-317, B-318]
acceptance: [0038](0038-restart-handoff.md) §5's edits: [FAILURE_MODEL.md](../FAILURE_MODEL.md)'s idle-timeout and reset row (`docs/FAILURE_MODEL.md:158`) lists `PeerRestarting` among the causes; [PATTERNS.md](../PATTERNS.md) §1's loss paragraph (`docs/PATTERNS.md:201-205`) lists `PeerRestarting`; [ARCHITECTURE.md](../ARCHITECTURE.md)'s API sketch names `Listener::bind_quic_socket` and `Binding::restart`; [IMPLEMENTATION.md](../IMPLEMENTATION.md)'s loss-cause row (`docs/IMPLEMENTATION.md:1673`) lists the sixth cause; [requirements/griasdi-restart-handoff.md](../requirements/griasdi-restart-handoff.md) points at what shipped.

## 6. What this note does not decide

- **The local-account-pin proposal.** `PROPOSAL-griasdi-local-account-pin.md` is a separate
  piece of work and gets its own plan.
- **Application close codes.** Refused for now (§3, option A). A later requirement for an
  application's own close reasons would need its own wire range.
- **Restart on local bindings.** `LocalBinding`, `UnixBinding` and `PipeBinding` have no close
  today and get none; nothing local sends `RESTARTING`.
- **The `weida::blocking` and Python surfaces.** `bind_quic_socket` and `restart` are not
  mirrored: griasdi uses the async surface. The gap is recorded here and filed when a caller
  needs it.
- **Stateless-reset key sharing between A and B.** `bind_quic_socket` gives B the socket, not
  A's reset key; whether the two processes should share one is a later question.

## 7. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §7; decisions 0009, 0031, 0035;
[requirements/griasdi-restart-handoff.md](../requirements/griasdi-restart-handoff.md).

Code read for this note, at `e8a6403`: `crates/weida/src/listener.rs:333-365, 629-663, 930-932`;
`crates/weida/src/conn.rs:294-307`; `crates/weida/src/reconnect.rs:31-110`;
`crates/weida/src/stream.rs:729-861`; `crates/weida/src/runtime.rs:625-666`;
`crates/weida/src/config.rs:105-113`; `crates/protocol/src/codes.rs`;
`crates/weida/tests/reconnect.rs:25-230`. `quinn` 0.11.12 `src/endpoint.rs:105-115, 810-817`;
`quinn-udp` 0.5.16 `src/unix.rs:109`.
