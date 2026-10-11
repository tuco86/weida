# Proposal from griasdi: restart handoff and close codes

Written by the griasdi side against weida `e8a6403` (0.1.0-alpha.4), the rev
griasdi pins.

**Answered by [decisions/0038](../decisions/0038-restart-handoff.md) (accepted; shipped in
B-316 to B-318).** (a) is `Listener::bind_quic_socket(socket, tls)`, which shares one serve path
with `bind_quic`; the socket is taken as it is, and a successor should serve it only once its
predecessor's `restart` has returned. (b) is `Binding::restart`, a close with the new protocol
code `10` `RESTARTING`, and `Binding::close` is now bounded by `RuntimeConfig::shutdown_timeout`
like `restart`; there is no `close_with(code, reason, drain)`. (c) is
`LossCause::PeerRestarting`, which `conn_error` maps from `RESTARTING` and which reaches
operations as well as `PeerEvent::Lost`; `ReconnectPolicy` redials it with no first delay,
whatever `stop_on_peer_closed` says. Not taken: application close codes, so the dialler learns a
restart and no other reason.

## Why griasdi asks

griasdi ships every push to `main`: the server installs the new build and
restarts, and every desktop follows. griasdi now restarts its server in place
(`exec` of the new binary after a bounded `Binding::close`); on sadala that
took the gap from 2.4-5.3 s down to 0.27 s. What is left is one shutdown plus
one start, and every client redialling.

The next step is a handoff to a parallel process: the new server B starts next
to the old one A, A hands B its bound UDP socket over a unix socket
(`SCM_RIGHTS`) together with a snapshot of its in-memory voice and share
state, closes its QUIC connections with a code that means "restart, redial
now", and exits. QUIC connections cannot move between processes, so every
client still redials once, but it can do so at once and into a server that is
already listening. The database is on disk and shared, so nothing else has to
move. `SO_REUSEPORT` was rejected for this: the kernel spreads the existing
4-tuples across both processes, so packets of A's connections would reach B,
which cannot decrypt them.

Three pieces of that are transport work. griasdi will not work around them.

## (a) Serve on a UDP socket the process received

- **Missing.** `Listener::bind_quic(addr, tls)` (`listener.rs:333`) creates
  and binds the socket itself. A socket handed over by `SCM_RIGHTS` cannot be
  used.
- **API.** `Listener::bind_quic_socket(socket: std::net::UdpSocket, tls: impl
  Into<ServerTls>) -> Result<Binding, Error>`, identical to `bind_quic` from
  the point where the socket exists. The socket arrives already bound; weida
  sets it non-blocking and registers it with the reactor as it does today.
- **Why in weida.** The socket goes straight into the quinn endpoint, which
  weida owns; griasdi never sees it.
- **griasdi code that would use it.** `griasdi_server::start`
  (`crates/griasdi-server/src/lib.rs`) would take the socket from the
  takeover channel instead of binding `config.listen`.

## (b) Close every connection with a caller-chosen code, bounded

- **Missing.** `Binding::close` (`listener.rs:644`) closes with
  `shutdown_code()` and `b"binding closed"` and then awaits
  `endpoint.wait_idle()` without a bound. On sadala that wait took 3.08 s
  once; griasdi now wraps it in `tokio::time::timeout(1 s, ..)`.
- **API.** `Binding::close_with(code: u32, reason: &str, drain: Duration)`:
  close every connection with the application code and reason, then wait at
  most `drain` for the endpoint to go idle. `close()` stays as
  `close_with(SHUTDOWN, "binding closed", <no bound or a default>)`. Which
  code range belongs to the application and which to weida (`codes::*`) is
  weida's call.
- **Why in weida.** The CONNECTION_CLOSE frame and its code are connection
  state; only the endpoint can send them for all connections at once.
- **griasdi code that would use it.** `RunningServer::shutdown` (today
  `tokio::time::timeout(Duration::from_secs(1), binding.close())`) and the
  handoff in A.

## (c) The dialler sees that code

- **Missing.** `conn_error` (`conn.rs:294`) maps every application close code
  other than `NEGOTIATION_FAILED`, `PROTOCOL_VIOLATION` and `LIMIT_EXCEEDED`
  to `LossCause::PeerClosed`, which carries no code. A client cannot tell
  "restart, redial now" from "shutting down, back off". `ReconnectPolicy`'s
  `stop_on_peer_closed` documents the same ambiguity ("a server that
  restarts closes deliberately on its way down").
- **API.** Either `LossCause::PeerClosed { code: u32 }` (the type is `Copy`
  and stays so), or a separate `PeerEvent::Lost { url, cause, code:
  Option<u32> }`. Either way the code also reaches what a `Requester` call
  reports, since griasdi's session loop learns of the loss from a failed
  read of its event stream, not from `PeerEvents`.
- **Why in weida.** weida reads the close frame; the code is gone by the time
  griasdi sees `Error::ConnectionLost`.
- **griasdi code that would use it.** `Client::server_loop` in
  `crates/griasdi-client/src/sync.rs` would redial at once on the restart
  code and keep its backoff otherwise. Today it guesses: a session that held
  for 10 s is redialled within 0-500 ms (`redial_delay`), anything shorter
  waits the doubling backoff, so a server that drops every client right after
  accepting is not hammered.

## What griasdi measured, for context

The close frame of a shutting-down server reaches the client within 2 ms and
a fresh dial takes milliseconds, so weida was not part of the slow reconnect
griasdi had (that was a griasdi task holding the old binding alive). Nothing
here is a bug report; it is the API the handoff needs.

griasdi's side of the design is in
`griasdi/docs/proposals/seamless-updates.md`, section "Phase 2".
