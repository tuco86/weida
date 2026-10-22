# Patterns

The reference for what each weida pattern does, in the shape of `zmq_socket(3)`: one table
per pattern naming its compatible peer, direction, routing strategy and behaviour when it has
nowhere to send, followed by what happens at every failure. Where ZeroMQ's tables describe
what its threads and queues do, these describe what the **transport** does, because a weida
pattern is a thin wrapper over transport streams and inherits its behaviour from them.

Four transports carry those streams ([decisions/0010](decisions/0010-local-transport.md),
[0012](decisions/0012-local-connection-grouping.md)): QUIC, an in-process channel pair,
`AF_UNIX` and, on Windows, a named pipe, where the OS connection **is** the stream. Every statement below is written for
QUIC unless it says otherwise, because QUIC is the transport whose flow control the patterns
were designed against; §1.10 says which of them mean something different locally, and the
failure tables name the transport where the two diverge.

Every statement below that could be false is defended by a test named in the text. The
measured numbers are from `crates/weida/tests/streams.rs` on loopback with `quinn 0.11`; the
statements hold on any link, the numbers do not. `crates/weida/tests/transports.rs` runs one
body per pattern — Req/Rep, Push/Pull, Pub/Sub, PAIR, SURVEY and BUS — over every one of
those transports, which is what makes the pattern semantics a fact about weida rather than
about QUIC. PAIR earns its place there twice over: its **bound** side is the only one that
opens a stream toward a peer that dialled it, so it is the pattern that proves the reverse
pool of [0012](decisions/0012-local-connection-grouping.md) §4.4 carries an ordinary send.

Related: [ARCHITECTURE.md](ARCHITECTURE.md) (layer model, primitives P1-P4),
[GUARANTEES.md](GUARANTEES.md) (the vocabulary), [FAILURE_MODEL.md](FAILURE_MODEL.md)
(outcome rules), [PROTOCOL.md](PROTOCOL.md) (the wire).

---

## 1. Common ground: what a stream is

Every user data flow is one stream: one QUIC stream over the network, one channel pair in
process, one `AF_UNIX` connection locally — "the OS connection **is** the stream" [0010 §4.2].
A Push message, a published copy, the request half of an exchange and its reply half are each
a stream of their own. What follows is true of all of them, whichever pattern opened them and
whichever transport carries them, except where §1.10 says a local transport differs.

### 1.1 `finish()` is a commitment

Once `OutgoingTransfer::finish` has queued the FIN, the payload arrives without any local
handle. The transfer is consumed, the `Delivery` may be dropped, the endpoint may go out of
scope; only the connection has to live, and the runtime's pool holds it.
`Runtime::shutdown` is the one thing that cuts a finished transfer short. Its counterpart is
`Runtime::drain(deadline)`: it stops admitting work, gives the transfers that were already
finished until the deadline to reach the peer's **transport**, and only then performs the
same close ([decisions/0009](decisions/0009-drain.md) §4.1-§4.6). What comes back is a pair
of counts, `Drained { delivered, outstanding }`, not a promise — and an expired drain with
something still outstanding is not an error. The deadline is mandatory and finite: there is
no infinite variant to set by accident [0009 §4.3].

*`a_finished_transfer_needs_no_local_handle_to_arrive`,
`a_finished_transfer_that_shutdown_cuts_short_arrives_under_drain`.*

### 1.2 The receipt, inside and beyond the window

`Delivery::delivered()` resolves when the peer's **transport** holds every byte and the FIN.
Inside the peer's stream receive window that is all it means: the receipt resolves before the
peer's application has called `recv`, and even while the transfer is still parked in the
peer's accept queue.

Beyond the window it means more, because QUIC cannot accept more bytes than the window until
the application has consumed some. For a payload of `p` bytes against a window of `w`, the
receipt cannot resolve until the reader has consumed at least `p - w` bytes, rounded up to the
next eighth of a window (quinn announces window credit in eighths). So a receipt for a large
transfer is evidence that the application is reading it; a receipt for a small one is not.

**And it stays an explanation rather than an interface.** There is deliberately no byte-cursor on
`Delivery`: QUIC tracks the acknowledged ranges and `quinn` keeps them internal, ZeroMQ hides even
the connect and offers a separate opt-in monitor socket instead, and an API here would invite an
application to rebuild the reliability QUIC already provides — the mistake
[ARCHITECTURE.md](ARCHITECTURE.md) §1 records for brokerless application ACKs. What *is*
cursor-shaped is a store's durability and a consumer's settlement, which are application state and
travel as frames ([decisions/0023](decisions/0023-completion-is-a-cursor.md)).

*`push_delivery_receipt` (1 KiB, resolves before `recv`);
`a_receipt_beyond_the_window_implies_the_reader_consumed` (160 KiB against a 64 KiB window:
the write completed once 131072 bytes were consumed, the receipt at 163840);
`the_stream_budget_is_backpressure_not_an_error` (receipt resolves for a transfer still in
the accept queue).*

### 1.3 Two windows, one shared

Flow control is per stream and per connection, and the two behave differently:

- **Per stream, transfers are isolated.** A stream nobody reads does not delay its siblings.
- **Per connection, one slow reader stalls every other writer on that connection** — and
  "that connection" is one **endpoint path**. The connection window is shared by every
  stream on the connection. Unread streams consume it; when it is spent, every writer on the
  connection blocks until the slow reader consumes at least an eighth of the window. Because
  there is one connection per dialled path
  ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2), "everyone" means
  the writers on that path and nothing else: another path is another connection and another
  window. What is *not* isolated is a path's own control traffic: a SUBSCRIBE rides the
  connection of the path it names, because that is the only route back to the subscriber
  ([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.1-§4.2), so an endpoint
  that publishes *and* subscribes on one path can queue its own subscription behind its own
  payload. A pure subscriber writes nothing there and a pure publisher sends no SUBSCRIBE,
  so neither is affected [0011 §4.4].
- **The header spends the window too.** The DATA header rides on the transfer's own stream, so
  the payload that fits in one window is `stream_receive_window - header`, and the last eighth
  only clears once the application reads. A payload sized exactly to the window therefore
  blocks until the reader starts. `stream_receive_window` is a buffer size, not a message size.

*`a_stalled_stream_does_not_block_its_siblings` (64 KiB stream window, 256 KiB connection
window, 32 KiB per stream: siblings flow past an unread stream; the seventh unread stream
stalls the connection at 229376 bytes; reading the first one releases it);
`a_stalled_path_does_not_stall_another_path` (the same numbers on two paths: one path's window
is filled until a write parks, and a send on the other path arrives anyway);
`a_payload_the_size_of_the_stream_window_waits_for_the_reader`.*

### 1.4 The stream budget is backpressure, and it lands on `open`

A peer grants `max_concurrent_uni_streams` and `max_concurrent_bidi_streams`. A transfer holds
its stream until it has been read to EOF, dropped or refused — including while it sits in a
bounded accept queue. When the budget is spent, the next `open` waits. It does not fail, and
`write_all` and `finish` are never reached. A deeper `endpoint_queue` changes nothing: a queued
transfer still owns its stream.

*`the_stream_budget_is_backpressure_not_an_error`,
`a_deeper_endpoint_queue_does_not_raise_the_stream_budget`,
`a_replier_that_stops_accepting_stalls_requesters_after_the_queue_fills` (bidi budget 2,
queue 1: exactly two exchanges complete, the third waits in `open`, one `accept` releases it).*

**The stream budget *is* weida's message credit at L0.** There is no application credit, at
L0 or on the wire: QUIC's byte windows are the byte credit and the concurrent-stream budget is
the message credit, both receiver-granted through transport parameters and both absolute and
idempotent ([decisions/0003](decisions/0003-credit-unit.md) §4.1). A consumer that wants a
prefetch of *n* messages grants `max_concurrent_uni_streams = n` on the connection it reads
from — one per dialled path, per §1.3 — and that is the whole mechanism
[0003 §5]. A per-subscription message credit is the L2 broker's credit frame
([PROTOCOL.md](PROTOCOL.md) §6.6): it travels on the connection of the path its subscription
names, never at L0 [0003 §4.2, as amended by
[0011](decisions/0011-answered-where-it-arrived.md) §4.3].

### 1.5 Cancel: not a retraction, and never EOF

`OutgoingTransfer::cancel` (and dropping an unfinished transfer) resets the stream. The reader
**never** observes the transfer as complete, on every transport: `AsyncRead` fails with
`io::ErrorKind::ConnectionReset`, `read_capped`/`collect` with `Error::Canceled`, never
`Ok(0)`. Bytes the reader already took are unaffected. Bytes already buffered at the receiver
may still be read before the reset is processed: cancellation guarantees the peer cannot
mistake the transfer for a whole one, not that the peer saw fewer bytes.

**Every transport carries the distinction, and the socket transports carry it in the payload.**
QUIC has `RESET_STREAM`; the in-process transport checks the writer's reset flag before
reporting EOF; both socket transports frame every write and carry a `CHUNK_RESET` with its
code. A stream socket has no abort — `SO_LINGER` with a zero timeout followed by `close` is
byte-for-byte indistinguishable from a plain close at the reader — so without the framing
`finish` and `reset` would be the same call, and a cancelled transfer, a transfer dropped
without `finish()` and a producer that died mid-payload would all read as a **complete**
transfer. The framing costs five bytes per write, plus a FIN chunk and a RESET chunk
carrying the code
([decisions/0012](decisions/0012-local-connection-grouping.md) §4.7(e)).

*`cancel_discards_unread_bytes_and_keeps_read_ones`, `push_cancel_mid_transfer`,
`cancel_mid_transfer`, `an_abandoned_transfer_over_a_socket_is_a_named_cancellation`,
`a_dropped_transfer_over_a_socket_is_a_cancellation_and_not_a_short_payload`,
`a_socket_payload_written_in_several_chunks_arrives_whole`.*

### 1.6 Refusal of a one-way transfer can lose the race to the receipt

A peer refuses a one-way transfer with `STOP_SENDING` and a code (`UNKNOWN_ENDPOINT`,
`UNSUPPORTED`, `REJECTED`). That is an application act, and it races the transport
acknowledgement: a payload that fits in flight can be acknowledged by the peer's transport
before its application refuses it, and `delivered()` then resolves `Ok` — truthfully, since a
receipt says nothing about the application, including that it said no. **This is a decided
position, not an open question** ([decisions/0005](decisions/0005-refusal-race.md)): no
application-level signal is added to the L0 wire to order a refusal ahead of the receipt, and
the deterministic counterpart is the `Accepted` of an L2 broker hop. A refusal is
guaranteed to be observed in exactly two constructions [0005 §4.3]: a payload beyond the peer's
stream receive window, where flow control forces the application to act before the write can
finish, and an exchange, whose ERROR frame is written by the application on the reply half and
takes precedence over the request half's receipt. Once the receipt has resolved, a later
refusal reaches **no observer at all** — the `Delivery` is consumed, and no counter, metric or
late error is added for it [0005 §4.4].

*`push_to_an_unknown_path_is_reported`, `push_to_rep_path_is_unsupported`,
`a_publisher_path_refuses_inbound_transfers` (all with 2 MiB payloads for this reason);
`unknown_endpoint_is_reported`, `request_to_pull_path_is_unsupported` (Req/Rep).*

### 1.7 Ordering is per stream and nothing else

Bytes within a stream arrive in order. Streams arrive in no particular order relative to each
other: a peer that opens A then B may see B dispatched first. Every pattern's ordering is
therefore `None` across messages ([GUARANTEES.md](GUARANTEES.md) §6), and the within-stream
order is the only order there is. An application that needs message order must carry it in
the payload or keep one long-lived stream (§5). The sequence key of
[PROTOCOL.md](PROTOCOL.md) §6.2 lets a receiver *detect* a gap or reassemble in order, and
the cost of reassembling is measured: reverse-order completion forces a reorder buffer of
N − 1 of the transfers in flight, and 84 of 256 were held with no adversarial pattern at all
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.7). Ordering is not free at the receiver, which
is why it is negotiated rather than default ([GUARANTEES.md](GUARANTEES.md) §3).

### 1.8 Liveness: idle timeout, keep-alive, redial

- A connection with no traffic is declared dead after `RuntimeConfig::idle_timeout` (30 s
  default), the smaller of the two peers' values governing both.
- Only the **dialling** side sends keep-alives (`RuntimeConfig::keep_alive`, 10 s default). A
  binding with an idle timeout shorter than its clients' keep-alive interval drops them.
- Loss surfaces as `Error::ConnectionLost(cause)` from an operation that was on the dead
  connection, and the cause is kept rather than flattened: `IdleTimeout`, `PeerClosed`,
  `PeerRestarting`, `LocallyClosed`, `Reset` or `TransportError`. It is not a second outcome —
  the failure is definite whichever it is — but it is what a `PeerEvent::Lost` carries and what
  decides whether the redial policy continues. `PeerRestarting` is a peer that closed with
  `RESTARTING` (sent by the successor through `Binding::take_over`): the successor already
  serves the same address, so the first redial waits for nothing and is made even under
  `stop_on_peer_closed` ([decisions/0038](decisions/0038-restart-handoff.md),
  [0039](decisions/0039-handover-says-when.md)). `Peer::peer_count` stops counting the dead
  peer.
- **The address reconnects; the connection does not.** `connect` records a slot the runtime
  redials under `ReconnectPolicy` (100 ms doubling to 30 s with jitter by default;
  `ReconnectPolicy::never()` for none) until the endpoint is dropped or the address
  is disconnected. A redial that reaches a peer with a different key does not go live. A
  `Subscriber` re-registers its route and re-sends its filters on the new connection; nothing
  else is restored — the peer kept nothing ([decisions/0008](decisions/0008-session-identity.md)
  §4.5), and what was published during the gap is gone. Every transition is reported on the
  endpoint's `PeerEvent` stream ([decisions/0031](decisions/0031-transparent-redial-and-the-sender-outbox.md)
  §4.8), and every dialling endpoint's `connection_stats()` counts the redials per address and
  restarts the connection's `age` on each, so a reconnect is visible to an application that
  subscribed to no events ([decisions/0036](decisions/0036-connection-statistics.md) §4.3).
- **`open` waits, `NotConnected` means never connected.** With every slot down, `open` waits
  for the next live connection, bounded by `RuntimeConfig::send_timeout`; an endpoint that was
  never told an address still fails at once with `Error::NotConnected`.

*`idle_timeout_reports_loss_within_the_window` (server idle timeout 500 ms, client
keep-alive 10 s, `PeerEvent::Lost { cause: IdleTimeout }` after 1.5 s of silence, and the
next request on the redialled connection); `crates/weida/tests/reconnect.rs`, all ten:
restart on the same address over QUIC, `AF_UNIX` and inproc with the same `Lost`, `Retrying`,
`Connected` order, `never()`, a replacement server with a new key refused as `PeerChanged`,
`open` waiting through the outage, `send_timeout`, re-subscription, and the outbox of
§1.11.*

### 1.9 Identity: who is on the other side

A peer is named by the SHA-256 fingerprint of its public key (`Fingerprint`, text form
`sha256:<64 hex>`). The dialling side states whom it accepts with `Trust` — pins, anchors, or
only what the address names (`weida://sha256:…@host:port/path`) — and a binding may require a
client identity with `ServerTls::require_client`: one a `Trust` accepts, or, with
`ClientTrust::AnyKey`, any key the client proves it holds — the SSH user key or WireGuard peer
model, where the key *is* the identity and nothing about it is judged
([decisions/0035](decisions/0035-keys-proved-not-judged.md) §4.1). Whatever arrives on a
stream carries the peer's identity in `IncomingMeta::peer`: `PeerIdentity::Key(fingerprint)`
over QUIC, `None` for an anonymous client, `PeerIdentity::Local { uid, gid, pid }` over
`AF_UNIX` and `PeerIdentity::Windows { sid, pid }` over a named pipe, where the **kernel** is
the prover instead of TLS and a PID is an observation that must not be authorized on [0010
§4.4]. In process there is no identity at all, because there is no boundary to prove anything
across. Whichever it is, it comes from the transport and never from a header, so it can be
authorized on but not forged. A peer outside the terms fails `connect` with
`Error::Untrusted(fingerprint)`, carrying what answered so an operator can pin it after
checking it out of band.

Over QUIC, `IncomingMeta::peer_chain` (and `FlowInfo::peer_chain`) carries the chain behind the
key as the peer presented it, DER and leaf first, at most 8 certificates and 32 KiB. weida
parses only the leaf's public key; a device certified by its user's key is a chain the
application verifies and derives the user from, while the device fingerprint stays the peer
[0035 §4.2].

**Authorizing on it is the application's, and the shape is decided**
([decisions/0015](decisions/0015-peer-authorization.md)): the handshake carries no
application credential and will not grow one, so a handler authorizes on `meta().peer`
together with the path the stream dispatched to, and refuses with `Rejected` — or, to keep
the path invisible, by not registering it. A **token** flow, where the grant comes from a
third party rather than from the key, needs two things and they are both existing pieces: a
companion Req/Rep path the client posts the token to, and `ServerTls::require_client` — with a
`Trust` or with `ClientTrust::AnyKey` — so that the verdict can be held against a proved
fingerprint. Without client identity a verdict lives no longer than the connection it was
given on, and a *subscription* cannot present a token at all, because SUBSCRIBE has no reply
half ([PROTOCOL.md](PROTOCOL.md) §6.4) — the one cost that answer has, named here rather than
discovered. A verdict that changes is enforced with the two runtime operations of 0035:
`Radio::evict(peer, filter)` withdraws a join (§6.4), and `Binding::disconnect(fingerprint)`
closes every connection of a key with `REJECTED`. The dialler redials under its
`ReconnectPolicy`, so a disconnect is not a ban; a ban is refusing that peer's arrivals and
joins.

**Where the identity comes from is a source, not a value**
([decisions/0032](decisions/0032-identity-sources-and-the-handoff.md)). `ServerTls` and
`ClientTls` take an `IdentitySource` and a `TrustSource`; a plain `Identity` or `Trust`
converts into the source that never changes. A binding serves whatever its source holds at
each handshake, so a certificate renewed under it reaches the next peer with no re-bind, and
a dialling endpoint presents the identity of the moment on each connection. Three sources
ship in `weida`: `ephemeral` (generated per process, the default and what pinning by address
needs), `files` (a directory bootstrapped owner-only on first use and reloaded when an agent
rewrites it — Vault Agent, cert-manager, certbot, SPIRE all end here), and a static value.
`weida-openbao` adds a certificate signed by a PKI role over the local key (`PkiSign`), trust
anchored on the mount's CA (`PkiAnchor`), material read from KV (`Kv`), and the three ways to
authenticate to the store, the wrapped hand-off among them. **What rotation does to the
fingerprint is the one thing to know:** the fingerprint is the key's, so a certificate renewed
under the same key is invisible to every pinning peer and to the redial of §1.8, while a new
key is a new peer — `IdentityEvent::Renewed` against `KeyChanged` on the source's event stream,
and `PeerEvent::GaveUp { PeerChanged }` on the peers that pinned the old one. Revocation is
not checked, on any path; a short TTL under a renewing source is the answer weida has.

*`crates/weida/tests/identity.rs`, all ten; `crates/weida/tests/identity_source.rs`: a
certificate replaced under a live binding is served to the next handshake, a replaced key is
`KeyChanged` and `PeerChanged`, a bootstrapped directory is owner-only and yields the same key
twice; `crates/openbao/weida-openbao/tests/scripted.rs`, and `tests/bao_dev.rs` against the
real server.*

### 1.10 What a local transport changes

Everything above is written against QUIC. Over the three local transports of
[0010](decisions/0010-local-transport.md) five of those statements mean something else, and
they have the same cause: there is no QUIC connection to share, because one transfer is
one channel pair (inproc) or one OS connection (`AF_UNIX`, a named pipe).

- **§1.2's window arithmetic does not apply.** A receipt still means the peer's transport
  holds the bytes, but "beyond the window" has no local analogue: inproc hands over a buffer
  and a socket has the kernel's own buffer, so a receipt is never evidence that the
  application is reading. The weaker reading — a transport receipt is not an application
  read — is the one that holds everywhere.
- **§1.3's two windows collapse into one.** There is no connection window to share, so the
  head-of-line coupling the per-path connections of
  [0002](decisions/0002-control-and-bulk-separation.md) exist to remove cannot arise:
  `a_stalled_path_does_not_stall_another_path` is a statement about QUIC, and locally it is
  true by construction. What replaces it is a descriptor count — every live transfer is a
  file descriptor, bounded by `max_local_streams` (255, the number Windows' pipe instance cap
  fixes) rather than by a stream budget. The process's own descriptor limit applies on top:
  macOS starts at 256, which the default nearly fills on one side, so a process that holds
  both ends raises `RLIMIT_NOFILE` or lowers `max_local_streams`.
- **§1.4's stream budget becomes that descriptor count.** `open` waits for a descriptor at
  the local ceiling exactly as it waits for a QUIC stream at the peer's budget: the
  backpressure is a wait on every transport, bounded by the caller's own deadline and
  cancelled by dropping the future ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.13). A descriptor comes free when a transfer ends,
  which locally means when both ends are done with it — so a consumer that stops reading
  slows its producer down instead of failing it. What `LimitExceeded` still means locally is
  a refusal somebody decided on, the reverse pool of §4 below, and never a busy transport.
- **§1.8's liveness is the kernel's, and the loss is learned at the next open.** There is no
  idle timeout and no keep-alive locally: a peer that goes away closes its socket or drops its
  channel. In process both ends share one closed-state cell, so the dialling side learns it
  at once. On a socket transport nothing travels on the control connection after the HELLOs,
  so the first `open` that finds nobody answering on the address is what reports
  `ConnectionLost(PeerClosed)` and marks the peer gone — and it is also what starts the
  redial of §1.8; a body handed to `send` at that moment is held in the outbox like any
  other. That is a *better* signal than a timer, and it is why the two timers are not
  simulated — an invented local heartbeat would only be able to say what the kernel already
  said. The redial itself waits on the clock for a socket and on the name registry for a
  bus, which is back exactly when its name is bound again
  ([decisions/0031](decisions/0031-transparent-redial-and-the-sender-outbox.md) §4.10).
- **§4's fan-out needs a parked connection.** A publisher on a socket transport writes each
  copy on a reverse connection the subscriber parked in advance
  ([0012](decisions/0012-local-connection-grouping.md) §4.4), bounded by
  `Limits::max_parked_reverse` (8). An exhausted pool is a **second drop cause** beside the
  subscriber byte budget of §4: the copy is dropped, counted in `Publisher::dropped`, and the
  subscription survives. A subscriber that parks nothing is refused at `connect` rather than
  silently receiving nothing.
- **§1.5's cancellation guarantee is kept by framing rather than by the kernel.** Neither
  socket transport gets it for free: a named pipe has no half-close, and a stream socket has a
  half-close and no abort, which is the same problem from the other side — `SO_LINGER` with a
  zero timeout plus `close` is byte-for-byte a plain close at the reader. So both frame their
  payload the same way (`crates/weida/src/chunked.rs`): five bytes per write, a FIN chunk, and
  a RESET chunk carrying the code, which is what makes a cancelled transfer `Canceled` at the
  reader instead of a short payload that looks whole. In process the reader checks the writer's
  reset flag instead, which needs no framing at all. The cost is priced in 0012 §4.7(e): five
  bytes per write and the zero-copy read.

*`crates/weida/tests/transports.rs`: the three pattern bodies over all three transports,
`a_unix_peer_presents_the_principal_the_kernel_proved`,
`a_local_peer_that_goes_away_is_reported_as_connection_loss`,
`an_exhausted_reverse_pool_drops_the_copy_and_counts_it`,
`a_subscriber_that_parks_nothing_is_refused_at_connect`.*

---

### 1.11 An interrupted stream: cancel, reschedule, reconnect — never a resend

A message can be batched and retried because it is complete before it is sent. A **stream is
unfinished by definition** until its FIN, so the interesting failure is not "was it delivered"
but "what does a half-sent stream mean". weida answers that in one sentence and then hands the
decision over:

> **Within a connection, QUIC retransmits; when the connection ends, an unfinished stream is
> gone, and weida does not resend it.**

What the two sides observe is exact, on every transport. The sender gets
`ConnectionLost` before FIN and `Ok` after it (§1.1, the pattern tables below); the receiver
never sees an unfinished stream as complete — `AsyncRead` fails with `ConnectionReset`,
`collect` with `Error::Canceled`, **never** `Ok(0)` (§1.5). "Half a frame is not a frame"
(§4.1) is the rule and not a special case, and every transport enforces it: the socket
transports with the framing of §1.5.

What happens next is the **application's** choice, and there are exactly three, one of which is
usually wrong:

| Answer | When it is right | What it costs |
| --- | --- | --- |
| **cancel** — the transfer is abandoned | the payload was only useful whole and only useful now: a video frame, a live tail, a snapshot that is already stale | nothing; this is the default, because it is what the transport already did |
| **reschedule** — the work is re-derived and sent as a *new* stream | the sender still holds or can regenerate the source, and the receiver is idempotent or does not care | the sender keeps the source, or can produce it again |
| **reconnect and continue** — a new stream carries the remainder | the payload is large, immutable and expensive to re-send, and the receiver reported **how far it got** | an application-level identity for "the same payload", plus a receiver that persisted a prefix. Both are the application's; weida supplies neither |

The third is the one that needs the cursors of
[decisions/0023](decisions/0023-completion-is-a-cursor.md): a receiver that reports "durable up to
*N*" is telling the sender what it may skip, and the sender decides. weida does **not** decide
and does not re-open a stream on anyone's behalf — a library that did would be rebuilding
retries, which in every system this repository surveyed are application-level
(ZeroMQ's Lazy Pirate and Titanic are recipes, not features). The one payload it does remember
is not a stream: a body handed to `send` is a **message**, owned by the runtime from the call,
held in a bounded outbox until it is written to a live connection and never resent after that
([decisions/0031](decisions/0031-transparent-redial-and-the-sender-outbox.md) §4.2-§4.3).

Which answer a pattern needs follows from the pattern, not from the transport:

| Pattern | The usual answer | Why |
| --- | --- | --- |
| Req/Rep | **reschedule** | a request has a reply half, so the requester learns the outcome and can re-issue; `Indeterminate` is exactly the case where re-issuing must be safe ([FAILURE_MODEL.md](FAILURE_MODEL.md)) |
| Push/Pull | **reschedule**, and only the sender can | the puller cannot ask: a one-way transfer has no reply half. This is the trade the pattern is |
| Pub/Sub | **cancel** | the copy is per subscriber and best effort by definition; a dropped copy is counted, not retried ([GUARANTEES.md](GUARANTEES.md) §6) |
| PAIR | **reschedule** | symmetric, so either side can re-send what it still holds; neither has a reply half to ask with, exactly as Push/Pull |
| SURVEY | **cancel** | a survey is a partial result by construction: an answer that did not arrive before the deadline is dropped and counted, and re-asking is a new survey |
| BUS | **cancel** | a copy is per member and best effort, as Pub/Sub's is; a member that could not take one is counted in `dropped()` |
| RADIO/DISH | **cancel** | a segment is worth something only until its successor opens or the dish's `max_age` passes; a lost segment is a gap in the segment numbers the dish sees, counted at the radio by cause (§6.4, `crates/weida/tests/radio.rs`) |
| a queue (L2) | **cancel on the way in, reschedule on the way out** | an inbound stream that never reached FIN was never a message, so nothing was admitted and nothing was confirmed; a delivery that broke is redelivered, because the queue still holds the message |

That last row is the whole bridge from the stream primitives to the message world: **a message is
a stream that reached FIN.** Which is why the stream and message pattern families **overlap rather
than stack**: Req/Rep is both, because a request is a stream and a completed request is a message,
and the message vocabulary adds an assumption — the payload is whole before it is used — rather
than a layer ([decisions/0024](decisions/0024-three-families-one-back-channel.md) §4.1). A queue's unit is a completed stream, which is why an interrupted
admission needs no vocabulary of its own — there is nothing to talk about yet — and why a
redelivery is an ordinary new stream rather than a continuation.

**Every row of both tables above is a test**
(`crates/weida/tests/interrupted.rs`), and so is the rule they follow from: a transfer
interrupted before FIN is a definite `ConnectionLost` for the sender
(*`a_transfer_interrupted_before_fin_is_connection_lost_for_the_sender`*); a reader never sees
an interrupted stream as complete, with `collect` failing `Canceled`, `AsyncRead` failing
`ConnectionReset` and **never** `Ok(0)`
(*`a_reader_never_sees_an_interrupted_stream_as_complete`* over QUIC, and over a socket
transport *`a_dropped_transfer_over_a_socket_is_a_cancellation_and_not_a_short_payload`*, §1.5); a requester learns
`Indeterminate` and can re-issue (*`a_reqrep_requester_can_reissue_after_an_indeterminate_outcome`*);
a Push producer is the only side that can (*`a_push_producer_is_the_only_side_that_can_reschedule`*);
a lost fan-out copy is counted and never re-sent
(*`a_pubsub_copy_lost_to_a_dead_subscriber_is_counted_not_retried`*); and a cursor reported
before the break **survives** it
(*`a_cursor_reported_before_the_break_survives_the_break`*) — which is the whole value of a
cursor over a verdict: a whole-message verdict tells an interrupted sender nothing, a cursor
tells it a number. Beside them sits the liveness bound a reliable work chain depends on:
a bound side observes a dead dialler within `Limits::idle_timeout`
(*`a_bound_side_observes_a_dead_dialler_within_the_idle_timeout`*).

### 1.12 Expiry and priority

Two sender-local mechanisms, neither on the wire
([decisions/0034](decisions/0034-late-is-lost.md) §4.3):

- **`OutgoingTransfer::expire_at(deadline)`** resets the stream with `CANCELED` if its bytes
  are not all acknowledged by then. A `write_all` still blocked at the deadline fails with
  `Error::Expired`; on QUIC the deadline also holds after `finish`, whose receipt then resolves
  to `Expired`, because a reset is accepted until every byte is acknowledged; on a local
  transport it acts until `finish`. The reader sees what it sees for any cancellation —
  `Canceled`, never EOF (§1.5). `Expired` is **not** a definite failure: the peer may have read
  every byte before the reset landed. `Runtime::expired_transfers` counts expiries apart from
  cancels, which the wire cannot tell apart.
- **`OutgoingTransfer::set_priority(i32)`** orders a stream against the other streams of **its
  connection**, on `quinn`'s scale. A connection is one dialled path (§1.3), so priority orders
  transfers on one path and never across paths; on a local transport, where each stream is its
  own OS connection, it is a no-op. A datagram outranks every stream on its connection already.
  Segment copies take theirs at open, from the layer and the sender's `SegmentTerms::priority`:
  `(15 - layer) * 65536 + priority`, so every base layer on a connection goes before any
  enhancement layer ([decisions/0037](decisions/0037-layered-segments.md) §4.4).

*`a_transfer_past_its_deadline_is_reset`.*

---

## 2. Req/Rep

One bidirectional stream per exchange. The requester writes the request on its half and reads
the reply, or an ERROR, on the other. The stream is the correlation; nothing on the wire names
an exchange.

| | `Requester` (`Req`) | `Replier` (`Rep`) |
| --- | --- | --- |
| Compatible peer | `Replier`, `Acceptor` | `Requester`, `Peer::open_bi` |
| Direction | connects | binds |
| Send/receive pattern | any number of concurrent exchanges, each `open` → write → `finish` → `recv` | `accept` → read body → `reply` → write → `finish`, or `refuse(code)`, or drop for `NO_REPLY` |
| Incoming routing | the reply half of the exchange that asked | fair, bounded queue per path (`endpoint_queue`) |
| Outgoing routing | round-robin over live peers, one exchange per pick | the exchange that asked |
| Action with no peer | `Error::NotConnected` immediately; `ConnectionLost` if every peer died | `accept` waits |
| Transport | one client-opened bidirectional stream | |
| Ordering | `None` across exchanges; request and reply each in order | |
| Delivery signal | the reply itself; the request's receipt is available from `open` but proves less than the reply | |
| Backpressure | `max_concurrent_bidi_streams` on `open`, then both halves' windows | a full queue stalls the requester's `open` |
| Cancellation | drop the `ReplyStream`: `STOP_SENDING(CANCELED)` on the reply half, `IncomingRequest::canceled` fires | drop the request: ERROR `NO_REPLY` + `STOP_SENDING(REJECTED)` |

Failure modes, from the requester's side:

| Event | Result of `recv` / `request` |
| --- | --- |
| Path unknown at the peer | `Error::UnknownEndpoint` (ERROR frame) |
| Path serves another pattern | `Error::Unsupported` (ERROR frame) |
| Replier dropped the request | `Error::NoReply`; the request may have had an effect |
| Replier refused on purpose | the code it chose: `Error::Rejected`, or `Error::NoReply` where the request was taken and nobody will answer it — a handler whose far side dropped it silently |
| Connection lost before the request FIN | `Error::ConnectionLost` from the write: definitely not delivered |
| Connection lost after the FIN, no reply seen | `Error::Indeterminate`: the replier may have acted |
| Replier reset the reply mid-stream | `Error::Canceled` from the read |

Request and reply stream simultaneously: the replier may `take_body` and `reply` before the
request has finished, and a requester writing a large request must drain the reply
concurrently or it stalls the replier and therefore itself
(*`streaming_overlap`*). Router/Dealer are not separate types: unlimited concurrent exchanges
give Dealer's multiplexing, and the reply riding the originating stream gives Router's
addressing for free ([ARCHITECTURE.md](ARCHITECTURE.md) §6a).

---

## 3. Push/Pull

One unidirectional stream per message. Fire-and-forget with an optional transport receipt.

| | `Pusher` (`Push`) | `Puller` (`Pull`) |
| --- | --- | --- |
| Compatible peer | `Puller`, `Acceptor` | `Pusher`, `Peer::open` |
| Direction | connects | binds |
| Send/receive pattern | `send` (returns at FIN, receipt dropped) or `open` → write → `finish` → `delivered` | `recv` → read |
| Incoming routing | — | fair, bounded queue per path (`endpoint_queue`) |
| Outgoing routing | round-robin over live peers, one message per pick | — |
| Action with no peer | `Error::NotConnected` immediately; `ConnectionLost` if every peer died — never blocks, unlike ZeroMQ's PUSH | `recv` waits |
| Transport | one client-opened unidirectional stream | |
| Ordering | `None` | |
| Delivery signal | none (`send`) or the transport receipt (§1.2) | none; EOF is EOF |
| Backpressure | `max_concurrent_uni_streams` on `open` (§1.4), then the windows (§1.3) | a puller that stops reading stalls its pushers after the budget; nothing is dropped |
| Cancellation | `cancel` or drop: the puller's read fails, never EOF (§1.5), on every transport | drop an unread transfer: `STOP_SENDING(REJECTED)` |

Failure modes, from the pusher's side:

| Event | `send` | `delivered()` |
| --- | --- | --- |
| Path unknown / wrong pattern | `UnknownEndpoint` / `Unsupported`, or `Ok` if the transport acknowledged first (§1.6) | same |
| Connection lost before FIN | `ConnectionLost` | — |
| Connection lost after FIN | `Ok` (the FIN was queued) | `Indeterminate` |
| Puller refused mid-transfer | `Rejected` | `Rejected` |

Round-robin is per message, and a peer is skipped only once its connection is closed; a peer
that is merely slow keeps receiving its share and eventually stalls the pusher through its
windows. Spreading work by capacity rather than by turn is broker work (L2).
*`push_round_robins_two_peers`*.

---

## 4. Pub/Sub

One unidirectional stream per subscriber per message, opened by the publisher's per-subscriber
writer. Filters are **segmented patterns** carried in SUBSCRIBE/UNSUBSCRIBE frames: segments
separated by `.`, `*` for exactly one whole segment, a trailing `#` for zero or more segments,
everything else literal ([PROTOCOL.md](PROTOCOL.md) §6.4,
[decisions/0007](decisions/0007-topic-namespace.md) §4.2). `sensors.*.temp` selects one
segment, `sensors.#` selects `sensors` and everything under it, and `sensors.temp` does *not*
select `sensors.temperature` — which a byte prefix did, and which is the reason the grammar
changed. Endpoint **paths** are untouched by any of this: they stay opaque and are matched
exactly [0007 §4.1].

| | `Publisher` (`Pub`) | `Subscriber` (`Sub`) |
| --- | --- | --- |
| Compatible peer | `Subscriber` | `Publisher` |
| Direction | binds | connects |
| Send/receive pattern | `publish(topic, bytes)`: synchronous, returns the number of subscribers reached; `open(topic)` for a payload written chunk by chunk (§4.1) | `subscribe`/`unsubscribe`, then `recv` |
| Incoming routing | — | one bounded queue (`endpoint_queue`) over every peer |
| Outgoing routing | fan-out to every subscriber whose filter matches, one copy each | — |
| Action with no peer | `publish` returns `0`; nothing is queued for a subscriber that does not exist yet | `recv` waits; `peer_count` is the only sign that the publisher is gone |
| Transport | one server-opened unidirectional stream per (subscriber, message) | |
| Ordering | `None`; one subscriber's copies are enqueued in publication order, but that is not a guarantee | |
| Delivery signal | none, and none is possible: `publish` never awaits a subscriber | none |
| Backpressure | `Drop`: a copy that does not fit in `subscriber_buffer_bytes` for that subscriber is dropped and counted in `dropped()`, and per topic and cause in `dropped_on(topic)` / `drops()` — budget, full queue, or no parked connection on a socket transport — so a starving signal can be named rather than inferred; the publisher never blocks | a subscriber that stops reading fills its budget at the publisher and then loses messages |
| Payload | whole `Bytes`, at most `subscriber_buffer_bytes`; larger is `LimitExceeded` before fan-out — **or unbounded through `open`**, where the budget bounds one chunk (§4.1) | |

Failure modes:

| Event | Publisher | Subscriber |
| --- | --- | --- |
| Slow subscriber | drops for that subscriber only, `dropped()` grows and `dropped_on(topic)` says which topic and why | silently misses messages: nothing on the wire says so |
| Subscriber's connection lost | its filters and writer are removed | `recv` keeps waiting; `peer_count` drops |
| Publisher's connection lost | — | `recv` keeps waiting; filters are remembered and re-sent on the next `connect` |
| Too many filters on one connection | closes it with `LIMIT_EXCEEDED` | `connect`/`subscribe` fails |
| Message beyond the budget | `LimitExceeded`, nothing sent | — |
| Streamed transfer a subscriber cannot keep up with | that subscriber's stream is reset with `CANCELED` and the drop counted; the others keep receiving | a partial payload, ended by a reset rather than a FIN, so it is never mistaken for a whole one, on every transport (§1.5) |

This is the one place weida answers overload by discarding, and it is confined to fan-out
([GUARANTEES.md](GUARANTEES.md) §6). Under `core` a subscriber cannot detect a drop; **subscriber-side
drop detection is what the sequence key of [PROTOCOL.md](PROTOCOL.md) §6.2 exists for**
([decisions/0001](decisions/0001-sequence-field.md) §7.2). A subscriber that has negotiated
the detect level of `PerProducer` sees the gap — how many messages were missed, expected
against seen — without anything being held back, which is the honest answer to a policy that
drops on purpose. The reassemble level holds messages instead, bounded by
`Limits::max_reorder_hold` and releasing the oldest held transfer with its gap reported at
the cap, at the buffer cost measured in [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.7.
Both are **on the wire and implemented**: DATA keys 6 and 7 carry the sequence and the
producer ([PROTOCOL.md](PROTOCOL.md) §6.2), and a fan-out drop reaches a detecting
subscriber as a `Gap`.
*`slow_subscriber_drops_not_blocks`, `subscribe_filters_topics_by_segment`,
`a_dropped_fan_out_copy_shows_up_as_a_gap`,
`a_full_hold_reports_the_pub_sub_drop_it_was_waiting_for`.*

**What the width costs, measured** ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11). The
per-subscriber stream above is what buys the isolation one shared queue per socket cannot.
At 1 KiB on loopback with both
ends in one process: **396-436 KiB** of transport state per subscriber, **88.8 ns** of
publisher CPU per subscriber per message (22.94 µs for a publish at width 256, against 287 ns
at width 1), **276-279 Kcopies/s** delivered, and a median idle latency of **507-539 µs** at
width 256 against 34.7-37.9 µs at width 1. The consequence for sizing: neither memory nor the
publisher's CPU binds at 10⁴ subscribers — the delivery rate does, at about **27 messages per
second** to 10⁴ subscribers or **276** to 10³.

**Which ceiling a stalled subscriber reaches is a function of the payload**, and both sides are
measured. A subscriber that stops reading is bounded twice, by `endpoint_queue` messages and by
`subscriber_buffer_bytes` of payload, so the two cross where the message size is
`subscriber_buffer_bytes / endpoint_queue` — **32 KiB** at the defaults. Below it the queue
refuses first and the publisher holds **0.34-1.19 MiB** per stalled subscriber; above it the
budget refuses first and the publisher holds **9.3-10.7 MiB**. `dropped_on(topic)` distinguishes
the two causes, which is what makes this diagnosable in production rather than in a benchmark.
One correction that follows: sizing a publisher by `subscribers × subscriber_buffer_bytes`
overstates it, because one `publish` is one `Bytes` that every copy shares — the budget is an
accounting bound and the resident cost is one payload per distinct message.

### 4.1 Streaming fan-out: a payload the publisher never holds

`Publisher::open(topic)` returns a `FanOut`: one stream per matched subscriber, written
chunk by chunk. It exists because `publish` takes a whole `Bytes` and refuses anything above
`subscriber_buffer_bytes` — so a 33 MB video frame could not be published through it at all,
and raising the limit would buy a per-subscriber copy of it inside the publisher, which
is exactly the materialization [INVARIANTS.md](INVARIANTS.md) forbids
([requirements/zeughaus-video.md](requirements/zeughaus-video.md) request 1). With `open` the
budget bounds a **chunk**, one `Bytes` allocation is shared by every copy, and the payload
has no ceiling.

| | What it does |
| --- | --- |
| `write_within(chunk, limit)` | waits up to `limit` for the subscribers with no room — one bound for the whole call, all of them waited for together, so a thousand stalled subscribers cost one `limit` — then drops **those** subscribers' copies. The bound is mandatory and finite for the reason `drain(Duration)`'s is ([decisions/0009](decisions/0009-drain.md) §4.4): an unbounded wait is how a publisher hangs on a peer, and never waiting would make a payload larger than the budget undeliverable to anybody — the publisher would outrun its own budget and abort every copy. A chunk larger than the budget could never be queued for anyone and fails with `LimitExceeded` at once. **A call that is dropped while it waits abandons the transfer**: the chunk is queued for the subscribers that had room and not for the rest, and nothing remembers which, so later writes fail with `Canceled` and `finish()` resets every copy and counts none |
| `write_now(chunk)` | never waits: a subscriber without room right now loses the transfer. Fan-out's `Drop` in its purest form, and the right call where a later chunk supersedes an earlier one |
| `finish()` | FIN on every remaining copy; returns how many subscribers got all of it as far as this side can tell. A fan-out copy carries no receipt, so the acknowledgement is the drain's business and nobody else's. After a `write_within` that was dropped while it waited it resets every copy instead and returns 0 |
| dropping the handle | resets every copy, so no subscriber mistakes a partial payload for a whole one, on every transport (§1.5) |

**This is also v0's conflation**, and that is decided rather than a workaround
([decisions/0016](decisions/0016-conflation.md) §4.3): a producer that wants "keep the newest,
discard the rest" holds one slot for the latest value, publishes it with `open` plus
`write_now`, and sets `subscriber_buffer_bytes` to about one value — then a subscriber that
falls behind loses that value and receives the next one, which is what a conflating queue would
have done for it. The transport grows no key for it, because the producer already has one.

Two things are deliberately unlike `publish`. The subscriber set is **fixed at `open`**: a
subscriber that arrives mid-payload would receive a fragment with no way to know it, so it
gets the next message. And the drop is per subscriber and per *transfer* rather than per
message — a subscriber that misses one chunk loses the whole payload, because half a frame is
not a frame. Both are counted exactly like any other fan-out drop, in `dropped_on(topic)`.
*`a_streamed_publish_carries_a_payload_no_publish_could_take`,
`a_streamed_publish_drops_the_subscriber_that_stalls_and_keeps_the_other`,
`a_streamed_publish_that_never_waits_drops_at_the_budget`.*

---

## 5. Raw streams: `Peer` and `Acceptor`

The L0 core, for topologies the patterns do not cover. A `Peer` dials and opens either stream
kind or a datagram flow; an `Acceptor` binds one path and receives them as `Incoming::Stream`,
`Incoming::Exchange` or `Incoming::Flow`. Everything in §1 applies without translation, and
nothing else is added: no selection policy beyond round-robin over peers, no fan-out, no
filters.

Two things the patterns cannot express are natural here:

- **A long-lived stream.** Open once, write many messages with a framing of your own, and the
  transport orders them for you — the only ordered channel weida has, and one every transport
  provides, since a stream is ordered bytes wherever it runs. Over QUIC it keeps its window
  and its place in the budget for as long as it is open, and its reader's pace is its writer's
  pace (§1.3); locally it keeps a descriptor instead (§1.10). A standing feed of frames to one
  viewer is this shape.
- **Both stream kinds on one path.** A control exchange and a bulk one-way stream to the same
  endpoint, dispatched by one accept loop.

*`acceptor_receives_both_stream_kinds`.*

**A segment from the dialling side** ([decisions/0037](decisions/0037-layered-segments.md)
§4.2, §4.11): `Peer::segment(topic, SegmentTerms)` opens one copy of a segment toward the bound
path, with a radio's supersession, expiry, layers and never-waiting `write_layer`; its number
comes from the connection, so every `Peer` sharing it sends one rising sequence, and its
budget is the `Peer`'s `subscriber_buffer_bytes` over all its segments. `Peer::segment_drops`
counts what its copies lost. The acceptor receives each layer as `Incoming::Stream` with
`meta.topic`, `meta.segment` and `meta.layer` set, and a stale layer — a segment not newer than
one delivered on that path and topic of the connection, or a layer of it already delivered — is
refused with `CANCELED` before it reaches the acceptor.

*`a_peer_segment_reaches_an_acceptor_with_its_number`,
`a_peer_segment_supersedes_the_previous_one_on_its_topic`,
`two_peers_on_one_connection_number_one_sequence`,
`a_peer_segment_carries_its_layers_to_the_acceptor`,
`an_acceptor_refuses_a_segment_older_than_one_it_delivered`,
`a_receiver_delivers_each_layer_of_a_segment_once` (`crates/weida/tests/radio.rs`).*

**A datagram flow** is the third carrier
([decisions/0034](decisions/0034-late-is-lost.md) §4.2), for units worthless once late — a
20 ms voice frame. `Peer::open_flow(FlowMeta)` writes a FLOW header on a stream that stays open
as long as the flow and returns without waiting; `Flow::send(bytes)` is synchronous and never
waits. Its errors are `TooLarge { max }` against the connection's current datagram size,
`DatagramsUnavailable` where the capability was not agreed — both runtimes need
`Limits::datagram_receive_bytes` above zero, and weida never substitutes a stream — or, once
the flow has ended, the error that ended it: the acceptor's refusal (`IncomingFlow::refuse` is
`Rejected`), its release (dropping an `IncomingFlow` is `Canceled`), `UnknownEndpoint`,
`Unsupported`, `LimitExceeded` past `max_flows`, or `ConnectionLost`. The receiver holds each
flow's unread datagrams in a drop-oldest ring of `flow_queue_bytes`, so a slow consumer loses
its oldest and stalls nobody; `FlowStats` counts both ends and `PathStats` passes the path's
RTT, window and loss through. `PathStats` is the connection's, not the flow's: every dialling
endpoint reads it for each live connection through `connection_stats()`, with the UDP traffic in
both directions, no flow needed ([decisions/0036](decisions/0036-connection-statistics.md)).
A flow opened through a dialling `Peer` survives a redial:
sends while no connection is live are dropped and counted `not_live`, never queued, and the
flow is registered again on the new connection. On the local transports the FLOW stream carries
the datagrams itself, at most 1200 bytes each.

*`a_flow_carries_datagrams_to_an_acceptor`, `a_flow_to_an_unregistered_path_is_refused`,
`a_stalled_flow_loses_its_oldest_and_its_sibling_keeps_receiving`,
`a_runtime_without_datagrams_cannot_open_a_flow`,
`a_payload_over_the_datagram_size_is_too_large` (`crates/weida/tests/flows.rs`);
`flow_over_quic`, `flow_over_inproc`, `flow_over_unix`;
`a_flow_is_reopened_after_the_server_restarts`.*

---

## 6. PAIR, SURVEY, BUS, RADIO/DISH and SWARM

PAIR, SURVEY and BUS complete the nanomsg set ([ARCHITECTURE.md](ARCHITECTURE.md) §6b maps
them), and RADIO/DISH is the pattern ZeroMQ names for lossy fan-out. The first three add **no
wire vocabulary**: a `Paired` talks to a bare `Peer` and `Acceptor` on the same path, a
`Respondent`'s route is byte-for-byte a replier's, a `BusMember`'s is a puller's. RADIO/DISH
adds two keys, DATA `13` and SUBSCRIBE `2` (§6.4). SWARM is no ZeroMQ pattern: it fetches one
verified blob from many holders, sits behind the off-by-default feature `swarm`, rides the
request route like SURVEY and adds nothing to [PROTOCOL.md](PROTOCOL.md) (§6.5).
Router/Dealer are emergent (§2); connecting publishers and binding pushers are deferred.

### 6.1 PAIR

`Runtime::pair` dials, `Listener::pair` binds, and after that the two are the same type with
the same calls: **one-way transfers in both directions**, one connection, one peer.

| | `Paired`, dialling | `Paired`, bound |
| --- | --- | --- |
| Compatible peer | a bound `Paired`, `Acceptor`, `Puller` | a dialling `Paired`, `Peer`, `Pusher` |
| Send/receive pattern | `connect` once, then `send`/`open` and `recv` concurrently | `send`/`open` and `recv` concurrently |
| Outgoing routing | the one connection it dialled | the one connection its peer dialled |
| Action with no peer | a second `connect` is `Error::LimitExceeded` | the first send **waits** for a peer to appear |
| Second peer | — | refused with `LIMIT_EXCEEDED` while the first is **live**, and the first is **kept** |
| Backpressure | `Block`: `open` waits for the peer's `max_concurrent_uni_streams` (§1.4) and then for the two windows (§1.3); a peer that stops calling `recv` fills its `endpoint_queue`, which the dispatcher awaits a slot in, so the sender stalls once the budget is spent. Nothing is dropped | the same, preceded by one wait more — and the only wait in this library with **no bound at all** |

Two rules a caller can get wrong. **The first peer is kept**: ZeroMQ's PAIR drops the newcomer
silently, weida refuses it and says so, because a capacity decision is reported
([decisions/0005](decisions/0005-refusal-race.md)) — the refused sender reads
`Error::LimitExceeded` and the connection survives. And a **bound** pair cannot address a peer
it has not heard from, so its first send waits rather than buffering: a queue there would be a
guarantee nobody asked for.

**The claim is on a connection, not on eternity.** A pair whose peer went away — a restarted
process, a connection past `idle_timeout`, an ordinary shutdown — is claimable again, and the
next peer to speak takes it. Holding a bound endpoint against a connection that no longer
exists would make one peer's shutdown permanent, which is neither ZeroMQ's behaviour nor
nanomsg's and is not what "one peer at a time" means. A dialling pair likewise releases its
path when it is dropped, so the next pair on the same pooled connection can claim it.

**That first wait is unbounded, which the "Action with no peer" row understates.** A bound
pair's `open` blocks in `PairOwner::peer` until some connection has claimed the endpoint, and
that wait has no deadline, no timeout knob and no idle timer under it — nothing has been dialled
yet, so none of §1.8's bounds apply. It is the caller's to bound: wrap the send in
`Exec::within`, or drop the future. Everything after it is ordinary `Block` backpressure, the
same on both halves.

Both halves also **number their transfers** under a negotiated `PerProducer` ordering. That is
worth saying because the two halves are different send paths in the code, and one link with
two ordering behaviours — numbered one way, unnumbered the other, neither side told — is the
failure that shape invites.

*`both_directions_carry_transfers_concurrently`,
`a_second_connection_is_refused_and_the_first_keeps_working`,
`a_peer_that_goes_away_leaves_the_endpoint_claimable`,
`a_dropped_pair_releases_its_path_on_a_pooled_connection`,
`both_halves_of_a_pair_number_their_transfers`,
`a_pair_talks_to_a_bare_peer_and_acceptor_on_the_same_path`.*

### 6.2 SURVEY

`Surveyor::survey(body, deadline)` opens **one exchange per connected respondent** — not the
round-robin pick Req/Rep uses — and `SurveyRun::next(max_bytes)` yields answers as they arrive
until the deadline. A `Respondent` is a `Replier` with a different name: same route, same
`accept`, same backpressure.

| | `Surveyor` | `Respondent` |
| --- | --- | --- |
| Direction | connects, and **accumulates** peers on purpose | binds |
| Outgoing routing | every live peer, one exchange each | the exchange that asked |
| Deadline | the caller's, per survey — it bounds the **asking** as well as the collecting; nothing on the wire carries it | never learns it |
| A late answer | dropped and **counted** in `late()`; an answer that arrived *in time* is delivered even to a caller that reads after the deadline | cannot tell |
| No respondents | an empty run, not an error | — |

Three rules. The deadline is **not negotiated** and a respondent never learns it, so a survey
is a local decision about how long to wait. A late answer is counted **where it arrives**
rather than where the caller reads, so `late()` means "after the deadline" whatever the caller
does with its handle ([GUARANTEES.md](GUARANTEES.md) §6). And "nobody answered" is an answer:
a survey with no respondents returns a run whose first `next` is `None`, never
`Error::NotConnected`.

A respondent that refuses or dies mid-reply is **one `Err` among the answers** and ends
nothing: the exchanges are independent. A respondent that accepts a question and stops reading
is bounded by the same deadline: asking is two awaits the peer controls — its stream budget and
its flow-control window — so the deadline covers them, and a respondent that could not be asked
inside it is simply not counted in `respondents()`. Dropping a `SurveyRun` ends the exchanges
it was collecting, which is what frees their streams: a respondent that never answers is told,
exactly as a requester that walks away tells its replier.

*`every_respondent_answers_within_the_deadline`, `a_late_reply_is_counted_and_not_delivered`,
`an_answer_that_arrived_in_time_survives_a_caller_that_reads_late`,
`a_respondent_that_never_reads_cannot_hold_the_survey_open`,
`dropping_a_run_ends_the_exchanges_it_was_collecting`,
`a_respondent_that_refuses_is_one_error_among_replies`,
`a_respondent_that_dies_mid_reply_does_not_end_the_survey`,
`a_survey_with_no_respondents_is_empty_not_an_error`.*

### 6.3 BUS

`Listener::bus(path, tls)` is the only factory that takes both a path and dialling terms,
because a bus member is the one role that is **bound and dialling at once**. Joining is an
ordinary `connect`, leaving an ordinary disconnect; there is no membership protocol.

| | `BusMember` |
| --- | --- |
| Compatible peer | another `BusMember` on the same path |
| Send/receive pattern | `connect` per member, then `send` → every other member, `recv` |
| Outgoing routing | every member this one dialled; **never itself** |
| Backpressure | `Drop`, per member: one writer each, bounded twice — `endpoint_queue` messages and `subscriber_buffer_bytes` of payload — and a copy that fits neither is dropped and counted in `dropped()` |
| Payload | one `Bytes` for the whole fan-out, at most `subscriber_buffer_bytes`; larger is `LimitExceeded` before any member is charged |
| Ordering | `None` across members, per stream within one |
| Relay | none: *n* members is *n* × (*n* − 1) deliveries |

A message reaches every **other** member structurally — `send` writes to the peers this member
dialled, and a member does not dial itself — so nothing filters a copy out, because no copy is
ever addressed to it. There is **no relay**: weida forwards on nobody's behalf, which is the
trade nanomsg's BUS makes too. And the fan-out needs a **writer per member** for the same
reason Pub/Sub's does: without one, a member that stops reading stalls its stream tasks, the
sender exhausts `max_concurrent_uni_streams` and its next `send` blocks. With one, a slow
member costs its own copies — counted in `dropped()`, at the queue when it is full and at the
wire when a write fails — and never the sender's time.

The two bounds are the same pair Pub/Sub charges per subscriber, and for the same reason: a
queue counted only in messages bounds nothing, because a bus send costs `endpoint_queue` ×
`body.len()` per member and then multiplies by the members, and `body.len()` is the
application's number. A body above the byte budget is refused to the **sender**
(`LimitExceeded`) rather than dropped once per member, because such a message could not be
enqueued for anybody, and one copy of it is made for the whole fan-out: every member's queue
holds a reference to the same `Bytes`, so *n* members cost one copy rather than *n*.

*`every_member_sees_every_other_members_message`,
`a_sender_never_receives_its_own_message`,
`a_dead_member_is_dropped_and_the_others_continue`,
`a_slow_member_is_dropped_and_counted_rather_than_blocking`,
`a_body_above_the_fan_out_budget_is_refused_rather_than_dropped_for_everyone`,
`a_slow_member_is_bounded_in_bytes_before_its_message_count`.*

### 6.4 RADIO/DISH

Lossy fan-out of **segments** ([decisions/0034](decisions/0034-late-is-lost.md) §4.6). A
segment is a unit whose parts may depend on each other and on nothing earlier — a voice frame, a
raw preview frame, a video GOP — and it is the join point. `Listener::radio` binds,
`Runtime::dish` connects.

| | `Radio` | `Dish` |
| --- | --- | --- |
| Compatible peer | `Dish` | `Radio` |
| Direction | binds | connects |
| Send/receive pattern | `segment(topic, terms)` → `write` / `write_layer(layer, chunk)` → `finish_layer` / `finish`; `datagram(topic, bytes)` for a one-packet segment | `join(filter, terms)` with `JoinTerms { max_age, max_layer }` / `leave(filter)`, then `recv` → `Received::Segment` (a streamed `IncomingTransfer`, one per layer) or `Received::Datagram { topic, segment, payload }` |
| Topics | Pub/Sub's namespace and filter grammar, unchanged — ZeroMQ's "group" is weida's topic | |
| Outgoing routing | every dish whose filter matches, one copy each; the dish set is fixed when a segment opens | — |
| Carrier | a stream segment: one uni DATA stream per `(dish, layer)`, opened on the layer's first chunk; key `13` numbers the segment and key `14` names the layer above 0; a datagram segment: a flow per `(dish, topic)`, opened lazily on the connection the join arrived on, its payload `varint segment` + bytes; a refused flow waits out a backoff of 100 ms, doubling to 30 s, before the next one opens, and the datagrams meanwhile are counted drops | |
| Supersession | opening segment *n+1* on a topic resets at once every layer of *n* whose writer had not finished it, or whose queued chunks the transport does not take at once, and every layer above it; a finished layer gets the larger of `rtt + 50 ms + rtt * bytes / cwnd` and the settle time its connection learned (mean plus four deviations of FIN-to-acknowledgement times), and is reset only if still unacknowledged then ([decisions/0037](decisions/0037-layered-segments.md) §4.5); `SegmentTerms::follows_upstream` treats a relay's copy as finished until a write has to wait (§4.11) | a layer not newer than what this connection delivered on that path and topic is discarded on arrival, so segments never go backwards |
| Expiry | the smaller of the sender's `SegmentTerms::max_age` and the dish's `max_age`, on the radio's clock from the segment's open; the smallest among the dish's matching filters applies | states `max_age` when it joins (SUBSCRIBE key `2`) |
| Layers | a segment carries layers `0..=15` in dependency order; a cap means a layer above it is never opened for that dish, and is not a drop | states `max_layer` when it joins (SUBSCRIBE key `3`); with several matching filters the largest cap applies, and a filter without one means no cap; freshness is per layer |
| Backpressure | never blocks; the cut rule: a copy without room for a chunk of layer *k* cuts its layers above *k* first, whose queued bytes return to its budget, then *k* and above; a copy that loses layer 0 loses the segment | a full receive queue discards on arrival and never blocks the connection |
| Delivery | `BestEffort`; a lost segment is a gap in the numbers the dish sees, counted per topic and cause in `dropped_on`: budget, queue, no parked connection, superseded, expired, too large, no datagrams, and `layers_cut` for a copy that kept layer 0; `Radio::dish_drops` counts the same per joined dish connection | `stale()` and `overflow()` count the dish's own discards |
| From a dialling peer | `Peer::segment(topic, terms)` (§5): one copy toward the bound path, numbered per connection, `Peer::segment_drops` | the bound side receives `Incoming::Stream` with `topic`, `segment` and `layer` set |
| Late joiner | receives the next segment; nothing is retained for it | |

Five rules a caller can get wrong. **Supersession holds nothing**: it discards bytes already in
flight, which is why it is not the coalescer [0016](decisions/0016-conflation.md) §4.2 refused.
**A late joiner waits for the next segment**: a segment is the only point a decoder can start
from, so the application chooses how often a join point comes. **Topics under one radio share
one connection per dish**, so a datagram outranks a stream segment there and every topic's
layer 0 outranks every topic's enhancement layer, by the layer-major priority of §1.12; topics
on different paths cannot be ordered at all. **A
datagram segment is never turned into a stream segment**: a dish whose connection did not agree
the datagram capability loses it with the cause named. **Joins are subscriptions**: they ride
SUBSCRIBE and UNSUBSCRIBE, count against `max_subscriptions`, and a redialling dish re-sends
them. Segments never carry DATA key `6`, so `PerProducer(reassemble)` passes them straight
through and never holds one for a predecessor that was superseded on purpose.

**Who may join is the radio's application's to decide**
([decisions/0035](decisions/0035-keys-proved-not-judged.md) §4.3). `Radio::with_admission`
installs a policy that sees every join as a `Join { peer, peer_chain, filter }` before it is
recorded, repeats included. A refused join records nothing, reserves no `max_subscriptions`
slot and closes nothing: the dish is told nothing, which is exactly what
[0017](decisions/0017-subscription-verdict.md) §4.1 defines silence to mean — "received and not
refused" is indistinguishable, by design, from every copy declined. Installing a policy screens
the joins already recorded, so there is no window between `Listener::radio` and the policy.
`Radio::evict(peer, filter)` withdraws a recorded join, by the exact filter string, from every
connection of that peer and frees its slot, again without telling the dish; a room whose
membership changed is one call. An anonymous dish can be admitted or refused but not evicted by
name, which is one more reason an SFU's binding requires `ClientTrust::AnyKey`.

*`a_stalled_dish_loses_old_segments_while_a_fast_one_gets_every_one`,
`a_healthy_dish_behind_a_slow_path_gets_back_to_back_segments_whole`,
`a_segment_finished_right_before_its_successor_still_arrives_whole`,
`a_segment_whose_finish_found_a_full_queue_is_cut_at_once`,
`a_segment_following_upstream_keeps_its_copy_while_its_successor_opens`,
`a_segment_following_upstream_still_resets_a_stalled_dish`,
`a_dish_short_of_budget_keeps_layer_zero_whole_while_upper_layers_are_cut`,
`a_dish_capped_at_layer_zero_is_never_sent_layer_one`,
`a_layer_cut_also_cuts_every_higher_layer_of_that_segment`,
`a_dish_redialled_to_a_restarted_radio_takes_its_numbers_from_zero`,
`dish_drops_names_the_dish_that_lost_and_forgets_it_when_it_leaves`,
`a_segment_supersedes_only_its_own_topic`,
`a_joiner_receives_the_next_segment_and_nothing_earlier`,
`a_dish_max_age_expires_its_copy`, `a_datagram_segment_reaches_every_joined_dish`,
`a_dish_without_datagrams_is_a_named_drop`,
`a_datagram_larger_than_the_dish_carries_is_counted_too_large`,
`admission_refuses_a_join_silently_and_records_nothing`,
`evict_withdraws_a_join_and_frees_its_subscription_slot`,
`installing_an_admission_screens_joins_already_recorded`
(`crates/weida/tests/radio.rs`).*

### 6.5 SWARM

Many sources, one verified blob ([decisions/0040](decisions/0040-swarm-pattern.md),
[decisions/0041](decisions/0041-swarm-sessions.md)). A blob is named by the BLAKE3 hash of its
bytes and cut into pieces of a power-of-two size (1 KiB to 16 MiB, `PieceSize`); the pieces'
hashes, a `PieceList`, merge to that root, so every piece checks against the name alone and
arrives in any order from any holder. `Listener::seeder` binds the holder's side and
`Runtime::swarm` dials the fetcher's: `Swarm::fetch` opens **one session per connected
seeder**, asks each what it holds (HAVE), then orders from each pieces the others are not
already sending, and yields a `Piece` only after it verified. Both types need the cargo feature
`swarm`, which links `blake3`. From one seeder a fetch behaves like a normal download with its
pieces in any order: the orders are pipelined as BitTorrent's are (BEP 3), so the seeder always
has the next answer to send.

The pattern owns the transport and the check and nothing else. **The application owns storage**
(a `Seeder` answers from whatever the application keeps; a `Fetch` hands the pieces over and
remembers none), **admission** (every session reaches the application once as a `SeedSession`
carrying `blob()`, `meta()` and `connection()`, and it serves or `decline`s it) and
**discovery** (which peers to `connect`, which `Blob`s exist; connecting a seeder while a fetch
runs is how a new holder joins it). Connections run one way: a `Swarm` dials and orders, a
`Seeder` binds and answers, and nothing is fetched back over a connection the peer opened. A
swarm path is its own connection beside any other path to the same peer, so bulk pieces do not
share flow-control windows with a voice connection
([decisions/0002](decisions/0002-control-and-bulk-separation.md)); a swarm dials it with
receive windows of at least 32 MiB per stream and 64 MiB per connection, because quinn fixes
them at dial.

| | `Swarm` | `Seeder` |
| --- | --- | --- |
| Compatible peer | `Seeder`, or a `Replier` that speaks the version-1 ask below | `Swarm`, or a `Requester` that sends either wire below |
| Direction | connects, and **accumulates** peers on purpose | binds |
| Send/receive pattern | `connect` per seeder, then `fetch(blob, have, options)` and `Fetch::next` until `None` | `accept` → `SeedSession` → admit by `blob()` / `meta()` once → `next` → `answer_list` / `answer_have` / `answer_piece`, repeated until `next` is `None`; or `decline` |
| Incoming routing | the session's reply, answers in the order of the orders; a verified piece is yielded once, the first copy to verify | fair, bounded queue per path (`endpoint_queue`), the open read and decoded before the application sees it |
| Outgoing routing | **one session per (fetch, seeder), on the connection of the seeder it targets** — not Req/Rep's round-robin pick; pieces are claimed per seeder from what its HAVE says it holds | the session's reply |
| Action with no peer | `Error::NotConnected` from `Fetch::next`, immediately for a swarm that never connected; connecting a seeder and calling `next` again resumes | `accept` waits |
| Transport | one exchange per session on `Route::Request`, the route a replier uses; orders on the request body, framed answers on the reply body | the same |
| Ordering | none: pieces arrive in any order, and `Piece::index` says which | — |
| Delivery signal | a `Piece` the fetch verified against the root. Nothing weaker is yielded | none per answer: `answer_*` returns once its bytes are written into the stream |
| Backpressure | a **credit of `per_peer` (W, default 8) orders per seeder**, each held from the moment it is written until the caller takes its piece from `next`; of those W the fetch uses a depth that follows the seeder's rate (two at first, one more per piece in slow start, then 3 s of the measured rate in pieces, libtorrent's queue time); `max_inflight` (32) orders over all seeders | the seeder reads an order only when the application calls `next`, with no timeout: a busy seeder simply calls it later, the orders wait in the stream and QUIC flow control holds the rest. At most `MAX_PENDING_SESSIONS` (64) sessions read or waiting for `accept`; one more waits for a place, in the route's queue and on its stream |
| Cancellation | drop the `Fetch`: every session and ask is aborted and nothing more is ordered. Once every piece is delivered, each session is finished instead, and its seeder's `next` returns `None` | drop the `SeedSession`: a reply not yet opened is ERROR `NO_REPLY`, an open one is reset |

End-to-end, a caller that stops taking pieces stops the sender: no new order goes out, the
seeder's `next` waits, it sends nothing, and what is in flight is at most W pieces per seeder
and `max_inflight` pieces in all.

Three rules a caller can get wrong. **Verification is the fetch's, not the seeder's**:
`answer_piece` checks the length and not the bytes; the fetch checks every piece and drops a
seeder that sends a wrong one, so a bad piece never reaches the application's storage through a
fetch. **A `Blob` of one piece has no list and an empty one has no
pieces**: the first is checked against the root directly, the second yields `None` at once,
with no seeder. And **a fetch orders only what the caller lacks**: `have` says which pieces
are already stored, `Fetch::delivered` is the running map, `Fetch::in_flight` the pieces
ordered and not yet taken (a progress display's third state), and `Fetch::list` hands out the
verified list so a resumed fetch skips LIST.

The session, big-endian ([decisions/0041](decisions/0041-swarm-sessions.md) §4.1):

| unit | bytes | layout |
| --- | --- | --- |
| open (start of the request body) | 48 | `u8` version = 2, `u8` piece log2 (10..=24), `[u8; 32]` root, `u64` size, 6 zero bytes |
| order (request body, after the open) | 5 | `u8` 1 = LIST, 2 = HAVE, 3 = PIECE, `u32` index (0 for LIST and HAVE) |
| answer head (reply body) | 9 | `u8` 0 = DECLINE, 1 = LIST, 2 = HAVE, 3 = PIECE, `u32` index (0 except PIECE), `u32` length (0 for DECLINE) |

Each answer head is followed by exactly `length` bytes: LIST `count × 32` (the chaining
values), HAVE `ceil(count / 8)` (bit `i` is byte `i / 8`, mask `1 << (i % 8)`, padding bits
zero), PIECE the piece's span (the last piece is short). An open of another version, with
non-zero padding, a piece size out of range, size 0 or a `Blob` that `Blob::new` refuses is
refused with `Rejected` before the application sees it; an order of an unknown kind, with an
index on LIST or HAVE, LIST for a blob of at most one piece, or PIECE past the end refuses the
session. A DECLINE ends it.

The version-1 ask ([decisions/0040](decisions/0040-swarm-pattern.md) §4.2) is still served,
as a session of one order, and is what a fetch falls back to toward a seeder that refuses a
session. One exchange per ask:

| offset | field |
| --- | --- |
| 0 | `u8` version = 1 |
| 1 | `u8` ask: 1 = LIST, 2 = HAVE, 3 = PIECE |
| 2 | `u8` piece log2 (10..=24) |
| 3 | `[u8; 32]` root |
| 35 | `u64` size |
| 43 | `u32` index (PIECE only) |

LIST and HAVE are 43 bytes, PIECE 47; the reply is the raw answer of the same exact length. A
version-1 seeder reads an ask under that 47-byte cap, so a 48-byte open overflows it at once and
is refused with `Rejected`: that refusal before any answer head is how a fetch tells a
version-1 seeder, and a session never refuses a well-formed open with `Rejected`.

Failure modes, from the fetching side unless the row says otherwise:

| Event | Result | Pinned by |
| --- | --- | --- |
| A seeder sends a piece that does not verify | the seeder is dropped **for this fetch**; its session is aborted, its orders given back, and the blob completes from the rest | *`a_seeder_that_sends_a_wrong_piece_is_dropped_and_the_blob_completes`* |
| A seeder sends a list that does not merge to the root | the same; no PIECE is asked of it | *`a_list_that_does_not_hash_to_the_root_drops_the_seeder`* |
| A seeder declines (DECLINE), answers with a head that does not match the order, or a length other than the known one | the same: the fetch reads an error or a wrong head, and does not retry the seeder | no test of its own: one path with the two rows above (`read_answer` in `swarm/gather.rs`) |
| A seeder takes an order and never answers | the answer at the head of the line is due `stall + (9 + len) / min_rate` after its order was written or the answer before it was read, whichever is later; past that the seeder is dropped and its pieces are ordered elsewhere, so the last piece is not held up | *`a_silent_seeder_does_not_hold_up_the_last_piece`* |
| A seeder speaks only the version-1 ask | it refuses the session open with `Rejected`; the fetch asks it one exchange per ask from then on, under the same credit | *`a_seeder_that_speaks_only_version_1_still_serves_a_fetch`* |
| The caller stops taking pieces | after W orders per seeder nothing more is ordered; one taken piece is one more order | *`a_caller_that_stops_taking_pieces_stops_the_seeder`* |
| The connection to a seeder is lost mid-session (`ConnectionLost`, `Indeterminate`) | the seeder **keeps its place**, its orders are given back, and its next order opens a new session once a redial brings it back; an order only reads, so ordering again is safe | no test of its own |
| No order in flight and no seeder left that has not failed this fetch | `Err(Error::NotConnected)` from `next`; connecting a seeder and calling `next` again resumes | *`a_fetch_without_a_usable_seeder_is_not_connected`* |
| A seeder joins during the fetch | it is asked HAVE at the next `next`, and takes pieces | *`a_seeder_that_connects_during_a_fetch_joins_it`* |
| The `Fetch` is dropped | every session and ask is aborted and nothing more is ordered | *`a_dropped_fetch_asks_nothing_more`* |
| A session open or an ask body is malformed (seeder's side; the fetching side reads `Error::Rejected`) | refused before `accept`; the application never sees it | *`a_malformed_ask_is_refused_before_accept`* |
| The application answers with the wrong kind or shape (seeder's side) | the session is refused (a version-1 asker reads an error and **no bytes**), and `answer_*` returns `Error::Protocol` | *`an_answer_of_the_wrong_length_is_refused_and_reported`* |
| More than `MAX_PENDING_SESSIONS` sessions wait (seeder's side) | the next waits for a place in the route's queue and on its stream; its fetch's first answer is still due `stall + (9 + len) / min_rate` after its order was written, so a seeder busy longer than that is dropped by that fetch | *`a_busy_seeder_makes_a_session_wait_rather_than_drop_it`* |

A seeder that is still fetching serves what it has: it feeds the pieces its `Fetch` yields into
the storage its `Seeder` answers from, answers HAVE with the bits it holds so far, and a seeder
with nothing to give is asked HAVE again every `have_refresh` (5 s), which is how a node that is
still filling serves another
(*`a_seeder_that_is_still_fetching_serves_what_it_has`*). The other tests of the fetch are
*`a_blob_is_gathered_from_seeders_holding_different_halves`*,
*`a_fetch_asks_only_for_pieces_the_caller_lacks`* and
*`a_blob_of_one_piece_needs_no_list_and_an_empty_blob_no_seeder`*
(`crates/weida/tests/swarm.rs`, run with `cargo test -p weida --features swarm`); the scheduler's
credit and depth are unit-tested in `crates/weida/src/swarm/schedule.rs`, and
`crates/weida/tests/swarm_large.rs` (ignored) measures 256 MiB from one seeder against a
normal download on the same 27 ms path.

---

## 7. Choosing

| You need | Use | Because |
| --- | --- | --- |
| an answer per message | Req/Rep | the reply is the strongest signal weida has (§2) |
| work distributed over workers, nothing lost under load | Push/Pull | backpressure is `Block`; the only losses are explicit refusals and `Indeterminate` after a loss (§3) |
| the newest of a feed, many readers, laggards may lose | Pub/Sub | drops are per subscriber and counted (§4) |
| ordered messages to one peer | a raw stream | QUIC orders bytes within a stream and nowhere else (§1.7, §5) |
| a signal larger than `subscriber_buffer_bytes` to many readers | `Publisher::open`, which streams one per subscriber (§4.1) | `publish` takes a whole `Bytes` and refuses it; `open` bounds a chunk instead |
| proof the peer's application acted | Req/Rep, or an L2 broker (`weida-broker`) | a transport receipt never says that (§1.2); `Accepted`/`Stored`/`Processed` belong to a hop that owns the message, and a broker reports `Accepted` as a cursor (§1.11) |
| to know whether the receiver accepted it | Req/Rep | a one-way refusal can lose the race with the receipt and then reaches no observer (§1.6, [decisions/0005](decisions/0005-refusal-race.md) §4.3) |
| exactly one peer, both directions, no selection policy | PAIR | a second peer is refused rather than silently preferred (§6.1) |
| an answer from everyone who is there, within a deadline | SURVEY | the deadline, the partial result and the late-reply rule are what an application otherwise rebuilds wrongly (§6.2) |
| every member to hear every other member | BUS | *n* × (*n* − 1) deliveries, no relay, counted drops (§6.3) |
| a reliable verdict without an exchange | any pattern, plus `TransferMeta::with_report` | a cursor rides a stream of its own, so a Push transfer stays one unidirectional stream and still gets an answer (§1.11, [decisions/0024](decisions/0024-three-families-one-back-channel.md) §4.4a) |
| how far the far end got, not merely whether it finished | cursors, via `OutgoingTransfer::cursors` | a whole-message verdict tells an interrupted sender nothing; an absolute offset tells it a number (§1.11) |
| voice, or any unit worthless once late, to one peer | a datagram flow | never sent late, never waits, loses its oldest at a bound, and says what it lost (§5) |
| a live feed to many, where a laggard should lose the old unit rather than the new one | RADIO/DISH | supersession and per-dish expiry reset what is late, a layered unit loses its upper layers first, and a joiner starts at the next segment (§6.4) |
| one blob from many holders, each piece checked against the blob's name | SWARM (feature `swarm`) | a fetch keeps one session of pipelined orders with every connected seeder, claims each missing piece from one holder, and yields it only after it verified against the BLAKE3 root; a caller that stops taking pieces stops the seeders; storage and discovery stay the application's (§6.5) |
