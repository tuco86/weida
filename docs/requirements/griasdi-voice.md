# Lossy real-time transport over weida (griasdi voice)

## Context

griasdi is a community chat and voice application in which every
byte travels over weida: text, presence, file transfer and voice. Users are
public keys; servers are independent nodes that relay, host communities and act
as the selective forwarding unit (SFU) for voice. A server forwards media it
cannot read: every frame is end-to-end encrypted by griasdi (MLS-derived keys,
SFrame, RFC 9605) before weida sees it.

Everything except voice fits weida's streams. Voice does not: a 20 ms audio
frame that arrives after its playout time is worthless, so retransmitting it is
pure cost, and a lost packet on a stream holds every later frame behind it until
the retransmission lands. This document states what griasdi needs from a lossy
transport in weida, what weida provides today, and a proposal. It is a
requirements document, not a decision; the decision belongs in a note under
`docs/decisions/`.

Everything under "What weida provides today" was read from the weida source at
commit `5a20f15` (`0.1.0-alpha.2`); file and line references are given so a
reader can check rather than trust. Everything under "Proposal" is design, not
fact.

**Answered by [decisions/0034](../decisions/0034-late-is-lost.md) (accepted; shipped in 270d588).** The flow of
"Proposal" becomes an L0 carrier with frame kind `7` and capability code `1`, answering open
question 1 for a frame kind and open question 4 with a redial that re-opens flows; beyond this
document, the note adds RADIO/DISH so that the SFU's fan-out has a name in weida's vocabulary
while staying the application's code. It also verifies the one unverified line below: quinn's
defaults do leave datagrams enabled on every weida connection today (B-279). Open questions 2
and 3 and the adjacent requirements stay open.

## What griasdi needs

| Need | Shape | Volume |
| --- | --- | --- |
| Voice from a client to its server | one flow per active microphone per session | 50 datagrams/s, 80-250 bytes each |
| Voice from the server to a client | one flow per remote speaker the client hears | 50 datagrams/s per speaker; 1-3 concurrent speakers typical, tens possible |
| Voice between two clients directly (later) | the same flows on a direct connection | same |
| Speaking indicator, levels | carried inside the voice datagram header, griasdi's business | none extra |

Payload arithmetic for one datagram, 20 ms Opus frame: 60-100 bytes of Opus at
24-32 kbit/s, plus Deep Redundancy (DRED) data in the Opus padding, plus the
SFrame header and authentication tag (up to 17 + 16 bytes), plus griasdi's own
header (sequence number, timestamp; ~10 bytes). That is well under 300 bytes,
far below the "little over a kilobyte" minimum quinn guarantees when the peer's
limit is large (`docs/decisions/0024-three-families-one-back-channel.md:60-62`).
Fragmentation is not needed and not wanted.

Latency target: mouth-to-ear under 150 ms one-way (ITU-T G.114). Capture,
encoding, the jitter buffer and playout consume most of it; the transport's
share must be the network path and nothing queued behind it.

## What weida provides today

Verified at `5a20f15`.

- **Datagrams are an explicit non-feature.** "QUIC datagrams. Only streams are
  used." (`docs/PROTOCOL.md:1468`). Capability negotiation exists (HELLO keys
  `3` and `4`) with no capability code assigned (`docs/PROTOCOL.md:1466-1467`).
- **The datagram analysis already exists.** Decision 0024 quotes quinn 0.11.11:
  datagrams are unreliable and unordered, must fit one QUIC packet, and
  "previously queued datagrams which are still unsent may be discarded to make
  space for this datagram, in order of oldest to newest"; `max_datagram_size()`
  is `None` when the peer or the local side disabled them
  (`docs/decisions/0024-three-families-one-back-channel.md:55-63`). 0024 rejected
  datagrams for cursors because cursors must arrive (`:198-204`). Voice is the
  opposite case: oldest-first discard is exactly the policy a stale audio frame
  wants.
- **Prior art is already in the research sheets.** Zenoh sends best-effort
  traffic as QUIC DATAGRAM (`docs/research/prior-art.md:1263`); moq-lite maps a
  datagram to one single-frame group of at most 1200 bytes
  (`docs/research/prior-art.md:1267`); RFC 9221 is summarised in
  `docs/research/quic-standards.md:323-327`.
- **weida does not configure datagram buffers.** `transport_config` sets
  windows, idle timeout and keep-alive and nothing about datagrams
  (`crates/weida/src/tls.rs:426-444`). Whether quinn's defaults leave datagrams
  enabled on weida connections is not verified here.
- **Keep-alive is sent by the dialling side only**, default 10 s, idle timeout
  30 s (`crates/weida/src/tls.rs:423-443`, `crates/core/src/limits.rs:38-40,115-116`).
  That keeps a client's NAT binding to its server open, which a server-to-client
  voice flow depends on.
- **Each dialled path gets its own bulk connection** (decision 0002 §6 item 2,
  `docs/decisions/0002-control-and-bulk-separation.md:281-285`; the per-path half
  is implemented per `docs/decisions/README.md`). A voice path would therefore
  get its own flow-control and congestion domain, isolated from a file upload on
  another path.
- **Server-originated traffic rides the connection the registration arrived on**
  (decision 0011). A client behind NAT is reachable from its server only that
  way, so server-to-client voice must follow the same rule.
- **The proved peer is per connection.** `IncomingMeta::peer` carries the
  fingerprint the TLS handshake proved (`crates/weida/src/transfer.rs:162-168`);
  a datagram on the same connection has the same sender.

## Proposal

### A flow is registered once, reliably; its frames are datagrams

A datagram cannot carry an endpoint path per frame, and should not: the path,
the sender's metadata and the authorization verdict are decided once. So a flow
is opened on a stream -- the way a CURSOR stream names the payload stream it
reports on (0024 §4.4) -- and carries a sender-allocated flow id. Every datagram
of that flow is `varint flow id` followed by opaque payload. Closing the flow is
reliable too; so is the end of the connection.

```
Sender                                            Receiver
  |  FLOW_OPEN {flow id, path, meta}  (stream)        |
  |-------------------------------------------------->|  authorize (peer, path)
  |  accepted / REJECTED / UNKNOWN_ENDPOINT           |
  |<--------------------------------------------------|
  |  DATAGRAM {flow id} payload   x 50/s              |
  |- - - - - - - - - - - - - - - - - - - - - - - - - >|
  |  FLOW_CLOSE {flow id}  (stream, or conn end)      |
  |-------------------------------------------------->|
```

Authorization stays 0015's shape: the acceptor decides on `(proved peer, path)`
when the flow opens, with the refusals that already exist. A datagram naming an
unknown or closed flow id is dropped and counted, never an error that closes the
connection.

### Both directions, on whichever connection exists

A flow can be opened by either side of a connection. The server opens flows
toward a client on the connection that client dialled (0011); on a direct
client-to-client connection, either peer opens flows toward the other.

### Sending never waits and never queues stale data

`send` is synchronous and non-blocking, like `Publisher::publish`. It returns
whether the datagram was handed to the connection or refused (`TooLarge`,
`FlowClosed`, `Unsupported`). The send buffer is bounded so that queued data
stays within about two frames per flow (40 ms); beyond that the oldest unsent
datagram is discarded, which is quinn's policy already. Discards are counted per
flow.

### Receiving never blocks the connection

Incoming datagrams are demultiplexed to a per-flow receiver with a small bounded
queue. A slow consumer loses its oldest datagrams; it never stalls other flows
or the connection's datagram reader. Overflow is counted per flow.

### No silent downgrade

When datagrams are unavailable (`max_datagram_size()` is `None`, or smaller than
a payload), opening or sending fails with a named error. weida does not fall
back to streams on its own: a requested guarantee "MUST NEVER be silently
weakened" (`docs/GUARANTEES.md` §4, as cited in 0002 §6). griasdi decides
whether to fall back.

### Enough statistics for adaptation

griasdi sizes its jitter buffer and picks Opus bitrate and DRED depth from
network conditions. It needs, per connection: smoothed RTT and its variation,
lost-packet counts, and the current `max_datagram_size`; per flow: sent,
discarded-before-send, received, and dropped-on-overflow counts. quinn exposes
the connection half (`Connection::stats`, `rtt`); weida should pass it through
rather than hide it.

## What weida would need, in priority order

1. **Datagram flows at L0**: registration on a stream, a varint flow id on each
   datagram, both directions, per-flow bounded receive queues, non-blocking
   send, named errors, per-flow counters. This is a primitive beside streams,
   not a pattern; griasdi does not need RADIO/DISH-style group semantics from
   weida, because fan-out is the SFU's job and weida forwards on nobody's behalf
   (`crates/weida/src/endpoint.rs:1352`).
2. **Negotiation**: datagram support declared in HELLO -- the first assigned
   capability code (`docs/PROTOCOL.md:1466-1467`) -- plus the QUIC transport
   parameter `max_datagram_frame_size`, which weida must then set explicitly.
3. **Configuration**: datagram send and receive buffer sizes in the `Limits`
   profile of the connection that carries the flow; the keep-alive already
   exists.
4. **Statistics**: the per-connection and per-flow numbers above.

## Adjacent requirements from the same application

Not part of the lossy transport, but they decide whether voice can run at all
in griasdi's topology, so they are recorded here.

- **Client admission decided by the application.** A griasdi server admits
  users it has never seen: any key that proves possession in the TLS handshake,
  with authorization afterwards on `(proved peer, path)`. Today
  `ServerTls::require_client` takes a `TrustSource` (`crates/weida/src/config.rs:477-497`),
  a source holds only a `Trust` of pins and anchors
  (`crates/weida/src/identity.rs:564-605`), and an empty trust is refused
  (`crates/weida/src/tls.rs:551-555`). griasdi needs a client-verification hook
  that receives the presented chain and returns accept (reporting the
  fingerprint as today) or reject. The defaults stay as they are. **Answered by
  [decisions/0035](../decisions/0035-keys-proved-not-judged.md):** a binding accepts any
  proved key with `ServerTls::require_client(ClientTrust::AnyKey)` and the presented chain
  reaches the application as `IncomingMeta::peer_chain`; the handshake-time hook that may
  reject is deferred there as `ClientPolicy`.
- **Device certificates.** A griasdi user is a root key; each device has its own
  key, certified by the root. The minimum weida needs is the hook above; griasdi
  can bind device to user with an application exchange on an `/auth` path (0015
  option B). If the hook also sees the full chain, the binding can happen in the
  handshake instead.
- **Direct connections through NAT (later).** A server coordinates hole
  punching between two clients. That needs: (a) dialling and listening on one
  UDP socket -- today dialling uses a separate client endpoint on an ephemeral
  port (`crates/weida/src/pool.rs:338-345`) and each binding its own server
  endpoint (`crates/weida/src/listener.rs:324-341`), so the NAT mapping a server
  observes belongs to a socket that cannot accept; (b) the observed remote
  address of a connection, available to the application -- `IncomingMeta` has
  no address (`crates/weida/src/transfer.rs:145-204`), and the listener sees it
  only internally (`crates/weida/src/listener.rs:981`); (c) dialling a peer from
  that shared socket while the peer dials back, with the role (client or
  server) fixed by comparing fingerprints so exactly one connection survives.
  None of this needs a different QUIC implementation.

## Rejected alternatives

- **One uni stream per frame.** It is weida's existing shape and works without
  wire changes, but every frame pays a stream open and a DATA header, and a lost
  packet is retransmitted after the frame is already useless. moq-lite, which is
  stream-first, uses a datagram for exactly this case: a group of one frame
  (`docs/research/prior-art.md:1267`).
- **One long-lived stream per flow.** Head-of-line blocking: a single lost
  packet holds every later frame for at least one RTT.
- **A separate RTP/UDP socket beside weida.** A second identity, a second
  encryption layer, a second NAT mapping, and no proved peer.
- **WebRTC or MoQ crates.** A second transport stack and a second identity model
  for one feature; griasdi has no browser client that would justify it.

## Open questions

1. **Frame kind or reserved path for flow registration?** A new frame kind is
   fatal to a v0 peer rather than additive (0017); a registration riding a DATA
   stream on a reserved path needs no new kind. The capability code decides who
   may use either.
2. **DSCP marking.** Separate connections allow marking voice as Expedited
   Forwarding (0002 cites "separate connections and separate DSCPs"). A shared
   socket for hole punching (above) makes per-connection marking a per-packet
   question.
3. **Congestion control for media connections.** quinn offers several
   controllers; whether a voice-only connection wants a different one than bulk
   is unmeasured.
4. **Flow ids across reconnects.** 0031's transparent redial re-registers
   subscriptions; whether it should also re-open flows, or report their loss
   and let the application re-open them, is undecided.
