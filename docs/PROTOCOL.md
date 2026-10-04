# weida wire protocol, version 0

## Status and scope

Wire protocol version: `0`.
Status: **experimental**. Per master doc §15, `0.x` protocol versions are explicitly
experimental and breaking changes are permitted. Implementations MUST NOT assume any
compatibility guarantee across `0.x` releases. The library version (`0.1.0` and its
pre-releases) and the wire protocol version (`0`) are independent.

This version is a deliberate break from earlier `0.x` drafts: ACK and CANCEL frames are
gone, the DATA header lost `transfer_id`, `role`, `correlation_id` and `ack_mode`, and
Req/Rep moved onto bidirectional streams. Old and new binaries do not interoperate. The
ALPN token stays `weida/0` because `0.x` carries no compatibility promise to preserve.

This document is the single normative source for the weida wire format. Anything not stated
here is unspecified in v0. Implementations MUST NOT infer wire behaviour from
implementation source; where this document and an implementation disagree, this document is
authoritative and the implementation is a defect.

**Wire version 0 is unchanged by the decided-but-unbuilt parts of this document.** Sections
marked *spec ahead of code* — §2.5, HELLO keys `5` and `6` (§6.1), the guarantee set of §6.5,
the profiles of §10.1 — specify behaviour that the accepted decision notes in
[decisions/](decisions/) have settled and that no v0 implementation yet produces. They are
written here rather than left to the implementation for two reasons: a key number and an
encoding must be fixed once, by the specification, before two implementations can disagree
about them; and every one of them is already load-bearing for an adapter mapping document.
They change nothing for a v0 peer, because every one of them is either an optional key that an
encoder omits and a decoder skips (§5), or a statement about how many connections a pair
holds. The version stays `0`.

DATA keys `6` and `7` (§6.2) are a narrower case and no longer only on paper: the codec
encodes and decodes both and §8 pins their vectors, while no v0 *sender* sets either, so they
are implemented and unused rather than unimplemented.

One change was not merely additive: the topic filter grammar of §6.4 replaced the byte-prefix
match of earlier drafts, so the same SUBSCRIBE bytes can select a different set of topics than
they used to ([decisions/0007](decisions/0007-topic-namespace.md) §4.2). The implementation
follows it as of B-020 — the matcher is a segment walk and an invalid filter is rejected at
the codec boundary — so this is history rather than a pending defect. `0.x` carries no
compatibility promise, which is what made fixing the grammar before the adapters exist
cheaper than fixing it after.

Related documents: [ARCHITECTURE.md](ARCHITECTURE.md),
[GUARANTEES.md](GUARANTEES.md), [FAILURE_MODEL.md](FAILURE_MODEL.md),
[INVARIANTS.md](INVARIANTS.md), [IMPLEMENTATION.md](IMPLEMENTATION.md), and the accepted
decision notes in [decisions/](decisions/README.md), which are normative for the sections
marked *spec ahead of code*.

---

## 1. Notation

- The keywords MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be interpreted as
  described in RFC 2119.
- Byte values are written in hexadecimal, e.g. `0x57`.
- Byte sequences in code blocks are space-separated hexadecimal octets in wire order.
- "QUIC varint" means the variable-length integer encoding of RFC 9000 §16.
- "uni stream" means a QUIC unidirectional stream; "bidi stream" means a QUIC bidirectional
  stream.
- An **exchange** is one bidi stream. Its **initiating half** is the direction opened by the
  requester; its **reply half** is the other direction.
- "FIN" means the QUIC stream final offset, i.e. clean end of stream.
- "tstr" and "uint" are the CBOR (RFC 8949) major types 3 (text string) and 0 (unsigned
  integer) respectively.
- "peer" means the other side of a connection, regardless of which side opened it. On the
  network transport that is a QUIC connection; on a local transport it is the socket, pipe or
  channel of §2.1.

---

## 2. Connection establishment

### 2.1 Transport

**The network transport is QUIC.** The reference implementation uses `quinn`.

The TLS ALPN token MUST be exactly `weida/0`. A peer MUST offer this token and MUST NOT
accept a connection that negotiated any other token. An ALPN mismatch MUST fail the TLS
handshake; it is not signalled at the weida protocol layer.

**A local transport carries the same protocol without TLS**
([decisions/0010](decisions/0010-local-transport.md)). In-process channels, `AF_UNIX`
`SOCK_STREAM` sockets and Windows named pipes in byte mode carry the same frames (§4), the
same headers (§6), the same HELLO exchange (§2.2) and the same negotiation (§2.3). Three
differences, and only three:

- **There is no TLS and therefore no ALPN.** The version fence moves to where the real work
  was always done: the `versions` intersection of §2.3. A local peer MUST still send HELLO and
  MUST still fail the connection with `NEGOTIATION_FAILED` on an empty intersection.
- **The OS connection is the stream.** A local transport has no stream multiplexing, so one
  transfer is one local connection and a transfer's lifetime is that connection's
  [0010 §4.2]. Nothing in §3-§9 changes: a preamble and a header still open every stream,
  because the stream *is* the connection. There is consequently no per-connection window, so
  the shared-window coupling of §10 does not arise locally.

  One consequence is worth stating rather than deriving. A local connection is bidirectional
  by nature, so it carries no equivalent of QUIC's stream kind, and the dispatch of §9.4 —
  today a function of the stream kind *and* the addressed path — is locally a function of the
  **path alone**: the pattern registered there says whether a reply is expected. A replier path
  answers on the same connection; a puller or publisher path never writes back, and the
  initiator MUST NOT wait for a reply on it. The mismatch case is unchanged and already
  specified: a transfer addressed to a path whose pattern cannot serve it is refused with
  `UNSUPPORTED` (§9.4).
- **The peer is proved by the kernel, not by a key.** `SO_PEERCRED` on Linux, `LOCAL_PEERCRED`
  on macOS — which carries no PID — and on Windows the client's token SID through
  `ImpersonateNamedPipeClient` on the accepting side and the pipe object's owner SID on the
  dialling side; an in-process peer has no identity at all, because there is nobody else to
  prove [0010 §4.4]. A PID is an observation and MUST NOT be authorized on.

A local transport is named by its own URL scheme, never by `weida://`
([ARCHITECTURE.md](ARCHITECTURE.md) §3): the transport is part of the address, and there is no
automatic fallback from one to another, because that would change who may connect and what
proves them without saying so [0010 §4.6], [0010 §4.8].

**How local connections become a peer.** An accepted socket cannot be dialled
back, so the connections of one peer are grouped rather than multiplexed
([decisions/0012](decisions/0012-local-connection-grouping.md)). On a socket
transport every connection begins with a **local preamble**, which is framing
of the transport and appears on no QUIC connection:

```text
0x01                     control connection; the server answers with
                         16 bytes of group token, then HELLO both ways
0x02 <16-byte token>     transfer connection; one weida stream
0x03 <16-byte token>     reverse connection; parked, for a stream the
                         accepting side opens toward this peer
```

A transfer connection is admitted only if the token names a live control
connection **and** the kernel credentials of the new connection match that
control connection's — the uid or SID always, the pid where the platform
reports one [0012 §4.2]. An unbound connection is dispatched nowhere. The
token binds connections and resumes nothing: no subscriptions, no sequence
position, no dedup window, and it is meaningless once the control connection
closes, which is why it is not the session state §11 excludes [0012 §4.5].

A server has no way to *dial* a peer that dialled it, so a stream toward such
a peer rides a connection that peer parked in advance: a subscriber over a
socket transport opens `Limits::max_parked_reverse` reverse connections and
parks a replacement as each is spent [0012 §4.4]. A parked connection is
admitted by the same rule as a transfer connection, and counts against
`max_local_streams` on both sides. Whatever the server writes on one is a
DATA frame dispatched by the path in its header, exactly as an accepted
unidirectional stream is on QUIC; there is no reverse-specific frame.

**A publisher that finds no parked connection drops that copy and counts
it** — the answer fan-out already gives an exhausted subscriber budget (§6.4,
[GUARANTEES.md](GUARANTEES.md) §6), and never a stall of the publisher. A
subscriber configured to park nothing therefore cannot receive fan-out at
all, and is refused when it subscribes rather than left silent.

**A named pipe carries each stream in chunks**, because a pipe has no half-close: a
socket ends a stream by shutting down one direction, and a pipe handle closes whole,
taking the reply direction with it. After the local preamble, everything written on a
pipe connection is a sequence of chunks, and the end of a stream is a chunk:

```text
0x00 <u32 length, little-endian> <bytes>   payload
0x01                                       FIN: the stream is complete
0x02 <u64 code, little-endian>             RESET: the stream was abandoned, with why
```

A reader MUST treat FIN as the end of the stream, MUST report RESET with its code as it
would report `RESET_STREAM` on QUIC, and MUST refuse a chunk kind it does not know. A
writer MUST NOT write a FIN or RESET inside a payload chunk. The chunk layer carries what
the socket cannot — a reset code — and lacks what the socket has: there is no reader-side
refusal, because the only direction a reader could signal on is the one the reply owns. A
pipe reader that stops therefore reads and discards the rest of the stream, and the writer
learns of the refusal from the reply or from nothing, which is the socket's own named loss
of [0012 §4.7] moved from the write to the receipt.

**A FLOW stream carries its datagrams itself on a local transport** (§6.8,
[decisions/0034](decisions/0034-late-is-lost.md) §4.4). There is no DATAGRAM frame locally, so
after its header a FLOW stream carries zero or more records until FIN:

```text
<varint length> <length bytes>             one datagram, length <= 1200 (LOCAL_MAX_DATAGRAM)
```

The varint is §6.9's QUIC variable-length integer. A record longer than 1200 bytes MUST close
the connection with `PROTOCOL_VIOLATION`. On a named pipe the records ride inside payload
chunks like any other stream bytes, and a record boundary need not coincide with a chunk
boundary.

### 2.2 HELLO exchange

Immediately after the QUIC handshake completes, each side MUST open exactly one uni stream
carrying a single HELLO frame and MUST then FIN that stream.

A side MUST NOT process any frame other than HELLO before it has processed the peer's HELLO
frame.

QUIC streams are unordered relative to one another: a DATA stream — uni or bidi — MAY be
accepted before the peer's HELLO stream. Receiving DATA before HELLO is **not** a protocol
violation. The receiver MUST park such a stream (retain it, unread beyond its preamble,
without emitting any error) until the peer HELLO has been processed and negotiation has
succeeded, and MUST then process the parked stream normally. In the reference
implementation the parked task awaits a `tokio::sync::watch<Option<Agreed>>`.

If the peer HELLO has not arrived within `hello_timeout` (default 10 s, see §10), the local
side MUST close the connection with application error code `NEGOTIATION_FAILED`.

### 2.3 Negotiation

Negotiation is a pure function of the two HELLO frames. The reference signature is:

```text
negotiate(ours: &Hello, theirs: &Hello) -> Result<Agreed, NegotiateError>
```

The algorithm is:

1. Compute the intersection of `ours.versions` and `theirs.versions`.
2. If the intersection is empty, negotiation fails.
3. The effective version is the maximum element of the intersection.
4. If any code in `theirs.required_capabilities` is outside the local supported capability
   set (`ours.capabilities`), negotiation fails. The only code defined is `1` `datagram`
   (§6.1), so a peer that requires anything else, or requires `1` of a side that did not list
   it, fails negotiation.
5. Compute the effective guarantee set, dimension by dimension, as the **weaker** of
   `ours.guarantees_offered` and `theirs.guarantees_offered`, an absent declaration meaning
   `core` (§6.1, §6.5). For a dimension whose levels are not ordered — `backpressure`, and
   the two independent axes of a durability level (§6.5) — "weaker" is not defined, so the
   two declarations MUST be equal or negotiation fails.
6. If the effective set does not reach `theirs.guarantees_required` on **every** dimension,
   negotiation fails. There is no downgrade path: a level the peer does not offer is a failed
   handshake, never a quieter success
   ([decisions/0006](decisions/0006-guarantee-sets.md) §4.4). Because a v0 peer declares
   nothing, it offers and requires `core`, and the step is a no-op between two v0 peers.
7. On success the result is

```text
Agreed {
    version: u64,                 // the effective version from step 3
    send_max_header_bytes: u64,   // = theirs.max_header_bytes
    guarantees: GuaranteeSet,     // the effective set from step 5; spec ahead of code
    datagrams: bool,              // both HELLOs listed capability code 1 (§6.1)
}
```

`send_max_header_bytes` is the **peer's** advertised `max_header_bytes`; it bounds the
headers this side may send. The local receive limit remains the local
`limits.max_header_bytes` and is not affected by the peer's advertisement.

Negotiation is still a pure function of the two HELLO frames: the effective set is computed,
not agreed in a second round trip, and both sides compute the same one from the same two
frames.

Negotiation failure MUST close the connection with `CONNECTION_CLOSE`, application error
code `NEGOTIATION_FAILED`. All in-flight local operations on that connection then resolve
per the connection-loss rules in [FAILURE_MODEL.md](FAILURE_MODEL.md).

### 2.4 Timers

| Timer | Value | Applies to |
| --- | --- | --- |
| Keep-alive | 10 s | client side only |
| Idle timeout | 30 s | both sides |
| `hello_timeout` | 10 s | both sides, until peer HELLO is processed |

### 2.5 Connections per peer

A peer pair holds more than one QUIC connection
([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2-§6.3):

- one **connection per dialled endpoint path**, carrying that path's transfers and the control
  frames that name it. **Implemented**;
- one **control** connection per peer, for peer-scoped frames.
  **Not implemented, and parked** ([decisions/0011](decisions/0011-answered-where-it-arrived.md)
  §4.3): no frame that v0 has or reserves is peer-scoped enough to need it.

The point is head-of-line coupling: a QUIC connection's receive window is shared, so one slow
reader can stall every writer on that connection ([PATTERNS.md](PATTERNS.md) §1.3). One
connection per path means two paths share no window and therefore cannot stall each other,
which is asserted end to end by `a_stalled_path_does_not_stall_another_path` in
`crates/weida/tests/streams.rs`. Each connection is an ordinary weida connection: it performs
its own HELLO (§2.2) and its own negotiation (§2.3), and nothing on the wire distinguishes one
from another.

**Traffic a side originates for a peer's registration MUST be written on the connection that
carried the registration** ([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.1).
No header field selects a connection. A publisher therefore writes fan-out on the connection
the SUBSCRIBE arrived on, which is what every protocol in the catalogue does — EMQX sends a
server-initiated PUBLISH "on the stream where it received that topic's subscription", NATS
answers a `SUB` on its own connection, a RabbitMQ consumer tag is channel-scoped.

**A frame that names a path is path-scoped and rides that path's connection; a frame that names
only the peer is peer-scoped** [0011 §4.2]. SUBSCRIBE and UNSUBSCRIBE name an endpoint path
(§6.4), so they ride that path's connection and are **not** control-tier traffic: moving them
to a per-peer connection would separate a subscription from the only route to its subscriber.
The credit frame of §6.6 is granted per subscription, so it is path-scoped for the same
reason [0011 §4.3].

What remains coupled is stated rather than hidden: since a path's connection carries both its
payload and its subscriptions, an endpoint that publishes *and* subscribes on one path can
queue its own SUBSCRIBE behind its own payload. A pure subscriber writes nothing there and a
pure publisher sends no SUBSCRIBE, so neither is affected [0011 §4.4].

**What binds a peer's connections is the proved fingerprint, and nothing else**
([decisions/0008](decisions/0008-session-identity.md) §4.2). No HELLO field names a peer's
other connections. A connection belongs to the peer that proved the same fingerprint under the
same authority and terms; a connection whose fingerprint differs is a different peer and MUST
NOT be bound to it — a dialling side MUST refuse it rather than serve a transfer on it. Two
connections that proved no fingerprint at all — an anonymous client — MUST NOT be treated as
one peer, which is why `max_connections_per_peer` (§10) counts only connections that proved an
identity and a deployment that wants that bound requires a client identity.

Nothing is retained between connections: there is no session, no subscription resumption and
no sequence resumption at wire version 0 (§11). A peer that reconnects is the same *peer* and
starts again.

---

## 3. Stream framing

Every stream, of either kind and in both directions, MUST begin with the following
preamble. On a bidi stream this holds for **each half independently**: the initiating half
opens with a DATA preamble and the reply half opens with a DATA or ERROR preamble.

```text
[0x57][kind: u8][header_len: QUIC varint][CBOR header: exactly header_len bytes]
```

- `0x57` is the stream magic byte (ASCII `W`).
- `kind` is a single octet, see §4.
- `header_len` is a QUIC varint (RFC 9000 §16). Any of the four varint encoding lengths
  MUST be accepted for a given value; a non-minimal encoding is **not** an error at this
  layer.
- The CBOR header occupies exactly `header_len` bytes.

### 3.1 Header length check

If `header_len` is greater than the local `limits.max_header_bytes` (default `16384`), the
receiver MUST close the connection with `PROTOCOL_VIOLATION` **before allocating** any
buffer for the header. The check MUST precede allocation; allocating and then rejecting is
a defect because it lets a remote peer choose the allocation size.

### 3.2 Conditions that MUST close the connection with PROTOCOL_VIOLATION

- Magic byte not equal to `0x57`.
- Unknown `kind` value, i.e. anything in `8..=255`. Kinds `5` and `6` were reserved for the
  L2 credit frame and the cursor stream and are now defined (§6.6, §6.7): the reservation was
  a promise not to reuse the number, and the promise was kept when the number was spent.
- `header_len` greater than the local `limits.max_header_bytes` (§3.1).
- CBOR parse failure of the header.
- A required key missing from the header.
- A duplicate key in the header map.
- A map key that is not a CBOR unsigned integer.
- A value whose CBOR type does not match the type required for its key.
- A text or byte string longer than the cap defined for its key.
- A frame kind used on a stream kind where §4 does not permit it.
- A DATA frame without `endpoint` on a stream that initiates a transfer (§6.2).
- A guarantee set whose value or dimension combination §6.5 forbids.
- A FLOW stream (§6.8) from a peer whose HELLO did not list capability code `1` in key `3`.
- Any byte after a FLOW header on a QUIC stream, a FLOW naming a flow id that is still live on
  that connection and direction, or a local FLOW record longer than 1200 bytes (§2.1).

These rules are symmetric: they apply identically to streams received by the QUIC client
and by the QUIC server. All network input is hostile (master doc §81 rule 18); neither role
is trusted.

Where a condition below is specified to produce an ERROR frame or a `STOP_SENDING` rather
than a connection close, that more specific rule governs. Everything in this section is a
connection-fatal framing violation.

---

## 4. Frame kinds

| Kind | Name | Stream shape | Permitted on |
| --- | --- | --- | --- |
| `0` | HELLO | header only, FIN directly after the header | uni |
| `1` | DATA | header followed by opaque payload bytes until FIN | uni, and both halves of a bidi stream |
| `2` | ERROR | header only, FIN directly after the header | **reply half of a bidi stream only** |
| `3` | SUBSCRIBE | header only, FIN directly after the header | uni |
| `4` | UNSUBSCRIBE | header only, FIN directly after the header | uni |
| `5` | CREDIT | header only, FIN directly after the header | uni |
| `6` | CURSOR | head frame, then (level, offset) records until FIN | uni |
| `7` | FLOW | header, then nothing on QUIC (datagrams on local transports, §2.1); FIN closes the flow | uni |

Kinds `8..=255` are reserved and MUST close the connection with `PROTOCOL_VIOLATION`. This
is not a forward-compatibility hook: a receiver cannot know whether an unknown stream kind
carries payload it would have to drain.

**Kind `5` is known as of B-202**, and it is the L2 credit of
[decisions/0003](decisions/0003-credit-unit.md) §4.2: a consumer's absolute delivery limit
for one subscription (§6.6). A peer that does not run a queue **ignores** it rather than
closing the connection — credit is idempotent state, not a transfer, and the frame has no
reply half to refuse on — which is the same position SUBSCRIBE takes for a path no publisher
has claimed.

**Kind `6` is known as of B-233**, and it is the cursor stream of
[decisions/0023](decisions/0023-completion-is-a-cursor.md) and
[decisions/0024](decisions/0024-three-families-one-back-channel.md) §4.4: a report about one
payload stream, on a unidirectional stream of its own. It is the one kind that is neither
header-only nor payload-carrying — a head frame naming the report, then `(level, offset)`
records until FIN (§6.7) — and it **never shares a stream with payload**, which is what
makes a report add nothing to a pattern's topology. A peer that ordered no report has no
state for the id a CURSOR stream names and resets that stream with `CANCELED` rather than
closing the connection: nothing was allocated for an id it never handed out.

**Kind `7` is known as of B-281**, and it is the datagram flow of
[decisions/0034](decisions/0034-late-is-lost.md) §4.2: a registration whose stream is the
flow's lifetime and whose units travel as QUIC DATAGRAM frames (§6.8, §6.9). It is sent only
to a peer that listed capability code `1` (§6.1), so a peer that never listed the code never
sees one. On QUIC a FLOW stream carries its header and then nothing until FIN; a byte after
the header is a `PROTOCOL_VIOLATION`. Kind `8` is now the first free number.

HELLO, ERROR, SUBSCRIBE, UNSUBSCRIBE and CREDIT are header-only frames: the sender MUST FIN
the stream immediately after the header. Receiver handling of bytes appearing after the
header on a header-only stream is unspecified in v0; a receiver MAY ignore them and MAY stop
reading the stream after the header.

For DATA, everything after the header up to FIN is opaque user payload. The protocol
imposes no internal framing on the payload: there is no `[length][payload]` chunking inside
a DATA stream.

### 4.1 Which frame may open which stream

- A **uni stream** MUST open with HELLO, DATA, SUBSCRIBE, UNSUBSCRIBE, CREDIT, CURSOR or FLOW. An
  ERROR frame on a uni stream is a violation (§3.2): an ERROR is the alternative to a reply,
  and it therefore has meaning only where a reply would have gone.
- A **bidi stream** MUST open with DATA on its initiating half. Any other kind there is a
  violation.
- The **reply half** of a bidi stream MUST carry either DATA or ERROR, exactly one frame,
  followed by FIN. **A report does not change this.** A cursor rides a uni stream of its own,
  so ordering one adds no frame to a reply half and no half to an exchange.

---

## 5. CBOR header encoding rules

Encoder requirements:

- The header MUST be a CBOR **definite-length** map. Indefinite-length maps MUST NOT be
  emitted and MUST be rejected on decode.
- Map keys MUST be CBOR unsigned integers.
- Keys MUST be emitted in ascending numeric order.
- Integers MUST use minimal-length encoding.
- Absent optional keys MUST be omitted entirely. A key MUST NOT be present with a null or
  placeholder value to mean "absent".
- There are no keyed defaults in v0. Every DATA field is genuinely optional and is either
  written or omitted; nothing is encoded to restate a default.

Decoder requirements:

- Unknown unsigned-integer keys MUST be skipped, preserving forward compatibility
  (master doc §15: unknown optional fields must be ignorable).
- Skipping MUST be performed by an iterative, depth-limited skip with `max_depth = 8`;
  exceeding the depth limit is a CBOR parse failure. A recursive skip is a stack-exhaustion
  vector on hostile input and MUST NOT be used.
- Duplicate keys MUST be rejected (§3.2), for every key including extension keys the
  decoder skips.
- Keys MUST be strictly ascending on decode as well as on encode. Rejecting a key that is
  not greater than its predecessor makes duplicate detection complete in constant space; a
  set of seen extension keys would itself be remote-controlled allocation (§10).
- Non-uint keys MUST be rejected (§3.2).
- Indefinite-length byte strings, text strings, arrays and maps MUST be rejected anywhere
  in a header, not only at the top level.
- CBOR tags, half-precision floats and simple values other than `false`, `true`, `null` and
  `undefined` MUST be rejected. Extensions carry plain data items only.
- Bytes remaining after the header map MUST be rejected: `header_len` describes exactly one
  CBOR map.
- A list-valued field MUST NOT declare more than 64 items, and a decoder MUST NOT reserve
  memory from a declared length before that check. Without this bound a peer could pin
  `max_concurrent_uni_streams` worth of large lists by opening many HELLO streams.
- A **nested** definite-length map is permitted only where a key's type says so — in v0 that
  is the guarantee set of §6.5 and nothing else — and every rule above applies to it
  unchanged: uint keys, strictly ascending, no duplicates, unknown keys skipped. Nesting is
  one level deep by specification; the `max_depth = 8` skip bound is what makes an
  unspecified deeper nesting harmless rather than fatal to the decoder.

The key space `0..=63` is reserved for this specification. Extensions MUST use keys `64`
and above.

---

## 6. Frame headers

### 6.1 HELLO (kind 0)

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `[uint]` | `versions` | yes | — | wire protocol versions supported by the sender; v0 sends `[0]` |
| `1` | `uint` | `max_header_bytes` | yes | — | largest header size the sender is willing to receive |
| `2` | `uint` | `max_transfers` | yes | — | maximum concurrent inbound transfers; **advisory in v0**, not enforced |
| `3` | `[uint]` | `capabilities` | yes | — | optional capability codes supported (table below); a side that enables nothing sends `[]` |
| `4` | `[uint]` | `required_capabilities` | yes | — | capability codes the sender requires the peer to support; `[]` unless a profile requires one |
| `5` | `map` | `guarantees_offered` | no | §6.5 | guarantee set the sender can honour; **optional, absent in v0** |
| `6` | `map` | `guarantees_required` | no | §6.5 | guarantee set the sender requires of the peer; **optional, absent in v0** |

Keys `0` to `4` are required. A missing one of them is a framing violation per §3.2.

Capability codes:

| Code | Name | Meaning |
| --- | --- | --- |
| `1` | `datagram` | the sender reads QUIC DATAGRAM frames and FLOW streams (§6.8, §6.9) |

A FLOW stream or a DATAGRAM frame MUST NOT be sent unless both HELLOs listed code `1` in key
`3`. A FLOW stream from a peer whose HELLO did not list it closes the connection with
`PROTOCOL_VIOLATION` (§3.2). A side lists code `1` exactly when its profile enables flows
(`datagram_receive_bytes > 0`, §10); on QUIC the same setting decides whether the transport
parameter `max_datagram_frame_size` is advertised at all
([decisions/0034](decisions/0034-late-is-lost.md) §4.5).

**Keys `5` and `6` are specified ahead of code** (§11) and are the only optional HELLO keys.
They declare guarantee sets per [decisions/0006](decisions/0006-guarantee-sets.md) §4.4,
encoded as §6.5. An absent key means the default set `core` — which is exactly what every v0
peer offers and requires — so a v0 HELLO is unchanged on the wire and a v0 decoder skips both
keys by the rule of §5. `guarantees_required` MUST be a subset-or-equal of the sender's own
`guarantees_offered` on every dimension: requiring what you cannot yourself honour is a
configuration error, not a negotiation position.

### 6.2 DATA (kind 1)

| Key | CBOR type | Name | Required at the decoder | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `tstr` | `endpoint` | no | 512 B | endpoint path being addressed |
| `1` | `uint` | `content_len` | no | — | payload length in bytes; **advisory**, not enforced |
| `2` | `tstr` | `content_type` | no | 256 B | opaque media type label |
| `3` | `tstr` | `traceparent` | no | 128 B | W3C Trace Context `traceparent` |
| `4` | `tstr` | `tracestate` | no | 512 B | W3C Trace Context `tracestate`, opaque passthrough |
| `5` | `tstr` | `topic` | no | 256 B | Pub/Sub topic; opaque bytes, selected by the filter grammar of §6.4 |
| `6` | `uint` | `sequence` | no | — | per-producer sequence number; **written by a publisher whose connection negotiated `PerProducer` ordering**, absent under `core` |
| `7` | `bstr` | `producer` | no | exactly 32 B | producer identity, the raw digest; coded, **written by no v0 sender** |
| `8` | `uint` | `achieved` | no | one of §6.5's `acknowledgement` values | the completion level the sender **achieved** for the message it is answering; the L2 publisher confirm |
| `9` | `uint` | `report_id` | no | — | identifier of the CURSOR stream that will report on this transfer (§6.7) |
| `10` | `array` of `uint` | `report` | no | 16 items, strictly ascending | levels the sender **orders** a report for; an order, not a guarantee |
| `11` | `uint` | `report_mode` | no | one of §6.2's `report_mode` values | `0` `progress` (default, never written), `1` `final-only` |
| `12` | `uint` | `delivery_attempt` | no | — | how often an L2 queue has handed this message out, first attempt included; **absent means `1`**. A count rather than a flag: a repeat is visible, and a poison message is countable. **Reserved and written by nobody today** — the number is spent so it cannot be spent twice, and B-203 is the slice that writes it |
| `13` | `uint` | `segment` | no | — | segment number per (sender, path, topic), from 0; **written by a radio, or by a dialling peer's `Peer::segment` toward a bound path** ([decisions/0034](decisions/0034-late-is-lost.md) §4.6) |

**Every key is optional at the decoder, and that is deliberate.** A decoder sees a byte
slice, not a stream: it cannot tell an initiating half from a reply half, so it cannot
enforce a rule that depends on which one it is looking at. The conditional requirement lives
one layer up, at dispatch, which does know:

- On a stream that **initiates** a transfer — any uni DATA stream, and the initiating half of
  a bidi stream — `endpoint` is REQUIRED. Its absence MUST close the connection with
  `PROTOCOL_VIOLATION` (§3.2). There is nowhere to route the stream and nothing meaningful
  to answer with.
- On the **reply half** of a bidi stream, `endpoint` carries no meaning and MUST be ignored
  if present. The minimal reply header is therefore the empty CBOR map `A0`.

There is no `transfer_id`, no `role` and no `correlation_id`. The stream carries all three:
its kind says whether a reply is expected, its direction says which side initiated, and its
identity is the correlation. Nothing on the wire names an exchange.

`topic` is meaningful on the fan-out copies a publisher emits (§9.5) **and** on a message sent
to a queue, whose consumers each select on it with the filter grammar of §6.4 (§9.4,
[decisions/0018](decisions/0018-minimal-broker.md) §4.5). It is opaque bytes: weida never
parses it, and no character in it is special.

`achieved` is the L2 confirm, and it appears on a **reply half** only. A broker that has taken
responsibility for a producer's message answers the exchange with DATA carrying
`achieved = 2` (`Accepted`) and a FIN, no payload: an exchange already has a reply half, so a
publisher confirm needs no frame kind of its own
([decisions/0018](decisions/0018-minimal-broker.md) §4.6). Three rules bind it.

- It is **achieved, never requested.** A level a peer *wants* is negotiated in HELLO (§6.5)
  and the connection fails if it cannot be reached; this key reports what happened to one
  message.
- It is a statement about **one hop**, and it is never relayed. The producer's confirm says
  nothing about what a consumer later does with the message
  ([GUARANTEES.md](GUARANTEES.md) §2).
- An **absent** key is not `acknowledgement = 0`. Absence is the v0 case: this hop claims
  nothing beyond the transport receipt QUIC already gave. `0` is an explicit statement that
  nothing is reported, which only a sender that negotiated it writes.

A value this version does not define is a protocol violation at the decoder, not an unknown
key to skip: the number decides what a producer believes about its message, so reading an
unknown one as the weakest level would invent a claim. This is the one exception to §5's
skip-the-unknown rule, and it is narrow — the *key* may be skipped by a decoder that does not
know it; the *value* may not be guessed by one that does.

`content_len` is advisory: the receiver MUST NOT reject a payload for disagreeing with it,
and MUST NOT size an allocation from it.

`traceparent` and `tracestate` carry W3C Trace Context. `tracestate` is opaque to weida and
MUST be forwarded unmodified where trace context is propagated.

**Key `3` is optional, and this runtime writes it only on propagation.** A `traceparent` is
written exactly when the sending application supplied a context — from an inbound transfer's
metadata, or minted deliberately — and nothing in the library mints one on an application's
behalf. The reason is stated where it belongs, in
[decisions/0028](decisions/0028-trace-propagation-is-the-callers.md): a minted root is not a
safe default but a fabricated fact, and it cost a fixed 60 bytes of every frame of every
pattern (measured: a 64-byte push is 76 B without it and 136 B with it,
[IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-246). A peer that writes the key on every frame
is **conformant**; a peer that treats an absent key as an error is **not**, and a receiver
reads an absent value exactly as it reads a malformed one — no trace context for this transfer.

**Key `6` is written, and key `7` is not** — a distinction worth stating, because both were
"coded but unused" in earlier versions of this section and only one still is.

`sequence` is set by a **publisher whose connection negotiated `PerProducer` ordering**: the
number is assigned once per published message, before fan-out, so every subscriber's copy carries
the same one and a copy a subscriber lost shows up as a hole in its own sequence
(`crates/weida/src/pubsub.rs`, both the whole-message and the streamed path). Under `core` — what
every peer that declares nothing offers and requires — no sender sets it, so a `core` connection
sees the key never. That is not "unused": it is a key whose presence is decided by negotiation.

`producer` is genuinely unwritten by anything in this repository, and deliberately: the receiver
already knows the sending peer's proved fingerprint from the handshake, so the key names a
producer only where it is *not* the connection peer — a relay, or a name an L2 subscription
supplies ([decisions/0008](decisions/0008-session-identity.md) §4.4). `weida-protocol` encodes and
decodes both and §8 pins their bytes, so a second implementation has one definition rather than
one per implementation — and a decoder that meets either accepts it, while a decoder that meets
an unknown key still skips it under §5.

`sequence` is a monotonically increasing `uint` scoped to (producer, endpoint or topic)
([decisions/0001](decisions/0001-sequence-field.md) §7.1). It is not a transfer identifier and
it does not correlate anything: an exchange is still correlated by its stream (§9.1). Its
purpose is ordering and gap detection, and a receiver that has not negotiated a `PerProducer`
level (§6.5) MUST ignore it.

`producer` names the producer when the producer is **not** the connection peer — a relay, an
L2 hop forwarding another producer's output, or a stable name supplied by an L2 subscription.
It is **absent** in the default case, because the receiver already knows the sending peer's
proved fingerprint from the handshake and a claimed name could not be trusted anyway
([decisions/0008](decisions/0008-session-identity.md) §4.4). Where present it is the raw
32-byte digest as a `bstr`; the `sha256:<64 hex>` spelling is presentation only and MUST NOT
appear on the wire. The length is exact rather than merely capped — a `bstr` of any other
length is a framing violation (§3.2), because a truncated digest names nobody — and the
absent default is also what keeps a connection that negotiated no ordering from paying for
one: the measured cost of writing the text form was +80 B and −9 % of the message rate at a
64-byte payload ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-009).

`sequence` and `producer` are independent: either may appear without the other. Ordering and
deduplication are separate guarantee dimensions and neither implies the other [0001 §7.1].

**Keys `9`, `10` and `11` order a report** — the cursor of
[decisions/0023](decisions/0023-completion-is-a-cursor.md). They say: report these levels
for *this* transfer, on a CURSOR stream (§6.7) carrying this id. They add no half to the
stream they ride and no frame to a reply, which is what lets a Push producer get a verdict
without an exchange ([0024](decisions/0024-three-families-one-back-channel.md) §4.4a).

- **The level space is open.** Values `0..=15` are weida's own ladder, the `acknowledgement`
  levels of §6.5; values `16` and above are application stages weida carries and orders but
  never interprets. An undefined value **below** `16` is a protocol violation, not an
  application level: the reserved range is where a later version of this specification will
  put its own stages, and reading one as an opaque stage now would spend the number twice.
- **Keys `9` and `10` travel together.** `report` without `report_id` names no stream, and
  `report_id` without `report` names no levels; both MUST close the connection with
  `PROTOCOL_VIOLATION`.
- **The order is canonical.** The array MUST be strictly ascending, which makes a duplicate
  a violation for free and makes two peers ordering the same levels send the same bytes. It
  MUST carry at most 16 entries, which is more than the defined space below the application
  floor and therefore constrains nothing a sender legitimately wants.
- **`report_mode = 0` is never written.** `progress` is what an absent key already says, on
  §6.5's rule for `core` levels.
- **An order is not a guarantee.** A receiver that cannot reach an ordered level simply does
  not report it, and the transfer does not fail for it. A level a peer MUST reach is the
  negotiated `acknowledgement` dimension of §6.5 instead
  ([decisions/0006](decisions/0006-guarantee-sets.md) §4.4).
- **`report_id` is scoped to the connection and to the direction that allocated it.** A peer
  reports only on transfers it received, so the two directions' id spaces cannot collide and
  no shared numbering rule is needed. `0` is never allocated, so an id that was never handed
  out cannot masquerade as one.
- The reply half of an exchange may order its own report. That is how the **reply
  direction** is reported without a second header field: the responder orders levels in its
  own reply header, on its own id.
- **`delivery_attempt` is written only by a queue, and only from `2` upwards.** Absent means
  the first attempt, so a first delivery costs zero bytes for it. It counts what an L2 queue
  has handed out for one message — a consumer that died after processing and before reporting
  is the case that makes it non-zero — and it carries **no** claim that the earlier attempts
  failed, because the queue does not know that either. A consumer MUST treat any value above
  `1` as "this may be a duplicate" and nothing more
  ([decisions/0029](decisions/0029-a-report-is-relayed-a-certificate-is-not.md) §4.6).
- **A relayed report reuses the reader's own `report_id`.** A queue that re-emits a consumer's
  `(level, offset)` toward the producer writes it on the producer's cursor stream, under the id
  **the producer** allocated — a record is copied, never forwarded, because an id is scoped to
  the connection and the allocating side. The pair itself is unchanged, which is honest only
  while the body is byte-identical across the hop
  ([decisions/0029](decisions/0029-a-report-is-relayed-a-certificate-is-not.md) §4.3).

### 6.3 ERROR (kind 2)

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `uint` | `code` | yes | — | error code, see below |
| `1` | `tstr` | `message` | no | 1024 B | human-readable detail, not machine-interpreted |

An ERROR frame is legal **only on the reply half of a bidi stream**, where it is the
alternative to a reply (§4.1). It therefore needs no reference to what it answers: the
stream is the reference. The `re` key of earlier drafts is gone.

Error codes:

| Code | Name | Meaning |
| --- | --- | --- |
| `1` | `UNKNOWN_ENDPOINT` | no endpoint is registered for the requested path |
| `2` | `REJECTED` | the receiving side declined to accept the transfer |
| `3` | `UNSUPPORTED` | the endpoint exists but does not serve this stream kind |
| `4` | `INTERNAL` | the receiving side failed internally |
| `5` | `NO_REPLY` | the request was accepted but no reply will be produced |

An ERROR frame resolves the exchange as failed. Codes other than `1..=5` are unspecified in
v0; because an ERROR resolves the exchange regardless of code, a receiver SHOULD treat an
unrecognised code as a generic failure of that exchange rather than as a framing violation.

### 6.4 SUBSCRIBE (kind 3) and UNSUBSCRIBE (kind 4)

Both kinds carry the same header. Only the kind byte distinguishes registering interest
from withdrawing it.

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `tstr` | `endpoint` | yes | 512 B | publisher endpoint path the subscription applies to |
| `1` | `tstr` | `filter` | yes | 256 B | topic filter; the grammar below. The empty string matches every topic |
| `2` | `uint` | `max_age_ms` | no | — | the dish's latency budget; meaningful on a RADIO path only ([decisions/0034](decisions/0034-late-is-lost.md) §4.6). Encoded only when present |

Keys `0` and `1` are required. `filter` is required even when empty: an absent key and an empty
string would otherwise be indistinguishable, and the empty filter is the "every topic"
subscription.

Receiver behaviour:

- The filter is a **segmented pattern**, not a byte prefix
  ([decisions/0007](decisions/0007-topic-namespace.md) §4.2). A topic and a filter are byte
  strings split on `.` (U+002E, one byte) into segments, and the filter matches a topic when
  every segment matches:
  - `*` alone in a segment matches exactly one whole segment. `*` MUST occupy a whole
    segment; a segment that merely contains it (`a*b`) is a grammar violation.
  - `#` alone in the **final** segment matches zero or more trailing segments, so `a.#`
    matches `a`, `a.b` and `a.b.c`. `#` MUST be the last segment and MUST be alone in it.
  - Every other byte is literal, compared byte for byte. There is no escape character, no
    normalization and no case folding. Empty segments are permitted and match only empty
    segments.
  - The empty filter matches every topic and is equivalent to the single-segment filter `#`.
  - A **`topic` is never a pattern**: `*` and `#` are special only inside a filter, so a
    published topic containing them is matched literally.
  Matching is a single left-to-right walk over both strings — `#` only in final position is
  what removes backtracking — and allocates nothing, so it is bounded by the 256 B filter cap.
- A filter that violates the grammar (`*` not alone in its segment, `#` not final or not
  alone) MUST close the connection with `PROTOCOL_VIOLATION`. Like an oversized filter, it is
  malformed content on a uni stream with no reply half to answer on, so the connection is the
  only granularity available; and unlike `max_subscriptions`, it is not an overload but a
  peer sending something the grammar does not permit.
- SUBSCRIBE for a filter already held on that connection and path is idempotent.
- UNSUBSCRIBE naming an unknown filter, path or connection MUST be ignored.
- SUBSCRIBE for a path no publisher has registered yet MUST still be recorded: a subscriber
  may connect before the publisher exists, and the subscription is bounded by
  `max_subscriptions` either way.
- When accepting the frame would take the connection past `max_subscriptions` filters
  summed over all paths, the receiver MUST close the connection with `LIMIT_EXCEEDED`. A
  SUBSCRIBE arrives on a uni stream and so has no reply half to carry an ERROR; the
  connection is the only granularity available.
- All subscriptions held by a connection are dropped when that connection closes.
- Like every non-HELLO frame, a SUBSCRIBE that arrives before the peer's HELLO is parked
  until negotiation completes; it is not a violation (§2.2).
- A peer that registers no publishers MAY ignore these frames. Subscribing to a side that
  publishes nothing is useless, not hostile.

### 6.5 Guarantee set encoding (spec ahead of code)

A **guarantee set** is the unit of configuration and of negotiation: one level per guarantee
dimension of [GUARANTEES.md](GUARANTEES.md) §3, carried as one CBOR map
([decisions/0006](decisions/0006-guarantee-sets.md) §4.1). It appears only in HELLO keys `5`
and `6` (§6.1); no DATA frame carries a guarantee level, because a set is a property of the
connection and not of a message.

| Key | CBOR type | Name | Values |
| --- | --- | --- | --- |
| `0` | `uint` | `delivery` | `0` BestEffort, `1` AtMostOnce, `2` AtLeastOnce |
| `1` | `uint` | `acknowledgement` | `0` None, `1` TransportReceipt, `2` Accepted, `3` Stored, `4` Replicated, `5` Processed |
| `2` | `uint` | `durability` | `0` Written, `1` Flushed; permitted only with `acknowledgement` `3` or `4` |
| `3` | `uint` | `replicas` | the replica count `n`, leader included; permitted only with `acknowledgement` `4`, and MUST be ≥ 2 |
| `4` | `uint` | `ordering` | `0` None, `1` PerProducer detect, `2` PerProducer reassemble, `3` PerKey, `4` Total |
| `5` | `uint` | `deduplication` | `0` None, `1` Bounded, `2` Durable |
| `6` | `uint` | `dedup_window_ms` | window length in milliseconds; REQUIRED with `deduplication` `1`, forbidden otherwise |
| `7` | `uint` | `backpressure` | `0` Block, `1` Reject, `2` Drop, `3` Spill, `4` Coalesce |
| `8` | `uint` | `producer_naming` | `0` fingerprint, `1` stable; how a sequenced transfer's producer is named ([decisions/0001](decisions/0001-sequence-field.md) §7.3, [decisions/0008](decisions/0008-session-identity.md) §4.3) |
| `9` | `uint` | `control_isolated` | `0` no, `1` yes: control traffic cannot stall behind bulk ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.1) |

Rules:

- **An absent key means the `core` level for that dimension**: `delivery` BestEffort,
  `acknowledgement` TransportReceipt, `ordering` None, `deduplication` None, `backpressure`
  Block, `producer_naming` fingerprint, `control_isolated` no [0006 §4.2]. An empty map is
  therefore exactly `core`, and so is an absent HELLO key. An encoder MUST NOT write a
  dimension left at its `core` level, so a `core` declaration is indistinguishable from no
  declaration on the wire.
- The map is bounded by `max_header_bytes` like every other header, and every value is a
  `uint`, so a guarantee set introduces no new allocation a peer can influence.
- A key whose value is outside the list above, or a dimension combination the table forbids
  (`durability` without `Stored`/`Replicated`, `replicas` without `Replicated`, `replicas`
  of `1`, a missing `dedup_window_ms` under `Bounded`) is a framing violation (§3.2). The
  levels reserved for the L2 broker — `acknowledgement` `2` to `5` — are legal to *declare*
  and impossible to honour **in the v0 core**, so a peer that requires one gets a failed
  negotiation (§2.3), never a quieter success. `2` (`Accepted`) is the first to become
  honourable: a process running `weida-broker` reaches it for a message it has taken into a
  queue, and reports it in DATA key `8` (§6.2, B-201). `3` and `4` stay declare-only, with
  their `durability` and `replicas` axes unchanged, until a store and a replica set exist to
  make them true.
- **`backpressure` `4` (Coalesce) with `ordering` `2` (PerProducer reassemble) is forbidden**,
  because it asks for two incompatible things at once: reassembly holds an arrival until its
  predecessors arrive, and a conflating hop is the reason some of them never will
  ([decisions/0016](decisions/0016-conflation.md) §4.5). Either declaration alone is legal;
  the combination is a framing violation like the others above. A conflating hop under
  `ordering` `1` (detect) is coherent and is the intended pairing: the copy is numbered before
  it is discarded, so what a subscriber sees is a gap rather than a renumbering.
- **If `backpressure` `4` is ever honoured, the coalescing key is the DATA `topic` of §6.2**
  and not a declared value. Nothing in this map carries a key, deliberately: a peer can read
  the topic off the header it already parses, and a parameterized level would need a second
  guarantee-set key for a value that is already on the wire [0016 §4.8].
- The state set is **not a ladder**. Persistence level and replica count are independent axes,
  so `Stored(Flushed)` and `Replicated(3, flushed: false)` are incomparable and comparison is
  per axis ([decisions/0004](decisions/0004-durability-levels.md) §4.4). `backpressure` and
  `producer_naming` are not ordered at all: their values are behaviours and names, not
  strengths, so two peers either state the same one or fail to negotiate. Every other
  dimension *is* a ladder in the order its values are listed above, `control_isolated`
  included — isolation is strictly stronger than none, so the intersection of §2.3 is the
  logical AND.
- When the intersection weakens `acknowledgement` below `Stored`, the `durability` and
  `replicas` axes it qualified are **dropped** rather than carried: dropping is what "weaker"
  means here, and keeping them would produce a set this section forbids. The same holds for
  `dedup_window_ms` when `deduplication` weakens to `None`.
- An unknown map key MUST be skipped, per §5. That is how a later version adds a dimension
  without breaking this one — and it is also why a peer MUST NOT infer agreement from a key it
  skipped: what binds is the intersection of §2.3, computed over the dimensions both sides
  know.


### 6.6 CREDIT (kind 5)

| Key | CBOR type | Name | Required at the decoder | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `tstr` | `endpoint` | **yes** | 512 B | path of the queue the subscription is on |
| `1` | `tstr` | `filter` | **yes** | 256 B | topic filter of the subscription; the empty filter is the whole queue |
| `2` | `uint` | `limit` | **yes** | — | messages this subscription will accept in total, counted from its creation |

All three keys are required. `endpoint` and `filter` are what names a subscription — the same
pair SUBSCRIBE carries (§6.4), on the connection the frame arrives on
([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3) — and an absent `limit`
would be indistinguishable from `0`, which is the pause. The filter is validated against §6.4's
grammar at the decoder, exactly as a SUBSCRIBE's is.

**The limit is absolute and cumulative: a total, not a delta and not a window.** It counts
deliveries on that subscription since the subscription was created, so a lost frame costs
nothing and a duplicated one changes nothing
([decisions/0003](decisions/0003-credit-unit.md) §4.3). Three rules follow, and together they
are the whole scheme:

1. **Initial credit is zero.** A consumer that subscribes and grants nothing receives nothing.
   It is the only default that cannot surprise a consumer with a flood, and it is AMQP 1.0's
   ([decisions/0018](decisions/0018-minimal-broker.md) §4.4).
2. **A receiver keeps the highest limit it has seen** for a subscription. Every control frame
   rides its own unidirectional stream and QUIC orders no stream against another, so a grant
   that arrives late would otherwise lower a limit the consumer has already raised. Keeping the
   maximum is what makes a *reordered* grant harmless, not just a duplicated one.
3. **There is no way to lower a standing limit in v0.** Rule 2 is exact: a receiver ignores any
   grant that is not strictly greater than the limit it holds, so restating the delivered count
   changes nothing unless the subscription had exhausted its credit anyway. The only pause is
   the `0` a fresh subscription starts at. AMQP 1.0 can shrink `link-credit` against an absolute
   baseline; this frame cannot, and a consumer that wants to stay in control therefore grants in
   increments it is willing to receive rather than expecting to stop a grant it has made.

A large limit is not a hostile number: what a broker holds is bounded by its own `queue_bytes`
(§10), not by what a consumer promises to take, and a consumer that states `u64::MAX` has said
"send me everything" rather than "allocate something". What a broker *does* cap is the number
of deliveries it lets go **unsettled** at once — `weida-broker`'s `max_unsettled` — and that
cap became binding with the consumer settlement of
[decisions/0018](decisions/0018-minimal-broker.md) §4.3 and
[decisions/0029](decisions/0029-a-report-is-relayed-a-certificate-is-not.md) §4.7 (B-203):
a delivery is outstanding until its consumer reports `Processed` on the cursor stream the
delivery ordered, and its bytes stay charged to the queue until then. A receiver MUST NOT
deliver beyond the limit it has accepted, and a limit it declines to honour in full is not an
error to report: the consumer observes it as delivery stopping, which is exactly what credit
means.

### 6.7 CURSOR (kind 6)

A CURSOR stream is a report about **one** payload stream: a head frame naming the report,
then a sequence of records until FIN. It carries no payload and never shares a stream with
payload ([decisions/0024](decisions/0024-three-families-one-back-channel.md) §4.4), which is
what makes a report add nothing to any pattern's topology — a Push transfer that orders a
report is still one unidirectional stream of payload.

The head frame is a CBOR header under §5's rules, with one key:

| Key | CBOR type | Name | Required at the decoder | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `uint` | `report_id` | **yes** | — | the `report_id` the DATA header of §6.2 allocated |

The id is required for the same reason CREDIT's `limit` is: a stream that names no report
reports on nothing. Everything else a record could need to name — which transfer, which
endpoint, which direction — is already fixed by the id.

**Records are QUIC varint pairs, not CBOR.** After the head frame the stream carries zero or
more records, each

```
[level: varint][offset: varint]
```

with `level` from the open space of §6.2 key `10` and `offset` an **absolute byte offset**
into the payload of the transfer the report is about. A record is self-delimiting and at most
16 bytes; a CBOR map per record would cost a map header per reported range and buy nothing,
because the head frame already carries every field a record would otherwise name.

Four rules govern a reader:

1. **A cursor is absolute, so coalescing is free.** A receiver keeps the **maximum** offset
   per level. A record that repeats or moves an offset backwards changes nothing and is not
   an error: that is the whole reason a completion is a cursor rather than a verdict
   ([0023](decisions/0023-completion-is-a-cursor.md) §4.3b).
2. **A level that was never ordered is ignored**, and so is a level the reporter cannot
   honour — it is simply absent from the report. Neither is fatal, because a cursor is never
   load-bearing.
3. **An unknown `report_id` resets the stream with `CANCELED`** (§7) and the connection
   survives. No state is allocated for an id the receiver never handed out, which is the
   hostile case this rule exists for.
4. **A record truncated at FIN is a protocol violation.** A record is at most 16 bytes and a
   sender writes whole records, so half of one at FIN is a codec bug rather than a race. A
   short read *before* FIN is not: the reader reads more bytes and retries.

An undefined level below the application floor is a protocol violation at the decoder,
exactly as an undefined `achieved` value is (§6.2): the number decides what a sender believes
about its own transfer.

### 6.8 FLOW (kind 7)

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `tstr` | `endpoint` | yes | 512 B | endpoint path the flow is addressed to |
| `1` | `uint` | `flow` | yes | — | flow id, chosen by the sender; prefixes every datagram of the flow (§6.9) |
| `2` | `tstr` | `content_type` | no | 256 B | opaque media type label |
| `3` | `tstr` | `traceparent` | no | 128 B | W3C Trace Context `traceparent` |
| `4` | `tstr` | `tracestate` | no | 512 B | W3C Trace Context `tracestate`, opaque passthrough |
| `5` | `tstr` | `topic` | no | 256 B | topic the flow carries; set by a radio's datagram flow |

A FLOW stream is the registration **and the lifetime** of a datagram flow
([decisions/0034](decisions/0034-late-is-lost.md) §4.2). Rules:

1. **The flow id is the sender's.** It is unique per connection and direction. A second FLOW
   naming an id that is still live on that connection and direction is a
   `PROTOCOL_VIOLATION`; an id may be reused once its FLOW stream has ended.
2. **Refusal is `STOP_SENDING`** with `UNKNOWN_ENDPOINT` (no path), `UNSUPPORTED` (a path whose
   pattern takes no flow), `REJECTED` (the acceptor declined it) or `LIMIT_EXCEEDED` (more
   inbound flows than the receiver's `max_flows`). A receiver that no longer wants an accepted
   flow stops it with `CANCELED`. The sender learns any of them as the stop code of its FLOW
   stream; datagrams it sent before the stop arrived are dropped at the receiver, which is
   [decisions/0005](decisions/0005-refusal-race.md)'s race, unchanged.
3. **FIN is an orderly close; `RESET_STREAM` is an abandoned flow.** Either ends the flow at
   the receiver, which removes the id.
4. **On QUIC nothing follows the header**; on a local transport §2.1's records follow it.

### 6.9 DATAGRAM payload

A QUIC DATAGRAM frame's payload is

```text
<varint flow> <opaque bytes>
```

where `varint` is RFC 9000 §16's variable-length integer, the same encoding as §6.7's cursor
records. The receiver demultiplexes by `flow` into the flow its FLOW stream registered.

- **An unknown id is held, never an error.** Streams and datagrams are unordered relative to
  each other, so a flow's first datagrams can arrive before its FLOW header. A datagram naming
  an id with no live flow is held for at most `flow_early_hold` in one per-connection ring of
  `flow_early_bytes`, oldest first, and dropped after that; a FLOW header claiming the id
  adopts its held datagrams in order. A truncated varint is dropped the same way and is not an
  error either.
- **A RADIO datagram segment** carries, inside the flow's opaque bytes, `<varint segment>
  <opaque bytes>`: the segment number of DATA key `13`, for a one-packet segment. This is L1
  framing inside the flow payload, not a wire rule of the flow itself.

---

## 7. QUIC application error codes

These codes are used for `CONNECTION_CLOSE`, `RESET_STREAM` and `STOP_SENDING`. They are
distinct from the ERROR frame codes of §6.3.

| Code | Name | Typical use |
| --- | --- | --- |
| `0` | `NO_ERROR` | orderly close |
| `1` | `INTERNAL` | local failure the peer cannot act on |
| `2` | `PROTOCOL_VIOLATION` | any condition in §3.2 |
| `3` | `CANCELED` | transfer abandoned by either side |
| `4` | `NEGOTIATION_FAILED` | §2.3 failure, or `hello_timeout` expiry |
| `5` | `LIMIT_EXCEEDED` | connection refused because a local limit is reached |
| `6` | `SHUTDOWN` | runtime shutting down |
| `7` | `REJECTED` | receiver declines the inbound payload |
| `8` | `UNKNOWN_ENDPOINT` | requested endpoint path is not registered |
| `9` | `UNSUPPORTED` | endpoint exists but does not serve this stream kind |

`UNSUPPORTED` is new in this revision. A misrouted uni stream has no reply half to carry an
ERROR frame, so the refusal has to be a stop code; without a distinct code, "wrong pattern"
would be indistinguishable from "declined".

---

## 8. Golden test vectors

Implementations MUST encode exactly these bytes for these inputs, and MUST decode these
bytes back to these field sets. Byte-exact conformance in both directions is a requirement,
not a convenience: these vectors define the encoding rules of §5 operationally.

```text
DATA   {endpoint:"/t"}                       (initiating half)
       57 01 05  A1 00 62 2F 74

DATA   {}                                    (reply half)
       57 01 01  A0

DATA   {achieved:2}                          (the L2 confirm, on a reply half)
       57 01 03  A1 08 02

HELLO  {versions:[0], max_header_bytes:16384, max_transfers:1024, caps:[], req_caps:[]}
       57 00 10  A5 00 81 00 01 19 40 00 02 19 04 00 03 80 04 80

HELLO  {…, guarantees_offered:{ordering:1}, guarantees_required:{ordering:1}}
       57 00 18  A7 00 81 00 01 19 40 00 02 19 04 00 03 80 04 80 05 A1 04 01 06 A1 04 01

ERROR  {code:5}                              (NO_REPLY, on a reply half)
       57 02 03  A1 00 05

DATA   {endpoint:"/md", topic:"px.eur"}      (publisher fan-out copy)
       57 01 0E  A2 00 63 2F 6D 64 05 66 70 78 2E 65 75 72

DATA   {endpoint:"/t", sequence:1}           (key 6, under negotiated PerProducer ordering)
       57 01 07  A2 00 62 2F 74 06 01

DATA   {endpoint:"/t", sequence:1, producer:<32-byte digest>}   (keys 6 and 7)
       57 01 2A  A3 00 62 2F 74 06 01 07 58 20
                 9F 86 D0 81 88 4C 7D 65 9A 2F EA A0 C5 5A D0 15
                 A3 BF 4F 1B 2B 0B 82 2C D1 5D 6C 15 B0 F0 0A 08

SUB    {endpoint:"/md", filter:"px."}
       57 03 0B  A2 00 63 2F 6D 64 01 63 70 78 2E

UNSUB  {endpoint:"/md", filter:"px."}
       57 04 0B  A2 00 63 2F 6D 64 01 63 70 78 2E

DATA   {endpoint:"/md", topic:"px.*"}        (a topic is never a pattern)
       57 01 0C  A2 00 63 2F 6D 64 05 64 70 78 2E 2A

SUB    {endpoint:"/md", filter:"px.eur"}     (literal filter)
       57 03 0E  A2 00 63 2F 6D 64 01 66 70 78 2E 65 75 72

SUB    {endpoint:"/md", filter:"sensors.*.temp"}   (one-segment wildcard)
       57 03 16  A2 00 63 2F 6D 64 01 6E 73 65 6E 73 6F 72 73 2E 2A 2E 74 65 6D 70

SUB    {endpoint:"/md", filter:"ctl.#"}      (rest wildcard, final segment)
       57 03 0D  A2 00 63 2F 6D 64 01 65 63 74 6C 2E 23

SUB    {endpoint:"/md", filter:""}           (every topic)
       57 03 08  A2 00 63 2F 6D 64 01 60

CREDIT {endpoint:"/jobs", filter:"px.eur", limit:5}
       57 05 12  A3 00 65 2F 6A 6F 62 73 01 66 70 78 2E 65 75 72 02 05

CREDIT {endpoint:"/jobs", filter:"", limit:0}   (the zero every subscription starts at)
       57 05 0C  A3 00 65 2F 6A 6F 62 73 01 60 02 00

CURSOR {report_id:1}                         (head frame of a report)
       57 06 03  A1 00 01

RECORD (accepted, 64)                        (a record, not a frame: no preamble)
       02 40 40

RECORD (stored, 1000000)
       03 80 0F 42 40

DATA   {endpoint:"/t", report_id:1, report:[2]}
       57 01 0A  A3 00 62 2F 74 09 01 0A 81 02

DATA   {endpoint:"/t", report_id:1, report:[3,17], report_mode:1}
       57 01 0D  A4 00 62 2F 74 09 01 0A 82 03 11 0B 01

FLOW   {endpoint:"/v", flow:7}
       57 07 07  A2 00 62 2F 76 01 07

DATAGRAM flow 7, payload "hi"                (a DATAGRAM payload, not a frame: no preamble)
       07 68 69

DATA   {endpoint:"/t", segment:5}            (key 13, written by a radio or Peer::segment)
       57 01 07  A2 00 62 2F 74 0D 05

SUB    {endpoint:"/t", filter:"a", max_age_ms:150}   (key 2, a dish's latency budget)
       57 03 0B  A3 00 62 2F 74 01 61 61 02 18 96

HELLO  {versions:[0], max_header_bytes:16384, max_transfers:1024, caps:[1], req_caps:[]}
       57 00 11  A5 00 81 00 01 19 40 00 02 19 04 00 03 81 01 04 80
```

Decoded field lists:

**Initiating DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x05` (5 bytes),
CBOR map of 1 entry: key `0` `endpoint = "/t"`. Every other DATA key is absent and therefore
omitted. This is the smallest legal request or push header.

**Reply DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x01` (1 byte), the
empty CBOR map. The bidi stream is the correlation, so a reply that carries no metadata
carries no header fields either.

**Confirm DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x03` (3 bytes),
CBOR map of 1 entry: key `8` `achieved = 2` (`Accepted`). This is a whole publisher confirm:
three header bytes, no payload, and a FIN. It is the cheapest frame in this table that says
something a transport cannot, which is the argument for spending an exchange's reply half on
it rather than a frame kind ([decisions/0018](decisions/0018-minimal-broker.md) §4.6).

**Fan-out DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x0E` (14 bytes),
CBOR map of 2 entries: key `0` `endpoint = "/md"`, key `5` `topic = "px.eur"`. This is the
shape of a copy a publisher writes to one subscriber. A real fan-out copy additionally
carries `content_len` and `traceparent`; they are omitted here to keep the vector minimal.

**Sequenced DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x07` (7 bytes),
CBOR map of 2 entries: key `0` `endpoint = "/t"`, key `6` `sequence = 1`. The sequence is a
plain minimal `uint`, so the whole field costs two bytes here and six at `u64::MAX`. A publisher
writes it once its connection has negotiated `PerProducer` ordering and never under `core`
(§6.2).

**Relayed DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x2A` (42 bytes),
CBOR map of 3 entries: key `0` `endpoint = "/t"`, key `6` `sequence = 1`, key `7` `producer`
= the 32 bytes `9f86…0a08`, which is SHA-256 of `"test"` — the digest the address examples
of §4 already use. The `bstr` header is `58 20`: major type 2, one-byte length `0x20` = 32.
This is the shape a relay or an L2 hop writes when the producer is **not** the connection
peer; the `sha256:<64 hex>` spelling never appears on the wire, and a value of any other
length is a framing violation (§6.2).

**SUBSCRIBE and UNSUBSCRIBE vectors** — magic `0x57`, kind `0x03` / `0x04`,
`header_len = 0x0B` (11 bytes), CBOR map of 2 entries: key `0` `endpoint = "/md"`, key `1`
`filter = "px."`. The two frames differ in exactly one byte, the kind.

These two vectors fix an *encoding*, not a match: under the filter grammar of §6.4 the byte
string `px.` is a two-segment filter `["px", ""]`, so it does **not** select the topic
`px.eur` of the fan-out vector above — `px.*` or `px.#` does. The vector predates the grammar
and its bytes are still exactly what an encoder must produce for that filter string.

**Filter grammar vectors** — four SUBSCRIBE frames, one per construct of §6.4, all on
`endpoint = "/md"` and differing only in key `1`: the literal `px.eur`
(`header_len = 0x0E`), the one-segment wildcard `sensors.*.temp` (`0x16`), the rest wildcard
`ctl.#` (`0x0D`), and the empty filter (`0x08`, value `0x60` — the empty text string, present
because absent and empty must stay distinguishable). UNSUBSCRIBE carries the same header
under kind `0x04`. Each fixes an encoding; what each *selects* is the matcher's business, and
the pairs are pinned together in `crates/weida/src/pubsub.rs`.

**Literal-wildcard topic vector** — magic `0x57`, kind `0x01` (DATA),
`header_len = 0x0C` (12 bytes), CBOR map of 2 entries: key `0` `endpoint = "/md"`, key `5`
`topic = "px.*"`. A **`topic` is never a pattern** (§6.2, §6.4): the `*` here is an ordinary
byte, and the filter `px.*` selects this topic exactly as it selects `px.eur`.

**CREDIT vectors** — magic `0x57`, kind `0x05` (CREDIT), a CBOR map of 3 entries in both
cases, because all three keys are required (§6.6). The granting frame is
`header_len = 0x12` (18 bytes): key `0` `endpoint = "/jobs"`, key `1` `filter = "px.eur"`,
key `2` `limit = 5`. The second is `header_len = 0x0C` (12 bytes), with the empty filter
(`0x60`, the whole-queue subscription — present rather than omitted, so absent and empty stay
distinguishable) and `limit = 0`. That zero is where every subscription begins and the only
stop the scheme has: a limit is kept only when it is strictly greater than the standing one,
so restating a number already delivered cannot bring a running subscription back to it.

**CURSOR head vector** — magic `0x57`, kind `0x06` (CURSOR), `header_len = 0x03` (3 bytes),
CBOR map of 1 entry: key `0` `report_id = 1`. That is the whole head frame: a report names
its id and nothing else, because the DATA header that ordered it already fixed which
transfer, which levels and which direction (§6.7).

**CURSOR record vectors** — **not frames**: these are the bytes that follow a head frame on
the same stream, so they carry no preamble. `02 40 40` is `(level 2 = Accepted, offset 64)`:
the level is a one-byte varint, and `64` needs the two-byte form because the one-byte form
stops at `63`. `03 80 0F 42 40` is `(level 3 = Stored, offset 1000000)`, the four-byte varint
form. A record is at most 16 bytes — two eight-byte varints — which is what bounds a reader's
retry when a record is split across reads (§6.7).

**Report-ordering DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x0A`
(10 bytes), CBOR map of 3 entries: key `0` `endpoint = "/t"`, key `9` `report_id = 1`, key
`10` `report = [2]` — one level, `Accepted`. Key `11` is **absent**: `progress` is the
default mode and is never written, on §6.5's rule for `core` levels. This is a Push transfer
that will get a verdict without an exchange
([decisions/0024](decisions/0024-three-families-one-back-channel.md) §4.4a): ten header
bytes, one unidirectional payload stream, and a CURSOR stream carrying the answer.

**Final-only report vector** — the same three keys with `report = [3, 17]` and key `11`
`report_mode = 1` (`final-only`); `header_len = 0x0D` (13 bytes). Two things it pins. The
array is **strictly ascending**, so two peers ordering `Stored` and application stage `17`
send the same bytes. And `17` is above the application floor of `16`, so weida carries it
without interpreting it, while `15` in the same position would be a protocol violation —
the reserved range belongs to a later version of this specification, not to an application
(§6.2).

Every vector §8 once deferred has now landed with its codec. The DATA key `6` and `7`
vectors and the extended HELLO below pin encodings rather than describe traffic:
`weida-protocol` reads and writes all three; key `6` is written by a publisher under negotiated
`PerProducer` ordering, key `7` by nothing here, and a peer that declares nothing offers and
requires `core`.

**HELLO vector** — magic `0x57`, kind `0x00` (HELLO), `header_len = 0x10` (16 bytes),
CBOR map of 5 entries: key `0` `versions = [0]`, key `1` `max_header_bytes = 16384`,
key `2` `max_transfers = 1024`, key `3` `capabilities = []`, key `4`
`required_capabilities = []`. Keys `5` and `6` are absent, which is the declaration every v0
peer makes: offering and requiring the default guarantee set `core` (§6.1, §6.5).

**HELLO-with-guarantees vector** — the same five keys plus key `5`
`guarantees_offered = {4: 1}` and key `6` `guarantees_required = {4: 1}`, each a nested
one-entry map declaring `ordering = PerProducer detect`; `header_len = 0x18` (24 bytes).
Two things it pins. A dimension left at `core` is **not** written, so a HELLO whose
declarations are explicitly `core` is byte-identical to the v0 HELLO above — that is what
makes the declarations free for a peer that wants none. And the nested map obeys every rule
of §5 unchanged: uint keys, strictly ascending, no duplicates, unknown keys skipped.

**ERROR vector** — magic `0x57`, kind `0x02` (ERROR), `header_len = 0x03` (3 bytes), CBOR
map of 1 entry: key `0` `code = 5` (`NO_REPLY`).

**FLOW vector** — magic `0x57`, kind `0x07` (FLOW), `header_len = 0x07` (7 bytes), CBOR map
of 2 entries: key `0` `endpoint = "/v"`, key `1` `flow = 7`. Both keys are required; the
optional keys are omitted when absent (§6.8).

**DATAGRAM payload vector** — not a frame: the payload of one QUIC DATAGRAM frame. `07` is the
one-byte varint of flow id `7`, and `68 69` is the opaque payload `"hi"` (§6.9).

**Segment DATA vector** — `header_len = 0x07`, CBOR map of 2 entries: key `0` `endpoint = "/t"`,
key `13` `segment = 5` (`0D 05`). Key `13` is written by a radio, or by a dialling peer's
`Peer::segment` toward a bound path.

**SUBSCRIBE with `max_age_ms` vector** — `header_len = 0x0B` (11 bytes), CBOR map of 3 entries:
key `0` `endpoint = "/t"`, key `1` `filter = "a"`, key `2` `max_age_ms = 150`, which needs the
one-byte-argument form `18 96` because it is above `23`.

**HELLO-with-datagram vector** — the v0 HELLO with key `3` `capabilities = [1]` (`81 01`
instead of `80`), so `header_len` grows by one to `0x11` (17 bytes). This is what a side that
enables flows sends (§6.1).

---

## 9. Operational semantics

### 9.1 Request/reply is one bidirectional stream

A requester opens a bidi stream and writes, on the initiating half: a DATA header carrying
`endpoint`, then the request payload, then FIN.

The responder answers on the reply half with exactly one of:

- a DATA header (no `endpoint`), the reply payload, and FIN; or
- an ERROR header and FIN.

**Correlation is the stream.** There is no correlation id, no per-connection pending table
and no ordering requirement between exchanges. Two consequences follow directly, and both
are intended:

- Unlimited concurrent exchanges on one connection, bounded only by
  `max_concurrent_bidi_streams`. This is what DEALER/ROUTER exists for in socket-oriented
  systems; here it is emergent rather than a separate pattern.
- Exactly one reply per exchange, structurally. A second reply is not representable, so the
  "reset a duplicate correlation" rule of earlier drafts has nothing left to guard.

The responder MAY write the reply header at any time, including **before** the request half
has reached FIN. The two halves of a QUIC bidi stream are independent, so simultaneous
streaming in both directions is an intended and required capability (master doc §10, §12).

A peer learns of a bidi stream only when its first bytes arrive. Because the DATA header is
always written first, a receiver never sees a bidi stream it cannot classify.

### 9.2 Delivery is QUIC's transport receipt

There is no application-level acknowledgement in v0. The only delivery signal is the one
QUIC already provides: the peer's acknowledgement of every stream byte and the FIN.

In the reference implementation `OutgoingTransfer::finish()` marks the FIN and returns a
`Delivery`; awaiting `Delivery::delivered()` resolves when the peer's transport has
acknowledged the whole payload. quinn's own wording for that condition is that the peer
"acknowledges receipt of all stream data (although not necessarily the processing of it)".

That parenthesis is the entire semantic content:

> The receipt means the peer's **transport** holds every byte. It says nothing about the
> peer's application having read, stored or processed them.

Dropping the receipt is legal and free; it is the fire-and-forget path.

**A refusal is not ordered against the receipt, and no frame will be added at wire version 0
to order it** ([decisions/0005](decisions/0005-refusal-race.md) §4.1-§4.3). A `STOP_SENDING`
refusal (§9.3, §9.4) is an application act, while the receipt is the transport's, so a payload
small enough to fit in flight can be acknowledged before the peer's application refuses it:
`delivered()` then resolves `Ok` for a transfer that was discarded, truthfully, since the
receipt never claimed anything about the application. A refusal is *guaranteed* to be observed
in exactly two constructions: a payload larger than the peer's `stream_receive_window`, where
flow control forces the application to act before the write can finish, and an exchange, whose
ERROR frame on the reply half is written by the receiving application and takes precedence
over the request half's receipt (§9.1). An application that must observe a refusal uses
Req/Rep.

The acknowledgement vocabulary of Accepted / Stored / Replicated / Processed is reserved for
a broker layer (§11) and has no v0 wire representation. See
[GUARANTEES.md](GUARANTEES.md) for why a brokerless application ACK would restate what QUIC
already guarantees.

### 9.3 Cancellation

Cancellation uses QUIC's own stream teardown throughout. No frame carries it.

| Situation | Mechanism |
| --- | --- |
| Sender abandons its own outgoing payload | `RESET_STREAM(CANCELED)` on the sending half |
| Receiver refuses inbound payload | `STOP_SENDING(REJECTED)` on the receiving half |
| Receiver refuses because the path is unknown | `STOP_SENDING(UNKNOWN_ENDPOINT)` |
| Receiver refuses because the path serves another stream kind | `STOP_SENDING(UNSUPPORTED)` |
| Requester abandons the reply | `STOP_SENDING(CANCELED)` on the reply half |
| Replier will not answer | ERROR `{NO_REPLY}` + FIN on the reply half |

A receiver observing a reset before FIN MUST discard all partial state for that transfer.

The requester's stop on the reply half is the direct replacement for the CANCEL frame of
earlier drafts. In the reference implementation it is what dropping a `ReplyStream` before
`recv()` does; the responder observes it as its reply half's `stopped()` future resolving,
and as a failure on its next write.

### 9.4 Endpoint dispatch and refusal

Dispatch is a function of the stream kind and the addressed path. A refusal is never a
connection error: the connection survives all of it.

**Uni stream carrying DATA:**

| Registered at the path | Action |
| --- | --- |
| a puller or subscriber | accept and queue the transfer |
| a raw acceptor | accept and queue the transfer |
| a queue (L2) | admit into the queue, no confirm; `STOP_SENDING(REJECTED)` when the queue is at its byte bound |
| a replier | `STOP_SENDING(UNSUPPORTED)` |
| a publisher | `STOP_SENDING(UNSUPPORTED)` |
| nothing | `STOP_SENDING(UNKNOWN_ENDPOINT)` |

**Bidi stream (an exchange):**

| Registered at the path | Action |
| --- | --- |
| a replier | accept and queue the exchange |
| a raw acceptor | accept and queue the exchange |
| a queue (L2) | admit into the queue and confirm: DATA with the achieved level (§6.2 key `8`) + FIN on the reply half; ERROR `{REJECTED}` + FIN when the queue is at its byte bound |
| a puller or subscriber | ERROR `{UNSUPPORTED}` + FIN on the reply half, `STOP_SENDING(UNSUPPORTED)` on the request half |
| a publisher | same as above |
| nothing | ERROR `{UNKNOWN_ENDPOINT}` + FIN on the reply half, `STOP_SENDING(UNKNOWN_ENDPOINT)` on the request half |

A receiver MUST NOT reinterpret a misrouted stream as something the path does serve.

**The queue rows are the same dispatch, not an exception to it.** A queue is an endpoint path
like any other (`weida-broker` registers one acceptor per queue), so "exactly one answer per
(stream kind, path)" still holds and `UNKNOWN_ENDPOINT` stays decidable. What differs is the
*object* at the path: a publisher fans a message out to every matching subscriber, a queue
hands each message to exactly one consumer. Neither the frame nor the filter encodes that
difference — the registration does
([decisions/0018](decisions/0018-minimal-broker.md) §4.5). There is no declare frame: a queue
exists because the broker's configuration named it, and a path with no queue is
`UNKNOWN_ENDPOINT` from the same table row as any other unregistered path.

**This table is the authorization surface**, and that is a decision rather than an accident
([decisions/0015](decisions/0015-peer-authorization.md)). Dispatch answers "is this stream
servable here"; whether *this peer* may be served here is the acceptor's own decision, taken
on the identity the handshake proved (`IncomingMeta::peer`) and the path the stream arrived
at, and expressed with the refusals above and nothing new: a registered path MAY refuse an
accepted stream with `REJECTED` (ERROR `{REJECTED}` on an exchange's reply half,
`STOP_SENDING(REJECTED)` on a one-way stream), and an acceptor that prefers not to reveal
that the path exists answers `UNKNOWN_ENDPOINT` instead — the two are indistinguishable to
the peer on purpose. What an acceptor MUST NOT do: authorize on anything *claimed* rather
than proved (a header field, the `producer` key of §6.2, a PID), read structure into an
opaque path, or expect a one-way refusal to be observed at all
([decisions/0005](decisions/0005-refusal-race.md)).

The `NO_REPLY` rule is mandatory: an application that takes an exchange and drops it without
answering MUST cause ERROR `{NO_REPLY}` on the reply half. Without it a requester would hang
until the idle timeout.

### 9.5 Push/Pull and Pub/Sub

Both are one-way transfers: a DATA frame on a fresh uni stream, addressed to a path, with no
reply half and nothing to correlate. They differ only in who selects the recipients.

**Push/Pull.** One DATA frame per message, addressed to the puller's path. A sender with
several peers selects one per transfer; the reference implementation uses round-robin over
live connections, but the selection policy is local and not part of the wire contract. The
sender MAY await the transport receipt (§9.2) or discard it.

**Pub/Sub.** A subscriber sends SUBSCRIBE frames (§6.4) naming the publisher's path and a
topic **filter** (§6.4). Publishing a message means writing one DATA frame per matching subscriber,
each on its own uni stream, each carrying `topic` (key `5`). Every copy is independent.
Delivery to a subscriber is best effort with **explicit drops**. A publisher bounds the
payload bytes it will hold queued for one subscriber (`subscriber_buffer_bytes`); a message
that does not fit is dropped for that subscriber alone, and the publisher continues. A slow
consumer therefore cannot stall a publisher or its other subscribers. Publishing a payload
larger than that bound fails locally rather than being dropped for everyone.

**Ordering is `None` for both patterns in v0.** Every message is its own unidirectional
stream, and QUIC does not order streams relative to each other. The per-pipe ordering of
socket-oriented messaging systems does not carry over. A publisher's per-subscriber writer
is serialized, so copies are *enqueued* in publication order, but the receiving application
MUST NOT rely on observing them in that order. Per-producer ordering needs the sequence key
of §6.2, which a publisher writes once the connection has negotiated it; a
subscriber that has negotiated the detect level of `PerProducer` (§6.5) can then observe a
drop instead of missing it silently, which is the whole reason the key exists
([decisions/0001](decisions/0001-sequence-field.md) §7.2).

Ordering between exchanges is likewise `None`, for the same reason.

---

## 10. Resource limits

Limits live in two places, split by what they bound. `weida-core::Limits` is a
**per-connection** profile: every field applies to one connection, which is what lets a
runtime hold one profile per connection tier when the control tier of §2.5 arrives. Numbers
that belong to a runtime rather than to a connection live on `RuntimeConfig`. Every limit
exists to bound memory or state that a remote peer can cause to be allocated (master doc §50,
§81 rule 17).

Per connection (`Limits`):

| Field | Default | Bound enforced |
| --- | --- | --- |
| `max_header_bytes` | `16384` | largest header a peer may make this side buffer; checked before allocation (§3.1) |
| `max_concurrent_uni_streams` | `2048` | QUIC `TransportConfig::max_concurrent_uni_streams`; bounds parked/in-flight one-way stream tasks per connection |
| `max_concurrent_bidi_streams` | `1024` | QUIC `TransportConfig::max_concurrent_bidi_streams`; bounds live exchanges per connection |
| `stream_receive_window` | 1 MiB | per-stream QUIC flow-control window; bounds unread payload buffered per stream |
| `connection_receive_window` | 16 MiB | per-connection QUIC flow-control window; bounds unread payload buffered per connection |
| `keep_alive` | 10 s | QUIC keep-alive interval; sent by the dialling side only, so a binding's value is not read (§2.4) |
| `idle_timeout` | 30 s | QUIC idle timeout, applied in both directions (§2.4) |
| `hello_timeout_ms` | 10 s | time a connection may exist without a processed peer HELLO |
| `max_subscriptions` | `256` | subscription filters one peer connection may hold, summed over paths; exceeding it closes the connection with `LIMIT_EXCEEDED` (§6.4) |
| `subscriber_buffer_bytes` | 8 MiB | payload bytes a publisher will hold queued for one subscriber; a message that does not fit is dropped for that subscriber (§9.5) |
| `max_sequence_scopes` | `1024` | producer scopes — paths and topics — a receiver tracks per connection for gap detection or reassembly under `PerProducer` ordering; the peer names the scopes, so at the cap a new one is simply not tracked |
| `max_reorder_hold` | `256` | transfers a receiver holds back at once, over all scopes, under `PerProducer(reassemble)`; at the cap the oldest held transfer is released out of order with its gap reported (§6.5, [GUARANTEES.md](GUARANTEES.md) §3). A held transfer is an unread stream, so the bytes it pins are bounded again by `stream_receive_window` and `connection_receive_window` |
| `max_dedup_entries` | `4096` | identities a receiver remembers per connection under `Bounded` deduplication; the negotiated window bounds how long an identity is kept and this bounds how many, evicting the oldest at the cap (§6.5) |
| `max_local_streams` | `255` | live transfers on one **local** connection (§2.1), where the stream is the OS object and there is no multiplexing; an `open` at the cap waits for a live transfer to end, exactly as a QUIC `open` waits on the peer's stream budget, rather than refusing. The number was chosen as Windows' named-pipe instance range [0010 §4.2]; the pipe itself is created with `PIPE_UNLIMITED_INSTANCES`, so this per-peer count is the only ceiling on every platform |
| `max_parked_reverse` | `8` | connections a subscriber parks toward a peer it dialled, so that peer can open a stream back (§2.1); each is a descriptor held for a copy that may never come and each counts against `max_local_streams` on both sides. A publisher that finds none parked drops that copy and counts it; zero disables the pool, which makes subscribing over a socket transport an error rather than a silence [0012 §4.4] |
| `datagram_receive_bytes` | `0` | datagrams quinn buffers unread per connection, oldest dropped first; `0` turns flows off, so neither capability code `1` nor `max_datagram_frame_size` is advertised and nothing is buffered. `DEFAULT_DATAGRAM_RECEIVE_BYTES` (64 KiB) is the documented value for turning flows on ([decisions/0034](decisions/0034-late-is-lost.md) §4.5) |
| `datagram_send_bytes` | 64 KiB | datagrams queued for sending per connection; beyond it the oldest is discarded (§6.9) |
| `max_flows` | `64` | inbound flows one connection may hold live; a FLOW beyond it is stopped with `LIMIT_EXCEEDED` (§6.8) |
| `flow_queue_bytes` | 16 KiB | unread datagram bytes held per inbound flow; a slow consumer loses its oldest datagrams, counted per flow |
| `flow_early_bytes` | 4 KiB | per-connection ring for datagrams naming an id with no live flow yet (§6.9) |
| `flow_early_hold` | 1 s | how long a datagram waits in that ring for its FLOW header before it is dropped |
| `congestion` | `Cubic` | the QUIC congestion controller of this profile's connections: `Cubic`, `NewReno` or `Bbr`. Not a bound and not on the wire; it is here because it is chosen per profile ([decisions/0034](decisions/0034-late-is-lost.md) §6, [IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-289) |

Per runtime (`RuntimeConfig`):

| Field | Default | Bound enforced |
| --- | --- | --- |
| `max_connections` | `1024` | connections accepted per server binding; excess connections are closed immediately with `LIMIT_EXCEEDED` |
| `max_connections_per_peer` | `64` | connections one **peer** may hold on one binding, counted by the fingerprint it proved; the excess connection is closed with `LIMIT_EXCEEDED`. It exists because one connection per dialled path (§2.5) lets the dialling side choose the count, and 64 connections to one peer measured about 50 MiB of transport state on the pair ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-011). Connections that proved no identity are each their own peer and are bounded only by `max_connections` (§2.5) |
| `endpoint_queue` | `256` | depth of the accept channel per registered endpoint |
| `max_resolved_addresses` | `8` | addresses a dialling endpoint will try for one hostname, in the resolver's order; a resolver answer is remote input, so its length needs a ceiling |
| `connect_attempt_timeout` | 250 ms | how long a dial waits on one resolved address before trying the next. Every address but the last is bounded by it; an IP literal and a single-address name keep the full handshake budget. The value is RFC 8305's Connection Attempt Delay, and it exists because an address that answers nothing gives QUIC no refusal to observe |
| `shutdown_timeout` | 1 s | how long `Runtime::shutdown` waits for closed sockets to go idle before returning anyway ([decisions/0009](decisions/0009-drain.md) §4.4) |

Worst-case hostile per-connection header memory is bounded by

```text
max_header_bytes * (max_concurrent_uni_streams + max_concurrent_bidi_streams)
        = 16 KiB * (2048 + 1024) = 48 MiB
```

Both stream budgets count: a peer may open its full unidirectional *and* bidirectional
allowance, and every accepted stream starts with one header. This is the governing number
for hostile-peer memory sizing in v0.

`max_concurrent_bidi_streams` replaces the previous hardcoded `0`, which refused bidi
streams outright. It is also the concurrency ceiling for inbound requests: a peer cannot
hold more live exchanges than this, which is what the deleted `max_pending` used to bound
locally — except that it is now enforced by the transport, on the side that pays for it.

`endpoint_queue` produces natural backpressure: when the queue is full, the inbound stream
task awaits queue capacity, which stops reading the payload, which closes the QUIC
flow-control window back to the sender.

`max_subscriptions` is bounded per connection rather than per path because a subscriber
chooses both: without the sum, one connection could hold `max_subscriptions` filters on
each of unboundedly many paths.

`subscriber_buffer_bytes` is the one limit that answers overload by **discarding** rather
than by backpressure. That is deliberate and confined to fan-out: a publisher that blocked
on its slowest subscriber would let one consumer degrade every other (master doc §17).

Per broker (`weida_broker::BrokerConfig`), for a process that runs the L2 layer:

| Field | Default | Bound enforced |
| --- | --- | --- |
| `queue_bytes` | 8 MiB | bytes one queue holds, charged as the payload plus the retained labels plus a fixed `PER_MESSAGE_OVERHEAD` (256 B) per message. A queue at the bound refuses admission — ERROR `{REJECTED}` on an exchange's reply half, `STOP_SENDING(REJECTED)` on a one-way stream — and never discards a message it has already confirmed ([decisions/0018](decisions/0018-minimal-broker.md) §4.8) |
| `max_queues` | `64` | queues one broker registers; a configuration naming more is refused before any path is claimed |
| `max_unsettled` | `256` | deliveries **one subscription** may have unsettled at once, live since B-203. A consumer's credit limit is cumulative and is honoured in full (§6.6); this bounds what may be in flight without an outcome, and an unsettled delivery keeps its bytes charged against `queue_bytes`. Named `max_unacked` until [decisions/0029](decisions/0029-a-report-is-relayed-a-certificate-is-not.md) §4.7: a consumer reports, it does not acknowledge |

These are a **third** profile rather than fields on `Limits`, because a queue is not a
per-connection object: it outlives every connection that touches it, which is the whole point
of a queue. The per-message overhead exists because a budget counting payload bytes only would
bound nothing against a producer of empty messages — with it, `queue_bytes / 256` is also the
ceiling on the message count.

### 10.1 Control and bulk profiles

*Spec ahead of code, for the control half only.*
[decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2-§6.3 gives a peer pair one
**control** connection and one **bulk** connection per dialled path (§2.5). The bulk half is
implemented; the control half is not, and §2.5 says why. The two carry different traffic —
control frames are small, latency-sensitive and few; bulk streams are large, many and
throughput-sensitive — so one set of numbers cannot size both, and `Limits` is already a
per-connection profile so that a second one can be added without moving anything:

| Profile | Sized for | Fields that would differ from the table above |
| --- | --- | --- |
| `control` | a handful of short frames at a time, never a payload | small `stream_receive_window` and `connection_receive_window`; a `max_concurrent_uni_streams` budget that only has to cover HELLO, SUBSCRIBE, UNSUBSCRIBE and CREDIT; `max_concurrent_bidi_streams` may be `0` |
| `bulk` | payload transfers on one path | the windows and stream budgets of the table above, which are also the byte and message credit a consumer grants ([decisions/0003](decisions/0003-credit-unit.md) §4.1) |

The control numbers are not chosen here, and deliberately not in the code either: a profile
nothing reads would be a number nobody has to justify. They are chosen with the tier, from the
measured cost of a connection — a cold handshake of ~1.1 ms and 750-850 KiB of resident state
per live connection counting both ends ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-011) — and
from the per-path fan measurement that follows it. What is normative now is that a peer MUST
NOT be charged twice for the same limit: profiles are separate budgets, not one budget shared,
and `max_connections_per_peer` bounds the connections of a peer whatever tier they belong to.

Both profiles are advertised the same way: `max_header_bytes` in HELLO applies to the
connection the HELLO arrived on (§2.3), so a control connection may advertise a smaller header
limit than a bulk connection to the same peer.

---

## 11. Not specified in v0

The following are deliberately absent from wire protocol version 0. Implementations MUST
NOT invent wire representations for them; they will be specified in later protocol
versions.

- **A frame kind for an application-level acknowledgement.** The levels themselves are no
  longer all unreachable: `Accepted` and `Processed` are what an L2 broker issues, with the
  exact conditions each certifies fixed
  ([decisions/0004](decisions/0004-durability-levels.md) §4.1-§4.4,
  [GUARANTEES.md](GUARANTEES.md) §1), and `Accepted` is reachable today through
  `weida-broker` — but **no frame carries one**, and none is needed: a confirm rides the
  reply half of the producer's exchange as DATA key `8` (§6.2), and a consumer's outcome
  rides the reply half of the delivery exchange
  ([decisions/0018](decisions/0018-minimal-broker.md) §4.6). `Stored(Written|Flushed)` and
  `Replicated(n, flushed)` remain undeclarable-and-unhonourable, so a peer that requires one
  fails negotiation (§2.3): without a store there is nothing to survive a restart, and
  reporting them for an in-memory buffer is prohibited rather than optimistic
  ([GUARANTEES.md](GUARANTEES.md) §1). The v0 core itself still certifies nothing above the
  transport receipt (§9.2), and that a receipt cannot be ordered against an application
  refusal is a decided position, not an omission
  ([decisions/0005](decisions/0005-refusal-race.md), §9.2).
- **Router/Dealer equivalents.** Not needed as wire constructs: an exchange is a stream, so
  unlimited concurrent unsynchronized requests and correctly matched replies both fall out
  of §9.1. What Router adds beyond that — forwarding to third parties, identity envelopes —
  is broker work and has no v0 representation. Pair patterns are likewise unspecified.
- **Connecting publishers and binding pushers.** In v0 Rep, Pull and Pub bind while Req,
  Push and Sub connect. The reverse directions have no v0 representation.
- **Nothing for streaming fan-out**, because it needed nothing. A publisher can write one
  stream per subscriber chunk by chunk (`Publisher::open`, B-064,
  [PATTERNS.md](PATTERNS.md) §4.1), and that is the same DATA frame on the same
  unidirectional stream as §9.5's whole message with the `content_len` key absent — a
  subscriber reads to FIN, as it does for every streamed transfer. What the drop looks like
  is `RESET_STREAM(CANCELED)` on that subscriber's stream and nothing on anybody else's,
  which is §7's existing vocabulary. The design the deferral expected — a wire construct for
  tee-ing — did not exist to be added.
- **Ordering and deduplication beyond what is negotiated.** The sequence and producer keys
  of §6.2 are coded, pinned (§8) and now acted on: a peer that negotiated `PerProducer`
  numbers its one-way transfers and, in `detect`, reports gaps, or, in `reassemble`, holds
  arrivals back up to `max_reorder_hold` ([decisions/0001](decisions/0001-sequence-field.md)
  §7.5, [GUARANTEES.md](GUARANTEES.md) §3). What stays unspecified: `PerKey` ordering has no
  wire representation at all and is L2 work [0001 §7.4], `Total` is unspecified, and
  `Durable` deduplication needs a store and belongs to the broker [0001 §7.6]. None of these
  adds a frame: the guarantee set of §6.5 is the whole wire surface for them.
- **A quiescence signal.** `Runtime::drain(deadline)` is a **local** operation and has no
  wire representation: it stops admitting work, waits on transport receipts the connection
  already produces and then closes with `SHUTDOWN` like any other close. A peer observes
  exactly what it observes today — refused streams and a close — and nothing announces the
  drain. The alternative, a frame the peer answers when it has taken everything, is an
  application acknowledgement and is closed by
  [decisions/0005](decisions/0005-refusal-race.md); no protocol in the catalogue offers one
  either ([decisions/0009](decisions/0009-drain.md) §4.8, §3 option E). Implementations MUST
  NOT invent a quiescence frame for it.
- **Session state.** No session identifier, no subscription resumption and no sequence
  resumption. The peer's proved fingerprint identifies it across connections and carries no
  retained state; resumption is L2 work
  ([decisions/0008](decisions/0008-session-identity.md) §4.5, §4.6).
- ~~**The L2 credit frame.**~~ **Specified and implemented** as of B-202: kind `5`, the fields
  of [decisions/0003](decisions/0003-credit-unit.md) §4.2-§4.3, encoded in §6.6. It names a
  subscription, so it is path-scoped and rides that path's connection rather than a control
  connection, which amends 0003 §4.2
  ([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3). It is listed here so
  the change is visible from the section that used to forbid it: a v0 receiver treated kind `5`
  as unknown, and now every receiver knows it — a peer with no queue ignores it rather than
  closing (§4).
- **Multiple replies per exchange.** Exactly one reply or one ERROR per reply half; a second
  is not representable.
- **Persistence.** No wire concept of durability, storage acknowledgement or recovery.
- **Deduplication as a behaviour.** The producer-identity key of §6.2 is coded and pinned in
  §8; `Bounded(window)` and the receiver-side window that would use it are decided
  [0001 §7.6], [0008 §4.4] and unbuilt, so no v0 peer suppresses a duplicate. `Durable`
  deduplication is L2 work.
- **Resumable or checkpointed streams.** No offsets, content addressing or resume semantics
  (master doc §82 leaves this for after the basic protocol is proven).
- **Retries.** The protocol carries no retry or attempt metadata; retry is entirely an
  application concern in v0.
- **Authentication beyond TLS.** No application-level authentication fields in HELLO, and
  [decisions/0015](decisions/0015-peer-authorization.md) makes that a decision with a reason
  rather than an omission: the handshake proves a key, and *authorization* is the acceptor's
  local decision on `(proved peer, dispatched path)`, expressed with §9.4's refusals. The
  positive half is therefore already specified; what is absent is any credential on the wire.
  A later version that wanted one would take **HELLO key `7`** — the first free key in §6.1 —
  and pay the rest of the bill that note itemizes.
