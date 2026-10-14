# 0008 — Session identity — the proved fingerprint, and nothing else

- **Status:** accepted
- **Date:** 2026-09-11
- **Related:** the open item of 0001 §8; P2, P6, P14; decisions 0001 §7.3 and §8, 0002 §6.3
  and §7

## 1. The question

Decision [0002](0002-control-and-bulk-separation.md) gave each peer pair a control connection
and one bulk connection per dialled path, and bound them together by "the proved fingerprint,
which is already the identity the pool keys on" [0002 §6.3] — while leaving open "how a bulk
connection names its control connection (a HELLO field, or the fingerprint alone)"
[0002 §7]. Decision [0001](0001-sequence-field.md) made the same fingerprint the default
producer name of the sequence field, with the counter restarting per connection, and deferred
the stable-name variant because it "requires a session concept weida does not have"
[0001 §7.3]; §8 of that note records the deferral as a new open decision: "**Session and
stable producer name.** The variant of §7.3 that survives a reconnect needs a session concept
weida does not have; it is a new open decision to record in SYNTHESIS §8, distinct from §8.4
which this note closes" [0001 §8].

The question is therefore threefold: what names a peer across more than one connection;
whether weida acquires session state at L0 so that something can be resumed; and where
subscription and sequence resumption live if not there.

## 2. The evidence, condensed

**weida already has exactly one peer name, and it is proved rather than claimed.** "A peer is
named by the SHA-256 fingerprint of its public key (`Fingerprint`, text form
`sha256:<64 hex>`)… Whatever arrives on a stream carries the peer's proved fingerprint in
`IncomingMeta::peer` (`None` for an anonymous client); it comes from the handshake, never from
a header, so it can be authorized on but not forged" ([PATTERNS.md](../PATTERNS.md) §1.9),
[GUARANTEES §6]. The verification path makes the claim exact: "Whatever the trust path, the
handshake signature is verified with the crypto provider's algorithms, so a peer is only ever
accepted for a key it proved it holds" ([ARCHITECTURE.md](../ARCHITECTURE.md) §5 TLS). The
client connection pool already keys on it, and every part of that key is load-bearing: "two
endpoints dialling one authority under different trust, under different client identities, or
expecting different peers must never share a connection" [ARCHITECTURE §5].

**QUIC fixes that identity to the connection and refuses to let it change.** "A server MUST
NOT use post-handshake client authentication" (RFC 9001 §4.4) — "One identity per connection,
fixed at the handshake" [quic-standards §12 item 12]. So within a connection the fingerprint
is immutable, and across connections two connections either proved the same key or are two
peers. Nothing weaker is available: QUIC also gives "no resumption of stream state" — 0-RTT
resumes the TLS session and remembered transport parameters, and "QUIC itself does not depend
on any state being retained when resuming a connection unless 0-RTT is also used" (RFC 9001
§4.5); "Stream IDs, offsets and unacknowledged data do not survive" [quic-standards §12 item 6].

**weida has no session today, and says so in the places where a session would show.**
"**Nothing reconnects.** The application calls `connect` again, with the same or a new
address; the dead entry is reaped then. A `Subscriber` re-sends its filters on `connect`"
[PATTERNS §1.8]. A peer's subscriptions are "bounded by `max_subscriptions` filters per
connection… and dropped wholesale when the connection closes"
([INVARIANTS.md](../INVARIANTS.md) v0 mechanical checks). The sequence counter of 0001
restarts with the connection, because its producer name is the connection's fingerprint
[0001 §7.3].

**The catalogue says this is the normal state of affairs, and names the two exceptions.**
D8: "Nobody re-establishes application state on reconnect… Reconnect restores a transport, not
a registration" — ZeroMQ workers must close and reopen to re-register, RabbitMQ re-establishes
nothing and publisher sequence numbers restart, MQTT session state is not transferred on
redirection, NNG subscriptions are not durable protocol objects. "Only two mechanisms in the
whole set genuinely resume application state: AMQP 1.0 link resumption against a durable
terminus [amqp10 §12/P6] and MQTT session resumption keyed on ClientID [mqtt5 §1] — plus
JetStream's durable consumers, which keep the state server-side so there is nothing to resume"
[SYNTHESIS §2 D8]. Both exceptions need storage: a durable terminus, or a broker-held session.

**The QUIC-native projects report the same split, and one of them had to add a protocol
message for it.** "Session and subscription state, not the transport, is what breaks on
reconnect": EMQX's two documented limitations are both state — session state is not preserved,
and an unexpectedly closed data stream loses the QoS 1 and QoS 2 message state — while MOQT
needed `GOAWAY` to migrate subscriptions because a session "can take days" to drain
[prior-art §8 lesson 10], [prior-art §3].

**Key-as-identity is the majority design among the QUIC-native peers, with its cost named.**
iroh's public key "is also known as the EndpointId", the peer is authenticated automatically,
but "it is up to the application to decide if a particular peer is allowed to connect or not";
libp2p derives the peer id from the key; WireGuard identifies a peer strictly by its
Curve25519 key; Cap'n Proto expects the vat id to be "typically some sort of public key";
Tailscale is the counter-example, an entire coordination service to distribute keys and compile
policy [prior-art §8 lesson 9]. iroh also already holds several connections to one identity —
a connection is "always for *exactly one* ALPN", so peers speaking several protocols hold
several connections with shared internal state [prior-art §2] — which is 0002's shape with the
identity question answered the same way.

**The alternative, a self-asserted session identifier, imports an authorization problem the
sheets document.** MQTT's session is "keyed by Client Identifier", at most one live session per
ClientID, and a second CONNECT with the same ClientID "takes it over and evicts the incumbent"
with DISCONNECT 0x8E [mqtt5 §1], [mqtt5 §2]. Because the identifier is claimed rather than
proved, the specification has to add a security rule: authorization must "check that the client
is authorized to use its Client Identifier", since "the ClientID is the key to session state
and a colliding identifier gives access to another client's session" [mqtt5 §10]; the failure
table records the resulting ambiguity — after a takeover, nothing distinguishes "the same
application restarting" from "a rogue client reusing the ClientID" [mqtt5 §8]. A proved
fingerprint has neither the rule nor the ambiguity, which is precisely the difference
[PATTERNS §1.9].

**Where a session would be needed, the work is already assigned to L2.** The completion
vocabulary `Accepted`/`Stored`/`Replicated`/`Processed` is reserved for the broker layer
[GUARANTEES §3]; the L2 credit of 0003 is granted per subscription on the control connection
"so that credit can be withdrawn without closing streams and can be re-stated after a
connection change" [0003 §4.2]; and `PerKey` ordering is already closed as L2-only
[0001 §7.4]. A bridge that offers MQTT session resumption "owns that state itself; a bridge
that does not must refuse Clean Start 0 with a non-zero expiry, or document that it maps to
Clean Start 1" [SYNTHESIS §7.2].

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| A — the proved fingerprint is the identity; no session state at L0 | connections are bound by the key each side proved; nothing survives a connection | iroh `EndpointId`, libp2p, WireGuard, Cap'n Proto vat id [prior-art §8 lesson 9] | a peer that reconnects is the same *peer* but keeps nothing: filters are re-sent, the 0001 counter restarts [PATTERNS §1.8], [0001 §7.3] |
| B — a session identifier in HELLO, self-asserted | a new HELLO field names a logical session across connections | MQTT ClientID [mqtt5 §1] | it can be claimed, so it needs an authorization rule and produces the takeover ambiguity MQTT documents [mqtt5 §10], [mqtt5 §8]; it would be the first weida identity that is not proved [PATTERNS §1.9] |
| C — an L0 session object with retained state | subscriptions, sequence position and unread transfers survive a connection loss for an expiry interval | MQTT session state [mqtt5 §2]; AMQP 1.0 durable terminus [amqp10 §12/P6] | storage and an expiry timer in the stream core: remote-controlled state with no owner, against "no remote input can cause unbounded memory allocation" [INVARIANTS]; both precedents are broker features, and the layering argument already rejected importing broker semantics into the socket layer [ARCHITECTURE §1] |
| D — 0-RTT resumption as the session mechanism | reuse the TLS session ticket to carry application state forward | QUIC 0-RTT [quic-standards §12 item 6] | it resumes nothing application-level by construction, and 0-RTT has no replay protection [quic-standards §12 item 15]; it answers a different question |

## 4. Decision

1. **The proved public-key fingerprint is the peer identity, and it is the only one.** Option
   A. A peer is the key it proved in the handshake [PATTERNS §1.9], [ARCHITECTURE §5], and
   that name is valid across connections for as long as the key is: two connections that
   proved the same fingerprint are the same peer, and two that did not are two peers. No
   identifier is added to HELLO or to any header for this purpose; Option B is closed, because
   a claimed identifier would be the only unproved identity in the system and brings MQTT's
   authorization rule and takeover ambiguity with it [mqtt5 §10], [mqtt5 §8].

   **Amended by [0010](0010-local-transport.md) §4.4.** On a local transport there is no TLS
   and therefore no key to present, and the prover is the kernel — a stronger statement about
   a process on the same machine than a certificate makes. `IncomingMeta::peer` is therefore a
   key **or** a local principal, and an in-process peer is neither. What this note decided and
   what survives is the rule, *proved and never claimed*; what gives way is only the word
   "only". Option B stays closed: a claimed identifier is still refused, on every transport.
2. **This answers the open follow-up of 0002 §7: the fingerprint alone binds a bulk connection
   to its control connection.** No HELLO field names the control connection. A bulk connection
   is associated with the control connection that proved the same fingerprint under the same
   `(host, port, terms)` pool key; one whose fingerprint differs "is not the same peer"
   [0002 §6.3], [ARCHITECTURE §5]. The binding therefore requires that the peer be
   authenticated in both directions: a client that dials anonymously has
   `IncomingMeta::peer == None` [PATTERNS §1.9], [GUARANTEES §6], so a binding that accepts
   anonymous clients cannot bind their control and bulk connections and MUST NOT treat two
   anonymous connections as one peer. A deployment that wants 0002's two-tier isolation
   requires a client identity (`ServerTls::require_client`) [PATTERNS §1.9],
   [ARCHITECTURE §5 TLS].
3. **The fingerprint is the default producer name of 0001 §7.3, and that is now its settled
   meaning**: the counter is scoped to (proved peer, endpoint or topic) and restarts whenever
   the connection does, because the name is the connection's identity and the connection
   carries no position [0001 §7.3], [quic-standards §12 item 6]. The optional stable producer
   name of 0001 §7.3 remains a field-shape requirement — the producer name MUST be able to come
   from somewhere other than the fingerprint — and its *supplier* is now named: an L2
   subscription, not an L0 session.
4. **Where the producer name is the fingerprint it is not written on the wire at all; where
   it is written, it is the raw 32-byte digest.** The receiver already knows the sending
   peer's fingerprint from the handshake — it is on every inbound transfer as
   `IncomingMeta::peer` and cannot be claimed [PATTERNS §1.9], [GUARANTEES §6] — so in the
   default case of §4.3 the producer-identity key that 0001 §8 leaves open is simply absent,
   and the name is resolved from the connection. The cost of doing otherwise is measured:
   B-009 records a 64-byte push as a 135 B DATA frame minimally, while a sequence `uint`
   plus a producer identity carried as the `sha256:<64 hex>` text form costs +80 B — 215 B
   per message, +59.3 % of the whole frame — and 179.4 -> 162.9 Kmsg/s on loopback, a 9 %
   throughput loss larger than that machine's run-to-run spread. The split is what decides
   the encoding: the sequence is 6 B (1 B key + 5 B `uint`) and the text-form identity 74 B
   (1 B key + 2 B `tstr` prefix + 71 B), against 1 + 2 + 32 = 35 B for the same fingerprint
   as a CBOR `bstr`. Three rules follow. The text form is presentation only — an API,
   address and log spelling [PATTERNS §1.9] — and MUST NOT be the wire encoding. The key is
   written only where the producer is *not* the connection peer: a relay or an L2 hop
   forwarding another producer's output, or the stable name of §4.6; it is then a CBOR
   `bstr` carrying the raw 32-byte digest where it names a fingerprint. And the default
   therefore pays the sequence's 6 B alone, not the measured 80 B, which is also what
   "disabled guarantees should not participate in the hot path" asks of a connection that
   negotiated no ordering [INVARIANTS], [0001 §7.8]. The numbers are B-009's, recorded in
   [IMPLEMENTATION.md](../IMPLEMENTATION.md) §4 under "Verified results — DATA header cost
   at a high message rate (B-009)".
5. **There is no L0 session state. Nothing is retained across a connection.** Option C is
   closed. Subscriptions stay per connection and are dropped wholesale when it closes
   [INVARIANTS]; a `Subscriber` re-sends its filters on the next `connect` [PATTERNS §1.8];
   the sequence counter restarts (§4.3); no expiry interval, no session table and no
   reconnect logic enters the runtime. The reasons are the ones §2 records: every mechanism in
   the catalogue that genuinely resumes application state owns storage
   [SYNTHESIS §2 D8], storage is the L2 layer's job [ARCHITECTURE §1], and a retained session
   is remote-controlled state that the stream core has no owner for [INVARIANTS].
   **Amended by [0031](0031-transparent-redial-and-the-sender-outbox.md):** "no reconnect
   logic enters the runtime" is withdrawn for the *transport* half — the runtime redials a
   dialled address and the subscriber re-sends its own filters on the new connection. The
   session half stands unchanged: nothing server-side is retained, the counter restarts, and
   the outbox 0031 adds is sender-local state with a sender-local owner and bound.
6. **Subscription and sequence resumption are L2 work, and this note assigns them there
   rather than leaving them unowned.** The broker hop of Phase 6 is where a subscription
   outlives its connection, because the broker already holds the per-subscription credit that
   "can be re-stated after a connection change" [0003 §4.2] and already owns the vocabulary for
   responsibility transfer [GUARANTEES §3]. The precedent shape is JetStream's durable
   consumer, which keeps the state server-side "so there is nothing to resume" [SYNTHESIS §2
   D8]. What L2 must define when it is designed: what a subscription is bound to (the
   fingerprint plus an application-chosen name), how long it outlives a connection, and what a
   resumed sequence position means. None of that is decided here, and none of it may be
   approximated on the v0 wire [PROTOCOL §11].
7. **Identity remains authentication, not authorization.** "A completed handshake says which
   key answered and nothing about what that peer may do" [ARCHITECTURE §5], which is lesson 9's
   division of labour [prior-art §8 lesson 9]. Nothing in this note grants a peer a capability
   by virtue of being recognized across connections.
8. **The open item of 0001 §8 is closed by this note**, and 0001 §8 is edited to say so. The
   session concept it asked for does not arrive at L0; the stable producer name it wanted is
   an L2 subscription property per §4.6.

## 5. Consequences and follow-ups

- **[0001](0001-sequence-field.md) §8.** The "Session and stable producer name" follow-up is
  marked closed by this note in that file (done with this change). The field-shape requirement
  of 0001 §7.3 stands: the producer-identity key must admit a name that is not the
  fingerprint, because L2 will supply one.
- **[PROTOCOL.md](../PROTOCOL.md) §6.2 and §8.** The producer-identity key whose "exact
  encoding stays open" in 0001 §8 is answered by §4.4: a CBOR `bstr`, absent whenever the
  producer is the connection peer, carrying the raw 32-byte digest where it names a
  fingerprint; the `sha256:<64 hex>` spelling stays presentation. Golden vectors (§8) cover
  both the absent case and the 32-byte form, and the header-size arithmetic of 0001's option
  table is corrected by B-009's measured 135 B / 215 B frames rather than estimated.
- **[PROTOCOL.md](../PROTOCOL.md) §2.** 0002's follow-up is resolved: no HELLO field names the
  control connection, and §2 must state the binding rule — a bulk connection belongs to the
  control connection that proved the same fingerprint under the same authority and terms, and
  a mismatch is a different peer, refused. §2.3 must also state that this binding is
  unavailable for anonymous clients (§4.2). Whether ALPN distinguishes the two connection
  kinds is untouched by this note and stays part of 0002's follow-up.
- **[PROTOCOL.md](../PROTOCOL.md) §11.** Add to "not specified in v0": session state, session
  identifiers, subscription resumption and sequence resumption, each with decision 0008 as the
  reason and L2 as the owner.
- **[PATTERNS.md](../PATTERNS.md) §1.8 and §1.9.** §1.8's "Nothing reconnects" gains the
  positive statement of what *does* persist: the peer's name. §1.9 gains the cross-connection
  sentence — the fingerprint identifies the peer across connections, is what binds a control
  connection to its bulk connections, and is the default producer name of the sequence field —
  and the anonymous-client exception of §4.2.
- **[GUARANTEES.md](../GUARANTEES.md) §6.** The peer-identity row states that the identity is
  per connection *and* stable across connections for the same key, and that it carries no
  session: nothing about a previous connection is restored by recognizing a peer.
- **[ARCHITECTURE.md](../ARCHITECTURE.md) §5.** The client pool section, which 0002 already
  changes to two tiers, states that the tie between a control entry and its bulk entries is the
  proved fingerprint and nothing else, and that `Option<Fingerprint> from the address` remains
  a *dialling expectation* rather than the identity itself — the identity is what the handshake
  proved.
- **[INVARIANTS.md](../INVARIANTS.md).** No new bound is created by this note; that is the
  point of §4.5, and the v0 mechanical-checks entry for subscriptions ("dropped wholesale when
  the connection closes") is now a decided property rather than an implementation detail, so it
  must not be relaxed without a new decision.
- **Code.** No change in `crates/weida` or `crates/weida-core` for this note by itself: the
  behaviour it decides is what the tree does. The rule it adds lands with 0002's two-tier pool
  — `crates/weida/src/pool.rs` must compare the proved fingerprint of a bulk connection against
  its control connection's and refuse a mismatch, and `crates/weida/tests/identity.rs` is where
  that refusal belongs as a test once the two tiers exist.
- **Adapters.** An adapter offering a foreign session (MQTT Clean Start 0 with a non-zero
  expiry, AMQP 1.0 link resumption against a durable terminus) owns that state in its own crate
  or refuses the configuration; weida contributes the peer's proved name and nothing else
  [SYNTHESIS §7.2], [SYNTHESIS §2 D8], [INVARIANTS]. An adapter MUST NOT present a foreign,
  self-asserted session identifier as a weida peer identity, for the reason §2 gives and the
  one SYNTHESIS §7.2 already records for `user-id`.
- **The open item of [0001](0001-sequence-field.md) §8** is closed by this note, in that file.
  Note the discrepancy: 0001 §8 says the item "is a new open decision to record in
  [SYNTHESIS.md](../research/SYNTHESIS.md) §8", but SYNTHESIS §8 never received it — its
  items run 8.1 to 8.9 and none is about sessions. Nothing is therefore marked closed there
  by this note; the record of the question and of its answer is 0001 §8 plus this note. A
  SYNTHESIS §8 entry is not created retroactively merely to strike it out.

## 6. Sources

weida documents: [PATTERNS.md](../PATTERNS.md) §1.8, §1.9, §4; [GUARANTEES.md](../GUARANTEES.md)
§3, §6; [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §2 (Identity and Trust), §5 (client
connection pool, TLS); [PROTOCOL.md](../PROTOCOL.md) §2, §6.2, §8, §11;
[INVARIANTS.md](../INVARIANTS.md); [IMPLEMENTATION.md](../IMPLEMENTATION.md) §4 ("Verified
results — DATA header cost at a high message rate (B-009)"); [0001](0001-sequence-field.md) §7.3,
§7.4, §7.8, §8; [0002](0002-control-and-bulk-separation.md) §6.3, §7;
[0003](0003-credit-unit.md) §4.2.

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §1 (P2, P6, P14), §2 (D8), §7.2;
[quic-standards.md](../research/quic-standards.md) §12 items 6, 12, 15;
[prior-art.md](../research/prior-art.md) §2 (iroh), §3 (EMQX), §8 lessons 9 and 10;
[mqtt5.md](../research/mqtt5.md) §1, §2, §8, §10, §12/P6; [amqp10.md](../research/amqp10.md)
§12/P6; [nats.md](../research/nats.md) §12/P6.
