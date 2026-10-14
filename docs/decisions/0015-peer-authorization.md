# 0015 — Peer authorization — no credential in the handshake, a verdict per path

- **Status:** accepted
- **Date:** 2026-09-13
- **Accepted:** 2026-10-13
- **Related:** B-066; [requirements/zeughaus-video.md](../requirements/zeughaus-video.md) request
  3; [0005](0005-refusal-race.md); [0007](0007-topic-namespace.md) §4;
  [0008](0008-session-identity.md) §4.1, §4.2, §4.5, §4.7; [0010](0010-local-transport.md) §4.4;
  [0011](0011-answered-where-it-arrived.md) §4.1-§4.3; [PROTOCOL.md](../PROTOCOL.md) §2.2, §5,
  §6.1, §6.4, §9.4, §11; [INVARIANTS.md](../INVARIANTS.md); P14 and D6 of
  [SYNTHESIS.md](../research/SYNTHESIS.md).

## 1. The question

B-066, verbatim in its acceptance line: whether a weida handshake carries an application
credential at all — "a HELLO-carried token validated by the acceptor, an application handshake
on a separate path, or neither". The source is request 3 of
[requirements/zeughaus-video.md](../requirements/zeughaus-video.md), which files it as a
question rather than a blocker: "a user who reaches a SpacetimeDB over the internet and wants
monitoring needs something that says *which* client may attach. If weida grows a credential in
the handshake, zeughaus would pass the session token it already has. If not, an application
handshake on a `/auth` path is zeughaus-side work" [zeughaus-video, "What weida would need",
item 3]. The same document records the present state flatly: "There is no authorization concept
beyond certificate identity" [zeughaus-video, "Integration"].

Two things have to be settled together, because the first alone answers the wrong question:
whether anything about *permission* belongs on the wire, and what an acceptor may decide **per
endpoint path** — the granularity the requester asked for, and the one that collides with the
invariant that "endpoint paths are opaque identifiers" [INVARIANTS].

## 2. The evidence, condensed

**0008 already settled who the peer is, and deliberately did not settle what it may do.** A
peer is the key it proved in the handshake: the identity "comes from the handshake, never from
a header, so it can be authorized on but not forged" ([PATTERNS.md](../PATTERNS.md) §1.9),
amended by [0010](0010-local-transport.md) §4.4 only in the count — on a local transport the
kernel is the prover and `IncomingMeta::peer` is a key *or* a local principal, while the rule
that survives is *proved and never claimed* [0008 §4.1], [INVARIANTS]. 0008 §4.7 draws the
line this note starts from: "**Identity remains authentication, not authorization.** 'A
completed handshake says which key answered and nothing about what that peer may do'
([ARCHITECTURE.md](../ARCHITECTURE.md) §5)… Nothing in this note grants a peer a capability by
virtue of being recognized across connections." ARCHITECTURE §5 also names what exists today
and what does not: "Deciding that is the application's job, on `IncomingMeta::peer`: the
identity is on every inbound transfer and request, so a handler can refuse per endpoint, per
topic or per payload. The only allow list built into v0 is a `Trust` pin list on a binding,
which is connection-wide and all-or-nothing; the authorization hooks of master doc §46 are not
implemented."

**The wire says the same thing in the negative.** [PROTOCOL.md](../PROTOCOL.md) §11 lists under
"Not specified in v0": "**Authentication beyond TLS.** No application-level authentication
fields in HELLO." HELLO carries keys `0`-`6` — `versions`, `max_header_bytes`, `max_transfers`,
`capabilities`, `required_capabilities`, and the two optional guarantee-set maps — and "keys
`0` to `4` are required"; "Capability code assignment is unspecified in v0: no codes are
defined and the v0 supported set is empty" [PROTOCOL §6.1], [PROTOCOL §11]. HELLO names no
endpoint path, and that is not an oversight: [0011](0011-answered-where-it-arrived.md) §3
option B — "a HELLO key naming the dialled path" — was rejected because "the tiers become
distinguishable on the wire, against [PROTOCOL.md](../PROTOCOL.md) §2.5; the path is then
stated twice (HELLO and SUBSCRIBE) and the two can disagree; a remote-keyed table needs a new
named bound [INVARIANTS]".

**ZeroMQ carries a credential in the handshake and still cannot answer this question.** ZMTP
announces "precisely one security mechanism, unlike SASL", asserted per socket, with a mismatch
closing the connection [zeromq §1, 37/ZMTP]. PLAIN sends a username and password in clear text
(`C:HELLO`/`S:WELCOME`/`C:INITIATE`/`S:READY`), exists for "minimal authentication of clients
(to avoid errors in configuration)" and is "not robust against even the simplest traffic
snooping or spoofing attacks" [zeromq §10, 24/ZMTP-PLAIN]. CURVE authenticates a 32-byte
long-term key and names three security models, the strongest of which is "Where each client has
its own key, that the server checks. In this case the server can grant access to clients
according to their authenticated identity" [zeromq §10, 26/CURVEZMQ] — which is exactly what
weida's TLS handshake already does. The *decision* is not in ZMTP at all: it is delegated to
ZAP, an in-process REQ/REP dialog on `inproc://zeromq.zap.01` whose request carries version,
request id, domain, client address, `Identity`, mechanism and credentials, and whose reply is a
status of "200" success, "300" temporary error, "400" authentication failure or "500" internal
error plus a user id [zeromq §10, 27/ZAP]. Its granularity is the finding that matters here:
"**Authorization granularity.** Per connection, at handshake time. The domain string is the
only scoping handle, and its meaning is left open: 'The significance of domains are an
application issue and not relevant to ZAP.' No per-message, per-topic, per-service or
per-operation authorization exists anywhere in the specifications" [zeromq §10], [zeromq
§12/P14]. And the mechanism does not stand alone: measured against libzmq 4.3.5, "its PLAIN and
CURVE servers refuse every connection without a ZAP handler bound on
`inproc://zeromq.zap.01` in the same context" [zeromq §13, source 41].

**MQTT 5 is the fullest in-handshake credential in the catalogue, and its authorization lives
somewhere else.** Basic: `User Name` and `Password` in the CONNECT payload (3.1.2.9, 3.1.3.5,
3.1.3.6) [mqtt5 §10]. Enhanced: `Authentication Method` in CONNECT names a scheme,
"commonly a SASL mechanism", and AUTH packets carry `Authentication Data` both ways with reason
code 0x18 until CONNACK 0x00 (4.12); every AUTH and any successful CONNACK MUST repeat the same
method ([MQTT-4.12.0-5]); an unsupported method earns CONNACK 0x8C or 0x87 and the connection
MUST close ([MQTT-4.12.0-1]); it is OPTIONAL for both parties and, absent a client-named method,
the server MUST NOT send AUTH ([MQTT-4.12.0-6]) [mqtt5 §10]. Re-authentication is the one
capability the exchange adds over a certificate: a client may send AUTH 0x19 at any time after
CONNACK with the same method ([MQTT-4.12.1-1]), other traffic continues on the previous
authentication during the exchange, and on failure both sides MUST close ([MQTT-4.12.1-2]) —
"the protocol's answer to credential rotation on long-lived connections" (4.12.1, 5.4.10)
[mqtt5 §10]. Against that, authorization: "**Granularity.** Per connection for authentication;
per topic name and per topic filter for authorization, at publish and subscribe time.
In-protocol verdict carriers: CONNACK 0x87, SUBACK/UNSUBACK 0x87 or 0x8F per filter,
PUBACK/PUBREC 0x87 per message, DISCONNECT 0x87 (2.4)", while "'While topic authorization is
outside of this specification, it is recommended that Servers implement such authorization'
(4.10.1)" [mqtt5 §10]. The credential and the verdict are two unrelated mechanisms: the first
is a handshake field, the second a reason code on the operation. Two further facts are worth
carrying: an unauthorized publish at QoS 0 gives "no signal, or DISCONNECT 0x87", so "the
publisher cannot distinguish 'not authorized' from 'delivered'" [mqtt5 §8]; and an
authentication failure may legitimately be silence, because "the spec recommends silence on
public networks so as not to reveal that an MQTT server is present (3.1.4, 3.2.2.2, 4.12)"
[mqtt5 §8]. The exchange is also widely unimplemented: AWS IoT Core "does not support MQTT 5
**AUTH** or server redirection" [mqtt5 §13, source 60], NanoMQ lists it unsupported [mqtt5 §13,
source 58], and neither Rust broker offers it, "so 4.12's AUTH exchange is unexercised against
both" [mqtt5 §13].

**NATS puts the credential on the connection and the policy in the server.** A connection
authenticates with configured credentials, NKeys signing a server-provided nonce, JWTs, TLS
certificates or an authorization callout, "then are authorized by account and subject
permissions" [nats §12/P14], [nats §10]. The model's units are not wire fields: "An account is
an isolated subject namespace and tenant boundary… A user authenticates into one account…
Publish and subscribe permissions are subject-pattern grants and denies for that user/account…
Accounts can explicitly export and import selected subjects for cross-account communication"
[nats §10]. The callout is the part B-066 names, and it is delegation, not carriage: "An
authorization callout allows the server to ask an external service to authenticate a client
connection" [nats §10] — the same shape as ZAP, one protocol layer up. The verdict surface is
an operation-level refusal: "The server denies a publish or subscription outside the
authenticated user's subject permissions… the operation is rejected; `-ERR` may be sent and the
connection may be closed depending on the error" [nats §8]. And the identity does not travel:
"The Core message wire format does not attach the authenticated connection identity to every
delivered message" [nats §10].

**AMQP 1.0 has the credential as a *layer* and no authorization model at all.** SASL is selected
by protocol-id `%d3` in the 8-byte header, run in type-`0x01` frames, and followed by a fresh
`AMQP %d0 1.0.0` exchange [amqp10 §1, Part 5 §5.3]; the server advertises
`sasl-server-mechanisms` in decreasing preference and `sasl-outcome` carries `ok` (0), `auth`
(1), `sys` (2), `sys-perm` (3) or `sys-temp` (4) [amqp10 §1, Part 5 §5.3.2-5.3.3, §5.3.3.6].
Then: "**Authorization model.** The core standard has none. It supplies the error conditions
(`amqp:unauthorized-access`, `amqp:not-allowed`, `amqp:resource-locked`) and a place to refuse
— `attach` with a null terminus, or `detach(error=...)` — and stops [3 §2.6.3, §2.8.15]"
[amqp10 §10]. The nearest thing to a per-resource credential is an extension, not the standard,
and it is B-066's second option in literal form: claims-based security is "not in the core
standard, and two incompatible shapes exist", the OASIS draft defining "a connection-scoped
token cache, default node address `$cbs`… a `set-token` message with `subject`=`set-token`…
success signalled by the ordinary `accepted`/`rejected` disposition" [amqp10 §10, source 11].
A token posted as an ordinary message to a well-known address, answered by an ordinary
disposition: an application handshake on a separate path, carried by the existing message
machinery rather than by a new field.

**The catalogue's own summary.** D6: "**Authenticated identity is per connection everywhere;
per-message identity is optional, self-asserted, or absent.**… No sheet reports per-message
authorization anywhere: granularity is per connection (ZeroMQ's ZAP domain), per entity name
(RabbitMQ), per subject pattern (NATS), per topic (MQTT), or per resource scope (Kafka ACLs)"
[SYNTHESIS §2 D6]. Weida is already on the strong side of that comparison: "This is the one
dimension where weida is *stronger* than every sheet's answer to P14 (D6): the per-transfer
identity is proved rather than self-asserted" [SYNTHESIS §5 P14].

**What weida's wire already offers as a refusal, per path.** Dispatch is "a function of the
stream kind and the addressed path", and "a refusal is never a connection error: the connection
survives all of it": a misrouted stream gets `UNSUPPORTED` and an unregistered path
`UNKNOWN_ENDPOINT`, as a stop code on a uni stream and as an ERROR frame on a bidi one
[PROTOCOL §9.4]. ERROR codes are `1` UNKNOWN_ENDPOINT, `2` REJECTED, `3` UNSUPPORTED, `4`
INTERNAL, `5` NO_REPLY [PROTOCOL §6.3]; the QUIC application error codes run `0`-`9` and include
`7` REJECTED and `8` UNKNOWN_ENDPOINT [PROTOCOL §7].

**And where it offers nothing.** SUBSCRIBE has no reply half: "A SUBSCRIBE arrives on a uni
stream and so has no reply half to carry an ERROR; the connection is the only granularity
available", which is why exceeding `max_subscriptions` closes the connection with
`LIMIT_EXCEEDED`; "SUBSCRIBE for a path no publisher has registered yet MUST still be
recorded"; and "A peer that registers no publishers MAY ignore these frames" [PROTOCOL §6.4].
A publisher path also refuses bidi streams outright [PROTOCOL §9.4], so on a pure Pub/Sub path
there is no exchange in which a client could present anything.

**Paths are not a permission language.** "Endpoint paths are opaque identifiers": `EndpointAddr`
"validates bytes and length only; the endpoint namespace in `weida` is a flat map keyed by the
exact path string — no splitting, no prefix match, no wildcards", and topic filters are a
separate namespace matched only against topics, never against paths [INVARIANTS],
[0007](0007-topic-namespace.md) §4.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — a token in HELLO, validated by the acceptor** | a new HELLO key carries an opaque bearer token; the acceptor validates it and fails negotiation if it does not like it | ZMTP PLAIN's user/password [zeromq §10, 24/ZMTP-PLAIN]; MQTT's CONNECT User Name/Password (3.1.2.9) [mqtt5 §10]; AMQP's SASL layer [amqp10 §1, Part 5 §5.3] | Wrong scope for the question: HELLO names no path [PROTOCOL §6.1], and giving it one is 0011's rejected option B [0011 §3]. Costs a wire key and a protocol version (§4.6), makes the acceptor's policy a decoder concern, and cannot be revised mid-connection, because QUIC forbids post-handshake client authentication (RFC 9001 §4.4) [0008 §2] and weida has no credential-refresh frame |
| **B — an application handshake on a separate endpoint path** | the application registers a path, the client posts its token there as an ordinary exchange, and the acceptor associates the verdict with the peer it proved | AMQP CBS: a `set-token` message to the `$cbs` node, answered by an ordinary `accepted`/`rejected` disposition [amqp10 §10, source 11]; NATS' authorization callout, which asks an external service [nats §10]; ZAP's out-of-band handler [zeromq §10, 27/ZAP] | Needs no protocol change at all, so it is not a wire option: it is what an application does on top of the wire. Its real costs are the ones §4.5 names — the association is by fingerprint, so an anonymous client cannot carry a verdict from one connection to another, and a Pub/Sub path offers no exchange to carry the token on |
| **C — neither: the acceptor decides, per path, on the identity it already has** | no credential on the wire; authorization is a local decision on `(proved peer, the path the stream dispatched to)`, expressed with the existing refusals | AMQP 1.0's core standard, which "has none… supplies the error conditions… and a place to refuse… and stops" [amqp10 §10]; ZAP, whose decision is outside ZMTP [zeromq §10]; iroh, where "it is up to the application to decide if a particular peer is allowed to connect or not" [prior-art §8 lesson 9] | The acceptor must hold its own policy table; weida ships no hook for it (master doc §46's hooks are unimplemented) [ARCHITECTURE §5]. A refusal on one-way traffic may not be observed at all [0005]. A subscription cannot be refused individually [PROTOCOL §6.4] |
| D — a capability code for authorization | reuse HELLO keys `3`/`4` to let a peer declare and require "I authenticate" | the mechanism exists and is empty: "Capability code assignment is unspecified in v0" [PROTOCOL §6.1], [PROTOCOL §11] | A capability declares *support*, not a grant; it answers "does the peer speak it" and never "may this peer attach here". It would still need option A's field to carry anything |
| E — a permission grammar over paths | the acceptor is configured with path patterns, NATS-style subject permissions | NATS accounts and subject-pattern grants and denies [nats §10] | Refused by the invariant, not by taste: paths are opaque identifiers matched exactly, with "no splitting, no prefix match, no wildcards" [INVARIANTS], [0007 §4]. NATS can do it because subjects are a token hierarchy by specification; weida's paths are bytes |

## 4. Decision

**Option C. A weida handshake carries no application credential, and it does not grow one for
this.** The handshake proves a key; authorization is the acceptor's decision, taken per
endpoint path on the identity that already arrives with every transfer, and the protocol
contributes only the refusals it already has. [PROTOCOL.md](../PROTOCOL.md) §11's
"Authentication beyond TLS. No application-level authentication fields in HELLO" is confirmed
as a decision with a reason rather than an unexamined omission.

**4.1 Why not option A, in the form the question asks.** The request is for something that says
"*which* client may attach **to which endpoint**" [zeughaus-video, item 3]. A HELLO field cannot
say that, because HELLO names no path [PROTOCOL §6.1] and a HELLO key that named one was already
examined and rejected in [0011](0011-answered-where-it-arrived.md) §3 option B, for reasons that
are unchanged: the tiers become distinguishable on the wire, the path would be stated twice and
the two statements can disagree, and a remote-keyed table needs a new named bound. A token in
HELLO answers "may this peer connect at all", which is the granularity `Trust` pinning already
has [ARCHITECTURE §5] — and which every precedent that carries a handshake credential is stuck
with: ZAP is "per connection, at handshake time", with the domain "the only scoping handle"
[zeromq §10]; NATS authenticates the connection and authorizes by account and subject
permissions held in the server [nats §12/P14]; MQTT's credential is in CONNECT while its
verdicts are reason codes on SUBSCRIBE and PUBLISH (2.4, 4.10.1) [mqtt5 §10]. The catalogue is
unanimous in shape: the credential admits a connection, and the per-resource decision is made by
the acceptor's own policy and reported per operation [SYNTHESIS §2 D6].

**4.2 And what a token would have bought is available without it.** The honest argument for
option A is that a bearer token separates *who holds the key* from *who holds the grant*: a
third party (zeughaus' SpacetimeDB) issues a session token, and the acceptor need not know the
client's key in advance. That separation does not require a wire field, because weida's
transfers are already the carrier an application needs — an exchange is one bidi stream and its
reply is the verdict [PROTOCOL §9.1] — which is precisely the shape AMQP's claims-based security
took when it needed the same thing: a `set-token` message to a well-known node, answered by an
ordinary disposition, as a draft extension rather than a change to the core standard [amqp10
§10, source 11]. What weida would add by putting the token in HELLO is not capability but
placement: the acceptor's policy would move into the decoder, before negotiation, on a frame
that cannot name what is being asked for.

**4.3 What an acceptor MAY decide, per endpoint path.** All of this is available in v0 and
needs no wire change:

- **Whether the path exists.** Registration is the coarsest authorization there is, and a
  non-registration answers `UNKNOWN_ENDPOINT` [PROTOCOL §9.4], which does not distinguish "no
  such endpoint" from "not for you". That indistinguishability is a property to keep, not a
  defect: MQTT recommends exactly this silence "so as not to reveal that an MQTT server is
  present (3.1.4, 3.2.2.2, 4.12)" [mqtt5 §8].
- **Whether to accept an individual transfer or exchange that dispatched to the path**, deciding
  on `(the peer proved in the handshake, the path the stream arrived at)`. The refusal is
  `ERROR {REJECTED}` on the reply half of an exchange, or `STOP_SENDING(REJECTED)` on a one-way
  stream [PROTOCOL §9.4], [PROTOCOL §6.3], [PROTOCOL §7].
- **Whether a given subscriber gets a given topic**, by declining to write a copy. That is a
  local fan-out decision inside a path the subscriber is already allowed to hold, and the
  subscriber observes silence.
- **Whether the peer may connect at all**, with the `Trust` pin list on a binding —
  connection-wide and all-or-nothing [ARCHITECTURE §5] — surfacing at the dialling side as
  `Error::Untrusted(fingerprint)`, which carries what answered so an operator can pin it
  [PATTERNS §1.9].

**4.4 What an acceptor MUST NOT do.**

- **It MUST NOT authorize on anything claimed.** The *subject* of the decision is the proved
  peer and nothing else: not a header field, not the `producer` key of [PROTOCOL.md](../PROTOCOL.md)
  §6.2, and not a PID, which "is an observation that MUST NOT be authorized on" [INVARIANTS],
  [0010 §4.4]. A claimed value may be the *object* of a decision — a topic or a path is chosen
  by the sender and may perfectly well be restricted — but it is never evidence about the
  subject.
- **It MUST NOT read structure into a path.** Paths are opaque identifiers matched exactly, with
  "no splitting, no prefix match, no wildcards" [INVARIANTS], [0007 §4]. weida does not grow a
  NATS-style subject-permission grammar over them [nats §10]; an application that wants a
  hierarchy computes it in its own code, and the topic filter grammar of [PROTOCOL.md](../PROTOCOL.md)
  §6.4 stays a subscription selector, never a permission language.
- **It MUST NOT expect a refusal to be observed on one-way traffic.** [0005](0005-refusal-race.md)
  already decided that a refusal is guaranteed only beyond the peer's stream window or in
  Req/Rep, so a Push or Pub sender may see success for a transfer the acceptor refused. This is
  not a weida quirk: at QoS 0 "the publisher cannot distinguish 'not authorized' from
  'delivered'" [mqtt5 §8]. An authorization scheme that needs the sender to *learn* it was
  refused must use Req/Rep.
- **It MUST NOT treat two anonymous connections as one peer** [0008 §4.2]. An acceptor doing
  per-path authorization over anonymous clients has nothing to authorize on —
  `IncomingMeta::peer` is `None` [PATTERNS §1.9] — so it must require a client identity
  (`ServerTls::require_client`) or restrict every decision to the single connection it was taken
  on.
- **It MUST NOT take a decision on one connection and signal it on another.** By
  [0011](0011-answered-where-it-arrived.md) §4.1-§4.2 a verdict about a path rides that path's
  connection, because that is where the frame it concerns arrived. There is no peer-wide
  refusal channel, and inventing one is not a small addition: a peer-scoped authorization frame
  is literally one of the three revival conditions 0011 §4.3 names for the parked control tier
  ("a peer-wide pause/resume, **an authorization refresh**, a liveness signal with application
  meaning").

**4.5 What this costs, named rather than hidden.** For a Req/Rep or a Push path, option B is
free: the client posts its token in an exchange and the acceptor keys its table on the proved
peer. For **Pub/Sub it is not free**, and this is the one place where "neither" has a real
price. SUBSCRIBE has no reply half, so a subscription can be refused only by dropping it
silently or by closing the connection with `LIMIT_EXCEEDED` [PROTOCOL §6.4]; a publisher path
refuses bidi streams [PROTOCOL §9.4], so there is no exchange on that connection in which a
token could be presented; and weida has no session, so a verdict obtained elsewhere attaches to
another connection only through the proved fingerprint [0008 §4.2], [0008 §4.5], which an
anonymous client does not have. An application that wants token-authorized *subscriptions*
therefore has exactly two shapes: require client identity and key the policy on the fingerprint,
or register a companion Req/Rep path for the token and accept per-connection scope. For
zeughaus' stage 3 this is the whole answer: a `/auth` exchange plus `ServerTls::require_client`,
with the session token validated once and the verdict held against the peer's fingerprint.

**4.6 If this is reopened, here is the bill.** Naming it now is what makes the decision
reversible rather than merely stated.

| Piece | What it would take | Who allocates it |
| --- | --- | --- |
| A HELLO credential field | **HELLO key `7`**, the first free key in the §6.1 table (`0`-`6` are taken), typed `bstr` for an opaque token | [PROTOCOL.md](../PROTOCOL.md), in a wire protocol version that defines it. The key space `0..=63` "is reserved for this specification", and an out-of-tree extension "MUST use keys `64` and above" [PROTOCOL §5] — so an application may never allocate this itself |
| A per-transfer credential | **DATA key `8`**, the first free key in the §6.2 table (`0`-`7` are taken) | the same. It would also be the first weida header field carrying authority, against the rule that identity is proved and never claimed [0008 §4.1] — no sheet reports per-message authorization at all [SYNTHESIS §2 D6] |
| A credential-refresh exchange | **frame kind `6`**: kinds `0`-`4` are in use and kind `5` is reserved for the L2 credit frame [PROTOCOL §4], [PROTOCOL §11] | the same. It names only the peer, so by 0011 §4.2 it is peer-scoped, and building it revives the parked control tier of 0011 §4.3 |
| A distinct refusal code | ERROR code `6` (codes `1`-`5` are in use, §6.3) and QUIC application error code `10` (codes `0`-`9` are in use, §7) | the same — and §4.7 argues against wanting one |
| Requiring it of a peer | a capability code, via the existing HELLO keys `3`/`4` mechanism, which is specified and empty [PROTOCOL §6.1], [PROTOCOL §11] | [PROTOCOL.md](../PROTOCOL.md) §6.1 |

**4.7 Consequence for `weida-core` and for the error vocabulary: nothing new, deliberately.**
No type is added to `weida-core` by this note. The input an acceptor authorizes on already
exists as `IncomingMeta::peer`, a `PeerIdentity` that is a key on QUIC, a `Local { uid, gid,
pid }` principal on `AF_UNIX`, a `Windows { sid, pid }` principal on a named pipe (B-039) and
`None` for an anonymous or in-process peer
[GUARANTEES.md](../GUARANTEES.md) §6, [0010 §4.4]. No `Credential` type, no `Authorizer` trait
and no `Error::Unauthorized` variant. A refusal uses the variants `crates/core/src/error.rs`
already defines, and which one is a question of *outcome*, because the enum is "deliberately
outcome-shaped rather than cause-shaped" (`crates/core/src/error.rs`, the `Error` doc comment):

| Refusal | Wire | Sender observes |
| --- | --- | --- |
| this peer may not use this path | `ERROR {REJECTED}` / `STOP_SENDING(REJECTED)` | `Error::Rejected` |
| the path is hidden from this peer | `ERROR {UNKNOWN_ENDPOINT}` / `STOP_SENDING(UNKNOWN_ENDPOINT)` | `Error::UnknownEndpoint` |
| this key is not trusted by this binding | TLS failure | `Error::Untrusted(Fingerprint)` |
| a hypothetical HELLO credential is refused (§4.6) | `CONNECTION_CLOSE(NEGOTIATION_FAILED)` | `Error::Negotiation(String)` |
| too many subscriptions to hold | `CONNECTION_CLOSE(LIMIT_EXCEEDED)` | `Error::LimitExceeded` |

`Error::Rejected` is the right outcome for an authorization refusal precisely because it says no
more than "the peer refused the transfer": a distinct "not authorized" outcome would tell an
unauthorized sender which of its guesses was a real endpoint, and the precedent for staying
quiet is MQTT's own recommendation of silence [mqtt5 §8]. An application that wants to say why
has the ERROR frame's `message` field, "human-readable detail, not machine-interpreted"
[PROTOCOL §6.3].

**4.8 Status is `provisional` and why.** The answer as taken is docs-only and reversible: no
frame, no key, no type, no config default. It stays provisional because the question it closes
is wire-affecting in the other direction — if the answer were ever A, §4.6's bill falls due —
and that is the user's to confirm rather than an agent's to settle.

**4.9 What would make this wrong.** One combination, and it is checkable: a deployment that
needs a grant which (a) cannot be keyed on a proved key, because the issuer is a third party and
the client holds no certificate, *and* (b) must be revised mid-connection, because the grant
expires, *and* (c) must apply to **subscriptions**, where §4.5 shows v0 offers no in-band
exchange. Any two of the three are answerable today. All three together are what option A
exists for, and then the bill of §4.6 is paid — starting with HELLO key `7` and the control-tier
revival of 0011 §4.3.

## 5. Consequences and follow-ups

- **[PROTOCOL.md](../PROTOCOL.md) §11.** The existing entry "Authentication beyond TLS. No
  application-level authentication fields in HELLO" gains this note as its reason and its
  positive half: authorization is the acceptor's decision on the proved identity, expressed with
  §9.4's refusals, and HELLO key `7` is what a later version would take if that changes (§4.6).
- **[PROTOCOL.md](../PROTOCOL.md) §9.4.** The dispatch table is the authorization surface, and
  should say so: a registered path MAY refuse an accepted stream with `REJECTED`, and an
  acceptor that prefers to hide the path answers `UNKNOWN_ENDPOINT` instead (§4.3).
- **[ARCHITECTURE.md](../ARCHITECTURE.md) §5.** "Authentication is not authorization" is now
  backed by a decision; the paragraph gains the per-path rules of §4.3 and §4.4 and the note
  that master doc §46's hooks remain unimplemented **by decision**, not by omission.
- **[PATTERNS.md](../PATTERNS.md) §1.9.** Gains the application-side shape of §4.5: authorize in
  the handler on `meta().peer`, and for a token flow use a companion Req/Rep path plus
  `ServerTls::require_client`, because a verdict crosses connections only by the proved
  fingerprint. **[GUARANTEES.md](../GUARANTEES.md) §6**'s peer-identity row states what an
  identity is *not*: a grant.
- **[0011](0011-answered-where-it-arrived.md) §4.3.** Its "authorization refresh" revival
  condition is now a live possibility with a bill attached (§4.6) rather than a hypothetical.
- **Code.** No change in `crates/core` or `crates/weida`. `Error` gains no variant (§4.7).
- **A follow-up this note does not take:** SUBSCRIBE cannot be refused individually
  [PROTOCOL §6.4], so a publisher admitting some subscribers and not others has only silence or
  a connection close, where MQTT answers per filter with SUBACK 0x87 or 0x8F (3.9.3) [mqtt5 §8],
  [mqtt5 §10]. Whether weida wants a per-subscription verdict — a reply half for SUBSCRIBE, or a
  new frame — is a separate wire question, worth a backlog item, and is **not** decided here.
- **Backlog.** B-066 is answered by this note; no code item falls out of it, and the requester's
  stage 3 is unblocked in the shape §4.5 names.

## 6. What this note does not decide

- **Whether weida ever grows an authorization hook.** Master doc §46's hooks stay unimplemented
  [ARCHITECTURE §5]; this note says where the decision belongs, not that the framework offers a
  registration point.
- **Anything about how an application stores or validates a token.** Issuance, expiry, rotation
  and revocation are the application's, and weida contributes only the peer's proved name.
- **A per-subscription verdict** (see the follow-up above), and **multi-tenancy**: NATS-style
  accounts as an isolation boundary [nats §10] have no weida counterpart and none is proposed;
  a binding is the only isolation unit there is.
- **Anything about L2.** A broker hop may need a richer model, exactly as it needs the session
  0008 §4.5 refused at L0; that is Phase 6's to define and may not be approximated on the v0
  wire [PROTOCOL §11].

## 7. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §2.2, §4, §5, §6.1, §6.2, §6.3, §6.4, §7, §9.1,
§9.4, §11; [PATTERNS.md](../PATTERNS.md) §1.9; [GUARANTEES.md](../GUARANTEES.md) §6;
[ARCHITECTURE.md](../ARCHITECTURE.md) §5; [INVARIANTS.md](../INVARIANTS.md);
[requirements/zeughaus-video.md](../requirements/zeughaus-video.md) ("Integration", "What weida
would need" item 3); [BACKLOG.md](../BACKLOG.md) B-066; [0005](0005-refusal-race.md);
[0007](0007-topic-namespace.md) §4; [0008](0008-session-identity.md) §4.1, §4.2, §4.5, §4.7;
[0010](0010-local-transport.md) §4.4; [0011](0011-answered-where-it-arrived.md) §3, §4.1-§4.3.

Code read: `crates/core/src/error.rs` (the `Error` enum and its outcome-shaped doc comment;
variants `Rejected`, `UnknownEndpoint`, `Untrusted`, `Negotiation`, `LimitExceeded`).

Research sheets: [zeromq.md](../research/zeromq.md) §1 (mechanism agreement, authentication
step), §10 (PLAIN, CURVE, ZAP, authorization granularity), §12/P14, §13 (measured libzmq 4.3.5
behaviour, source 41), with RFCs 24/ZMTP-PLAIN, 25/ZMTP-CURVE, 26/CURVEZMQ and 27/ZAP;
[mqtt5.md](../research/mqtt5.md) §1 (handshake, negotiation, authentication step), §8 (failure
table), §10 (basic authentication 3.1.3.5-3.1.3.6, enhanced authentication 4.12,
re-authentication 4.12.1, granularity 2.4 and 4.10.1), §12/P14, §13 (AWS IoT Core and NanoMQ
without AUTH, no Rust broker implementing 4.12);
[nats.md](../research/nats.md) §8 (permission violation), §10 (accounts, subject permissions,
authorization callout), §12/P14; [amqp10.md](../research/amqp10.md) §1 (security layers,
authentication), §10 (authorization model, claims-based security), §12/P14, with AMQP 1.0 Part 2
§2.6.3, §2.8.15 and Part 5 §5.3-§5.3.3.6;
[prior-art.md](../research/prior-art.md) §8 lesson 9 (iroh: the application decides who may
connect); [SYNTHESIS.md](../research/SYNTHESIS.md) §1 P14, §2 D6, §5 P14.
