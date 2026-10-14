# 0011 — Server-initiated traffic is answered where the registration arrived

- **Status:** accepted
- **Date:** 2026-09-11
- **Related:** [0002](0002-control-and-bulk-separation.md) §6.3, §7; [0003](0003-credit-unit.md)
  §4.2; [0008](0008-session-identity.md) §4.2; [PROTOCOL.md](../PROTOCOL.md) §2.5, §6.4, §9.5,
  §11; P4 and P12 of [SYNTHESIS.md](../research/SYNTHESIS.md) §8.2.

## 1. The question

[0002](0002-control-and-bulk-separation.md) §7 left one thing open: "how a bulk connection
names its control connection (a HELLO field, or the fingerprint alone) is a follow-up for
[PROTOCOL.md](../PROTOCOL.md) §2". [0008](0008-session-identity.md) §4.2 answered half of it —
the fingerprint binds a peer's connections and no HELLO field names another connection — and
B-017 walked into the other half, which nobody had noticed was a different question:

**A publisher writes fan-out on the connection the SUBSCRIBE arrived on. Once a peer holds one
connection per dialled endpoint path, which of them is that supposed to be?**

The binding rule of 0008 says which connections belong to one peer. It does not say which of
them a *server-initiated* message should be written on, and nothing on the wire does either: a
SUBSCRIBE header carries an endpoint path and a topic filter and nothing else
([PROTOCOL.md](../PROTOCOL.md) §6.4). The question is not academic. It is what blocks
0002 §6.3's control connection: if SUBSCRIBE moves to a per-peer control connection, the
publisher that must answer it has a peer and no route.

The decision is therefore: **what makes a connection the right one for traffic this side
originates?**

## 2. The evidence, condensed

**Nothing in QUIC answers it.** ALPN selects an application protocol per connection handshake,
not a role within a peer relationship [quic-standards §2.2]; connection IDs are opaque, carry
no specified meaning and exist for transport state and migration [quic-standards §2.1]; stream
IDs encode only the initiator and the direction, and stream priority "is not exposed to either
the network or the receiver" [quic-standards §2.3]. A peer-level route must therefore be
application protocol state, invented here or not at all [inference].

**Every messaging protocol in the catalogue answers it the same way: the registering side's own
action establishes the route.** The mechanisms differ; the shape does not.

- **EMQX's MQTT-over-QUIC is this exact case, decided.** The client opens the data streams and
  chooses their grouping; the broker's rule is that "server-initiated MQTT PUBLISH goes on the
  stream where it received that topic's subscription", and its PUBACK is expected back on that
  same stream [prior-art §3]. The subscribe *arrival* is the mapping authority — not a lookup
  the broker performs from a path.
- **NATS Core** binds interest to the connection it was declared on, with a client-chosen `sid`
  that the server echoes in `MSG` on that same connection [nats §4]. The identifier selects a
  subscription *within* the connection; it never selects between connections.
- **RabbitMQ** consumer tags are channel-scoped, the channel belongs to one connection, and
  `basic.deliver` carries the tag on the channel the `basic.consume` arrived on; a tag cannot be
  used on another channel [rabbitmq-amqp091 §4, §12/P6].
- **AMQP 1.0** is the one protocol with a first-class name for the route — a named link with a
  handle — and even there a link is active on one connection at a time, and re-attaching the
  same name elsewhere *steals* it rather than addressing it from outside [amqp10 §2, §4].
- **MQTT 5** is the apparent counter-example and is not one: a subscription belongs to a
  session, and the broker owns the session-to-current-connection association. The Subscription
  Identifier tells the *receiver* which subscription caused a delivery; it does not let a
  publisher name a transport connection [mqtt5 §4, §6]. Being able to survive a connection
  change is exactly what a session buys, and weida has no session ([0008](0008-session-identity.md)
  §4.5, [PROTOCOL.md](../PROTOCOL.md) §11).
- **Kafka does not push at all**: a consumer fetches by offset, so the server never chooses a
  delivery connection [kafka §2, §6].
- **MOQT** binds a request and its responses to the request stream, and object delivery follows
  the subscription's own track/group/subgroup mapping [prior-art §4].

**What the split costs where it exists.** EMQX's per-stream split removes head-of-line blocking
and pays for it in recovery: cross-stream order is not guaranteed and an unexpectedly closed
data stream loses QoS 1/2 in-flight state [prior-art §3]. Its motivating measurement is the
argument *for* separation: on a constrained link a single stream let one large PUBLISH starve
keep-alive into a reconnect loop, while separate streams kept small-topic traffic flowing
[prior-art §3].

**And what weida already pays.** One connection per dialled path costs ~1.1 ms of handshake and
under a megabyte of resident state ([IMPLEMENTATION.md](../IMPLEMENTATION.md) §4, B-011,
B-012); a control connection per peer costs that again.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — answered where it arrived** | A registration binds its route: server-initiated traffic for a subscription is written on the connection its SUBSCRIBE arrived on, so a path-scoped control frame must ride the connection of the path it names | EMQX's subscription-stream rule [prior-art §3]; NATS `sid` [nats §4]; RabbitMQ channel-scoped tags [rabbitmq-amqp091 §4] | An endpoint that publishes *and* subscribes on one path can queue its own SUBSCRIBE behind its own payload; and the control tier loses its cargo (§4.3) |
| B — a HELLO key naming the dialled path | Each connection declares the path it was dialled for; the server keeps a peer → path → connection table and writes fan-out there | iroh's one-ALPN-per-connection, as a role declaration at handshake [prior-art §2] | The tiers become distinguishable on the wire, against [PROTOCOL.md](../PROTOCOL.md) §2.5; the path is then stated twice (HELLO and SUBSCRIBE) and the two can disagree; a remote-keyed table needs a new named bound [INVARIANTS]; and it races — a SUBSCRIBE on the control connection may arrive before that path's connection exists, or after it died, so the subscription must be parked or resolved lazily |
| C — fan-out on the control connection | Subscriptions and their deliveries both ride the peer's control connection | none | Payload on the tier whose whole purpose is to carry none; it defeats 0002 rather than implementing it |
| D — the fingerprint alone | The publisher picks any live connection of the peer that proved the same key | 0008 §4.2 for *peer* binding | Ambiguous by construction once a peer holds several connections; 0008 binds peers, not routes, and reading it as a route makes fan-out land on an arbitrary path's window |
| E — a connection identifier in the header | SUBSCRIBE names the connection that should receive the fan-out | AMQP 1.0's link name [amqp10 §2] | To name a connection a peer must have a stable identifier for it, which is a session identifier — excluded from v0 by [PROTOCOL.md](../PROTOCOL.md) §11 and closed by 0008 §4.5. AMQP pays for its link names with durable termini and recovery rules weida has no layer for |

## 4. Decision

Option A.

**4.1 A registration is answered on the connection it arrived on.** Traffic this side
originates on behalf of a peer's registration — today only Pub/Sub fan-out — MUST be written on
the connection that carried the registration. No header field selects a connection, and none is
added. This is what the code already does; what changes is that it is now a rule with a reason
rather than an accident of implementation.

**4.2 Therefore a path-scoped control frame rides the connection of the path it names.**
SUBSCRIBE and UNSUBSCRIBE name an endpoint path, so they belong on that path's connection.
[PROTOCOL.md](../PROTOCOL.md) §2.5's list of control-tier traffic is corrected accordingly:
they are not control-tier traffic, because moving them there would separate a subscription from
the only route to its subscriber.

The general form, which is the rule a later frame is measured against: **a frame that names a
path is path-scoped and rides that path's connection; a frame that names only the peer is
peer-scoped and may ride a per-peer connection.** Scope is decided by what the frame names, not
by how big or how urgent it is.

**4.3 The control tier is parked, not refuted.** Applying §4.2 to every frame weida has or has
reserved leaves it empty:

- HELLO is peer-scoped, but every connection performs its own ([PROTOCOL.md](../PROTOCOL.md)
  §2.2), so a control connection carries no HELLO that protects anything.
- SUBSCRIBE and UNSUBSCRIBE are path-scoped by §4.2.
- The reserved credit frame of [PROTOCOL.md](../PROTOCOL.md) §11 is granted "per subscription
  (or queue, in L2's vocabulary)" [0003 §4.2]. A subscription names a path, so per-subscription
  credit is path-scoped and rides the subscription's connection. **This amends 0003 §4.2**,
  which put it on the control connection: the frame's own scope decides, and 0003's reason for
  naming the control connection — that credit must be withdrawable "independent of which bulk
  connection carries the deliveries" — is answered better by §4.1, since the connection that
  carries the deliveries is the one whose credit is being withdrawn.

So no frame that exists or is reserved needs a per-peer connection, and building one would cost
a handshake and a timer pair per peer to carry nothing. The tier is not wrong — the isolation it
offers is real for the case §4.4 names — so it is parked with an explicit revival condition
rather than dropped: **a genuinely peer-scoped, latency-sensitive frame** (a peer-wide
pause/resume, an authorization refresh, a liveness signal with application meaning), **or a
measurement showing the residual coupling of §4.4 hurting a real workload.**

**4.4 The residual coupling is named, not hidden.** With one connection per path, a SUBSCRIBE
can only be delayed by payload on *its own* path, from *its own* side — an endpoint that both
publishes and subscribes on one path. A pure subscriber writes nothing on that connection and a
pure publisher sends no SUBSCRIBE, so neither is affected. This is narrower than the coupling
MQTT forbids [mqtt5 §5] and narrower than what EMQX measured [prior-art §3], and it is stated
in [PATTERNS.md](../PATTERNS.md) §1.3 and [GUARANTEES.md](../GUARANTEES.md) §6 rather than left
for a user to find under load.

**4.5 `control_isolated` keeps its meaning and stays `No`.** The dimension declares that
control traffic cannot stall behind bulk ([0002](0002-control-and-bulk-separation.md) §6.1).
Under §4.4 that is true of everything except a dual-role endpoint's own path, so a v0 peer
declares `No` and does not claim it. The declaration remains the thing an adapter can
*require*, which is what SYNTHESIS §8.2 asked for — and an adapter that needs it still gets it
the way 0002 §6.7 says: one weida connection per foreign session, which under §4.1 is one
connection per path, which is what the pool already does.

**4.6 What would make this wrong.** If a future frame is peer-scoped *and* must be answered on
a specific connection, §4.1 has nothing to say and option B becomes live. The test to apply
then: does the frame name a path? If it does, §4.2 settles it; if it does not, it belongs to the
parked tier of §4.3 and revives it.

## 5. Consequences and follow-ups

- **[PROTOCOL.md](../PROTOCOL.md) §2.5.** SUBSCRIBE and UNSUBSCRIBE leave the control tier's
  traffic list; the normative rule of §4.1 and the scope rule of §4.2 are stated there, and the
  section already records that the control tier is not built.
- **[0003](0003-credit-unit.md) §4.2** carries an amendment marker pointing at §4.3: the
  reserved credit frame is path-scoped, not control-tier.
- **[0002](0002-control-and-bulk-separation.md) §6.3, §7** carry an amendment marker: the
  control connection is parked under §4.3 with its revival condition, and §7's open follow-up is
  closed.
- **[SYNTHESIS.md](../research/SYNTHESIS.md) §8.2's closing paragraph** is amended: the
  separation that was built is the per-path one, and the per-peer control connection named there
  is parked.
- **Backlog.** B-045 (control connection per peer) moves from `blocked` to `parked` with §4.3's
  condition as the trigger. No new code item falls out of this note: §4.1 is what the code does.
- **What is still not decided anywhere:** whether a *stable* subscription that survives a
  connection change exists at all. That needs a session, which is L2's to define
  ([0008](0008-session-identity.md) §4.5); this note deliberately decides only where traffic
  goes while the connection lives.

## 6. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §2.2, §2.5, §6.4, §9.5, §10, §11;
[PATTERNS.md](../PATTERNS.md) §1.3, §1.4; [GUARANTEES.md](../GUARANTEES.md) §3, §6;
[INVARIANTS.md](../INVARIANTS.md); [ARCHITECTURE.md](../ARCHITECTURE.md) §5;
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §4 (B-011, B-012), §5;
[0002](0002-control-and-bulk-separation.md) §6, §7; [0003](0003-credit-unit.md) §4.2;
[0007](0007-topic-namespace.md) §4.6; [0008](0008-session-identity.md) §4.2, §4.5.

Code read: `crates/weida/src/pubsub.rs` (the writer task holds the `ConnHandle` a SUBSCRIBE
arrived on), `crates/weida/src/endpoint.rs` (`Subscriber::connect`, `broadcast`, `SubState::drop`),
`crates/weida/src/conn.rs` (`handle_subscription`), `crates/protocol/src/header.rs`
(`SubscriptionHeader` keys `0`/`1`, HELLO keys `0`-`6`).

Research sheets: [prior-art.md](../research/prior-art.md) §1 (Zenoh traffic classes), §2 (iroh:
one ALPN per connection), §3 (EMQX: control and data streams, the subscription-stream rule, the
constrained-link measurement), §4 (MoQ: control stream pair, request streams, subgroup
mapping); [quic-standards.md](../research/quic-standards.md) §2.1, §2.2, §2.3;
[mqtt5.md](../research/mqtt5.md) §4, §5, §6; [amqp10.md](../research/amqp10.md) §2, §4;
[rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §4, §12/P6;
[nats.md](../research/nats.md) §4; [kafka.md](../research/kafka.md) §2, §6;
[SYNTHESIS.md](../research/SYNTHESIS.md) §7.2, §8.2.
