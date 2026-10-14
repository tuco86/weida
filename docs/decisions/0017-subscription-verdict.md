# 0017 — A per-subscription verdict — the silence stays, and it is now defined

- **Status:** provisional
- **Date:** 2026-09-13
- **Related:** B-193; [0015](0015-peer-authorization.md) §4.3-§4.5, §5;
  [0011](0011-answered-where-it-arrived.md) §3, §4.1-§4.3; [0005](0005-refusal-race.md);
  [0003](0003-credit-unit.md) §4.2; [0007](0007-topic-namespace.md) §4;
  [0012](0012-local-connection-grouping.md) §4.4; [0016](0016-conflation.md) §4.6;
  [PROTOCOL.md](../PROTOCOL.md) §3.2, §4, §4.1, §5, §6.1-§6.5, §7, §9.4, §9.5, §11;
  [PATTERNS.md](../PATTERNS.md) §4, §4.1; [GUARANTEES.md](../GUARANTEES.md) §6;
  [INVARIANTS.md](../INVARIANTS.md); [libraries/zmq.md](../libraries/zmq.md) §9.

## 1. The question

B-193, verbatim in its acceptance line: "**SUBSCRIBE cannot be refused individually**
([PROTOCOL.md](../PROTOCOL.md) §6.4 gives it no reply half), so a publisher that admits some
subscribers and not others has only silence or a connection close with `LIMIT_EXCEEDED`, while
MQTT answers per filter with SUBACK reason codes `0x87`/`0x8F` (3.9.3) and NATS answers per
subject permission. The note decides whether weida grows a per-subscription verdict — a reply
half for SUBSCRIBE, a new frame kind, or nothing — with the cost of each named against the
wire… and against [0011]" [BACKLOG B-193].

The source is the follow-up [0015](0015-peer-authorization.md) §5 named and deliberately did
not take — "a separate wire question, worth a backlog item, and is **not** decided here" — and
whose §4.5 states the price it was recording rather than paying: "SUBSCRIBE has no reply half,
so a subscription can be refused only by dropping it silently or by closing the connection with
`LIMIT_EXCEEDED`" [0015 §5], [0015 §4.5].

This is a wire question, and it decomposes into two that are easy to conflate: **can a
publisher say no to one subscription**, and **can a subscriber tell that it was told no**. The
second is the one that costs a frame.

## 2. The evidence, condensed

**What v0 actually has, stated from the tables rather than from memory.** SUBSCRIBE is frame
kind `3` and UNSUBSCRIBE kind `4`; both are "header only, FIN directly after the header" and
both are permitted on **uni** streams only [PROTOCOL §4]. The header carries exactly two keys,
both required: `0` `endpoint`, a `tstr` capped at 512 B, and `1` `filter`, capped at 256 B
[PROTOCOL §6.4]. There is no subscription identifier of any kind — nothing like NATS'
client-chosen `sid` [nats §2, §4] or MQTT's Packet Identifier [mqtt5 §2] — so a subscription's
only name is the pair it was registered under. ERROR is kind `2` and is legal "**only on the
reply half of a bidi stream**" [PROTOCOL §6.3]; "An ERROR frame on a uni stream is a violation
(§3.2): an ERROR is the alternative to a reply, and it therefore has meaning only where a reply
would have gone" [PROTOCOL §4.1]. **There is therefore no frame a v0 peer may legally send in
answer to a SUBSCRIBE.**

**The two refusals that exist, in the protocol's own sentences.** Both are connection-fatal,
and both say why in the same words:

> "A filter that violates the grammar (`*` not alone in its segment, `#` not final or not
> alone) MUST close the connection with `PROTOCOL_VIOLATION`. Like an oversized filter, it is
> malformed content on a uni stream with no reply half to answer on, so the connection is the
> only granularity available" [PROTOCOL §6.4].

> "When accepting the frame would take the connection past `max_subscriptions` filters summed
> over all paths, the receiver MUST close the connection with `LIMIT_EXCEEDED`. A SUBSCRIBE
> arrives on a uni stream and so has no reply half to carry an ERROR; the connection is the
> only granularity available" [PROTOCOL §6.4].

`handle_subscription` in `crates/weida/src/conn.rs` says the same thing in the same words: it
closes with `codes::LIMIT_EXCEEDED` and the reason phrase "too many subscriptions on one
connection", commented "there is no reply half to answer with an ERROR frame".

**And the three silences, which are the actual subject of this note.** A SUBSCRIBE that is not
answered is not one case but three, and v0 makes them indistinguishable to the sender:
*recorded against a publisher that has not published*, since "SUBSCRIBE for a path no publisher
has registered yet MUST still be recorded"; *ignored entirely*, since "A peer that registers no
publishers MAY ignore these frames. Subscribing to a side that publishes nothing is useless,
not hostile" [PROTOCOL §6.4] — which the implementation does, logging "ignoring a subscription
frame: this side publishes nothing" and returning `Ok`; and *recorded, with every copy
declined*.

**What the fan-out side does today when it declines a copy.** It declines a *copy*, never a
subscription, and says nothing on the wire: "a copy that does not fit in
`subscriber_buffer_bytes` for that subscriber is dropped and counted in `dropped()`, and per
topic and cause in `dropped_on(topic)` / `drops()`… the publisher never blocks", while the
subscriber "silently misses messages: nothing on the wire says so" [PATTERNS §4]. Under `core`
the loss is invisible; under `PerProducer` it is a `Gap`, because "The copy is numbered before
fan-out" [GUARANTEES §6], [0016 §4.6]. The subscription survives all of it — "the subscription
survives its drops" [GUARANTEES §6]. **A declined copy is not a verdict**, and 0015 §4.3 calls
it what it is: "a local fan-out decision inside a path the subscriber is already allowed to
hold, and the subscriber observes silence" [0015 §4.3].

**There is also no local decision point, which is prior to the wire question.** `Publisher`
exposes `path`, `publish`, `publish_with_trace`, `open`, `open_with_trace`, `subscriber_count`,
`filter_count`, `dropped`, `dropped_on` and `drops` (`crates/weida/src/endpoint.rs`) — counters
and a fan-out handle, no hook — and `handle_subscription` records the filter and asks nobody. A
publisher application cannot refuse a subscription even in principle: no place to take the
decision, no identity handed to it, nothing to say it with. Any wire verdict is the *second*
half of a feature whose first half does not exist.

**The one place inside weida where silence already lost the argument — and it is local.** Over
a socket transport a publisher needs one of the subscriber's parked reverse connections per
copy [0012 §4.4]; with `max_parked_reverse = 0` there is no route at all, so
`Subscriber::connect` returns `Error::Unsupported` "rather than registering a subscription that
would be silent" [IMPLEMENTATION §1], and a subscriber that parks nothing "is refused when it
subscribes, because silence is not an answer" [GUARANTEES §6]. The decisive detail is *who*
refuses: the subscriber's **own** runtime, on local knowledge. Nothing crossed the wire.

**MQTT 5 — the only protocol in the catalogue that answers per subscription.** SUBACK carries
one reason code per Topic Filter: "SUBACK 0x87 or 0x8F for that filter only; other filters in
the same SUBSCRIBE may succeed (3.9.3)" [mqtt5 §8] — `0x87` Not authorized, `0x8F` Topic Filter
invalid — and the same codes stand among the "In-protocol verdict carriers" for a granularity
that is "per topic name and per topic filter for authorization, at publish and subscribe time"
(2.4) [mqtt5 §10]. SUBACK also reports a *granted* QoS rather than only a refusal (3.9.3)
[mqtt5 §6]. Two structural facts make this possible and both are absent here: SUBSCRIBE is
acknowledged by construction, a Packet Identifier freeing on SUBACK [mqtt5 §2], and the
acknowledgement is *positional* — fragile enough that a measured broker got the UNSUBACK count
wrong, where "position is the only binding between a code and a filter and a disagreeing count
is unreadable rather than merely surprising" [mqtt5 §13]. MQTT takes the opposite reflex about
the *connection*: silence "so as not to reveal that an MQTT server is present" (3.1.4, 3.2.2.2,
4.12) [mqtt5 §8].

**NATS — permissions per subject, and a verdict that is not per subscription.** "Publish and
subscribe permissions are subject-pattern grants and denies for that user/account", inside an
account that "is an isolated subject namespace and tenant boundary" [nats §10]. What the client
observes is not subscription-scoped: `SUB` travels client to server and `-ERR` server to client
[nats §1], "`-ERR` reports a protocol or authorization error and can be followed by disconnect"
[nats §6], and the failure table reads "The operation is rejected; `-ERR` may be sent and the
connection may be closed depending on the error" [nats §8]. The routine acknowledgement is
`+OK`, only in verbose mode — "the setting every current client" turns off [nats §1] — and it
"acknowledges a well-formed protocol operation, not application processing or subscriber
receipt" [nats §6]. NATS' in-band answer to an unauthorized subscribe is therefore closer to
weida's connection close than to MQTT's per-filter code.

**ZeroMQ — can decline to apply a subscription, cannot say why.** Filtering "SHALL happen at
the publisher side (the PUB or XPUB socket)"; XPUB delivers subscription commands to the
application and `ZMQ_XPUB_MANUAL` makes applying them the application's act [zeromq §4]. There
is no verdict of any kind, and 29/PUBSUB's whole vocabulary for an unwanted or slow subscriber
is a silent drop, with the publisher "not told" which peer lost what [zeromq §8]. Subscriptions
are additive and non-idempotent, so there is not even a stable object to answer about:
"Subscribing to 'A' and 'A' counts as two subscriptions, and would require two CANCEL commands
to undo" [zeromq §7].

**And this repository already refused that silence — for a foreign protocol.** `weida-zmq`
ships a capability libzmq's API does not have:

> "`XPubSocket::refuse` sends 37/ZMTP's `ERROR` command to the subscriber whose subscription
> arrived last… The protocol has always had the command — 'the peer SHALL treat an incoming
> ERROR command as fatal' — and libzmq's API exposes no way to send one: an XPUB application
> can decline to *apply* a subscription (`ZMQ_XPUB_MANUAL`) and cannot say why. That silence is
> unusable for an adapter, because a subscription silently ignored is a subscriber waiting
> forever for messages nobody will send" [zmq.md §9].

Read precisely, that refusal is **connection-fatal with a reason attached** rather than a
per-subscription verdict that leaves the connection alive: ZMTP's own rule is that the peer
treats ERROR as fatal. §4.8 returns to it, because it is where a different answer needs a reason.

**AMQP 1.0 — refusal at link establishment, the closest analogue in the catalogue.** A
receiver's `attach` can be refused: "A partner that will not provide a terminus answers with
that field null… and MUST then immediately detach [3 §2.6.3]" [amqp10 §4]. Its authorization
model is exactly that and no more: "The core standard has none. It supplies the error conditions
(`amqp:unauthorized-access`, `amqp:not-allowed`, `amqp:resource-locked`) and a place to refuse —
`attach` with a null terminus, or `detach(error=...)` — and stops" [amqp10 §10], costing "The
link or the connection" [amqp10 §8]. The price is a named, stateful, bidirectional link with
handles, termini and resumption rules [amqp10 §2] — a layer weida does not have [0008 §4.5].

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — nothing; the silence stays and is defined** | no wire change; the note states exactly what silence means and what a subscriber may not infer from it (§4.1, §4.2) | AMQP's core standard, which supplies error conditions and "a place to refuse" and "stops" [amqp10 §10]; MQTT's own recommendation of silence where the connection is concerned [mqtt5 §8] | A subscriber cannot distinguish "recorded, nothing published yet" from "ignored" from "every copy declined". The honest defence is that the third is not a refusal at all (§2) — but the first two remain a genuine ambiguity that only a frame can close |
| B — a reply half for SUBSCRIBE | kind `3` moves to a bidi stream whose reply half carries ERROR or an empty FIN | MQTT's SUBACK [mqtt5 §6]; AMQP's answering `attach` [amqp10 §4] | Breaks two rules at once: "A bidi stream MUST open with DATA on its initiating half. Any other kind there is a violation" and "The reply half of a bidi stream MUST carry either DATA or ERROR, **exactly one frame**" [PROTOCOL §4.1] — so the "empty FIN" half of the proposal is itself a spec change. Costs a bidi stream slot per subscription against `max_concurrent_bidi_streams`, which is weida's message credit [GUARANTEES §6], and collides with §9.4, where a publisher path answers a bidi stream with `UNSUPPORTED` |
| C — a new frame kind, a SUBACK equivalent | kind `6`, header-only on a uni stream, naming the subscription it answers and a code; path-scoped, so it rides that path's connection [0011 §4.1-§4.2] | MQTT SUBACK per filter (3.9.3) [mqtt5 §8]; NATS `-ERR` after `SUB` [nats §6] | The frame must repeat `endpoint` (512 B) and `filter` (256 B) to name its subject, because weida has no subscription identifier and a subscriber-chosen one is a remote-keyed table needing a named bound [INVARIANTS] — the same objection that killed [0011] §3 option B. And it is **not additive**: kind `6` is unknown to a v0 peer and closes the connection with `PROTOCOL_VIOLATION` [PROTOCOL §3.2] |
| **D — refuse at the connection level only, sharpened** | the status quo's `LIMIT_EXCEEDED` close, with §4.6 stating when it is legitimate and when it is an abuse | ZMTP's `ERROR` command, fatal by its own rule, which is what `weida-zmq`'s `XPubSocket::refuse` sends [zmq.md §9]; NATS' `-ERR`-then-close [nats §8] | All-or-nothing: one refused filter costs the peer every other subscription on that connection, and "All subscriptions held by a connection are dropped when that connection closes" [PROTOCOL §6.4]. Legitimate for overload, wrong for authorization (§4.6) |
| E — piggyback the verdict on the first fan-out copy or its absence | a welcome copy means yes; no copy means no | ZeroMQ's `ZMQ_XPUB_WELCOME_MSG`, honoured in `weida-zmq` [zmq.md §5] | Makes **absence meaningful**, which collides head-on with the drop semantics: a missing copy is already a drop, and under `PerProducer` a `Gap` [GUARANTEES §6], [0016 §4.6]. It also cannot answer before the first publication, which may never come — and a SUBSCRIBE for a path with no publisher "MUST still be recorded" [PROTOCOL §6.4]. A distinguishable welcome copy needs DATA key `8` (§6.2's `0`-`7` are taken), i.e. C's bill with worse semantics |

## 4. Decision

**Option A, with option D sharpened. No frame, no key, no code.** SUBSCRIBE keeps its shape:
header-only, unidirectional, unanswered. What this note adds is what was missing and what made
the silence look like an oversight: a statement of what silence *means*, what a subscriber may
not read into it, what the connection close is for, and what would have to exist first.

**4.1 What silence means, exactly.** A SUBSCRIBE that produces no connection close has been
**received and not refused** — the whole content of the guarantee, and worth having, since the
two refusals of §2 are connection-fatal and a live connection is positive evidence that neither
fired. The cases behind the silence, all of which a subscriber must treat alike:

| Case | Wire | What the subscriber may conclude |
| --- | --- | --- |
| recorded against a registered publisher | nothing | it will receive copies whose topic matches, while the connection lives |
| recorded, no publisher registered at that path yet | nothing | the same; the registration is kept [PROTOCOL §6.4] |
| ignored, because this side publishes nothing | nothing | nothing — and the peer is within its rights [PROTOCOL §6.4] |
| copies declined at `subscriber_buffer_bytes`, the queue, or an empty reverse pool | nothing, or a `Gap` under `PerProducer` | that it lost copies, not that it lost the subscription [GUARANTEES §6] |
| a streamed transfer declined mid-payload | `RESET_STREAM(CANCELED)` on that subscriber's stream [PROTOCOL §7], [PROTOCOL §11] | that transfer is partial and is never a whole message [PATTERNS §4.1] |
| filter grammar violation | `CONNECTION_CLOSE(PROTOCOL_VIOLATION)` | its filter is malformed [PROTOCOL §6.4] |
| past `max_subscriptions` | `CONNECTION_CLOSE(LIMIT_EXCEEDED)` → `Error::LimitExceeded` | it asked for more than this connection may hold |

**4.2 What a subscriber MUST NOT infer from silence.** Four things, the third being the one
that matters for [0015](0015-peer-authorization.md):

- **Not that a publisher exists at that path.** A path with no registration and a path that
  ignores subscriptions are the same event from outside, by design — the same deliberate
  indistinguishability §9.4 gives `UNKNOWN_ENDPOINT` [PROTOCOL §9.4], [0015 §4.3].
- **Not that any message will ever arrive.** "nothing is queued for a subscriber that does not
  exist yet" [PATTERNS §4] has a mirror: nothing promises a publication.
- **Not that it is authorized.** An acceptor may decline every copy for this peer and remain
  fully within §9.4's authorization surface [0015 §4.3]. Silence is not a grant.
- **Not that the absence of a copy is a decision.** Drops, an exhausted reverse pool and "no
  matching topic" are the same silence; the intent lives only in the publisher's per-cause
  counters [PATTERNS §4], [0016 §4.6].

Positively: **a subscriber that needs a verdict must ask for one over Req/Rep.** That is the
shape 0015 §4.5 already prescribes for token-authorized subscriptions — "register a companion
Req/Rep path for the token and accept per-connection scope" — and it is the same rule 0005
gives for one-way traffic generally: "An authorization scheme that needs the sender to *learn*
it was refused must use Req/Rep" [0015 §4.4], [0005].

**4.3 Why option B is refused.** A reply half for SUBSCRIBE is the cheapest-looking option and
is the most expensive one. It is not a key addition but a change to the stream grammar: today
"A bidi stream MUST open with DATA on its initiating half. Any other kind there is a violation"
and "The **reply half** of a bidi stream MUST carry either DATA or ERROR, exactly one frame,
followed by FIN" [PROTOCOL §4.1]. So the proposal's benign half — an empty FIN meaning yes —
does not exist and would have to be invented, and the frame kind table's "Permitted on" column
for kind `3` would change from `uni` to both [PROTOCOL §4]. It also inverts §9.4: a publisher
path today answers an exchange with ERROR `{UNSUPPORTED}` plus `STOP_SENDING(UNSUPPORTED)`
[PROTOCOL §9.4], so a v0 publisher meeting a bidi SUBSCRIBE either closes the connection
(unknown kind on the initiating half, §3.2) or, if the kind were permitted, refuses it as
unsupported — a refusal indistinguishable from "this version does not do that". And it spends a
bidi stream slot per subscription out of the budget that *is* weida's message credit
[GUARANTEES §6], [0003 §4.1].

**4.4 Why option C is refused now, and its bill, itemized.** A SUBACK-equivalent frame is the
option that would actually work, and none of its cost is where the question assumed it would be.
It costs **nothing** against [0011](0011-answered-where-it-arrived.md): the frame names an
endpoint path, so it is path-scoped and rides that path's connection, which is where the
SUBSCRIBE arrived and where the fan-out already goes [0011 §4.1-§4.2]. No frame selects a
connection, the invariant holds, and the parked control tier stays parked — *unless* the frame
is made peer-scoped (a peer-wide "your grants changed"), which is literally one of the three
revival conditions 0011 §4.3 names, "an authorization refresh". What it costs instead:

| Piece | What it would take | Who allocates it |
| --- | --- | --- |
| the frame | **kind `6`**: kinds `0`-`4` are in use and kind `5` is reserved for the L2 credit frame [PROTOCOL §4], [PROTOCOL §11], [0003 §4.2] | [PROTOCOL.md](../PROTOCOL.md), in a wire version that defines it |
| its keys | `0` `endpoint` and `1` `filter`, repeating the SUBSCRIBE it answers (512 B + 256 B), plus `2` for a code. Weida has no subscription identifier, and a subscriber-chosen one is a remote-keyed table needing a named bound [INVARIANTS] — the objection that sank [0011] §3 option B | the same. Keys `0..=63` are "reserved for this specification"; an out-of-tree extension "MUST use keys `64` and above" [PROTOCOL §5] |
| a refusal code | **ERROR code `6`** — codes `1`-`5` are in use [PROTOCOL §6.3] — reused as the verdict frame's own code space rather than minted twice | the same |
| a QUIC application error code | **none**. Nothing here is connection-fatal, and `REJECTED` (`7`) and `UNKNOWN_ENDPOINT` (`8`) already exist for stream-level refusals [PROTOCOL §7]. Were one needed, the first free is **`10`** (`0`-`9` are in use) | the same |
| making it optional | a capability code through HELLO keys `3`/`4`, a mechanism that is "specified and empty" [PROTOCOL §6.1], [PROTOCOL §11] | [PROTOCOL.md](../PROTOCOL.md) §6.1 |
| a subscriber asking for it | **SUBSCRIBE key `2`**, the first free key in §6.4 (`0` and `1` are taken, both required) | the same |

**What a v0 peer does when it meets the new frame, which is the reason this is not a small
change.** It closes the connection. "Unknown `kind` value, i.e. anything in `5..=255`" is a
`PROTOCOL_VIOLATION` [PROTOCOL §3.2], and §4 repeats it for the reserved kind: "a reservation is
a promise not to reuse the number, not a permission to send it" [PROTOCOL §4]. A verdict frame
is therefore **not additive** — unlike an optional header key, which "an encoder omits and a
decoder skips" [PROTOCOL "Status and scope"], [PROTOCOL §5]. It needs a wire version or a
negotiated capability, and a publisher that sends it unilaterally kills exactly the subscribers
it meant to inform.

**4.5 Why option E is refused.** Making the first copy — or its absence — carry the verdict
would make *every* drop a refusal. The transport's one discarding behaviour is per copy, counted
per cause, and deliberately indistinguishable at the subscriber from any other loss
[PATTERNS §4], [0016 §4.6]; a `Gap` under `PerProducer` says how many, never why
[GUARANTEES §6]. Overloading absence would also answer too late or never, since a recorded
subscription outlives the absence of any publisher [PROTOCOL §6.4]. A distinguishable welcome
copy is DATA key `8` — the first free in §6.2 — which is option C's bill with none of its
clarity.

**4.6 When the connection close is legitimate, and when it is an abuse.** `LIMIT_EXCEEDED` is
the right answer to **overload** and to nothing else. "connection refused because a local limit
is reached" is its stated use [PROTOCOL §7], and §6.4 fires it for exactly one condition:
`max_subscriptions` filters summed over all paths. Three rules follow, and they are new here:

- A peer MUST NOT use `LIMIT_EXCEEDED` to express an **authorization** decision about one
  filter. It costs the subscriber every other subscription on that connection
  [PROTOCOL §6.4], and it tells an unauthorized peer that its guess named something real —
  the disclosure `UNKNOWN_ENDPOINT` exists to avoid [PROTOCOL §9.4], [0015 §4.7].
- A peer that does not want a subscription and is not over its limit SHOULD record it and
  decline its copies. That is 0015 §4.3's third bullet, it keeps the connection's other
  subscriptions alive, and it is counted locally in `dropped_on(topic)` [PATTERNS §4].
- `PROTOCOL_VIOLATION` stays what it is: malformed content, never a policy verdict
  [PROTOCOL §3.2], [PROTOCOL §6.4].

**4.7 What would have to exist before any verdict frame, and it is not on the wire.** There is
**no local decision point**. `Publisher` offers `publish`, `open`, `subscriber_count`,
`filter_count`, `dropped`, `dropped_on` and `drops` (`crates/weida/src/endpoint.rs`), and
`handle_subscription` (`crates/weida/src/conn.rs`) records a filter without asking anyone. A
per-subscription verdict needs, in this order: an application-visible subscription event
carrying the proved peer of `IncomingMeta::peer` [GUARANTEES §6], [0015 §4.7]; a decision the
application returns; a bound on whatever that costs [INVARIANTS]; and only then a way to say it.
The first three are `weida-core` and `crates/weida` work and need no wire change at all. Asking
for the frame first is building the mouth before the mind.

**4.8 Coherence with `weida-zmq`'s `XPubSocket::refuse`.** This repository ships a
per-subscription refusal for a *foreign* protocol and declines one for its own, so the
difference needs a stated reason. There are two, and both are properties of the protocols rather
than preferences:

1. **ZMTP's refusal is connection-fatal, and so is weida's.** `XPubSocket::refuse` sends the
   `ERROR` command, and "the peer SHALL treat an incoming ERROR command as fatal" [zmq.md §9].
   Its granularity is therefore the connection with a reason attached — which is precisely
   weida's `CONNECTION_CLOSE(LIMIT_EXCEEDED)` plus the QUIC reason phrase the code already
   writes ("too many subscriptions on one connection", `crates/weida/src/conn.rs`). The two
   libraries make the *same* choice; only the spelling differs.
2. **The command already existed there; here it does not.** 37/ZMTP has carried `ERROR` all
   along and libzmq merely exposes no way to send it, so `weida-zmq` added an API over an
   existing wire construct — "It is additive: nothing sends an `ERROR` unless an application
   asks" [zmq.md §9]. On weida's wire the equivalent is a new frame kind that is fatal to every
   v0 peer (§4.4). Additive there, a wire version here.

The rule the adapter row states — "a subscription silently ignored is a subscriber waiting
forever for messages nobody will send" [zmq.md §9] — is accepted, not rejected. §4.1 and §4.2
are this note's answer to it, and §4.6's second rule is what keeps a weida publisher from
producing that situation carelessly.

**4.9 Status is `provisional` and why.** Nothing changes: no frame kind, no header key, no error
code, no QUIC code, no type, no config field, no default. It stays provisional for the reason
0015 §4.8 gives for its own — the question is wire-affecting in the other direction, so if it is
ever reopened §4.4's bill falls due, and choosing to pay a wire version is the user's call and
not an agent's.

## 5. Consequences and follow-ups

- **[PROTOCOL.md](../PROTOCOL.md) §6.4.** The two connection-close rules gain their positive
  half: a SUBSCRIBE that is not answered has been received and not refused, and the three
  silences of §2 are indistinguishable by design. §4.6's rule belongs here too —
  `LIMIT_EXCEEDED` is for overload, and a path that does not want a subscription records it and
  declines its copies rather than closing the connection.
- **[PROTOCOL.md](../PROTOCOL.md) §9.5.** The Pub/Sub paragraph should say that SUBSCRIBE is
  never acknowledged and that a subscriber therefore cannot distinguish "subscribed, nothing
  published yet" from "every copy declined" — beside the existing sentence about `PerProducer`
  making a *drop* observable, which is a different fact.
- **[PATTERNS.md](../PATTERNS.md) §4.** Beside the failure table's "nothing on the wire says
  so", the one sentence a user needs before reaching for a verdict: a subscription is never
  confirmed, and an application that needs a yes/no asks on a companion Req/Rep path [0015 §4.5].
- **[GUARANTEES.md](../GUARANTEES.md) §6.** The Pub/Sub bullet that already distinguishes a
  drop from a torn-down subscription gains its converse: no wire signal confirms a
  subscription either, so "silence is not an answer" — the sentence the local-transport row
  already earns [0012 §4.4] — is true of the wire in the other direction too.
- **[0011](0011-answered-where-it-arrived.md) §4.3.** Unchanged, and deliberately: a verdict
  frame names a path, so it is path-scoped and does **not** revive the parked control tier
  (§4.4). Only a peer-scoped variant would, under the "authorization refresh" condition that
  [0015 §5] already made live.
- **Code.** `weida-core` gains nothing: no `Error` variant, no type. `crates/weida` gains
  nothing **required**, and one thing worth an item if the silence is ever felt in practice —
  the decision point of §4.7, a local API question and not a wire question. One smaller
  observation, recorded rather than acted on: the `LIMIT_EXCEEDED` close carries a reason phrase
  on the wire and `conn_error` in `crates/weida/src/conn.rs` maps the code to
  `Error::LimitExceeded` and discards it, so a refused subscriber learns the code, not the
  sentence.
- **Backlog.** B-193 is answered by this note. No code item falls out of it; the measurement of
  §6 is what would justify one.

## 6. What this note does not decide

- **Whether a publisher application should ever be asked about a subscription.** §4.7 names what
  such a hook would need; whether `crates/weida` grows one is an API question for its own item,
  and it needs no wire change either way.
- **Anything about L2.** A broker hop has queues, credit per subscription [0003 §4.2] and a
  place to hold a verdict; whether it acknowledges subscriptions is Phase 6's to define and may
  not be approximated on the v0 wire [PROTOCOL §11].
- **UNSUBSCRIBE.** It is unanswered for the same reason and needs no verdict: "UNSUBSCRIBE
  naming an unknown filter, path or connection MUST be ignored" [PROTOCOL §6.4], so its only
  failure mode is already defined as a no-op.
- **A subscription identifier.** Weida has none, MQTT and NATS both do, and adding one is a
  precondition of option C rather than a decision of its own — with the bound of [INVARIANTS]
  attached before the table exists.
- **Multi-tenancy or a permission grammar over filters.** NATS-style subject permissions
  [nats §10] have no weida counterpart, and 0015 §4.4 forbids reading structure into a path;
  the filter grammar of [PROTOCOL.md](../PROTOCOL.md) §6.4 "stays a subscription selector, never
  a permission language" [0015 §4.4].

**What would reopen this.** One deployment shape, and it is checkable: a publisher that must
admit some subscribers and refuse others **on a path they are allowed to hold**, where the
refused subscriber's next action depends on knowing it was refused — a failover to another
publisher, a fallback to polling, an operator alert — *and* where the companion Req/Rep path of
[0015 §4.5] is unavailable because a managed Connector's explicit policy exposes only the
subscription boundary. The native ZeroMQ precedent remains `weida-zmq`'s
`XPubSocket::refuse`; that does not make the socket equivalent to a weida pattern
[zmq.md §9]. The weaker second trigger is a measurement: subscribers observed waiting
indefinitely on a path that publishes nothing for them, where `dropped_on(topic)` shows no drops
at all — the failure mode where the subscription was never going to produce anything and nothing
said so.

## 7. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) "Status and scope", §3.2, §4, §4.1, §5, §6.1,
§6.2, §6.3, §6.4, §6.5, §7, §9.4, §9.5, §11; [PATTERNS.md](../PATTERNS.md) §4, §4.1;
[GUARANTEES.md](../GUARANTEES.md) §6; [INVARIANTS.md](../INVARIANTS.md);
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §1 (the reverse pool and its `connect`-time refusal);
[BACKLOG.md](../BACKLOG.md) B-193; [libraries/zmq.md](../libraries/zmq.md) §5, §9;
[0003](0003-credit-unit.md) §4.1, §4.2; [0005](0005-refusal-race.md);
[0007](0007-topic-namespace.md) §4; [0008](0008-session-identity.md) §4.5;
[0011](0011-answered-where-it-arrived.md) §3, §4.1-§4.3; [0012](0012-local-connection-grouping.md)
§4.4; [0015](0015-peer-authorization.md) §4.3-§4.5, §4.7, §4.8, §5; [0016](0016-conflation.md) §4.6.

Code read: `crates/weida/src/conn.rs` (`handle_subscription`, its `LIMIT_EXCEEDED` close and
reason phrase, the "this side publishes nothing" ignore path; `conn_error`, which maps the close
code and drops the phrase); `crates/weida/src/endpoint.rs` (`impl Publisher`, counters only;
`Subscriber::connect`'s `Error::Unsupported` when the reverse pool is zero);
`crates/weida/tests/transports.rs`, `a_subscriber_that_parks_nothing_is_refused_at_connect`.

Research sheets: [mqtt5.md](../research/mqtt5.md) §2 (Packet Identifier lifetime), §6 (granted
QoS in SUBACK, 3.9.3), §8 (the failure table and the recommendation of silence), §10
(granularity and the verdict carriers, 2.4), §13 (the measured UNSUBACK count);
[nats.md](../research/nats.md) §1, §2, §6 (`+OK`, `-ERR`), §8 (permission violation), §10
(accounts, subject permissions); [zeromq.md](../research/zeromq.md) §4 (XPUB,
`ZMQ_XPUB_MANUAL`, `ZMQ_XPUB_WELCOME_MSG`), §7, §8, §12/P4;
[amqp10.md](../research/amqp10.md) §2, §4 (the null-terminus refusal), §8, §10 (its authorization model and its "place to refuse").
