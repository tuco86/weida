# 0007 — The topic namespace — opaque paths, segmented topic filters

- **Status:** accepted
- **Date:** 2026-09-11
- **Related:** SYNTHESIS §8.9; P5, P11; decisions 0002 §6.2, 0003 §4.2

## 1. The question

"Endpoint paths are opaque identifiers" is an invariant, and topic matching is deliberately
confined to Pub/Sub topics ([INVARIANTS.md](../INVARIANTS.md)). NATS subjects with `*`/`>`
[nats §4], MQTT topic filters [mqtt5 §4.1] and RabbitMQ topic exchanges
[rabbitmq-amqp091 §4] use their own hierarchical namespaces. This decision defines weida's
native topic grammar only; it neither amends endpoint paths nor equates those foreign
grammars with weida. A managed Connector must specify any concrete conversion.

Two namespaces are in play and the question is different for each. An **endpoint path** is
the addressing namespace: `weida://host:port/path`, carried as DATA key `0` and capped at
512 B ([PROTOCOL.md](../PROTOCOL.md) §6.2), resolved by "a flat map keyed by the exact path
string — no splitting, no prefix match, no wildcards" [INVARIANTS]. A **topic** is a
subscription selector inside one publisher path: DATA key `5`, `tstr`, 256 B [PROTOCOL §6.2],
selected by a `filter` that today "is a **byte prefix**, not a pattern. A topic matches when
`filter` is a prefix of `topic` compared byte for byte. No character is special, there is no
wildcard syntax, and there is no case folding" [PROTOCOL §6.4], [PATTERNS §4].

## 2. The evidence, condensed

**Every hierarchical namespace in the catalogue is segmented, and the three that matter agree
on the shape while disagreeing on the details.** MQTT: `/` separates levels, `+` matches
exactly one level, `#` matches "the parent level and any number of child levels"; `#` must be
last and alone in its level ([MQTT-4.7.1-1]) and `+` must occupy a whole level
([MQTT-4.7.1-2]); the spec's own example has `sport/tennis/player1/#` matching
`sport/tennis/player1` itself as well as its children [mqtt5 §4.1]. NATS: "Subject tokens are
separated by dots"; "`*` matches exactly one complete subject token"; "`>` matches one or more
trailing subject tokens and must be the final token", so "`orders.*` matches `orders.created`
but not `orders.eu.created`; `orders.>` matches both" [nats §4]. AMQP 0-9-1 topic exchange:
"Routing and binding keys are dot-delimited word lists — the spec requires 'zero or more words
delimited by dots'. `*` matches exactly one word, `#` zero or more", with
`audit.events.#` matching `audit.events` and `audit.events.users.signup`
[rabbitmq-amqp091 §4].

**The one real semantic disagreement is the arity of the rest wildcard.** MQTT `#` and AMQP
`#` match *zero* or more trailing segments — both spell out that the pattern matches the
parent itself [mqtt5 §4.1], [rabbitmq-amqp091 §4] — while NATS `>` matches *one* or more and
must be final [nats §4]. Two of three take zero-or-more.

**ZeroMQ is the outlier, and its own ecosystem has moved away from byte prefixes twice.** ZMTP:
"A subscription of 'A' SHALL match all messages starting with 'A'. An empty subscription SHALL
match all messages", with filtering at the publisher [zeromq §4.3]. The guide's recommended
practice is already a boundary discipline: put the key in its own frame, because "Subscription
is a prefix match" and the envelope "prevents accidental payload matches" since "the match
won't cross a frame boundary" [zeromq §4.3]. The draft RADIO/DISH pattern replaces the whole
mechanism with "exact-match groups instead of prefix topics" [zeromq §4.6]. So ZeroMQ's own
answer to prefix matching's ambiguity is to align subscriptions to a boundary or to abandon
matching altogether.

**A byte prefix cannot express what any of the three needs, and it silently over-matches.**
A prefix has no notion of a boundary, so `sensors.temp` also selects `sensors.temperature`
and `sensors.tempest`; nothing in the filter can say "exactly one segment here", and nothing
can say "any middle segment, this leaf". That is why SYNTHESIS §8.9 calls the decision a
prerequisite "before any adapter maps a foreign hierarchical namespace onto weida endpoints"
[SYNTHESIS §8.9].

**The implementation's own objection is on record and must be answered, not ignored.**
`matches_filter` in `crates/weida/src/pubsub.rs` carries it: "A filter is a byte prefix, not a
pattern: no character in it is special, and the empty filter matches everything… treating `*`
as a wildcard here would make topics with a literal `*` unaddressable and would put a matching
language in the hot path." Both halves are true and both are costs this note accepts
explicitly (§4.6, §5).

**What must not move.** The invariant list is amended only with the reasoning recorded first
[INVARIANTS], and two entries bear on this: "Endpoint paths are opaque identifiers", enforced
by `EndpointAddr` validating "bytes and length only" and by a flat exact-match map; and the
carve-out that already exists — "Pub/Sub **topics** are a separate namespace from endpoint
paths and are matched by byte prefix; that prefix match is on topics only and never on paths"
[INVARIANTS]. Resource bounds are unchanged by any of this: a filter is capped at 256 B and a
connection at `max_subscriptions` filters, with subscriptions dropped wholesale when the
connection closes [PROTOCOL §6.4], [INVARIANTS].

**The wire is free to change.** "`0.x` protocol versions are explicitly experimental and
breaking changes are permitted. Implementations MUST NOT assume any compatibility guarantee
across `0.x` releases" [PROTOCOL Status and scope].

**Where a richer selector language leads.** AMQP 1.0 is the warning: "Three generations
coexist" — Apache's proposed `apache.org:selector-filter:string`, the OASIS Filter Expressions
CSD with an entirely different `amqp:*-filter` family, and vendor filters on top — with no
interoperability matrix [amqp10 §13]. Predicate languages over message content are a
per-broker dialect, not a portable namespace.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| A — keep the byte prefix | filters stay opaque byte prefixes [PROTOCOL §6.4] | ZMTP `SUBSCRIBE` [zeromq §4.3] | no boundary, so `sensors.temp` selects `sensors.temperature`; no single-segment wildcard; a concrete foreign conversion must over-deliver or refuse filters it cannot express |
| B — segmented patterns for topics, paths stay opaque | a separator, a one-segment wildcard, a trailing rest wildcard; endpoint paths untouched | MQTT [mqtt5 §4.1], NATS [nats §4], AMQP 0-9-1 [rabbitmq-amqp091 §4] | special bytes in a filter stop being literal; matching cost moves into the publisher's fan-out path |
| C — hierarchical endpoint paths too | amend "endpoint paths are opaque identifiers" and match paths by pattern | MQTT and NATS, where the addressing namespace *is* the hierarchy [mqtt5 §4.1], [nats §4] | the invariant's enforcement (a flat exact-match map) is replaced by a matching structure on the dispatch path of every transfer [INVARIANTS]; endpoint dispatch is currently "a function of the stream kind and the addressed path" with one answer [PROTOCOL §9.4] — pattern dispatch makes it a set, which changes refusal, `UNKNOWN_ENDPOINT` and the pool key of 0002 §6.2 |
| D — a selector/predicate language | filter expressions over headers or content | AMQP 1.0 filters and JMS selectors [amqp10 §13] | three incompatible generations with no interoperability matrix [amqp10 §13]; unbounded matching cost in the publisher's hot path against [INVARIANTS] |

## 4. Decision

1. **Endpoint paths stay opaque. The invariant is not amended.** Option C is closed. A path
   remains an exact key into a flat map, validated for bytes and length only [INVARIANTS],
   [PROTOCOL §6.2], and endpoint dispatch keeps exactly one answer per (stream kind, path),
   which is what makes `UNKNOWN_ENDPOINT` and `UNSUPPORTED` decidable [PROTOCOL §9.4].
   Hierarchy lives in the topic namespace, which the invariant already separates from paths
   [INVARIANTS].
2. **Pub/Sub filters become segmented patterns.** Option B. The grammar:
   - **Separator: `.` (U+002E, one byte).** A topic and a filter are byte strings split on
     that byte into segments. `.` is chosen over `/` for two reasons: it is what two of the
     three surveyed hierarchical namespaces use — NATS tokens [nats §4] and AMQP 0-9-1
     dot-delimited word lists [rabbitmq-amqp091 §4] — and `/` is already the endpoint path
     separator in `weida://host:port/path` [PROTOCOL §6.2], where reusing it would invite
     exactly the path/topic conflation the invariant of §4.1 exists to prevent.
   - **One-segment wildcard: `*`.** It matches exactly one whole segment and MUST occupy a
     whole segment. This is NATS's `*` [nats §4] and AMQP 0-9-1's `*` [rabbitmq-amqp091 §4];
     the whole-segment rule is MQTT's ([MQTT-4.7.1-2]) [mqtt5 §4.1].
   - **Rest wildcard: `#`, matching zero or more trailing segments.** It MUST be the last
     segment of the filter and MUST be alone in that segment, which is MQTT's rule for `#`
     ([MQTT-4.7.1-1]) [mqtt5 §4.1]. Zero-or-more follows the majority: MQTT's
     `sport/tennis/player1/#` matches `sport/tennis/player1` itself [mqtt5 §4.1] and AMQP's
     `audit.events.#` matches `audit.events` [rabbitmq-amqp091 §4]; NATS's one-or-more `>` is
     the minority reading [nats §4] and becomes a named loss in §5.
   - **Everything else is literal, byte for byte.** No normalization, no case folding, no
     escape character; empty segments are permitted and match only empty segments. MQTT's
     server behaviour is the same — "the server performs no normalization" [mqtt5 §4.7].
   - **A `topic` is never a pattern.** `*` and `#` are special only inside a `filter`; a
     published topic containing them is matched literally.
   - **The empty filter still matches every topic** [PROTOCOL §6.4], and is equivalent to the
     single-segment filter `#`.
3. **Matching stays bounded and allocation-free.** With `#` restricted to the final segment,
   a match is one left-to-right walk over both strings: no backtracking, no segment vector, no
   allocation, and work linear in the 256 B filter cap [PROTOCOL §6.4]. This restriction is
   also what closes the door on patterns such as `#.leaf`, which AMQP 0-9-1 permits and which
   would require backtracking; the loss is named in §5.
4. **The 256 B filter cap and `max_subscriptions` are unchanged**, and no new remote-influenced
   allocation is introduced [PROTOCOL §6.4], [INVARIANTS]. A pattern is not more expensive to
   store than a prefix of the same length.
5. **Foreign topic syntaxes are independent.** ZeroMQ byte prefixes, MQTT topic filters, NATS
   subjects and AMQP routing keys are not aliases for this grammar. The standalone libraries
   expose each protocol unchanged. If an application or a managed Connector converts a topic,
   its explicit configuration owns that one transformation and its losses; this decision
   defines no protocol-wide mapping.
6. **Two costs are accepted explicitly, against the objection recorded in
   `crates/weida/src/pubsub.rs`.** First, a filter can no longer select a topic segment that
   contains `*`, `#` or `.` literally; there is no escape character, and adding one is
   rejected as complexity for a case no sheet reports. A publisher whose topics contain those
   bytes is still fully addressable by a filter that covers the segment with `*` or `#`, and
   an application that needs literal selection must choose different topic bytes. Second,
   matching is a language rather than a `starts_with`, and it runs in the publisher's fan-out
   path; the bound of §4.3 is what keeps it affordable, and the number is a measurement task
   in §5.
7. **Nothing here changes the guarantee dimensions.** Fan-out stays best effort with explicit
   drops, ordering stays `None`, and topic matching adds no delivery promise
   [PROTOCOL §9.5], [PATTERNS §4].

## 5. Protocol boundary

The segmented grammar above is only weida's Pub/Sub namespace. Similar separators or wildcard
characters in another protocol do not establish semantic equality. Research and library parity
documents describe those protocols on their own terms; a future Connector resource must name
its concrete conversion policy rather than referring to a global table.

## 6. Consequences and follow-ups

- **[PROTOCOL.md](../PROTOCOL.md) §6.4.** Replace the byte-prefix rule with the grammar of
  §4.2: separator, `*`, `#`, the whole-segment and final-position restrictions, literal
  matching for everything else, the empty filter, and the statement that a `topic` is never a
  pattern. Keep the 256 B cap, the idempotence of SUBSCRIBE, the UNSUBSCRIBE rules and the
  `LIMIT_EXCEEDED` behaviour unchanged. A filter that violates the grammar (`*` not alone in
  its segment, `#` not final) MUST be rejected — state which code, which is the same
  connection-granularity problem SUBSCRIBE already has: it arrives on a uni stream with no
  reply half [PROTOCOL §6.4].
- **[PROTOCOL.md](../PROTOCOL.md) §8 and §9.5.** §9.5 says a SUBSCRIBE names "a topic prefix";
  it becomes a topic filter. §8 gains golden vectors for at least: a literal filter, `*` in a
  middle segment, a trailing `#`, the empty filter, and a topic containing a literal `*`.
- **[INVARIANTS.md](../INVARIANTS.md).** The enforcement cell for "endpoint paths are opaque
  identifiers" currently reads "Pub/Sub topics… are matched by byte prefix"; it becomes
  "matched by a segmented pattern over a separate namespace, never applied to paths". The
  invariant itself is unchanged, which is the decision of §4.1.
- **[PATTERNS.md](../PATTERNS.md) §4.** "Filters are byte prefixes carried in
  SUBSCRIBE/UNSUBSCRIBE frames" becomes the segmented grammar, with one example line. The
  named test `subscribe_prefix_filters_topics` in `crates/weida/tests/pubsub.rs` is renamed
  and extended to cover a one-segment wildcard, a trailing rest wildcard matching the parent,
  and a boundary case that a byte prefix would have over-matched (`sensors.temp` vs
  `sensors.temperature`) — the last is the regression test that fails on the old
  implementation.
- **Code, `crates/weida/src/pubsub.rs`.** `matches_filter` becomes the segment walker of
  §4.3, allocation-free and without backtracking, and its doc comment is rewritten: the
  objection it records is answered by this note, not deleted. `crates/weida/src/endpoint.rs`:
  `Subscriber::subscribe`'s documentation ("Registers interest in every topic starting with
  `filter`… no character is special") must state the grammar instead. Filter validation
  belongs in `weida-protocol` beside the other header rules, so that an invalid filter is
  rejected at the codec boundary rather than in the fan-out path.
- **Measurement.** The cost this note moves into the publisher's fan-out path is unmeasured;
  the objection in `pubsub.rs` asserts it without a number. A bench comparing `starts_with`
  against the segment walker at a realistic subscriber and filter count is a task, in the
  shape of the measurement follow-ups of 0001 §8 and 0002 §7, and its number belongs in
  `docs/IMPLEMENTATION.md`.
- **Managed Connectors.** A Connector that converts topic filters names the concrete conversion
  and its loss in the resource specification. It MUST NOT present boundary-aligned or
  over-matching behavior as the foreign semantics [INVARIANTS]; a filter it cannot express
  faithfully is refused at configuration time [0006 §4.7]. No global table equates a foreign
  topic grammar with weida's.
- **SYNTHESIS §8.9** is closed by this note.

## 7. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) Status and scope, §6.2, §6.4, §8, §9.4, §9.5;
[PATTERNS.md](../PATTERNS.md) §4; [INVARIANTS.md](../INVARIANTS.md);
[0002](0002-control-and-bulk-separation.md) §6.2; [0003](0003-credit-unit.md) §4.2;
[0006](0006-guarantee-sets.md) §4.7, §4.9;
`crates/weida/src/pubsub.rs` (`matches_filter`), `crates/weida/src/endpoint.rs`
(`Subscriber::subscribe`).

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §1 (P5, P11), §8.9;
[mqtt5.md](../research/mqtt5.md) §4.1, §4.2, §4.7, §11; [nats.md](../research/nats.md) §4;
[rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §4; [zeromq.md](../research/zeromq.md)
§4.3, §4.6; [amqp10.md](../research/amqp10.md) §13.
