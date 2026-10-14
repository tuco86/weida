# 0020 — A cluster is a set of nodes the client may know about — DNS says who, the protocol says who leads

- **Status:** provisional
- **Date:** 2026-09-13
- **Related:** the owner's Phase 6/7 direction; [0002](0002-control-and-bulk-separation.md) §6.2;
  [0003](0003-credit-unit.md) §4.2; [0004](0004-durability-levels.md) §4.2, §4.3;
  [0006](0006-guarantee-sets.md) §4.4; [0008](0008-session-identity.md) §4.2-§4.6;
  [0011](0011-answered-where-it-arrived.md) §4.3; [0018](0018-minimal-broker.md) §4.5, §4.7, §4.9;
  [0021](0021-consensus-openraft.md); [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §3, §4;
  [GUARANTEES.md](../GUARANTEES.md) §1, §2, §4; [PROTOCOL.md](../PROTOCOL.md) §2.5, §4, §9.4, §10;
  [FAILURE_MODEL.md](../FAILURE_MODEL.md) §3; [IMPLEMENTATION.md](../IMPLEMENTATION.md) §1, §6 and
  its non-goals 9 and 10; [INVARIANTS.md](../INVARIANTS.md).

## 1. The question

The owner's direction, condensed: RabbitMQ's design is the base, **Raft brings the fault
tolerance**, and that decides "wer sich mit wem verbindet und wie konfigurierbar das sein soll —
ist unsere Runtime *aware* dass da ein Cluster steht, oder spricht sie nur mit einem zufälligen
und tut so als gäbe es nur einen". The stated preference: aware, configurable if possible, and
DNS resolution through "das svc Konstrukt in welchem host:port Paare aufgelöst werden können".

Three sub-questions follow, and this note answers all three: how a client learns the node set,
how it finds the node that may take a write, and what replication means for the guarantee
vocabulary that [0004](0004-durability-levels.md) already fixed.

## 2. The evidence, condensed

**This repository already took a position, and it is one word away from the owner's.**
[INVARIANTS.md](../INVARIANTS.md) lists four deferred invariants verbatim: "A broker cluster
appears as one logical broker", "Raft coordinates control state, not bulk payload transport",
"Payload replication remains stream-oriented", and — in the non-goals of
[IMPLEMENTATION.md](../IMPLEMENTATION.md) — "Do not put bulk payload replication through Raft
without an exceptional demonstrated reason" and "Do not expose broker topology to normal clients
**as a requirement**". The operative words are *as a requirement*: a client that knows the
topology is an optimization, a client that must know it is a broken abstraction.

**What the four families actually do, in the two columns that decide this.**

| Family | How a client learns the node set | Who may take a write, and how the client finds it |
| --- | --- | --- |
| RabbitMQ / AMQP 0-9-1 | **nothing in the protocol**: "Discovery: none in the protocol; clients get endpoint lists, cluster formation uses static config or DNS/AWS/Kubernetes/Consul/etcd plugins" [rabbitmq-amqp091 §P11] | any node; "Any node may be connected to and quorum-queue operations are routed to the leader **transparently**, streams excepted" [rabbitmq-amqp091 §P6] |
| Kafka | bootstrap addresses plus a `Metadata` request; "Bootstrap is brokered discovery: clients try configured bootstrap addresses, issue Metadata, and refresh cached metadata after a socket or leadership error" [kafka §4] | only the partition leader — "A producer connects directly to the current partition leader" [kafka §4]; a stale one answers `NOT_LEADER_OR_FOLLOWER` and the client refreshes [kafka §8] |
| NATS / JetStream | the **server tells it**: "`INFO.connect_urls` is the server-to-client topology discovery mechanism in the client protocol", also asynchronously [nats §1], [nats §P11] | any server; a JetStream publish acknowledgement "follows the stream leader's quorum commit, not merely reception by the client's connected server" [nats §6] |
| AMQP 1.0 | nothing | any peer; `amqp:connection:redirect` / `amqp:link:redirect` carry `hostname`, `network-host`, `port` and, for links, `address` — and "no retry limit, loop detection or backoff is specified" [amqp10 §9], [amqp10 §P6] |

Two mechanisms, then: **the server tells the client** (NATS, Kafka's metadata, AMQP's redirect)
or **the operator tells the client** (RabbitMQ's endpoint lists). Nobody specifies DNS in the
protocol, and NATS' own mechanism has a documented hole our sheet recorded: "An asynchronous
`INFO` is not documented as complete or partial… a topology notice read as a complete document
would revoke `headers` support and every other capability it omits" [nats §1].

**What Kubernetes DNS actually serves**, which is what makes the owner's proposal concrete
(quoted from the Kubernetes documentation, read 2026-09-13):

- Headless Services "are also assigned DNS A and/or AAAA records… Unlike normal Services, this
  resolves to **the set of IPs of all of the Pods** selected by the Service. Clients are expected
  to consume the set or else use standard round-robin selection from the set."
- "**SRV Records** are created for named ports… `_port-name._port-protocol.my-svc.my-namespace.svc.cluster-domain.example`… For a headless Service, this resolves to **multiple answers, one for each Pod** that is backing the Service, and contains the port number and the domain name of the Pod."
- Membership is **readiness-filtered**: "the Pod needs to be ready in order to have a record
  unless `publishNotReadyAddresses=True` is set on the Service."
- A caveat to honour: "Any other layout or names or queries that happen to work are considered
  implementation details and are subject to change without warning."

Kubernetes DNS serves A/AAAA, SRV, PTR and TXT. It does **not** serve SVCB/HTTPS records
(RFC 9460), which would otherwise be the modern choice for a QUIC service because they carry
ALPN, port and address hints in one answer. The environment decides: SRV.

**What the runtime already has.** A resolver with a bound — `max_resolved_addresses`, default 8,
because "a resolver answer is remote input, so its length needs a ceiling" [PROTOCOL §10] — and
a per-address dial deadline, `connect_attempt_timeout` 250 ms, which is RFC 8305's Connection
Attempt Delay. A connection pool keyed on `(authority, terms, expected fingerprint, path)`
[IMPLEMENTATION §Phase 3]. And an identity model where the **proved fingerprint is the peer**,
binds a peer's connections together and bounds them with `max_connections_per_peer`
[0008 §4.2], [PROTOCOL §10].

**What Raft brings and what it costs.** RabbitMQ's quorum queues are Raft with `(N/2)+1`
majority, default group size 3, one member per node, odd sizes recommended, and a
`queue-leader-locator` for placement; "a confirmed message should not be lost while a majority of
hosting nodes is not permanently unavailable", losing the majority makes the queue "permanently
unavailable and must be force-deleted and recreated", and the per-node WAL is capped at 512 MiB
with node memory recommended at 3-4× that, "at least 32 bytes of metadata per message and 1 MiB
per 30,000", with "throughput falls with message size and member count"
[rabbitmq-amqp091 §9]. Under a partition, the minority side "cannot progress: Raft consensus
needs a reachable majority" [rabbitmq-amqp091 §8].

**Which failure rows this phase owns.** [FAILURE_MODEL.md](../FAILURE_MODEL.md) §3 assigns four
rows to Phase 7: "broker persists but crashes before replication", "replicated quorum completes
but broker dies before upstream ACK", "network partition splits broker cluster" and "old shard
owner returns after epoch changed". The last one is not closable without an **epoch on the
wire**, which is what Raft's term gives.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — aware by default, opaque always possible: DNS for membership, a protocol redirect for leadership** | a URL without a port resolves SRV (then headless A/AAAA) into a node set; any node accepts and either forwards or answers with a redirect; a client may follow the hint or ignore it | RabbitMQ's transparent leader routing [rabbitmq-amqp091 §P6] plus AMQP's redirect [amqp10 §9] plus Kubernetes' own headless set | Two mechanisms rather than one, and a redirect vocabulary to specify (error code, hint, loop bound — the thing AMQP left unspecified) |
| B — opaque only: one random node, the cluster hides everything | the client dials one address; every operation is proxied internally | RabbitMQ's default behaviour | Every write for a queue whose leader is elsewhere costs an extra intra-cluster hop, permanently; the owner's stated preference is refused; and nothing can ever be optimized later without a protocol change |
| C — metadata RPC, Kafka's shape | a `Metadata` request returns the queue→leader map, cached and refreshed on error | Kafka [kafka §4] | A new request/response pair and a cache-invalidation protocol in every binding; "the client must know the map" is exactly the requirement non-goal 10 forbids |
| D — server-pushed topology, NATS' shape | the node sends its peer list after the handshake and again on change | `INFO.connect_urls` [nats §1] | Needs a frame kind and an answer to the complete-or-partial question our own sheet records as undocumented in NATS; duplicates what DNS already answers, and goes stale exactly when the connection is broken |
| E — payload through the Raft log, RabbitMQ's quorum queue | every message body is a log entry | RabbitMQ quorum queues [rabbitmq-amqp091 §9] | Forbidden by non-goal 9 without a demonstrated reason, and the reason points the other way: a 33 MB video frame ([requirements/zeughaus-video.md](../requirements/zeughaus-video.md)) through a 512 MiB WAL with throughput falling in message size is the case this repository was asked to serve |

## 4. Decision

**Option A.** DNS answers *who is in the cluster*, the protocol answers *who may take this
write*, and payload never enters the consensus log.

**4.1 Awareness is a capability, not a requirement — and that is the configuration knob.**
`Discovery::Aware` is the default and `Discovery::Single` the opt-out; both speak the same wire
protocol, and a node behaves identically toward either. This keeps "a broker cluster appears as
one logical broker" [INVARIANTS] true as a *guarantee* while letting a client that has the
information skip a hop. Non-goal 10 is honoured literally: topology is never a requirement.

An adapter edge is why `Single` must exist at all: a ZMTP or MQTT bridge has one foreign socket
and no way to express a redirect to its foreign peer, so it needs a mode where the cluster is one
address and the extra hop is the price [0006 §4.6].

**4.2 A URL without a port means "ask DNS for the set".**

| URL | Meaning |
| --- | --- |
| `weida://jobs.prod.svc.cluster.local/queue` | resolve SRV `_weida._udp.jobs.prod.svc.cluster.local`; on an empty answer, A/AAAA of the same name plus the default port. The answer is the **node set** |
| `weida://10.0.0.7:4433/queue` | exactly one endpoint, no discovery — `Discovery::Single` by construction, and the escape hatch for every deployment without DNS |
| `weida://sha256:…@host:port/path` | unchanged: the pinned form of [0008](0008-session-identity.md) keeps working, and §4.4 says what pinning means for a set |

The service port must be **named `weida`** and its protocol label is `_udp`, because QUIC is UDP.
`max_resolved_addresses` bounds the set, unchanged. SRV priority is honoured, **weight is
ignored**: weight is load-balancing, and leader routing makes it moot — a decision recorded here
so that no implementation invents weighted random selection later. SVCB/HTTPS records are
**not** used, because the environment that motivates this does not serve them; a later note may
add them as an alternative source of the same set.

**Correction from implementing it (B-218): the A/AAAA half is the whole thing, and SRV is an
enhancement.** A headless Service's A/AAAA answer already *is* the node set — "the set of IPs of
all of the Pods selected by the Service" — and every Pod of one Service listens on the same
container port, so the port SRV would supply is a constant. `DEFAULT_PORT` (7443, in
`weida-core`) is therefore enough, and the discovery form works with the resolver the runtime
already has: `tokio::net::lookup_host`, which returns every address a name answers with.

SRV would add one thing — a **per-node port** — and cost a DNS client, because neither std nor
tokio resolves SRV: it needs a resolver crate (`hickory-resolver`) and a dependency decision of
its own.

**And that per-node port is a real deployment, not a corner case.** The owner's: a cloud load
balancer hands out **one address** and forwards several ports to several backends. A/AAAA cannot
express it at all — one address, one port for the whole set — while SRV is exactly that shape: one
record per port on one host. So SRV is not a Kubernetes refinement; it is what a load-balanced
deployment needs, and Kubernetes is the case that happens not to need it.

**The answer is therefore a seam rather than a choice between the two** (B-228): a
[`Resolver`](../../crates/runtime/src/resolve.rs) trait the application may replace, with the
system resolver as the default. This is the shape this workspace already uses for the decisions
that belong to the application — `Trust` decides what a dial believes, `weida-amqp` takes the
caller's `rustls::ClientConfig` — and it settles the dependency question by not taking it:
shipping only A/AAAA would make the load-balancer deployment unreachable, shipping a DNS stack
would put one in every dependency graph to serve one deployment shape, and a seam does neither. An
SRV resolver over `hickory-resolver` behind a non-default feature is then a convenience we *may*
ship (B-226), not a prerequisite, and a deployment can answer from a service registry, the cloud
provider's API or a table in a configuration file just as well.

**Readiness filtering is a property we get and rely on.** A restarting node loses its DNS record,
so discovery needs no gossip layer and no health protocol of its own. What it does *not* give is
liveness within a TTL, which is why §4.3 exists.

**4.3 Leadership is answered on the connection, never by DNS.** A leader election takes seconds
[rabbitmq-amqp091 §8]; no TTL expresses that. So: **any node accepts any request for any queue.**
A node that is not the leader of the addressed queue does one of two things, and which one is its
local configuration rather than a protocol variant:

- **forward** the exchange to the leader and relay the reply — RabbitMQ's transparent routing,
  the behaviour an unaware client sees; or
- **refuse with a redirect**: ERROR with a code naming "not the leader" plus the leader's
  `host:port` and the term it was learned in, which an aware client follows.

Three rules bind the redirect, and they exist because AMQP 1.0 left exactly these unspecified
("no retry limit, loop detection or backoff is specified" [amqp10 §9]):

1. A client MUST bound redirects — at most **two** per operation, then it fails with the last
   error. A third redirect is a cluster problem, not a client's to retry away.
2. A redirect hint is **advisory and stale by construction**: following it may produce another
   redirect, and a client that ignores it is always still served by the node it asked.
3. The hint carries the **term** it was learned in, so a client that has seen a newer term
   discards an older hint instead of oscillating between two nodes.

The concrete frame shape — a new ERROR code plus a hint map, versus a new frame kind — is left to
the slice that implements it (§5), because `PROTOCOL.md` §3.2 makes a new frame kind fatal to a
peer that does not know it and an ERROR code is additive on a reply half that already exists.

**4.4 The node id is the proved fingerprint, and a cluster is a set of them.** etcd's own raft
documentation states the constraint this must satisfy: "An ID represents a unique node in a
cluster **for all time**. A given ID MUST be used only once even if the old node has been
removed. This means that for example **IP addresses make poor node IDs** since they may be
reused." A weida node already has exactly such an identifier — the SPKI fingerprint it proves in
the handshake, the thing [0008 §4.2] makes "the peer across connections". Consequences:

- **A client pins a set, not a node.** `Trust::pin` takes one fingerprint today; following a
  redirect to a second node requires the second node's fingerprint to be trusted too. Either the
  cluster shares one identity (simple, and weakens per-node attribution), or `Trust` grows a set
  and the set is what an operator distributes (already expressible: `Trust::and_pin` accumulates).
  **The set is the decision**; one shared key is refused, because it would make
  `max_connections_per_peer` count the whole cluster as one peer and destroy the accounting
  [PROTOCOL §10].
- A DNS answer is therefore **not** authority: it names candidates, and the handshake decides.
  An attacker who controls DNS can redirect a client to a node it will refuse to talk to.
- Node ids are **stable across restarts** as long as the key file is, which makes key material
  part of a node's identity rather than of its transport configuration — an operational note that
  belongs in the deployment documentation the first cluster slice writes.

**4.5 Payload is stream-replicated; only control state goes through consensus.** The leader
streams a message body to its followers as an ordinary one-way transfer on an ordinary
connection, and the consensus log carries the **commit record** — which queue, which offset,
which digest — not the bytes. This is what "Raft coordinates control state, not bulk payload
transport" and "Payload replication remains stream-oriented" [INVARIANTS] already said, and it is
what makes `Replicated(n, flushed)` reportable without putting a 33 MB frame through a WAL.

The cost is stated rather than hidden: **a commit record whose payload never arrived is a hole**,
so the follower's payload write and the leader's commit must be ordered — the payload first, the
commit second, which is the same ordering etcd/raft demands of a local append ("no messages be
sent until the latest HardState has been persisted"). Getting that wrong loses data that was
reported `Replicated`, which is the worst failure this system can have, and it is the reason
[0021](0021-consensus-openraft.md) exists as its own note with its own test strategy.

**4.6 What replication means for the guarantee vocabulary — nothing new.**
[0004](0004-durability-levels.md) already fixed it: `Replicated(n, flushed)` counts replicas that
reached at least `Stored(Written)`, leader included, **achieved at the commit** rather than
configured, and `Replicated(1, …)` is not reportable. A cluster therefore reports:

| Configuration | Achieved level |
| --- | --- |
| in-memory queue, one node | `Accepted` (today, B-201) |
| store, one node, flushed | `Stored(Flushed)` |
| store, quorum commit, followers written but not flushed | `Replicated(n, flushed: false)` — Kafka's `acks=all` and RabbitMQ streams map here [0004 §5] |
| store, quorum commit, majority flushed | `Replicated(n, flushed: true)` — RabbitMQ's quorum queue [0004 §5] |

A configuration asking for a level the deployment cannot reach is refused **when it is
configured**, never degraded at runtime [GUARANTEES §4], [0006 §4.7]. Below quorum a write is
refused rather than accepted weakly, which is Kafka's behaviour under
`min.insync.replicas` [kafka §6] and the only honest answer.

**4.7 A credit grant and a subscription do not survive a leader change, and that is stated
rather than fixed.** Credit rides the connection of the path it names [0011 §4.3]; a new leader
is a new connection, so a consumer's subscription is new and its credit starts at zero — the
default [0018 §4.4] chose. Subscription and sequence resumption are assigned to L2 by
[0008 §4.6] and remain unbuilt, so a consumer **must** re-grant after a redirect. That is a
client obligation this note creates, and it belongs in the binding documentation of every
language.

**4.8 What the drain means in a cluster.** [0018 §4.7]'s four steps stay, with one addition
before them: a leader that is draining **gives up leadership first** (a Raft leadership transfer)
and only then stops admitting, because a node that stops admitting while still holding leadership
makes the queue unavailable rather than moving it. `Broker::drain(Duration)` keeps its mandatory
finite deadline and its local count [0009 §4.3], and it keeps not being the L0 drain.

**4.9 Status is `provisional`, and what would change it.** Two choices are the reversible kind:
the redirect's encoding (§4.3) and SRV over SVCB (§4.2). What would reopen the note: a deployment
whose DNS cannot serve SRV for the service (then the set must come from configuration and §4.2
grows a third form), or a measurement showing that forwarding inside the cluster costs less than
a redirect round trip even for an aware client (then §4.1's default flips and awareness becomes
the opt-in).

## 5. Consequences and follow-ups

- **[ARCHITECTURE.md](../ARCHITECTURE.md) §3** gains the portless URL form and what it resolves
  to; **§1's L2 paragraph** gains one sentence on the cluster appearing as one broker.
- **[PROTOCOL.md](../PROTOCOL.md) §9.4** gains the not-the-leader answer beside the queue rows
  B-201 added; **§10** gains the discovery numbers (`max_resolved_addresses` already exists; a
  redirect bound is new).
- **[GUARANTEES.md](../GUARANTEES.md) §1** gains the table of §4.6 — which configuration reaches
  which level — beside the definitions it already has.
- **[INVARIANTS.md](../INVARIANTS.md)**: the four deferred cluster invariants stay, and this note
  is the first document that says how each is kept.
- **[FAILURE_MODEL.md](../FAILURE_MODEL.md) §3**: the four Phase 7 rows get an owner and the
  epoch requirement of §2 is recorded against the "old shard owner" row.
- **Backlog.** The items below replenish Phase 6/7's head. Their links are written relative to
  `docs/BACKLOG.md`, because that is the file they are pasted into.

### B-218 — The discovery form: a portless URL resolves a node set
kind: code | size: 60 | status: ready | needs: []
acceptance: `weida://name/path` without a port resolves SRV `_weida._udp.name` and, on an empty answer, A/AAAA of the same name plus the default port, bounded by `max_resolved_addresses` and with SRV priority honoured and **weight ignored** ([0020](0020-cluster-and-discovery.md) §4.2); a URL with a port resolves nothing and dials exactly one endpoint. `Discovery::{Aware, Single}` on `RuntimeConfig` with `Aware` the default. Tests drive a stub resolver rather than DNS: a set of three answers dialled in priority order, an empty SRV answer falling through to A/AAAA, an answer longer than the bound truncated at it, and a portless URL under `Single` refused at parse with a message naming the reason.
note: the one slice that is useful before any cluster exists — a portless URL against a headless service already selects one of several equivalent brokers.

### B-219 — Trust is a set, and a redirect may only be followed inside it
kind: code | size: 60 | status: ready | needs: [B-218]
acceptance: `Trust` accumulating several pins is already expressible (`and_pin`); this item makes it the documented cluster form and proves the negative: a redirect to a node whose fingerprint is not in the set fails with `Error::Untrusted(fp)` and **does not retry**, and a DNS answer naming an untrusted host is not authority for anything ([0020](0020-cluster-and-discovery.md) §4.4). One shared cluster key is refused by documentation rather than by code, with the accounting reason (`max_connections_per_peer` would count the cluster as one peer).
note: this is the half of "aware" that is a security property rather than a performance one.

### B-220 — The not-the-leader answer, with a bound on following it
kind: code | size: 90 | status: ready | needs: [B-219]
acceptance: the redirect of [0020](0020-cluster-and-discovery.md) §4.3 on the wire — an ERROR code on an exchange's reply half plus a hint carrying `host`, `port` and the **term** it was learned in — with golden vectors in [PROTOCOL.md](../PROTOCOL.md) §8's shape, the client following at most **two** hints per operation, an older term's hint discarded rather than followed, and a client that ignores the hint entirely still being served. Tests assert all four properties, including the loop bound, which is the thing AMQP 1.0 left unspecified.
note: needs no cluster to test: a node that claims another node is the leader is one configuration flag.

### B-221 — Payload replication as a stream, with the commit record after it
kind: code | size: 90 | status: blocked | needs: [B-220, the Phase 5 store, B-222]
acceptance: the leader streams a message body to each follower as an ordinary one-way transfer and the consensus log carries only the commit record — queue, offset, digest — never the bytes ([0020](0020-cluster-and-discovery.md) §4.5), with the ordering proved rather than assumed: a test that drops a follower's payload stream mid-write and asserts that **no commit record naming it is ever committed**, and a test that asserts `Replicated(n, flushed)` reports `n` achieved at the commit with the leader counted, per [0004](0004-durability-levels.md) §4.2.
note: blocked on the store and on [0021](0021-consensus-openraft.md)'s first slice; this is the item where a mistake loses data that was reported replicated, so it gets the harshest test of the phase.

## 6. What this note does not decide

- **Queue placement.** Which node leads which queue, whether an operator may pin it, and what
  rebalancing does. RabbitMQ has `queue-leader-locator` with `client-local` as the default and
  `balanced` under 1000 queues [rabbitmq-amqp091 §9]; weida has no such vocabulary yet.
- **Whether a queue may be sharded.** Today a queue is one endpoint path with one order
  [0018 §4.5]. Sharding makes it Kafka's partition and makes ordering a new question; this note
  neither adds it nor forecloses it.
- **The configuration source.** Queues come from the broker's configuration [0018 §4.5] and the
  workspace has **no `serde` and no `toml`**: today everything is CLI flags and code. Flags carry
  the first broker slice; cluster membership and placement will not fit in them, and adding
  `toml` + `serde` is then a deliberate dependency addition of the kind `tokio-rustls` was.
- **Management and observability.** No admin protocol, no metrics surface, no `queue.declare` for
  AMQP 0-9-1 clients. All three are named in [0018 §6] as unowned and stay unowned here.
- **Multi-cluster.** NATS' gateways and leaf nodes, RabbitMQ's federation and shovels: out of
  scope, and not forecloseable from here anyway.
- **Dead-letter queues, TTL, priorities, transactions.** Still unowned [0018 §6].

## 7. Sources

weida documents: [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §3, §4;
[GUARANTEES.md](../GUARANTEES.md) §1, §2, §4; [PROTOCOL.md](../PROTOCOL.md) §2.5, §3.2, §4, §8,
§9.4, §10; [FAILURE_MODEL.md](../FAILURE_MODEL.md) §3;
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §1, §6, non-goals 9 and 10;
[INVARIANTS.md](../INVARIANTS.md); [requirements/zeughaus-video.md](../requirements/zeughaus-video.md);
[0002](0002-control-and-bulk-separation.md) §6.2; [0003](0003-credit-unit.md) §4.2;
[0004](0004-durability-levels.md) §4.2, §4.3, §5; [0006](0006-guarantee-sets.md) §4.4, §4.6, §4.7;
[0008](0008-session-identity.md) §4.2, §4.6; [0011](0011-answered-where-it-arrived.md) §4.3;
[0018](0018-minimal-broker.md) §4.4, §4.5, §4.7, §6.

Research sheets: [rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §8, §9, §P6, §P11;
[kafka.md](../research/kafka.md) §4, §6, §8; [nats.md](../research/nats.md) §1, §6, §P11;
[amqp10.md](../research/amqp10.md) §9, §P6.

External, read 2026-09-13: *DNS for Services and Pods*, Kubernetes documentation —
<https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/> — headless A/AAAA
resolving to the set of Pod IPs, SRV records for named ports with one answer per Pod, the
readiness requirement for a record, and the warning that unspecified layouts are implementation
details. *etcd-io/raft* `doc.go`, Apache-2.0 —
<https://github.com/etcd-io/raft/blob/main/doc.go> — the node-id constraint quoted in §4.4.
