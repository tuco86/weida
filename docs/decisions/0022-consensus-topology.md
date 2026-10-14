# 0022 — One consensus group for the cluster, one per replicated queue, and payload in neither

- **Status:** provisional
- **Date:** 2026-09-13
- **Related:** the owner's Phase 7 direction; [0004](0004-durability-levels.md) §4.2, §4.3;
  [0018](0018-minimal-broker.md) §4.5, §4.7, §4.8; [0020](0020-cluster-and-discovery.md) §4.3-§4.6;
  [0021](0021-consensus-openraft.md) §4.3-§4.6; [STORE.md](../STORE.md) §1, §5;
  [GUARANTEES.md](../GUARANTEES.md) §1, §4; [FAILURE_MODEL.md](../FAILURE_MODEL.md) §3;
  [INVARIANTS.md](../INVARIANTS.md); [IMPLEMENTATION.md](../IMPLEMENTATION.md) non-goal 9.

## 1. The question

[0020](0020-cluster-and-discovery.md) decided that control state is replicated through consensus
and payload is not, and [0021](0021-consensus-openraft.md) decided whose consensus. What neither
decided is **how many groups there are**, which the owner named directly: "multi-raft bedeutet
ein raft pro queue, ich versteh den overhead, macht aber queue wirklich unabhängig voneinander.
ich dachte eigentlich erst ein raft für control und metadaten, aber nicht für daten selbst. aber
ein raft für control und je ein raft pro queue hört sich eigentlich nicht so verkehrt an, data
raft könnte ja optional sein, aber dann müssen wir uns eh was anderes überlegen."

Three questions inside that: how many groups, what each holds, and what "optional" can mean
without inventing a second replication protocol.

## 2. The evidence, condensed

**Three of the four families already have the shape the owner describes, and the fourth is the
alternative.**

| Family | Metadata | Queue/stream data |
| --- | --- | --- |
| RabbitMQ | Khepri, the metadata store, itself Raft-based — under a partition "Khepri serves local cached reads" [rabbitmq-amqp091 §8] | **one Raft group per quorum queue**: `(N/2)+1` majority, default group size 3, one member per node, odd sizes recommended, `queue-leader-locator` for placement — `client-local` by default, `balanced` under 1000 queues [rabbitmq-amqp091 §9] |
| NATS JetStream | "JetStream metadata **and** replicated stream/consumer state use RAFT groups" — a meta group plus per-stream groups [nats §3] | per-stream Raft; a publish acknowledgement "follows the stream leader's quorum commit" [nats §6] |
| Kafka | the KRaft controller quorum; a broker "unable to receive controller metadata updates is **fenced** and omitted from client metadata" [kafka §8] | **not Raft**: per-partition leader/follower log shipping with an ISR, committed = "applied by all ISR replicas", `acks=all` subject to `min.insync.replicas` [kafka §5], [kafka §6] |
| AMQP 1.0 | nothing: node lifecycle is out of scope [amqp10 §2] | nothing |

So "one group for control plus one per queue" is not an invention; it is RabbitMQ's and
JetStream's architecture. Kafka is the one that separates them differently: consensus for
metadata, **a purpose-built replication protocol for data**.

**What per-queue groups cost, in the one sheet that gives numbers.** RabbitMQ: always-persistent
writes, a per-node WAL capped at 512 MiB with node memory recommended at 3-4× that, "at least 32
bytes of metadata per message and 1 MiB per 30,000", and "throughput falls with message size and
member count"; losing a group's majority makes that queue "permanently unavailable and must be
force-deleted and recreated"; membership changes need a quorum; 3 members tolerate 1 failure, 5
tolerate 2 [rabbitmq-amqp091 §9].

**But most of that cost is a cost of putting bodies in the log, which this design already
refuses.** [0020 §4.5] keeps payload out of consensus — the leader streams bodies to followers and
the log carries a commit record: queue, offset, digest. So a per-queue group's entries are tens of
bytes rather than the message, which removes "throughput falls with message size" from the list
and leaves the per-group overheads: one election clock, one heartbeat flow, one log.

**And one of those overheads is cheaper here than in the systems that measured it.** RabbitMQ's
groups ride Erlang distribution and Kafka's replication rides one TCP connection per broker pair;
weida's transport is QUIC, where **one connection carries independent streams**, so every group's
RPCs between two nodes share one connection and one congestion controller while remaining
independent flows. The per-group cost is therefore a timer and a log, not a socket — which is the
first place this design is structurally cheaper than the precedent rather than merely different.

**What "optional" would have to mean.** Dropping the per-queue group but keeping replication needs
two things the group was providing: a **leader with an epoch** and a **commit rule over a set of
replicas**. Kafka builds both — the controller assigns leadership and an epoch, the ISR defines
the commit boundary, and `min.insync.replicas` decides when a write is refused rather than weakly
accepted [kafka §6], with "unclean leader election" as the documented way to trade committed data
for availability [kafka §8]. That is a second replication protocol with its own correctness
argument, and [IMPLEMENTATION.md](../IMPLEMENTATION.md)'s non-goals already warn against inventing
mechanisms where one exists.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — one control group, one group per *replicated* queue, payload streamed outside both; replication factor 1 means no group at all** | the cluster's metadata is one Raft; a queue with replicas gets its own Raft for its commit records and consumer state; a single-replica queue has no group and its owner is a control-group entry | RabbitMQ (Khepri + per-queue Raft) [rabbitmq-amqp091 §8, §9]; JetStream (meta group + per-stream groups) [nats §3] | N groups is N election clocks and N logs, and a placement policy is now required rather than optional. Losing one queue's majority takes that queue out |
| B — one group for everything | every queue's commit record goes through the cluster's single log | none of the four | Every queue's write rate adds to one log; one slow disk on one node slows every queue; and queue independence — the owner's stated reason for wanting per-queue groups — is lost by construction |
| C — control group plus Kafka-style ISR replication for data | the control group assigns leader and epoch; data replicates leader-to-follower with an in-sync set and a high watermark | Kafka [kafka §5], [kafka §6] | A second replication protocol to get right: ISR membership, lag eviction, the commit boundary, `min.insync.replicas`, and unclean election as an explicit data-loss switch. Better throughput at large sizes, paid for with the correctness work openraft otherwise does for us |
| D — per-queue groups with bodies in the log, as RabbitMQ does | the message *is* a log entry | RabbitMQ quorum queues | Refused already by [0020 §4.5] and non-goal 9, and the numbers say why: a 512 MiB WAL and throughput falling with message size, against a requirement that names 33 MB frames |

## 4. Decision

**Option A.** One consensus group for the cluster, one per **replicated** queue, payload in
neither, and a queue whose replication factor is 1 has no group.

**4.1 The control group holds desired resource state, and nothing that scales with traffic.**
Its log carries cluster membership — nodes identified by the fingerprint [0020 §4.4] assigns
them — and a declarative resource registry. The first resource kinds are **Queue** and
**Connector**. A Queue specification names its endpoint path, limits, replication factor and
placement. A Connector specification names one concrete foreign-protocol source or sink, its
address and policy, and the Queue it attaches to; it does not assert a general equivalence
between that protocol and a weida pattern.

Each resource has a stable id, a name, a monotonically increasing specification generation and
an optional deletion request. Its reconciled status records the observed generation, placement
and a small phase such as Pending, Ready, Degraded or Deleting. High-frequency health samples,
metrics and message traffic never enter this log.

The committed log index is the resource version. Mutating calls may carry an expected version
and are rejected on conflict; retrying the same apply with the same name and specification is
idempotent. Names are unique within their resource kind, while the stable id prevents a deleted
and later recreated name from inheriting the old resource's queue group or connector assignment.
Deletion is a persisted intent: controllers first stop new work, drain according to policy,
release placement and group membership, and only then remove the record.

An administrative API performs create/apply, get/list/watch and delete by proposing these
records to the control group. A successful mutation means the desired state is committed; it
does not mean the resource is already ready. Controllers observe committed generations and
idempotently create, update, drain or remove the runtime objects, then advance the observed
generation. The messaging data plane has no declare operation and never creates a resource as
a side effect.

Node bootstrap remains local: cluster seeds, identities, storage paths and the addresses needed
to start the process. Queue and Connector existence, configuration, placement and ownership are
cluster state. This inventory changes when an operator changes something, not when a producer
sends something, which is what makes one control group enough.

**4.2 Each replicated queue is its own group, and that is the answer to "independent".** Its log
carries what [0021 §4.5] listed minus the cluster-wide part: the commit record per admitted
message (queue, offset, digest, producer and sequence when present) and the consumer state —
subscriptions, credit grants, and the settled/unsettled position of every delivery. Two queues
then share nothing: not a leader, not an election, not a log, not a disk queue. A queue whose
majority is lost is unavailable **alone**, which is the property the owner asked for and the one
Option B cannot have.

**4.3 Replication factor 1 means no group, and that is what "optional" means here.** Not a
degraded group and not a second protocol: a queue configured with one replica has an owner — a
node recorded in the control group's log — and no consensus of its own. It reports `Accepted` or,
once the store exists, `Stored(*)`; it can never report `Replicated`, because
[0004 §4.2] requires `n` achieved at a commit and `Replicated(1, …)` is explicitly not
reportable. A factor change is a control-group entry that creates or dissolves a group.

This is what makes the owner's "data raft könnte optional sein" coherent without inventing
anything: the optionality is **per queue** and it is expressed by the absence of a group, not by a
fallback replication path. Option C's ISR machinery stays unbuilt and stays available as the note
to write if measurement ever demands it (§6).

**4.4 Three or more members, or one. Two is refused at configuration.** [0021 §4.6] already fixed
this from etcd's rule — a two-member group cannot remove a member safely — and it now applies per
group: a queue with replication factor 2, and a cluster with exactly two nodes, are refused when
they are configured, with the reason named. Odd sizes are recommended, as RabbitMQ recommends
them, for the same arithmetic: 3 tolerate 1, 5 tolerate 2 [rabbitmq-amqp091 §9].

**4.5 Every group between two nodes shares one connection.** One QUIC connection per peer pair
carries every group's RPCs as independent streams, which is the structural advantage named in §2:
adding a queue adds a timer and a log, not a socket and not a congestion controller. Two
consequences to hold on to:

- **A stalled stream must not stall another group.** That is [0002 §6.2]'s argument, and it holds
  here because each RPC is its own stream with its own flow control; what is shared is the
  connection window, which is why the Raft connection carries no payload (§4.6).
- **`max_concurrent_bidi_streams` is now a cluster bound.** With one exchange per RPC, the stream
  budget on the Raft connection bounds in-flight RPCs across all groups between two nodes. It
  needs its own number in the broker's configuration rather than the client default — a bound
  named here so it is not discovered under load.

**4.6 Payload never rides the Raft connection, and never enters any log.** The leader streams
bodies to followers over ordinary weida transfers on ordinary connections, the commit record
follows the payload [STORE.md §5], and the Raft connection stays small-frame traffic. This is
[0020 §4.5] applied to the topology: it is what keeps a 33 MB frame out of every log and the
election heartbeats out of a payload's flow-control window.

**4.7 The availability split, stated because it is the reason to have two kinds of group.** When
the **control group** has no quorum: no resource may be created, deleted, reconfigured or moved,
and no group membership may change — but every existing queue keeps serving under its own
group's quorum, and every running Connector keeps its last committed specification and
assignment. Administration and replacement placement stop; established messaging does not.
That is deliberately unlike Kafka, where a broker cut off from the controller “is fenced and
omitted from client metadata” [kafka §8], and closer to RabbitMQ, where Khepri serves local
cached reads while quorum queues continue [rabbitmq-amqp091 §8]. The reason is the one the
owner gave: queues are meant to be independent, and a metadata outage that stops traffic would
make them dependent again through the back door.

When a **queue's group** has no quorum, that queue refuses admission and pauses delivery, and
nothing else on the cluster notices. A confirmed message is not lost while a majority survives;
if a majority is permanently lost, the queue is unavailable and needs operator action, exactly
as RabbitMQ documents [rabbitmq-amqp091 §9].

**4.8 What the numbers will be is not decided here, but which numbers exist is.** Per cluster: the
control group's membership. Per queue: replication factor, placement, and its group's membership.
Per node pair: the stream budget of §4.5. Per group: election and heartbeat intervals, which
[0021]'s engine takes as ticks and which have to be one setting for the whole node rather than one
per queue, or a thousand queues become a thousand clocks with a thousand values.

**4.9 Status is `provisional`, and what would change it.** Two measurements. First, the per-group
overhead at a realistic queue count: RabbitMQ's own guidance changes shape around 1000 queues
[rabbitmq-amqp091 §9], and if a thousand groups on one node cost more than the independence buys,
the answer is not Option B but **grouping several queues into one consensus group by placement** —
a third shape this note deliberately leaves unbuilt. Second, throughput at large payloads: if
streaming the body outside consensus turns out not to be enough, Option C's ISR is the written-down
alternative.

## 5. Consequences and follow-ups

- **[ARCHITECTURE.md](../ARCHITECTURE.md) §1** gains one sentence on the two kinds of group, and
  the `weida-raft` section gains the fact that one connection carries many groups.
- **[GUARANTEES.md](../GUARANTEES.md) §1** gains the rule of §4.3: a single-replica queue cannot
  report `Replicated`, whatever it is configured with.
- **[FAILURE_MODEL.md](../FAILURE_MODEL.md) §3** gains the availability split of §4.7 against the
  "network partition splits broker cluster" row, which is the row this note actually answers.
- **[STORE.md](../STORE.md) §1** gains the consequence that a node holds *several* logs — one per
  group it is a member of — so the segmented log is instantiated per group and its bounds are
  per group, not per node.
- **Backlog.** Three items, links relative to `docs/BACKLOG.md`.

### B-229 — Managed resources, several groups on one connection, and queue reconciliation
kind: code | size: 90 | status: blocked | needs: [B-224, B-227]
acceptance: the control-group state machine stores Queue and Connector resources with stable id, name, resource version, specification generation, observed generation, status and deletion intent. An administrative API proves create/apply with optimistic concurrency, get/list/watch and asynchronous delete; a repeated identical apply is idempotent. `weida-raft` carries more than one group over one connection per peer pair, and the Queue controller creates a queue's group from the committed resource: a three-node cluster where a Queue is created, replicated and its group dissolved when the replication factor drops to 1, with the queue still serving throughout. Connector runtime controllers remain protocol-specific follow-ups, but their lifecycle records are the same control-plane type.
note: this item makes the control/data split real. Messaging paths never declare resources; the existing process-local `BrokerConfig::queues` remains only the single-process implementation until this cutover is complete.

### B-230 — The availability split: administration stops, messaging does not
kind: code | size: 60 | status: blocked | needs: [B-229]
acceptance: with the control group below quorum, create/apply/delete for Queue and Connector resources are refused with the reason named, while every existing queue keeps admitting and delivering under its own group's quorum and every running Connector keeps its last committed assignment. Conversely, a queue whose own group is below quorum refuses admission and pauses delivery while every other queue is unaffected. Two tests kill the different majorities.
note: this is the reason the topology has two kinds of group at all. Deliberately unlike Kafka fencing.

### B-231 — The bounds a cluster adds, each with the failure it prevents
kind: spec | size: 45 | status: ready | needs: []
acceptance: the numbers of [0022](0022-consensus-topology.md) §4.8 written into [PROTOCOL.md](../PROTOCOL.md) §10's broker profile with a default and a named failure each: the Raft connection's stream budget (bounds in-flight RPCs across all groups between two nodes), the election and heartbeat ticks (one setting per node, not per queue), the per-group log segment size and retained-segment count, and the refusal of a two-member group or a two-node cluster. Nothing is implemented by this item; what it prevents is a number being invented per slice.
note: filed because [0022](0022-consensus-topology.md) §4.8 names which numbers exist and deliberately not what they are, and a number chosen under load is a number chosen badly.

## 6. What this note does not decide

- **Placement.** Which node leads a new queue's group and whether an operator may pin it.
  RabbitMQ has `queue-leader-locator` with `client-local` as the default and `balanced` under 1000
  queues [rabbitmq-amqp091 §9]; this note establishes that placement is a control-group entry and
  leaves the policy open.
- **Whether several queues may share one group.** §4.9's escape hatch if per-group overhead bites,
  and a different design rather than a tuning knob: it would reintroduce coupling deliberately, for
  queues an operator groups on purpose.
- **Sharding a single queue across groups.** Still open from [0020 §6]: it makes a queue Kafka's
  partition and makes "one queue, one order" false. Untouched here.
- **Kafka-style ISR replication** as an alternative data path (Option C). Written down, unbuilt.
- **Snapshot policy per group**, which [0021 §6] already holds, and which gets worse with many
  groups: a thousand groups snapshotting on the same trigger is a thousand simultaneous writes.
- **Read scaling.** Whether a follower may serve anything, which needs ReadIndex or a lease and is
  [0021 §6]'s open question.

## 7. Sources

weida documents: [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §4;
[GUARANTEES.md](../GUARANTEES.md) §1, §4; [PROTOCOL.md](../PROTOCOL.md) §10;
[STORE.md](../STORE.md) §1, §5; [FAILURE_MODEL.md](../FAILURE_MODEL.md) §3;
[IMPLEMENTATION.md](../IMPLEMENTATION.md) non-goal 9; [INVARIANTS.md](../INVARIANTS.md);
[0002](0002-control-and-bulk-separation.md) §6.2; [0004](0004-durability-levels.md) §4.2, §4.3;
[0018](0018-minimal-broker.md) §4.5, §4.8; [0020](0020-cluster-and-discovery.md) §4.3-§4.6;
[0021](0021-consensus-openraft.md) §4.3-§4.6, §6.

Research sheets: [rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §8, §9;
[nats.md](../research/nats.md) §3, §6; [kafka.md](../research/kafka.md) §5, §6, §8;
[amqp10.md](../research/amqp10.md) §2.
