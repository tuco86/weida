# The store interface

*Specification ahead of code.* Nothing in this repository implements it; the persistence
subsystem is Phase 5 of the tracker ([IMPLEMENTATION.md](IMPLEMENTATION.md) §1). This document exists because the interface is **not**
the store's to choose — [0021](decisions/0021-consensus-openraft.md) made the consensus engine a
dependency, and a consensus engine prescribes what a log must be able to do. Writing the contract
down before the store means the store is built once.

Every requirement below names the failure it prevents, and §7 names the three things an
append-only file cannot do without a rewrite path.

## 1. What the store is, and what it is not

The store is **two things behind one crate**: a *log* of consensus entries and a *payload store*
for message bodies. They are separate because [0020](decisions/0020-cluster-and-discovery.md) §4.5
keeps payload out of the consensus log — "Raft coordinates control state, not bulk payload
transport" [INVARIANTS.md](INVARIANTS.md) — so the two have different sizes, different lifetimes
and different durability questions.

| | The log | The payload store |
| --- | --- | --- |
| Holds | consensus entries: queue registry, membership, leadership, commit records, consumer state ([0021](decisions/0021-consensus-openraft.md) §4.5) | message bodies |
| Entry size | small and bounded | unbounded; a 33 MB frame is an ordinary case ([requirements/zeughaus-video.md](requirements/zeughaus-video.md)) |
| Written by | the consensus engine, through §2's contract | the broker, before the commit record that names it |
| Read back by | recovery and replication | delivery |
| Deleted by | `purge` after a snapshot (§2.7) | acknowledgement, or a queue's byte bound |

**A node holds several logs, not one.** Consensus is one group for the cluster plus one per
replicated queue ([decisions/0022](decisions/0022-consensus-topology.md) §4.1-§4.2), so the log
half of this document is instantiated **per group** a node is a member of, and every bound in §4
is per group rather than per node. The payload half is one store per node: bodies are content-
addressed and shared (§3.2), and a body referenced by two queues is one payload.

What the store is **not**: a queue. A queue is the in-memory structure of
[0018](decisions/0018-minimal-broker.md) with its own byte budget; the store is what makes it
survive a restart. And it is not a database — there are no queries, no indexes and no
transactions across the two halves beyond the ordering rule of §5.

## 2. The log contract, method by method

These are openraft's v2 storage traits, which are the ones to implement and are sealed unless the
`storage-v2` feature is enabled ([0021](decisions/0021-consensus-openraft.md) §4.3). The names are
that library's; the **requirements** are what any consensus engine needs and what this document
holds the store to.

### 2.1 `get_log_state` — where the log ends, and where it begins

Returns the last log id and the **last purged** log id. Both, always: a log whose entries were all
purged after a snapshot still has a history, and a store that forgets where it ended cannot answer
a vote request about entries it no longer holds. *Prevents:* a restarted node voting for a
candidate whose log is behind its own.

### 2.2 `save_vote` / `read_vote` — one durable cell

A single small value, written before the node acts on it. *Prevents:* voting twice in one term
after a crash, which is the safety property Raft rests on.

### 2.3 `save_committed` / `read_committed` — a second durable cell, optional but not free

openraft's own documentation says this pair is optional **if** the state machine flushes before
`apply` returns; otherwise "your application has to deal with state reversion of state machine
carefully upon restart. E.g., do not serve read operation a new `commit` message is received."
This store implements it. *Prevents:* serving a read from a state machine that has silently gone
backwards across a restart.

### 2.4 `append(entries, callback)` — and the callback is the durability

Three rules, quoted from the trait's own contract, because each is a way to get it wrong:

- "When this method returns, the entries must be **readable**, i.e., a `LogReader` can read these
  entries."
- "When the **callback** is called, the entries must be **persisted on disk**." The callback may
  fire before or after the method returns.
- "There must not be a **hole** in logs. Because Raft only examine the last log id to ensure
  correctness."

The callback is the persist-before-send rule of the Raft thesis in a signature: nothing may be
sent to a peer before it has fired. *Prevents:* a leader acknowledging a write its own disk never
took, and a follower claiming a match index it cannot reproduce.

**The callback's offset is what a `Stored` cursor reports.** A store's durability granularity is
therefore the cursor's granularity: a hop cannot report "durable up to *N*" more finely than its
store can make *N* true ([decisions/0023](decisions/0023-completion-is-a-cursor.md) §4.2, §4.5).
A store that flushes in 4 MiB segments reports in 4 MiB steps, and that is honest rather than
coarse.

A consequence for testing: openraft's `LogFlushed::new` is `pub(crate)`, so **a store cannot
construct the callback in a unit test**.
`append` conformance is exercised through a live consensus group; everything else in this document
is unit-testable directly.

### 2.5 `truncate(log_id)` — discard a suffix, inclusive

A follower whose log diverged from the leader's must drop everything from a given index on. "It
must not leave a **hole** in logs." *Prevents:* two nodes with the same index holding different
entries, which is a split-brain that no later election can repair.

### 2.6 `try_get_log_entries(range)` and `limited_get_log_entries(start, end)`

Ranged reads, `end` exclusive. The *limited* form exists so the store may return fewer entries
than asked for — "If the specified range is too large, the implementation may return only the
first few log entries to ensure the result is not excessively large" — with one hard rule: "It
must not return empty result if the input range is not empty." *Prevents:* a replication read
materializing an arbitrary slice of the log in memory, which is the same bound-before-allocation
rule [INVARIANTS.md](INVARIANTS.md) applies to every remote-influenced allocation.

### 2.7 `purge(log_id)` — discard a prefix, inclusive

Compaction after a snapshot. Same hole rule. *Prevents:* a log that grows for the life of the
process, and — with §2.1 — a purge that loses the knowledge of what was purged.

### 2.8 The state machine: `applied_state`, `apply`

`applied_state` returns the last applied log id **and** the stored membership; `apply` applies a
batch and answers one response per entry. The applied position and the log's position are
separate, and a restart must agree with both. *Prevents:* replaying an applied entry, or skipping
an unapplied one — both of which are visible to a consumer as a duplicate or a lost message.

### 2.9 Snapshots: `get_snapshot_builder`, `begin_receiving_snapshot`, `install_snapshot`, `get_current_snapshot`

A snapshot is a **stream**, not a value: it may be larger than memory, which is the constraint
every weida transfer already has ([INVARIANTS.md](INVARIANTS.md): "core transport does not require
payload materialization"). Receiving one is `begin_receiving_snapshot` → write → `install_snapshot`
with its metadata; serving one is `get_current_snapshot`. *Prevents:* a node that cannot catch up
because the log it needs was purged, and a snapshot install that requires the whole state in RAM.

## 3. What the payload store owes

Not prescribed by the consensus engine, and therefore decided here:

1. **Write before the commit record.** The commit record in the log names a payload by digest;
   the payload must be durable before that record is proposed. *Prevents:* a committed record
   whose bytes no node has — the hole [0020](decisions/0020-cluster-and-discovery.md) §4.5 names
   as the worst failure of this design.
2. **Content-addressed by digest, deduplicated on it.** Two producers sending identical bytes
   cost one payload. *Prevents:* fan-out of a large message multiplying its storage.
3. **Streamed in and out.** Write and read take a stream, never a `Vec<u8>`. A 33 MB frame is an
   ordinary case and a 1 GiB one must not be a special one.
4. **Deletion is refcounted against the queues that reference it**, and a payload nobody
   references is removable. *Prevents:* both a leak and a delivery reading bytes that were freed.

## 4. Bounds, because every allocation needs one

| Bound | Applies to | Why it is not optional |
| --- | --- | --- |
| entries per read | `limited_get_log_entries` | a peer names the range ([INVARIANTS.md](INVARIANTS.md)) |
| bytes per read | the same call | entry count alone is not a memory bound, as the zmq queue and the broker queue already apply ([INVARIANTS.md](INVARIANTS.md)) |
| snapshot chunk | `install_snapshot`'s stream | a peer chooses the size of what it sends |
| log segment | the log's own files | a segment that grows without limit cannot be purged incrementally |
| payload store bytes | the payload half | a disk that fills is a failure row of [FAILURE_MODEL.md](FAILURE_MODEL.md) §3 |

## 5. The one ordering rule across the two halves

> A commit record naming a payload may be proposed only after that payload is durable on the node
> proposing it, and a follower may acknowledge such a record only after the payload is durable on
> the follower.

Everything else about the two halves is independent. This rule is what makes
`Replicated(n, flushed)` ([0004](decisions/0004-durability-levels.md) §4.2) true rather than
optimistic, and it is the property a conformance test must break on purpose: drop a follower's
payload stream mid-write and assert that no commit record naming it is ever committed.

## 6. What recovery must establish, in order

1. Read the vote (§2.2) and the committed pointer (§2.3).
2. Read the log's end and its purged prefix (§2.1).
3. Load the newest snapshot (§2.9) and the applied position (§2.8).
4. Reconcile: entries after the applied position are replayed; entries after the committed
   pointer are **not** treated as committed.
5. Verify that every commit record's payload is present (§3.1); a record whose payload is missing
   is a corrupt store, not a recoverable state — and it is the case [FAILURE_MODEL.md](FAILURE_MODEL.md)
   §3 files as "persisted state becomes corrupt".

## 7. What an append-only file cannot do

Three of the requirements above have no implementation in a plain append-only file, and they are
the reason this document exists before the store rather than after it:

1. **`truncate` (§2.5)** discards a suffix. An append-only file needs either a rewrite of the tail
   segment or a segmented log whose last segment can be dropped and rebuilt.
2. **`purge` (§2.7)** discards a prefix. That requires segments — a single file would have to be
   rewritten in full, which makes compaction cost the size of the log.
3. **The two durable cells (§2.2, §2.3)** are overwritten in place, not appended. They are small
   enough for a two-slot alternating write with a checksum, which is the standard answer, but they
   are not log entries and must not be stored as such.

The shape those three push toward is a **segmented log plus two small cells**, which is what every
implementation this repository studied ended up with; the numbers to carry into that design are
RabbitMQ's, because they are the warning: a per-node WAL capped at 512 MiB with node memory
recommended at 3-4× that, at least 32 bytes of metadata per message, and throughput falling with
message size and member count ([research/rabbitmq-amqp091.md](research/rabbitmq-amqp091.md) §9).

## 8. Sources

weida documents: [INVARIANTS.md](INVARIANTS.md); [FAILURE_MODEL.md](FAILURE_MODEL.md) §3;
[IMPLEMENTATION.md](IMPLEMENTATION.md) §1; [GUARANTEES.md](GUARANTEES.md) §1;
[requirements/zeughaus-video.md](requirements/zeughaus-video.md);
[0004](decisions/0004-durability-levels.md) §4.2; [0018](decisions/0018-minimal-broker.md) §4.8;
[0020](decisions/0020-cluster-and-discovery.md) §4.5;
[0021](decisions/0021-consensus-openraft.md) §4.3, §4.5.

Research sheets: [research/rabbitmq-amqp091.md](research/rabbitmq-amqp091.md) §9.

External, read 2026-09-13: openraft 0.9.25 source, `MIT OR Apache-2.0` — `src/storage/v2.rs` for
the `RaftLogStorage` and `RaftStateMachine` contracts and the quoted correctness rules,
`src/storage/mod.rs` for `limited_get_log_entries`, `src/storage/callback.rs` for `LogFlushed` and
its `pub(crate)` constructor.
