# 0021 — Consensus is a dependency, not a subsystem — openraft inside `weida-broker`

- **Status:** provisional
- **Date:** 2026-09-13
- **Related:** the owner's Phase 7 direction; [0004](0004-durability-levels.md) §4.2, §4.3;
  [0008](0008-session-identity.md) §4.2; [0013](0013-competitor-libraries.md) §4.1-§4.3;
  [0018](0018-minimal-broker.md) §4.2, §4.7, §4.8; [0020](0020-cluster-and-discovery.md) §4.4-§4.6;
  [ARCHITECTURE.md](../ARCHITECTURE.md) §4; [IMPLEMENTATION.md](../IMPLEMENTATION.md) §1, §6 and
  non-goal 9; [INVARIANTS.md](../INVARIANTS.md); [PROTOCOL.md](../PROTOCOL.md) §2.2, §4, §5.

## 1. The question

[0020](0020-cluster-and-discovery.md) decided that control state goes through consensus and
payload does not. This note decides **whose consensus**: an implementation in this repository, in
the shape the rest of the workspace uses for protocol work, or an existing crate.

The question is not rhetorical, because this repository has a rule that points both ways.
[0013](0013-competitor-libraries.md) made *reimplementing* the default for the protocols weida
competes with — five codecs with empty `[dependencies]` are the result — while the same
repository depends on quinn for QUIC, rustls for TLS, minicbor for CBOR and `crypto_box` for
NaCl. Which of the two is Raft?

The owner's initial direction was to build it: "zum ursprünglichen raft paper gabs eine sehr gute
go library die die mechanik und protokolle spezifiziert sodass die garantieen des rafts gelten…
es gab schon rust implementationen davon, aber noch keine mit async traits soweit ich weiß… also
will ich dass du da teil von weida machst." The evidence below made that direction change, and
the change is the decision.

## 2. The evidence, condensed

**The Go library is `etcd-io/raft`, and its defining property is that it does no I/O.** From its
own `doc.go` (Apache-2.0, read 2026-09-13): the caller holds a `Node`, reads from `Node.Ready()`
and then has four responsibilities — persist `HardState`, `Entries` and `Snapshot` ("when writing
an Entry with Index i, any previously-persisted entries with Index >= i must be discarded"); send
the messages, where "**no messages be sent until the latest HardState has been persisted to
disk**"; apply the snapshot and the committed entries, calling `ApplyConfChange` for a
configuration entry; and call `Advance()`. Incoming messages arrive by `Step(m)`, and
`Tick()` is called at intervals because "internally to the raft package **time is represented by
an abstract 'tick'**".

Three of its implementation notes are constraints on anything built here, not details:

- Membership changes happen **one node at a time**, and in this implementation "the membership
  change takes effect when its entry is **applied**, not when it is added to the log"; a further
  change is disallowed "while any uncommitted change appears in the leader's log", because two at
  once "would be unsafe since they should have different quorum requirements".
- Removing a member from a two-member cluster can deadlock, hence "it is **highly recommended to
  use three or more nodes** in every cluster".
- "An ID represents a unique node in a cluster **for all time**… for example **IP addresses make
  poor node IDs** since they may be reused", and IDs must be non-zero. [0020 §4.4] answers this
  with the proved fingerprint.

**The Rust landscape, measured rather than remembered** (crates.io registry index and API,
2026-09-13):

| Crate | What it is | State |
| --- | --- | --- |
| `raft-rs` (TiKV) | the port of etcd/raft's design: "includes the core Consensus Module only… you will need to build your own Log, State Machine and Transport components" | **synchronous**; last release 2021, with only scattered issues and pull requests since — maintenance mode |
| `openraft` (databendlabs) | async-first, runtime-agnostic through an `AsyncRuntime` trait, "deterministically tested: a turmoil-based simulation fuzzer steps the cluster tick by tick and **verifies Raft invariants from the paper and the TLA+ spec after every tick**"; the consensus engine of Databend's meta-service | stable line **0.9.25 (2026-07-28)**, 0.10 in `alpha.34`; **1 600 517** downloads total, **742 720 in 90 days**; licence **MIT OR Apache-2.0**; its own warning: "API is not stable yet. Before 1.0.0, an upgrade may contain incompatible changes" |

So the premise that no async Rust Raft exists is not quite right; what does not exist is
etcd/raft's *I/O-free, tick-driven core* with async storage and transport traits. `raft-rs` has
the shape and is synchronous and frozen; `openraft` is async and owns the driving loop.

**What openraft asks of us, read from its source rather than its README** (`openraft-0.9.25`):

- `AsyncRuntime` is **nine associated types and eight functions**: `JoinError`, `JoinHandle`,
  `Sleep`, `Instant`, `TimeoutError`, `Timeout`, `ThreadLocalRng`, `OneshotSender`,
  `OneshotReceiver`(+ its error), and `spawn`, `sleep`, `sleep_until`, `timeout`, `timeout_at`,
  `is_panic`, `thread_rng`, `oneshot`. Its default implementation is Tokio's, and every type maps
  onto a Tokio type we already depend on.
- `RaftStorage` is **thirteen methods**: `save_vote`/`read_vote`, `save_committed`/
  `read_committed`, `get_log_state`, `get_log_reader`, `append_to_log`,
  **`delete_conflict_logs_since`**, **`purge_logs_upto`**, `last_applied_state`,
  `apply_to_state_machine`, `get_snapshot_builder`, `begin_receiving_snapshot`,
  `install_snapshot`, `get_current_snapshot`; plus `RaftLogReader::try_get_log_entries` over a
  range and `limited_get_log_entries`.
- Its dependency set is **17 normal dependencies** on 0.9 (24 on 0.10-alpha), and a noticeable
  share are small utility crates by the same author — `anyerror`, `display-more`, `peel-off`,
  `validit`, `base2histogram`, `backoff-series`. That concentrates the audit surface on one
  maintainer; it is not *additional* trust, since it is the same person we would be trusting for
  openraft itself, but it is more crates to pin.

**What the dependency actually weighs, measured after the decision was taken (B-222).** Adding
`openraft = "0.9"` puts **39 crates** into the lockfile, and the reason is not consensus: openraft
0.9 depends **non-optionally** on `clap` (with `derive` and `env`), on `byte-unit` — which pulls
`rust_decimal`, `borsh`, `schemars` and `arrayvec` — and on `chrono`, which pulls
`iana-time-zone` and five `windows-*` crates onto a Linux host. Its `[features]` list has no
`default`, and none of those dependencies is optional, so no feature flag removes them.

Two facts soften that, and both are measured rather than assumed:

- **openraft's own code contains zero occurrences of `unsafe`** across its 208 source files. The
  `unsafe` in the added subtree is entirely transitive — `arrayvec` 58, `byte-unit` 21, `chrono`
  12, `ref-cast-impl` 10, `borsh` 7, `dyn-clone` 5, `derive_more` 5 — and arrives through exactly
  the dependencies consensus does not need.
- **0.10 removes them.** Its dependency list drops `clap` and `byte-unit` entirely and moves
  `tokio` behind a separate `openraft-rt` crate. So the migration this note treats as a risk is
  also the fix for the weight, which is an argument for doing it when 0.10 stabilizes rather than
  for avoiding it.

**What this repository already does with infrastructure.** It did not write QUIC (quinn), TLS
(rustls), the CBOR codec (minicbor), the SHA-2/SPKI parsing (ring, rustls-webpki) or the NaCl box
(`crypto_box`). [0013 §4.1]'s reimplement-it rule is about **competitor protocol libraries**,
where being a drop-in replacement is the product; nobody replaces weida with a Raft.

**What correctness costs.** A Raft that is subtly wrong is worse than none, because it claims a
guarantee. The mitigation that works is not more unit tests but determinism plus invariant
checking — which is exactly what openraft already runs (turmoil, tick by tick, paper and TLA+
invariants after every tick) and what a from-scratch core would have to build before its first
production use.

**Licence.** openraft is `MIT OR Apache-2.0`, and since B-068 this workspace is too, so the terms
are a superset match rather than a new condition. Reading a third party's code as a reference and
writing from the paper is free; copying code or test fixtures is not, and this note does not.

## 3. Options considered

| Option | Shape | Precedent | Named loss |
| --- | --- | --- | --- |
| **A — depend on openraft from `weida-broker`, with no openraft type in a public signature** | `[dependencies] openraft = "0.9"` in one crate; a module holds the `RaftTypeConfig`, the storage adapter, the network adapter and the `AsyncRuntime` over `Exec` | quinn, rustls, minicbor, `crypto_box` in this workspace; Databend's meta-service for openraft itself | A pre-1.0 dependency whose own README promises incompatible changes before 1.0; 17 crates added, several of them one author's utilities; the consensus engine's behaviour is not ours to fix under a deadline |
| B — write an etcd/raft-shaped core in `crates/raft/weida-raft`, no dependencies, I/O-free | `step`/`tick`/`ready`/`advance`, deterministic, `unsafe_code = "forbid"`, driven by `Exec` | etcd/raft's own design; `raft-rs` as the existing Rust port; the five dependency-free codecs in this tree | Months, and the first production use would be the first serious test. The invariant-checking harness openraft already has would have to exist **before** the core is trusted, which doubles the work that actually matters |
| C — a façade crate over openraft, so it can be swapped later | `weida-consensus` exposing our own traits, openraft behind it | — | A crate and a trait layer for a swap that may never happen; the same encapsulation is available for free by keeping openraft out of public signatures, which is how quinn is already handled |
| D — `raft-rs` | the etcd/raft port | TiKV | Synchronous in an async runtime — every call would need a blocking boundary — and effectively unmaintained since 2021 |

## 4. Decision

**Option A: openraft as an ordinary dependency of `weida-broker`, with a module boundary instead
of a crate boundary.** Option B is not refused as a design — it is the better shape, and it is
what etcd/raft proved — but it is refused as *this* project's work, because the part of it that
matters is the invariant harness and that work buys nothing a maintained crate does not already
have.

**4.1 One crate names it, and that crate is `weida-raft`: openraft plus the I/O openraft
deliberately leaves out.** This replaces what this sub-section first said — "a module inside
`weida-broker`, no openraft type in a public signature" — and the owner's argument is what
changed it: openraft is the mechanics without a transport and without a store, weida *is* a
transport, and the two together are a thing worth having on its own. A service that wants Raft
then writes a state machine instead of a network layer.

So `crates/raft/weida-raft` depends on openraft and on `weida`; `weida-broker` depends on
`weida-raft` (behind its `cluster` feature) and not on openraft directly; `weida-core`,
`weida-protocol`, `weida-runtime` and `weida` depend on none of it, which the dependency
direction of [ARCHITECTURE §4] already forbids.

**And `weida-raft` re-exports openraft, in its public API, deliberately.** This is the opposite
of the treatment quinn gets in `weida`, and the reason is that the relationship is the opposite: a
`weida` caller never implements a quinn trait, while a `weida-raft` user **implements openraft's
own traits** — `RaftStateMachine` is their application's core — so they need openraft's types, and
two versions of them in one binary do not compose. Hiding it would make the crate unusable. What
that costs is stated instead of avoided: openraft is pre-1.0, so a `weida-raft` release names one
openraft minor line, and `weida_raft::openraft` is the version it was built against. That is the
same contract `tokio-rustls` has with `rustls`, which this workspace already depends on.

The reason is mechanical rather than aesthetic: openraft promises incompatible changes before
1.0, and a leaked type would turn every such bump into a breaking change of weida's API — and of
every language binding built on it, where the type would have no representation at all.

**And it is optional for the broker: the `cluster` feature, off by default.** A broker that leads
no cluster must not link a consensus engine, and with the weight §2 measured — 39 crates, among
them a command-line parser — that is not a stylistic preference. `weida-broker` without `cluster`
is what B-201 and B-202 built; with it, `weida-raft` comes along. The cost is the one this repository
already knows: a non-default feature is invisible to the gate of [LOOP.md](../LOOP.md) §6 (B-107),
so the two extra commands — `clippy` and `test` with `--features cluster` — are run by hand and
recorded in each item's note until that gate names them.

**4.2 The runtime binding is a construction-site rule, not a type — and that is a correction to
what this note first said.** The prediction here was a sixty-line `WeidaRuntime` whose `spawn`
enters the handle `Exec` holds. B-222 implemented it and found that it cannot exist in that
shape: `AsyncRuntime`'s functions are **associated functions, not methods** — `fn spawn<T>(future:
T)` takes no `self` — and the trait is chosen at the type level through `RaftTypeConfig`, so there
is no instance to carry an `Exec` and no way to bind one engine to one runtime through the trait.

What decides where openraft's tasks run is therefore the **ambient** runtime at the construction
site. The rule is consequently: **a consensus group is built and driven from inside its broker's
own `Exec`**, which makes the broker's runtime the ambient one, which is where every task openraft
spawns lands. `AsyncRuntime = openraft::TokioRuntime` is then the honest choice, because a wrapper
that only delegates to the same Tokio functions would document the invariant without enforcing
anything.

The invariant is **tested rather than asserted**: a plain `#[test]` — deliberately with no ambient
runtime — builds a `weida::Runtime::owned`, starts the engine inside `exec().spawn(…)` and waits
for a committed entry on a channel. Without inheritance the `spawn` inside openraft would panic
and nothing would arrive. That is as close to proving "it runs on our runtime" as a static
interface allows, and it is why this sub-section no longer claims a type does the work.

**4.3 The storage traits are a requirements list for the Phase 5 store.** This is the
load-bearing consequence of choosing a library, and it inverts a sequencing assumption: the store
is built first, but its *interface* is decided here, because openraft prescribes what a log must
be able to do. A store that can only append and read will not fit.

Three findings from implementing it (B-222) refine what §2 read off the v1 trait:

- **The v2 traits are the ones to implement**, and they are **sealed unless the `storage-v2`
  feature is enabled** — without it the only implementable trait is the v1 `RaftStorage` behind an
  `Adaptor`. v2 is what 0.10 makes the sole API, so the feature is on and the migration is
  cheaper.
- The v2 split is `RaftLogStorage` — `get_log_state`, `get_log_reader`, `save_vote`/`read_vote`,
  `save_committed`/`read_committed`, `append`, `truncate`, `purge`, plus `RaftLogReader`'s
  `try_get_log_entries` and `limited_get_log_entries` — and `RaftStateMachine`: `applied_state`,
  `apply`, `get_snapshot_builder`, `begin_receiving_snapshot`, `install_snapshot`,
  `get_current_snapshot`.
- **`append` takes a flush callback**, `LogFlushed`, and the callback is what reports durability.
  The persist-before-send rule of the Raft thesis is therefore in a signature rather than in
  prose — and `LogFlushed::new` is `pub(crate)`, so a third-party store **cannot construct one in
  a unit test**. A store's conformance to `append` is only exercisable through a live `Raft`,
  which is a constraint on how B-223's store is tested, not a defect.

| Requirement | Method | Why it is not obvious |
| --- | --- | --- |
| truncate **backwards** | `truncate(log_id)` | a follower whose log diverged must discard a suffix — an append-only file needs a rewrite path |
| truncate **forwards** | `purge(log_id)` | compaction after a snapshot; the store must be able to forget a prefix without rewriting the rest |
| ranged reads | `try_get_log_entries(range)`, `limited_get_log_entries(start, end)` | replication reads arbitrary windows, and the *limited* form exists so a reader can respect a byte budget |
| hard state | `save_vote`/`read_vote`, `save_committed`/`read_committed` | two small durable cells with their own ordering rules, not entries in the log |
| snapshot in and out | `get_snapshot_builder`, `begin_receiving_snapshot`, `install_snapshot`, `get_current_snapshot` | a snapshot is a **stream**, not a value: it may be larger than memory, which is the same constraint weida's transfers already have |
| applied position | `applied_state`, `apply` | the state machine's position and the log's are separate, and a restart must agree with both |

**4.4 Raft traffic gets its own ALPN and its own connections: `weida-raft/0`.** Consensus
messages are not client traffic and must not share a client connection: [0002 §6.2]'s whole
argument is that a shared receive window couples flows, and a heartbeat that waits behind a
payload is how an election gets triggered by backpressure. A separate ALPN also keeps the client
protocol's frame budget untouched — no new frame kind for `AppendEntries` — and makes a broker's
peer port distinguishable from its client port without a second listener concept.

openraft's network trait is request/response shaped, which maps onto weida exchanges: one bidi
stream per RPC, the request on the initiating half, the response on the reply half. Encoding is
**CBOR through minicbor**, in `weida-protocol`'s discipline (ascending integer keys, unknown keys
skipped, cap checked before allocation), rather than openraft's optional `serde` feature — one
codec in this repository, not two.

**4.5 What goes into the log, stated as a list.** Queue registry and configuration, membership
and node metadata, per-queue leadership and term, the commit record for each admitted message
(queue, offset, payload digest, producer and sequence when present), and consumer state:
subscription registry, credit grants, and the settled/unsettled position of each delivery. What
stays out: **payload bytes** [0020 §4.5], and everything a node can recompute locally.

Consumer state in the log is the part worth naming, because it is what makes an acknowledgement
survive a leader change — and it is why B-203's in-memory `HashMap` of outstanding deliveries was
parked rather than finished: that table is a replicated structure, not a local one.

**4.6 Three or more nodes, and the two-node case refused at configuration.** etcd/raft's own
recommendation is a constraint here: a two-member cluster cannot remove a member safely. A
`BrokerConfig` naming exactly two cluster members is **refused when it is configured**, with a
message naming the reason — the same treatment [GUARANTEES §4] gives a guarantee level a
deployment cannot reach. One node is legal and means "no replication"; three is the smallest
fault-tolerant configuration.

**4.7 Testing: determinism first, then the invariants.** openraft's own approach is the model,
and it is available to us because the engine is the same: a deterministic simulation that steps
the cluster tick by tick and asserts the paper's invariants. What this repository adds on top is
what it always adds — the properties a *user* observes: a confirmed message is never lost while a
majority survives, a `Replicated(n, flushed)` report is never stronger than what happened, an
acknowledged delivery is never redelivered after a leader change, and an unacknowledged one
always is. Those are the tests that earn their place; re-testing Raft itself is testing a
dependency.

**4.8 Status is `provisional`, and what would change it.** Two measurements can reopen it: the
`AsyncRuntime`-over-`Exec` implementation turning out not to be clean (§4.2), or openraft's 0.10
migration proving disruptive enough that pinning 0.9 becomes a liability. A third, slower path
would be openraft becoming unmaintained — in which case Option B is still there, with this note's
§2 as its specification and the storage list of §4.3 as its interface.

## 5. Consequences and follow-ups

- **[ARCHITECTURE.md](../ARCHITECTURE.md) §4** gains `weida-raft` in the crate map and its own
  section: openraft plus the I/O it lacks, depended on by `weida-broker` and by nothing in the
  core. The re-export rule of §4.1 belongs beside the dependency direction, because it is the one
  place in this workspace where a foreign type is public on purpose.
- **[IMPLEMENTATION.md](../IMPLEMENTATION.md)** non-goal 9 is honoured and gains a pointer to
  [0020 §4.5] as the place where "no bulk through Raft" is made concrete; §6's debt list gains
  the pre-1.0 dependency as a known risk with its mitigation (§4.1).
- **[PROTOCOL.md](../PROTOCOL.md)** gains one paragraph: `weida-raft/0` is a separate ALPN whose
  frames are not part of the client protocol, so a client implementation needs none of it.
- **Backlog.** Four items, links relative to `docs/BACKLOG.md`.

### B-222 — `AsyncRuntime` over `Exec`, and openraft compiling inside `weida-broker`
kind: code | size: 60 | status: ready | needs: []
acceptance: openraft 0.9 in `crates/broker/Cargo.toml`, a `WeidaRuntime` implementing its nine associated types and eight functions over `weida-runtime`'s `Exec` — `spawn` entering the runtime's handle rather than calling `tokio::spawn`, which is [0013](0013-competitor-libraries.md) §4.2's rule — and a single-node `Raft` instance that elects itself leader and commits one entry, driven entirely through our own types. **No `openraft::` type in any public signature** of `weida-broker`, asserted by a test that only touches the public API. `cargo geiger` run once and its `unsafe` count for the new subtree recorded in the item's note.
note: this is the go/no-go for [0021](0021-consensus-openraft.md): if `AsyncRuntime` cannot be implemented cleanly over `Exec`, the note is reopened in favour of its Option B.

### B-223 — The store interface the consensus engine requires
kind: spec | size: 45 | status: ready | needs: [B-222]
acceptance: the Phase 5 store's interface written down **before** the store, as the thirteen methods of [0021](0021-consensus-openraft.md) §4.3 translated into requirements on a log: append, truncate backwards from a log id, purge forwards up to a log id, ranged and byte-limited reads, two durable cells with their ordering rule, and a snapshot that is a stream rather than a value. Each requirement names the failure it prevents, and the document states which of them an append-only file cannot satisfy without a rewrite path.
note: filed because choosing a library inverted a sequencing assumption: the store is built first and its interface is decided by this note, not by the store.

### B-224 — Raft traffic on its own ALPN
kind: code | size: 90 | status: blocked | needs: [B-222, B-223]
acceptance: `weida-raft/0` as a separate ALPN on its own connections, openraft's network trait implemented as one weida exchange per RPC — request on the initiating half, response on the reply half — encoded with minicbor in [PROTOCOL.md](../PROTOCOL.md) §5's discipline rather than openraft's `serde` feature. A three-node cluster on loopback elects a leader, replicates entries and survives the leader being killed, with the client protocol untouched: a test asserts that a client connection carries no Raft frame and that a Raft connection carries no client frame.
note: blocked on B-222's verdict and B-223's interface.

### B-225 — What the log holds, and what a leader change preserves
kind: code | size: 90 | status: blocked | needs: [B-224, the Phase 5 store]
acceptance: the list of [0021](0021-consensus-openraft.md) §4.5 as the replicated state machine — queue registry, membership, leadership and term, the per-message commit record, and **consumer state**: subscriptions, credit grants and the settled/unsettled position of every delivery. The user-observable properties are the tests: an acknowledged delivery is never redelivered after a leader change, an unacknowledged one always is, and a `Replicated(n, flushed)` report is never stronger than what happened ([0004](0004-durability-levels.md) §4.2). Re-testing Raft's own invariants is explicitly **not** in scope: that is the dependency's test suite.
note: this is where B-203's parked in-memory table of outstanding deliveries reappears as a replicated structure, which is why that item was stopped rather than finished.

## 6. What this note does not decide

- **The store's implementation.** Phase 5 owns it; §4.3 constrains only its interface.
- **Snapshot policy.** When to snapshot, how much log to keep, and whether a snapshot is a queue
  dump or an incremental structure. RabbitMQ's numbers are a warning worth carrying into that
  decision: a 512 MiB WAL cap and node memory at 3-4× it [rabbitmq-amqp091 §9].
- **ReadIndex and lease reads.** Whether a read may be served by a follower, and at what
  staleness. Kafka's answer is "committed records only, from the leader or configured followers"
  [kafka §4]; weida has no read path to decide it for yet.
- **Whether the JVM and Python bindings ever see a cluster concept.** Discovery is a URL form
  [0020 §4.2], so possibly nothing has to change; a redirect that a binding must follow is
  [0020 §4.3]'s slice to answer.
- **Multi-Raft.** One consensus group per cluster versus one per queue. RabbitMQ does one group
  per quorum queue with a default size of 3 [rabbitmq-amqp091 §9]; that scales differently and is
  its own decision once placement exists.

## 7. Sources

weida documents: [ARCHITECTURE.md](../ARCHITECTURE.md) §4;
[GUARANTEES.md](../GUARANTEES.md) §4; [PROTOCOL.md](../PROTOCOL.md) §2.2, §4, §5;
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §1, §6, non-goal 9; [INVARIANTS.md](../INVARIANTS.md);
[0002](0002-control-and-bulk-separation.md) §6.2; [0004](0004-durability-levels.md) §4.2, §4.3;
[0008](0008-session-identity.md) §4.2; [0013](0013-competitor-libraries.md) §4.1, §4.2, §4.3;
[0018](0018-minimal-broker.md) §4.2, §4.7; [0020](0020-cluster-and-discovery.md) §4.4, §4.5.

Research sheets: [rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §9;
[kafka.md](../research/kafka.md) §4.

External, read 2026-09-13: *etcd-io/raft* `doc.go`, Apache-2.0 —
<https://github.com/etcd-io/raft/blob/main/doc.go> — the `Ready`/`Advance` contract, the
persist-before-send rule, the abstract tick, the one-at-a-time membership rule and its two-node
consequence, and the node-id constraint. *openraft* 0.9.25 source, `MIT OR Apache-2.0` —
<https://crates.io/crates/openraft> — `src/async_runtime.rs` for the nine associated types and
eight functions, `src/storage/mod.rs` for the thirteen `RaftStorage` methods and the two
`RaftLogReader` reads, `Cargo.toml` for the dependency set; the crates.io registry index and API
for the version dates and download counts quoted in §2. *raft-rs* —
<https://github.com/tikv/raft-rs> — "includes the core Consensus Module only", the build-your-own
list, and its release history.
