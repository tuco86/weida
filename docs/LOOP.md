# The loop

Standing instructions for an unattended weida session of unknown length. Read this file
whole before doing anything, then run the loop of §1 until the user interrupts. There is no
other stop condition: an empty backlog, a blocked item, a failed gate and a confusing state
are all the same instruction — go to the section that handles it and continue.

The user reads `docs/NIGHTLOG.md` first thing in the morning. Every line you write there is
written for that reader.

## 0. Orientation (once, at most 15 minutes)

Read in this order: `README.md`; `docs/INVARIANTS.md`; the `Status:` line of every file in
`docs/decisions/`, then the accepted ones in full; `docs/BACKLOG.md`; the last 30 lines of
`docs/NIGHTLOG.md`. Skim `docs/ARCHITECTURE.md` §1-2 and `docs/PATTERNS.md` §1. Do not read
the research catalogue now; read a sheet when an item needs it.

Then `git status --short` and `git log --oneline -5`. A dirty tree left by a previous session:
if it builds and the gate (§6) is green, commit it as `chore: recover uncommitted work`;
otherwise `git stash push -m "recovered <date>"` and log that under Review needed.

## 1. One iteration

1. Open `docs/BACKLOG.md`. Take the first item with status `ready` whose `needs` are all
   `done`. If there is none, do §4 and return here.
2. Set it `in_progress` with a UTC timestamp; commit only the backlog:
   `chore(backlog): start B-NNN`.
3. Scope it. If the item lacks a one-line acceptance, write one now. Assign the size class
   of §5. If it cannot fit its class, split: keep the first slice as this item, insert the
   remainder as new `ready` items directly after it, and note the split in the item.
4. Do the work under §2 and §3. Delegate independent research or codec slices to subagents
   when the tooling offers them; you alone run the gate, edit the backlog and the nightlog,
   and commit. Subagents get their own `CARGO_TARGET_DIR` or a worktree and never run
   `cargo build/test/clippy` in this tree.
5. Gate (§6). Green: commit the work with a conventional message (`type(scope): summary`,
   imperative, ≤ 60 chars, why over what), set the item `done` with the short hash. Red
   twice in a row on the same item: revert the item's paths (`git checkout -- <paths>` and
   `git clean -fd <paths>`, never the backlog or nightlog), set the item `blocked` with the
   exact first error line, continue.
6. Append the nightlog line (§7). Commit backlog and nightlog together:
   `chore(backlog): finish B-NNN`.
7. Every fifth finished item, run the review pass (§8) before taking the next item.
8. Return to 1.

## 2. Never hang

- Every `cargo` call carries an explicit `timeout`: 900 s for check/test/clippy/doc, 1800 s
  for a `--release` build. One cargo invocation at a time in this tree.
- Never in the foreground: ignored tests, the 1 GiB memory test, `cargo bench` without a
  name filter and `--warm-up-time 1 --measurement-time 3`, anything that reaches the
  internet. Such runs go `async: true` and are awaited with a bounded wait (≤ 900 s); still
  running after that means cancel, record the fact, move on.
- Every test you write bounds every await with a deadline ≤ 10 s (the `within()` helper of
  the existing suites) and orders events with channels or reads, never with sleeps above
  2 s. A test that needs a wall-clock phenomenon (idle timeout) uses the smallest interval
  that demonstrates it.
- Daemons and upstream reference implementations (a broker, a libzmq example) run only
  through the process supervisor (`hub start` with a `ready` condition) and are stopped in
  the same item, on success and on failure alike. Never background a process with `&`.
- No interactive commands, no `pty`, no `sudo`, no package installation, no prompts. A
  missing system dependency makes the item `blocked: needs <package>` and the test
  `#[ignore]` with the install command in its doc comment.
- Per-item wall-clock cap: 90 minutes of your own activity. At the cap, commit what is
  green, split the remainder into a new item, move on.
- A tool call that fails or returns nothing useful twice is not retried a third time the
  same way; change approach or block the item.

## 3. Never stop

- The turn ends only when the user interrupts. Never yield to ask a question, never end
  with a list of next steps, never wait on anything without a timeout.
- A decision you cannot look up in `docs/decisions/` or the research catalogue: take the
  evidence-backed option, write `docs/decisions/NNNN-<slug>.md` with `Status: provisional`
  and the evidence, keep the change reversible (a config default, a feature flag, or
  docs-only), and list it under Review needed. Never block on a human.
- An item that blocks three times across iterations becomes `parked` and is not touched
  again until a review pass reconsiders it.
- If you find yourself re-reading the same files without acting, take the top `ready` item
  and act.

## 4. Replenish the backlog

When fewer than five items are `ready`, add items until at least ten are, in priority
order, from these sources in this order:

1. Follow-ups in the `Consequences` section of accepted decisions not yet reflected in
   code or docs.
2. Known debt in `docs/IMPLEMENTATION.md` §6.
3. The next unstarted line of the roadmap, §9.
4. Findings of the last review pass.
5. Your own judgement of what a user of the current tree would hit first.

Item format, one per line group:

```
### B-017 — ZMTP greeting and handshake codec
kind: adapter | size: 90 | status: ready | needs: [B-012]
acceptance: `weida-zmtp` decodes and encodes greeting, NULL handshake and READY per 37/ZMTP; golden vectors from the RFC; fuzz target; no I/O.
```

Ids are monotonic and never reused. Items are never deleted; `dropped: <reason>` is the only
removal. Keep the file ordered by priority, ready items first.

## 5. Item kinds and size classes

| kind | what | cap (min) | must end with |
| --- | --- | --- | --- |
| research | one sheet or one decision note in `docs/research` or `docs/decisions` | 60 | file committed, the README table of that directory updated |
| spec | a change to a normative document | 45 | cross-references updated; `grep` shows no stale term left |
| measure | a benchmark or probe that yields a number | 60 | the number in `docs/IMPLEMENTATION.md` verified results |
| code | an implementation slice with tests | 90 | gate green; every public change reflected in docs |
| protocol | a codec, standalone library, binding or interop slice for a foreign protocol | 90 | an interop test against the upstream implementation, or `#[ignore]` with the reason |
| review | the pass of §8 | 45 | findings as backlog items |

## 6. The gate

Before any commit that touches Rust, in this order: `cargo fmt --all --check`;
`cargo clippy --workspace --all-targets -- -D warnings` (and again
`-p weida --no-default-features --all-targets`); `cargo nextest run -p <crate> --no-fail-fast`
for every crate the commit touches (the unit tier of `.config/nextest.toml`); the doc step
below. Docs-only commits run none.
Keep the failures block of the test output; a count without names is not a gate result.

The integration tier and every platform run in the CI after the push (`.zeughaus-ci/`): a
push to `main` runs the unit tier and the doctests on linux-amd64, linux-arm64, darwin-arm64
and windows-amd64, the nightly cron and a `v*` tag both tiers. Never run
`cargo test --workspace`, `--profile integration` or `--profile ci` locally.

**`--workspace` is not the whole workspace.** It builds every crate in its *default*
configuration, so code behind a default-off feature is compiled by none of the four commands
above — 909 tests and four synchronous facades, as of this writing. A commit that touches Rust
therefore also runs, for each crate whose sources it touched:

| crate | feature | commands |
| --- | --- | --- |
| `weida` | `blocking` | `clippy -p weida --features blocking --all-targets -- -D warnings`, `nextest run -p weida --features blocking` |
| `weida` | `swarm` | `clippy -p weida --features swarm --all-targets -- -D warnings`, `nextest run -p weida --features swarm` |
| `weida-zmq` | `blocking` | the same two with `-p weida-zmq` |
| `weida-mqtt` | `blocking` | the same two with `-p weida-mqtt` |
| `weida-nng` | `blocking` | the same two with `-p weida-nng` |

A commit that touches the core (`weida`, `weida-protocol`, `weida-runtime`, `weida-core`) runs
all four, because every facade sits on it.

Two features are **excluded on purpose**, with the reason: `nng-interop` and `libzmq-interop`
need a foreign C library present at run time, so they belong to the item that touches them and
to the interop suites that are `#[ignore]`d without it — `clippy -p weida-nng --features
nng-interop --all-targets` and the `weida-zmq` equivalent still compile in 2 s each and are
worth running when either protocol library changes. `extension-module` on the five `weida-*-py` crates is
turned on by maturin, never by cargo: a cargo run with it fails to link, and the wheel is proved
by `develop.sh` and `package.sh` instead.

**The doc step is blind in two directions.** An intra-doc link to a `#[cfg(feature = "…")]`
item resolves when the feature is on and is a **hard rustdoc error** when it is off, so the
failing configuration is exactly the one `--workspace` never runs. It has now happened three
times, most recently `crate::blocking::Cursors::changed` in `crates/weida/src/cursor.rs`, which
broke `cargo doc -p weida --no-default-features` while every `--workspace` doc run was green.
So the doc step is ten runs, not one, and they cost **8 s** warm together:

```sh
export RUSTDOCFLAGS="-D warnings"
for c in weida weida-mqtt weida-amqp weida-nats weida-nng weida-http; do
  cargo doc -q -p "$c" --no-default-features --no-deps || exit 1
done
for c in weida weida-zmq weida-mqtt weida-nng weida-broker weida-http; do
  cargo doc -q -p "$c" --all-features --no-deps || exit 1
done
```

The second blindness is the cache: `cargo doc` does not re-document an unchanged crate, so a
green doc step can be a cache hit rather than a check. `cargo clean --doc` costs **0.1 s** and
the full re-documentation **3.6 s**, so the guarantee is bought for four seconds and the doc
step above starts with it.

The feature runs cost the *next* plain gate almost nothing: measured right after them,
`clippy --workspace` took 3.7 s and `doc --workspace` 9.2 s, because cargo keys a fingerprint
per crate and feature set and reuses the dependencies both configurations share.

Never weaken a test, widen an `allow`, or add `#[ignore]` to make the gate pass. A test that
is flaky twice gets a deterministic rewrite or becomes a `blocked` item; it is not retried.
Every test you keep must fail on a plausible bug; tests that pin wording or wiring are
deleted, not maintained.

## 7. The nightlog

`docs/NIGHTLOG.md` is append-only below its header. One line per finished, blocked or parked
item:

```
2026-09-11T02:14Z | B-017 | done a1b2c3d | ZMTP greeting+handshake codec, 3 golden vectors, fuzz target | next B-018
```

At the top of the file keep a `## Review needed` section current: provisional decisions,
parked items, recovered stashes, anything a human should read first. Below it a
`## Numbers` section where every `measure` item adds its result with the command that
produced it.

## 8. Review pass (every fifth finished item)

1. Full gate on a clean tree.
2. `git diff <last-review-hash> --stat`; re-read every public API changed since then as its
   user would, and every new test as a reviewer would.
3. `docs/INVARIANTS.md` against the code: any new remote-influenced allocation without a
   named cap becomes a `ready` item at the top of the backlog.
4. Docs against code: grep the normative docs for identifiers that no longer exist.
5. Reconcile the backlog: reorder by dependency and value, park stale items, turn anything
   the implementation taught you into a research item that updates the relevant sheet.
6. Log the pass with the new anchor hash: `review | <hash> | <n findings>`.

## 9. Roadmap (seed order; the backlog refines it)

Stabilize the foundation first, then build on it; never start a later phase's slice while a
`ready` item of an earlier phase exists that it depends on.

**Phase A — the decided spec, realized.**
A1 Decisions 0004-0008 as accepted notes (8.3 durability levels `Stored(Written|Flushed)`,
   `Replicated(n, flushed)`; 8.5 refusal race closed on RFC 9000 §3.2; 8.7 guarantee sets:
   a default set plus a configurable superset inside the weida network, and an explicit
   managed Connector policy at a foreign-protocol boundary; 8.9 opaque paths, segmented
   Pub/Sub topics with wildcards, foreign topic syntaxes independent; session identity by
   fingerprint, resumption with L2).
A2 Spec sync: PROTOCOL, GUARANTEES, PATTERNS, INVARIANTS, ARCHITECTURE on the decided state;
   SYNTHESIS §8 items marked closed with the decision number.
A3 The four measurements of 0001/0002 §8 as benches or probes on the current tree; numbers
   into IMPLEMENTATION.md.
A4 0001 on the wire: DATA keys 6 and 7, HELLO guarantee declarations, negotiation
   intersection, detector mode, dedup window, golden vectors, fuzz targets, hostile tests.
A5 0002 as amended by 0011: one connection per dialled path, bound to the peer by the
   proved fingerprint, the named overload condition; the per-peer control connection and its
   second `Limits` profile are parked under 0011 §4.3 until a peer-scoped, latency-sensitive
   frame exists (B-045).
A6 Reassembly mode (eager, capped) and subscriber-side drop detection.
A7 Segmented topic filters with wildcards.
A8 Runtime ownership (`Runtime::owned`, `with_handle`), spawn/timer/DNS centralized in
   `runtime.rs`, `futures-io` traits beside `tokio::io` — the binding prerequisite.
A9 Local transports: inproc binding first; then `AF_UNIX` (Linux, macOS) and named pipes
   (Windows) per `docs/research/ipc.md` §11, peer credentials as the identity.

**Phase B — standalone foreign-protocol implementations.**
Each foreign protocol family is its own directory under `crates/<family>/` and produces a
standalone library that a user of that protocol can use with no weida in the picture
([decisions/0013](decisions/0013-competitor-libraries.md)). The common slices are:
(1) a sans-I/O codec with golden vectors and fuzz targets, with an empty `[dependencies]`
where practical;
(2) the library, in named sub-slices: context and endpoints; messages and bounded per-peer
queues; connection engine and reconnect; native protocol primitives; security and
authorization; options, monitoring and protocol-native devices;
(3) interop in both roles against named upstream implementations, plus measured numbers;
(4) language bindings, each following its own library, async first and sync second.

There is no general bridge or cross-protocol test slice. Applications may compose public
libraries explicitly. Broker integration is later Phase D work and takes the form of a managed
Connector resource attached to a Queue, with one explicit conversion policy rather than a
protocol-wide mapping.
B1 ZeroMQ/ZMTP 3.1: `weida-zmq`, complete to the definition of done of
   [0013](decisions/0013-competitor-libraries.md) §4.7.
B2 nanomsg/NNG SP: `weida-nng`, the library per
   [0013](decisions/0013-competitor-libraries.md) on the existing `weida-sp` codec, the same
   six slices in the same order minus the security slice, because SP defines no message-level
   authentication, authorization or sender identity.
B3 MQTT 5 **client**: `weida-mqtt`, the library per
   [0013](decisions/0013-competitor-libraries.md) on a new sans-I/O `weida-mqtt-codec`. The
   server side stays out of Phase B, because an MQTT server is a broker and the broker is
   Phase D.
B4 AMQP 1.0 client `weida-amqp` (link credit onto the L2 credit of 0003) and NATS core client
   `weida-nats`, the library per [0013](decisions/0013-competitor-libraries.md) each, on the
   codecs `weida-amqp-codec` and `weida-nats-codec`.
After every Phase B slice, one research item: update that protocol's sheet with what the
implementation taught, or open the next protocol's unknowns.

**Phase C — bindings, one per library, per
[0014](decisions/0014-parallel-libraries.md).** A binding follows its own library and nothing
else: the library's interop slice is green, then the binding — **asyncio first, then a sync
surface** over that library's blocking facade, which is the order that builds the facade once.
Python (PyO3 plus maturin) first, then Java, then Node; every binding sits on the one shared
`weida-py-core` (error mapping, the Runtime/Context bridge to asyncio, the `bytes` boundary)
and no binding re-derives it. Each reuses the library's interop suite: binding client against
the Rust implementation and a named upstream peer, and later binding against binding.

**Phase D — the L2 broker** (master plan Phase 6): managed Queue and Connector resources in
one control Raft, with one independent Raft group per replicated Queue and payload outside the
logs. The current in-memory broker is the single-process slice; the control-plane cutover
replaces `BrokerConfig::queues` as the inventory without adding declarations to the data plane.
D2 protocol integrations follow as managed Connectors. AMQP 0-9-1 against RabbitMQ is first:
publisher confirms map onto the attached Queue's achieved `Accepted`/`Stored` level,
`basic.qos` drives consumer credit, and lifecycle is an administrative resource rather than a
foreign-protocol backend hidden behind a weida pattern.
