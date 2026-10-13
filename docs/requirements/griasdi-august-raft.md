# Proposal from griasdi: a `weida-raft` an application can run (B-224)

Status: proposal from griasdi, for tuco86 to take over into weida.
Read against: `weida-raft` 0.1.0-alpha.5 (`crates/raft/weida-raft/src/lib.rs`),
openraft 0.9 with `storage-v2`, BACKLOG B-224, decisions 0020 §4.4, 0021 §4.2 and §4.4,
0022 §4.6.

**Answered by B-224 (shipped in `3e80585`).** The capabilities of §3 are there; the shipped API
differs from the proposal in these places. `Consensus::start(runtime, identity: impl
Into<IdentitySource>, log, state, config)` takes no `Listener`: it binds its own QUIC port on
`ConsensusConfig::listen` with ALPN `weida-raft/0` (§4's second-address option, not the shared
socket), and the node's URL is `weida://<fingerprint>@<advertise or bound addr>/raft`.
`leader()` is synchronous, not `async`. `add_node` returns a `Role`, because a second member is
parked as a learner (a two-voter group stays refused); `id()` and `node()` were added so a caller
can hand its own address to another member. `Request` and `Response` are opaque bytes, and
`weida_raft::codec` has the ten functions of §3.2. The crates.io release is not out yet: the tree
is at 0.1.0-alpha.6-dev, so griasdi pins the git rev until it is.

## 1. What griasdi builds

`griasdi-august` is griasdi's central service: the catalog of update builds and model
files, and the sink for encrypted survey reports. Servers watch its catalog and fetch
files in 1 MiB pieces from august and from each other; CI publishes into it.

- It runs as **one node on one VPS** now, and grows to three or more nodes later.
- The Raft log holds only **small catalog commands**: a signed manifest of at most
  32 KiB plus the BLAKE3 piece roots of its files. The state machine is the catalog
  itself (the current head per channel), a postcard value well under 1 MiB.
- **Payload stays out of the log**, exactly as 0022 §4.6 says: the files themselves,
  the pieces and the survey reports travel over ordinary weida transfers on ordinary
  connections and never enter consensus.

griasdi wants to write this once against the API it will keep, so that going from one
node to three is an `add_node` call and not a rewrite.

## 2. What blocks it in weida-raft 0.1.0-alpha.5

All of it is in `crates/raft/weida-raft/src/lib.rs`, and the crate says so itself
("The transport is not written yet", B-224):

- **`TypeConfig` fixes the application types.** `declare_raft_types!` sets
  `D = Command` (one variant, `Noop`) and `R = Applied` (a counter). An application
  cannot put its own command into the log.
- **There is no network.** `NoNetwork` answers every `RaftNetwork` method with
  `unreachable!()`, so any membership beyond one node panics instead of replicating,
  and a follower cannot forward a write.
- **The only constructor is bound to the in-memory store.** `Consensus::single_node(id)`
  builds a `MemoryStore` internally and calls `initialize` unconditionally; there is
  no way to hand in an application log store and state machine, and no way to start
  a node that is already initialized after a restart.
- **Nothing can persist openraft's own types.** A durable store must write `Entry`,
  `Vote`, `LogId`, `StoredMembership` and `SnapshotMeta` to disk. Without openraft's
  `serde` feature, which B-224 (as filed in 0021) explicitly avoids in favour of
  minicbor, an application has no encoding for them. If weida-raft encodes them on the
  wire with minicbor, the application should persist exactly those bytes rather than
  invent a second encoding.
- **`NodeId` is `u64`**, not the proved fingerprint (the crate's own comment on
  `NodeId`, and 0020 §4.4).

## 3. Proposed API

These are the names griasdi will code against. Where weida prefers other names or
shapes, griasdi adapts one module (§6); the capabilities are what matters.

### 3.1 Types

```rust
/// The proved key fingerprint of a node (0020 §4.4). The all-zero digest is the
/// `Default` openraft requires and is refused as a real id (B-224).
pub struct NodeId(pub [u8; 32]);

/// How to reach a node: its weida URL, which carries the fingerprint.
pub struct Node {
    pub url: String,
}

/// Opaque application payload of a log entry.
pub struct Request(pub Vec<u8>);

/// Opaque application answer to an applied entry.
pub struct Response(pub Vec<u8>);

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = Node,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>, // or a streamed type, see §5
        AsyncRuntime = openraft::TokioRuntime,
);
```

Opaque `Vec<u8>` payloads mean the transport needs no generics over the application,
the wire encoding of an entry stays weida's, and each application picks its own
payload encoding (griasdi uses postcard). An application that wants typed `D`/`R`
later can still get a generic variant; griasdi does not need one.

### 3.2 The wire codec, public

```rust
pub mod codec {
    pub fn encode_entry(e: &Entry<TypeConfig>) -> Vec<u8>;
    pub fn decode_entry(b: &[u8]) -> Result<Entry<TypeConfig>, DecodeError>;
    pub fn encode_vote(v: &Vote<NodeId>) -> Vec<u8>;
    pub fn decode_vote(b: &[u8]) -> Result<Vote<NodeId>, DecodeError>;
    pub fn encode_log_id(l: &LogId<NodeId>) -> Vec<u8>;
    pub fn decode_log_id(b: &[u8]) -> Result<LogId<NodeId>, DecodeError>;
    pub fn encode_membership(m: &StoredMembership<NodeId, Node>) -> Vec<u8>;
    pub fn decode_membership(b: &[u8]) -> Result<StoredMembership<NodeId, Node>, DecodeError>;
    pub fn encode_snapshot_meta(m: &SnapshotMeta<NodeId, Node>) -> Vec<u8>;
    pub fn decode_snapshot_meta(b: &[u8]) -> Result<SnapshotMeta<NodeId, Node>, DecodeError>;
}
```

The same minicbor encoding the transport uses, with PROTOCOL.md §5's bounds on decode.
An application store then persists exactly what travels, and a decode of bytes from
disk is as bounded as a decode of bytes from the network.

### 3.3 The consensus handle

```rust
impl Consensus {
    /// Starts this node's engine and its `weida-raft/0` transport.
    /// Called from inside the runtime's own `Exec` (0021 §4.2).
    pub async fn start(
        runtime: &weida::Runtime,
        listener: &weida::Listener,
        identity: weida::IdentitySource,
        log: impl RaftLogStorage<TypeConfig>,
        state: impl RaftStateMachine<TypeConfig>,
        config: ConsensusConfig,
    ) -> Result<Consensus, Error>;

    /// Initializes a one-member group: this node. A no-op when the store is
    /// already initialized, so it is safe on every start.
    pub async fn bootstrap(&self) -> Result<(), Error>;

    /// Proposes one entry and returns once it is committed and applied.
    /// On a follower it forwards to the leader over `weida-raft/0`.
    pub async fn write(&self, request: Request) -> Result<Response, Error>;

    /// Returns once this node has applied everything committed before the call
    /// (a linearizable read point).
    pub async fn read_barrier(&self) -> Result<(), Error>;

    /// Membership changes, joint consensus underneath; the two-member group stays
    /// refused by name (0021 §4.6).
    pub async fn add_node(&self, node: Node) -> Result<(), Error>;
    pub async fn remove_node(&self, id: NodeId) -> Result<(), Error>;

    pub async fn leader(&self) -> Option<NodeId>;

    pub async fn shutdown(self);
}
```

`ConsensusConfig` carries the cluster name, the election and heartbeat ticks and, if
needed, the raft listen address (§4). `Error` is weida-raft's own error type, not a
`String`.

## 4. Why this belongs in the transport

- **Node identity is the proved fingerprint.** Only weida knows which key a peer
  proved in the handshake; mapping it to `NodeId` and refusing a peer whose proved key
  differs from the `NodeId` it claims is transport work (0020 §4.4).
- **The ALPN `weida-raft/0` keeps consensus traffic off client connections**
  (0021 §4.4, 0002 §6.2): a heartbeat must not wait behind a file transfer in the
  same receive window. griasdi's august serves large piece transfers on the same
  node, so this separation is exactly what it needs.
- **Leader forwarding is network work.** A follower that receives `write` has to
  reach the leader by its `Node` URL and carry the answer back; doing that in each
  application would be every application writing half a transport.
- **One UDP port per node is preferred.** Today a `Listener`'s TLS sets ALPN to exactly
  `[b"weida/0"]` (ARCHITECTURE.md, server TLS). If weida can serve two ALPNs on one
  binding, raft connections should share the application's socket, so an operator
  opens one port. Otherwise `ConsensusConfig` names a second listen address.

## 5. What stays in griasdi

tuco86 has set the boundary: the log and the store are out of weida's scope for this
use. griasdi writes:

- a SQLite-backed `RaftLogStorage<TypeConfig>` and `RaftStateMachine<TypeConfig>`,
  method for method like `MemoryStore` (the reference), with `append` committing its
  transaction before it fires the flush callback;
- what a snapshot is: a postcard catalog under 1 MiB. `SnapshotData =
  Cursor<Vec<u8>>` is fine for griasdi, and so is any streamed type B-224 picks for
  the snapshot channel.

B-227 (the segmented log in weida-raft) is therefore not on griasdi's path; the codec
of §3.2 is.

## 6. The griasdi code that uses it

- `crates/griasdi-august/src/consensus.rs`: the only module that calls the weida-raft
  API. It wraps `Consensus::{start, bootstrap, write, shutdown}` into
  griasdi's own `open`, `bootstrap`, `write(Command) -> Applied`, `subscribe` and
  `shutdown`, encoding commands with postcard into `Request`.
- `crates/griasdi-august/src/raft_store.rs`: the SQLite store of §5, persisting
  openraft types through `weida_raft::codec`.

If the released API differs from this proposal, griasdi adapts only these two files.

## 7. Acceptance griasdi needs

1. **A single node bootstraps**: `start` with an empty application store, `bootstrap`,
   `write` returns the state machine's `Response`.
2. **It survives a process restart with an application store**: shut down, `start`
   again on the same store, `bootstrap` is a no-op, the node leads again and the
   applied state is intact; a further `write` succeeds.
3. **Three loopback nodes elect a leader, and a write on a follower is applied**:
   the follower forwards it over `weida-raft/0`, the call returns the leader's
   `Response`, and every node's state machine applied the entry.

And the release that carries it is on crates.io at the same version as `weida`, so
griasdi can pin both exactly.
