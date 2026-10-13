//! openraft plus the I/O it deliberately leaves out.
//!
//! openraft is the Raft mechanics, the clock and the task driving; what it
//! does **not** have is a transport, and it says so by asking for one as a
//! trait. This crate fills that half with weida — QUIC connections, one
//! exchange per RPC, TLS-proved node identity and a streamed snapshot — so
//! that building a replicated service is writing a state machine rather than
//! writing a network layer
//! ([0021](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0021-consensus-openraft.md)).
//!
//! # What this crate provides, and what is left to you
//!
//! | Provided here | Yours |
//! | --- | --- |
//! | the transport: one exchange per RPC on the ALPN `weida-raft/0`, on its own port | the **state machine**: `apply`, and what a committed entry means |
//! | node identity: the fingerprint a peer proves in the handshake ([0020](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0020-cluster-and-discovery.md) §4.4) | what the bytes of a [`Request`] and a [`Response`] mean |
//! | follower writes forwarded to the leader, membership changes, the two-voter configuration refused by name | the **log store**; [`MemoryStore`] is the reference it has to match |
//! | the snapshot as one streamed exchange, not a chunked RPC | what a snapshot of your state machine *is* |
//! | a public minicbor codec for openraft's persisted types ([`codec`]) | where those bytes are stored |
//!
//! # Its own port and its own ALPN
//!
//! Consensus traffic runs on a second QUIC binding, on
//! [`ConsensusConfig::listen`], which offers only `weida-raft/0`
//! ([0021](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0021-consensus-openraft.md)
//! §4.4). A heartbeat therefore never waits behind application payload in a
//! shared receive window, and a client dialling `weida/0` fails the handshake
//! there instead of reaching a consensus endpoint.
//!
//! # Opaque payloads
//!
//! The log carries bytes: [`Request`] in, [`Response`] out. One concrete
//! [`TypeConfig`] fixes everything else — [`NodeId`], [`Node`], the entry and
//! the snapshot type — so the transport and the codec are written once and an
//! application encodes its own commands however it likes.
//!
//! # openraft is re-exported, on purpose
//!
//! A user of this crate implements openraft's own storage traits, so it needs
//! openraft's own types — and two versions of them in one binary do not
//! compose. [`openraft`] is therefore re-exported and **is** the version this
//! crate was built against. The coupling is stated rather than hidden:
//! openraft is pre-1.0 and promises incompatible changes before 1.0, so a
//! `weida-raft` release names one openraft minor line.
//!
//! # Where the tasks run, and why that is a construction-site rule
//!
//! openraft's `AsyncRuntime` is a **type-level** choice whose functions are
//! *associated* rather than methods: `fn spawn<T>(future: T)` takes no `self`.
//! There is therefore nothing to thread a `weida_runtime::Exec` handle
//! through, and no way to bind one instance of the engine to one runtime by
//! implementing the trait. What decides which runtime openraft's tasks land on
//! is the **ambient** one at the construction site.
//!
//! So the rule this crate follows is: [`Consensus::start`] is called and the
//! [`Consensus`] driven from inside its runtime's own `Exec`, which makes the
//! ambient runtime the caller's runtime, which is where every task openraft
//! spawns ends up. That is an invariant of *where* it is called, verified by a
//! test, rather than a property of a type.

pub mod codec;
mod consensus;
mod memory;
mod network;
mod server;

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use weida_core::{EndpointAddr, Fingerprint};

pub use codec::DecodeError;
pub use consensus::Consensus;
pub use memory::MemoryStore;
pub use openraft;

/// The endpoint path every node serves its RPCs on, inside the raft binding.
pub const RAFT_PATH: &str = "/raft";

/// A node's id: the fingerprint of the key it proves in every handshake.
///
/// The all-zero id is openraft's required `Default`: it appears legitimately
/// in `Vote::default()` and in the first log id, and it is refused as the id
/// of a real node.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub [u8; 32]);

impl NodeId {
    /// True for the all-zero placeholder, which names no node.
    pub fn is_zero(&self) -> bool {
        self.0 == [0; 32]
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&Fingerprint::from_bytes(self.0), f)
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&Fingerprint::from_bytes(self.0), f)
    }
}

impl From<Fingerprint> for NodeId {
    fn from(fp: Fingerprint) -> NodeId {
        NodeId(*fp.as_bytes())
    }
}

impl From<NodeId> for Fingerprint {
    fn from(id: NodeId) -> Fingerprint {
        Fingerprint::from_bytes(id.0)
    }
}

/// Where a node is reached: `weida://<fingerprint>@<host>:<port>/raft`.
///
/// The fingerprint in the URL **is** the node's [`NodeId`], so an address
/// cannot point at a node other than the one it names: the dial pins it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Node {
    /// The URL as written.
    pub url: String,
}

impl Node {
    /// Validates `url` as a node address.
    ///
    /// Refused: a URL that does not parse, names no fingerprint or the
    /// all-zero one, writes no port (consensus peers are dialled exactly, not
    /// discovered), or has a path other than [`RAFT_PATH`].
    pub fn new(url: impl Into<String>) -> Result<Node, Error> {
        let node = Node { url: url.into() };
        node.id()?;
        Ok(node)
    }

    /// The id the URL names, after the same validation as [`Node::new`].
    ///
    /// The `Default` node (an empty URL) fails here.
    pub fn id(&self) -> Result<NodeId, Error> {
        let addr = EndpointAddr::parse(&self.url)
            .map_err(|e| Error::InvalidNode(format!("{}: {e}", self.url)))?;
        let invalid = |reason: &str| Error::InvalidNode(format!("{}: {reason}", self.url));
        let id = NodeId::from(
            addr.peer
                .ok_or_else(|| invalid("the URL names no fingerprint"))?,
        );
        if id.is_zero() {
            return Err(invalid("the all-zero fingerprint names no node"));
        }
        if addr.port.is_none() {
            return Err(invalid("a consensus peer needs an explicit port"));
        }
        if addr.path != RAFT_PATH {
            return Err(invalid("the path must be /raft"));
        }
        Ok(id)
    }
}

/// An application command, as opaque bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Request(pub Vec<u8>);

/// What applying a [`Request`] answered, as opaque bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Response(pub Vec<u8>);

/// What [`Consensus::add_node`] made of a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// The node votes.
    Voter,
    /// The node replicates and does not vote: it is parked there because
    /// promoting it would have made a two-voter group.
    Learner,
}

openraft::declare_raft_types!(
    /// The one type configuration of a weida consensus group.
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = Node,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Everything a [`Consensus`] operation can fail with.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Binding, dialling, or an RPC the peer refused or that failed.
    Weida(weida::Error),
    /// A peer's bytes did not decode.
    Decode(DecodeError),
    /// A [`Node`] URL that names no node; carries the reason.
    InvalidNode(String),
    /// [`ConsensusConfig`] or the openraft configuration built from it was
    /// refused; carries the reason.
    Config(String),
    /// The change would leave exactly two voters.
    TwoVoters,
    /// The node to remove is the last voter.
    LastVoter,
    /// No leader is known, or it moved more often than a request follows.
    NoLeader,
    /// A forwarded request or a read barrier waited longer than
    /// [`ConsensusConfig::forward_timeout`].
    Timeout,
    /// The engine stopped or failed; carries openraft's text.
    Raft(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Weida(e) => write!(f, "consensus transport: {e}"),
            Error::Decode(e) => write!(f, "undecodable consensus message: {e}"),
            Error::InvalidNode(reason) => write!(f, "invalid node: {reason}"),
            Error::Config(reason) => write!(f, "invalid consensus configuration: {reason}"),
            Error::TwoVoters => f.write_str(
                "a two-voter group is refused: it tolerates no failure and doubles the quorum (0021 §4.6)",
            ),
            Error::LastVoter => f.write_str("the last voter cannot be removed"),
            Error::NoLeader => f.write_str("no leader is known"),
            Error::Timeout => f.write_str("the consensus operation timed out"),
            Error::Raft(text) => write!(f, "consensus engine: {text}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Weida(e) => Some(e),
            Error::Decode(e) => Some(e),
            _ => None,
        }
    }
}

impl From<weida::Error> for Error {
    fn from(e: weida::Error) -> Error {
        Error::Weida(e)
    }
}

impl From<DecodeError> for Error {
    fn from(e: DecodeError) -> Error {
        Error::Decode(e)
    }
}

/// How a [`Consensus`] binds, ticks, snapshots and bounds what it accepts.
#[derive(Clone, Debug)]
pub struct ConsensusConfig {
    /// openraft's cluster name, used in its logs. Default `"weida"`.
    pub cluster_name: String,
    /// The address the raft binding listens on.
    pub listen: SocketAddr,
    /// The `host:port` authority written into this node's URL. `None` uses
    /// the bound address, which must then not be unspecified.
    pub advertise: Option<String>,
    /// How often a leader sends heartbeats. Default 500 ms.
    ///
    /// openraft also uses it as the AppendEntries timeout, so it bounds the
    /// time one batch of [`ConsensusConfig::max_payload_entries`] may take
    /// to reach a follower and be answered.
    pub heartbeat_interval: Duration,
    /// Lower bound of the randomized election timeout. Default 1500 ms.
    pub election_timeout_min: Duration,
    /// Upper bound of the randomized election timeout. Default 3000 ms.
    pub election_timeout_max: Duration,
    /// How long sending one whole snapshot may take. Default 30 s.
    pub install_snapshot_timeout: Duration,
    /// Entries per AppendEntries, at most [`codec::MAX_ENTRIES`]. Default 64.
    pub max_payload_entries: u64,
    /// Applied entries since the last snapshot that trigger a new one.
    /// Default 5000.
    pub snapshot_logs_since_last: u64,
    /// Entries kept in the log behind a snapshot. Default 1000.
    pub max_in_snapshot_log_to_keep: u64,
    /// Largest RPC message, excluding snapshot data, sent or accepted.
    /// Default 16 MiB.
    pub max_rpc_bytes: usize,
    /// Largest snapshot accepted from a leader. Default 256 MiB.
    pub max_snapshot_bytes: usize,
    /// How long a forwarded request or a read barrier may take.
    /// Default 10 s.
    pub forward_timeout: Duration,
}

impl ConsensusConfig {
    /// The defaults, listening on `listen`.
    pub fn new(listen: SocketAddr) -> ConsensusConfig {
        ConsensusConfig {
            cluster_name: "weida".to_owned(),
            listen,
            advertise: None,
            heartbeat_interval: Duration::from_millis(500),
            election_timeout_min: Duration::from_millis(1500),
            election_timeout_max: Duration::from_millis(3000),
            install_snapshot_timeout: Duration::from_secs(30),
            max_payload_entries: 64,
            snapshot_logs_since_last: 5000,
            max_in_snapshot_log_to_keep: 1000,
            max_rpc_bytes: 16 << 20,
            max_snapshot_bytes: 256 << 20,
            forward_timeout: Duration::from_secs(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_url_must_name_a_pinned_exact_raft_endpoint() {
        let fp = Fingerprint::from_bytes([7; 32]);
        let good = format!("weida://{fp}@127.0.0.1:9000/raft");
        assert_eq!(
            Node::new(good).expect("valid node").id().expect("id"),
            NodeId([7; 32])
        );
        for bad in [
            "weida://127.0.0.1:9000/raft".to_owned(),
            format!("weida://{fp}@127.0.0.1/raft"),
            format!("weida://{fp}@127.0.0.1:9000/other"),
            format!(
                "weida://{}@127.0.0.1:9000/raft",
                Fingerprint::from_bytes([0; 32])
            ),
            String::new(),
        ] {
            assert!(
                matches!(Node::new(bad.clone()), Err(Error::InvalidNode(_))),
                "{bad:?} was accepted"
            );
        }
    }
}
