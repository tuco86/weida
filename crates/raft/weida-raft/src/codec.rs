//! The codec of openraft's persisted types and of this crate's RPC messages.
//!
//! The bytes are minicbor, and the discipline is the one of
//! `docs/PROTOCOL.md` §5: every composite is a **definite-length map** with
//! **unsigned integer keys in strictly ascending order**, so a duplicate or a
//! disordered key is rejected in constant space and the encoding of a value is
//! canonical. A key the decoder does not know is **skipped** with the bounded
//! skip of [`weida_protocol::skip_value`], which leaves room for later keys
//! (a group id, say) without a format break, while a hostile nesting is
//! refused rather than followed.
//!
//! Every length a peer states is checked against a cap *before* anything is
//! reserved for it ([`MAX_URL_BYTES`], [`MAX_MEMBERS`], [`MAX_ENTRIES`], ...),
//! and every top-level decode rejects bytes after the value. The public half
//! is exactly what an application needs to persist openraft's types in its own
//! store: `encode_x(&T) -> Vec<u8>` and `decode_x(&[u8]) -> Result<T,
//! DecodeError>` for [`Entry`], [`Vote`], [`LogId`], [`StoredMembership`] and
//! [`SnapshotMeta`].
//!
//! # Wire shapes
//!
//! Keys ascend; "opt" means the key is omitted when the value is `None`.
//!
//! | Type | Keys |
//! | --- | --- |
//! | leader id | 0 term, 1 node id (byte string of 32) |
//! | log id | 0 leader id, 1 index |
//! | vote | 0 leader id, 1 committed |
//! | node | 0 url (text) |
//! | membership | 0 configs (array of arrays of node ids), 1 nodes (array of maps: 0 node id, 1 node) |
//! | stored membership | 0 log id (opt), 1 membership |
//! | snapshot meta | 0 last log id (opt), 1 last membership, 2 snapshot id (text) |
//! | entry | 0 log id, 1 kind (0 blank, 1 normal, 2 membership), 2 request bytes (kind 1), 3 membership (kind 2) |

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;

use minicbor::data::Type;
use minicbor::{Decoder, Encoder};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{
    Entry, EntryPayload, LeaderId, LogId, Membership, SnapshotMeta, StoredMembership, Vote,
};
use weida_protocol::HeaderError;

use crate::network::{Done, ForwardOp, ForwardOutcome};
use crate::{Error, Node, NodeId, Request, Response, Role, TypeConfig};

/// The longest [`Node::url`] a message may carry, in bytes.
pub const MAX_URL_BYTES: usize = 1024;

/// The longest snapshot id a message may carry, in bytes.
pub const MAX_SNAPSHOT_ID_BYTES: usize = 256;

/// The most nodes a membership may list, and the most ids one config of it may
/// hold.
pub const MAX_MEMBERS: usize = 64;

/// The most configs a membership may hold: one, or two while it is a joint
/// configuration.
pub const MAX_JOINT_CONFIGS: usize = 2;

/// The most entries one AppendEntries request may carry.
///
/// A `u64` because it is compared against
/// [`ConsensusConfig::max_payload_entries`](crate::ConsensusConfig::max_payload_entries),
/// which is one.
pub const MAX_ENTRIES: u64 = 4096;

/// The longest failure text a forwarded request's reply may carry, in bytes.
pub const MAX_MESSAGE_BYTES: usize = 1024;

/// What an AppendEntries envelope weighs beyond the payload of its one entry,
/// as an upper bound.
///
/// The vote, the previous and the last log id, the entry's own log id and
/// kind and the map and byte-string headers around them, with every integer
/// at its widest. [`ConsensusConfig::max_rpc_bytes`](crate::ConsensusConfig::max_rpc_bytes)
/// caps the whole envelope on the receiving side, so a request of
/// `max_rpc_bytes - ENTRY_ENVELOPE_OVERHEAD` bytes is the largest a leader may
/// append: one more byte and every follower refuses the envelope, for ever.
pub const ENTRY_ENVELOPE_OVERHEAD: usize = 512;

/// The smallest [`ConsensusConfig::max_rpc_bytes`](crate::ConsensusConfig::max_rpc_bytes)
/// a node starts with.
///
/// A membership of [`MAX_MEMBERS`] nodes with the longest URLs, mid-change as
/// a joint configuration, is one log entry of about 73 KiB; a limit below it
/// would let a group grow to a membership that cannot be replicated.
pub const MIN_RPC_BYTES: usize = 128 << 10;

/// How deep an unknown key's value may nest before the decoder refuses it:
/// the depth bound the header codec of `weida-protocol` uses for the same
/// skip.
pub use weida_protocol::header_limits::MAX_SKIP_DEPTH;

/// Why a peer's or a store's bytes did not decode.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// The bytes are not well-formed CBOR, or a value had the wrong type.
    Malformed(&'static str),
    /// An indefinite-length item was used; every item is definite-length.
    Indefinite,
    /// A map key was not an unsigned integer.
    NonUintKey,
    /// A key was smaller than the one before it; keys strictly ascend.
    UnorderedKey(u64),
    /// A key occurred twice.
    DuplicateKey(u64),
    /// A required key was absent.
    MissingKey(u64),
    /// A string, byte string or array was longer than its cap.
    TooLong {
        /// The key whose value was too long.
        key: u64,
        /// The length the message stated.
        len: u64,
        /// The cap.
        max: u64,
    },
    /// A kind, operation or code value this version does not know.
    UnknownKind(u64),
    /// Bytes remained after the value.
    TrailingBytes,
    /// An unknown key's value was malformed or nested too deeply to skip.
    Skip(HeaderError),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Malformed(what) => write!(f, "malformed: {what}"),
            DecodeError::Indefinite => f.write_str("an indefinite-length item is not allowed"),
            DecodeError::NonUintKey => f.write_str("a map key is not an unsigned integer"),
            DecodeError::UnorderedKey(key) => write!(f, "key {key} is out of order"),
            DecodeError::DuplicateKey(key) => write!(f, "key {key} occurs twice"),
            DecodeError::MissingKey(key) => write!(f, "required key {key} is missing"),
            DecodeError::TooLong { key, len, max } => {
                write!(f, "key {key} is {len} long, the cap is {max}")
            }
            DecodeError::UnknownKind(kind) => write!(f, "unknown kind {kind}"),
            DecodeError::TrailingBytes => f.write_str("bytes remain after the value"),
            DecodeError::Skip(e) => write!(f, "an unknown key could not be skipped: {e}"),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DecodeError::Skip(e) => Some(e),
            _ => None,
        }
    }
}

type Res<T> = Result<T, DecodeError>;
type Enc = Encoder<Vec<u8>>;
type EncResult = Result<(), minicbor::encode::Error<Infallible>>;

// ---------------------------------------------------------------------------
// Public codec
// ---------------------------------------------------------------------------

/// Encodes a log entry.
pub fn encode_entry(entry: &Entry<TypeConfig>) -> Vec<u8> {
    build(|e| put_entry(e, entry))
}

/// Decodes a log entry written by [`encode_entry`].
pub fn decode_entry(bytes: &[u8]) -> Result<Entry<TypeConfig>, DecodeError> {
    decode_with(bytes, get_entry)
}

/// Encodes a vote.
pub fn encode_vote(vote: &Vote<NodeId>) -> Vec<u8> {
    build(|e| put_vote(e, vote))
}

/// Decodes a vote written by [`encode_vote`].
pub fn decode_vote(bytes: &[u8]) -> Result<Vote<NodeId>, DecodeError> {
    decode_with(bytes, get_vote)
}

/// Encodes a log id.
pub fn encode_log_id(log_id: &LogId<NodeId>) -> Vec<u8> {
    build(|e| put_log_id(e, log_id))
}

/// Decodes a log id written by [`encode_log_id`].
pub fn decode_log_id(bytes: &[u8]) -> Result<LogId<NodeId>, DecodeError> {
    decode_with(bytes, get_log_id)
}

/// Encodes a stored membership: the membership and the log id that carried it.
pub fn encode_membership(membership: &StoredMembership<NodeId, Node>) -> Vec<u8> {
    build(|e| put_stored_membership(e, membership))
}

/// Decodes a stored membership written by [`encode_membership`].
pub fn decode_membership(bytes: &[u8]) -> Result<StoredMembership<NodeId, Node>, DecodeError> {
    decode_with(bytes, get_stored_membership)
}

/// Encodes the metadata of a snapshot.
pub fn encode_snapshot_meta(meta: &SnapshotMeta<NodeId, Node>) -> Vec<u8> {
    build(|e| put_snapshot_meta(e, meta))
}

/// Decodes snapshot metadata written by [`encode_snapshot_meta`].
pub fn decode_snapshot_meta(bytes: &[u8]) -> Result<SnapshotMeta<NodeId, Node>, DecodeError> {
    decode_with(bytes, get_snapshot_meta)
}

// ---------------------------------------------------------------------------
// Crate-private RPC messages
// ---------------------------------------------------------------------------

/// The request envelope of one consensus exchange.
pub(crate) enum Rpc {
    /// A candidate asks for a vote.
    Vote(VoteRequest<NodeId>),
    /// A leader replicates entries, or sends a heartbeat.
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    /// A leader installs a snapshot; the snapshot bytes follow the envelope
    /// on the wire and are not part of it.
    Snapshot {
        /// The sender's vote.
        vote: Vote<NodeId>,
        /// What the snapshot covers.
        meta: SnapshotMeta<NodeId, Node>,
    },
    /// A follower hands an operation to the leader.
    Forward(ForwardOp),
}

const RPC_VOTE: u64 = 1;
const RPC_APPEND: u64 = 2;
const RPC_SNAPSHOT: u64 = 3;
const RPC_FORWARD: u64 = 4;

const OP_WRITE: u64 = 1;
const OP_READ_INDEX: u64 = 2;
const OP_ADD_NODE: u64 = 3;
const OP_REMOVE_NODE: u64 = 4;

const FAIL_TWO_VOTERS: u64 = 1;
const FAIL_LAST_VOTER: u64 = 2;
const FAIL_NO_LEADER: u64 = 3;
const FAIL_RAFT: u64 = 4;
const FAIL_INVALID_NODE: u64 = 5;
const FAIL_REQUEST_TOO_LARGE: u64 = 6;
const FAIL_TOO_MANY_NODES: u64 = 7;

/// Encodes a request envelope.
pub(crate) fn encode_rpc(rpc: &Rpc) -> Vec<u8> {
    build(|e| {
        e.map(2)?;
        match rpc {
            Rpc::Vote(req) => {
                e.u64(0)?.u64(RPC_VOTE)?;
                e.u64(1)?;
                put_vote_request(e, req)
            }
            Rpc::AppendEntries(req) => {
                e.u64(0)?.u64(RPC_APPEND)?;
                e.u64(1)?;
                put_append_request(e, req)
            }
            Rpc::Snapshot { vote, meta } => {
                e.u64(0)?.u64(RPC_SNAPSHOT)?;
                e.u64(1)?;
                e.map(2)?;
                e.u64(0)?;
                put_vote(e, vote)?;
                e.u64(1)?;
                put_snapshot_meta(e, meta)
            }
            Rpc::Forward(op) => {
                e.u64(0)?.u64(RPC_FORWARD)?;
                e.u64(1)?;
                put_forward_op(e, op)
            }
        }
    })
}

/// Decodes a request envelope.
///
/// An unknown kind is [`DecodeError::UnknownKind`]; the server answers exactly
/// that with an `Unsupported` refusal and every other error with `Rejected`.
pub(crate) fn decode_rpc(bytes: &[u8]) -> Result<Rpc, DecodeError> {
    decode_with(bytes, get_rpc)
}

/// Encodes the reply to a vote request.
pub(crate) fn encode_vote_response(r: &VoteResponse<NodeId>) -> Vec<u8> {
    build(|e| {
        e.map(2 + opt(&r.last_log_id))?;
        e.u64(0)?;
        put_vote(e, &r.vote)?;
        e.u64(1)?.bool(r.vote_granted)?;
        if let Some(l) = &r.last_log_id {
            e.u64(2)?;
            put_log_id(e, l)?;
        }
        Ok(())
    })
}

/// Decodes the reply to a vote request.
pub(crate) fn decode_vote_response(b: &[u8]) -> Result<VoteResponse<NodeId>, DecodeError> {
    decode_with(b, |d| {
        let mut r = MapReader::new(d)?;
        let (mut vote, mut granted, mut last_log_id) = (None, None, None);
        while let Some(key) = r.next_key()? {
            match key {
                0 => vote = Some(get_vote(r.d)?),
                1 => granted = Some(r.bool()?),
                2 => last_log_id = Some(get_log_id(r.d)?),
                _ => r.skip()?,
            }
        }
        Ok(VoteResponse {
            vote: vote.ok_or(DecodeError::MissingKey(0))?,
            vote_granted: granted.ok_or(DecodeError::MissingKey(1))?,
            last_log_id,
        })
    })
}

/// Encodes the reply to an AppendEntries request.
pub(crate) fn encode_append_response(r: &AppendEntriesResponse<NodeId>) -> Vec<u8> {
    build(|e| {
        match r {
            AppendEntriesResponse::Success => {
                e.map(1)?;
                e.u64(0)?.u64(0)?;
            }
            AppendEntriesResponse::PartialSuccess(log_id) => {
                e.map(1 + opt(log_id))?;
                e.u64(0)?.u64(1)?;
                if let Some(l) = log_id {
                    e.u64(1)?;
                    put_log_id(e, l)?;
                }
            }
            AppendEntriesResponse::Conflict => {
                e.map(1)?;
                e.u64(0)?.u64(2)?;
            }
            AppendEntriesResponse::HigherVote(vote) => {
                e.map(2)?;
                e.u64(0)?.u64(3)?;
                e.u64(2)?;
                put_vote(e, vote)?;
            }
        }
        Ok(())
    })
}

/// Decodes the reply to an AppendEntries request.
pub(crate) fn decode_append_response(
    b: &[u8],
) -> Result<AppendEntriesResponse<NodeId>, DecodeError> {
    decode_with(b, |d| {
        let mut r = MapReader::new(d)?;
        let (mut kind, mut log_id, mut vote) = (None, None, None);
        while let Some(key) = r.next_key()? {
            match key {
                0 => kind = Some(r.u64()?),
                1 => log_id = Some(get_log_id(r.d)?),
                2 => vote = Some(get_vote(r.d)?),
                _ => r.skip()?,
            }
        }
        match kind.ok_or(DecodeError::MissingKey(0))? {
            0 => {
                none(&[log_id.is_some(), vote.is_some()])?;
                Ok(AppendEntriesResponse::Success)
            }
            1 => {
                none(&[vote.is_some()])?;
                Ok(AppendEntriesResponse::PartialSuccess(log_id))
            }
            2 => {
                none(&[log_id.is_some(), vote.is_some()])?;
                Ok(AppendEntriesResponse::Conflict)
            }
            3 => {
                none(&[log_id.is_some()])?;
                Ok(AppendEntriesResponse::HigherVote(
                    vote.ok_or(DecodeError::MissingKey(2))?,
                ))
            }
            other => Err(DecodeError::UnknownKind(other)),
        }
    })
}

/// Encodes the reply to a snapshot installation.
pub(crate) fn encode_snapshot_response(r: &SnapshotResponse<NodeId>) -> Vec<u8> {
    build(|e| {
        e.map(1)?;
        e.u64(0)?;
        put_vote(e, &r.vote)
    })
}

/// Decodes the reply to a snapshot installation.
pub(crate) fn decode_snapshot_response(b: &[u8]) -> Result<SnapshotResponse<NodeId>, DecodeError> {
    decode_with(b, |d| {
        let mut r = MapReader::new(d)?;
        let mut vote = None;
        while let Some(key) = r.next_key()? {
            match key {
                0 => vote = Some(get_vote(r.d)?),
                _ => r.skip()?,
            }
        }
        Ok(SnapshotResponse {
            vote: vote.ok_or(DecodeError::MissingKey(0))?,
        })
    })
}

const KIND_DONE: u64 = 0;
const KIND_FORWARD_TO_LEADER: u64 = 1;
const KIND_FAILED: u64 = 2;

const DONE_WRITTEN: u64 = 0;
const DONE_READ_INDEX: u64 = 1;
const DONE_ADDED: u64 = 2;
const DONE_REMOVED: u64 = 3;

/// Encodes the reply to a forwarded operation.
///
/// An [`Error`] that has no code of its own travels as the engine-failure
/// code with its text, which is truncated to [`MAX_MESSAGE_BYTES`] at a
/// character boundary.
pub(crate) fn encode_forward_outcome(o: &ForwardOutcome) -> Vec<u8> {
    build(|e| {
        match o {
            ForwardOutcome::Done(done) => match done {
                Done::Written(response) => {
                    e.map(3)?;
                    e.u64(0)?.u64(KIND_DONE)?;
                    e.u64(1)?.bytes(&response.0)?;
                    e.u64(7)?.u64(DONE_WRITTEN)?;
                }
                Done::ReadIndex(log_id) => {
                    e.map(2 + opt(log_id))?;
                    e.u64(0)?.u64(KIND_DONE)?;
                    if let Some(l) = log_id {
                        e.u64(2)?;
                        put_log_id(e, l)?;
                    }
                    e.u64(7)?.u64(DONE_READ_INDEX)?;
                }
                Done::Added(role) => {
                    e.map(3)?;
                    e.u64(0)?.u64(KIND_DONE)?;
                    e.u64(3)?.u64(match role {
                        Role::Voter => 0,
                        Role::Learner => 1,
                    })?;
                    e.u64(7)?.u64(DONE_ADDED)?;
                }
                Done::Removed => {
                    e.map(2)?;
                    e.u64(0)?.u64(KIND_DONE)?;
                    e.u64(7)?.u64(DONE_REMOVED)?;
                }
            },
            ForwardOutcome::ForwardToLeader(leader) => {
                e.map(1 + opt(leader))?;
                e.u64(0)?.u64(KIND_FORWARD_TO_LEADER)?;
                if let Some(node) = leader {
                    e.u64(4)?;
                    put_node(e, node)?;
                }
            }
            ForwardOutcome::Failed(error) => {
                let (code, message) = failure_parts(error);
                e.map(2 + opt(&message))?;
                e.u64(0)?.u64(KIND_FAILED)?;
                e.u64(5)?.u64(code)?;
                if let Some(text) = &message {
                    e.u64(6)?.str(text)?;
                }
            }
        }
        Ok(())
    })
}

/// Decodes the reply to a forwarded operation.
pub(crate) fn decode_forward_outcome(b: &[u8]) -> Result<ForwardOutcome, DecodeError> {
    decode_with(b, |d| {
        let mut r = MapReader::new(d)?;
        let mut kind = None;
        let mut response: Option<Vec<u8>> = None;
        let mut read: Option<LogId<NodeId>> = None;
        let mut role: Option<u64> = None;
        let mut leader: Option<Node> = None;
        let mut failure: Option<u64> = None;
        let mut message: Option<String> = None;
        let mut done: Option<u64> = None;
        while let Some(key) = r.next_key()? {
            match key {
                0 => kind = Some(r.u64()?),
                1 => response = Some(r.bytes()?),
                2 => read = Some(get_log_id(r.d)?),
                3 => role = Some(r.u64()?),
                4 => leader = Some(get_node(r.d)?),
                5 => failure = Some(r.u64()?),
                6 => message = Some(r.text(6, MAX_MESSAGE_BYTES)?),
                7 => done = Some(r.u64()?),
                _ => r.skip()?,
            }
        }
        match kind.ok_or(DecodeError::MissingKey(0))? {
            KIND_DONE => {
                none(&[leader.is_some(), failure.is_some(), message.is_some()])?;
                let outcome = match done.ok_or(DecodeError::MissingKey(7))? {
                    DONE_WRITTEN => {
                        none(&[read.is_some(), role.is_some()])?;
                        Done::Written(Response(response.ok_or(DecodeError::MissingKey(1))?))
                    }
                    DONE_READ_INDEX => {
                        none(&[response.is_some(), role.is_some()])?;
                        Done::ReadIndex(read)
                    }
                    DONE_ADDED => {
                        none(&[response.is_some(), read.is_some()])?;
                        Done::Added(match role.ok_or(DecodeError::MissingKey(3))? {
                            0 => Role::Voter,
                            1 => Role::Learner,
                            other => return Err(DecodeError::UnknownKind(other)),
                        })
                    }
                    DONE_REMOVED => {
                        none(&[response.is_some(), read.is_some(), role.is_some()])?;
                        Done::Removed
                    }
                    other => return Err(DecodeError::UnknownKind(other)),
                };
                Ok(ForwardOutcome::Done(outcome))
            }
            KIND_FORWARD_TO_LEADER => {
                none(&[
                    response.is_some(),
                    read.is_some(),
                    role.is_some(),
                    failure.is_some(),
                    message.is_some(),
                    done.is_some(),
                ])?;
                Ok(ForwardOutcome::ForwardToLeader(leader))
            }
            KIND_FAILED => {
                none(&[
                    response.is_some(),
                    read.is_some(),
                    role.is_some(),
                    leader.is_some(),
                    done.is_some(),
                ])?;
                let error = match failure.ok_or(DecodeError::MissingKey(5))? {
                    FAIL_TWO_VOTERS => {
                        none(&[message.is_some()])?;
                        Error::TwoVoters
                    }
                    FAIL_LAST_VOTER => {
                        none(&[message.is_some()])?;
                        Error::LastVoter
                    }
                    FAIL_NO_LEADER => {
                        none(&[message.is_some()])?;
                        Error::NoLeader
                    }
                    FAIL_REQUEST_TOO_LARGE => {
                        none(&[message.is_some()])?;
                        Error::RequestTooLarge
                    }
                    FAIL_TOO_MANY_NODES => {
                        none(&[message.is_some()])?;
                        Error::TooManyNodes
                    }
                    FAIL_RAFT => Error::Raft(message.ok_or(DecodeError::MissingKey(6))?),
                    FAIL_INVALID_NODE => {
                        Error::InvalidNode(message.ok_or(DecodeError::MissingKey(6))?)
                    }
                    other => return Err(DecodeError::UnknownKind(other)),
                };
                Ok(ForwardOutcome::Failed(error))
            }
            other => Err(DecodeError::UnknownKind(other)),
        }
    })
}

/// The wire code of an error and the text that goes with it, if any.
fn failure_parts(error: &Error) -> (u64, Option<String>) {
    match error {
        Error::TwoVoters => (FAIL_TWO_VOTERS, None),
        Error::LastVoter => (FAIL_LAST_VOTER, None),
        Error::NoLeader => (FAIL_NO_LEADER, None),
        Error::RequestTooLarge => (FAIL_REQUEST_TOO_LARGE, None),
        Error::TooManyNodes => (FAIL_TOO_MANY_NODES, None),
        Error::InvalidNode(text) => (
            FAIL_INVALID_NODE,
            Some(truncate(text, MAX_MESSAGE_BYTES).to_owned()),
        ),
        // The engine's own text is carried as it is; any other error has no
        // code of its own and travels as its display text.
        Error::Raft(text) => (
            FAIL_RAFT,
            Some(truncate(text, MAX_MESSAGE_BYTES).to_owned()),
        ),
        other => (
            FAIL_RAFT,
            Some(truncate(&other.to_string(), MAX_MESSAGE_BYTES).to_owned()),
        ),
    }
}

/// `s` cut to at most `max` bytes, at a character boundary.
fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Runs an encoder against a fresh buffer.
fn build(f: impl FnOnce(&mut Enc) -> EncResult) -> Vec<u8> {
    let mut e = Encoder::new(Vec::new());
    f(&mut e).expect("encoding into a Vec is infallible");
    e.into_writer()
}

/// `1` when the optional key will be written, else `0`: the map-length term.
fn opt<T>(value: &Option<T>) -> u64 {
    u64::from(value.is_some())
}

fn put_leader_id(e: &mut Enc, leader: &LeaderId<NodeId>) -> EncResult {
    e.map(2)?;
    e.u64(0)?.u64(leader.term)?;
    e.u64(1)?.bytes(&leader.node_id.0)?;
    Ok(())
}

fn put_log_id(e: &mut Enc, log_id: &LogId<NodeId>) -> EncResult {
    e.map(2)?;
    e.u64(0)?;
    put_leader_id(e, &log_id.leader_id)?;
    e.u64(1)?.u64(log_id.index)?;
    Ok(())
}

fn put_vote(e: &mut Enc, vote: &Vote<NodeId>) -> EncResult {
    e.map(2)?;
    e.u64(0)?;
    put_leader_id(e, &vote.leader_id)?;
    e.u64(1)?.bool(vote.committed)?;
    Ok(())
}

fn put_node(e: &mut Enc, node: &Node) -> EncResult {
    e.map(1)?;
    e.u64(0)?.str(&node.url)?;
    Ok(())
}

fn put_membership(e: &mut Enc, membership: &Membership<NodeId, Node>) -> EncResult {
    e.map(2)?;
    e.u64(0)?;
    let configs = membership.get_joint_config();
    e.array(configs.len() as u64)?;
    for set in configs {
        e.array(set.len() as u64)?;
        for id in set {
            e.bytes(&id.0)?;
        }
    }
    e.u64(1)?;
    let nodes: Vec<(&NodeId, &Node)> = membership.nodes().collect();
    e.array(nodes.len() as u64)?;
    for (id, node) in nodes {
        e.map(2)?;
        e.u64(0)?.bytes(&id.0)?;
        e.u64(1)?;
        put_node(e, node)?;
    }
    Ok(())
}

fn put_stored_membership(e: &mut Enc, stored: &StoredMembership<NodeId, Node>) -> EncResult {
    e.map(1 + opt(stored.log_id()))?;
    if let Some(l) = stored.log_id() {
        e.u64(0)?;
        put_log_id(e, l)?;
    }
    e.u64(1)?;
    put_membership(e, stored.membership())
}

fn put_snapshot_meta(e: &mut Enc, meta: &SnapshotMeta<NodeId, Node>) -> EncResult {
    e.map(2 + opt(&meta.last_log_id))?;
    if let Some(l) = &meta.last_log_id {
        e.u64(0)?;
        put_log_id(e, l)?;
    }
    e.u64(1)?;
    put_stored_membership(e, &meta.last_membership)?;
    e.u64(2)?.str(&meta.snapshot_id)?;
    Ok(())
}

fn put_entry(e: &mut Enc, entry: &Entry<TypeConfig>) -> EncResult {
    let (kind, has_payload) = match &entry.payload {
        EntryPayload::Blank => (0, false),
        EntryPayload::Normal(_) => (1, true),
        EntryPayload::Membership(_) => (2, true),
    };
    e.map(2 + u64::from(has_payload))?;
    e.u64(0)?;
    put_log_id(e, &entry.log_id)?;
    e.u64(1)?.u64(kind)?;
    match &entry.payload {
        EntryPayload::Blank => {}
        EntryPayload::Normal(request) => {
            e.u64(2)?.bytes(&request.0)?;
        }
        EntryPayload::Membership(membership) => {
            e.u64(3)?;
            put_membership(e, membership)?;
        }
    }
    Ok(())
}

fn put_vote_request(e: &mut Enc, req: &VoteRequest<NodeId>) -> EncResult {
    e.map(1 + opt(&req.last_log_id))?;
    e.u64(0)?;
    put_vote(e, &req.vote)?;
    if let Some(l) = &req.last_log_id {
        e.u64(1)?;
        put_log_id(e, l)?;
    }
    Ok(())
}

fn put_append_request(e: &mut Enc, req: &AppendEntriesRequest<TypeConfig>) -> EncResult {
    e.map(2 + opt(&req.prev_log_id) + opt(&req.leader_commit))?;
    e.u64(0)?;
    put_vote(e, &req.vote)?;
    if let Some(l) = &req.prev_log_id {
        e.u64(1)?;
        put_log_id(e, l)?;
    }
    e.u64(2)?;
    e.array(req.entries.len() as u64)?;
    for entry in &req.entries {
        put_entry(e, entry)?;
    }
    if let Some(l) = &req.leader_commit {
        e.u64(3)?;
        put_log_id(e, l)?;
    }
    Ok(())
}

fn put_forward_op(e: &mut Enc, op: &ForwardOp) -> EncResult {
    e.map(if matches!(op, ForwardOp::ReadIndex) {
        1
    } else {
        2
    })?;
    match op {
        ForwardOp::Write(request) => {
            e.u64(0)?.u64(OP_WRITE)?;
            e.u64(1)?.bytes(&request.0)?;
        }
        ForwardOp::ReadIndex => {
            // Only the operation: the body is a map of one key.
            e.u64(0)?.u64(OP_READ_INDEX)?;
        }
        ForwardOp::AddNode(node) => {
            e.u64(0)?.u64(OP_ADD_NODE)?;
            e.u64(2)?;
            put_node(e, node)?;
        }
        ForwardOp::RemoveNode(id) => {
            e.u64(0)?.u64(OP_REMOVE_NODE)?;
            e.u64(3)?.bytes(&id.0)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Decodes one top-level value and refuses bytes after it.
fn decode_with<T>(bytes: &[u8], f: impl FnOnce(&mut Decoder<'_>) -> Res<T>) -> Res<T> {
    let mut d = Decoder::new(bytes);
    let value = f(&mut d)?;
    if d.position() != bytes.len() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(value)
}

/// Refuses an optional-key value that the message's kind does not allow.
fn none(present: &[bool]) -> Res<()> {
    if present.iter().any(|p| *p) {
        Err(DecodeError::Malformed("a key is not valid for this kind"))
    } else {
        Ok(())
    }
}

/// Reads a definite array header and checks it against `max` before the
/// caller reserves anything for it.
fn array_len(d: &mut Decoder<'_>, key: u64, max: u64) -> Res<u64> {
    let len = d
        .array()
        .map_err(|_| DecodeError::Malformed("expected an array"))?
        .ok_or(DecodeError::Indefinite)?;
    if len > max {
        return Err(DecodeError::TooLong { key, len, max });
    }
    Ok(len)
}

/// Refuses an indefinite string before minicbor's definite-only accessors
/// would report it as a type mismatch.
fn reject_indefinite(d: &Decoder<'_>) -> Res<()> {
    match d.datatype() {
        Ok(Type::BytesIndef | Type::StringIndef | Type::ArrayIndef | Type::MapIndef) => {
            Err(DecodeError::Indefinite)
        }
        _ => Ok(()),
    }
}

/// Reads a byte string of exactly 32 bytes: a node id.
fn get_node_id(d: &mut Decoder<'_>, key: u64) -> Res<NodeId> {
    reject_indefinite(d)?;
    let bytes = d
        .bytes()
        .map_err(|_| DecodeError::Malformed("expected a byte string"))?;
    if bytes.len() > 32 {
        return Err(DecodeError::TooLong {
            key,
            len: bytes.len() as u64,
            max: 32,
        });
    }
    let id: [u8; 32] = bytes
        .try_into()
        .map_err(|_| DecodeError::Malformed("a node id is not 32 bytes"))?;
    Ok(NodeId(id))
}

/// Reader for one strict map.
struct MapReader<'a, 'b> {
    d: &'a mut Decoder<'b>,
    remaining: u64,
    /// Previously read key. Keys strictly ascend, which makes duplicate
    /// detection complete for *every* key, unknown ones included, in constant
    /// space.
    last: Option<u64>,
}

impl<'a, 'b> MapReader<'a, 'b> {
    fn new(d: &'a mut Decoder<'b>) -> Res<MapReader<'a, 'b>> {
        let len = d
            .map()
            .map_err(|_| DecodeError::Malformed("expected a map"))?
            .ok_or(DecodeError::Indefinite)?;
        Ok(MapReader {
            d,
            remaining: len,
            last: None,
        })
    }

    fn next_key(&mut self) -> Res<Option<u64>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        match self.d.datatype() {
            Ok(Type::U8 | Type::U16 | Type::U32 | Type::U64) => {}
            Ok(_) => return Err(DecodeError::NonUintKey),
            Err(_) => return Err(DecodeError::Malformed("truncated key")),
        }
        let key = self.d.u64().map_err(|_| DecodeError::NonUintKey)?;
        if let Some(prev) = self.last {
            if key == prev {
                return Err(DecodeError::DuplicateKey(key));
            }
            if key < prev {
                return Err(DecodeError::UnorderedKey(key));
            }
        }
        self.last = Some(key);
        Ok(Some(key))
    }

    /// Skips the value of a key this version does not know.
    fn skip(&mut self) -> Res<()> {
        weida_protocol::skip_value(self.d, MAX_SKIP_DEPTH).map_err(DecodeError::Skip)
    }

    fn u64(&mut self) -> Res<u64> {
        self.d
            .u64()
            .map_err(|_| DecodeError::Malformed("expected an unsigned integer"))
    }

    fn bool(&mut self) -> Res<bool> {
        self.d
            .bool()
            .map_err(|_| DecodeError::Malformed("expected a boolean"))
    }

    fn text(&mut self, key: u64, max: usize) -> Res<String> {
        reject_indefinite(self.d)?;
        let s = self
            .d
            .str()
            .map_err(|_| DecodeError::Malformed("expected a text string"))?;
        if s.len() > max {
            return Err(DecodeError::TooLong {
                key,
                len: s.len() as u64,
                max: max as u64,
            });
        }
        Ok(s.to_owned())
    }

    /// A byte string with no cap of its own: the enclosing message is capped
    /// by whoever reads it off the wire.
    fn bytes(&mut self) -> Res<Vec<u8>> {
        reject_indefinite(self.d)?;
        let b = self
            .d
            .bytes()
            .map_err(|_| DecodeError::Malformed("expected a byte string"))?;
        Ok(b.to_vec())
    }

    fn node_id(&mut self, key: u64) -> Res<NodeId> {
        get_node_id(self.d, key)
    }
}

fn get_leader_id(d: &mut Decoder<'_>) -> Res<LeaderId<NodeId>> {
    let mut r = MapReader::new(d)?;
    let (mut term, mut node_id) = (None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => term = Some(r.u64()?),
            1 => node_id = Some(r.node_id(1)?),
            _ => r.skip()?,
        }
    }
    Ok(LeaderId::new(
        term.ok_or(DecodeError::MissingKey(0))?,
        node_id.ok_or(DecodeError::MissingKey(1))?,
    ))
}

fn get_log_id(d: &mut Decoder<'_>) -> Res<LogId<NodeId>> {
    let mut r = MapReader::new(d)?;
    let (mut leader_id, mut index) = (None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => leader_id = Some(get_leader_id(r.d)?),
            1 => index = Some(r.u64()?),
            _ => r.skip()?,
        }
    }
    Ok(LogId::new(
        leader_id.ok_or(DecodeError::MissingKey(0))?,
        index.ok_or(DecodeError::MissingKey(1))?,
    ))
}

fn get_vote(d: &mut Decoder<'_>) -> Res<Vote<NodeId>> {
    let mut r = MapReader::new(d)?;
    let (mut leader_id, mut committed) = (None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => leader_id = Some(get_leader_id(r.d)?),
            1 => committed = Some(r.bool()?),
            _ => r.skip()?,
        }
    }
    Ok(Vote {
        leader_id: leader_id.ok_or(DecodeError::MissingKey(0))?,
        committed: committed.ok_or(DecodeError::MissingKey(1))?,
    })
}

/// Reads a node. Its URL is bounded but not validated: a node that names no
/// peer is refused where it is used, not where it is read.
fn get_node(d: &mut Decoder<'_>) -> Res<Node> {
    let mut r = MapReader::new(d)?;
    let mut url = None;
    while let Some(key) = r.next_key()? {
        match key {
            0 => url = Some(r.text(0, MAX_URL_BYTES)?),
            _ => r.skip()?,
        }
    }
    Ok(Node {
        url: url.ok_or(DecodeError::MissingKey(0))?,
    })
}

fn get_configs(d: &mut Decoder<'_>) -> Res<Vec<BTreeSet<NodeId>>> {
    let count = array_len(d, 0, MAX_JOINT_CONFIGS as u64)?;
    let mut configs = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let len = array_len(d, 0, MAX_MEMBERS as u64)?;
        let mut set = BTreeSet::new();
        for _ in 0..len {
            if !set.insert(get_node_id(d, 0)?) {
                return Err(DecodeError::Malformed("a config lists a node twice"));
            }
        }
        configs.push(set);
    }
    Ok(configs)
}

fn get_member(d: &mut Decoder<'_>) -> Res<(NodeId, Node)> {
    let mut r = MapReader::new(d)?;
    let (mut id, mut node) = (None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => id = Some(r.node_id(0)?),
            1 => node = Some(get_node(r.d)?),
            _ => r.skip()?,
        }
    }
    Ok((
        id.ok_or(DecodeError::MissingKey(0))?,
        node.ok_or(DecodeError::MissingKey(1))?,
    ))
}

fn get_nodes(d: &mut Decoder<'_>) -> Res<BTreeMap<NodeId, Node>> {
    let len = array_len(d, 1, MAX_MEMBERS as u64)?;
    let mut nodes = BTreeMap::new();
    for _ in 0..len {
        let (id, node) = get_member(d)?;
        if nodes.insert(id, node).is_some() {
            return Err(DecodeError::Malformed("the node list names a node twice"));
        }
    }
    Ok(nodes)
}

fn get_membership(d: &mut Decoder<'_>) -> Res<Membership<NodeId, Node>> {
    let mut r = MapReader::new(d)?;
    let (mut configs, mut nodes) = (None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => configs = Some(get_configs(r.d)?),
            1 => nodes = Some(get_nodes(r.d)?),
            _ => r.skip()?,
        }
    }
    Ok(Membership::new(
        configs.ok_or(DecodeError::MissingKey(0))?,
        nodes.ok_or(DecodeError::MissingKey(1))?,
    ))
}

fn get_stored_membership(d: &mut Decoder<'_>) -> Res<StoredMembership<NodeId, Node>> {
    let mut r = MapReader::new(d)?;
    let (mut log_id, mut membership) = (None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => log_id = Some(get_log_id(r.d)?),
            1 => membership = Some(get_membership(r.d)?),
            _ => r.skip()?,
        }
    }
    Ok(StoredMembership::new(
        log_id,
        membership.ok_or(DecodeError::MissingKey(1))?,
    ))
}

fn get_snapshot_meta(d: &mut Decoder<'_>) -> Res<SnapshotMeta<NodeId, Node>> {
    let mut r = MapReader::new(d)?;
    let (mut last_log_id, mut last_membership, mut snapshot_id) = (None, None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => last_log_id = Some(get_log_id(r.d)?),
            1 => last_membership = Some(get_stored_membership(r.d)?),
            2 => snapshot_id = Some(r.text(2, MAX_SNAPSHOT_ID_BYTES)?),
            _ => r.skip()?,
        }
    }
    Ok(SnapshotMeta {
        last_log_id,
        last_membership: last_membership.ok_or(DecodeError::MissingKey(1))?,
        snapshot_id: snapshot_id.ok_or(DecodeError::MissingKey(2))?,
    })
}

fn get_entry(d: &mut Decoder<'_>) -> Res<Entry<TypeConfig>> {
    let mut r = MapReader::new(d)?;
    let mut log_id = None;
    let mut kind = None;
    let mut request: Option<Vec<u8>> = None;
    let mut membership = None;
    while let Some(key) = r.next_key()? {
        match key {
            0 => log_id = Some(get_log_id(r.d)?),
            1 => kind = Some(r.u64()?),
            2 => request = Some(r.bytes()?),
            3 => membership = Some(get_membership(r.d)?),
            _ => r.skip()?,
        }
    }
    let log_id = log_id.ok_or(DecodeError::MissingKey(0))?;
    let payload = match kind.ok_or(DecodeError::MissingKey(1))? {
        0 => {
            none(&[request.is_some(), membership.is_some()])?;
            EntryPayload::Blank
        }
        1 => {
            none(&[membership.is_some()])?;
            EntryPayload::Normal(Request(request.ok_or(DecodeError::MissingKey(2))?))
        }
        2 => {
            none(&[request.is_some()])?;
            EntryPayload::Membership(membership.ok_or(DecodeError::MissingKey(3))?)
        }
        other => return Err(DecodeError::UnknownKind(other)),
    };
    Ok(Entry { log_id, payload })
}

fn get_vote_request(d: &mut Decoder<'_>) -> Res<VoteRequest<NodeId>> {
    let mut r = MapReader::new(d)?;
    let (mut vote, mut last_log_id) = (None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => vote = Some(get_vote(r.d)?),
            1 => last_log_id = Some(get_log_id(r.d)?),
            _ => r.skip()?,
        }
    }
    Ok(VoteRequest {
        vote: vote.ok_or(DecodeError::MissingKey(0))?,
        last_log_id,
    })
}

fn get_entries(d: &mut Decoder<'_>) -> Res<Vec<Entry<TypeConfig>>> {
    // The cap is checked against the stated length before the first element
    // is read or any space reserved.
    let len = array_len(d, 2, MAX_ENTRIES)?;
    let mut entries = Vec::with_capacity(len as usize);
    for _ in 0..len {
        entries.push(get_entry(d)?);
    }
    Ok(entries)
}

fn get_append_request(d: &mut Decoder<'_>) -> Res<AppendEntriesRequest<TypeConfig>> {
    let mut r = MapReader::new(d)?;
    let (mut vote, mut prev_log_id, mut entries, mut leader_commit) = (None, None, None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => vote = Some(get_vote(r.d)?),
            1 => prev_log_id = Some(get_log_id(r.d)?),
            2 => entries = Some(get_entries(r.d)?),
            3 => leader_commit = Some(get_log_id(r.d)?),
            _ => r.skip()?,
        }
    }
    Ok(AppendEntriesRequest {
        vote: vote.ok_or(DecodeError::MissingKey(0))?,
        prev_log_id,
        entries: entries.ok_or(DecodeError::MissingKey(2))?,
        leader_commit,
    })
}

fn get_snapshot_body(d: &mut Decoder<'_>) -> Res<Rpc> {
    let mut r = MapReader::new(d)?;
    let (mut vote, mut meta) = (None, None);
    while let Some(key) = r.next_key()? {
        match key {
            0 => vote = Some(get_vote(r.d)?),
            1 => meta = Some(get_snapshot_meta(r.d)?),
            _ => r.skip()?,
        }
    }
    Ok(Rpc::Snapshot {
        vote: vote.ok_or(DecodeError::MissingKey(0))?,
        meta: meta.ok_or(DecodeError::MissingKey(1))?,
    })
}

fn get_forward_op(d: &mut Decoder<'_>) -> Res<ForwardOp> {
    let mut r = MapReader::new(d)?;
    let mut op = None;
    let mut request: Option<Vec<u8>> = None;
    let mut node: Option<Node> = None;
    let mut node_id: Option<NodeId> = None;
    while let Some(key) = r.next_key()? {
        match key {
            0 => op = Some(r.u64()?),
            1 => request = Some(r.bytes()?),
            2 => node = Some(get_node(r.d)?),
            3 => node_id = Some(r.node_id(3)?),
            _ => r.skip()?,
        }
    }
    match op.ok_or(DecodeError::MissingKey(0))? {
        OP_WRITE => {
            none(&[node.is_some(), node_id.is_some()])?;
            Ok(ForwardOp::Write(Request(
                request.ok_or(DecodeError::MissingKey(1))?,
            )))
        }
        OP_READ_INDEX => {
            none(&[request.is_some(), node.is_some(), node_id.is_some()])?;
            Ok(ForwardOp::ReadIndex)
        }
        OP_ADD_NODE => {
            none(&[request.is_some(), node_id.is_some()])?;
            Ok(ForwardOp::AddNode(node.ok_or(DecodeError::MissingKey(2))?))
        }
        OP_REMOVE_NODE => {
            none(&[request.is_some(), node.is_some()])?;
            Ok(ForwardOp::RemoveNode(
                node_id.ok_or(DecodeError::MissingKey(3))?,
            ))
        }
        other => Err(DecodeError::UnknownKind(other)),
    }
}

fn get_rpc(d: &mut Decoder<'_>) -> Res<Rpc> {
    let mut r = MapReader::new(d)?;
    let mut kind = None;
    let mut body = None;
    while let Some(key) = r.next_key()? {
        match key {
            0 => kind = Some(r.u64()?),
            1 => {
                // Keys ascend, so the kind is known before the body starts.
                let kind = kind.ok_or(DecodeError::MissingKey(0))?;
                body = Some(match kind {
                    RPC_VOTE => Rpc::Vote(get_vote_request(r.d)?),
                    RPC_APPEND => Rpc::AppendEntries(get_append_request(r.d)?),
                    RPC_SNAPSHOT => get_snapshot_body(r.d)?,
                    RPC_FORWARD => Rpc::Forward(get_forward_op(r.d)?),
                    other => return Err(DecodeError::UnknownKind(other)),
                });
            }
            _ => r.skip()?,
        }
    }
    let kind = kind.ok_or(DecodeError::MissingKey(0))?;
    if !(RPC_VOTE..=RPC_FORWARD).contains(&kind) {
        return Err(DecodeError::UnknownKind(kind));
    }
    body.ok_or(DecodeError::MissingKey(1))
}

#[cfg(test)]
mod tests {
    use openraft::CommittedLeaderId;
    use weida::Fingerprint;

    use super::*;

    fn id(n: u8) -> NodeId {
        NodeId([n; 32])
    }

    fn url(n: u8) -> String {
        format!(
            "weida://{}@127.0.0.1:9000/raft",
            Fingerprint::from_bytes([n; 32])
        )
    }

    fn node(n: u8) -> Node {
        Node { url: url(n) }
    }

    fn log_id(term: u64, n: u8, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, id(n)), index)
    }

    /// Two voter sets that overlap on node 2, plus node 4 as a learner.
    fn joint_membership() -> Membership<NodeId, Node> {
        Membership::new(
            vec![
                BTreeSet::from([id(1), id(2)]),
                BTreeSet::from([id(2), id(3)]),
            ],
            BTreeMap::from([
                (id(1), node(1)),
                (id(2), node(2)),
                (id(3), node(3)),
                (id(4), node(4)),
            ]),
        )
    }

    fn stored(log_id: Option<LogId<NodeId>>) -> StoredMembership<NodeId, Node> {
        StoredMembership::new(log_id, joint_membership())
    }

    fn meta() -> SnapshotMeta<NodeId, Node> {
        SnapshotMeta {
            last_log_id: Some(log_id(2, 1, 40)),
            last_membership: stored(Some(log_id(1, 1, 3))),
            snapshot_id: "2-40-1".to_owned(),
        }
    }

    fn entries() -> Vec<Entry<TypeConfig>> {
        vec![
            Entry {
                log_id: log_id(1, 1, 1),
                payload: EntryPayload::Blank,
            },
            Entry {
                log_id: log_id(1, 1, 2),
                payload: EntryPayload::Normal(Request(b"payload".to_vec())),
            },
            Entry {
                log_id: log_id(2, 2, 3),
                payload: EntryPayload::Membership(joint_membership()),
            },
        ]
    }

    #[test]
    fn entries_round_trip_for_every_payload_kind() {
        for entry in entries() {
            assert_eq!(decode_entry(&encode_entry(&entry)).unwrap(), entry);
        }
    }

    #[test]
    fn votes_round_trip() {
        for vote in [Vote::new(3, id(1)), Vote::new_committed(4, id(2))] {
            assert_eq!(decode_vote(&encode_vote(&vote)).unwrap(), vote);
        }
    }

    #[test]
    fn log_ids_round_trip() {
        let l = log_id(7, 3, u64::MAX);
        assert_eq!(decode_log_id(&encode_log_id(&l)).unwrap(), l);
    }

    #[test]
    fn stored_memberships_round_trip_with_and_without_a_log_id() {
        for s in [stored(Some(log_id(1, 1, 3))), stored(None)] {
            assert_eq!(decode_membership(&encode_membership(&s)).unwrap(), s);
        }
    }

    #[test]
    fn snapshot_meta_round_trips() {
        let mut m = meta();
        assert_eq!(decode_snapshot_meta(&encode_snapshot_meta(&m)).unwrap(), m);
        m.last_log_id = None;
        assert_eq!(decode_snapshot_meta(&encode_snapshot_meta(&m)).unwrap(), m);
    }

    fn rt_rpc(rpc: &Rpc) -> Rpc {
        decode_rpc(&encode_rpc(rpc)).unwrap()
    }

    #[test]
    fn every_rpc_envelope_round_trips() {
        let vote = Vote::new_committed(3, id(1));

        let req = VoteRequest::new(Vote::new(5, id(2)), Some(log_id(2, 1, 7)));
        match rt_rpc(&Rpc::Vote(req.clone())) {
            Rpc::Vote(got) => assert_eq!(got, req),
            _ => panic!("not a vote"),
        }
        let req = VoteRequest::new(Vote::new(5, id(2)), None);
        match rt_rpc(&Rpc::Vote(req.clone())) {
            Rpc::Vote(got) => assert_eq!(got, req),
            _ => panic!("not a vote"),
        }

        let append = AppendEntriesRequest::<TypeConfig> {
            vote,
            prev_log_id: Some(log_id(1, 1, 0)),
            entries: entries(),
            leader_commit: Some(log_id(1, 1, 2)),
        };
        match rt_rpc(&Rpc::AppendEntries(append.clone())) {
            Rpc::AppendEntries(got) => {
                assert_eq!(got.vote, append.vote);
                assert_eq!(got.prev_log_id, append.prev_log_id);
                assert_eq!(got.entries, append.entries);
                assert_eq!(got.leader_commit, append.leader_commit);
            }
            _ => panic!("not an append"),
        }
        let heartbeat = AppendEntriesRequest::<TypeConfig> {
            vote,
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: None,
        };
        match rt_rpc(&Rpc::AppendEntries(heartbeat)) {
            Rpc::AppendEntries(got) => {
                assert_eq!(got.vote, vote);
                assert_eq!(got.prev_log_id, None);
                assert!(got.entries.is_empty());
                assert_eq!(got.leader_commit, None);
            }
            _ => panic!("not an append"),
        }

        match rt_rpc(&Rpc::Snapshot { vote, meta: meta() }) {
            Rpc::Snapshot { vote: v, meta: m } => {
                assert_eq!(v, vote);
                assert_eq!(m, meta());
            }
            _ => panic!("not a snapshot"),
        }

        match rt_rpc(&Rpc::Forward(ForwardOp::Write(Request(b"w".to_vec())))) {
            Rpc::Forward(ForwardOp::Write(r)) => assert_eq!(r, Request(b"w".to_vec())),
            _ => panic!("not a write"),
        }
        assert!(matches!(
            rt_rpc(&Rpc::Forward(ForwardOp::ReadIndex)),
            Rpc::Forward(ForwardOp::ReadIndex)
        ));
        match rt_rpc(&Rpc::Forward(ForwardOp::AddNode(node(9)))) {
            Rpc::Forward(ForwardOp::AddNode(n)) => assert_eq!(n, node(9)),
            _ => panic!("not an add"),
        }
        match rt_rpc(&Rpc::Forward(ForwardOp::RemoveNode(id(8)))) {
            Rpc::Forward(ForwardOp::RemoveNode(n)) => assert_eq!(n, id(8)),
            _ => panic!("not a remove"),
        }
    }

    #[test]
    fn responses_round_trip() {
        let v = VoteResponse::new(Vote::new_committed(2, id(1)), Some(log_id(1, 1, 4)), true);
        assert_eq!(decode_vote_response(&encode_vote_response(&v)).unwrap(), v);
        let v = VoteResponse::new(Vote::new(2, id(1)), None, false);
        assert_eq!(decode_vote_response(&encode_vote_response(&v)).unwrap(), v);

        for r in [
            AppendEntriesResponse::Success,
            AppendEntriesResponse::PartialSuccess(Some(log_id(1, 1, 9))),
            AppendEntriesResponse::PartialSuccess(None),
            AppendEntriesResponse::Conflict,
            AppendEntriesResponse::HigherVote(Vote::new_committed(9, id(3))),
        ] {
            assert_eq!(
                decode_append_response(&encode_append_response(&r)).unwrap(),
                r
            );
        }

        let s = SnapshotResponse {
            vote: Vote::new_committed(2, id(1)),
        };
        assert_eq!(
            decode_snapshot_response(&encode_snapshot_response(&s))
                .unwrap()
                .vote,
            s.vote
        );
    }

    fn rt_outcome(o: &ForwardOutcome) -> ForwardOutcome {
        decode_forward_outcome(&encode_forward_outcome(o)).unwrap()
    }

    #[test]
    fn every_forward_outcome_round_trips() {
        match rt_outcome(&ForwardOutcome::Done(Done::Written(Response(
            b"r".to_vec(),
        )))) {
            ForwardOutcome::Done(Done::Written(r)) => assert_eq!(r, Response(b"r".to_vec())),
            _ => panic!("not written"),
        }
        match rt_outcome(&ForwardOutcome::Done(Done::ReadIndex(Some(log_id(
            1, 1, 5,
        ))))) {
            ForwardOutcome::Done(Done::ReadIndex(l)) => assert_eq!(l, Some(log_id(1, 1, 5))),
            _ => panic!("not a read index"),
        }
        // `ReadIndex(None)` stays distinguishable from `Removed`.
        match rt_outcome(&ForwardOutcome::Done(Done::ReadIndex(None))) {
            ForwardOutcome::Done(Done::ReadIndex(l)) => assert_eq!(l, None),
            _ => panic!("not a read index"),
        }
        for role in [Role::Voter, Role::Learner] {
            match rt_outcome(&ForwardOutcome::Done(Done::Added(role))) {
                ForwardOutcome::Done(Done::Added(got)) => assert_eq!(got, role),
                _ => panic!("not added"),
            }
        }
        assert!(matches!(
            rt_outcome(&ForwardOutcome::Done(Done::Removed)),
            ForwardOutcome::Done(Done::Removed)
        ));

        match rt_outcome(&ForwardOutcome::ForwardToLeader(Some(node(2)))) {
            ForwardOutcome::ForwardToLeader(n) => assert_eq!(n, Some(node(2))),
            _ => panic!("not a forward"),
        }
        match rt_outcome(&ForwardOutcome::ForwardToLeader(None)) {
            ForwardOutcome::ForwardToLeader(n) => assert_eq!(n, None),
            _ => panic!("not a forward"),
        }

        assert!(matches!(
            rt_outcome(&ForwardOutcome::Failed(Error::TwoVoters)),
            ForwardOutcome::Failed(Error::TwoVoters)
        ));
        assert!(matches!(
            rt_outcome(&ForwardOutcome::Failed(Error::LastVoter)),
            ForwardOutcome::Failed(Error::LastVoter)
        ));
        assert!(matches!(
            rt_outcome(&ForwardOutcome::Failed(Error::NoLeader)),
            ForwardOutcome::Failed(Error::NoLeader)
        ));
        match rt_outcome(&ForwardOutcome::Failed(Error::Raft("stopped".into()))) {
            ForwardOutcome::Failed(Error::Raft(m)) => assert_eq!(m, "stopped"),
            _ => panic!("not raft"),
        }
        match rt_outcome(&ForwardOutcome::Failed(Error::InvalidNode("bad".into()))) {
            ForwardOutcome::Failed(Error::InvalidNode(m)) => assert_eq!(m, "bad"),
            _ => panic!("not invalid node"),
        }
        // An error without a code of its own travels as the engine failure.
        match rt_outcome(&ForwardOutcome::Failed(Error::Timeout)) {
            ForwardOutcome::Failed(Error::Raft(m)) => {
                assert_eq!(m, Error::Timeout.to_string());
            }
            _ => panic!("not raft"),
        }
    }

    #[test]
    fn a_long_failure_text_is_cut_at_a_character_boundary() {
        // "x" then two-byte characters: the cap of 1024 falls inside a
        // character, so the cut backs off to the boundary at 1023.
        let text = format!("x{}", "\u{e9}".repeat(MAX_MESSAGE_BYTES));
        match rt_outcome(&ForwardOutcome::Failed(Error::Raft(text))) {
            ForwardOutcome::Failed(Error::Raft(m)) => {
                assert_eq!(m.len(), MAX_MESSAGE_BYTES - 1);
            }
            _ => panic!("not raft"),
        }
    }

    #[test]
    fn bytes_after_the_value_are_rejected() {
        let mut bytes = encode_vote(&Vote::new(1, id(1)));
        bytes.push(0);
        assert_eq!(decode_vote(&bytes), Err(DecodeError::TrailingBytes));
    }

    #[test]
    fn descending_keys_are_rejected() {
        let bytes = build(|e| {
            e.map(2)?;
            e.u64(1)?.u64(5)?;
            e.u64(0)?;
            put_leader_id(e, &LeaderId::new(1, id(1)))?;
            Ok(())
        });
        assert_eq!(decode_log_id(&bytes), Err(DecodeError::UnorderedKey(0)));
    }

    #[test]
    fn a_repeated_key_is_rejected() {
        let bytes = build(|e| {
            e.map(2)?;
            e.u64(0)?;
            put_leader_id(e, &LeaderId::new(1, id(1)))?;
            e.u64(0)?;
            put_leader_id(e, &LeaderId::new(1, id(1)))?;
            Ok(())
        });
        assert_eq!(decode_log_id(&bytes), Err(DecodeError::DuplicateKey(0)));
    }

    #[test]
    fn a_missing_required_key_is_named() {
        let bytes = build(|e| {
            e.map(1)?;
            e.u64(1)?.u64(5)?;
            Ok(())
        });
        assert_eq!(decode_log_id(&bytes), Err(DecodeError::MissingKey(0)));
    }

    #[test]
    fn an_indefinite_map_is_rejected() {
        let bytes = build(|e| {
            e.begin_map()?;
            e.end()?;
            Ok(())
        });
        assert_eq!(decode_log_id(&bytes), Err(DecodeError::Indefinite));
    }

    #[test]
    fn too_many_entries_are_refused_before_any_is_read() {
        // Nothing follows the array header: a decoder that read elements
        // first would fail with `Malformed`, not `TooLong`.
        let bytes = build(|e| {
            e.map(2)?;
            e.u64(0)?.u64(RPC_APPEND)?;
            e.u64(1)?;
            e.map(2)?;
            e.u64(0)?;
            put_vote(e, &Vote::new(1, id(1)))?;
            e.u64(2)?;
            e.array(MAX_ENTRIES + 1)?;
            Ok(())
        });
        assert_eq!(
            decode_rpc(&bytes).err(),
            Some(DecodeError::TooLong {
                key: 2,
                len: MAX_ENTRIES + 1,
                max: MAX_ENTRIES,
            })
        );
    }

    #[test]
    fn an_overlong_url_is_refused() {
        let long = Node {
            url: "a".repeat(MAX_URL_BYTES + 1),
        };
        let s = StoredMembership::new(
            None,
            Membership::new(
                vec![BTreeSet::from([id(1)])],
                BTreeMap::from([(id(1), long)]),
            ),
        );
        assert_eq!(
            decode_membership(&encode_membership(&s)),
            Err(DecodeError::TooLong {
                key: 0,
                len: MAX_URL_BYTES as u64 + 1,
                max: MAX_URL_BYTES as u64,
            })
        );
    }

    #[test]
    fn a_node_id_must_be_exactly_32_bytes() {
        let short = build(|e| {
            e.map(2)?;
            e.u64(0)?.u64(1)?;
            e.u64(1)?.bytes(&[1; 31])?;
            Ok(())
        });
        assert!(matches!(
            decode_vote_leader(&short),
            Err(DecodeError::Malformed(_))
        ));
        let long = build(|e| {
            e.map(2)?;
            e.u64(0)?.u64(1)?;
            e.u64(1)?.bytes(&[1; 33])?;
            Ok(())
        });
        assert!(matches!(
            decode_vote_leader(&long),
            Err(DecodeError::TooLong {
                key: 1,
                len: 33,
                max: 32
            })
        ));
    }

    fn decode_vote_leader(bytes: &[u8]) -> Result<LeaderId<NodeId>, DecodeError> {
        decode_with(bytes, get_leader_id)
    }

    /// A vote map with one unknown key, key 64, whose value `value` writes.
    fn vote_with_unknown(value: impl FnOnce(&mut Enc) -> EncResult) -> Vec<u8> {
        build(|e| {
            e.map(3)?;
            e.u64(0)?;
            put_leader_id(e, &LeaderId::new(4, id(2)))?;
            e.u64(1)?.bool(true)?;
            e.u64(64)?;
            value(e)
        })
    }

    #[test]
    fn an_unknown_key_with_a_scalar_value_is_skipped() {
        let bytes = vote_with_unknown(|e| e.u64(12345).map(|_| ()));
        assert_eq!(decode_vote(&bytes).unwrap(), Vote::new_committed(4, id(2)));
    }

    #[test]
    fn an_unknown_key_nested_too_deeply_is_refused() {
        let bytes = vote_with_unknown(|e| {
            for _ in 0..MAX_SKIP_DEPTH + 1 {
                e.array(1)?;
            }
            e.u64(0)?;
            Ok(())
        });
        assert_eq!(
            decode_vote(&bytes),
            Err(DecodeError::Skip(HeaderError::DepthExceeded))
        );
        // The same nesting one level shallower is skipped.
        let bytes = vote_with_unknown(|e| {
            for _ in 0..MAX_SKIP_DEPTH {
                e.array(1)?;
            }
            e.u64(0)?;
            Ok(())
        });
        assert!(decode_vote(&bytes).is_ok());
    }

    #[test]
    fn an_unknown_envelope_kind_is_named() {
        let bytes = build(|e| {
            e.map(2)?;
            e.u64(0)?.u64(9)?;
            e.u64(1)?;
            e.map(0)?;
            Ok(())
        });
        assert_eq!(decode_rpc(&bytes).err(), Some(DecodeError::UnknownKind(9)));
    }

    #[test]
    fn an_entry_payload_key_for_the_wrong_kind_is_malformed() {
        let bytes = build(|e| {
            e.map(3)?;
            e.u64(0)?;
            put_log_id(e, &log_id(1, 1, 1))?;
            e.u64(1)?.u64(0)?;
            e.u64(2)?.bytes(b"x")?;
            Ok(())
        });
        assert!(matches!(
            decode_entry(&bytes),
            Err(DecodeError::Malformed(_))
        ));
        let bytes = build(|e| {
            e.map(2)?;
            e.u64(0)?;
            put_log_id(e, &log_id(1, 1, 1))?;
            e.u64(1)?.u64(1)?;
            Ok(())
        });
        assert_eq!(decode_entry(&bytes), Err(DecodeError::MissingKey(2)));
        let bytes = build(|e| {
            e.map(2)?;
            e.u64(0)?;
            put_log_id(e, &log_id(1, 1, 1))?;
            e.u64(1)?.u64(7)?;
            Ok(())
        });
        assert_eq!(decode_entry(&bytes), Err(DecodeError::UnknownKind(7)));
    }

    /// An AppendEntries carrying `entries`, with every integer at its widest.
    fn widest_append(entries: Vec<Entry<TypeConfig>>) -> Rpc {
        let widest = |n: u8| log_id(u64::MAX, n, u64::MAX);
        Rpc::AppendEntries(AppendEntriesRequest {
            vote: Vote::new_committed(u64::MAX, id(1)),
            prev_log_id: Some(widest(2)),
            entries,
            leader_commit: Some(widest(3)),
        })
    }

    #[test]
    fn an_envelope_around_one_entry_weighs_less_than_its_payload_plus_the_overhead() {
        // The leader refuses a request that would not fit under the follower's
        // cap once enveloped, so the constant has to be what the encoder
        // writes, at every width of the byte-string header.
        for payload in [0usize, 1, 23, 24, 255, 256, 65_535, 65_536, 1 << 20] {
            let entry = Entry {
                log_id: log_id(u64::MAX, 4, u64::MAX),
                payload: EntryPayload::Normal(Request(vec![0xab; payload])),
            };
            let envelope = encode_rpc(&widest_append(vec![entry])).len();
            assert!(
                envelope <= payload + ENTRY_ENVELOPE_OVERHEAD,
                "a payload of {payload} bytes made an envelope of {envelope}, \
                 more than the {ENTRY_ENVELOPE_OVERHEAD} the leader allows for"
            );
        }
    }

    /// A membership of `count` nodes with the longest URLs, as a joint
    /// configuration over the first and the last `count - 1` of them.
    fn crowded_membership(count: usize) -> Membership<NodeId, Node> {
        let node_id = |n: usize| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(n as u64 + 1).to_be_bytes());
            NodeId(bytes)
        };
        let nodes: BTreeMap<NodeId, Node> = (0..count)
            .map(|n| {
                let padding = "a".repeat(MAX_URL_BYTES - url(0).len());
                (
                    node_id(n),
                    Node {
                        url: format!("{}{padding}", url(0)),
                    },
                )
            })
            .collect();
        let first: BTreeSet<NodeId> = (0..count.saturating_sub(1)).map(node_id).collect();
        let last: BTreeSet<NodeId> = (1..count).map(node_id).collect();
        Membership::new(vec![first, last], nodes)
    }

    #[test]
    fn the_fullest_membership_fits_the_smallest_rpc_limit() {
        let membership = crowded_membership(MAX_MEMBERS);
        let entry = Entry {
            log_id: log_id(u64::MAX, 4, u64::MAX),
            payload: EntryPayload::Membership(membership),
        };
        let envelope = encode_rpc(&widest_append(vec![entry])).len();
        assert!(
            envelope <= MIN_RPC_BYTES,
            "a full membership needs {envelope} bytes and the smallest limit is {MIN_RPC_BYTES}"
        );
    }

    #[test]
    fn a_membership_the_decoder_refuses_is_one_the_encoder_will_write() {
        // The asymmetry the leader's cap exists for: nothing on the encoding
        // side stops a 65th node, and every follower would answer it with a
        // decode failure for ever.
        let one_too_many = StoredMembership::new(None, crowded_membership(MAX_MEMBERS + 1));
        assert!(matches!(
            decode_membership(&encode_membership(&one_too_many)),
            Err(DecodeError::TooLong { max, .. }) if max == MAX_MEMBERS as u64
        ));
        let at_the_cap = StoredMembership::new(None, crowded_membership(MAX_MEMBERS));
        assert_eq!(
            decode_membership(&encode_membership(&at_the_cap)).unwrap(),
            at_the_cap
        );
    }

    #[test]
    fn the_leaders_refusals_cross_the_wire_by_name() {
        for error in [Error::RequestTooLarge, Error::TooManyNodes] {
            let name = error.to_string();
            match rt_outcome(&ForwardOutcome::Failed(error)) {
                ForwardOutcome::Failed(back) => assert_eq!(back.to_string(), name),
                other => panic!("not a failure: {other:?}"),
            }
        }
    }
}
