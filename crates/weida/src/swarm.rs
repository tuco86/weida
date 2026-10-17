//! SWARM: many sources, one verified blob.
//!
//! A blob is named by the BLAKE3 hash of its bytes. This module is the pattern
//! that lets a caller fetch one from every holder at once and check each piece
//! against that name the moment it arrives, in any order. It owns no storage
//! and no discovery: the application keeps the pieces it is handed and decides
//! which peers to connect to (`docs/decisions/0040-swarm-pattern.md`).
//!
//! The piece tree is [`Blob`] (root, size and piece size of a blob),
//! [`PieceList`] (the piece hashes, checked against [`Blob::root`]),
//! [`TreeHasher`] (builds that list in one pass over the bytes) and [`Bits`]
//! (which pieces someone holds).
//!
//! The bound side is a [`crate::Seeder`]: it hands each [`Ask`] to the
//! application as a [`SeedRequest`], which the application answers from its
//! own storage. One exchange per ask, on the request route a replier uses,
//! so SWARM adds no frame to the protocol. The dialling side is a
//! [`crate::Swarm`]: its [`Fetch`] asks every connected seeder at once and
//! yields each [`Piece`] once it verifies against the root.

mod gather;
mod seed;
mod tree;
mod wire;

pub use gather::{Fetch, FetchOptions, GatherState, Piece};
pub use seed::{MAX_PENDING_ASKS, MIN_RATE, SEED_STALL, SeedRequest, SeedState};
pub use tree::{
    Bits, Blob, BlobId, MAX_PIECES, ParseBlobIdError, PieceList, PieceSize, TreeHasher,
};

/// What a swarm asks a seeder about one [`Blob`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ask {
    /// The piece list, answered with [`SeedRequest::answer_list`]. Never
    /// asked for a blob of at most one piece, which has no list.
    List,
    /// Which pieces the seeder can serve now, answered with
    /// [`SeedRequest::answer_have`].
    Have,
    /// The bytes of one piece, answered with [`SeedRequest::answer_piece`].
    Piece(u32),
}
