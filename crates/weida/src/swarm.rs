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

mod tree;

pub use tree::{
    Bits, Blob, BlobId, MAX_PIECES, ParseBlobIdError, PieceList, PieceSize, TreeHasher,
};
