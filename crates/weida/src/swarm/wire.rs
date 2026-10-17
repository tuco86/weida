//! The body of one SWARM ask ([decisions/0040](../../../../docs/decisions/0040-swarm-pattern.md)
//! §4.2).
//!
//! One exchange per ask on the request route. The request body, big-endian:
//!
//! | offset | field |
//! | --- | --- |
//! | 0 | `u8` version = 1 |
//! | 1 | `u8` ask: 1 = LIST, 2 = HAVE, 3 = PIECE |
//! | 2 | `u8` piece log2 (10..=24) |
//! | 3 | `[u8; 32]` root |
//! | 35 | `u64` size |
//! | 43 | `u32` index (PIECE only) |
//!
//! The reply is the raw answer, of a length both sides know from the ask
//! alone ([`reply_len`]), so neither side needs a frame inside the stream.

use super::{Ask, Blob, BlobId, PieceSize};

/// The only version this side speaks.
const VERSION: u8 = 1;
const LIST: u8 = 1;
const HAVE: u8 = 2;
const PIECE: u8 = 3;

/// LIST and HAVE: everything but the index.
const SHORT: usize = 43;

/// The longest ask, PIECE: the cap a seeder reads an ask body under.
pub(crate) const MAX_ASK_BYTES: usize = 47;

/// Encodes `ask` for `blob`.
pub(crate) fn encode(blob: &Blob, ask: Ask) -> Vec<u8> {
    let mut out = Vec::with_capacity(MAX_ASK_BYTES);
    out.push(VERSION);
    out.push(match ask {
        Ask::List => LIST,
        Ask::Have => HAVE,
        Ask::Piece(_) => PIECE,
    });
    out.push(blob.piece().log2());
    out.extend_from_slice(blob.root().as_bytes());
    out.extend_from_slice(&blob.size().to_be_bytes());
    if let Ask::Piece(index) = ask {
        out.extend_from_slice(&index.to_be_bytes());
    }
    out
}

/// Decodes an ask, refusing everything a seeder must not hand its
/// application: a length that does not fit the ask, another version, an
/// unknown ask, a piece size out of range, a blob [`Blob::new`] refuses, an
/// empty blob (it has no pieces to ask about), LIST for a blob of at most one
/// piece (it has no list), and PIECE past the end.
pub(crate) fn decode(bytes: &[u8]) -> Option<(Blob, Ask)> {
    if bytes.len() < SHORT || bytes[0] != VERSION {
        return None;
    }
    let kind = bytes[1];
    let expected = match kind {
        LIST | HAVE => SHORT,
        PIECE => MAX_ASK_BYTES,
        _ => return None,
    };
    if bytes.len() != expected {
        return None;
    }
    let piece = PieceSize::from_log2(bytes[2])?;
    let root = BlobId::from_bytes(bytes[3..35].try_into().ok()?);
    let size = u64::from_be_bytes(bytes[35..43].try_into().ok()?);
    if size == 0 {
        return None;
    }
    let blob = Blob::new(root, size, piece)?;
    let ask = match kind {
        LIST if blob.count() >= 2 => Ask::List,
        LIST => return None,
        HAVE => Ask::Have,
        _ => {
            let index = u32::from_be_bytes(bytes[43..47].try_into().ok()?);
            if index >= blob.count() {
                return None;
            }
            Ask::Piece(index)
        }
    };
    Some((blob, ask))
}

/// The exact length of the answer to `ask`: LIST `count × 32`, HAVE one bit
/// per piece rounded up to bytes, PIECE the piece's span (the last piece is
/// short). Zero for a piece past the end, which [`decode`] never yields.
pub(crate) fn reply_len(blob: &Blob, ask: Ask) -> u64 {
    match ask {
        Ask::List => u64::from(blob.count()) * 32,
        Ask::Have => u64::from(blob.count()).div_ceil(8),
        Ask::Piece(index) => blob.span(index).map_or(0, |span| span.end - span.start),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(size: u64) -> Blob {
        // Any root will do for a non-empty blob: the wire carries it, and
        // only a fetcher checks content against it.
        Blob::new(BlobId::from_bytes([7; 32]), size, PieceSize::MIN).expect("a valid blob")
    }

    #[test]
    fn every_ask_round_trips() {
        let b = blob(3 * 1024 + 5);
        for ask in [Ask::List, Ask::Have, Ask::Piece(0), Ask::Piece(3)] {
            let bytes = encode(&b, ask);
            assert_eq!(
                bytes.len(),
                if matches!(ask, Ask::Piece(_)) { 47 } else { 43 }
            );
            assert_eq!(decode(&bytes), Some((b, ask)), "{ask:?}");
        }
        assert_eq!(reply_len(&b, Ask::List), 4 * 32);
        assert_eq!(reply_len(&b, Ask::Have), 1);
        assert_eq!(reply_len(&b, Ask::Piece(0)), 1024);
        assert_eq!(reply_len(&b, Ask::Piece(3)), 5);
    }

    #[test]
    fn every_malformed_ask_is_refused() {
        let b = blob(3 * 1024 + 5);
        let have = encode(&b, Ask::Have);
        let piece = encode(&b, Ask::Piece(1));
        let edit = |mut bytes: Vec<u8>, at: usize, value: u8| {
            bytes[at] = value;
            bytes
        };
        let with_size = |mut bytes: Vec<u8>, size: u64| {
            bytes[35..43].copy_from_slice(&size.to_be_bytes());
            bytes
        };
        let with_index = |mut bytes: Vec<u8>, index: u32| {
            bytes[43..47].copy_from_slice(&index.to_be_bytes());
            bytes
        };
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("empty", Vec::new()),
            ("one byte short", have[..42].to_vec()),
            ("HAVE with an index", edit(piece.clone(), 1, HAVE)),
            ("PIECE without an index", edit(have.clone(), 1, PIECE)),
            ("one trailing byte", [piece.clone(), vec![0]].concat()),
            ("version 0", edit(have.clone(), 0, 0)),
            ("version 2", edit(have.clone(), 0, 2)),
            ("ask 0", edit(have.clone(), 1, 0)),
            ("ask 4", edit(have.clone(), 1, 4)),
            ("piece log2 9", edit(have.clone(), 2, 9)),
            ("piece log2 25", edit(have.clone(), 2, 25)),
            ("size 0", with_size(have.clone(), 0)),
            (
                "more than MAX_PIECES",
                with_size(
                    have.clone(),
                    (u64::from(super::super::MAX_PIECES) + 1) * 1024,
                ),
            ),
            ("LIST of one piece", with_size(encode(&b, Ask::List), 1024)),
            ("PIECE at count", with_index(piece.clone(), 4)),
            ("PIECE at u32::MAX", with_index(piece, u32::MAX)),
        ];
        for (name, bytes) in cases {
            assert_eq!(decode(&bytes), None, "{name}");
        }
    }
}
