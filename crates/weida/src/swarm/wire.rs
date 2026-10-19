//! The SWARM wire ([decisions/0040](../../../../docs/decisions/0040-swarm-pattern.md) §4.2,
//! [decisions/0041](../../../../docs/decisions/0041-swarm-sessions.md) §4.1).
//!
//! **Version 1, one ask.** One exchange per ask on the request route. The
//! request body, big-endian:
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
//!
//! **Version 2, a session.** One exchange per (fetch, seeder). The request
//! body is an open ([`OPEN_BYTES`]) followed by orders ([`ORDER_BYTES`]); the
//! reply body is one answer head ([`FRAME_HEAD_BYTES`]) plus its payload per
//! order, in the order of the orders. The payload is what version 1 replies.

use super::{Ask, Blob, BlobId, PieceSize};

/// The version of a single ask.
pub(crate) const VERSION_1: u8 = 1;
/// The version of a session open.
pub(crate) const VERSION_2: u8 = 2;
const DECLINE: u8 = 0;
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
    out.push(VERSION_1);
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
    if bytes.len() < SHORT || bytes[0] != VERSION_1 {
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

/// The open that starts a session's request body: `u8` version 2, `u8`
/// piece log2, `[u8; 32]` root, `u64` size, 6 zero bytes.
///
/// The padding makes the open one byte longer than the longest version-1
/// ask ([`MAX_ASK_BYTES`]). A version-1 seeder reads an ask under that cap,
/// so an open overflows it at once and is refused with `REJECTED`, instead of
/// the fetch waiting out the seeder's `header_timeout_ms` for a FIN that a
/// session never sends.
pub(crate) const OPEN_BYTES: usize = 48;

/// One order: `u8` kind (1 LIST, 2 HAVE, 3 PIECE), `u32` index (0 for LIST
/// and HAVE).
pub(crate) const ORDER_BYTES: usize = 5;

/// The head of one answer: `u8` kind (0 DECLINE, 1 LIST, 2 HAVE, 3 PIECE),
/// `u32` index (0 except for PIECE), `u32` payload length (0 for DECLINE).
pub(crate) const FRAME_HEAD_BYTES: usize = 9;

/// Encodes the open of a session about `blob`.
pub(crate) fn encode_open(blob: &Blob) -> [u8; OPEN_BYTES] {
    let mut out = [0; OPEN_BYTES];
    out[0] = VERSION_2;
    out[1] = blob.piece().log2();
    out[2..34].copy_from_slice(blob.root().as_bytes());
    out[34..42].copy_from_slice(&blob.size().to_be_bytes());
    out
}

/// Decodes a session open, refusing another version, non-zero padding, a
/// piece size out of range, an empty blob and a blob [`Blob::new`] refuses.
pub(crate) fn decode_open(bytes: &[u8; OPEN_BYTES]) -> Option<Blob> {
    if bytes[0] != VERSION_2 || bytes[42..].iter().any(|&b| b != 0) {
        return None;
    }
    let piece = PieceSize::from_log2(bytes[1])?;
    let root = BlobId::from_bytes(bytes[2..34].try_into().ok()?);
    let size = u64::from_be_bytes(bytes[34..42].try_into().ok()?);
    if size == 0 {
        return None;
    }
    Blob::new(root, size, piece)
}

/// The kind byte and index of `ask`.
fn kind_and_index(ask: Ask) -> (u8, u32) {
    match ask {
        Ask::List => (LIST, 0),
        Ask::Have => (HAVE, 0),
        Ask::Piece(index) => (PIECE, index),
    }
}

/// Encodes one order.
pub(crate) fn encode_order(ask: Ask) -> [u8; ORDER_BYTES] {
    let (kind, index) = kind_and_index(ask);
    let mut out = [0; ORDER_BYTES];
    out[0] = kind;
    out[1..].copy_from_slice(&index.to_be_bytes());
    out
}

/// Decodes one order about `blob`, refusing an unknown kind, an index on
/// LIST or HAVE, LIST for a blob of at most one piece (it has no list), and
/// PIECE at or past the end.
pub(crate) fn decode_order(blob: &Blob, bytes: &[u8; ORDER_BYTES]) -> Option<Ask> {
    let index = u32::from_be_bytes(bytes[1..].try_into().ok()?);
    match bytes[0] {
        LIST if index == 0 && blob.count() >= 2 => Some(Ask::List),
        HAVE if index == 0 => Some(Ask::Have),
        PIECE if index < blob.count() => Some(Ask::Piece(index)),
        _ => None,
    }
}

/// The head of one answer in a session's reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Head {
    /// The answer to `ask`, followed by `len` payload bytes.
    Answer { ask: Ask, len: u32 },
    /// The seeder declines the blob or a piece; the session ends here.
    Decline,
}

/// Encodes an answer head.
pub(crate) fn encode_head(head: Head) -> [u8; FRAME_HEAD_BYTES] {
    let mut out = [0; FRAME_HEAD_BYTES];
    if let Head::Answer { ask, len } = head {
        let (kind, index) = kind_and_index(ask);
        out[0] = kind;
        out[1..5].copy_from_slice(&index.to_be_bytes());
        out[5..].copy_from_slice(&len.to_be_bytes());
    }
    out
}

/// Decodes an answer head, refusing an unknown kind, an index on DECLINE,
/// LIST or HAVE, and a length on DECLINE. Whether the head answers the order
/// it should is the reader's to check.
pub(crate) fn decode_head(bytes: &[u8; FRAME_HEAD_BYTES]) -> Option<Head> {
    let index = u32::from_be_bytes(bytes[1..5].try_into().ok()?);
    let len = u32::from_be_bytes(bytes[5..].try_into().ok()?);
    let ask = match bytes[0] {
        DECLINE if index == 0 && len == 0 => return Some(Head::Decline),
        LIST if index == 0 => Ask::List,
        HAVE if index == 0 => Ask::Have,
        PIECE => Ask::Piece(index),
        _ => return None,
    };
    Some(Head::Answer { ask, len })
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

    #[test]
    fn every_session_unit_round_trips() {
        let b = blob(3 * 1024 + 5);
        let open = encode_open(&b);
        assert_eq!(open.len(), MAX_ASK_BYTES + 1);
        assert_eq!(decode_open(&open), Some(b));
        for ask in [Ask::List, Ask::Have, Ask::Piece(0), Ask::Piece(3)] {
            assert_eq!(decode_order(&b, &encode_order(ask)), Some(ask), "{ask:?}");
            let head = Head::Answer { ask, len: 1234 };
            assert_eq!(decode_head(&encode_head(head)), Some(head), "{ask:?}");
        }
        assert_eq!(
            decode_head(&encode_head(Head::Decline)),
            Some(Head::Decline)
        );
        assert_eq!(encode_order(Ask::Piece(3)), [PIECE, 0, 0, 0, 3]);
        assert_eq!(
            encode_head(Head::Answer {
                ask: Ask::Piece(2),
                len: 5
            }),
            [PIECE, 0, 0, 0, 2, 0, 0, 0, 5]
        );
    }

    #[test]
    fn every_malformed_session_unit_is_refused() {
        let b = blob(3 * 1024 + 5);
        let open = encode_open(&b);
        let edit_open = |at: usize, value: u8| {
            let mut bytes = open;
            bytes[at] = value;
            bytes
        };
        let with_size = |size: u64| {
            let mut bytes = open;
            bytes[34..42].copy_from_slice(&size.to_be_bytes());
            bytes
        };
        let opens: Vec<(&str, [u8; OPEN_BYTES])> = vec![
            ("version 1", edit_open(0, VERSION_1)),
            ("version 3", edit_open(0, 3)),
            ("padding", edit_open(47, 1)),
            ("piece log2 9", edit_open(1, 9)),
            ("piece log2 25", edit_open(1, 25)),
            ("size 0", with_size(0)),
            (
                "more than MAX_PIECES",
                with_size((u64::from(super::super::MAX_PIECES) + 1) * 1024),
            ),
        ];
        for (name, bytes) in opens {
            assert_eq!(decode_open(&bytes), None, "open: {name}");
        }

        let one_piece = blob(1024);
        let orders: Vec<(&str, Blob, [u8; ORDER_BYTES])> = vec![
            ("kind 0", b, [0, 0, 0, 0, 0]),
            ("kind 4", b, [4, 0, 0, 0, 0]),
            ("LIST with an index", b, [LIST, 0, 0, 0, 1]),
            ("HAVE with an index", b, [HAVE, 0, 0, 0, 1]),
            ("LIST of one piece", one_piece, encode_order(Ask::List)),
            ("PIECE at count", b, encode_order(Ask::Piece(4))),
            ("PIECE at u32::MAX", b, encode_order(Ask::Piece(u32::MAX))),
        ];
        for (name, blob, bytes) in orders {
            assert_eq!(decode_order(&blob, &bytes), None, "order: {name}");
        }

        let heads: Vec<(&str, [u8; FRAME_HEAD_BYTES])> = vec![
            ("kind 4", [4, 0, 0, 0, 0, 0, 0, 0, 0]),
            ("DECLINE with an index", [DECLINE, 0, 0, 0, 1, 0, 0, 0, 0]),
            ("DECLINE with a length", [DECLINE, 0, 0, 0, 0, 0, 0, 0, 1]),
            ("LIST with an index", [LIST, 0, 0, 0, 1, 0, 0, 0, 32]),
            ("HAVE with an index", [HAVE, 0, 0, 0, 1, 0, 0, 0, 1]),
        ];
        for (name, bytes) in heads {
            assert_eq!(decode_head(&bytes), None, "head: {name}");
        }
    }
}
