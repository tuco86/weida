//! The REQ/REP and SURVEYOR/RESPONDENT tag stack.
//!
//! "Payload of the message is preceded by a stack of 32-bit tags. The most
//! significant bit of each tag is set to 0 except for the very last tag. That
//! allows the algorithm to find out where the tags end and where the message
//! payload begins" [rfc-reqrep §5]. The final tag is the request ID with its
//! high bit set; the tags before it are device-local peer IDs, pushed on the
//! way in and popped on the way back, which is how a reply finds its way to
//! the requester without anybody holding a global address
//! [nanomsg-nng §3, §4].
//!
//! SURVEYOR/RESPONDENT uses the identical construction with a survey ID in the
//! final position [nanomsg-nng §3, §4], so one type serves both. The
//! difference is entirely in what the ID means to the pattern above.
//!
//! **What this module refuses to do.** It does not generate request IDs: they
//! are 31 bits, seeded at random and incremented per requester context
//! [rfc-reqrep §5], which is state a codec has no business holding. A caller
//! that needs one owns the counter.
//!
//! **Where the bound comes from.** The stack has no length field, so a peer
//! could send tags forever; the bound is the local `MAXTTL`, which SP
//! documents as 1-255 with 8 the common default [nanomsg-nng §11] while NNG's
//! current source caps it at 15 [nng-src `core/defs.h`]. This module takes the
//! bound as an argument and ships both numbers as constants rather than
//! choosing between them - see `docs/IMPLEMENTATION.md`.

use crate::error::TagError;

/// Octets in one tag [rfc-reqrep §5].
pub const TAG_LEN: usize = 4;

/// The bit that marks the final tag: set on the request or survey ID, clear on
/// every peer ID before it [rfc-reqrep §5].
pub const TERMINAL: u32 = 0x8000_0000;

/// Largest value a request or survey ID can carry: the low 31 bits
/// [rfc-reqrep §5].
pub const MAX_ID: u32 = 0x7fff_ffff;

/// The default `MAXTTL` of the protocols that forward, and therefore the
/// default depth of a tag stack [nanomsg-nng §4, §11].
pub const DEFAULT_MAX_HOPS: usize = 8;

/// The largest `MAXTTL` the *specification side* documents [nanomsg-nng §11].
pub const SPEC_MAX_HOPS: usize = 255;

/// The largest `MAXTTL` NNG 1.10's source accepts, which is what a real peer
/// enforces [nng-src `core/defs.h`: `NNI_MAX_MAX_TTL`].
pub const NNG_MAX_HOPS: usize = 15;

/// A decoded tag stack: the forwarders' peer IDs, then the request or survey
/// ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Backtrace {
    /// Peer IDs pushed by forwarding devices, outermost first, each with its
    /// high bit clear [rfc-reqrep §5].
    pub peers: Vec<u32>,
    /// The request or survey ID, without its terminal bit. At most
    /// [`MAX_ID`].
    pub id: u32,
}

impl Backtrace {
    /// A stack with no forwarders: what a REQ or SURVEYOR socket sends
    /// directly to its peer.
    pub fn direct(id: u32) -> Backtrace {
        Backtrace {
            peers: Vec::new(),
            id: id & MAX_ID,
        }
    }

    /// Octets this stack occupies on the wire.
    pub fn encoded_len(&self) -> usize {
        (self.peers.len() + 1) * TAG_LEN
    }

    /// Appends the stack to `out`, terminal bit and all.
    ///
    /// A peer ID's high bit is cleared rather than rejected: the bit is the
    /// stack's terminator and belongs to this encoding, not to the caller's
    /// identifier space [rfc-reqrep §5].
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        for peer in &self.peers {
            out.extend_from_slice(&(peer & MAX_ID).to_be_bytes());
        }
        out.extend_from_slice(&((self.id & MAX_ID) | TERMINAL).to_be_bytes());
    }

    /// The stack alone, as octets.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut out);
        out
    }

    /// Builds a whole message body: the stack followed by `payload`.
    pub fn encode_message(&self, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len() + payload.len());
        self.encode_into(&mut out);
        out.extend_from_slice(payload);
        out
    }

    /// Pushes a forwarder's peer ID, as a device does on the way in
    /// [nanomsg-nng §4].
    pub fn push_peer(&mut self, peer: u32) {
        self.peers.push(peer & MAX_ID);
    }

    /// Pops the innermost forwarder's peer ID, as a device does when routing a
    /// reply back [nanomsg-nng §4].
    pub fn pop_peer(&mut self) -> Option<u32> {
        self.peers.pop()
    }
}

/// Where a tag stack ends in a message body, found without collecting its
/// peers.
///
/// What [`measure`] returns: enough for a caller that moves the stack
/// between a body and a header, or pushes and pops one tag, to do it as a
/// split of the same octets rather than a decode and a re-encode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Measured {
    /// Octets the stack occupies, terminator included. The payload starts
    /// here.
    pub len: usize,
    /// Peer IDs before the terminator.
    pub peers: usize,
    /// The request or survey ID, without its terminal bit. At most
    /// [`MAX_ID`].
    pub id: u32,
}

/// Finds the end of the tag stack at the front of `body`.
///
/// The same judgement as [`decode`] - `max_hops` bounds the peer IDs before
/// the terminator, and nothing beyond `(max_hops + 1) * 4` octets is ever
/// read - and the same two errors, but nothing is allocated for either
/// outcome and no tag is copied.
pub fn measure(body: &[u8], max_hops: usize) -> Result<Measured, TagError> {
    let mut offset = 0usize;
    let mut peers = 0usize;
    loop {
        let Some(tag) = body
            .get(offset..)
            .and_then(|rest| rest.get(..TAG_LEN))
            .and_then(|tag| <[u8; TAG_LEN]>::try_from(tag).ok())
        else {
            return Err(TagError::Truncated);
        };
        let value = u32::from_be_bytes(tag);
        offset += TAG_LEN;
        if value & TERMINAL != 0 {
            return Ok(Measured {
                len: offset,
                peers,
                id: value & MAX_ID,
            });
        }
        if peers >= max_hops {
            return Err(TagError::NoTerminator {
                hops: peers,
                max_hops,
            });
        }
        peers += 1;
    }
}

/// Splits a message body into its tag stack and its payload.
///
/// `max_hops` bounds the peer IDs *before* the terminator, which is the local
/// `MAXTTL` [nanomsg-nng §11]; a stack that reaches the bound without one is
/// [`TagError::NoTerminator`] and nothing beyond `(max_hops + 1) * 4` octets
/// is ever read. A body that ends inside a tag is [`TagError::Truncated`],
/// which is the malformed case the RFC says MUST be ignored [rfc-reqrep §5].
///
/// Nothing is allocated for a rejected stack, and the payload is borrowed.
pub fn decode(body: &[u8], max_hops: usize) -> Result<(Backtrace, &[u8]), TagError> {
    let shape = measure(body, max_hops)?;
    let stack = body.get(..shape.len).unwrap_or_default();
    let peers = stack
        .as_chunks::<TAG_LEN>()
        .0
        .iter()
        .take(shape.peers)
        .map(|tag| u32::from_be_bytes(*tag))
        .collect();
    Ok((
        Backtrace {
            peers,
            id: shape.id,
        },
        body.get(shape.len..).unwrap_or_default(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_direct_request_round_trips() {
        let stack = Backtrace::direct(1);
        let body = stack.encode_message(b"ping");
        assert_eq!(body.len(), TAG_LEN + 4);
        let (back, payload) = decode(&body, DEFAULT_MAX_HOPS).expect("decode");
        assert_eq!(back, stack);
        assert_eq!(payload, b"ping");
    }

    #[test]
    fn forwarder_ids_survive_in_order_and_pop_in_reverse() {
        let mut stack = Backtrace::direct(9);
        stack.push_peer(7);
        stack.push_peer(11);
        let body = stack.encode_message(b"x");
        let (mut back, payload) = decode(&body, DEFAULT_MAX_HOPS).expect("decode");
        assert_eq!(back.peers, vec![7, 11]);
        assert_eq!(back.id, 9);
        assert_eq!(payload, b"x");
        // Popping is what a device does to route the reply back
        // [nanomsg-nng §4].
        assert_eq!(back.pop_peer(), Some(11));
        assert_eq!(back.pop_peer(), Some(7));
        assert_eq!(back.pop_peer(), None);
    }

    #[test]
    fn the_terminal_bit_is_the_only_thing_that_ends_the_stack() {
        // Four peer tags and no terminator, with a bound of three: refused
        // after reading exactly the bound, never the whole body.
        let mut body = Vec::new();
        for peer in 0..4u32 {
            body.extend_from_slice(&peer.to_be_bytes());
        }
        body.extend_from_slice(b"payload that must never be reached");
        assert_eq!(
            decode(&body, 3),
            Err(TagError::NoTerminator {
                hops: 3,
                max_hops: 3
            })
        );
    }

    #[test]
    fn a_body_that_ends_inside_a_tag_is_malformed() {
        for short in [&b""[..], &b"\x80"[..], &b"\x80\x00\x00"[..]] {
            assert_eq!(decode(short, DEFAULT_MAX_HOPS), Err(TagError::Truncated));
            assert!(decode(short, DEFAULT_MAX_HOPS).unwrap_err().is_violation());
        }
        // Peer tag complete, request tag cut in half.
        let body = [0, 0, 0, 7, 0x80, 0];
        assert_eq!(decode(&body, DEFAULT_MAX_HOPS), Err(TagError::Truncated));
    }

    #[test]
    fn an_id_is_masked_to_31_bits_in_both_directions() {
        let stack = Backtrace::direct(u32::MAX);
        assert_eq!(stack.id, MAX_ID);
        let body = stack.encode_message(b"");
        assert_eq!(body, [0xFF, 0xFF, 0xFF, 0xFF]);
        let (back, payload) = decode(&body, DEFAULT_MAX_HOPS).expect("decode");
        assert_eq!(back.id, MAX_ID);
        assert!(payload.is_empty());
    }

    #[test]
    fn a_peer_ids_high_bit_is_cleared_rather_than_ending_the_stack() {
        let mut stack = Backtrace::direct(1);
        stack.push_peer(TERMINAL | 5);
        let body = stack.encode_message(b"");
        let (back, _) = decode(&body, DEFAULT_MAX_HOPS).expect("decode");
        assert_eq!(back.peers, vec![5]);
        assert_eq!(back.id, 1);
    }

    #[test]
    fn the_two_hop_ceilings_disagree_and_both_are_published() {
        // The sheet documents 1-255 [nanomsg-nng §11]; NNG's source caps at 15
        // [nng-src]. A decoder that assumed the larger one would accept a
        // stack a real peer rejects, so the caller chooses.
        // observable form: a stack that fits NNG's ceiling is refused under
        // the common default, so the bound is a real parameter and not
        // decoration.
        let mut stack = Backtrace::direct(1);
        for peer in 0..(NNG_MAX_HOPS as u32) {
            stack.push_peer(peer);
        }
        let body = stack.encode_message(b"");
        assert!(decode(&body, NNG_MAX_HOPS).is_ok());
        assert_eq!(
            decode(&body, DEFAULT_MAX_HOPS),
            Err(TagError::NoTerminator {
                hops: DEFAULT_MAX_HOPS,
                max_hops: DEFAULT_MAX_HOPS
            })
        );
    }

    /// Claim: `measure` judges a body exactly as `decode` does and reports
    /// where the payload starts, so a caller can split the stack off by
    /// length instead of decoding and re-encoding it.
    #[test]
    fn measure_agrees_with_decode_on_every_prefix_of_a_stack() {
        let mut stack = Backtrace::direct(0x1234);
        stack.push_peer(7);
        stack.push_peer(11);
        let body = stack.encode_message(b"payload");
        for cut in 0..=body.len() {
            let input = &body[..cut];
            match (
                measure(input, DEFAULT_MAX_HOPS),
                decode(input, DEFAULT_MAX_HOPS),
            ) {
                (Ok(shape), Ok((back, payload))) => {
                    assert_eq!(shape.len, stack.encoded_len());
                    assert_eq!(shape.peers, back.peers.len());
                    assert_eq!(shape.id, back.id);
                    assert_eq!(&input[shape.len..], payload);
                }
                (Err(measured), Err(decoded)) => assert_eq!(measured, decoded),
                (measured, decoded) => panic!("{cut}: {measured:?} against {decoded:?}"),
            }
        }
    }

    /// Claim: only a truncated tag finishes a connection. A stack that is
    /// merely deeper than the local bound is a message NNG's REP drops with
    /// the pipe kept, because devices legitimately forward deep stacks
    /// [nng-src `rep.c`].
    #[test]
    fn a_stack_that_is_merely_too_deep_is_not_a_violation() {
        let too_deep = TagError::NoTerminator {
            hops: DEFAULT_MAX_HOPS,
            max_hops: DEFAULT_MAX_HOPS,
        };
        assert!(!too_deep.is_violation());
        assert!(TagError::Truncated.is_violation());
    }
}
