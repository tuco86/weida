//! Framing: a flags octet, a size field of one or eight octets, and a body.
//!
//! ```text
//! command = ( %x04 short-size | %x06 long-size ) command-body
//! message = *message-more message-last
//! message-more = ( %x01 short-size | %x03 long-size ) message-body
//! message-last = ( %x00 short-size | %x02 long-size ) message-body
//! short-size = OCTET          ; body is 0 to 255 octets
//! long-size = 8OCTET          ; body is 0 to 2^63-1 octets
//! ```
//!
//! "The size does not include the flags field, nor itself, so an empty frame
//! has a size of zero."
//!
//! This is the module where hostile bytes are first believed, so it is where
//! the cap lives. A long frame may declare 2^63-1 octets and ZMTP grants no
//! credit and offers no defence except a local limit - `ZMQ_MAXMSGSIZE`,
//! default unlimited. Every decode therefore takes the cap as an argument and
//! rejects an over-large declaration **before** the body is looked at, let
//! alone reserved, which is the same rule weida's own `max_header_bytes`
//! obeys.

use crate::error::FrameError;

/// COMMAND, bit 2: the frame carries a command rather than a message.
pub const COMMAND: u8 = 0x04;
/// LONG, bit 1: the size field is eight octets, network order.
pub const LONG: u8 = 0x02;
/// MORE, bit 0: more frames follow in this message.
pub const MORE: u8 = 0x01;
/// Bits 7-3, reserved and required to be zero.
pub const RESERVED: u8 = 0xF8;

/// Largest body a short size field can describe.
pub const MAX_SHORT_BODY: u64 = 255;

/// Largest body the grammar allows: `long-size` is 8 octets, but the
/// specification bounds a long body at 2^63-1 rather than 2^64-1.
pub const MAX_BODY: u64 = i64::MAX as u64;

/// What a frame carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameKind {
    /// A message frame. `more` is the MORE flag: part of a multipart message
    /// rather than the last frame of one.
    Message {
        /// More frames follow in this message.
        more: bool,
    },
    /// A command frame. MORE "SHALL be zero on command frames", so this
    /// variant cannot carry it: the illegal combination has no
    /// representation, in either direction.
    Command,
}

/// A decoded frame header: what the frame is, and how long its body is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    /// Command or message, and for a message whether more frames follow.
    pub kind: FrameKind,
    /// Declared body length, already checked against the caller's cap.
    pub len: u64,
}

impl FrameHeader {
    /// The flags octet this header encodes to.
    ///
    /// LONG is derived from the length rather than stored: the encoder always
    /// picks the shortest form, which is what the specification recommends
    /// ("if the body is 0-255 octets, the command SHOULD use a short size
    /// field"). The decoder accepts either form for any length, because a
    /// peer that sends a long size for a small body is odd, not wrong.
    pub const fn flags(&self) -> u8 {
        let mut flags = match self.kind {
            FrameKind::Command => COMMAND,
            FrameKind::Message { more: true } => MORE,
            FrameKind::Message { more: false } => 0,
        };
        if self.len > MAX_SHORT_BODY {
            flags |= LONG;
        }
        flags
    }
}

/// Decodes a frame header from the front of `input`.
///
/// Returns the header and how many octets it occupied. `max_body_bytes` is the
/// local cap, and a larger declaration is rejected here - the only place the
/// check can happen before memory is committed. The effective cap is the
/// smaller of `max_body_bytes` and [`MAX_BODY`], so a caller cannot switch the
/// check off and accept an ungrammatical 2^64-1 frame by passing `u64::MAX`.
pub fn decode_header(
    input: &[u8],
    max_body_bytes: u64,
) -> Result<(FrameHeader, usize), FrameError> {
    let Some(&flags) = input.first() else {
        return Err(FrameError::Incomplete);
    };
    if flags & RESERVED != 0 {
        return Err(FrameError::ReservedFlags(flags));
    }
    let command = flags & COMMAND != 0;
    let more = flags & MORE != 0;
    if command && more {
        return Err(FrameError::MoreOnCommand);
    }

    let (len, used) = if flags & LONG != 0 {
        let Some(field) = input.get(1..9) else {
            return Err(FrameError::Incomplete);
        };
        let mut octets = [0u8; 8];
        octets.copy_from_slice(field);
        (u64::from_be_bytes(octets), 9)
    } else {
        let Some(&short) = input.get(1) else {
            return Err(FrameError::Incomplete);
        };
        (u64::from(short), 2)
    };

    let max = max_body_bytes.min(MAX_BODY);
    if len > max {
        return Err(FrameError::BodyTooLarge { len, max });
    }

    let kind = if command {
        FrameKind::Command
    } else {
        FrameKind::Message { more }
    };
    Ok((FrameHeader { kind, len }, used))
}

/// Decodes a whole frame, borrowing its body.
///
/// Returns the header, the body and the total octets consumed. Nothing is
/// copied and nothing is allocated: the body is a slice of the caller's
/// buffer, which is what lets a bridge hand a payload straight to a weida
/// transfer without a second copy.
pub fn decode(
    input: &[u8],
    max_body_bytes: u64,
) -> Result<(FrameHeader, &[u8], usize), FrameError> {
    let (header, used) = decode_header(input, max_body_bytes)?;
    // The cap has already bounded `len`; this narrowing can only fail on a
    // 32-bit target, where a body that large could never be held anyway.
    let len = usize::try_from(header.len).map_err(|_| FrameError::BodyTooLarge {
        len: header.len,
        max: usize::MAX as u64,
    })?;
    // `used` is 2 or 9 and `len` is already capped, so the sum cannot overflow
    // on a 64-bit target. A 32-bit target can overflow it, and a body that
    // large could not be held there anyway, so it is refused as too large.
    let end = used.checked_add(len).ok_or(FrameError::BodyTooLarge {
        len: header.len,
        max: usize::MAX as u64,
    })?;
    let body = input.get(used..end).ok_or(FrameError::Incomplete)?;
    Ok((header, body, end))
}

/// Appends a frame header for a body of `len` octets.
pub fn encode_header(kind: FrameKind, len: u64, out: &mut Vec<u8>) {
    let header = FrameHeader { kind, len };
    out.push(header.flags());
    if len > MAX_SHORT_BODY {
        out.extend_from_slice(&len.to_be_bytes());
    } else {
        out.push(len as u8);
    }
}

/// Builds a complete frame: header followed by `body`.
pub fn encode(kind: FrameKind, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + body.len());
    encode_header(kind, body.len() as u64, &mut out);
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_message_frame_round_trips() {
        let frame = encode(FrameKind::Message { more: false }, b"hello");
        assert_eq!(frame, b"\x00\x05hello");
        let (header, body, used) = decode(&frame, 1024).expect("decode");
        assert_eq!(header.kind, FrameKind::Message { more: false });
        assert_eq!(header.len, 5);
        assert_eq!(body, b"hello");
        assert_eq!(used, frame.len());
    }

    #[test]
    fn an_empty_frame_has_a_size_of_zero() {
        let frame = encode(FrameKind::Message { more: false }, b"");
        assert_eq!(frame, b"\x00\x00");
        let (header, body, used) = decode(&frame, 1024).expect("decode");
        assert_eq!(header.len, 0);
        assert!(body.is_empty());
        assert_eq!(used, 2);
    }

    #[test]
    fn the_short_to_long_boundary_is_at_255_octets() {
        let short = vec![0xAB; 255];
        let frame = encode(FrameKind::Message { more: false }, &short);
        assert_eq!(
            &frame[..2],
            b"\x00\xFF",
            "255 octets still fit a short size"
        );
        assert_eq!(frame.len(), 2 + 255);

        let long = vec![0xAB; 256];
        let frame = encode(FrameKind::Message { more: false }, &long);
        assert_eq!(
            &frame[..9],
            &[0x02, 0, 0, 0, 0, 0, 0, 1, 0],
            "256 octets need a long size, network order"
        );
        assert_eq!(frame.len(), 9 + 256);
        let (header, body, used) = decode(&frame, 4096).expect("decode");
        assert_eq!(header.len, 256);
        assert_eq!(body.len(), 256);
        assert_eq!(used, frame.len());
    }

    #[test]
    fn a_long_size_for_a_small_body_is_accepted() {
        // Non-canonical but legal: the encoder never produces this, and a peer
        // that does is not violating anything.
        let frame = [0x02, 0, 0, 0, 0, 0, 0, 0, 1, b'x'];
        let (header, body, used) = decode(&frame, 1024).expect("decode");
        assert_eq!(header.len, 1);
        assert_eq!(body, b"x");
        assert_eq!(used, 10);
    }

    #[test]
    fn the_more_flag_marks_every_frame_but_the_last() {
        let first = encode(FrameKind::Message { more: true }, b"A");
        let last = encode(FrameKind::Message { more: false }, b"B");
        assert_eq!(first, b"\x01\x01A");
        assert_eq!(last, b"\x00\x01B");

        let mut stream = first.clone();
        stream.extend_from_slice(&last);
        let (h1, b1, used) = decode(&stream, 1024).expect("first");
        assert_eq!(h1.kind, FrameKind::Message { more: true });
        assert_eq!(b1, b"A");
        let (h2, b2, _) = decode(&stream[used..], 1024).expect("second");
        assert_eq!(h2.kind, FrameKind::Message { more: false });
        assert_eq!(b2, b"B");
    }

    #[test]
    fn a_command_frame_sets_bit_2_and_never_more() {
        let frame = encode(FrameKind::Command, b"\x04PING");
        assert_eq!(frame, b"\x04\x05\x04PING");
        let (header, body, _) = decode(&frame, 1024).expect("decode");
        assert_eq!(header.kind, FrameKind::Command);
        assert_eq!(body, b"\x04PING");

        // MORE on a command frame is a violation, not a flag to ignore.
        assert_eq!(decode(b"\x05\x00", 1024), Err(FrameError::MoreOnCommand));
    }

    #[test]
    fn a_long_command_frame_uses_flags_0x06() {
        let body = vec![b'x'; 300];
        let frame = encode(FrameKind::Command, &body);
        assert_eq!(frame[0], COMMAND | LONG);
        assert_eq!(&frame[1..9], &[0, 0, 0, 0, 0, 0, 1, 44]);
    }

    #[test]
    fn reserved_flag_bits_are_a_violation() {
        for bit in 3..8 {
            let flags = 1u8 << bit;
            assert_eq!(
                decode(&[flags, 0], 1024),
                Err(FrameError::ReservedFlags(flags)),
                "bit {bit}"
            );
        }
    }

    #[test]
    fn an_over_large_declaration_is_refused_before_the_body() {
        // The whole point: 2^63-1 octets declared, ten octets present, no
        // allocation attempted.
        let mut frame = vec![0x02];
        frame.extend_from_slice(&MAX_BODY.to_be_bytes());
        assert_eq!(
            decode(&frame, 8 * 1024 * 1024),
            Err(FrameError::BodyTooLarge {
                len: MAX_BODY,
                max: 8 * 1024 * 1024
            })
        );
        assert!(FrameError::BodyTooLarge { len: 1, max: 0 }.is_violation());
    }

    #[test]
    fn the_grammar_bound_holds_even_with_no_local_cap() {
        // A caller that passes u64::MAX still cannot accept a frame outside
        // the grammar: the effective cap is the smaller of the two.
        let mut frame = vec![0x02];
        frame.extend_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode(&frame, u64::MAX),
            Err(FrameError::BodyTooLarge {
                len: u64::MAX,
                max: MAX_BODY
            })
        );
    }

    #[test]
    fn every_prefix_of_a_frame_asks_for_more() {
        let frame = encode(FrameKind::Message { more: false }, &vec![7u8; 300]);
        for n in 0..frame.len() {
            assert_eq!(
                decode(&frame[..n], 4096),
                Err(FrameError::Incomplete),
                "{n} octets"
            );
        }
        assert!(!FrameError::Incomplete.is_violation());
        assert!(decode(&frame, 4096).is_ok());
    }

    #[test]
    fn a_header_decodes_without_its_body_being_present() {
        // What a reader needs to size its next read: the header alone parses,
        // and says how many octets to wait for.
        let (header, used) = decode_header(b"\x00\xFF", 4096).expect("header");
        assert_eq!((header.len, used), (255, 2));
        assert_eq!(decode(b"\x00\xFF", 4096), Err(FrameError::Incomplete));
    }
}
