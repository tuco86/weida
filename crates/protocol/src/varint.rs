//! QUIC variable-length integers (RFC 9000 §16).
//!
//! Used for the `header_len` field of the stream preamble. The two most
//! significant bits of the first byte select the encoding length, so the
//! representable range is `0..=2^62-1`.
//!
//! Encoding always uses the shortest form. Decoding accepts *any* form,
//! including non-minimal ones, exactly as RFC 9000 requires: a peer that pads
//! its length field is unusual but not hostile, and rejecting it would be an
//! interoperability bug rather than a defence.

use std::fmt;

/// Largest value a QUIC varint can carry.
pub const MAX: u64 = (1 << 62) - 1;

/// Longest possible encoding, in bytes.
pub const MAX_ENCODED_LEN: usize = 8;

/// Why a varint could not be encoded or decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VarintError {
    /// The value exceeds `2^62-1` and has no QUIC varint representation.
    OutOfRange,
    /// The input ended in the middle of a varint. Read more bytes and retry;
    /// this is not a protocol violation.
    Truncated,
}

impl fmt::Display for VarintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VarintError::OutOfRange => f.write_str("value exceeds the QUIC varint range"),
            VarintError::Truncated => f.write_str("input ended inside a varint"),
        }
    }
}

impl std::error::Error for VarintError {}

/// Number of bytes [`encode_varint`] will write for `value`.
pub const fn varint_len(value: u64) -> usize {
    if value < 1 << 6 {
        1
    } else if value < 1 << 14 {
        2
    } else if value < 1 << 30 {
        4
    } else {
        8
    }
}

/// Writes `value` into the front of `out` in its shortest QUIC varint form,
/// and reports how many bytes it took.
///
/// The `Vec`-appending form below delegates to this one. A caller with a fixed
/// buffer — a send path building a preamble it will right-align against an
/// already-encoded header — allocates nothing.
///
/// # Errors
///
/// [`VarintError::OutOfRange`] above 2^62 − 1, and when `out` is shorter than
/// the form `value` needs.
pub fn write_varint(value: u64, out: &mut [u8]) -> Result<usize, VarintError> {
    if value > MAX {
        return Err(VarintError::OutOfRange);
    }
    let len = varint_len(value);
    if out.len() < len {
        return Err(VarintError::OutOfRange);
    }
    match len {
        1 => out[0] = value as u8,
        2 => out[..2].copy_from_slice(&((value as u16) | 0x4000).to_be_bytes()),
        4 => out[..4].copy_from_slice(&((value as u32) | 0x8000_0000).to_be_bytes()),
        _ => out[..8].copy_from_slice(&(value | 0xc000_0000_0000_0000).to_be_bytes()),
    }
    Ok(len)
}

/// Appends `value` to `out` in its shortest QUIC varint form.
///
/// # Errors
///
/// [`VarintError::OutOfRange`] above 2^62 − 1.
pub fn encode_varint(value: u64, out: &mut Vec<u8>) -> Result<(), VarintError> {
    let mut bytes = [0u8; 8];
    let len = write_varint(value, &mut bytes)?;
    out.extend_from_slice(&bytes[..len]);
    Ok(())
}

/// Decodes a varint from the front of `input`.
///
/// Returns the value and the number of bytes consumed.
pub fn decode_varint(input: &[u8]) -> Result<(u64, usize), VarintError> {
    let first = *input.first().ok_or(VarintError::Truncated)?;
    let len = 1usize << (first >> 6);
    if input.len() < len {
        return Err(VarintError::Truncated);
    }
    let mut value = u64::from(first & 0x3f);
    for b in &input[1..len] {
        value = (value << 8) | u64::from(*b);
    }
    Ok((value, len))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint(v, &mut out).unwrap();
        out
    }

    #[test]
    fn rfc_9000_appendix_a_vectors() {
        // The worked examples from RFC 9000 §A.1.
        assert_eq!(
            decode_varint(&[0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c]).unwrap(),
            (151_288_809_941_952_652, 8)
        );
        assert_eq!(
            decode_varint(&[0x9d, 0x7f, 0x3e, 0x7d]).unwrap(),
            (494_878_333, 4)
        );
        assert_eq!(decode_varint(&[0x7b, 0xbd]).unwrap(), (15_293, 2));
        assert_eq!(decode_varint(&[0x25]).unwrap(), (37, 1));
        // Non-minimal encoding of 37 in two bytes must still decode.
        assert_eq!(decode_varint(&[0x40, 0x25]).unwrap(), (37, 2));
    }

    #[test]
    fn encoding_is_minimal_at_every_boundary() {
        let cases = [
            (0u64, 1usize),
            (63, 1),
            (64, 2),
            (16_383, 2),
            (16_384, 4),
            (1_073_741_823, 4),
            (1_073_741_824, 8),
            (MAX, 8),
        ];
        for (value, len) in cases {
            assert_eq!(varint_len(value), len, "len of {value}");
            assert_eq!(enc(value).len(), len, "encoded len of {value}");
            assert_eq!(decode_varint(&enc(value)).unwrap(), (value, len));
        }
    }

    #[test]
    fn header_len_of_16384_is_the_four_byte_form() {
        assert_eq!(enc(16_384), vec![0x80, 0x00, 0x40, 0x00]);
    }

    #[test]
    fn out_of_range_is_rejected() {
        let mut out = Vec::new();
        assert_eq!(
            encode_varint(MAX + 1, &mut out),
            Err(VarintError::OutOfRange)
        );
        assert_eq!(
            encode_varint(u64::MAX, &mut out),
            Err(VarintError::OutOfRange)
        );
        assert!(out.is_empty(), "nothing is written on rejection");
    }

    #[test]
    fn truncated_input_is_distinguishable_from_corruption() {
        assert_eq!(decode_varint(&[]), Err(VarintError::Truncated));
        assert_eq!(decode_varint(&[0x40]), Err(VarintError::Truncated));
        assert_eq!(decode_varint(&[0x80, 0x00]), Err(VarintError::Truncated));
        assert_eq!(decode_varint(&[0xc0; 7]), Err(VarintError::Truncated));
        assert!(decode_varint(&[0xc0; 8]).is_ok());
    }

    #[test]
    fn trailing_bytes_are_left_for_the_caller() {
        let (v, n) = decode_varint(&[0x25, 0xff, 0xff]).unwrap();
        assert_eq!((v, n), (37, 1));
    }

    #[test]
    fn roundtrip_over_a_wide_spread_of_values() {
        let mut x = 0x2545_f491_4f6c_dd1du64;
        for _ in 0..2000 {
            // xorshift64: deterministic, no dependency.
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let v = x & MAX;
            assert_eq!(decode_varint(&enc(v)).unwrap().0, v);
        }
    }
}
