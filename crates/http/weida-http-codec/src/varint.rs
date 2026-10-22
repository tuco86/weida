//! QUIC variable-length integers (RFC 9000 §16), the integer of every HTTP/3
//! frame and stream header.
//!
//! The two most significant bits of the first octet give the length (1, 2, 4
//! or 8 octets); the rest is the value, network byte order, up to 2^62 - 1.

use crate::error::{DecodeError, EncodeError};

/// The largest value: 2^62 - 1.
pub const MAX: u64 = (1 << 62) - 1;

/// Decodes one integer. Returns it and the octets used. A value written in a
/// longer form than it needs is accepted, as RFC 9000 §16 allows.
///
/// # Errors
///
/// `Incomplete` with the exact number of missing octets.
pub fn decode(input: &[u8]) -> Result<(u64, usize), DecodeError> {
    let first = *input.first().ok_or(DecodeError::Incomplete { needed: 1 })?;
    let len = 1usize << (first >> 6);
    if input.len() < len {
        return Err(DecodeError::Incomplete {
            needed: len - input.len(),
        });
    }
    let value = input[1..len]
        .iter()
        .fold(u64::from(first & 0x3f), |acc, &b| (acc << 8) | u64::from(b));
    Ok((value, len))
}

/// The length of the shortest encoding of `v`; 8 for anything above
/// [`MAX`], which [`encode`] refuses.
#[must_use]
pub const fn len(v: u64) -> usize {
    if v < 1 << 6 {
        1
    } else if v < 1 << 14 {
        2
    } else if v < 1 << 30 {
        4
    } else {
        8
    }
}

/// Appends the shortest encoding of `v`.
///
/// # Errors
///
/// `TooLarge` `"varint"` above [`MAX`]: a caller bug, since every value this
/// crate's users encode is a length or an identifier far below it. Nothing is
/// written on error.
pub fn encode(v: u64, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    if v > MAX {
        return Err(EncodeError::TooLarge {
            what: "varint",
            len: v,
            cap: MAX,
        });
    }
    let n = len(v);
    let tag = match n {
        1 => 0x00,
        2 => 0x40,
        4 => 0x80,
        _ => 0xc0,
    };
    let bytes = v.to_be_bytes();
    let start = out.len();
    out.extend_from_slice(&bytes[8 - n..]);
    out[start] |= tag;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundaries_take_the_next_length() {
        for (v, n) in [
            (63, 1),
            (64, 2),
            (16_383, 2),
            (16_384, 4),
            ((1 << 30) - 1, 4),
            (1 << 30, 8),
            (MAX, 8),
        ] {
            let mut out = Vec::new();
            encode(v, &mut out).unwrap();
            assert_eq!(out.len(), n, "{v}");
            assert_eq!(decode(&out), Ok((v, n)));
        }
        let mut out = vec![7];
        assert!(encode(MAX + 1, &mut out).is_err());
        assert_eq!(out, [7]);
    }

    #[test]
    fn a_short_input_names_the_missing_octets() {
        assert_eq!(
            decode(&[0xc0, 0, 0]),
            Err(DecodeError::Incomplete { needed: 5 })
        );
        assert_eq!(decode(&[]), Err(DecodeError::Incomplete { needed: 1 }));
    }
}
