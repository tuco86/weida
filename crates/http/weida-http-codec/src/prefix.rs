//! Prefixed integers and string literals (RFC 7541 §5.1, §5.2), shared by
//! HPACK and QPACK (RFC 9204 §4.1.1, §4.1.2).

use crate::error::DecodeError;
use crate::huffman;

/// The most continuation octets a decoded integer may use: five carry 35
/// bits, enough for every value up to `u32::MAX` with any prefix.
const MAX_CONTINUATION: usize = 5;

/// Appends `value` with an N-bit prefix; `first` holds the bits above the
/// prefix.
pub(crate) fn encode_int(value: u64, prefix_bits: u8, first: u8, out: &mut Vec<u8>) {
    let max = (1u64 << prefix_bits) - 1;
    if value < max {
        out.push(first | value as u8);
        return;
    }
    out.push(first | max as u8);
    let mut v = value - max;
    while v >= 128 {
        out.push((v % 128) as u8 | 0x80);
        v /= 128;
    }
    out.push(v as u8);
}

/// Decodes an integer with an N-bit prefix. Returns it and the octets used.
///
/// # Errors
///
/// `Incomplete`; `Malformed` `"integer"` for a value above `u32::MAX` or more
/// than five continuation octets. No HPACK or QPACK quantity this crate reads
/// needs more: indices, lengths and table sizes are all far below it.
pub(crate) fn decode_int(input: &[u8], prefix_bits: u8) -> Result<(u64, usize), DecodeError> {
    let first = *input.first().ok_or(DecodeError::Incomplete { needed: 1 })?;
    let max = (1u64 << prefix_bits) - 1;
    let mut value = u64::from(first) & max;
    if value < max {
        return Ok((value, 1));
    }
    let mut shift = 0;
    for i in 1..=MAX_CONTINUATION {
        let byte = *input.get(i).ok_or(DecodeError::Incomplete { needed: 1 })?;
        value += u64::from(byte & 0x7f) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            if value > u64::from(u32::MAX) {
                return Err(DecodeError::Malformed { what: "integer" });
            }
            return Ok((value, i + 1));
        }
    }
    Err(DecodeError::Malformed { what: "integer" })
}

/// Appends a string literal: the length with an N-bit prefix whose `h_mask`
/// bit says Huffman, then the octets. `first` holds the bits above the H
/// flag. Huffman is used when `huffman` is set and the coded form is
/// strictly shorter.
pub(crate) fn encode_string(
    s: &[u8],
    first: u8,
    h_mask: u8,
    prefix_bits: u8,
    huffman: bool,
    out: &mut Vec<u8>,
) {
    let coded = huffman::encoded_len(s);
    if huffman && coded < s.len() {
        encode_int(coded as u64, prefix_bits, first | h_mask, out);
        huffman::encode(s, out);
    } else {
        encode_int(s.len() as u64, prefix_bits, first, out);
        out.extend_from_slice(s);
    }
}

/// Decodes a string literal whose length has an N-bit prefix and whose H flag
/// is `h_mask` in the first octet. Returns the octets and the input used.
///
/// # Errors
///
/// `Incomplete`; `TooLarge` `"string"` when the declared length, or the
/// Huffman-decoded length, exceeds `cap`, checked before anything is
/// reserved; the Huffman refusals.
pub(crate) fn decode_string(
    input: &[u8],
    h_mask: u8,
    prefix_bits: u8,
    cap: usize,
) -> Result<(Vec<u8>, usize), DecodeError> {
    let first = *input.first().ok_or(DecodeError::Incomplete { needed: 1 })?;
    let (len, used) = decode_int(input, prefix_bits)?;
    if len > cap as u64 {
        return Err(DecodeError::TooLarge {
            what: "string",
            declared: len,
            cap: cap as u64,
        });
    }
    let len = len as usize;
    let end = used + len;
    if input.len() < end {
        return Err(DecodeError::Incomplete {
            needed: end - input.len(),
        });
    }
    let raw = &input[used..end];
    if first & h_mask != 0 {
        let mut out = Vec::with_capacity(len);
        huffman::decode(raw, &mut out, cap)?;
        Ok((out, end))
    } else {
        Ok((raw.to_vec(), end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(value: u64, prefix: u8) -> Vec<u8> {
        let mut out = Vec::new();
        encode_int(value, prefix, 0, &mut out);
        out
    }

    #[test]
    fn rfc7541_c1_integers() {
        // C.1.1: 10 with a 5-bit prefix.
        assert_eq!(enc(10, 5), [0x0a]);
        assert_eq!(decode_int(&[0x0a], 5), Ok((10, 1)));
        // C.1.2: 1337 with a 5-bit prefix.
        assert_eq!(enc(1337, 5), [0x1f, 0x9a, 0x0a]);
        assert_eq!(decode_int(&[0x1f, 0x9a, 0x0a], 5), Ok((1337, 3)));
        // C.1.3: 42 starting at an octet boundary.
        assert_eq!(enc(42, 8), [0x2a]);
        assert_eq!(decode_int(&[0x2a], 8), Ok((42, 1)));
    }

    #[test]
    fn integers_are_bounded() {
        assert_eq!(
            decode_int(&[0x1f, 0x80], 5),
            Err(DecodeError::Incomplete { needed: 1 })
        );
        let mut big = enc(u64::from(u32::MAX), 5);
        assert_eq!(decode_int(&big, 5), Ok((u64::from(u32::MAX), big.len())));
        big = enc(u64::from(u32::MAX) + 1, 5);
        assert_eq!(
            decode_int(&big, 5),
            Err(DecodeError::Malformed { what: "integer" })
        );
        assert_eq!(
            decode_int(&[0x1f, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00], 5),
            Err(DecodeError::Malformed { what: "integer" })
        );
        // The top bits of the first octet are not part of the value.
        assert_eq!(decode_int(&[0xea], 5), Ok((10, 1)));
    }

    #[test]
    fn a_declared_length_is_checked_before_reading() {
        // A string declaring 2^31 octets in five octets of input.
        let mut input = Vec::new();
        encode_int(1 << 31, 7, 0, &mut input);
        assert!(matches!(
            decode_string(&input, 0x80, 7, 1024),
            Err(DecodeError::TooLarge { what: "string", declared, .. }) if declared == 1 << 31
        ));
    }

    #[test]
    fn strings_round_trip_both_ways() {
        for huffman in [false, true] {
            let mut out = Vec::new();
            encode_string(b"custom-key", 0, 0x80, 7, huffman, &mut out);
            assert_eq!(out[0] & 0x80 != 0, huffman);
            let (s, used) = decode_string(&out, 0x80, 7, 64).unwrap();
            assert_eq!((s.as_slice(), used), (&b"custom-key"[..], out.len()));
        }
    }
}
