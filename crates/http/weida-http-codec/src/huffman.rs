//! The static Huffman code of HPACK (RFC 7541 §5.2, Appendix B), shared with
//! QPACK (RFC 9204 §4.1.2).
//!
//! The table below is Appendix B transcribed: one `(code, bits)` pair per
//! symbol, 0 to 255 and EOS (256), code right-aligned. Decoding walks a binary
//! tree built from it at compile time by a `const fn`, so there is no
//! hand-written second table to disagree with the first.

use crate::error::DecodeError;

/// `(code, bits)` per symbol; index 256 is EOS.
const TABLE: [(u32, u8); 257] = [
    (0x1ff8, 13),
    (0x7fffd8, 23),
    (0xfffffe2, 28),
    (0xfffffe3, 28),
    (0xfffffe4, 28),
    (0xfffffe5, 28),
    (0xfffffe6, 28),
    (0xfffffe7, 28),
    (0xfffffe8, 28),
    (0xffffea, 24),
    (0x3ffffffc, 30),
    (0xfffffe9, 28),
    (0xfffffea, 28),
    (0x3ffffffd, 30),
    (0xfffffeb, 28),
    (0xfffffec, 28),
    (0xfffffed, 28),
    (0xfffffee, 28),
    (0xfffffef, 28),
    (0xffffff0, 28),
    (0xffffff1, 28),
    (0xffffff2, 28),
    (0x3ffffffe, 30),
    (0xffffff3, 28),
    (0xffffff4, 28),
    (0xffffff5, 28),
    (0xffffff6, 28),
    (0xffffff7, 28),
    (0xffffff8, 28),
    (0xffffff9, 28),
    (0xffffffa, 28),
    (0xffffffb, 28),
    (0x14, 6),
    (0x3f8, 10),
    (0x3f9, 10),
    (0xffa, 12),
    (0x1ff9, 13),
    (0x15, 6),
    (0xf8, 8),
    (0x7fa, 11),
    (0x3fa, 10),
    (0x3fb, 10),
    (0xf9, 8),
    (0x7fb, 11),
    (0xfa, 8),
    (0x16, 6),
    (0x17, 6),
    (0x18, 6),
    (0x0, 5),
    (0x1, 5),
    (0x2, 5),
    (0x19, 6),
    (0x1a, 6),
    (0x1b, 6),
    (0x1c, 6),
    (0x1d, 6),
    (0x1e, 6),
    (0x1f, 6),
    (0x5c, 7),
    (0xfb, 8),
    (0x7ffc, 15),
    (0x20, 6),
    (0xffb, 12),
    (0x3fc, 10),
    (0x1ffa, 13),
    (0x21, 6),
    (0x5d, 7),
    (0x5e, 7),
    (0x5f, 7),
    (0x60, 7),
    (0x61, 7),
    (0x62, 7),
    (0x63, 7),
    (0x64, 7),
    (0x65, 7),
    (0x66, 7),
    (0x67, 7),
    (0x68, 7),
    (0x69, 7),
    (0x6a, 7),
    (0x6b, 7),
    (0x6c, 7),
    (0x6d, 7),
    (0x6e, 7),
    (0x6f, 7),
    (0x70, 7),
    (0x71, 7),
    (0x72, 7),
    (0xfc, 8),
    (0x73, 7),
    (0xfd, 8),
    (0x1ffb, 13),
    (0x7fff0, 19),
    (0x1ffc, 13),
    (0x3ffc, 14),
    (0x22, 6),
    (0x7ffd, 15),
    (0x3, 5),
    (0x23, 6),
    (0x4, 5),
    (0x24, 6),
    (0x5, 5),
    (0x25, 6),
    (0x26, 6),
    (0x27, 6),
    (0x6, 5),
    (0x74, 7),
    (0x75, 7),
    (0x28, 6),
    (0x29, 6),
    (0x2a, 6),
    (0x7, 5),
    (0x2b, 6),
    (0x76, 7),
    (0x2c, 6),
    (0x8, 5),
    (0x9, 5),
    (0x2d, 6),
    (0x77, 7),
    (0x78, 7),
    (0x79, 7),
    (0x7a, 7),
    (0x7b, 7),
    (0x7ffe, 15),
    (0x7fc, 11),
    (0x3ffd, 14),
    (0x1ffd, 13),
    (0xffffffc, 28),
    (0xfffe6, 20),
    (0x3fffd2, 22),
    (0xfffe7, 20),
    (0xfffe8, 20),
    (0x3fffd3, 22),
    (0x3fffd4, 22),
    (0x3fffd5, 22),
    (0x7fffd9, 23),
    (0x3fffd6, 22),
    (0x7fffda, 23),
    (0x7fffdb, 23),
    (0x7fffdc, 23),
    (0x7fffdd, 23),
    (0x7fffde, 23),
    (0xffffeb, 24),
    (0x7fffdf, 23),
    (0xffffec, 24),
    (0xffffed, 24),
    (0x3fffd7, 22),
    (0x7fffe0, 23),
    (0xffffee, 24),
    (0x7fffe1, 23),
    (0x7fffe2, 23),
    (0x7fffe3, 23),
    (0x7fffe4, 23),
    (0x1fffdc, 21),
    (0x3fffd8, 22),
    (0x7fffe5, 23),
    (0x3fffd9, 22),
    (0x7fffe6, 23),
    (0x7fffe7, 23),
    (0xffffef, 24),
    (0x3fffda, 22),
    (0x1fffdd, 21),
    (0xfffe9, 20),
    (0x3fffdb, 22),
    (0x3fffdc, 22),
    (0x7fffe8, 23),
    (0x7fffe9, 23),
    (0x1fffde, 21),
    (0x7fffea, 23),
    (0x3fffdd, 22),
    (0x3fffde, 22),
    (0xfffff0, 24),
    (0x1fffdf, 21),
    (0x3fffdf, 22),
    (0x7fffeb, 23),
    (0x7fffec, 23),
    (0x1fffe0, 21),
    (0x1fffe1, 21),
    (0x3fffe0, 22),
    (0x1fffe2, 21),
    (0x7fffed, 23),
    (0x3fffe1, 22),
    (0x7fffee, 23),
    (0x7fffef, 23),
    (0xfffea, 20),
    (0x3fffe2, 22),
    (0x3fffe3, 22),
    (0x3fffe4, 22),
    (0x7ffff0, 23),
    (0x3fffe5, 22),
    (0x3fffe6, 22),
    (0x7ffff1, 23),
    (0x3ffffe0, 26),
    (0x3ffffe1, 26),
    (0xfffeb, 20),
    (0x7fff1, 19),
    (0x3fffe7, 22),
    (0x7ffff2, 23),
    (0x3fffe8, 22),
    (0x1ffffec, 25),
    (0x3ffffe2, 26),
    (0x3ffffe3, 26),
    (0x3ffffe4, 26),
    (0x7ffffde, 27),
    (0x7ffffdf, 27),
    (0x3ffffe5, 26),
    (0xfffff1, 24),
    (0x1ffffed, 25),
    (0x7fff2, 19),
    (0x1fffe3, 21),
    (0x3ffffe6, 26),
    (0x7ffffe0, 27),
    (0x7ffffe1, 27),
    (0x3ffffe7, 26),
    (0x7ffffe2, 27),
    (0xfffff2, 24),
    (0x1fffe4, 21),
    (0x1fffe5, 21),
    (0x3ffffe8, 26),
    (0x3ffffe9, 26),
    (0xffffffd, 28),
    (0x7ffffe3, 27),
    (0x7ffffe4, 27),
    (0x7ffffe5, 27),
    (0xfffec, 20),
    (0xfffff3, 24),
    (0xfffed, 20),
    (0x1fffe6, 21),
    (0x3fffe9, 22),
    (0x1fffe7, 21),
    (0x1fffe8, 21),
    (0x7ffff3, 23),
    (0x3fffea, 22),
    (0x3fffeb, 22),
    (0x1ffffee, 25),
    (0x1ffffef, 25),
    (0xfffff4, 24),
    (0xfffff5, 24),
    (0x3ffffea, 26),
    (0x7ffff4, 23),
    (0x3ffffeb, 26),
    (0x7ffffe6, 27),
    (0x3ffffec, 26),
    (0x3ffffed, 26),
    (0x7ffffe7, 27),
    (0x7ffffe8, 27),
    (0x7ffffe9, 27),
    (0x7ffffea, 27),
    (0x7ffffeb, 27),
    (0xffffffe, 28),
    (0x7ffffec, 27),
    (0x7ffffed, 27),
    (0x7ffffee, 27),
    (0x7ffffef, 27),
    (0x7fffff0, 27),
    (0x3ffffee, 26),
    (0x3fffffff, 30),
];

const EOS: u16 = 256;
const LEAF: u16 = 0x8000;

/// The decode tree: entry `[node][bit]` is either the next internal node or
/// `LEAF | symbol`. A complete prefix code of 257 symbols has 256 internal
/// nodes; 0 is the root and is never a child, so 0 marks "unset".
static TREE: [[u16; 2]; 256] = build_tree();

const fn build_tree() -> [[u16; 2]; 256] {
    let mut tree = [[0u16; 2]; 256];
    let mut next = 1u16;
    let mut sym = 0;
    while sym < TABLE.len() {
        let (code, bits) = TABLE[sym];
        let mut node = 0usize;
        let mut i = bits;
        while i > 1 {
            i -= 1;
            let bit = ((code >> i) & 1) as usize;
            if tree[node][bit] == 0 {
                tree[node][bit] = next;
                next += 1;
            }
            node = tree[node][bit] as usize;
        }
        tree[node][(code & 1) as usize] = LEAF | sym as u16;
        sym += 1;
    }
    tree
}

/// The length of `s` Huffman-coded, padding included.
#[must_use]
pub fn encoded_len(s: &[u8]) -> usize {
    let bits: usize = s
        .iter()
        .map(|&b| usize::from(TABLE[usize::from(b)].1))
        .sum();
    bits.div_ceil(8)
}

/// Appends `s` Huffman-coded, padded with the most significant bits of EOS
/// (all ones) to an octet boundary.
pub fn encode(s: &[u8], out: &mut Vec<u8>) {
    let mut acc: u64 = 0;
    let mut n: u32 = 0;
    for &b in s {
        let (code, bits) = TABLE[usize::from(b)];
        acc = (acc << bits) | u64::from(code);
        n += u32::from(bits);
        while n >= 8 {
            n -= 8;
            out.push((acc >> n) as u8);
        }
        acc &= (1u64 << n) - 1;
    }
    if n > 0 {
        out.push(((acc << (8 - n)) as u8) | (0xff >> n));
    }
}

/// Appends the decoding of `input` to `out`, refusing to produce more than
/// `cap` octets.
///
/// # Errors
///
/// `Malformed` `"Huffman padding"` for padding longer than 7 bits or not all
/// ones, `"EOS in a Huffman string"` for a decoded EOS (RFC 7541 §5.2);
/// `TooLarge` `"string"` past `cap`.
pub fn decode(input: &[u8], out: &mut Vec<u8>, cap: usize) -> Result<(), DecodeError> {
    let start = out.len();
    let mut node = 0usize;
    let mut pad_bits = 0u32;
    let mut pad_ones = true;
    for &byte in input {
        for shift in (0..8).rev() {
            let bit = usize::from((byte >> shift) & 1);
            let next = TREE[node][bit];
            if next & LEAF != 0 {
                let sym = next & !LEAF;
                if sym == EOS {
                    return Err(DecodeError::Malformed {
                        what: "EOS in a Huffman string",
                    });
                }
                if out.len() - start >= cap {
                    return Err(DecodeError::TooLarge {
                        what: "string",
                        declared: (out.len() - start + 1) as u64,
                        cap: cap as u64,
                    });
                }
                out.push(sym as u8);
                node = 0;
                pad_bits = 0;
                pad_ones = true;
            } else {
                node = usize::from(next);
                pad_bits += 1;
                pad_ones &= bit == 1;
            }
        }
    }
    if pad_bits > 7 || !pad_ones {
        return Err(DecodeError::Malformed {
            what: "Huffman padding",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(s: &[u8]) {
        let mut coded = Vec::new();
        encode(s, &mut coded);
        assert_eq!(coded.len(), encoded_len(s));
        let mut back = Vec::new();
        decode(&coded, &mut back, usize::MAX).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn every_symbol_round_trips() {
        let all: Vec<u8> = (0..=255).collect();
        round_trip(&all);
        for b in 0..=255u8 {
            round_trip(&[b]);
            round_trip(&[b, b, b]);
        }
    }

    #[test]
    fn rfc7541_c4_1_authority() {
        // C.4.1: "www.example.com" is f1e3 c2e5 f23a 6ba0 ab90 f4ff.
        let mut coded = Vec::new();
        encode(b"www.example.com", &mut coded);
        assert_eq!(
            coded,
            [
                0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff
            ]
        );
    }

    #[test]
    fn padding_must_be_short_and_all_ones() {
        let mut out = Vec::new();
        // '0' is 00000 (5 bits): 0x07 pads with 111 -> valid "0".
        decode(&[0x07], &mut out, 8).unwrap();
        assert_eq!(out, b"0");
        // Padding of zeros.
        assert_eq!(
            decode(&[0x00], &mut Vec::new(), 8),
            Err(DecodeError::Malformed {
                what: "Huffman padding"
            })
        );
        // A whole octet of ones is padding longer than 7 bits.
        assert_eq!(
            decode(&[0x07, 0xff], &mut Vec::new(), 8),
            Err(DecodeError::Malformed {
                what: "Huffman padding"
            })
        );
        // EOS itself: thirty ones.
        assert_eq!(
            decode(&[0xff, 0xff, 0xff, 0xff], &mut Vec::new(), 8),
            Err(DecodeError::Malformed {
                what: "EOS in a Huffman string"
            })
        );
    }

    #[test]
    fn the_cap_bounds_the_decoded_length() {
        let mut coded = Vec::new();
        encode(b"aaaa", &mut coded);
        assert!(matches!(
            decode(&coded, &mut Vec::new(), 3),
            Err(DecodeError::TooLarge { what: "string", .. })
        ));
    }
}
