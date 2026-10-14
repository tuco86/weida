//! Z85: the printable binary encoding of [32/Z85], which is how CURVE keys
//! are written down.
//!
//! Four octets become five characters out of an 85-character alphabet, so a
//! 32-octet key is 40 characters. The encoding is a plain base-85 of each
//! four-octet group read in network order - no padding, no line breaks, and
//! no partial groups: "the binary frame SHALL have a length divisible by 4"
//! and a text frame's length is therefore divisible by 5.
//!
//! [32/Z85]: https://rfc.zeromq.org/spec/32/
//!
//! This module is here rather than beside the key handling in `weida-zmq`
//! because Z85 is octets, which is what this crate is for, and because both
//! ends of a key's journey need it: a key arrives from a configuration file as
//! text and goes onto the wire as 32 binary octets.
//!
//! ```
//! use weida_zmtp::z85;
//!
//! // The test vector of 32/Z85.
//! assert_eq!(z85::encode(&[0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B]).unwrap(), "HelloWorld");
//! assert_eq!(z85::decode("HelloWorld").unwrap(), vec![0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B]);
//! ```

use crate::curve::KEY_LEN;
use crate::error::Z85Error;

/// The alphabet of 32/Z85, in digit order: value `n` is `ALPHABET[n]`.
///
/// It is the RFC's own table, which deliberately avoids the quote and
/// backslash characters so that a key can be pasted into source code and
/// shell arguments of any language without escaping.
pub const ALPHABET: &[u8; 85] =
    b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

/// A CURVE key as text: 32 octets divided by 4, times 5.
pub const KEY_TEXT_LEN: usize = KEY_LEN / 4 * 5;

/// Not a digit of the alphabet.
const NONE: u8 = 0xFF;

/// The reverse of [`ALPHABET`], built at compile time so the two cannot
/// drift. Indexed by the octet itself; anything above 127 is not a digit by
/// construction, the alphabet being ASCII.
const DIGITS: [u8; 128] = {
    let mut table = [NONE; 128];
    let mut value = 0;
    while value < ALPHABET.len() {
        table[ALPHABET[value] as usize] = value as u8;
        value += 1;
    }
    table
};

/// Encodes octets as Z85 text.
///
/// # Errors
///
/// [`Z85Error::BinaryLength`] if the input's length is not a multiple of
/// four, which the encoding has no way to express.
pub fn encode(data: &[u8]) -> Result<String, Z85Error> {
    let (groups, rest) = data.as_chunks::<4>();
    if !rest.is_empty() {
        return Err(Z85Error::BinaryLength(data.len()));
    }
    let mut text = String::with_capacity(groups.len() * 5);
    for group in groups {
        let mut value = u32::from_be_bytes(*group);
        let mut digits = [0u8; 5];
        let mut position = digits.len();
        while position > 0 {
            position -= 1;
            digits[position] = ALPHABET[(value % 85) as usize];
            value /= 85;
        }
        text.push_str(std::str::from_utf8(&digits).expect("the alphabet is ASCII"));
    }
    Ok(text)
}

/// Decodes Z85 text back to octets.
///
/// # Errors
///
/// [`Z85Error::TextLength`] if the length is not a multiple of five,
/// [`Z85Error::NotZ85`] for a character outside the alphabet, and
/// [`Z85Error::Overflow`] for a group whose base-85 value exceeds four
/// octets. The last one is real rather than theoretical: `#####` is five
/// legal characters whose value is 85^5 - 1, well above `u32::MAX`, so a
/// decoder that only checked the alphabet would wrap or panic on text a
/// human could type.
pub fn decode(text: &str) -> Result<Vec<u8>, Z85Error> {
    let (groups, rest) = text.as_bytes().as_chunks::<5>();
    if !rest.is_empty() {
        return Err(Z85Error::TextLength(text.len()));
    }
    let mut data = Vec::with_capacity(groups.len() * 4);
    for group in groups {
        let mut value: u32 = 0;
        for &character in group {
            let digit = DIGITS
                .get(usize::from(character))
                .copied()
                .filter(|&digit| digit != NONE)
                .ok_or(Z85Error::NotZ85(character))?;
            value = value
                .checked_mul(85)
                .and_then(|value| value.checked_add(u32::from(digit)))
                .ok_or(Z85Error::Overflow)?;
        }
        data.extend_from_slice(&value.to_be_bytes());
    }
    Ok(data)
}

/// Encodes a CURVE key as the 40 characters libzmq's `zmq_z85_encode` and
/// `zmq_curve_keypair` produce.
pub fn encode_key(key: &[u8; KEY_LEN]) -> String {
    encode(key).expect("32 is a multiple of 4")
}

/// Decodes a 40-character CURVE key.
///
/// # Errors
///
/// [`Z85Error::TextLength`] unless the text is exactly [`KEY_TEXT_LEN`]
/// characters; the alphabet and overflow errors of [`decode`] otherwise.
pub fn decode_key(text: &str) -> Result<[u8; KEY_LEN], Z85Error> {
    if text.len() != KEY_TEXT_LEN {
        return Err(Z85Error::TextLength(text.len()));
    }
    let data = decode(text)?;
    Ok(data.try_into().expect("40 characters are 32 octets"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rfc_test_vector_goes_both_ways() {
        const BINARY: [u8; 8] = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
        const TEXT: &str = "HelloWorld";

        assert_eq!(encode(&BINARY).expect("encode"), TEXT);
        assert_eq!(decode(TEXT).expect("decode"), BINARY);
    }

    #[test]
    fn every_digit_round_trips() {
        // One group per digit position catches a transposed alphabet, which a
        // single vector cannot.
        for (value, &character) in ALPHABET.iter().enumerate() {
            let text = encode(&(value as u32).to_be_bytes()).expect("encode");
            assert_eq!(text.as_bytes()[4], character, "value {value}");
            assert_eq!(decode(&text).expect("decode"), (value as u32).to_be_bytes());
        }
    }

    #[test]
    fn lengths_that_the_encoding_cannot_express_are_refused() {
        assert_eq!(encode(&[0; 3]), Err(Z85Error::BinaryLength(3)));
        assert_eq!(encode(&[0; 5]), Err(Z85Error::BinaryLength(5)));
        assert_eq!(decode("abcd"), Err(Z85Error::TextLength(4)));
        assert_eq!(decode("abcdef"), Err(Z85Error::TextLength(6)));
        // Empty is a multiple of both and encodes to nothing.
        assert_eq!(encode(&[]).expect("encode"), "");
        assert_eq!(decode("").expect("decode"), Vec::<u8>::new());
    }

    #[test]
    fn characters_outside_the_alphabet_are_refused() {
        assert_eq!(decode("Hello\"orld"), Err(Z85Error::NotZ85(b'"')));
        assert_eq!(decode("Hello\\orld"), Err(Z85Error::NotZ85(b'\\')));
        // Non-ASCII is two octets of UTF-8, neither of them a digit: eight
        // characters, ten octets, so the length rule lets it through to the
        // alphabet rule.
        assert_eq!(decode("Hell\u{e4}orld"), Err(Z85Error::NotZ85(0xC3)));
    }

    #[test]
    fn a_group_above_four_octets_is_refused_rather_than_wrapped() {
        // 85^5 - 1 = 4437053124 > u32::MAX = 4294967295.
        assert_eq!(decode("#####"), Err(Z85Error::Overflow));
        // The largest group that does fit: u32::MAX.
        let max = decode("%nSc0").expect("decode");
        assert_eq!(max, u32::MAX.to_be_bytes());
        assert_eq!(encode(&max).expect("encode"), "%nSc0");
    }

    #[test]
    fn a_key_is_forty_characters_both_ways() {
        let key = [0xC1; KEY_LEN];
        let text = encode_key(&key);
        assert_eq!(text.len(), KEY_TEXT_LEN);
        assert_eq!(decode_key(&text).expect("decode"), key);
        assert_eq!(decode_key("tooshort"), Err(Z85Error::TextLength(8)));
    }
}
