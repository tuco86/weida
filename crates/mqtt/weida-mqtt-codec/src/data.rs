//! The five data representations of chapter 1.5, and a cursor over them.
//!
//! | Representation | Wire form | Bound |
//! | --- | --- | --- |
//! | Byte | one octet | — |
//! | Two Byte Integer | big-endian `u16` | — |
//! | Four Byte Integer | big-endian `u32` | — |
//! | Variable Byte Integer | [`crate::varint`] | 268,435,455 |
//! | UTF-8 Encoded String | `u16` length, then that many bytes | 65,535 bytes |
//! | Binary Data | `u16` length, then that many bytes | 65,535 bytes |
//!
//! The two length-prefixed forms are capped by their prefix rather than by
//! policy: "the maximum size of a UTF-8 Encoded String is 65,535 bytes"
//! (1.5.4) and the same for Binary Data (1.5.6) [mqtt5 §3], and a Topic Name
//! or Filter "MUST NOT encode to more than 65,535 bytes" ([MQTT-4.7.3-3])
//! [mqtt5 §3] for exactly that reason. So the bound needs no configuration and
//! gets none; what *is* configurable, and lives on the packet decoders rather
//! than here, is the maximum packet size.
//!
//! **Decoding borrows.** A [`Reader`] hands back `&'a str` and `&'a [u8]`
//! slices of the caller's buffer: a decoded packet allocates nothing at all,
//! which is what lets a client hand a PUBLISH payload straight on without a
//! second copy. The reader also never reads past its input, so
//! [`Reader::string`] on a truncated buffer is [`DecodeError::Incomplete`] and
//! not a panic.
//!
//! # String validation, and one deliberate reading of a SHOULD
//!
//! Two rules are MUST and are enforced: the bytes are well-formed UTF-8
//! ([MQTT-1.5.4-1]) and they do not include U+0000 ([MQTT-1.5.4-2])
//! [mqtt5 §3]. Rust's `str::from_utf8` gives the first, including the
//! rejection of surrogate halves and over-long forms.
//!
//! The Disallowed Unicode code points — U+0001..U+001F, U+007F..U+009F and the
//! non-characters — are a SHOULD NOT, and this codec **accepts** them. That is
//! a decision rather than an omission: 5.4.9.2 documents a publisher-driven
//! denial of service against a *subscriber* that is strict where its broker is
//! lax, where at QoS 1 or 2 the message is redelivered and the subscriber
//! disconnects again, and the remedies it offers are to fix the broker or to
//! make the subscriber tolerant [mqtt5 §10]. A client library is the
//! subscriber, so it is tolerant. U+FEFF is likewise kept rather than
//! stripped, which [MQTT-1.5.4-3] requires outright.

use crate::error::{DecodeError, EncodeError};
use crate::varint;

/// The largest UTF-8 Encoded String or Binary Data field, from its two-byte
/// length prefix (1.5.4, 1.5.6) [mqtt5 §3].
pub const MAX_FIELD_LEN: usize = u16::MAX as usize;

/// A borrowing cursor over one packet's bytes.
///
/// Every read is bounds-checked and every failure past the end is
/// [`DecodeError::Incomplete`], so a partially received packet is never
/// mistaken for a malformed one.
#[derive(Clone, Copy, Debug)]
pub struct Reader<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// A reader positioned at the start of `input`.
    #[must_use]
    pub const fn new(input: &'a [u8]) -> Self {
        Reader { input, pos: 0 }
    }

    /// Bytes consumed so far.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.pos
    }

    /// Bytes not yet consumed.
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.input.len() - self.pos
    }

    /// Whether everything has been consumed.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// The bytes not yet consumed, without consuming them.
    #[must_use]
    pub fn peek_rest(&self) -> &'a [u8] {
        &self.input[self.pos..]
    }

    /// Consumes and returns the rest of the input.
    pub fn rest(&mut self) -> &'a [u8] {
        let rest = &self.input[self.pos..];
        self.pos = self.input.len();
        rest
    }

    /// Consumes exactly `len` bytes.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] when fewer than `len` bytes remain.
    pub fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(len).ok_or(DecodeError::Incomplete)?;
        let slice = self
            .input
            .get(self.pos..end)
            .ok_or(DecodeError::Incomplete)?;
        self.pos = end;
        Ok(slice)
    }

    /// Reads a Byte.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] at the end of the input.
    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        let byte = *self.input.get(self.pos).ok_or(DecodeError::Incomplete)?;
        self.pos += 1;
        Ok(byte)
    }

    /// Reads a Two Byte Integer, big-endian.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] when fewer than two bytes remain.
    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    /// Reads a Four Byte Integer, big-endian.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] when fewer than four bytes remain.
    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Reads a Variable Byte Integer.
    ///
    /// # Errors
    ///
    /// Whatever [`varint::decode`] reports: incomplete, non-minimal, or longer
    /// than four bytes.
    pub fn varint(&mut self) -> Result<u32, DecodeError> {
        let (value, used) = varint::decode(&self.input[self.pos..])?;
        self.pos += used;
        Ok(value)
    }

    /// Reads a UTF-8 Encoded String, borrowing it.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] when the declared length is not there,
    /// [`DecodeError::MalformedUtf8`] when the bytes are not well-formed, and
    /// [`DecodeError::NullCharacter`] when they contain U+0000.
    pub fn string(&mut self) -> Result<&'a str, DecodeError> {
        let len = usize::from(self.u16()?);
        let bytes = self.take(len)?;
        let text = core::str::from_utf8(bytes).map_err(|_| DecodeError::MalformedUtf8)?;
        if text.as_bytes().contains(&0) {
            return Err(DecodeError::NullCharacter);
        }
        Ok(text)
    }

    /// Reads Binary Data, borrowing it.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] when the declared length is not there.
    pub fn binary(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = usize::from(self.u16()?);
        self.take(len)
    }
}

/// Bytes a UTF-8 Encoded String or Binary Data field of `len` payload bytes
/// occupies, including its two-byte length prefix.
///
/// # Errors
///
/// [`EncodeError::FieldTooLong`] above [`MAX_FIELD_LEN`].
pub fn field_len(len: usize) -> Result<u32, EncodeError> {
    if len > MAX_FIELD_LEN {
        return Err(EncodeError::FieldTooLong { len });
    }
    Ok(len as u32 + 2)
}

/// Appends a UTF-8 Encoded String.
///
/// # Errors
///
/// [`EncodeError::FieldTooLong`] above [`MAX_FIELD_LEN`].
pub fn put_string(text: &str, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    put_binary(text.as_bytes(), out)
}

/// Appends Binary Data.
///
/// # Errors
///
/// [`EncodeError::FieldTooLong`] above [`MAX_FIELD_LEN`].
pub fn put_binary(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), EncodeError> {
    if bytes.len() > MAX_FIELD_LEN {
        return Err(EncodeError::FieldTooLong { len: bytes.len() });
    }
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_integer_widths_are_big_endian() {
        let mut reader = Reader::new(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]);
        assert_eq!(reader.u8(), Ok(0x01));
        assert_eq!(reader.u16(), Ok(0x0203));
        assert_eq!(reader.u32(), Ok(0x0405_0607));
        assert!(reader.is_empty());
    }

    #[test]
    fn a_string_borrows_and_round_trips() {
        let mut out = Vec::new();
        put_string("a/b", &mut out).unwrap();
        assert_eq!(out, [0x00, 0x03, b'a', b'/', b'b']);

        let mut reader = Reader::new(&out);
        assert_eq!(reader.string(), Ok("a/b"));
        assert_eq!(reader.position(), 5);
        assert_eq!(field_len(3), Ok(5));
    }

    #[test]
    fn an_empty_string_and_empty_binary_are_legal() {
        let mut reader = Reader::new(&[0x00, 0x00, 0x00, 0x00]);
        assert_eq!(reader.string(), Ok(""));
        assert_eq!(reader.binary(), Ok(&[][..]));
    }

    #[test]
    fn a_malformed_string_is_refused_and_a_truncated_one_asks_for_bytes() {
        // 0xFF is never a UTF-8 lead byte.
        assert_eq!(
            Reader::new(&[0x00, 0x01, 0xFF]).string(),
            Err(DecodeError::MalformedUtf8)
        );
        // A lone surrogate half, which WTF-8 permits and UTF-8 does not.
        assert_eq!(
            Reader::new(&[0x00, 0x03, 0xED, 0xA0, 0x80]).string(),
            Err(DecodeError::MalformedUtf8)
        );
        assert_eq!(
            Reader::new(&[0x00, 0x04, b'a']).string(),
            Err(DecodeError::Incomplete)
        );
    }

    /// [MQTT-1.5.4-2]: a null character is a Malformed Packet, even though it
    /// is well-formed UTF-8.
    #[test]
    fn a_null_character_is_refused() {
        assert_eq!(
            Reader::new(&[0x00, 0x02, b'a', 0x00]).string(),
            Err(DecodeError::NullCharacter)
        );
    }

    /// The SHOULD NOT of 1.5.3 read as documented in this module: control
    /// characters and U+FEFF are carried, not rejected and not stripped.
    #[test]
    fn disallowed_code_points_are_carried_rather_than_refused() {
        let mut out = Vec::new();
        put_string("a\u{1}b\u{feff}c\u{9f}", &mut out).unwrap();
        let decoded = Reader::new(&out).string().expect("tolerated");
        assert_eq!(decoded, "a\u{1}b\u{feff}c\u{9f}");
    }

    #[test]
    fn a_field_above_the_two_byte_bound_cannot_be_encoded() {
        let oversized = vec![0u8; MAX_FIELD_LEN + 1];
        let mut out = Vec::new();
        assert_eq!(
            put_binary(&oversized, &mut out),
            Err(EncodeError::FieldTooLong {
                len: MAX_FIELD_LEN + 1
            })
        );
        assert_eq!(
            field_len(MAX_FIELD_LEN + 1),
            Err(EncodeError::FieldTooLong {
                len: MAX_FIELD_LEN + 1
            })
        );
        assert_eq!(field_len(MAX_FIELD_LEN), Ok(65_537));
    }

    /// The declared length is never used to size an allocation: a 64 KiB
    /// declaration over a three-byte buffer is `Incomplete`, and the reader
    /// has reserved nothing.
    #[test]
    fn a_large_declaration_over_a_small_buffer_allocates_nothing() {
        let mut reader = Reader::new(&[0xFF, 0xFF, 0x00]);
        assert_eq!(reader.binary(), Err(DecodeError::Incomplete));
    }
}
