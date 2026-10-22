//! HTTP/3 frames and stream types (RFC 9114 §6, §7).
//!
//! Every HTTP/3 frame is a type and a length, both QUIC variable-length
//! integers, then the payload. [`decode_frame_header`] reads the two; the
//! connection then decides from the type how much of the payload it is
//! willing to hold (DATA is streamed, HEADERS is bounded by
//! [`Limits::max_head_bytes`], everything else by [`Limits::max_frame_size`])
//! before reading it.

use crate::error::{DecodeError, EncodeError};
use crate::limits::Limits;
use crate::varint;

/// Frame types (RFC 9114 §7.2).
pub mod frame {
    /// DATA.
    pub const DATA: u64 = 0x0;
    /// HEADERS.
    pub const HEADERS: u64 = 0x1;
    /// CANCEL_PUSH.
    pub const CANCEL_PUSH: u64 = 0x3;
    /// SETTINGS.
    pub const SETTINGS: u64 = 0x4;
    /// PUSH_PROMISE.
    pub const PUSH_PROMISE: u64 = 0x5;
    /// GOAWAY.
    pub const GOAWAY: u64 = 0x7;
    /// MAX_PUSH_ID.
    pub const MAX_PUSH_ID: u64 = 0xd;
}

/// Whether `kind` is an HTTP/2 frame type HTTP/3 reserves (PRIORITY, PING,
/// WINDOW_UPDATE, CONTINUATION): receiving one is `H3_FRAME_UNEXPECTED`
/// (RFC 9114 §7.2.8).
#[must_use]
pub const fn is_http2_reserved(kind: u64) -> bool {
    matches!(kind, 0x2 | 0x6 | 0x8 | 0x9)
}

/// Unidirectional stream types (RFC 9114 §6.2, RFC 9204 §4.2).
pub mod stream {
    /// The control stream.
    pub const CONTROL: u64 = 0x0;
    /// A push stream.
    pub const PUSH: u64 = 0x1;
    /// The QPACK encoder stream.
    pub const QPACK_ENCODER: u64 = 0x2;
    /// The QPACK decoder stream.
    pub const QPACK_DECODER: u64 = 0x3;
}

/// Setting identifiers (RFC 9114 §7.2.4.1, RFC 9204 §5).
pub mod setting {
    /// `SETTINGS_QPACK_MAX_TABLE_CAPACITY`.
    pub const QPACK_MAX_TABLE_CAPACITY: u64 = 0x1;
    /// `SETTINGS_MAX_FIELD_SECTION_SIZE`.
    pub const MAX_FIELD_SECTION_SIZE: u64 = 0x6;
    /// `SETTINGS_QPACK_BLOCKED_STREAMS`.
    pub const QPACK_BLOCKED_STREAMS: u64 = 0x7;
}

/// Error codes (RFC 9114 §8.1, RFC 9204 §6).
pub mod code {
    /// No error.
    pub const H3_NO_ERROR: u64 = 0x100;
    /// General protocol error.
    pub const H3_GENERAL_PROTOCOL_ERROR: u64 = 0x101;
    /// Internal error.
    pub const H3_INTERNAL_ERROR: u64 = 0x102;
    /// Stream creation error.
    pub const H3_STREAM_CREATION_ERROR: u64 = 0x103;
    /// Critical stream was closed.
    pub const H3_CLOSED_CRITICAL_STREAM: u64 = 0x104;
    /// Frame not permitted in the current state.
    pub const H3_FRAME_UNEXPECTED: u64 = 0x105;
    /// Frame violated layout or size rules.
    pub const H3_FRAME_ERROR: u64 = 0x106;
    /// Peer generating excessive load.
    pub const H3_EXCESSIVE_LOAD: u64 = 0x107;
    /// An identifier was used incorrectly.
    pub const H3_ID_ERROR: u64 = 0x108;
    /// SETTINGS frame contained invalid values.
    pub const H3_SETTINGS_ERROR: u64 = 0x109;
    /// No SETTINGS frame received.
    pub const H3_MISSING_SETTINGS: u64 = 0x10a;
    /// Request not processed.
    pub const H3_REQUEST_REJECTED: u64 = 0x10b;
    /// Data no longer needed.
    pub const H3_REQUEST_CANCELLED: u64 = 0x10c;
    /// Stream terminated early.
    pub const H3_REQUEST_INCOMPLETE: u64 = 0x10d;
    /// Malformed message.
    pub const H3_MESSAGE_ERROR: u64 = 0x10e;
    /// TCP reset or error on CONNECT request.
    pub const H3_CONNECT_ERROR: u64 = 0x10f;
    /// Retry over HTTP/1.1.
    pub const H3_VERSION_FALLBACK: u64 = 0x110;
    /// The decoder failed to interpret an encoded field section.
    pub const QPACK_DECOMPRESSION_FAILED: u64 = 0x200;
    /// The decoder failed to interpret an encoder instruction.
    pub const QPACK_ENCODER_STREAM_ERROR: u64 = 0x201;
    /// The encoder failed to interpret a decoder instruction.
    pub const QPACK_DECODER_STREAM_ERROR: u64 = 0x202;
}

/// A frame's type and payload length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    /// The frame type.
    pub kind: u64,
    /// The payload length.
    pub len: u64,
}

/// Decodes a frame header. Returns it and the octets used.
///
/// # Errors
///
/// `Incomplete`.
pub fn decode_frame_header(input: &[u8]) -> Result<(FrameHeader, usize), DecodeError> {
    let (kind, a) = varint::decode(input)?;
    let (len, b) = varint::decode(&input[a..])?;
    Ok((FrameHeader { kind, len }, a + b))
}

const fn malformed(what: &'static str) -> DecodeError {
    DecodeError::Malformed { what }
}

/// Decodes a SETTINGS payload into `(identifier, value)` pairs.
///
/// # Errors
///
/// `TooLarge` `"frame"` for a payload above [`Limits::max_frame_size`],
/// checked before parsing; `Malformed` `"SETTINGS"` for a truncated pair,
/// `"duplicate setting"`, or `"HTTP/2 setting in HTTP/3"` for identifiers
/// 0x2 to 0x5 (RFC 9114 §7.2.4.1), all `H3_SETTINGS_ERROR`.
pub fn decode_settings(payload: &[u8], limits: Limits) -> Result<Vec<(u64, u64)>, DecodeError> {
    if payload.len() as u64 > u64::from(limits.max_frame_size) {
        return Err(DecodeError::TooLarge {
            what: "frame",
            declared: payload.len() as u64,
            cap: u64::from(limits.max_frame_size),
        });
    }
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut pos = 0;
    while pos < payload.len() {
        let (id, a) = varint::decode(&payload[pos..]).map_err(|_| malformed("SETTINGS"))?;
        pos += a;
        let (value, b) = varint::decode(&payload[pos..]).map_err(|_| malformed("SETTINGS"))?;
        pos += b;
        if (0x2..=0x5).contains(&id) {
            return Err(malformed("HTTP/2 setting in HTTP/3"));
        }
        if out.iter().any(|&(seen, _)| seen == id) {
            return Err(malformed("duplicate setting"));
        }
        out.push((id, value));
    }
    Ok(out)
}

/// Decodes a GOAWAY payload: one stream or push identifier.
///
/// # Errors
///
/// `Malformed` `"GOAWAY"` unless the payload is exactly one integer.
pub fn decode_goaway(payload: &[u8]) -> Result<u64, DecodeError> {
    match varint::decode(payload) {
        Ok((id, used)) if used == payload.len() => Ok(id),
        _ => Err(malformed("GOAWAY")),
    }
}

fn header(kind: u64, len: u64, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    varint::encode(kind, out)?;
    varint::encode(len, out)
}

/// Appends a SETTINGS frame.
///
/// # Errors
///
/// `TooLarge` `"varint"` for a value above 2^62 - 1. Nothing is written on
/// error.
pub fn encode_settings(settings: &[(u64, u64)], out: &mut Vec<u8>) -> Result<(), EncodeError> {
    let mut payload = Vec::new();
    for &(id, value) in settings {
        varint::encode(id, &mut payload)?;
        varint::encode(value, &mut payload)?;
    }
    header(frame::SETTINGS, payload.len() as u64, out)?;
    out.extend_from_slice(&payload);
    Ok(())
}

/// Appends the header of a DATA frame whose payload of `len` octets the
/// caller writes next.
///
/// # Errors
///
/// `TooLarge` `"varint"` above 2^62 - 1.
pub fn encode_data_header(len: u64, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    header(frame::DATA, len, out)
}

/// Appends a HEADERS frame carrying `block`, an encoded field section.
pub fn encode_headers(block: &[u8], out: &mut Vec<u8>) {
    header(frame::HEADERS, block.len() as u64, out).expect("a slice length fits a varint");
    out.extend_from_slice(block);
}

/// Appends a GOAWAY frame naming `id`.
///
/// # Errors
///
/// `TooLarge` `"varint"` above 2^62 - 1. Nothing is written on error.
pub fn encode_goaway(id: u64, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    if id > varint::MAX {
        return Err(EncodeError::TooLarge {
            what: "varint",
            len: id,
            cap: varint::MAX,
        });
    }
    header(frame::GOAWAY, varint::len(id) as u64, out)?;
    varint::encode(id, out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_round_trip_and_refuse_by_name() {
        let mut out = Vec::new();
        encode_settings(
            &[(setting::MAX_FIELD_SECTION_SIZE, 65_536), (0x21, 0)],
            &mut out,
        )
        .unwrap();
        let (head, used) = decode_frame_header(&out).unwrap();
        assert_eq!(
            head,
            FrameHeader {
                kind: frame::SETTINGS,
                len: (out.len() - used) as u64
            }
        );
        assert_eq!(
            decode_settings(&out[used..], Limits::DEFAULT),
            Ok(vec![(6, 65_536), (0x21, 0)])
        );
        let refuse = |p: &[u8]| match decode_settings(p, Limits::DEFAULT) {
            Err(DecodeError::Malformed { what }) => what,
            other => panic!("{other:?}"),
        };
        assert_eq!(refuse(&[0x06, 0x01, 0x06, 0x02]), "duplicate setting");
        assert_eq!(refuse(&[0x04, 0x00]), "HTTP/2 setting in HTTP/3");
        assert_eq!(refuse(&[0x06]), "SETTINGS");
        let small = Limits {
            max_frame_size: 1,
            ..Limits::DEFAULT
        };
        assert!(matches!(
            decode_settings(&[0x06, 0x01], small),
            Err(DecodeError::TooLarge { what: "frame", .. })
        ));
    }

    #[test]
    fn goaway_is_one_integer() {
        let mut out = Vec::new();
        encode_goaway(400, &mut out).unwrap();
        let (head, used) = decode_frame_header(&out).unwrap();
        assert_eq!(head.kind, frame::GOAWAY);
        assert_eq!(decode_goaway(&out[used..]), Ok(400));
        assert_eq!(decode_goaway(&[0x01, 0x02]), Err(malformed("GOAWAY")));
    }

    #[test]
    fn the_reserved_http2_types() {
        assert!([0x2, 0x6, 0x8, 0x9].into_iter().all(is_http2_reserved));
        assert!(!is_http2_reserved(frame::DATA));
        assert!(!is_http2_reserved(frame::GOAWAY));
    }
}
