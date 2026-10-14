//! W3C Trace Context propagation.
//!
//! v0 propagates `traceparent`/`tracestate` in transfer metadata and logs the
//! ids through `tracing` (master doc §58); an OpenTelemetry SDK is outside this
//! crate. This module is the codec only: it never generates identifiers,
//! because randomness belongs to the runtime, not to the model.

use std::fmt;

/// Wire cap for the DATA `traceparent` field, in bytes.
pub const MAX_TRACEPARENT_BYTES: usize = 128;

/// Wire cap for the DATA `tracestate` field, in bytes.
pub const MAX_TRACESTATE_BYTES: usize = 512;

/// Length of a `traceparent` value for version `00`.
const TRACEPARENT_LEN: usize = 55;

/// The `sampled` flag bit.
pub const FLAG_SAMPLED: u8 = 0x01;

/// A W3C Trace Context `traceparent`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceContext {
    /// 16-byte trace identifier; never all zero.
    pub trace_id: [u8; 16],
    /// 8-byte span identifier of the sender's span; never all zero.
    pub span_id: [u8; 8],
    /// Trace flags byte; bit 0 is `sampled`.
    pub flags: u8,
}

/// Why a `traceparent` value was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceError {
    /// Not exactly 55 bytes (the only length defined for version `00`).
    Length,
    /// Hyphens are not at offsets 2, 35 and 52.
    Layout,
    /// A field contained a byte that is not a lowercase hex digit.
    NotHex,
    /// Version `ff` is forbidden by the specification.
    ForbiddenVersion,
    /// `trace_id` or `span_id` was all zero.
    ZeroId,
}

impl fmt::Display for TraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraceError::Length => f.write_str("traceparent must be exactly 55 bytes"),
            TraceError::Layout => f.write_str("traceparent field layout is malformed"),
            TraceError::NotHex => f.write_str("traceparent contains a non-hex-digit byte"),
            TraceError::ForbiddenVersion => f.write_str("traceparent version ff is forbidden"),
            TraceError::ZeroId => f.write_str("traceparent trace-id/span-id must not be all zero"),
        }
    }
}

impl std::error::Error for TraceError {}

const fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        // The specification mandates lowercase; uppercase is rejected rather
        // than normalised, because hostile input gets no leniency.
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

fn decode_hex(src: &[u8], dst: &mut [u8]) -> Result<(), TraceError> {
    debug_assert_eq!(src.len(), dst.len() * 2);
    // `as_chunks` over `chunks_exact`: the pair width is a constant, so the
    // compiler gets `[u8; 2]` and the indexing below needs no bounds checks.
    let (pairs, _remainder) = src.as_chunks::<2>();
    for (out, &[hi, lo]) in dst.iter_mut().zip(pairs) {
        let hi = hex_val(hi).ok_or(TraceError::NotHex)?;
        let lo = hex_val(lo).ok_or(TraceError::NotHex)?;
        *out = (hi << 4) | lo;
    }
    Ok(())
}

fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for b in bytes {
        f.write_str(
            core::str::from_utf8(&[HEX[(*b >> 4) as usize], HEX[(*b & 0x0f) as usize]])
                .expect("hex digits are ascii"),
        )?;
    }
    Ok(())
}

impl TraceContext {
    /// Builds a context from raw identifiers.
    ///
    /// Returns `None` if either identifier is all zero, which the W3C
    /// specification forbids.
    pub const fn new(trace_id: [u8; 16], span_id: [u8; 8], flags: u8) -> Option<TraceContext> {
        if is_zero_16(&trace_id) || is_zero_8(&span_id) {
            return None;
        }
        Some(TraceContext {
            trace_id,
            span_id,
            flags,
        })
    }

    /// Parses a `traceparent` header value.
    ///
    /// Only version `00` layout is accepted: exactly 55 bytes,
    /// `vv-<32 hex>-<16 hex>-<2 hex>`, lowercase hex, version not `ff`, neither
    /// identifier all zero.
    pub fn parse_traceparent(value: &str) -> Result<TraceContext, TraceError> {
        let b = value.as_bytes();
        if b.len() != TRACEPARENT_LEN {
            return Err(TraceError::Length);
        }
        if b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
            return Err(TraceError::Layout);
        }

        let mut version = [0u8; 1];
        decode_hex(&b[0..2], &mut version)?;
        if version[0] == 0xff {
            return Err(TraceError::ForbiddenVersion);
        }

        let mut trace_id = [0u8; 16];
        decode_hex(&b[3..35], &mut trace_id)?;
        let mut span_id = [0u8; 8];
        decode_hex(&b[36..52], &mut span_id)?;
        let mut flags = [0u8; 1];
        decode_hex(&b[53..55], &mut flags)?;

        TraceContext::new(trace_id, span_id, flags[0]).ok_or(TraceError::ZeroId)
    }

    /// Formats the context as a version-`00` `traceparent` value.
    pub fn to_traceparent(&self) -> String {
        self.to_string()
    }

    /// The trace id as 32 lowercase hex digits, for log fields.
    pub fn trace_id_hex(&self) -> String {
        let mut s = String::with_capacity(32);
        push_hex(&mut s, &self.trace_id);
        s
    }

    /// The span id as 16 lowercase hex digits, for log fields.
    pub fn span_id_hex(&self) -> String {
        let mut s = String::with_capacity(16);
        push_hex(&mut s, &self.span_id);
        s
    }

    /// True if the `sampled` flag is set.
    pub const fn is_sampled(&self) -> bool {
        self.flags & FLAG_SAMPLED != 0
    }

    /// Same trace, new span. Returns `None` for an all-zero span id.
    pub const fn with_span_id(&self, span_id: [u8; 8]) -> Option<TraceContext> {
        TraceContext::new(self.trace_id, span_id, self.flags)
    }
}

const fn is_zero_16(v: &[u8; 16]) -> bool {
    let mut i = 0;
    while i < 16 {
        if v[i] != 0 {
            return false;
        }
        i += 1;
    }
    true
}

const fn is_zero_8(v: &[u8; 8]) -> bool {
    let mut i = 0;
    while i < 8 {
        if v[i] != 0 {
            return false;
        }
        i += 1;
    }
    true
}

fn push_hex(s: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for b in bytes {
        s.push(HEX[(*b >> 4) as usize] as char);
        s.push(HEX[(*b & 0x0f) as usize] as char);
    }
}

impl fmt::Display for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("00-")?;
        write_hex(f, &self.trace_id)?;
        f.write_str("-")?;
        write_hex(f, &self.span_id)?;
        f.write_str("-")?;
        write_hex(f, &[self.flags])
    }
}

impl fmt::Debug for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceContext({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn parses_the_w3c_example() {
        let t = TraceContext::parse_traceparent(VALID).unwrap();
        assert_eq!(t.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(t.span_id_hex(), "00f067aa0ba902b7");
        assert_eq!(t.flags, 1);
        assert!(t.is_sampled());
    }

    #[test]
    fn format_parse_roundtrip() {
        let t = TraceContext::parse_traceparent(VALID).unwrap();
        assert_eq!(t.to_traceparent(), VALID);
        assert_eq!(
            TraceContext::parse_traceparent(&t.to_traceparent()).unwrap(),
            t
        );
    }

    #[test]
    fn formats_a_cleared_flags_byte() {
        let t = TraceContext::new([0x11; 16], [0x22; 8], 0).unwrap();
        assert_eq!(
            t.to_traceparent(),
            "00-11111111111111111111111111111111-2222222222222222-00"
        );
        assert!(!t.is_sampled());
    }

    #[test]
    fn rejects_invalid_values() {
        let cases = [
            (
                "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                TraceError::ForbiddenVersion,
            ),
            (
                "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
                TraceError::ZeroId,
            ),
            (
                "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
                TraceError::ZeroId,
            ),
            (
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0",
                TraceError::Length,
            ),
            (
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-011",
                TraceError::Length,
            ),
            ("", TraceError::Length),
            (
                "00_4bf92f3577b34da6a3ce929d0e0e4736_00f067aa0ba902b7_01",
                TraceError::Layout,
            ),
            (
                "00-4bf92f3577b34da6a3ce929d0e0e4736+00f067aa0ba902b7-01",
                TraceError::Layout,
            ),
            (
                "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
                TraceError::NotHex,
            ),
            (
                "00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01",
                TraceError::NotHex,
            ),
        ];
        for (input, want) in cases {
            assert_eq!(
                TraceContext::parse_traceparent(input),
                Err(want),
                "input {input:?}"
            );
        }
    }

    #[test]
    fn future_versions_parse_with_the_v00_layout() {
        // Version 01 with the version-00 layout is still readable; only `ff`
        // is forbidden outright.
        let t = TraceContext::parse_traceparent(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .unwrap();
        assert_eq!(t.span_id_hex(), "00f067aa0ba902b7");
    }

    #[test]
    fn formatted_value_fits_the_wire_cap() {
        let t = TraceContext::parse_traceparent(VALID).unwrap();
        assert!(t.to_traceparent().len() <= MAX_TRACEPARENT_BYTES);
        assert_eq!(t.to_traceparent().len(), TRACEPARENT_LEN);
    }

    #[test]
    fn zero_ids_are_unconstructable() {
        assert!(TraceContext::new([0; 16], [1; 8], 0).is_none());
        assert!(TraceContext::new([1; 16], [0; 8], 0).is_none());
        let t = TraceContext::new([1; 16], [1; 8], 0).unwrap();
        assert!(t.with_span_id([0; 8]).is_none());
        assert_eq!(t.with_span_id([9; 8]).unwrap().trace_id, [1; 16]);
    }
}
