//! weida wire protocol v0: framing, CBOR headers, negotiation and QUIC
//! application error codes.
//!
//! The crate performs no I/O. It parses and encodes byte slices, which makes it
//! directly fuzzable and keeps hostile-input handling in one place. Every
//! decoder treats its input as hostile: lengths are checked before allocation,
//! duplicate map keys are rejected and nesting depth is bounded.
//!
//! `docs/PROTOCOL.md` is the normative specification; this crate is its
//! implementation, and the golden vectors in §8 are asserted byte-for-byte by
//! the unit tests.

pub mod codes;
pub mod datagram;
pub mod frame;
pub mod header;
pub mod negotiate;
pub mod varint;

pub use datagram::{LOCAL_MAX_DATAGRAM, flow_prefix, split_flow_datagram};
pub use frame::{
    FrameKind, MAGIC, MAX_PREAMBLE_LEN, Preamble, PreambleError, encode_frame, encode_preamble,
    parse_preamble, preamble_bytes,
};
pub use header::{
    CreditHeader, CursorHeader, CursorLevel, DataHeader, ErrorHeader, FlowHeader, HeaderError,
    Hello, PathRecord, ReportHead, ReportMode, SubscriptionHeader, decode_cursor_record,
    encode_cursor_record, filter, limits as header_limits, skip_value,
};
pub use negotiate::{
    Agreed, CAPABILITY_DATAGRAM, CAPABILITY_PATH_REPORT, NegotiateError, negotiate,
};
pub use varint::{VarintError, decode_varint, encode_varint, varint_len};

/// Wire protocol version implemented by this crate (experimental).
pub const VERSION: u64 = 0;

/// TLS ALPN token identifying the weida native QUIC protocol.
pub const ALPN: &[u8] = b"weida/0";

/// TLS ALPN token of a consensus connection (weida-raft); same protocol, separate connections.
pub const RAFT_ALPN: &[u8] = b"weida-raft/0";

#[cfg(test)]
mod tests {
    #[test]
    fn alpn_matches_the_wire_version() {
        assert_eq!(super::ALPN, b"weida/0");
        assert_eq!(super::VERSION, 0);
    }
}
