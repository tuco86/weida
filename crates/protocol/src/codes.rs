//! QUIC application error codes used in `CONNECTION_CLOSE`, `RESET_STREAM` and
//! `STOP_SENDING` (`docs/PROTOCOL.md` §7).
//!
//! These are plain `u64` values: the protocol crate stays free of transport
//! types, and the `weida` crate wraps them in `quinn::VarInt`.

use weida_core::StopReason;

/// Normal closure.
pub const NO_ERROR: u64 = 0;
/// The local side failed internally.
pub const INTERNAL: u64 = 1;
/// The peer broke the wire protocol.
pub const PROTOCOL_VIOLATION: u64 = 2;
/// The transfer is no longer wanted.
pub const CANCELED: u64 = 3;
/// Version or capability negotiation failed.
pub const NEGOTIATION_FAILED: u64 = 4;
/// A resource limit was reached.
pub const LIMIT_EXCEEDED: u64 = 5;
/// Orderly shutdown of the runtime.
pub const SHUTDOWN: u64 = 6;
/// The receiving application declined the transfer.
pub const REJECTED: u64 = 7;
/// No endpoint is registered for the requested path.
pub const UNKNOWN_ENDPOINT: u64 = 8;
/// The endpoint exists but does not serve this stream kind.
pub const UNSUPPORTED: u64 = 9;
/// The binding closes because its server is being replaced; the address is served again at once.
pub const RESTARTING: u64 = 10;

/// Human-readable name for a code, for logs and close reasons.
pub const fn name(code: u64) -> &'static str {
    match code {
        NO_ERROR => "NO_ERROR",
        INTERNAL => "INTERNAL",
        PROTOCOL_VIOLATION => "PROTOCOL_VIOLATION",
        CANCELED => "CANCELED",
        NEGOTIATION_FAILED => "NEGOTIATION_FAILED",
        LIMIT_EXCEEDED => "LIMIT_EXCEEDED",
        SHUTDOWN => "SHUTDOWN",
        REJECTED => "REJECTED",
        UNKNOWN_ENDPOINT => "UNKNOWN_ENDPOINT",
        UNSUPPORTED => "UNSUPPORTED",
        RESTARTING => "RESTARTING",
        _ => "UNKNOWN",
    }
}

/// Interprets a `STOP_SENDING` code as a sender-side outcome reason.
pub const fn stop_reason(code: u64) -> StopReason {
    match code {
        REJECTED => StopReason::Rejected,
        CANCELED => StopReason::Canceled,
        UNKNOWN_ENDPOINT => StopReason::UnknownEndpoint,
        UNSUPPORTED => StopReason::Unsupported,
        LIMIT_EXCEEDED => StopReason::LimitExceeded,
        SHUTDOWN => StopReason::ShuttingDown,
        other => StopReason::Other(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_match_the_protocol_document() {
        assert_eq!(
            [
                NO_ERROR,
                INTERNAL,
                PROTOCOL_VIOLATION,
                CANCELED,
                NEGOTIATION_FAILED,
                LIMIT_EXCEEDED,
                SHUTDOWN,
                REJECTED,
                UNKNOWN_ENDPOINT,
                UNSUPPORTED,
                RESTARTING
            ],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        );
    }

    #[test]
    fn names_are_defined_for_known_codes() {
        assert_eq!(name(PROTOCOL_VIOLATION), "PROTOCOL_VIOLATION");
        assert_eq!(name(NEGOTIATION_FAILED), "NEGOTIATION_FAILED");
        assert_eq!(name(UNSUPPORTED), "UNSUPPORTED");
        assert_eq!(name(RESTARTING), "RESTARTING");
        assert_eq!(name(11), "UNKNOWN");
    }

    #[test]
    fn stop_reasons_map_the_refusal_codes() {
        assert_eq!(stop_reason(REJECTED), StopReason::Rejected);
        assert_eq!(stop_reason(CANCELED), StopReason::Canceled);
        assert_eq!(stop_reason(UNKNOWN_ENDPOINT), StopReason::UnknownEndpoint);
        assert_eq!(stop_reason(UNSUPPORTED), StopReason::Unsupported);
        // A paired endpoint that already has its peer refuses a newcomer's
        // stream with this code, so it has to read as a capacity refusal
        // rather than an unknown number (`docs/PATTERNS.md`, PAIR).
        assert_eq!(stop_reason(LIMIT_EXCEEDED), StopReason::LimitExceeded);
        // A draining peer stops a stream that arrived too late with this
        // code, so a sender must be able to read it as a refusal rather than
        // as an unknown number (`docs/decisions/0009-drain.md` §4.5).
        assert_eq!(stop_reason(SHUTDOWN), StopReason::ShuttingDown);
        assert_eq!(stop_reason(42), StopReason::Other(42));
    }
}
