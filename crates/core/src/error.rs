//! The single error type of the framework.
//!
//! Hand-written `Display`/`Error` impls: the project's dependency discipline
//! (master doc §72) does not admit `thiserror` for one enum.

use std::fmt;

/// Convenience alias used throughout the framework.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything that can go wrong in a weida operation.
///
/// The variants are deliberately outcome-shaped rather than cause-shaped: an
/// application must distinguish `ConnectionLost` (definitely not delivered)
/// from `Indeterminate` (may or may not have been delivered) because the two
/// permit different retry decisions. See `docs/FAILURE_MODEL.md`.
#[derive(Debug)]
pub enum Error {
    /// The runtime could not be created or used (e.g. no ambient reactor).
    Runtime(String),
    /// A `weida://` URL could not be parsed.
    InvalidAddress(String),
    /// An endpoint path violated the addressing rules.
    InvalidEndpointPath,
    /// A fingerprint's text form was not `sha256:` plus 64 hex digits.
    InvalidFingerprint(String),
    /// An endpoint path is already registered on this listener.
    AlreadyRegistered,
    /// The endpoint has no usable peer connection.
    NotConnected,
    /// The connection was lost before the local transfer reached FIN; the
    /// payload was definitely not delivered. The [`LossCause`] says why,
    /// which is what an application deciding whether to redial needs.
    ConnectionLost(LossCause),
    /// Version/capability negotiation failed.
    Negotiation(String),
    /// The peer violated the wire protocol.
    Protocol(String),
    /// The peer refused the transfer (`STOP_SENDING(REJECTED)` or
    /// `ERROR{REJECTED}`).
    Rejected,
    /// The peer has no endpoint registered under the requested path.
    UnknownEndpoint,
    /// The peer does not support a requested protocol feature.
    Unsupported,
    /// A stream toward the peer was needed and the peer had parked no
    /// connection for one. Only a local socket transport can produce this:
    /// an accepted socket cannot be dialled back, so fan-out rides the
    /// connections a subscriber parks
    /// ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.4). A publisher treats it as a drop of that copy, not as a failure
    /// of the subscription.
    NoParkedConnection,
    /// The peer accepted the request but never opened a reply stream.
    NoReply,
    /// The transfer was canceled, locally or by the peer.
    Canceled,
    /// The transfer's deadline passed before the peer acknowledged every
    /// byte, so this side reset it with `CANCELED`
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.3).
    /// **Not** a definite failure: the peer may have read every byte before
    /// the reset landed.
    Expired,
    /// The connection was lost after the local FIN while awaiting an ACK or a
    /// reply: the outcome is genuinely unknown (master doc §22).
    Indeterminate,
    /// A local or negotiated resource limit was reached.
    LimitExceeded,
    /// The connection carries no datagrams: one side's profile did not
    /// enable flows, so capability code `1` was not agreed
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.5).
    /// weida never substitutes a stream, which would deliver the unit late.
    DatagramsUnavailable,
    /// A datagram payload exceeds the largest datagram this connection
    /// carries right now, `max` bytes after the flow id's prefix.
    TooLarge {
        /// The largest payload the connection carries.
        max: usize,
    },
    /// TLS material could not be loaded or configured, or the handshake
    /// failed for a reason other than an untrusted peer.
    Tls(String),
    /// The peer proved possession of a key whose fingerprint is neither
    /// pinned nor certified by a configured anchor. Carries what the peer
    /// presented, so an operator can pin it after checking it out of band.
    Untrusted(crate::identity::Fingerprint),
    /// Underlying I/O failure.
    Io(std::io::Error),
    /// Transport-level failure that is not one of the modelled outcomes.
    Transport(String),
}

/// Why a connection is gone.
///
/// The *outcome* is the same whichever it is — nothing that was in flight
/// completed, which is what [`Error::ConnectionLost`] promises — so this is
/// not a second outcome vocabulary. It exists because the next action differs:
/// an idle timeout invites a redial, a peer that closed deliberately may not
/// want one yet, and a local close means the application already decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LossCause {
    /// No traffic for the idle period, on whichever side's timeout was
    /// shorter. Nothing is wrong with either peer.
    IdleTimeout,
    /// The peer closed the connection deliberately, with a code this side
    /// does not map to a more specific outcome — a shutdown, typically.
    PeerClosed,
    /// The peer closed with `RESTARTING`: its server is being replaced, and a
    /// successor serves, or is about to serve, the same address. A redial is
    /// due at once.
    PeerRestarting,
    /// This side closed it: `Runtime::shutdown`, or a dropped runtime.
    LocallyClosed,
    /// A stateless reset: the peer has forgotten the connection, usually
    /// because it restarted.
    Reset,
    /// A QUIC transport error ended the connection. Either peer may be at
    /// fault, and a redial is unlikely to behave differently.
    TransportError,
}

impl fmt::Display for LossCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LossCause::IdleTimeout => "idle timeout",
            LossCause::PeerClosed => "closed by the peer",
            LossCause::PeerRestarting => "the peer is restarting",
            LossCause::LocallyClosed => "closed locally",
            LossCause::Reset => "stateless reset",
            LossCause::TransportError => "transport error",
        })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Runtime(m) => write!(f, "runtime error: {m}"),
            Error::InvalidAddress(m) => write!(f, "invalid address: {m}"),
            Error::InvalidEndpointPath => f.write_str(
                "invalid endpoint path: must start with '/', be 1..=512 bytes and contain no control bytes",
            ),
            Error::InvalidFingerprint(m) => {
                write!(f, "invalid fingerprint: expected sha256:<64 hex digits>, got {m:?}")
            }
            Error::AlreadyRegistered => f.write_str("endpoint path already registered"),
            Error::NotConnected => f.write_str("endpoint is not connected to any peer"),
            Error::ConnectionLost(cause) => {
                write!(f, "connection lost before the transfer completed: {cause}")
            }
            Error::Negotiation(m) => write!(f, "negotiation failed: {m}"),
            Error::Protocol(m) => write!(f, "protocol violation: {m}"),
            Error::Rejected => f.write_str("peer rejected the transfer"),
            Error::UnknownEndpoint => f.write_str("peer has no such endpoint"),
            Error::Unsupported => f.write_str("peer does not support the requested feature"),
            Error::NoParkedConnection => {
                f.write_str("peer has no parked connection for a stream toward it")
            }
            Error::NoReply => f.write_str("peer accepted the request but sent no reply"),
            Error::Canceled => f.write_str("transfer canceled"),
            Error::Expired => {
                f.write_str("the transfer expired before the peer acknowledged it")
            }
            Error::Indeterminate => {
                f.write_str("outcome indeterminate: the transfer may or may not have been accepted")
            }
            Error::LimitExceeded => f.write_str("resource limit exceeded"),
            Error::DatagramsUnavailable => {
                f.write_str("datagrams are not available on this connection")
            }
            Error::TooLarge { max } => write!(
                f,
                "payload exceeds the largest datagram this connection carries ({max} bytes)"
            ),
            Error::Tls(m) => write!(f, "tls error: {m}"),
            Error::Untrusted(fp) => write!(f, "peer identity {fp} is not trusted"),
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Transport(m) => write!(f, "transport error: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// An `io::Error` that carries an [`Error`] (built by the reverse conversion
/// below, which is how a stream reports weida's outcome through `AsyncRead`
/// and `AsyncWrite`) gives that error back; any other is [`Error::Io`].
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        if e.get_ref().is_some_and(|inner| inner.is::<Error>()) {
            let inner = e.into_inner().expect("checked above");
            return *inner.downcast::<Error>().expect("checked above");
        }
        Error::Io(e)
    }
}

/// Carries the error inside an `io::Error`, so a reader of the I/O traits
/// still learns weida's outcome with `Error::from`. The kind is the closest
/// I/O reading of the outcome; [`Error::Io`] gives its own error back.
impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        use std::io::ErrorKind;
        let kind = match e {
            Error::Io(io) => return io,
            Error::Expired => ErrorKind::TimedOut,
            Error::ConnectionLost(_) | Error::Indeterminate => ErrorKind::NotConnected,
            Error::Rejected
            | Error::Canceled
            | Error::UnknownEndpoint
            | Error::NoReply
            | Error::LimitExceeded => ErrorKind::ConnectionReset,
            _ => ErrorKind::Other,
        };
        std::io::Error::new(kind, e)
    }
}

impl Error {
    /// True if the error proves the transfer had no effect at the peer.
    ///
    /// `Indeterminate` is deliberately *not* in this set: modelling it apart
    /// from definite failure is the point of master doc §22. Neither is
    /// `NoReply` — the request was accepted and may well have had an effect;
    /// only the answer is missing.
    pub fn is_definite_failure(&self) -> bool {
        matches!(
            self,
            Error::ConnectionLost(_)
                | Error::Rejected
                | Error::UnknownEndpoint
                | Error::Unsupported
                | Error::NoParkedConnection
                | Error::Canceled
                | Error::NotConnected
                | Error::LimitExceeded
                | Error::DatagramsUnavailable
                | Error::TooLarge { .. }
                | Error::Untrusted(_)
        )
    }
}

/// Wire codes carried in an ERROR frame (`docs/PROTOCOL.md` §6.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// No endpoint is registered for the requested path.
    UnknownEndpoint,
    /// The receiving side declined the transfer.
    Rejected,
    /// A reserved or unsupported header value was requested.
    Unsupported,
    /// The receiving side failed internally.
    Internal,
    /// The request was accepted but no reply will be produced.
    NoReply,
}

impl ErrorCode {
    /// The wire code.
    pub const fn to_wire(self) -> u64 {
        match self {
            ErrorCode::UnknownEndpoint => 1,
            ErrorCode::Rejected => 2,
            ErrorCode::Unsupported => 3,
            ErrorCode::Internal => 4,
            ErrorCode::NoReply => 5,
        }
    }

    /// Interprets a wire code, returning `None` for unknown values.
    pub const fn from_wire(code: u64) -> Option<ErrorCode> {
        match code {
            1 => Some(ErrorCode::UnknownEndpoint),
            2 => Some(ErrorCode::Rejected),
            3 => Some(ErrorCode::Unsupported),
            4 => Some(ErrorCode::Internal),
            5 => Some(ErrorCode::NoReply),
            _ => None,
        }
    }
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Error {
        match code {
            ErrorCode::UnknownEndpoint => Error::UnknownEndpoint,
            ErrorCode::Rejected => Error::Rejected,
            ErrorCode::Unsupported => Error::Unsupported,
            ErrorCode::Internal => Error::Transport("peer reported an internal error".into()),
            ErrorCode::NoReply => Error::NoReply,
        }
    }
}

/// Why a peer refused to receive more payload, as carried by
/// `STOP_SENDING`'s QUIC application error code.
///
/// The transport maps the numeric code (`weida_protocol::codes`) onto this enum;
/// the core stays free of transport constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StopReason {
    /// `REJECTED`: the application declined the transfer.
    Rejected,
    /// `CANCELED`: the peer is no longer interested.
    Canceled,
    /// `UNKNOWN_ENDPOINT`: no endpoint is registered for the path.
    UnknownEndpoint,
    /// `UNSUPPORTED`: the endpoint exists but does not serve this stream kind.
    Unsupported,
    /// `LIMIT_EXCEEDED`: the endpoint is at a capacity bound — a paired
    /// endpoint that already has its peer, for instance. A refusal of this
    /// stream, not of the connection.
    LimitExceeded,
    /// `SHUTDOWN`: the peer's runtime has stopped admitting work — it is
    /// draining or closing, and this stream arrived too late.
    ShuttingDown,
    /// Any other code, kept for diagnostics.
    Other(u64),
}

impl From<StopReason> for Error {
    fn from(reason: StopReason) -> Error {
        match reason {
            StopReason::Rejected => Error::Rejected,
            StopReason::Canceled => Error::Canceled,
            StopReason::UnknownEndpoint => Error::UnknownEndpoint,
            StopReason::Unsupported => Error::Unsupported,
            StopReason::LimitExceeded => Error::LimitExceeded,
            // A refusal, and a definite one: nothing of this transfer was
            // taken, and the peer will not take it later either. It is
            // `Rejected` rather than a variant of its own because the outcome
            // an application must act on is identical — do not retry against
            // this peer — and a second word for the same outcome would add
            // nothing.
            StopReason::ShuttingDown => Error::Rejected,
            StopReason::Other(code) => {
                Error::Transport(format!("peer stopped receiving with code {code}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_non_empty_for_every_variant() {
        let variants = [
            Error::Runtime("x".into()),
            Error::InvalidAddress("x".into()),
            Error::InvalidEndpointPath,
            Error::AlreadyRegistered,
            Error::NotConnected,
            Error::ConnectionLost(LossCause::IdleTimeout),
            Error::Negotiation("x".into()),
            Error::Protocol("x".into()),
            Error::Rejected,
            Error::UnknownEndpoint,
            Error::Unsupported,
            Error::NoReply,
            Error::Canceled,
            Error::Expired,
            Error::Indeterminate,
            Error::LimitExceeded,
            Error::DatagramsUnavailable,
            Error::TooLarge { max: 1200 },
            Error::Tls("x".into()),
            Error::Io(std::io::Error::other("x")),
            Error::Transport("x".into()),
        ];
        for v in &variants {
            assert!(!v.to_string().is_empty(), "{v:?}");
        }
    }

    #[test]
    fn definite_failures_exclude_the_unknowable_ones() {
        // A typed refusal proves the payload never reached an application.
        for definite in [
            Error::ConnectionLost(LossCause::PeerClosed),
            Error::Rejected,
            Error::UnknownEndpoint,
            Error::Unsupported,
            Error::Canceled,
            Error::NotConnected,
            Error::LimitExceeded,
            Error::DatagramsUnavailable,
            Error::TooLarge { max: 1200 },
        ] {
            assert!(definite.is_definite_failure(), "{definite:?}");
        }
        // `Indeterminate` is unknown by construction, a missing reply says
        // nothing about whether the request had an effect, and an expired
        // transfer may have been read whole before its reset landed.
        assert!(!Error::Indeterminate.is_definite_failure());
        assert!(!Error::NoReply.is_definite_failure());
        assert!(!Error::Expired.is_definite_failure());
    }

    #[test]
    fn io_error_is_the_source() {
        use std::error::Error as _;
        let e = Error::Io(std::io::Error::other("boom"));
        assert!(e.source().is_some());
        assert!(Error::Canceled.source().is_none());
    }

    #[test]
    fn an_outcome_survives_a_round_trip_through_io_error() {
        let back = Error::from(std::io::Error::from(Error::Canceled));
        assert!(matches!(back, Error::Canceled), "{back:?}");
        assert_eq!(
            std::io::Error::from(Error::Expired).kind(),
            std::io::ErrorKind::TimedOut
        );
        assert!(matches!(
            Error::from(std::io::Error::other("x")),
            Error::Io(_)
        ));
    }
}
