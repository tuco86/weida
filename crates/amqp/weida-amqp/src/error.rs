//! Why a connection failed, in AMQP's own vocabulary.
//!
//! AMQP names its failures with *error conditions* — symbols such as
//! `amqp:decode-error` and `amqp:connection:framing-error` (Part 2 §2.8.15) —
//! and this enum keeps them rather than inventing a second set. Three of its
//! variants exist only because the protocol's negotiation has failure modes
//! that look alike from the outside and must not:
//!
//! * [`Error::SecurityLayerRequired`] — we asked for bare AMQP and the server
//!   answered with a different protocol-id. Part 2 §2.2 is explicit that
//!   "highest supported version" does not apply to the protocol-id, so a
//!   `%d3` reply to a `%d0` request is not a version mismatch and not a
//!   transport error: it is the server saying a security layer is mandatory,
//!   and a client that retried blind would loop.
//! * [`Error::VersionMismatch`] — the same eight octets carrying a version
//!   the server chose instead. AMQP 0-9-1 shares this port and this prefix,
//!   so `AMQP` followed by `0 0 9 1` lands here and is worth saying plainly.
//! * [`Error::Sasl`] — one of the five outcome codes. `sys-temp` is the one
//!   the specification calls transient while saying nothing about how long to
//!   wait, so the code is reported and the waiting is the caller's.

use std::fmt;
use std::io;
use std::time::Duration;

use weida_amqp_codec::protocol_header::{ProtocolId, Version};
use weida_amqp_codec::{DecodeError, EncodeError, SaslCode};

/// The result of a `weida-amqp` operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An error condition and the text that came with it, owned.
///
/// The codec's `AmqpError` borrows from the frame it arrived in, and a frame
/// does not outlive the read that produced it. An error that a caller is
/// going to look at after the connection is gone has to own its strings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Condition {
    /// The `symbol` naming what went wrong — one of
    /// [`weida_amqp_codec::types::condition`]'s values, or a domain's own.
    pub condition: String,
    /// Loggable text, never a decision.
    pub description: Option<String>,
}

impl Condition {
    /// A condition with no description.
    #[must_use]
    pub fn new(condition: impl Into<String>) -> Self {
        Self {
            condition: condition.into(),
            description: None,
        }
    }

    /// A condition with a description.
    #[must_use]
    pub fn described(condition: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            condition: condition.into(),
            description: Some(description.into()),
        }
    }

    /// Copies a borrowed codec error into an owned one.
    #[must_use]
    pub fn from_codec(error: &weida_amqp_codec::AmqpError<'_>) -> Self {
        Self {
            condition: error.condition.to_owned(),
            description: error.description.map(str::to_owned),
        }
    }

    /// The borrowed form, for putting this condition back on the wire.
    #[must_use]
    pub fn as_codec(&self) -> weida_amqp_codec::AmqpError<'_> {
        weida_amqp_codec::AmqpError {
            condition: &self.condition,
            description: self.description.as_deref(),
            info: None,
        }
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.description {
            Some(description) => write!(f, "{}: {description}", self.condition),
            None => f.write_str(&self.condition),
        }
    }
}

/// Why an AMQP operation failed.
///
/// Deliberately **not** `#[non_exhaustive]`. A binding or a forwarder that
/// maps every variant onto its own vocabulary - `weida-amqp-py` turns each
/// one into an exception class - can only be checked for drift if a variant
/// added here is a compile error there; a wildcard arm, which
/// `#[non_exhaustive]` forces on every downstream match, would swallow the
/// new failure and report it as the base class instead. The cost is that
/// adding a variant is a breaking change, which is the honest description of
/// adding a failure a caller may have to handle.
#[derive(Debug)]
pub enum Error {
    /// The transport failed: a refused connect, a reset, a closed socket.
    Io(io::Error),

    /// The runtime or the resolver failed. `weida-core`'s vocabulary, which
    /// is the one `weida-runtime` speaks at its boundary.
    Runtime(weida_core::Error),

    /// A peer's octets could not be decoded. The remedy is a `close`: with
    /// `amqp:connection:framing-error` where the frame itself was malformed
    /// (a size beyond what was agreed, a header that cannot be one) and with
    /// `amqp:decode-error` where the frame was sound and its body was not;
    /// this client sends it before reporting (see [`Error::wire_condition`]).
    Decode(DecodeError),

    /// Something this client tried to send cannot be encoded: a frame above
    /// the negotiated `max-frame-size`, a delivery-tag over 32 octets.
    Encode(EncodeError),

    /// The eight octets the peer answered with were not a protocol header, or
    /// named a layer this client does not implement.
    BadProtocolHeader(DecodeError),

    /// The server requires a different layer than the one we asked for.
    ///
    /// Part 2 §2.2: a server requiring SASL answers a `%d0` request with
    /// `%d3` and closes. Reported rather than retried, because retrying is a
    /// policy decision — a client configured with no credentials cannot
    /// satisfy a mandatory SASL layer and should say so once.
    SecurityLayerRequired {
        /// What we sent.
        requested: ProtocolId,
        /// What the server answered with.
        offered: ProtocolId,
    },

    /// The server answered with a version other than 1.0.0.
    VersionMismatch {
        /// What the server said it speaks. `0.9.1` here means the peer is an
        /// AMQP 0-9-1 server on the shared port 5672.
        offered: Version,
    },

    /// The SASL dialog ended in something other than `ok`.
    Sasl {
        /// One of the five codes of Part 5 §5.3.3.6.
        code: SaslCode,
        /// The mechanism that was tried.
        mechanism: String,
    },

    /// The server offered no mechanism this client can speak.
    NoSharedSaslMechanism {
        /// What the server advertised, in its own preference order.
        offered: Vec<String>,
    },

    /// TLS was required and this build has no TLS, or the handshake failed.
    Tls(String),

    /// The peer closed the connection, with a condition if it gave one.
    Closed(Option<Condition>),

    /// This client closed the connection because of `condition`.
    Local(Condition),

    /// No frame arrived within the local idle threshold.
    ///
    /// Part 2 §2.4.5 says a peer SHOULD close with an error explaining why
    /// and MAY then drop the socket, which is what this client does — and
    /// the caller sees this variant rather than a bare transport error,
    /// because "the peer went quiet" and "the socket broke" call for
    /// different operational responses.
    IdleTimeout {
        /// The threshold that expired, in milliseconds.
        after_ms: u32,
    },

    /// The socket took no octets at all for `after` while output was waiting
    /// for it: the peer has stopped reading.
    ///
    /// Nothing is written in answer, because writing is exactly what does not
    /// work. The clock restarts whenever the socket takes anything, so a peer
    /// that reads slowly is not a stalled one.
    WriteTimeout {
        /// How long the socket took nothing.
        after: Duration,
    },

    /// A step of the handshake did not finish inside its deadline.
    HandshakeTimeout {
        /// Which step, as the specification names it.
        step: &'static str,
    },

    /// The connection is gone; nothing further can be sent on it.
    ConnectionGone,

    /// A configuration this client cannot deliver, refused where it was
    /// configured rather than silently corrected
    /// (`docs/decisions/0013-competitor-libraries.md` §4.4 item 4).
    Configuration(String),
}

impl Error {
    /// The error condition this client would put on the wire for this error.
    ///
    /// `None` where there is nothing to send — the transport is already gone,
    /// or the peer is the one that closed.
    #[must_use]
    pub fn wire_condition(&self) -> Option<&'static str> {
        use weida_amqp_codec::types::condition as c;
        Some(match self {
            Self::Decode(
                DecodeError::FrameTooSmall { .. }
                | DecodeError::FrameTooLarge { .. }
                | DecodeError::DataOffsetTooSmall { .. }
                | DecodeError::ExtendedHeaderOverrunsFrame { .. }
                | DecodeError::UnknownFrameType(_),
            ) => c::CONNECTION_FRAMING_ERROR,
            Self::Decode(_) | Self::BadProtocolHeader(_) => c::DECODE_ERROR,
            Self::Encode(_) => c::FRAME_SIZE_TOO_SMALL,
            Self::IdleTimeout { .. } => c::CONNECTION_FORCED,
            Self::Local(_) => return None,
            _ => return None,
        })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "transport: {error}"),
            Self::Runtime(error) => write!(f, "runtime: {error}"),
            Self::Decode(error) => write!(f, "decode: {error}"),
            Self::Encode(error) => write!(f, "encode: {error}"),
            Self::BadProtocolHeader(error) => write!(f, "protocol header: {error}"),
            Self::SecurityLayerRequired { requested, offered } => write!(
                f,
                "the server answered protocol-id {} to our {}: a security layer is mandatory",
                offered.octet(),
                requested.octet()
            ),
            Self::VersionMismatch { offered } => {
                write!(f, "the server speaks AMQP {offered}, not 1.0.0")
            }
            Self::Sasl { code, mechanism } => {
                write!(f, "SASL {mechanism} ended with code {}", code.octet())
            }
            Self::NoSharedSaslMechanism { offered } => write!(
                f,
                "the server offered {} and this client speaks none of them",
                offered.join(", ")
            ),
            Self::Tls(why) => write!(f, "TLS: {why}"),
            Self::Closed(Some(condition)) => write!(f, "the peer closed: {condition}"),
            Self::Closed(None) => f.write_str("the peer closed without an error"),
            Self::Local(condition) => write!(f, "closed locally: {condition}"),
            Self::IdleTimeout { after_ms } => {
                write!(f, "no frame within the {after_ms} ms idle threshold")
            }
            Self::WriteTimeout { after } => write!(
                f,
                "the socket accepted no octets for {after:?} while output was waiting: \
                 the peer has stopped reading"
            ),
            Self::HandshakeTimeout { step } => write!(f, "{step} did not finish in time"),
            Self::ConnectionGone => f.write_str("the connection is gone"),
            Self::Configuration(why) => write!(f, "configuration: {why}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::Encode(error) => Some(error),
            Self::BadProtocolHeader(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<weida_core::Error> for Error {
    fn from(error: weida_core::Error) -> Self {
        Self::Runtime(error)
    }
}

impl From<DecodeError> for Error {
    fn from(error: DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl From<EncodeError> for Error {
    fn from(error: EncodeError) -> Self {
        Self::Encode(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_amqp_codec::types::condition;

    #[test]
    fn a_protocol_id_mismatch_is_not_a_version_mismatch() {
        // The two arrive in the same eight octets and mean different things:
        // one says "authenticate first", the other says "I am not a 1.0
        // server". A client that folded them would retry the wrong one.
        let layer = Error::SecurityLayerRequired {
            requested: ProtocolId::Amqp,
            offered: ProtocolId::Sasl,
        };
        assert!(layer.to_string().contains("security layer is mandatory"));
        let version = Error::VersionMismatch {
            offered: Version {
                major: 0,
                minor: 9,
                revision: 1,
            },
        };
        assert_eq!(
            version.to_string(),
            "the server speaks AMQP 0.9.1, not 1.0.0"
        );
    }

    #[test]
    fn a_condition_round_trips_between_owned_and_borrowed() {
        let owned = Condition::described(condition::LINK_STOLEN, "attached elsewhere");
        let borrowed = owned.as_codec();
        assert_eq!(borrowed.condition, condition::LINK_STOLEN);
        assert_eq!(Condition::from_codec(&borrowed), owned);
        assert_eq!(owned.to_string(), "amqp:link:stolen: attached elsewhere");
    }

    #[test]
    fn a_decode_failure_names_the_condition_it_would_send() {
        let error = Error::Decode(DecodeError::OddMapCount(3));
        assert_eq!(error.wire_condition(), Some(condition::DECODE_ERROR));
        // A peer-initiated close has nothing to send back: `close` is the
        // last thing ever written, and the peer already wrote it.
        assert_eq!(Error::Closed(None).wire_condition(), None);
    }

    #[test]
    fn a_malformed_frame_is_a_framing_error_and_a_malformed_body_a_decode_error() {
        for malformed in [
            DecodeError::FrameTooLarge {
                size: 4096,
                max: 512,
            },
            DecodeError::FrameTooSmall { size: 3 },
            DecodeError::DataOffsetTooSmall { doff: 1 },
            DecodeError::UnknownFrameType(9),
        ] {
            assert_eq!(
                Error::Decode(malformed).wire_condition(),
                Some(condition::CONNECTION_FRAMING_ERROR),
                "{malformed}"
            );
        }
        assert_eq!(
            Error::Decode(DecodeError::InvalidUtf8).wire_condition(),
            Some(condition::DECODE_ERROR)
        );
    }

    #[test]
    fn a_stalled_write_has_nothing_to_send() {
        let stalled = Error::WriteTimeout {
            after: Duration::from_secs(10),
        };
        assert_eq!(
            stalled.wire_condition(),
            None,
            "the peer is not reading, so a close would go nowhere"
        );
        assert!(stalled.to_string().contains("stopped reading"), "{stalled}");
    }
}
