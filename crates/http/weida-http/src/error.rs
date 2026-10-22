//! What can go wrong, in the protocol's words.

use std::fmt;

use weida_http_codec::{DecodeError, EncodeError};

/// The result of every fallible call in this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why an HTTP operation failed.
#[derive(Debug)]
pub enum Error {
    /// The operating system refused a socket operation.
    Io(std::io::Error),
    /// The runtime refused: no reactor, the resolver, a task.
    Runtime(weida_core::Error),
    /// The peer sent octets the codec refused.
    Decode(DecodeError),
    /// A value this side was asked to send cannot be encoded.
    Encode(EncodeError),
    /// The TLS handshake or configuration failed.
    Tls(String),
    /// A step did not finish within its time: `"the name lookup"`,
    /// `"the TCP connect"`, `"the TLS handshake"`, `"the response head"`.
    Timeout {
        /// The step.
        step: &'static str,
    },
    /// The peer broke HTTP.
    Protocol(String),
    /// The peer reset the stream (HTTP/2 RST_STREAM, an HTTP/3 stream reset)
    /// with this error code.
    Reset {
        /// The error code.
        code: u64,
    },
    /// The request was not processed (HTTP/2 GOAWAY or REFUSED_STREAM, the
    /// HTTP/3 equivalents): safe to send again.
    GoingAway,
    /// The connection or the server is closed.
    Closed,
    /// A body is longer than the caller's cap.
    TooLarge {
        /// The cap.
        max: usize,
    },
    /// A feature this library does not offer.
    Unsupported(&'static str),
    /// The caller's options or fields are not usable.
    Configuration(String),
    /// A URL this client cannot request.
    InvalidUrl(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Runtime(e) => write!(f, "runtime error: {e}"),
            Self::Decode(e) => write!(f, "the peer sent a message the codec refused: {e}"),
            Self::Encode(e) => write!(f, "cannot encode the message: {e}"),
            Self::Tls(e) => write!(f, "TLS: {e}"),
            Self::Timeout { step } => write!(f, "{step} did not finish in time"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::Reset { code } => write!(f, "the peer reset the stream with code {code:#x}"),
            Self::GoingAway => {
                f.write_str("the server is going away; the request was not processed")
            }
            Self::Closed => f.write_str("the connection is closed"),
            Self::TooLarge { max } => write!(f, "the body is larger than {max} octets"),
            Self::Unsupported(what) => write!(f, "not supported: {what}"),
            Self::Configuration(e) => write!(f, "configuration: {e}"),
            Self::InvalidUrl(e) => write!(f, "invalid URL {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Runtime(e) => Some(e),
            Self::Decode(e) => Some(e),
            Self::Encode(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<weida_core::Error> for Error {
    fn from(e: weida_core::Error) -> Self {
        Self::Runtime(e)
    }
}

impl From<DecodeError> for Error {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e)
    }
}

impl From<EncodeError> for Error {
    fn from(e: EncodeError) -> Self {
        Self::Encode(e)
    }
}
