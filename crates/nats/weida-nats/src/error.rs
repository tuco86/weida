//! Why an operation failed, in the protocol's own vocabulary where it has
//! one.
//!
//! NATS names very few failures itself. What it does name, it names in
//! `-ERR '<reason>'` — `Authorization Violation`, `Stale Connection`,
//! `Maximum Payload Violation`, `Maximum Control Line Exceeded`,
//! `Invalid Subject` — and those are the names used here rather than a second
//! invented set:
//!
//! * [`Error::Server`] carries an `-ERR` reason through unchanged, because the
//!   server's sentence is the only explanation a client is going to get and
//!   folding it into a category would throw it away.
//! * [`Error::StaleConnection`] is the server's own term for a connection
//!   whose pings went unanswered (`-ERR 'Stale Connection'`), used here for
//!   the *client's* side of the same rule — the client counts unanswered
//!   pings too, and calling it anything else would invent a word for a thing
//!   the protocol has already named.
//! * [`Error::PayloadTooLarge`] and [`Error::ControlLineTooLong`] are the
//!   local halves of `Maximum Payload Violation` and `Maximum Control Line
//!   Exceeded`. They are produced *before the wire*: a publish above
//!   `INFO.max_payload` is a publish the server will refuse and then close
//!   on, so refusing it locally costs one comparison and saves the
//!   connection.
//! * [`Error::NoResponders`] is the `NATS/1.0 503` status message, which is a
//!   distinct outcome from [`Error::RequestTimeout`] and must never be
//!   reported as one: 503 means "nobody was listening, now", and a timeout
//!   means "somebody may well have been".
//!
//! Everything else is either the transport, the codec, or a configuration
//! this client refuses to deliver.

use std::fmt;
use std::io;
use std::time::Duration;

use weida_nats_codec::error::{DecodeError, EncodeError};

/// The result of a `weida-nats` operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a NATS operation failed.
///
/// **Exhaustive on purpose**, as `weida-zmq`'s own error enum is. A
/// `#[non_exhaustive]` enum forces every downstream `match` to carry a
/// wildcard arm, and the one
/// downstream that must not have one is a language binding: `weida-nats-py`
/// writes its exception-class list and its name lookup from a single macro so
/// that a variant added here and not added there is a *compile* error rather
/// than a failure that silently arrives as the base class. Keeping this enum
/// closed is what buys that check; the cost is that adding a variant is a
/// breaking change, which is the honest description of adding a failure a
/// caller may have to handle.
#[derive(Debug)]
pub enum Error {
    /// The transport failed: a refused connect, a reset, a closed socket.
    Io(io::Error),

    /// The runtime or the resolver failed. `weida-core`'s vocabulary, which
    /// is the one `weida-runtime` speaks at its boundary.
    Runtime(weida_core::Error),

    /// The server's octets could not be decoded.
    ///
    /// Every variant except `Incomplete` is a violation from which the
    /// protocol offers no recovery: NATS has no way to reject one operation
    /// and keep the connection, so the connection ends.
    Decode(DecodeError),

    /// Something this client tried to send cannot be encoded: a subject with
    /// a space in it, a header value the decoder would read differently.
    Encode(EncodeError),

    /// The server broke the client protocol in a way the codec cannot name —
    /// the first operation was not `INFO`, or an operation arrived where the
    /// handshake allows none.
    Protocol(String),

    /// `-ERR '<reason>'`, the server's own words.
    ///
    /// Most of these are followed by the server closing the connection: "a
    /// protocol, authorization, or other runtime connection error".
    Server(String),

    /// `INFO.auth_required` is set and no credentials are configured.
    ///
    /// Refused locally rather than sent and rejected, because a `CONNECT`
    /// with no credential form against a server that demands one earns
    /// `-ERR 'Authorization Violation'` and a close, and the client already
    /// knows it has nothing to offer.
    AuthenticationRequired,

    /// Credentials that sign the server's nonce are configured and the server
    /// sent no nonce to sign.
    NonceMissing,

    /// The caller's signer refused, or answered without the public key an
    /// NKey `CONNECT` needs.
    ///
    /// The signing itself is the application's — see
    /// [`Signer`](crate::options::Signer) — so its failures are the
    /// application's words.
    Signature(String),

    /// `INFO.tls_required` is set and this call cannot do TLS because it was
    /// given no `rustls::ClientConfig`.
    ///
    /// Which certificates are valid is the application's decision, so the
    /// only honest answer is to say TLS is required and let the caller come
    /// back through `Connection::connect_tls`. Not a link: that constructor
    /// exists only under the `tls` feature, and this variant is reported by
    /// a build that may not have it.
    TlsRequired,

    /// `INFO.tls_required` is set and this build has no TLS at all.
    ///
    /// Reported plainly rather than continued in the clear: a client that
    /// ignored `tls_required` would put a `CONNECT` — credentials and all —
    /// on an unencrypted socket the server is about to stop reading.
    TlsUnsupported,

    /// The TLS handshake failed, or the server name is not one TLS can
    /// validate against, or the caller supplied a TLS configuration and the
    /// server's `INFO` offers no TLS at all.
    ///
    /// The last is refused before a `CONNECT` is written: a client that asked
    /// for TLS and carried on in the clear because an unauthenticated `INFO`
    /// said it could would hand its credentials to whoever edited that line.
    Tls(String),

    /// A publish above the server's `INFO.max_payload`, refused before
    /// anything was written.
    ///
    /// For a headered publish the declared size is the *total*: "headers
    /// count within the `HPUB` total size and therefore within the server's
    /// accepted message size".
    PayloadTooLarge {
        /// What the operation would have declared, in octets.
        declared: u64,
        /// `INFO.max_payload`, the server's own bound.
        max: u64,
    },

    /// A control line above `max_control_line`, refused before anything was
    /// written.
    ControlLineTooLong {
        /// The line this operation would have written, in octets before its
        /// `CRLF`.
        len: usize,
        /// The bound in force.
        max: usize,
    },

    /// Headers were asked for and the server did not advertise `headers`.
    ///
    /// Sending `HPUB` anyway would earn `-ERR 'Invalid Protocol Operation'`
    /// from a server that does not know the verb.
    HeadersUnsupported,

    /// A request reached a subject with no responder: `NATS/1.0 503`.
    ///
    /// Arrives immediately, not at the end of the timeout, and only where
    /// both `headers` and `no_responders` were negotiated in `CONNECT`.
    NoResponders,

    /// No reply arrived inside the caller's window.
    ///
    /// The window is always the caller's and is always finite: a request API
    /// that can hang is the failure this type exists to make impossible.
    RequestTimeout {
        /// The window that elapsed.
        after: Duration,
    },

    /// Too many pings went unanswered, which is what the server calls a stale
    /// connection.
    StaleConnection {
        /// How many of this client's pings were outstanding.
        unanswered: u32,
        /// The bound that was reached.
        max: u32,
    },

    /// A step of the handshake did not finish inside its deadline.
    ///
    /// The protocol gives no deadline for any of them — not for the `INFO`
    /// that opens the connection — so this one is ours. The deadline is one
    /// budget for the whole handshake, and `step` names the step that was
    /// waiting when it ran out.
    HandshakeTimeout {
        /// Which step, as the reference names it.
        step: &'static str,
    },

    /// The socket accepted no octets for `after` while output was waiting for
    /// it: the peer has stopped reading.
    ///
    /// Fails the connection. A write that never completes would otherwise
    /// hold the connection, everything waiting to publish on it and the
    /// socket for as long as the peer likes; the clock restarts whenever the
    /// socket takes anything, so only no progress at all is a stall.
    WriteTimeout {
        /// How long the socket took nothing.
        after: Duration,
    },

    /// The connection is gone; nothing further can be sent on it.
    ConnectionGone,

    /// The subscription table is full.
    ///
    /// The bound is ours: the protocol bounds the table nowhere, and a client
    /// chooses its own `sid`s, so nothing but the client can stop the table
    /// from growing.
    TooManySubscriptions {
        /// The bound that was reached.
        max: usize,
    },

    /// The inbox has as many requests outstanding as it will hold.
    ///
    /// Also ours, and for the same reason: each pending request is a live map
    /// entry waiting for a reply that may never come.
    TooManyPendingRequests {
        /// The bound that was reached.
        max: usize,
    },

    /// A subject or subscription pattern this client will not put on the
    /// wire.
    InvalidSubject {
        /// The offending subject, lossily readable.
        subject: String,
        /// Which rule it broke.
        why: &'static str,
    },

    /// A configuration this client cannot deliver, refused where it was
    /// configured rather than silently corrected
    /// (`docs/decisions/0013-competitor-libraries.md` §4.4 item 4).
    Configuration(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "transport: {error}"),
            Self::Runtime(error) => write!(f, "runtime: {error}"),
            Self::Decode(error) => write!(f, "decode: {error}"),
            Self::Encode(error) => write!(f, "encode: {error}"),
            Self::Protocol(why) => write!(f, "the server broke the client protocol: {why}"),
            Self::Server(reason) => write!(f, "-ERR '{reason}'"),
            Self::AuthenticationRequired => f.write_str(
                "the server's INFO says auth_required and no credentials are configured",
            ),
            Self::NonceMissing => f.write_str(
                "credentials that sign the server's nonce are configured and INFO carried none",
            ),
            Self::Signature(why) => write!(f, "the caller's nonce signer: {why}"),
            Self::TlsRequired => f.write_str(
                "the server's INFO says tls_required; use Connection::connect_tls, \
                 which takes the rustls::ClientConfig the trust decision needs",
            ),
            Self::TlsUnsupported => f.write_str(
                "the server's INFO says tls_required and this build of weida-nats \
                 has no TLS: build with the `tls` feature",
            ),
            Self::Tls(why) => write!(f, "TLS: {why}"),
            Self::PayloadTooLarge { declared, max } => write!(
                f,
                "{declared} octets is above the server's max_payload of {max}"
            ),
            Self::ControlLineTooLong { len, max } => write!(
                f,
                "a {len}-octet control line is above the {max}-octet bound"
            ),
            Self::HeadersUnsupported => {
                f.write_str("the server's INFO did not advertise headers support")
            }
            Self::NoResponders => {
                f.write_str("no responder was subscribed to the request's subject (NATS/1.0 503)")
            }
            Self::RequestTimeout { after } => {
                write!(f, "no reply within {after:?}")
            }
            Self::StaleConnection { unanswered, max } => write!(
                f,
                "{unanswered} unanswered PING(s) reached the bound of {max}: \
                 the connection is stale"
            ),
            Self::HandshakeTimeout { step } => write!(f, "{step} did not finish in time"),
            Self::WriteTimeout { after } => write!(
                f,
                "the socket accepted no octets for {after:?} while output was waiting: \
                 the peer has stopped reading"
            ),
            Self::ConnectionGone => f.write_str("the connection is gone"),
            Self::TooManySubscriptions { max } => {
                write!(f, "the subscription table is full at {max} entries")
            }
            Self::TooManyPendingRequests { max } => {
                write!(f, "the inbox already holds {max} pending requests")
            }
            Self::InvalidSubject { subject, why } => {
                write!(f, "the subject {subject:?} {why}")
            }
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

    /// The two request outcomes must not read alike: a caller that logged one
    /// where the other happened would be looking for a slow responder that
    /// was never there, or for a missing service that was merely late.
    #[test]
    fn no_responders_and_a_timeout_say_different_things() {
        let no_responders = Error::NoResponders.to_string();
        let timeout = Error::RequestTimeout {
            after: Duration::from_millis(250),
        }
        .to_string();
        assert!(no_responders.contains("503"), "{no_responders}");
        assert!(!timeout.contains("503"), "{timeout}");
        assert!(timeout.contains("250ms"), "{timeout}");
    }

    /// An `-ERR` reason survives into the message unchanged, quotes included,
    /// because the server's sentence is the whole explanation.
    #[test]
    fn a_server_error_keeps_its_reason() {
        assert_eq!(
            Error::Server("Authorization Violation".into()).to_string(),
            "-ERR 'Authorization Violation'"
        );
    }
}
