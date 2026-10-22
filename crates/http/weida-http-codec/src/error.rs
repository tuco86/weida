//! Why bytes were rejected, split by the direction that produced them.
//!
//! * [`DecodeError`] comes from bytes a peer sent. Exactly one variant,
//!   [`DecodeError::Incomplete`], is *not* a violation: a head, a chunk line
//!   or a frame may be split across reads, and asking for more octets is the
//!   normal answer. [`DecodeError::is_violation`] says which is which, because
//!   the remedy differs: read again, or refuse the message.
//! * [`EncodeError`] comes from our own side: a value the wire cannot carry,
//!   such as a field value holding a line feed, which a recipient would read as
//!   the start of another field. The encoders refuse rather than escape,
//!   because HTTP has no escaping mechanism for either.
//!
//! The `what` of every variant names the rule that was broken in the words of
//! the RFC, so a library above can map it onto a status code or an error code
//! without parsing a message.

use core::fmt;

/// Why bytes could not be decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// More input is needed; `needed` is a lower bound of further octets.
    ///
    /// Every proper prefix of a valid input decodes to this variant, so a
    /// reader that gets it reads more and calls again with the longer buffer.
    Incomplete {
        /// At least this many more octets are needed.
        needed: usize,
    },
    /// A declared or searched size exceeds the caller's cap.
    TooLarge {
        /// What was too large: `"head"`, `"field count"`, `"chunk line"`,
        /// `"field section"`, `"frame"`, `"string"`.
        what: &'static str,
        /// The size the peer declared, or the octets searched without an end.
        declared: u64,
        /// The cap it exceeded.
        cap: u64,
    },
    /// The input breaks a rule of the protocol.
    Malformed {
        /// The rule, in the RFC's words: `"whitespace before the colon"`,
        /// `"obsolete line folding"`, `"version"`, `"transfer coding"`.
        what: &'static str,
    },
}

impl DecodeError {
    /// `true` for everything but [`DecodeError::Incomplete`]: the input is
    /// wrong, and reading more will not fix it.
    #[must_use]
    pub const fn is_violation(&self) -> bool {
        !matches!(self, Self::Incomplete { .. })
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete { needed } => write!(f, "incomplete: at least {needed} more octets"),
            Self::TooLarge {
                what,
                declared,
                cap,
            } => write!(
                f,
                "{what} too large: {declared} octets over the cap of {cap}"
            ),
            Self::Malformed { what } => write!(f, "malformed: {what}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Why a value could not be encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncodeError {
    /// A field name, or a method, that is not a token (RFC 9110 §5.1, §9.1).
    InvalidName {
        /// What was invalid: `"field name"`, `"method"`.
        what: &'static str,
    },
    /// A value the wire cannot carry.
    InvalidValue {
        /// What was invalid: `"field value"`, `"request target"`, `"status"`,
        /// `"reason phrase"`, `"version"`.
        what: &'static str,
    },
    /// A value larger than the encoding can express.
    TooLarge {
        /// What was too large.
        what: &'static str,
        /// Its length or value.
        len: u64,
        /// The largest the encoding allows.
        cap: u64,
    },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName { what } => write!(f, "invalid {what}: not a token"),
            Self::InvalidValue { what } => write!(f, "invalid {what}"),
            Self::TooLarge { what, len, cap } => {
                write!(f, "{what} too large: {len} over the limit of {cap}")
            }
        }
    }
}

impl std::error::Error for EncodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_incomplete_is_not_a_violation() {
        assert!(!DecodeError::Incomplete { needed: 1 }.is_violation());
        assert!(DecodeError::Malformed { what: "version" }.is_violation());
        assert!(
            DecodeError::TooLarge {
                what: "head",
                declared: 2,
                cap: 1
            }
            .is_violation()
        );
    }
}
