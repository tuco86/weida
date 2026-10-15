//! Why a control line, a header block or a JSON object was rejected.
//!
//! Two types, split by the direction that produces them, because the
//! directions disagree about what "not enough bytes" means.
//!
//! * [`DecodeError`] comes from bytes a peer sent. Exactly one of its
//!   variants — [`DecodeError::Incomplete`] — is *not* a protocol violation:
//!   a control line or a payload may be split across reads, and asking for
//!   more octets is the normal answer. Everything else is a violation, and
//!   [`DecodeError::is_violation`] says so, because the remedy differs: read
//!   again versus drop the connection. NATS has no in-band way to reject one
//!   operation and keep the connection — the server's own answer to a line it
//!   cannot parse is `-ERR 'Parser Error'` followed by a close, so a client
//!   that cannot parse the server's line has nothing softer available either.
//! * [`EncodeError`] comes from our own side, and every variant is a value
//!   the wire cannot carry: a subject with a space in it, which the peer
//!   would read as two arguments; a `-ERR` reason containing the quote that
//!   delimits it; a header name with a colon in it. There is no "incomplete"
//!   on the way out.
//!
//! The vocabulary is shared between the control line, the header block and
//! the JSON scanner, so a rule the decoder enforces is a rule the encoder
//! cannot break either.

use core::fmt;

/// Why bytes could not be decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// More octets are needed. `needed` is the smallest number of *further*
    /// octets that could complete what has been read so far — a lower bound,
    /// not a promise, because a byte count not yet read can demand more.
    ///
    /// The only variant that is not a violation.
    Incomplete {
        /// Further octets required before another attempt can make progress.
        needed: usize,
    },

    /// No `CRLF` appeared within the caller's bound for the line: the
    /// `max_control_line`, or the `max_info_line` for an `INFO`.
    ///
    /// The protocol reference names this one on the wire: the server answers
    /// `-ERR 'Maximum Control Line Exceeded'` when "message destination
    /// subject and reply subject length exceeded the maximum control line
    /// value specified by the `max_control_line` server option".
    ControlLineTooLong {
        /// The caller's bound, in octets before the `CRLF`.
        max: usize,
    },

    /// A bare `CR` or `LF` inside a control line, where only the `CRLF` that
    /// ends it may appear.
    ///
    /// `nats-server`'s parser ends an operation at a bare `LF`, so a line
    /// holding one means the sender and this reader disagree about where the
    /// line ends. Reading it as an ordinary argument octet would produce a
    /// subject this crate's own encoder could not write back.
    ControlLineHasNewline,

    /// The first token of a control line was not one of the twelve verbs.
    ///
    /// The server's counterpart is `-ERR 'Unknown Protocol Operation'`, which
    /// the reference marks unrecoverable.
    UnknownVerb,

    /// A verb arrived with a number of arguments none of its forms has.
    ///
    /// This is the one rule of the NATS control line that cannot be recovered
    /// from context: the optional middle arguments are positional and are
    /// distinguished *only* by how many arguments follow the verb.
    ArgumentCount {
        /// The verb, as the reference spells it.
        verb: &'static str,
        /// How many arguments followed it.
        found: usize,
    },

    /// A byte-count or `max_msgs` argument that was not a run of ASCII
    /// digits. A leading `+` or `-` is included here: the counts are sizes.
    NotANumber {
        /// The verb, as the reference spells it.
        verb: &'static str,
        /// The argument, as the reference names it.
        argument: &'static str,
    },

    /// A byte-count argument with more digits than a `u64` holds. Refused
    /// from the digits alone, before the number means anything.
    NumberTooLarge {
        /// The verb, as the reference spells it.
        verb: &'static str,
        /// The argument, as the reference names it.
        argument: &'static str,
    },

    /// A declared byte count above the caller's `max_payload`.
    ///
    /// Reported from the control line alone, before the payload is looked at
    /// and before anything is reserved. For `HPUB` and `HMSG` the count
    /// checked is the *total*, because headers count within it
    /// (`docs/research/nats.md` §3).
    PayloadTooLarge {
        /// What the control line declared, in octets.
        declared: u64,
        /// The caller's bound.
        cap: u64,
    },

    /// An `HPUB` or `HMSG` whose header count is above its total count. The
    /// payload length is total minus header, so this would be negative.
    HeaderBytesAboveTotal {
        /// The declared header-block size, in octets.
        header: u64,
        /// The declared total size, in octets.
        total: u64,
    },

    /// The two octets at the end of the declared payload length were not
    /// `CRLF`.
    ///
    /// The payload is taken *by length* and the terminator is then required
    /// at exactly the offset the count named; it is never searched for, so a
    /// payload containing `CRLF` cannot split a message early.
    MissingPayloadTerminator,

    /// A header block that does not begin with the `NATS/1.0` version line.
    HeaderVersionMissing,

    /// A version line carrying something after `NATS/1.0` that is not a
    /// three-digit status code, optionally followed by a description
    /// (`NATS/1.0 503`, `NATS/1.0 100 Idle Heartbeat`).
    InvalidHeaderStatus,

    /// A header block that never reached its blank line. The block is
    /// length-delimited by the control line, so this means the declared
    /// header count ended in the middle of the block.
    HeaderBlockNotTerminated,

    /// Octets after the header block's blank line but still inside the
    /// declared header count.
    TrailingHeaderBytes {
        /// How many octets were left over.
        extra: usize,
    },

    /// A header line with no colon in it.
    MalformedHeaderLine,

    /// A header name that was empty or held an octet outside ASCII graphic
    /// `0x21..=0x7e` — a space, a control, or non-ASCII.
    InvalidHeaderName,

    /// A header value that was not UTF-8, or held a C0 control other than
    /// the tab the value may contain.
    InvalidHeaderValue,

    /// More header entries than the caller's bound. Counted as the block is
    /// walked, because a header block declares no entry count of its own.
    TooManyHeaderEntries {
        /// The caller's bound.
        cap: u32,
    },

    /// The argument of `INFO` or `CONNECT` was not a JSON object.
    JsonNotAnObject,

    /// The JSON object ended in the middle of a token.
    JsonTruncated,

    /// An octet where the JSON grammar allows none.
    JsonUnexpected {
        /// Offset within the JSON object.
        at: usize,
    },

    /// A backslash escape this scanner does not accept, or a lone surrogate
    /// in a `\u` pair.
    JsonBadEscape {
        /// Offset within the JSON object.
        at: usize,
    },

    /// A JSON string that was not UTF-8 once its escapes were resolved.
    JsonInvalidUtf8,

    /// A field the protocol reference types as `int` that arrived as a
    /// fraction or with an exponent.
    JsonNotAnInteger {
        /// The field, as the reference names it.
        field: &'static str,
    },

    /// A field the protocol reference types as `int` that was negative or
    /// beyond `u64`. None of `port`, `proto` or `max_payload` has a
    /// meaningful negative value.
    JsonNumberOutOfRange {
        /// The field, as the reference names it.
        field: &'static str,
    },

    /// A known field with a value of the wrong JSON type.
    JsonWrongType {
        /// The field, as the reference names it.
        field: &'static str,
        /// The type the reference gives it.
        expected: &'static str,
    },

    /// A known field that appeared twice. Last-one-wins would make the
    /// meaning of an `INFO` depend on the reader, so a repeat is refused.
    JsonDuplicateField {
        /// The field, as the reference names it.
        field: &'static str,
    },

    /// A value of an unknown field nested deeper than
    /// [`MAX_JSON_DEPTH`](crate::limits::MAX_JSON_DEPTH).
    ///
    /// Only unknown fields can nest at all: every field this crate reads is a
    /// string, an integer, a boolean or an array of strings.
    JsonTooDeep {
        /// The bound.
        cap: u32,
    },

    /// A JSON array longer than the caller's bound, counted as it is read.
    /// A JSON array carries no length ahead of its elements, so the bound
    /// cannot be checked from a header the way a byte count can.
    ArrayTooLong {
        /// The field, as the reference names it.
        field: &'static str,
        /// The caller's bound.
        cap: u32,
    },
}

impl DecodeError {
    /// Whether this error means the connection must be torn down.
    ///
    /// [`DecodeError::Incomplete`] is the one answer that means "read more";
    /// every other variant is a violation. NATS offers no per-operation
    /// rejection: the server's own reply to an unparseable line is `-ERR`
    /// followed by a close, and a client that cannot parse the server has the
    /// same two choices.
    #[must_use]
    pub const fn is_violation(&self) -> bool {
        !matches!(self, Self::Incomplete { .. })
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete { needed } => write!(f, "{needed} more octet(s) needed"),
            Self::ControlLineTooLong { max } => {
                write!(f, "no CRLF within the {max}-octet control line bound")
            }
            Self::ControlLineHasNewline => {
                write!(f, "a bare CR or LF inside a control line")
            }
            Self::UnknownVerb => write!(f, "unknown protocol operation"),
            Self::ArgumentCount { verb, found } => {
                write!(f, "{verb} has no form with {found} argument(s)")
            }
            Self::NotANumber { verb, argument } => {
                write!(f, "{verb}: {argument} is not a decimal number")
            }
            Self::NumberTooLarge { verb, argument } => {
                write!(f, "{verb}: {argument} does not fit in 64 bits")
            }
            Self::PayloadTooLarge { declared, cap } => {
                write!(f, "declared {declared} octets, above the {cap}-octet cap")
            }
            Self::HeaderBytesAboveTotal { header, total } => {
                write!(f, "header count {header} above total count {total}")
            }
            Self::MissingPayloadTerminator => {
                write!(f, "no CRLF at the end of the declared payload")
            }
            Self::HeaderVersionMissing => write!(f, "header block does not start with NATS/1.0"),
            Self::InvalidHeaderStatus => write!(f, "version line status is not three digits"),
            Self::HeaderBlockNotTerminated => write!(f, "header block has no blank line"),
            Self::TrailingHeaderBytes { extra } => {
                write!(f, "{extra} octet(s) after the header block's blank line")
            }
            Self::MalformedHeaderLine => write!(f, "header line without a colon"),
            Self::InvalidHeaderName => write!(f, "header name is empty or not ASCII graphic"),
            Self::InvalidHeaderValue => write!(f, "header value is not printable UTF-8"),
            Self::TooManyHeaderEntries { cap } => {
                write!(f, "more than {cap} header entries")
            }
            Self::JsonNotAnObject => write!(f, "the argument is not a JSON object"),
            Self::JsonTruncated => write!(f, "the JSON object ends mid-token"),
            Self::JsonUnexpected { at } => write!(f, "unexpected octet at JSON offset {at}"),
            Self::JsonBadEscape { at } => write!(f, "bad escape at JSON offset {at}"),
            Self::JsonInvalidUtf8 => write!(f, "a JSON string is not UTF-8"),
            Self::JsonNotAnInteger { field } => write!(f, "{field} is not an integer"),
            Self::JsonNumberOutOfRange { field } => {
                write!(f, "{field} is negative or beyond 64 bits")
            }
            Self::JsonWrongType { field, expected } => {
                write!(f, "{field} is not {expected}")
            }
            Self::JsonDuplicateField { field } => write!(f, "{field} appears twice"),
            Self::JsonTooDeep { cap } => write!(f, "JSON nested deeper than {cap}"),
            Self::ArrayTooLong { field, cap } => write!(f, "{field} has more than {cap} elements"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Why a value could not be encoded.
///
/// Every variant is a value that would produce octets a conforming peer reads
/// as something else, which is why the encoder refuses rather than escapes:
/// the NATS control line has no escaping mechanism at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncodeError {
    /// A subject, `sid` or reply subject that was empty. The grammar has no
    /// place for a zero-length argument: it would vanish between separators.
    EmptyArgument {
        /// The verb, as the reference spells it.
        verb: &'static str,
        /// The argument, as the reference names it.
        argument: &'static str,
    },

    /// A subject, `sid`, queue group or reply subject holding a space, a tab,
    /// a `CR` or an `LF`.
    ///
    /// This is the encoder's half of the argument-count rule: `PUB "a b" 5`
    /// is not a subject with a space, it is `PUB a b 5`, which a peer reads
    /// as subject `a` with reply-to `b`.
    ArgumentNotOneToken {
        /// The verb, as the reference spells it.
        verb: &'static str,
        /// The argument, as the reference names it.
        argument: &'static str,
    },

    /// Raw octets destined for a control line that hold a `CR` or an `LF` —
    /// an `INFO` JSON object or a `-ERR` reason. Either would end the line
    /// early and make the rest of it a second operation.
    EmbeddedNewline {
        /// What was being written.
        field: &'static str,
    },

    /// A header name that was empty or held an octet outside ASCII graphic
    /// `0x21..=0x7e`, including the colon that separates it from its value.
    HeaderNameInvalid,

    /// A header value holding a `CR`, an `LF` or another C0 control.
    HeaderValueInvalid,

    /// A header status outside the three digits the version line has room
    /// for.
    HeaderStatusOutOfRange {
        /// The status that was asked for.
        status: u16,
    },

    /// A header description with no status. The version line's grammar puts
    /// the description after the code, so there is nowhere to write it.
    HeaderDescriptionWithoutStatus,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyArgument { verb, argument } => write!(f, "{verb}: empty {argument}"),
            Self::ArgumentNotOneToken { verb, argument } => {
                write!(f, "{verb}: {argument} holds a separator or a newline")
            }
            Self::EmbeddedNewline { field } => write!(f, "{field} holds a CR or an LF"),
            Self::HeaderNameInvalid => write!(f, "header name is empty or not ASCII graphic"),
            Self::HeaderValueInvalid => write!(f, "header value holds a control character"),
            Self::HeaderStatusOutOfRange { status } => {
                write!(f, "header status {status} does not fit in three digits")
            }
            Self::HeaderDescriptionWithoutStatus => {
                write!(f, "a header description needs a status before it")
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
        for error in [
            DecodeError::UnknownVerb,
            DecodeError::ControlLineTooLong { max: 4096 },
            DecodeError::MissingPayloadTerminator,
            DecodeError::PayloadTooLarge {
                declared: 1,
                cap: 0,
            },
        ] {
            assert!(error.is_violation(), "{error} must be a violation");
        }
    }
}
