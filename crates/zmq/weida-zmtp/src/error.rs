//! Why a greeting, a frame, a command or a piece of Z85 was rejected.
//!
//! Four types, split by the layer that produces them, because the layers
//! disagree about what "not enough bytes" means:
//!
//! * a **greeting** is 64 octets read once per connection, and a short read is
//!   normal;
//! * a **frame header** is decoded from a byte stream, so incompleteness is
//!   expected and is not a protocol violation;
//! * a **command body** is decoded from an already-complete frame body, where a
//!   field that runs off the end is a violation and never a request for more
//!   bytes. [`CommandError`] speaks for NULL and PLAIN, [`CurveError`] for
//!   CURVE, whose commands are fixed-size or have a minimum size and whose
//!   names collide with the other mechanisms';
//! * [`Z85Error`] is not a wire error at all: it is a key or a printable blob
//!   that a human or a configuration file got wrong, and it has no
//!   connection to close.
//!
//! Every wire type answers [`is_violation`](FrameError::is_violation): true
//! means close the connection, which is the only remedy ZMTP defines
//! ([37/ZMTP](https://rfc.zeromq.org/spec/37/), "Error Handling").

use std::fmt;

use crate::greeting::{Mechanism, Version};

/// Why a greeting was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GreetingError {
    /// Fewer octets than the form being parsed needs. Read more and retry;
    /// **not** a protocol violation.
    Incomplete,
    /// The signature is wrong: octet 0 must be `0xFF` and octet 9 `0x7F`. The
    /// eight padding octets between them are never checked.
    BadSignature {
        /// Offset of the octet that was wrong, 0 or 9.
        at: usize,
        /// What was there instead.
        found: u8,
    },
    /// The peer speaks a version this codec cannot talk to. ZMTP requires
    /// accepting anything at or above 3.1 and permits, but does not require,
    /// downgrading below it; this codec implements 3.1 only and so closes.
    UnsupportedVersion(Version),
    /// A mechanism name contains an octet the grammar does not allow, or a
    /// non-zero octet after the null padding began.
    BadMechanismName,
    /// The peer announced a different mechanism than we did. ZMTP security is
    /// "assertive": there is no negotiation, and a mismatch closes.
    MechanismMismatch {
        /// What the peer announced.
        theirs: Mechanism,
    },
    /// The `as-server` octet was neither 0 nor 1.
    BadAsServer(u8),
    /// `as-server` was set while the mechanism is NULL, where the
    /// specification requires zero.
    AsServerUnderNull,
}

impl GreetingError {
    /// True if the error is fatal for the connection. Only
    /// [`GreetingError::Incomplete`] is not.
    pub const fn is_violation(self) -> bool {
        !matches!(self, GreetingError::Incomplete)
    }
}

impl fmt::Display for GreetingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GreetingError::Incomplete => f.write_str("incomplete greeting"),
            GreetingError::BadSignature { at, found } => {
                let want = if *at == 0 { 0xFF } else { 0x7F };
                write!(
                    f,
                    "bad signature octet {at}: {found:#04x}, expected {want:#04x}"
                )
            }
            GreetingError::UnsupportedVersion(v) => {
                write!(
                    f,
                    "peer speaks ZMTP {v}, which this codec cannot downgrade to"
                )
            }
            GreetingError::BadMechanismName => f.write_str("malformed mechanism name"),
            GreetingError::MechanismMismatch { theirs } => {
                write!(f, "peer announced mechanism {theirs}, which is not ours")
            }
            GreetingError::BadAsServer(b) => write!(f, "as-server octet is {b}, expected 0 or 1"),
            GreetingError::AsServerUnderNull => {
                f.write_str("as-server is set, but the NULL mechanism requires zero")
            }
        }
    }
}

impl std::error::Error for GreetingError {}

/// Why a frame header was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// Fewer octets than the frame needs. Read more and retry; **not** a
    /// protocol violation.
    Incomplete,
    /// Bits 7-3 of the flags octet are reserved and MUST be zero.
    ReservedFlags(u8),
    /// The MORE flag was set on a command frame, where it "SHALL be zero".
    MoreOnCommand,
    /// The declared body length exceeds the local cap. Reported **before** any
    /// allocation: a long frame may declare up to 2^63-1 octets, and a cap is
    /// the only defence ZMTP offers, `ZMQ_MAXMSGSIZE` being unlimited by
    /// default.
    BodyTooLarge {
        /// Length the peer declared.
        len: u64,
        /// Local cap that was exceeded.
        max: u64,
    },
}

impl FrameError {
    /// True if the error is fatal for the connection. Only
    /// [`FrameError::Incomplete`] is not.
    ///
    /// `BodyTooLarge` is fatal on purpose and matches libzmq, where exceeding
    /// `ZMQ_MAXMSGSIZE` disconnects the peer rather than skipping the message:
    /// a frame whose body is not read cannot be skipped, because the next
    /// frame starts after a body this side refused to buffer.
    pub const fn is_violation(self) -> bool {
        !matches!(self, FrameError::Incomplete)
    }
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Incomplete => f.write_str("incomplete frame"),
            FrameError::ReservedFlags(b) => {
                write!(f, "reserved flag bits set in flags octet {b:#04x}")
            }
            FrameError::MoreOnCommand => f.write_str("MORE set on a command frame"),
            FrameError::BodyTooLarge { len, max } => {
                write!(f, "frame body of {len} octets exceeds the limit of {max}")
            }
        }
    }
}

impl std::error::Error for FrameError {}

/// Why a command body was rejected, on the way in or on the way out.
///
/// One vocabulary for both directions: every length rule the decoder enforces
/// is a rule the encoder must not break either, and a single set of names keeps
/// the two halves from drifting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandError {
    /// A length-prefixed field runs past the end of the body. The body is
    /// already complete when a command is decoded, so this is a violation and
    /// never a request for more bytes.
    Truncated,
    /// The command name is empty, or contains an octet other than a letter.
    BadName,
    /// A command name this codec does not implement. `JOIN` and `LEAVE` are
    /// the known omission: they belong to RADIO/DISH, which `weida-zmq` does
    /// not carry.
    UnknownName,
    /// A metadata property name is empty or contains an octet the grammar does
    /// not allow.
    BadPropertyName,
    /// A metadata value is longer than the four-octet size field can describe.
    ValueTooLong(usize),
    /// A `PING`/`PONG` context exceeds 16 octets.
    ContextTooLong(usize),
    /// An `ERROR` reason exceeds 255 octets.
    ReasonTooLong(usize),
    /// An `ERROR` reason contains a non-printable octet.
    ReasonNotPrintable,
    /// A PLAIN `HELLO` username or password exceeds 255 octets, which its
    /// own one-octet length field cannot describe.
    FieldTooLong(usize),
}

impl CommandError {
    /// True if the error is fatal for the connection. Every command error is:
    /// a command body arrives whole, so there is nothing to wait for.
    pub const fn is_violation(self) -> bool {
        true
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Truncated => f.write_str("command body ends inside a field"),
            CommandError::BadName => f.write_str("malformed command name"),
            CommandError::UnknownName => f.write_str("unimplemented command name"),
            CommandError::BadPropertyName => f.write_str("malformed metadata property name"),
            CommandError::ValueTooLong(n) => {
                write!(f, "metadata value of {n} octets exceeds 2147483647")
            }
            CommandError::ContextTooLong(n) => {
                write!(f, "ping context of {n} octets exceeds 16")
            }
            CommandError::ReasonTooLong(n) => write!(f, "error reason of {n} octets exceeds 255"),
            CommandError::ReasonNotPrintable => {
                f.write_str("error reason contains a non-printable octet")
            }
            CommandError::FieldTooLong(n) => {
                write!(f, "PLAIN field of {n} octets exceeds 255")
            }
        }
    }
}

impl std::error::Error for CommandError {}

/// Why a CURVE command body was rejected, on the way in or on the way out.
///
/// CURVE's commands are laid out by count rather than by delimiter: `HELLO`
/// is 200 octets and nothing else, `WELCOME` is 168, and the three variable
/// ones have a minimum below which their boxes cannot exist. So the errors
/// are counts too, and they carry the command name because the same names
/// belong to NULL and PLAIN with other bodies entirely.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CurveError {
    /// The command name is empty, runs past the body, or contains an octet
    /// other than a letter.
    BadName,
    /// A command name CURVE does not define. `ERROR` is the notable one: it
    /// is shared with the other mechanisms and stays with [`CommandError`].
    UnknownName,
    /// A fixed-size command of the wrong size. The interesting instance is a
    /// 198-octet `HELLO`, which is what 26/CURVEZMQ's prose describes and its
    /// own grammar and totals contradict.
    BadLength {
        /// The command name, as it was on the wire.
        name: &'static str,
        /// The only length this command has.
        expected: usize,
        /// What arrived.
        actual: usize,
    },
    /// A variable-size command, or an opened `INITIATE` box, below the
    /// minimum its own fields require.
    TooShort {
        /// The command name, as it was on the wire.
        name: &'static str,
        /// The smallest legal length.
        minimum: usize,
        /// What arrived.
        actual: usize,
    },
    /// `hello-version` is not 1.0, the only version 26/CURVEZMQ defines.
    UnsupportedVersion(u8, u8),
    /// The metadata inside an opened `INITIATE` or `READY` box is malformed.
    Metadata(CommandError),
}

impl CurveError {
    /// True if the error is fatal for the connection. Every CURVE error is:
    /// a command body arrives whole, and a handshake command that does not
    /// parse cannot be retried.
    pub const fn is_violation(self) -> bool {
        true
    }
}

impl fmt::Display for CurveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CurveError::BadName => f.write_str("malformed command name"),
            CurveError::UnknownName => f.write_str("not a CURVE command name"),
            CurveError::BadLength {
                name,
                expected,
                actual,
            } => write!(f, "CURVE {name} is {expected} octets, not {actual}"),
            CurveError::TooShort {
                name,
                minimum,
                actual,
            } => write!(
                f,
                "CURVE {name} of {actual} octets is below its minimum of {minimum}"
            ),
            CurveError::UnsupportedVersion(major, minor) => {
                write!(f, "CURVE version {major}.{minor} is not 1.0")
            }
            CurveError::Metadata(e) => write!(f, "CURVE metadata: {e}"),
        }
    }
}

impl std::error::Error for CurveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CurveError::Metadata(e) => Some(e),
            _ => None,
        }
    }
}

/// Why a Z85 conversion failed.
///
/// Not a protocol error: Z85 is how a CURVE key is written down in a
/// configuration file or on a command line, so these are a person's or a
/// file's mistakes and the remedy is to say which octet or which length was
/// wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Z85Error {
    /// The binary length is not a multiple of four, which the encoding
    /// cannot express: it has no padding form.
    BinaryLength(usize),
    /// The text length is not a multiple of five, or not the 40 characters a
    /// CURVE key must be.
    TextLength(usize),
    /// An octet outside the 85-character alphabet. The alphabet excludes the
    /// quote and backslash characters on purpose, so that a key survives
    /// being pasted into source code.
    NotZ85(u8),
    /// Five legal characters whose base-85 value does not fit in four
    /// octets - `#####` is 4437053124, above `u32::MAX`.
    Overflow,
}

impl fmt::Display for Z85Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Z85Error::BinaryLength(n) => {
                write!(f, "{n} octets is not a multiple of 4")
            }
            Z85Error::TextLength(n) => {
                write!(f, "{n} characters is not a Z85 length here")
            }
            Z85Error::NotZ85(b) => {
                write!(f, "octet {b:#04X} is not a Z85 character")
            }
            Z85Error::Overflow => f.write_str("a Z85 group exceeds four octets"),
        }
    }
}

impl std::error::Error for Z85Error {}
