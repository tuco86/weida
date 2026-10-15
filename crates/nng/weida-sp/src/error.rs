//! Why a protocol header, a message or a per-protocol header was rejected.
//!
//! Three types, split by the layer that produces them, because the layers
//! disagree about what "not enough bytes" means:
//!
//! * the **protocol header** is 8 octets read once per connection, and a short
//!   read is normal while a wrong octet is fatal - the TCP mapping says the
//!   connection MUST be closed immediately [rfc-tcp §2];
//! * a **message header** is decoded from a byte stream, so incompleteness is
//!   expected and is not a violation;
//! * a **per-protocol header** (the request/survey tag stack, the PAIR v1 hop
//!   count) is decoded from an already-complete message body, where a field
//!   that runs off the end is malformed and never a request for more bytes -
//!   "if the reply is shorter than 32 bits, it is malformed and the endpoint
//!   MUST ignore it" [rfc-reqrep §5].
//!
//! Every type answers `is_violation`. True means the connection is finished:
//! SP has no error frame and no code to carry, so closing is the only remedy
//! it defines [rfc-tcp §2]. `TagError` is the interesting case - a malformed
//! tag stack makes the *message* unusable, and NNG's REP drops the message
//! but keeps the pipe when the stack is merely too deep, because devices
//! legitimately forward deep stacks, while it does close the pipe on a
//! truncated tag [nng-src `rep.c`]. `is_violation` is exactly that split:
//! only [`TagError::Truncated`] is a violation, and what the adapter does
//! with the survivable two is the adapter's, not the codec's.

use std::fmt;

/// Why an 8-octet SP protocol header was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderError {
    /// Fewer than [`HEADER_LEN`](crate::header::HEADER_LEN) octets. Read more
    /// and retry; **not** a violation.
    Incomplete,
    /// The first four octets are not `0x00 'S' 'P' <version>`. The mapping
    /// requires them exactly and requires the connection to be closed when
    /// they differ [rfc-tcp §2].
    BadMagic([u8; 3]),
    /// The version octet is not one this codec speaks. Version 0 is the only
    /// one the mapping defines [rfc-tcp §2].
    UnsupportedVersion(u8),
    /// The two reserved octets are not zero. "If the protocol header from the
    /// peer contains anything else than zeroes in this field, the
    /// implementation MUST close the underlying TCP connection" [rfc-tcp §2].
    ReservedNotZero(u16),
    /// The endpoint type is not one of the SP protocols this codec knows. The
    /// mapping says the value SHOULD NOT be interpreted by the mapping itself
    /// [rfc-tcp §2]; interpreting it is this codec's job, and an unknown one
    /// cannot be paired with a local socket.
    UnknownEndpoint(u16),
}

impl HeaderError {
    /// True if the error is fatal for the connection. Only
    /// [`HeaderError::Incomplete`] is not.
    pub const fn is_violation(self) -> bool {
        !matches!(self, HeaderError::Incomplete)
    }
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeaderError::Incomplete => f.write_str("incomplete SP protocol header"),
            HeaderError::BadMagic(got) => {
                write!(
                    f,
                    "protocol header magic is {got:02x?}, expected [00, 53, 50]"
                )
            }
            HeaderError::UnsupportedVersion(v) => write!(f, "unsupported SP version {v}"),
            HeaderError::ReservedNotZero(r) => {
                write!(f, "reserved field is {r:#06x}, must be zero")
            }
            HeaderError::UnknownEndpoint(t) => write!(f, "unknown SP endpoint type {t:#06x}"),
        }
    }
}

impl std::error::Error for HeaderError {}

/// Why a message was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageError {
    /// Fewer octets than the message needs. Read more and retry; **not** a
    /// violation.
    Incomplete,
    /// The declared body length exceeds the local cap. Reported from the
    /// 64-bit size field alone, **before** anything is allocated: a message
    /// may declare up to 2^64-1 octets [rfc-tcp §3] and the only defence SP
    /// offers is `NNG_OPT_RECVMAXSZ`, which is unlimited by default
    /// [nanomsg-nng §5].
    BodyTooLarge {
        /// Length the peer declared.
        len: u64,
        /// Local cap that was exceeded.
        max: u64,
    },
}

impl MessageError {
    /// True if the error is fatal for the connection.
    ///
    /// `BodyTooLarge` is fatal here, and that is a decision rather than a
    /// reading: NNG *discards* the oversized message and keeps the pipe
    /// [nanomsg-nng §8], which it can do because it owns the transport and can
    /// drain the declared number of octets. A sans-I/O decoder cannot drain
    /// anything - the next message begins after a body this side refused to
    /// read - so it reports the condition as fatal and leaves draining to a
    /// caller that has the socket.
    pub const fn is_violation(self) -> bool {
        !matches!(self, MessageError::Incomplete)
    }
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageError::Incomplete => f.write_str("incomplete SP message"),
            MessageError::BodyTooLarge { len, max } => {
                write!(f, "message body of {len} octets exceeds the limit of {max}")
            }
        }
    }
}

impl std::error::Error for MessageError {}

/// Why a per-protocol header inside a message body was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagError {
    /// The body ended in the middle of a 32-bit tag, or holds fewer octets
    /// than one tag. "If the reply is shorter than 32 bits, it is malformed
    /// and the endpoint MUST ignore it" [rfc-reqrep §5].
    Truncated,
    /// The stack ran past `max_hops` tags without a terminator. The
    /// terminator is the tag with its most significant bit set
    /// [rfc-reqrep §5]; the bound is the local `MAXTTL` [nanomsg-nng §11].
    /// **Not** a connection violation: a device pushes its own peer ID onto
    /// the stack it forwards, so a stack at the bound arrives one tag deeper
    /// at the next hop, and NNG drops such a message and keeps the pipe
    /// [nng-src `rep.c`].
    NoTerminator {
        /// How many tags were read before giving up.
        hops: usize,
        /// The bound that was reached.
        max_hops: usize,
    },
    /// A hop count above what the local `MAXTTL` permits. NNG drops such a
    /// message and deliberately does **not** close the pipe, "because we can
    /// legitimately receive messages with too many hops from devices"
    /// [nng-src `rep.c`].
    TooManyHops {
        /// Hop count carried by the message.
        hops: u32,
        /// The local limit.
        max_hops: u32,
    },
}

impl TagError {
    /// True if the error is fatal for the connection.
    ///
    /// Only [`TagError::Truncated`] is: a body that ends inside a tag cannot
    /// be trusted, and NNG closes the pipe on it [nng-src `rep.c`].
    /// [`TagError::NoTerminator`] and [`TagError::TooManyHops`] are not: the
    /// message is dropped and the pipe survives, which is NNG's own behaviour
    /// for a stack or count that devices legitimately push past the bound.
    pub const fn is_violation(self) -> bool {
        matches!(self, TagError::Truncated)
    }
}

impl fmt::Display for TagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TagError::Truncated => f.write_str("message ended inside a 32-bit tag"),
            TagError::NoTerminator { hops, max_hops } => write!(
                f,
                "no terminating tag after {hops} of at most {max_hops} hops"
            ),
            TagError::TooManyHops { hops, max_hops } => {
                write!(f, "hop count {hops} exceeds the limit of {max_hops}")
            }
        }
    }
}

impl std::error::Error for TagError {}
