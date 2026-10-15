//! Why a value, a frame or a performative was rejected.
//!
//! Two types, split by direction, because the directions disagree about what
//! "not enough bytes" means.
//!
//! * [`DecodeError`] comes from bytes a peer sent. Exactly one of its
//!   variants — [`DecodeError::Incomplete`] — is *not* a protocol violation:
//!   a value may be split across reads, and asking for more octets is the
//!   normal answer. Everything else is a violation, and
//!   [`DecodeError::is_violation`] says so, because the remedy differs: more
//!   reading versus a `close` carrying `amqp:decode-error`.
//! * [`EncodeError`] comes from our own side, and every variant is a bug or a
//!   value the wire cannot carry: a symbol beyond a 32-bit size, a
//!   delivery-tag over 32 octets, a frame beyond the size the partner
//!   negotiated. There is no "incomplete" on the way out.
//!
//! The vocabulary is shared between the type system and the layers above it,
//! so a length rule the decoder enforces is a rule the encoder cannot break
//! either.

use core::fmt;

use crate::codes;

/// Why bytes could not be decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// More octets are needed. `needed` is the smallest number of *further*
    /// octets that could complete what has been read so far — a lower bound,
    /// not a promise, because a size field not yet read can demand more.
    ///
    /// The only variant that is not a violation.
    Incomplete {
        /// Further octets required before another attempt can make progress.
        needed: usize,
    },

    /// A constructor octet this codec does not implement. Part 1 leaves
    /// unnamed low nibbles open, and a field whose type is unknown is not a
    /// field a decoder may guess at.
    UnknownFormatCode(u8),

    /// `boolean` (`0x56`) carries one octet, and Part 1 gives it exactly two
    /// legal values.
    InvalidBoolean(u8),

    /// A compound or array declared a size that leaves no room for the count
    /// field the same header promises.
    SizeBelowCount {
        /// The constructor that declared it.
        code: u8,
        /// The declared size, in octets.
        size: u32,
    },

    /// A compound's declared size was not exactly consumed by the declared
    /// number of elements: the elements stopped short of it.
    CompoundSizeMismatch {
        /// The constructor that declared it.
        code: u8,
        /// Octets the size field promised for elements.
        declared: u32,
        /// Octets the elements actually occupied.
        used: u32,
    },

    /// A `map` declared an odd number of elements; a map is a sequence of
    /// alternating keys and values, so the count is always even
    /// (Part 1 §1.6.24).
    OddMapCount(u32),

    /// A declared element count is above the caller's bound. Reported from
    /// the count field alone, before a single element is decoded and before
    /// anything is reserved.
    ElementCountExceeded {
        /// What the header declared.
        declared: u32,
        /// The caller's bound.
        cap: u32,
    },

    /// Nesting is deeper than the caller's bound. Reported on the way down,
    /// so the recursion stops at the bound rather than at the stack.
    DepthExceeded {
        /// The caller's bound.
        cap: u32,
    },

    /// A decode declared more zero-width array elements than the caller's
    /// [`Limits::max_zero_width_elements`](crate::Limits::max_zero_width_elements)
    /// allows, summed over every array read so far. Reported from the array
    /// header, before the elements are reserved or built, because an array of
    /// `null` costs no octets per element and is otherwise unbounded by the
    /// input.
    ZeroWidthElementsExceeded {
        /// What the header declared, in zero-width elements.
        declared: u32,
        /// The caller's allowance for one decode.
        cap: u32,
    },

    /// A `string` was not UTF-8.
    InvalidUtf8,

    /// A `symbol` was not ASCII. Part 1 restricts symbolic values to
    /// "ASCII characters" (Part 1 §1.6.21).
    NonAsciiSymbol,

    /// `char` carries a UTF-32BE code point, and not every 32-bit value is
    /// one.
    InvalidChar(u32),

    /// A descriptor that is neither a `symbol` nor a `ulong` — the forms
    /// Part 1 §1.5 reserves. See [`Descriptor`](crate::Descriptor).
    DescriptorNotSymbolicOrNumeric(u8),

    /// An `array` element constructor this codec cannot use: `0x00` nested
    /// inside `0x00`, or a code with no width.
    InvalidArrayConstructor(u8),

    /// An `array` whose declared size left no room for the element
    /// constructor every array carries, empty or not: `array8 size=1 count=0`
    /// is one octet short of the shortest legal array.
    ArrayConstructorMissing,

    /// A described value appeared where the layer above demanded one
    /// particular descriptor, and the descriptor was a different one. The
    /// numeric code is reported where the descriptor was numeric; a symbolic
    /// descriptor reports `None`.
    UnexpectedDescriptor {
        /// What this layer required.
        expected: u64,
        /// What arrived, if it was numeric.
        found: Option<u64>,
    },

    /// A descriptor in a position this codec dispatches on, naming nothing it
    /// knows: a tenth performative, a sixth SASL body, a delivery state from
    /// an extension.
    UnknownComposite {
        /// What was being decoded, as the specification names the family.
        kind: &'static str,
        /// The numeric descriptor, or `None` where the descriptor was a
        /// symbol this codec could not resolve to one.
        descriptor: Option<u64>,
    },

    /// A value of the wrong type in a typed position: a `string` where the
    /// field is a `ulong`, for instance.
    WrongType {
        /// The field or position, as the specification names it.
        field: &'static str,
        /// The constructor that arrived.
        code: u8,
    },

    /// A field the specification marks mandatory was absent — either null or
    /// beyond the end of a trimmed field list.
    MissingMandatoryField {
        /// The composite type, as the specification names it.
        composite: &'static str,
        /// The field, as the specification names it.
        field: &'static str,
    },

    /// A value outside the restriction its type carries: a `delivery-tag`
    /// over 32 octets, a `sasl-code` above 4, a `role` that is not a boolean.
    RestrictionViolated {
        /// The restricted type, as the specification names it.
        restriction: &'static str,
        /// What arrived.
        value: u64,
        /// The largest value the restriction permits.
        limit: u64,
    },

    /// A frame header declaring fewer than the eight octets it occupies.
    /// Part 2 §2.3: "SIZE ... MUST be greater than or equal to 8".
    FrameTooSmall {
        /// What the header declared.
        size: u32,
    },

    /// A frame larger than the partner was told it could send.
    ///
    /// Before `open` has been read the bound is
    /// [`MIN_MAX_FRAME_SIZE`](crate::frame::MIN_MAX_FRAME_SIZE) = 512, which
    /// is the whole reason the bound is an argument: a peer that pipelined a
    /// 4 GiB frame ahead of its `open` must be refused on the header alone.
    FrameTooLarge {
        /// What the header declared.
        size: u32,
        /// The bound in force.
        max: u32,
    },

    /// A frame whose data offset is below two, i.e. which claims its body
    /// starts inside its own fixed header. Part 2 §2.3 makes this malformed.
    DataOffsetTooSmall {
        /// What the header declared, in four-octet words.
        doff: u8,
    },

    /// A frame whose extended header runs past the end of the frame itself.
    ExtendedHeaderOverrunsFrame {
        /// The data offset, in four-octet words.
        doff: u8,
        /// The declared frame size, in octets.
        size: u32,
    },

    /// A frame type this codec does not implement. Part 2 defines `0x00`
    /// (AMQP) and Part 5 defines `0x01` (SASL); nothing else is assigned.
    UnknownFrameType(u8),

    /// Eight octets that are not a protocol header: the first four were not
    /// `AMQP`.
    NotAProtocolHeader,

    /// A protocol header naming a layer this codec does not implement. Part 5
    /// assigns `0` (AMQP), `2` (TLS) and `3` (SASL).
    UnknownProtocolId(u8),

    /// A message section in a position Part 3 §3.2 does not put it in: a
    /// `properties` section after the body, a `header` after an annotation.
    /// The order is fixed, not conventional.
    SectionOutOfOrder {
        /// The section that arrived.
        section: &'static str,
        /// The section it arrived after.
        after: &'static str,
    },

    /// A second copy of a section Part 3 §3.2 permits "zero or one" of.
    DuplicateSection(&'static str),

    /// A body that is not one of the three choices Part 3 §3.2 allows: a
    /// `data` section beside an `amqp-sequence`, or a second `amqp-value`.
    MixedBodySections {
        /// The body form already seen.
        had: &'static str,
        /// The section that cannot join it.
        found: &'static str,
    },

    /// A reassembled message larger than the link's `max-message-size`.
    ///
    /// Checked against the accumulated length *before* the fragment is
    /// appended, which is the only place it can be: `max-frame-size` bounds
    /// one frame, and without this bound a peer can send unboundedly many of
    /// them under one delivery-tag. The peer's answer is
    /// `amqp:link:message-size-exceeded` (Part 2 §2.7.3).
    MessageTooLarge {
        /// What the message would have reached.
        size: u64,
        /// The link's limit.
        max: u64,
    },

    /// A continuation `transfer` whose `delivery-id`, `delivery-tag` or
    /// `message-format` differs from the first frame's.
    ///
    /// Part 2 §2.7.5 lets a continuation omit all three but says they "MUST
    /// NOT be inconsistent" where present, so a difference is a violation
    /// rather than an update.
    ContinuationMismatch {
        /// Which field disagreed.
        field: &'static str,
    },

    /// A `transfer` continuing a delivery when no delivery is in progress,
    /// or starting one while another is incomplete on the same link.
    ///
    /// Part 2 §2.6.14: deliveries on one link MUST NOT interleave.
    InterleavedDelivery,
}

impl DecodeError {
    /// Whether this error means the connection must be torn down.
    ///
    /// [`DecodeError::Incomplete`] is the one answer that means "read more";
    /// every other variant is a violation, for which the remedy is
    /// `close(error=amqp:decode-error)` or, where the frame layer caught it,
    /// `amqp:connection:framing-error` (Part 2 §2.8.15-2.8.16).
    #[must_use]
    pub const fn is_violation(&self) -> bool {
        !matches!(self, Self::Incomplete { .. })
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete { needed } => write!(f, "{needed} more octets needed"),
            Self::UnknownFormatCode(code) => {
                write!(f, "format code 0x{code:02x} is not implemented")
            }
            Self::InvalidBoolean(byte) => {
                write!(f, "boolean carries 0x00 or 0x01, not 0x{byte:02x}")
            }
            Self::SizeBelowCount { code, size } => write!(
                f,
                "{} declared size {size}, too small for its own count field",
                described(*code)
            ),
            Self::CompoundSizeMismatch {
                code,
                declared,
                used,
            } => write!(
                f,
                "{} declared {declared} octets of elements and used {used}",
                described(*code)
            ),
            Self::OddMapCount(count) => write!(f, "map declared an odd count of {count}"),
            Self::ElementCountExceeded { declared, cap } => {
                write!(f, "declared {declared} elements, cap is {cap}")
            }
            Self::DepthExceeded { cap } => write!(f, "nesting deeper than {cap}"),
            Self::ZeroWidthElementsExceeded { declared, cap } => {
                write!(
                    f,
                    "{declared} zero-width array elements exceed the budget of {cap} per decode"
                )
            }
            Self::InvalidUtf8 => f.write_str("string is not UTF-8"),
            Self::NonAsciiSymbol => f.write_str("symbol is not ASCII"),
            Self::InvalidChar(value) => {
                write!(f, "0x{value:08x} is not a Unicode code point")
            }
            Self::DescriptorNotSymbolicOrNumeric(code) => {
                write!(f, "descriptor 0x{code:02x} is neither a symbol nor a ulong")
            }
            Self::InvalidArrayConstructor(code) => {
                write!(f, "0x{code:02x} cannot construct array elements")
            }
            Self::ArrayConstructorMissing => f.write_str("array declared no element constructor"),
            Self::UnexpectedDescriptor { expected, found } => match found {
                Some(found) => write!(
                    f,
                    "expected descriptor 0x{expected:016x}, found 0x{found:016x}"
                ),
                None => write!(
                    f,
                    "expected descriptor 0x{expected:016x}, found a symbolic one"
                ),
            },
            Self::UnknownComposite { kind, descriptor } => match descriptor {
                Some(descriptor) => {
                    write!(f, "0x{descriptor:016x} is not a {kind} this codec knows")
                }
                None => write!(f, "that symbol is not a {kind} this codec knows"),
            },
            Self::WrongType { field, code } => {
                write!(f, "{field} cannot be a {}", described(*code))
            }
            Self::MissingMandatoryField { composite, field } => {
                write!(f, "{composite} requires {field}")
            }
            Self::RestrictionViolated {
                restriction,
                value,
                limit,
            } => write!(f, "{restriction} bounds {value} at {limit}"),
            Self::FrameTooSmall { size } => {
                write!(f, "a frame declared {size} octets, below the 8 it occupies")
            }
            Self::FrameTooLarge { size, max } => {
                write!(f, "a frame declared {size} octets, above the agreed {max}")
            }
            Self::DataOffsetTooSmall { doff } => write!(
                f,
                "a frame put its body at word {doff}, inside its own header"
            ),
            Self::ExtendedHeaderOverrunsFrame { doff, size } => write!(
                f,
                "an extended header reaching word {doff} does not fit {size} octets"
            ),
            Self::UnknownFrameType(kind) => {
                write!(
                    f,
                    "frame type 0x{kind:02x} is not AMQP (0x00) or SASL (0x01)"
                )
            }
            Self::NotAProtocolHeader => f.write_str("eight octets not beginning with AMQP"),
            Self::UnknownProtocolId(id) => {
                write!(f, "protocol id {id} is not AMQP (0), TLS (2) or SASL (3)")
            }
            Self::SectionOutOfOrder { section, after } => {
                write!(f, "a {section} section cannot follow {after}")
            }
            Self::DuplicateSection(section) => {
                write!(f, "a second {section} section")
            }
            Self::MixedBodySections { had, found } => {
                write!(f, "a body of {had} cannot also hold {found}")
            }
            Self::MessageTooLarge { size, max } => {
                write!(
                    f,
                    "a message reaching {size} octets is above the agreed {max}"
                )
            }
            Self::ContinuationMismatch { field } => {
                write!(f, "a continuation transfer changed {field}")
            }
            Self::InterleavedDelivery => f.write_str("deliveries on one link may not interleave"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Why a value could not be encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncodeError {
    /// A `binary`, `string`, `symbol`, list, map or array too large for a
    /// four-octet size field. The wire cannot carry it in any form.
    TooLarge {
        /// The constructor that would have carried it.
        code: u8,
        /// How many octets or elements it had.
        len: usize,
    },

    /// A `symbol` with a non-ASCII octet. Refused on the way out for the same
    /// reason it is refused on the way in.
    NonAsciiSymbol,

    /// An `array` element whose canonical encoding does not begin with the
    /// array's shared constructor.
    ///
    /// An array carries one constructor for all of its elements, so an
    /// element that would be written under a different one cannot go in it:
    /// `ulong` 300 does not fit an array declared `smallulong`. Refused
    /// rather than widened, because widening would change the type the array
    /// declared.
    ArrayElementMismatch {
        /// The array's constructor.
        expected: u8,
        /// The constructor the element would have been written under.
        found: u8,
    },

    /// A value outside the restriction its type carries.
    RestrictionViolated {
        /// The restricted type, as the specification names it.
        restriction: &'static str,
        /// What was offered.
        value: u64,
        /// The largest value the restriction permits.
        limit: u64,
    },

    /// A mandatory field left unset by the caller. Caught here rather than on
    /// the wire, because a peer's answer to a missing mandatory field is to
    /// close the connection.
    MissingMandatoryField {
        /// The composite type, as the specification names it.
        composite: &'static str,
        /// The field, as the specification names it.
        field: &'static str,
    },

    /// A frame larger than the partner said it would accept.
    ///
    /// Part 2 §2.7.1: "a peer MUST NOT send frames larger than its partner
    /// can handle", and the partner's answer is
    /// `amqp:connection:framing-error`. Refused before the octets leave.
    FrameTooLarge {
        /// The whole frame, header included.
        size: usize,
        /// What the partner advertised.
        max: u32,
    },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { code, len } => {
                write!(f, "{len} is beyond what {} can declare", described(*code))
            }
            Self::NonAsciiSymbol => f.write_str("symbol is not ASCII"),
            Self::ArrayElementMismatch { expected, found } => write!(
                f,
                "an array of {} cannot carry a {}",
                described(*expected),
                described(*found)
            ),
            Self::RestrictionViolated {
                restriction,
                value,
                limit,
            } => write!(f, "{restriction} bounds {value} at {limit}"),
            Self::MissingMandatoryField { composite, field } => {
                write!(f, "{composite} requires {field}")
            }
            Self::FrameTooLarge { size, max } => {
                write!(f, "a {size}-octet frame is above the agreed {max}")
            }
        }
    }
}

impl std::error::Error for EncodeError {}

/// A format code as a reader recognizes it: its AMQP name where the code is
/// one this crate implements, the raw octet otherwise.
fn described(code: u8) -> String {
    match codes::name(code) {
        Some(name) => name.to_owned(),
        None => format!("0x{code:02x}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_incomplete_is_not_a_violation() {
        assert!(!DecodeError::Incomplete { needed: 3 }.is_violation());
        assert!(DecodeError::UnknownFormatCode(0x57).is_violation());
        assert!(DecodeError::DepthExceeded { cap: 8 }.is_violation());
        assert!(
            DecodeError::FrameTooLarge {
                size: 4096,
                max: 512
            }
            .is_violation()
        );
    }

    #[test]
    fn messages_name_the_type_rather_than_the_octet() {
        assert_eq!(
            DecodeError::SizeBelowCount {
                code: codes::LIST8,
                size: 0
            }
            .to_string(),
            "list8 declared size 0, too small for its own count field"
        );
        assert_eq!(
            DecodeError::UnknownFormatCode(0x57).to_string(),
            "format code 0x57 is not implemented"
        );
        assert_eq!(
            DecodeError::FrameTooLarge {
                size: 131_072,
                max: 512
            }
            .to_string(),
            "a frame declared 131072 octets, above the agreed 512"
        );
    }
}
