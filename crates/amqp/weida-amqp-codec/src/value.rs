//! The Part 1 type system as one Rust type.
//!
//! [`Value`] is every primitive of Part 1 §1.6 plus the three compound kinds
//! and the described type, and it *borrows*: a `binary`, `string` or `symbol`
//! is a slice of the caller's buffer, so the only allocation on the way in is
//! the `Vec` a list, map or array needs for its elements — and that `Vec` is
//! never reserved before the declared count has been checked against
//! [`Limits::max_elements`](crate::Limits::max_elements), and an array of
//! zero-width elements is charged to
//! [`Limits::max_zero_width_elements`](crate::Limits::max_zero_width_elements)
//! before anything is reserved for it. A decoded tree holds at most two
//! `Value`s (48 octets each) per input octet, plus two per unit of that
//! allowance.
//!
//! One value, many encodings, one canonical encoding. `uint` has three forms
//! (`0x70`, `0x52`, `0x43`) and a decoder must read all three; an encoder
//! writes the shortest that fits, which is what makes
//! `decode(encode(v)) == v` hold while `encode(decode(bytes)) == bytes` does
//! not. The asymmetry is the specification's, not this crate's: Part 1 says
//! "the AMQP type system ... allows multiple encodings for the same value"
//! and names none of them preferred.

use crate::codes;

/// A value in the AMQP type system, borrowing its variable-width data from
/// the buffer it was decoded from.
#[derive(Clone, Debug, PartialEq)]
pub enum Value<'a> {
    /// `null`. Also what an absent field of a composite decodes to.
    Null,
    /// `boolean`.
    Boolean(bool),
    /// `ubyte`, an 8-bit unsigned integer.
    Ubyte(u8),
    /// `ushort`, a 16-bit unsigned integer.
    Ushort(u16),
    /// `uint`, a 32-bit unsigned integer.
    Uint(u32),
    /// `ulong`, a 64-bit unsigned integer.
    Ulong(u64),
    /// `byte`, an 8-bit two's-complement integer.
    Byte(i8),
    /// `short`, a 16-bit two's-complement integer.
    Short(i16),
    /// `int`, a 32-bit two's-complement integer.
    Int(i32),
    /// `long`, a 64-bit two's-complement integer.
    Long(i64),
    /// `float`, IEEE 754 binary32.
    Float(f32),
    /// `double`, IEEE 754 binary64.
    Double(f64),
    /// `decimal32`, four octets kept opaque.
    ///
    /// IEEE 754 decimal32 has no Rust counterpart and no arithmetic this
    /// crate needs, so the octets travel unread rather than through a lossy
    /// conversion. The same holds for the 64- and 128-bit forms.
    Decimal32([u8; 4]),
    /// `decimal64`, eight octets kept opaque.
    Decimal64([u8; 8]),
    /// `decimal128`, sixteen octets kept opaque.
    Decimal128([u8; 16]),
    /// `char`, a single Unicode code point (UTF-32BE on the wire).
    Char(char),
    /// `timestamp`, milliseconds since the Unix epoch, signed so that it can
    /// name a moment before it.
    Timestamp(i64),
    /// `uuid`, sixteen octets in the order RFC 4122 prints them.
    Uuid([u8; 16]),
    /// `binary`, borrowed.
    Binary(&'a [u8]),
    /// `string`, UTF-8, borrowed.
    String(&'a str),
    /// `symbol`, ASCII, borrowed.
    Symbol(&'a str),
    /// `list`: a sequence of values of any types.
    List(Vec<Value<'a>>),
    /// `map`: alternating keys and values on the wire, pairs here.
    ///
    /// A `Vec` of pairs and not a hash map, because AMQP maps are ordered on
    /// the wire, permit any value as a key, and are compared by an
    /// application rather than by this codec. Part 1 says a map with a
    /// duplicate key "is invalid"; this crate reports the duplicate rather
    /// than silently dropping one, which a hash map could not do.
    Map(Vec<(Value<'a>, Value<'a>)>),
    /// `array`: a sequence of values that all share one constructor.
    Array(Array<'a>),
    /// A described type: a descriptor and the value it describes.
    ///
    /// Boxed because the variant would otherwise make every `Value` as large
    /// as the largest described value.
    Described(Box<Described<'a>>),
}

impl Value<'_> {
    /// The format code this crate's canonical encoding would give this value.
    ///
    /// Useful for error reporting and for the array constructor: an array's
    /// elements must all encode under one code, so this is the question an
    /// array encoder asks of every element.
    #[must_use]
    pub fn canonical_code(&self) -> u8 {
        match self {
            Self::Null => codes::NULL,
            Self::Boolean(true) => codes::TRUE,
            Self::Boolean(false) => codes::FALSE,
            Self::Ubyte(_) => codes::UBYTE,
            Self::Ushort(_) => codes::USHORT,
            Self::Uint(0) => codes::UINT0,
            Self::Uint(v) if *v <= u32::from(u8::MAX) => codes::SMALLUINT,
            Self::Uint(_) => codes::UINT,
            Self::Ulong(0) => codes::ULONG0,
            Self::Ulong(v) if *v <= u64::from(u8::MAX) => codes::SMALLULONG,
            Self::Ulong(_) => codes::ULONG,
            Self::Byte(_) => codes::BYTE,
            Self::Short(_) => codes::SHORT,
            Self::Int(v) if i8::try_from(*v).is_ok() => codes::SMALLINT,
            Self::Int(_) => codes::INT,
            Self::Long(v) if i8::try_from(*v).is_ok() => codes::SMALLLONG,
            Self::Long(_) => codes::LONG,
            Self::Float(_) => codes::FLOAT,
            Self::Double(_) => codes::DOUBLE,
            Self::Decimal32(_) => codes::DECIMAL32,
            Self::Decimal64(_) => codes::DECIMAL64,
            Self::Decimal128(_) => codes::DECIMAL128,
            Self::Char(_) => codes::CHAR,
            Self::Timestamp(_) => codes::TIMESTAMP,
            Self::Uuid(_) => codes::UUID,
            Self::Binary(b) if b.len() <= usize::from(u8::MAX) => codes::VBIN8,
            Self::Binary(_) => codes::VBIN32,
            Self::String(s) if s.len() <= usize::from(u8::MAX) => codes::STR8,
            Self::String(_) => codes::STR32,
            Self::Symbol(s) if s.len() <= usize::from(u8::MAX) => codes::SYM8,
            Self::Symbol(_) => codes::SYM32,
            Self::List(items) if items.is_empty() => codes::LIST0,
            // Whether a compound fits the one-octet form depends on the
            // encoded size of its elements, which is not known here. The
            // wide code is the safe answer, and the encoder narrows it.
            Self::List(_) => codes::LIST32,
            Self::Map(_) => codes::MAP32,
            Self::Array(_) => codes::ARRAY32,
            Self::Described(_) => codes::DESCRIBED,
        }
    }

    /// Whether this value is `null`.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

/// A descriptor: the name or number that says what a described value means.
///
/// Part 1's grammar permits any value here, and §1.5 then describes exactly
/// two kinds: a symbolic descriptor in reverse-domain form
/// (`"amqp:open:list"`) and a numeric one whose high four octets are an
/// assigned domain and whose low four are a code inside it (`0x0000_0000_0000_0010`
/// for `open`). Every descriptor either specification part defines is one of
/// those two, and every implementation writes the numeric form on the wire
/// because it is eight octets against fourteen.
///
/// **This codec refuses the rest.** A descriptor that is a list, a map or a
/// timestamp is grammatical and meaningless, and accepting it would mean
/// carrying a `Value` in the one position where a decoder must be able to
/// dispatch. The refusal is
/// [`DecodeError::DescriptorNotSymbolicOrNumeric`](crate::DecodeError::DescriptorNotSymbolicOrNumeric)
/// and it is the crate's one deliberate narrowing of the type system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Descriptor<'a> {
    /// A numeric descriptor, as every performative and section on the wire
    /// uses.
    Code(u64),
    /// A symbolic descriptor, as the specification's tables print.
    Symbol(&'a str),
}

impl Descriptor<'_> {
    /// The numeric code, where this descriptor is numeric.
    #[must_use]
    pub const fn code(&self) -> Option<u64> {
        match self {
            Self::Code(code) => Some(*code),
            Self::Symbol(_) => None,
        }
    }

    /// Whether this descriptor is the given numeric code.
    #[must_use]
    pub const fn is(&self, code: u64) -> bool {
        matches!(self, Self::Code(found) if *found == code)
    }

    /// Whether this descriptor is the given numeric code or the symbolic
    /// name assigned alongside it.
    ///
    /// Both forms are legal. Every implementation writes the numeric one
    /// because it is two octets against sixteen, but Part 1 §1.5 assigns the
    /// pair together and a peer may send either, so a decoder that
    /// dispatched on the number alone would refuse a legal frame.
    #[must_use]
    pub fn matches(&self, code: u64, name: &str) -> bool {
        match self {
            Self::Code(found) => *found == code,
            Self::Symbol(found) => *found == name,
        }
    }
}

/// A described type: `0x00`, a descriptor, and the value being described.
#[derive(Clone, Debug, PartialEq)]
pub struct Described<'a> {
    /// What the value means.
    pub descriptor: Descriptor<'a>,
    /// The value itself.
    pub value: Value<'a>,
}

/// An `array`: one constructor, then the untyped data of each element.
///
/// The constructor is part of the value, not an encoding detail, because
/// nothing else can reproduce the octets: an empty array of `symbol` and an
/// empty array of `uint` hold the same zero elements and are different arrays.
#[derive(Clone, Debug, PartialEq)]
pub struct Array<'a> {
    element: ElementKind<'a>,
    items: Vec<Value<'a>>,
}

impl<'a> Array<'a> {
    /// An array of elements that all encode under `element`.
    ///
    /// No check that they do: [`crate::encode::array`] performs it, because
    /// it is the only place that can say what the octets would be.
    #[must_use]
    pub const fn new(element: ElementKind<'a>, items: Vec<Value<'a>>) -> Self {
        Self { element, items }
    }

    /// The shared constructor of every element.
    #[must_use]
    pub const fn element(&self) -> &ElementKind<'a> {
        &self.element
    }

    /// The elements.
    #[must_use]
    pub fn items(&self) -> &[Value<'a>] {
        &self.items
    }

    /// The elements, consumed.
    #[must_use]
    pub fn into_items(self) -> Vec<Value<'a>> {
        self.items
    }
}

/// The constructor shared by every element of an `array`.
///
/// An array element constructor is a format code, optionally preceded by a
/// descriptor — `array` of `described` is how a `multiple` field of described
/// types is written. It is never `0x00` alone and never a code with no width
/// rule, which is what [`DecodeError::InvalidArrayConstructor`](crate::DecodeError::InvalidArrayConstructor)
/// reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElementKind<'a> {
    /// Every element is this primitive type.
    Primitive(u8),
    /// Every element is a described value with this descriptor and this
    /// primitive type.
    Described(Descriptor<'a>, u8),
}

impl ElementKind<'_> {
    /// The primitive format code the untyped element data is read under.
    #[must_use]
    pub const fn code(&self) -> u8 {
        match self {
            Self::Primitive(code) | Self::Described(_, code) => *code,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_code_picks_the_shortest_integer_form() {
        assert_eq!(Value::Uint(0).canonical_code(), codes::UINT0);
        assert_eq!(Value::Uint(255).canonical_code(), codes::SMALLUINT);
        assert_eq!(Value::Uint(256).canonical_code(), codes::UINT);
        assert_eq!(Value::Ulong(0).canonical_code(), codes::ULONG0);
        assert_eq!(Value::Ulong(255).canonical_code(), codes::SMALLULONG);
        assert_eq!(Value::Ulong(256).canonical_code(), codes::ULONG);
        // Signed small forms are signed: -1 fits, 128 does not.
        assert_eq!(Value::Int(-1).canonical_code(), codes::SMALLINT);
        assert_eq!(Value::Int(127).canonical_code(), codes::SMALLINT);
        assert_eq!(Value::Int(128).canonical_code(), codes::INT);
        assert_eq!(Value::Long(-128).canonical_code(), codes::SMALLLONG);
        assert_eq!(Value::Long(128).canonical_code(), codes::LONG);
    }

    #[test]
    fn canonical_code_widens_at_the_one_octet_size_boundary() {
        let short = [0u8; 255];
        let long = [0u8; 256];
        assert_eq!(Value::Binary(&short).canonical_code(), codes::VBIN8);
        assert_eq!(Value::Binary(&long).canonical_code(), codes::VBIN32);
    }

    #[test]
    fn a_descriptor_answers_for_its_own_code_only() {
        assert!(Descriptor::Code(0x10).is(0x10));
        assert!(!Descriptor::Code(0x10).is(0x11));
        assert!(!Descriptor::Symbol("amqp:open:list").is(0x10));
        assert_eq!(Descriptor::Symbol("amqp:open:list").code(), None);
    }
}
