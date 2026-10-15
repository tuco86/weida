//! Bytes to [`Value`], in every form Part 1 permits.
//!
//! One entry point, [`value`], plus the pieces the typed layers above need:
//! [`described`] for a described restricted type such as `data`, and
//! [`composite`] for a performative, a section or a delivery state — a
//! described `list` walked by position without a `Vec` of it being built.
//!
//! # Where the bound is checked
//!
//! In exactly one place per compound: in `compound_body`, after the count
//! field has been read and before anything is reserved. The order is what
//! `count_is_refused_before_the_elements_are_read` asserts — a `list32`
//! declaring four billion elements in a nine-octet buffer is
//! [`DecodeError::ElementCountExceeded`], not an allocation and not a read
//! past the end.
//!
//! Zero-width array elements (`null`, `true`, `false`, `uint0`, `ulong0` and
//! `list0`) occupy no octets, so the input cannot bound them. Their count is
//! charged to [`Limits::max_zero_width_elements`] from the array header, and
//! one allowance covers the whole decode, so nesting cannot multiply it.
//!
//! # What "incomplete" means
//!
//! A decoder reading from a stream cannot tell a truncated value from a value
//! whose remaining octets have not arrived, so it does not try:
//! [`DecodeError::Incomplete`] carries how many further octets would let
//! another attempt make progress and is the one error that is not a
//! violation. It is a lower bound — a size field not yet read can demand
//! more — which is what a caller needs to size its next read.

use crate::codes;
use crate::error::DecodeError;
use crate::limits::Limits;
use crate::value::{Array, Described, Descriptor, ElementKind, Value};

/// Decodes one value from the front of `input`.
///
/// Returns the value and how many octets it occupied. Variable-width data is
/// borrowed from `input`; a list, map or array allocates one `Vec`, never
/// before its declared count has been checked against `limits.max_elements`,
/// and an array of zero-width elements draws on `limits.max_zero_width_elements`.
///
/// ```
/// use weida_amqp_codec::{Limits, Value, decode};
///
/// // smallulong 42, the shortest form of the number.
/// let (value, used) = decode::value(&[0x53, 0x2a], Limits::DEFAULT).expect("a ulong");
/// assert_eq!(value, Value::Ulong(42));
/// assert_eq!(used, 2);
///
/// // The same number in the wide form decodes to the same value.
/// let wide = [0x80, 0, 0, 0, 0, 0, 0, 0, 0x2a];
/// assert_eq!(decode::value(&wide, Limits::DEFAULT).expect("a ulong").0, Value::Ulong(42));
/// ```
pub fn value(input: &[u8], limits: Limits) -> Result<(Value<'_>, usize), DecodeError> {
    let mut zero_width = limits.max_zero_width_elements;
    value_at(input, limits, 0, &mut zero_width)
}

/// Decodes one described value from the front of `input`, refusing anything
/// that is not described.
///
/// This is the shape a described *restricted* type arrives in — `data`,
/// `amqp-value`, `application-properties` — where the described value is a
/// primitive rather than a field list. Use [`composite`] for the composite
/// types.
pub fn described(
    input: &[u8],
    limits: Limits,
) -> Result<(Descriptor<'_>, Value<'_>, usize), DecodeError> {
    let mut zero_width = limits.max_zero_width_elements;
    described_within(input, limits, &mut zero_width)
}

/// [`described`] under a zero-width allowance the caller already holds, so
/// that a message can decode all of its sections against one allowance.
pub(crate) fn described_within<'a>(
    input: &'a [u8],
    limits: Limits,
    zero_width: &mut u32,
) -> Result<(Descriptor<'a>, Value<'a>, usize), DecodeError> {
    need(input, 0, 1)?;
    if input[0] != codes::DESCRIBED {
        return Err(DecodeError::WrongType {
            field: "described type",
            code: input[0],
        });
    }
    let (descriptor, used) = descriptor_at(&input[1..])?;
    let (value, body) = value_at(&input[1 + used..], limits, 1, zero_width)?;
    Ok((descriptor, value, 1 + used + body))
}

/// Walks the elements of a `list` one at a time.
///
/// A composite type is a described `list` whose position decides the field,
/// and a decoder that collected a `Vec<Value>` first would allocate once per
/// frame for nothing. [`ListCursor::next_value`] answers `Value::Null` past
/// the declared count, because Part 1 permits a composite's trailing null
/// fields to be omitted and therefore makes a short list and a list of nulls
/// the same value.
#[derive(Clone, Debug, PartialEq)]
pub struct ListCursor<'a> {
    rest: &'a [u8],
    remaining: u32,
    limits: Limits,
    depth: u32,
    /// Zero-width elements this composite may still produce, from its own
    /// allowance: one composite has one allowance, whatever its fields hold.
    zero_width: u32,
}

impl<'a> ListCursor<'a> {
    const fn new(body: &'a [u8], count: u32, limits: Limits, depth: u32) -> Self {
        Self {
            rest: body,
            remaining: count,
            limits,
            depth,
            zero_width: limits.max_zero_width_elements,
        }
    }

    /// How many elements the list header declared and the cursor has not
    /// reached yet.
    #[must_use]
    pub const fn remaining(&self) -> u32 {
        self.remaining
    }

    /// The next element, or `Value::Null` once the declared count is
    /// exhausted.
    pub fn next_value(&mut self) -> Result<Value<'a>, DecodeError> {
        if self.remaining == 0 {
            return Ok(Value::Null);
        }
        let (value, used) = value_at(self.rest, self.limits, self.depth + 1, &mut self.zero_width)?;
        self.rest = &self.rest[used..];
        self.remaining -= 1;
        Ok(value)
    }
}

/// A described composite: the descriptor and a cursor over its fields.
#[derive(Clone, Debug, PartialEq)]
pub struct Composite<'a> {
    /// What the composite is.
    pub descriptor: Descriptor<'a>,
    /// Its fields, in the order the specification fixes.
    pub fields: ListCursor<'a>,
    /// Octets the whole described type occupied.
    pub used: usize,
}

/// Decodes a described `list` — a composite type — from the front of
/// `input`.
///
/// Refuses a described value whose value is not a list, which is what
/// separates a composite (`open`, `source`, `accepted`) from a described
/// restricted type (`data`, `amqp-value`).
pub fn composite(input: &[u8], limits: Limits) -> Result<Composite<'_>, DecodeError> {
    need(input, 0, 1)?;
    if input[0] != codes::DESCRIBED {
        return Err(DecodeError::WrongType {
            field: "composite type",
            code: input[0],
        });
    }
    let (descriptor, desc_used) = descriptor_at(&input[1..])?;
    let at = 1 + desc_used;
    need(input, at, 1)?;
    let code = input[at];
    let (fields, used) = match code {
        // `list0`: no size, no count, no elements.
        codes::LIST0 => (ListCursor::new(&[], 0, limits, 1), at + 1),
        codes::LIST8 | codes::LIST32 => {
            let (body, count, header) = compound_body(&input[at + 1..], code, limits)?;
            (
                ListCursor::new(body, count, limits, 1),
                at + 1 + header + body.len(),
            )
        }
        other => {
            return Err(DecodeError::WrongType {
                field: "composite field list",
                code: other,
            });
        }
    };
    Ok(Composite {
        descriptor,
        fields,
        used,
    })
}

/// How many further octets are needed before `from + n` is inside `input`.
fn need(input: &[u8], from: usize, n: u64) -> Result<(), DecodeError> {
    let have = (input.len() - from.min(input.len())) as u64;
    if have < n {
        let short = n - have;
        return Err(DecodeError::Incomplete {
            needed: usize::try_from(short).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}

fn u16_at(input: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([input[at], input[at + 1]])
}

fn u32_at(input: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([input[at], input[at + 1], input[at + 2], input[at + 3]])
}

fn u64_at(input: &[u8], at: usize) -> u64 {
    let mut octets = [0u8; 8];
    octets.copy_from_slice(&input[at..at + 8]);
    u64::from_be_bytes(octets)
}

/// A descriptor: `ulong` or `symbol`, in any of their forms.
fn descriptor_at(input: &[u8]) -> Result<(Descriptor<'_>, usize), DecodeError> {
    need(input, 0, 1)?;
    let code = input[0];
    match code {
        codes::ULONG0 => Ok((Descriptor::Code(0), 1)),
        codes::SMALLULONG => {
            need(input, 1, 1)?;
            Ok((Descriptor::Code(u64::from(input[1])), 2))
        }
        codes::ULONG => {
            need(input, 1, 8)?;
            Ok((Descriptor::Code(u64_at(input, 1)), 9))
        }
        codes::SYM8 | codes::SYM32 => {
            let (value, used) = variable_at(&input[1..], code)?;
            match value {
                Value::Symbol(text) => Ok((Descriptor::Symbol(text), 1 + used)),
                _ => unreachable!("a sym code decodes to a symbol"),
            }
        }
        other => Err(DecodeError::DescriptorNotSymbolicOrNumeric(other)),
    }
}

/// A fixed-width value. `body` starts at the untyped data, not at the
/// constructor, and is known to hold at least `fixed_width(code)` octets.
fn fixed(body: &[u8], code: u8) -> Result<Value<'static>, DecodeError> {
    Ok(match code {
        codes::NULL => Value::Null,
        codes::TRUE => Value::Boolean(true),
        codes::FALSE => Value::Boolean(false),
        codes::UINT0 => Value::Uint(0),
        codes::ULONG0 => Value::Ulong(0),
        codes::LIST0 => Value::List(Vec::new()),
        codes::BOOLEAN => match body[0] {
            0x00 => Value::Boolean(false),
            0x01 => Value::Boolean(true),
            other => return Err(DecodeError::InvalidBoolean(other)),
        },
        codes::UBYTE => Value::Ubyte(body[0]),
        codes::BYTE => Value::Byte(body[0] as i8),
        codes::SMALLUINT => Value::Uint(u32::from(body[0])),
        codes::SMALLULONG => Value::Ulong(u64::from(body[0])),
        codes::SMALLINT => Value::Int(i32::from(body[0] as i8)),
        codes::SMALLLONG => Value::Long(i64::from(body[0] as i8)),
        codes::USHORT => Value::Ushort(u16_at(body, 0)),
        codes::SHORT => Value::Short(u16_at(body, 0) as i16),
        codes::UINT => Value::Uint(u32_at(body, 0)),
        codes::INT => Value::Int(u32_at(body, 0) as i32),
        codes::FLOAT => Value::Float(f32::from_bits(u32_at(body, 0))),
        codes::CHAR => {
            let raw = u32_at(body, 0);
            Value::Char(char::from_u32(raw).ok_or(DecodeError::InvalidChar(raw))?)
        }
        codes::DECIMAL32 => {
            let mut octets = [0u8; 4];
            octets.copy_from_slice(&body[..4]);
            Value::Decimal32(octets)
        }
        codes::ULONG => Value::Ulong(u64_at(body, 0)),
        codes::LONG => Value::Long(u64_at(body, 0) as i64),
        codes::DOUBLE => Value::Double(f64::from_bits(u64_at(body, 0))),
        codes::TIMESTAMP => Value::Timestamp(u64_at(body, 0) as i64),
        codes::DECIMAL64 => {
            let mut octets = [0u8; 8];
            octets.copy_from_slice(&body[..8]);
            Value::Decimal64(octets)
        }
        codes::DECIMAL128 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&body[..16]);
            Value::Decimal128(octets)
        }
        codes::UUID => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&body[..16]);
            Value::Uuid(octets)
        }
        other => return Err(DecodeError::UnknownFormatCode(other)),
    })
}

/// A variable-width value: `binary`, `string` or `symbol`. `body` starts at
/// the size field.
fn variable_at(body: &[u8], code: u8) -> Result<(Value<'_>, usize), DecodeError> {
    let width = match code {
        codes::VBIN8 | codes::STR8 | codes::SYM8 => 1usize,
        codes::VBIN32 | codes::STR32 | codes::SYM32 => 4,
        other => return Err(DecodeError::UnknownFormatCode(other)),
    };
    need(body, 0, width as u64)?;
    let len = if width == 1 {
        u64::from(body[0])
    } else {
        u64::from(u32_at(body, 0))
    };
    need(body, width, len)?;
    let len = usize::try_from(len).expect("checked against the slice above");
    let data = &body[width..width + len];
    let value = match code {
        codes::VBIN8 | codes::VBIN32 => Value::Binary(data),
        codes::STR8 | codes::STR32 => {
            Value::String(core::str::from_utf8(data).map_err(|_| DecodeError::InvalidUtf8)?)
        }
        _ => {
            let text = core::str::from_utf8(data).map_err(|_| DecodeError::InvalidUtf8)?;
            if !text.is_ascii() {
                return Err(DecodeError::NonAsciiSymbol);
            }
            Value::Symbol(text)
        }
    };
    Ok((value, width + len))
}

/// The size and count fields of a compound or array, and the element region
/// they describe. `input` starts *after* the constructor octet.
///
/// Returns the element region, the declared count and how many octets the
/// size and count fields took. This is the one function where a declared
/// count meets the caller's bound.
fn compound_body(
    input: &[u8],
    code: u8,
    limits: Limits,
) -> Result<(&[u8], u32, usize), DecodeError> {
    let width = match code {
        codes::LIST8 | codes::MAP8 | codes::ARRAY8 => 1usize,
        codes::LIST32 | codes::MAP32 | codes::ARRAY32 => 4,
        other => return Err(DecodeError::UnknownFormatCode(other)),
    };
    need(input, 0, width as u64)?;
    let size = if width == 1 {
        u32::from(input[0])
    } else {
        u32_at(input, 0)
    };
    // The size counts the count field and everything after it, so a size
    // that cannot hold its own count field is malformed before the count has
    // even been read.
    let count_bytes = width as u32;
    let elements_len = size
        .checked_sub(count_bytes)
        .ok_or(DecodeError::SizeBelowCount { code, size })?;
    need(input, width, u64::from(count_bytes))?;
    let count = if width == 1 {
        u32::from(input[width])
    } else {
        u32_at(input, width)
    };
    // The bound, from the declared count alone: nothing has been reserved
    // and no element has been looked at.
    if count > limits.max_elements {
        return Err(DecodeError::ElementCountExceeded {
            declared: count,
            cap: limits.max_elements,
        });
    }
    if matches!(code, codes::MAP8 | codes::MAP32) && count % 2 != 0 {
        return Err(DecodeError::OddMapCount(count));
    }
    let header = width * 2;
    need(input, header, u64::from(elements_len))?;
    let elements_len = usize::try_from(elements_len).expect("checked against the slice above");
    Ok((&input[header..header + elements_len], count, header))
}

/// Reserves room for `count` elements without believing `count`.
///
/// A `list` or `map` element occupies at least one octet — its own
/// constructor — so the element region bounds the count and the reservation
/// is bounded by the input. An `array` element may occupy none (an array of
/// `null`); its count was charged to the zero-width allowance before this is
/// reached, so the reservation is bounded by the caller's limit instead.
fn reserve_bounded<T>(items: &mut Vec<T>, count: u32, region: usize) {
    let count = usize::try_from(count).unwrap_or(usize::MAX);
    items.reserve(count.min(region.max(1)));
}

/// Spends `count` zero-width elements from the allowance of one decode.
///
/// Refused from the declared count alone, before anything is reserved for the
/// array, as `max_elements` is.
fn spend_zero_width(zero_width: &mut u32, count: u32, limits: Limits) -> Result<(), DecodeError> {
    if count > *zero_width {
        return Err(DecodeError::ZeroWidthElementsExceeded {
            declared: count,
            cap: limits.max_zero_width_elements,
        });
    }
    *zero_width -= count;
    Ok(())
}

fn value_at<'a>(
    input: &'a [u8],
    limits: Limits,
    depth: u32,
    zero_width: &mut u32,
) -> Result<(Value<'a>, usize), DecodeError> {
    if !limits.admits_depth(depth) {
        return Err(DecodeError::DepthExceeded {
            cap: limits.max_depth,
        });
    }
    need(input, 0, 1)?;
    let code = input[0];

    if code == codes::DESCRIBED {
        let (descriptor, used) = descriptor_at(&input[1..])?;
        let (value, body) = value_at(&input[1 + used..], limits, depth + 1, zero_width)?;
        return Ok((
            Value::Described(Box::new(Described { descriptor, value })),
            1 + used + body,
        ));
    }

    // Fixed width: the category decides how many octets follow, so one
    // length check covers the whole category.
    if let Some(width) = codes::fixed_width(code) {
        need(input, 1, width as u64)?;
        return Ok((fixed(&input[1..], code)?, 1 + width));
    }

    match code {
        codes::VBIN8 | codes::VBIN32 | codes::STR8 | codes::STR32 | codes::SYM8 | codes::SYM32 => {
            let (value, used) = variable_at(&input[1..], code)?;
            Ok((value, 1 + used))
        }
        codes::LIST8
        | codes::LIST32
        | codes::MAP8
        | codes::MAP32
        | codes::ARRAY8
        | codes::ARRAY32 => {
            let (body, count, header) = compound_body(&input[1..], code, limits)?;
            let value = compound_value(body, count, code, limits, depth, zero_width)?;
            Ok((value, 1 + header + body.len()))
        }
        other => Err(DecodeError::UnknownFormatCode(other)),
    }
}

/// The elements of a compound or array, given the region its header
/// described.
fn compound_value<'a>(
    body: &'a [u8],
    count: u32,
    code: u8,
    limits: Limits,
    depth: u32,
    zero_width: &mut u32,
) -> Result<Value<'a>, DecodeError> {
    let value = match code {
        codes::LIST8 | codes::LIST32 => {
            let mut items = Vec::new();
            reserve_bounded(&mut items, count, body.len());
            let mut rest = body;
            for _ in 0..count {
                let (item, used) = value_at(rest, limits, depth + 1, zero_width)?;
                items.push(item);
                rest = &rest[used..];
            }
            exact(code, body, rest)?;
            Value::List(items)
        }
        codes::MAP8 | codes::MAP32 => {
            let mut entries = Vec::new();
            reserve_bounded(&mut entries, count / 2, body.len());
            let mut rest = body;
            for _ in 0..count / 2 {
                let (key, used) = value_at(rest, limits, depth + 1, zero_width)?;
                rest = &rest[used..];
                let (val, used) = value_at(rest, limits, depth + 1, zero_width)?;
                rest = &rest[used..];
                entries.push((key, val));
            }
            exact(code, body, rest)?;
            Value::Map(entries)
        }
        codes::ARRAY8 | codes::ARRAY32 => {
            let (element, ctor) = array_constructor(body)?;
            let element_code = element.code();
            // A zero-width element costs the input nothing, so its count is
            // charged before a `Vec` is reserved or one element is built.
            if codes::fixed_width(element_code) == Some(0) {
                spend_zero_width(zero_width, count, limits)?;
            }
            let mut rest = &body[ctor..];
            let mut items = Vec::new();
            reserve_bounded(&mut items, count, rest.len());
            // A described element's wrapper sits one level below the array and
            // its inner value one level below the wrapper, as in the tree
            // built here, so the inner value is the one the depth check sees.
            let inner_depth = match element {
                ElementKind::Primitive(_) => depth + 1,
                ElementKind::Described(..) => depth + 2,
            };
            for _ in 0..count {
                let (item, used) = untyped(rest, element_code, limits, inner_depth, zero_width)?;
                rest = &rest[used..];
                items.push(match element {
                    ElementKind::Primitive(_) => item,
                    ElementKind::Described(descriptor, _) => {
                        Value::Described(Box::new(Described {
                            descriptor,
                            value: item,
                        }))
                    }
                });
            }
            exact(code, body, rest)?;
            Value::Array(Array::new(element, items))
        }
        other => return Err(DecodeError::UnknownFormatCode(other)),
    };
    Ok(value)
}

/// The declared element region must be consumed exactly: elements that stop
/// short of it mean the sender and this decoder disagree about the encoding,
/// and elements that run past it were already refused by the slice bound.
fn exact(code: u8, body: &[u8], rest: &[u8]) -> Result<(), DecodeError> {
    if rest.is_empty() {
        return Ok(());
    }
    Err(DecodeError::CompoundSizeMismatch {
        code,
        declared: body.len() as u32,
        used: (body.len() - rest.len()) as u32,
    })
}

/// One element of an array, read under the array's shared constructor.
/// `input` starts at the element's untyped data.
fn untyped<'a>(
    input: &'a [u8],
    code: u8,
    limits: Limits,
    depth: u32,
    zero_width: &mut u32,
) -> Result<(Value<'a>, usize), DecodeError> {
    if !limits.admits_depth(depth) {
        return Err(DecodeError::DepthExceeded {
            cap: limits.max_depth,
        });
    }
    if let Some(width) = codes::fixed_width(code) {
        need(input, 0, width as u64)?;
        return Ok((fixed(input, code)?, width));
    }
    match code {
        codes::VBIN8 | codes::VBIN32 | codes::STR8 | codes::STR32 | codes::SYM8 | codes::SYM32 => {
            variable_at(input, code)
        }
        codes::LIST8
        | codes::LIST32
        | codes::MAP8
        | codes::MAP32
        | codes::ARRAY8
        | codes::ARRAY32 => {
            let (body, count, header) = compound_body(input, code, limits)?;
            let value = compound_value(body, count, code, limits, depth, zero_width)?;
            Ok((value, header + body.len()))
        }
        other => Err(DecodeError::UnknownFormatCode(other)),
    }
}

/// The element constructor of an array: a format code, or `0x00`, a
/// descriptor and a format code.
fn array_constructor(body: &[u8]) -> Result<(ElementKind<'_>, usize), DecodeError> {
    if body.is_empty() {
        return Err(DecodeError::ArrayConstructorMissing);
    }
    if body[0] != codes::DESCRIBED {
        let code = body[0];
        check_array_code(code)?;
        return Ok((ElementKind::Primitive(code), 1));
    }
    let (descriptor, used) = descriptor_at(&body[1..])?;
    let at = 1 + used;
    if body.len() <= at {
        return Err(DecodeError::ArrayConstructorMissing);
    }
    let code = body[at];
    check_array_code(code)?;
    Ok((ElementKind::Described(descriptor, code), at + 1))
}

/// An array element constructor must be a code this codec can read and must
/// not be `0x00`: Part 1 allows one descriptor per array, not a chain.
fn check_array_code(code: u8) -> Result<(), DecodeError> {
    if code == codes::DESCRIBED || codes::name(code).is_none() {
        return Err(DecodeError::InvalidArrayConstructor(code));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Limits = Limits::DEFAULT;

    #[test]
    fn every_form_of_a_number_decodes_to_one_value() {
        for bytes in [
            vec![codes::UINT0],
            vec![codes::SMALLUINT, 0],
            vec![codes::UINT, 0, 0, 0, 0],
        ] {
            assert_eq!(value(&bytes, L).expect("a uint").0, Value::Uint(0));
        }
        for bytes in [
            vec![codes::ULONG0],
            vec![codes::SMALLULONG, 0],
            vec![codes::ULONG, 0, 0, 0, 0, 0, 0, 0, 0],
        ] {
            assert_eq!(value(&bytes, L).expect("a ulong").0, Value::Ulong(0));
        }
        for bytes in [vec![codes::TRUE], vec![codes::BOOLEAN, 0x01]] {
            assert_eq!(value(&bytes, L).expect("a boolean").0, Value::Boolean(true));
        }
        for bytes in [
            vec![codes::LIST0],
            vec![codes::LIST8, 1, 0],
            vec![codes::LIST32, 0, 0, 0, 4, 0, 0, 0, 0],
        ] {
            assert_eq!(
                value(&bytes, L).expect("a list").0,
                Value::List(Vec::new()),
                "three ways to write the empty list"
            );
        }
    }

    #[test]
    fn small_signed_forms_are_signed() {
        assert_eq!(
            value(&[codes::SMALLINT, 0xff], L).expect("an int").0,
            Value::Int(-1)
        );
        assert_eq!(
            value(&[codes::SMALLLONG, 0x80], L).expect("a long").0,
            Value::Long(-128)
        );
    }

    #[test]
    fn boolean_has_exactly_two_one_octet_values() {
        assert_eq!(
            value(&[codes::BOOLEAN, 0x02], L),
            Err(DecodeError::InvalidBoolean(0x02))
        );
    }

    #[test]
    fn count_is_refused_before_the_elements_are_read() {
        // A list32 declaring four billion elements in a nine-octet buffer.
        // The count check must come first: anything else is either an
        // allocation or a read past the end.
        let hostile = [
            codes::LIST32,
            0xff,
            0xff,
            0xff,
            0xff,
            0xff,
            0xff,
            0xff,
            0xff,
        ];
        assert_eq!(
            value(&hostile, L),
            Err(DecodeError::ElementCountExceeded {
                declared: u32::MAX,
                cap: L.max_elements,
            })
        );
    }

    #[test]
    fn a_size_too_small_for_its_own_count_is_malformed() {
        assert_eq!(
            value(&[codes::LIST8, 0x00, 0x00], L),
            Err(DecodeError::SizeBelowCount {
                code: codes::LIST8,
                size: 0
            })
        );
    }

    #[test]
    fn depth_is_bounded_on_the_way_down() {
        // A list holding a list holding a list, eight deep.
        let mut bytes = vec![codes::LIST0];
        for _ in 0..8 {
            let inner = bytes.len();
            let mut wrapped = vec![codes::LIST8, (inner + 1) as u8, 1];
            wrapped.extend_from_slice(&bytes);
            bytes = wrapped;
        }
        let shallow = Limits {
            max_elements: 16,
            max_depth: 4,
            max_zero_width_elements: 16,
        };
        assert_eq!(
            value(&bytes, shallow),
            Err(DecodeError::DepthExceeded { cap: 4 })
        );
        assert!(
            value(&bytes, Limits::DEFAULT).is_ok(),
            "the default admits nine levels"
        );
    }

    #[test]
    fn an_odd_map_count_is_refused() {
        let bytes = [codes::MAP8, 0x02, 0x01, codes::NULL];
        assert_eq!(value(&bytes, L), Err(DecodeError::OddMapCount(1)));
    }

    #[test]
    fn a_truncated_value_asks_for_the_octets_it_needs() {
        assert_eq!(
            value(&[codes::ULONG, 0, 0], L),
            Err(DecodeError::Incomplete { needed: 6 })
        );
        assert!(!value(&[codes::ULONG, 0, 0], L).unwrap_err().is_violation());
    }

    #[test]
    fn a_symbol_must_be_ascii() {
        let bytes = [codes::SYM8, 0x02, 0xc3, 0xa9];
        assert_eq!(value(&bytes, L), Err(DecodeError::NonAsciiSymbol));
    }

    #[test]
    fn a_string_must_be_utf8() {
        let bytes = [codes::STR8, 0x01, 0xff];
        assert_eq!(value(&bytes, L), Err(DecodeError::InvalidUtf8));
    }

    #[test]
    fn a_descriptor_is_a_symbol_or_a_ulong_and_nothing_else() {
        let bytes = [codes::DESCRIBED, codes::TRUE, codes::NULL];
        assert_eq!(
            value(&bytes, L),
            Err(DecodeError::DescriptorNotSymbolicOrNumeric(codes::TRUE))
        );
        // A symbolic descriptor is accepted, which is what the
        // specification's own worked example uses.
        let bytes = [
            codes::DESCRIBED,
            codes::SYM8,
            3,
            b'a',
            b'b',
            b'c',
            codes::NULL,
        ];
        let (descriptor, value, used) = described(&bytes, L).expect("a described type");
        assert_eq!(descriptor, Descriptor::Symbol("abc"));
        assert_eq!(value, Value::Null);
        assert_eq!(used, bytes.len());
    }

    #[test]
    fn an_array_shares_one_constructor() {
        // size 5 = count(1) + constructor(1) + three one-octet elements.
        let bytes = [codes::ARRAY8, 5, 3, codes::SMALLULONG, 1, 2, 3];
        let (value, used) = value(&bytes, L).expect("an array");
        assert_eq!(used, bytes.len());
        let Value::Array(array) = value else {
            panic!("expected an array");
        };
        assert_eq!(array.element(), &ElementKind::Primitive(codes::SMALLULONG));
        assert_eq!(
            array.items(),
            &[Value::Ulong(1), Value::Ulong(2), Value::Ulong(3)]
        );
    }

    #[test]
    fn an_array_of_described_values_carries_the_descriptor_once() {
        // The element constructor is `0x00 0x53 0x24 0x45`: described
        // `accepted` of `list0`, three times, and no descriptor repeated.
        let bytes = [
            codes::ARRAY8,
            5,
            3,
            codes::DESCRIBED,
            codes::SMALLULONG,
            0x24,
            codes::LIST0,
        ];
        let (value, used) = value(&bytes, L).expect("an array");
        assert_eq!(used, bytes.len());
        let Value::Array(array) = value else {
            panic!("expected an array");
        };
        assert_eq!(
            array.element(),
            &ElementKind::Described(Descriptor::Code(0x24), codes::LIST0)
        );
        assert_eq!(array.items().len(), 3);
        for item in array.items() {
            let Value::Described(described) = item else {
                panic!("expected a described element");
            };
            assert!(described.descriptor.is(0x24));
            assert_eq!(described.value, Value::List(Vec::new()));
        }
    }

    #[test]
    fn an_array_with_no_element_constructor_is_malformed() {
        let bytes = [codes::ARRAY8, 1, 0];
        assert_eq!(value(&bytes, L), Err(DecodeError::ArrayConstructorMissing));
    }

    #[test]
    fn a_composite_is_a_described_list_walked_by_position() {
        let bytes = [
            codes::DESCRIBED,
            codes::SMALLULONG,
            0x10,
            codes::LIST8,
            6,
            2,
            codes::STR8,
            1,
            b'a',
            codes::SMALLULONG,
            7,
        ];
        let mut composite = composite(&bytes, L).expect("a composite");
        assert!(composite.descriptor.is(0x10));
        assert_eq!(composite.used, bytes.len());
        assert_eq!(composite.fields.remaining(), 2);
        assert_eq!(composite.fields.next_value().unwrap(), Value::String("a"));
        assert_eq!(composite.fields.next_value().unwrap(), Value::Ulong(7));
        assert_eq!(
            composite.fields.next_value().unwrap(),
            Value::Null,
            "a trimmed trailing field reads as null"
        );
    }

    #[test]
    fn a_composite_with_no_fields_may_use_list0() {
        let bytes = [codes::DESCRIBED, codes::SMALLULONG, 0x24, codes::LIST0];
        let mut composite = composite(&bytes, L).expect("a composite");
        assert!(composite.descriptor.is(0x24));
        assert_eq!(composite.used, bytes.len());
        assert_eq!(composite.fields.remaining(), 0);
        assert_eq!(composite.fields.next_value().unwrap(), Value::Null);
    }

    #[test]
    fn a_described_non_list_is_not_a_composite() {
        // described(0x75) of vbin8: the `data` section, described but not
        // composite.
        let bytes = [
            codes::DESCRIBED,
            codes::SMALLULONG,
            0x75,
            codes::VBIN8,
            1,
            0xff,
        ];
        assert_eq!(
            composite(&bytes, L),
            Err(DecodeError::WrongType {
                field: "composite field list",
                code: codes::VBIN8
            })
        );
        let (descriptor, value, used) = described(&bytes, L).expect("a described type");
        assert!(descriptor.is(0x75));
        assert_eq!(value, Value::Binary(&[0xff]));
        assert_eq!(used, bytes.len());
    }

    #[test]
    fn a_compound_whose_elements_disagree_with_its_size_is_refused() {
        // Size 3, count 1, and an element claiming five octets: the element
        // runs past the region, so the slice bound catches it first.
        let bytes = [codes::LIST8, 3, 1, codes::UINT, 0, 0, 0, 0];
        assert!(matches!(
            value(&bytes, L),
            Err(DecodeError::Incomplete { .. })
        ));
        // Size 4, count 1, and a one-octet element: the region is not
        // consumed.
        let bytes = [codes::LIST8, 4, 1, codes::NULL, codes::NULL, codes::NULL];
        assert_eq!(
            value(&bytes, L),
            Err(DecodeError::CompoundSizeMismatch {
                code: codes::LIST8,
                declared: 3,
                used: 1
            })
        );
    }

    #[test]
    fn unknown_format_codes_are_refused_rather_than_skipped() {
        assert_eq!(
            value(&[0x57, 0x00], L),
            Err(DecodeError::UnknownFormatCode(0x57))
        );
    }

    #[test]
    fn a_map_keeps_its_wire_order_and_its_duplicates() {
        // Two entries with the same key. A hash map would lose one; the
        // specification calls a duplicate key invalid, and an application
        // that must report it needs to see it.
        let bytes = [
            codes::MAP8,
            11,
            4,
            codes::SYM8,
            1,
            b'k',
            codes::SMALLUINT,
            1,
            codes::SYM8,
            1,
            b'k',
            codes::SMALLUINT,
            2,
        ];
        let (value, used) = value(&bytes, L).expect("a map");
        assert_eq!(used, bytes.len());
        assert_eq!(
            value,
            Value::Map(vec![
                (Value::Symbol("k"), Value::Uint(1)),
                (Value::Symbol("k"), Value::Uint(2)),
            ])
        );
    }

    #[test]
    fn a_flat_array_of_nulls_is_refused_before_it_is_built() {
        // The audit's reproducer: an array32 of 65 536 nulls, nine octets.
        let hostile = [0xf0, 0, 0, 0, 5, 0, 1, 0, 0, codes::NULL];
        assert_eq!(
            value(&hostile, Limits::BODY),
            Err(DecodeError::ZeroWidthElementsExceeded {
                declared: 65_536,
                cap: 256,
            })
        );
    }

    #[test]
    fn a_described_array_element_is_charged_and_depth_checked_as_built() {
        // The `value` fuzz crash: six described values, then an array8 whose
        // elements are described `list0`. The tree holds the inner `list0` at
        // depth 8, so a bound of 8 must refuse it, and a bound of 9 must not.
        let bytes = [
            0x00, 0x44, 0x00, 0x53, 0x00, 0x00, 0x53, 0x00, 0x00, 0x53, 0x02, 0x00, 0x53, 0x04,
            0x00, 0x44, 0xe0, 0x05, 0x04, 0x00, 0x53, 0x2d, 0x45, 0x00, 0x53, 0x00, 0x29, 0x9b,
            0x60, 0x14,
        ];
        let fuzzed = Limits {
            max_elements: 64,
            max_depth: 8,
            max_zero_width_elements: 16,
        };
        assert_eq!(
            value(&bytes, fuzzed),
            Err(DecodeError::DepthExceeded { cap: 8 })
        );
        for limits in [
            Limits {
                max_depth: 9,
                ..fuzzed
            },
            Limits::DEFAULT,
        ] {
            let (decoded, used) = value(&bytes, limits).expect("admitted at this depth");
            assert_eq!(used, 23, "the array ends at octet 23");
            let Value::Described(outer) = decoded else {
                panic!("expected a described value");
            };
            assert!(outer.descriptor.is(0), "the outer descriptor is ulong0");
        }
    }

    #[test]
    fn a_described_zero_width_element_is_charged_per_element() {
        // Each element is a described `null`: no octets of its own, and the
        // wrapper and the null are both nodes, so the count is charged.
        let bytes = [
            0xf0,
            0,
            0,
            0,
            8,
            0,
            0,
            0,
            0x11,
            codes::DESCRIBED,
            codes::SMALLULONG,
            0x24,
            codes::NULL,
        ];
        assert!(matches!(
            value(&bytes, Limits::DEFAULT),
            Err(DecodeError::ZeroWidthElementsExceeded { declared: 17, .. })
        ));
    }
}
