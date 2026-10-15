//! Deterministic hostile-input smoke tests for the AMQP type system.
//!
//! The always-on companion to the `cargo fuzz` targets under
//! `crates/amqp/weida-amqp-codec/fuzz`: the same properties, driven by a
//! seeded xorshift generator so they run on stable Rust, in `cargo test`,
//! with no nightly toolchain and no corpus. The libFuzzer targets explore far
//! deeper; these make sure the properties are never left unchecked.
//!
//! The property that matters most is the one Part 1 gives no help with. A
//! nine-octet `list32` header can declare 2^32-1 elements, a five-octet
//! `vbin32` header can declare 4 GiB, and there is no credit on the wire
//! before `open` has been read — so the caller's [`Limits`] is the whole
//! defence: **an accepted compound never holds more elements than the bound,
//! and a rejected one allocates nothing.** The first is asserted directly;
//! the second by construction, because every input here is a few dozen
//! octets and a decoder that reserved a declared count would abort the test
//! process rather than fail it.

use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::message::Message;
use weida_amqp_codec::{
    Array, DecodeError, ElementKind, Limits, Performative, SaslFrame, Value, codes, decode, encode,
};

const ITERATIONS: usize = 100_000;

/// Deliberately small: a declared count above it must be refused from the
/// header alone, and a 48-octet input can never satisfy it by accident.
const LIMITS: Limits = Limits {
    max_elements: 32,
    max_depth: 6,
    max_zero_width_elements: 16,
};

/// Deterministic xorshift64* generator: no dependency, reproducible failures.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn byte(&mut self) -> u8 {
        (self.next_u64() >> 33) as u8
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() >> 33) as usize % n
    }

    fn bytes(&mut self, max: usize) -> Vec<u8> {
        let len = self.below(max + 1);
        (0..len).map(|_| self.byte()).collect()
    }

    /// Bytes that begin with a real format code, so that the generator
    /// reaches the decoders rather than bouncing off
    /// `UnknownFormatCode` nine times in ten.
    fn plausible(&mut self, max: usize) -> Vec<u8> {
        const CODES: [u8; 40] = [
            0x00, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56,
            0x60, 0x61, 0x70, 0x71, 0x72, 0x73, 0x74, 0x80, 0x81, 0x82, 0x83, 0x84, 0x94, 0x98,
            0xa0, 0xa1, 0xa3, 0xb0, 0xb1, 0xb3, 0xc0, 0xc1, 0xd0, 0xd1, 0xe0, 0xf0,
        ];
        let mut bytes = self.bytes(max);
        if bytes.is_empty() {
            bytes.push(0);
        }
        bytes[0] = CODES[self.below(CODES.len())];
        bytes
    }

    /// A well-formed `list8` of `count` values drawn from the encodings of
    /// [`ATOMS`].
    ///
    /// Random bytes reach a *performative's* field decoding about never: the
    /// body has to be a list before the first field is even looked at, and
    /// then every field has to carry a type the field accepts. So the
    /// generator builds a real list of real values of random types, which is
    /// what makes the wrong-type, missing-mandatory and restriction paths
    /// the ones being explored.
    fn field_list(&mut self, max_fields: usize) -> Vec<u8> {
        let count = self.below(max_fields + 1);
        let mut body = Vec::new();
        for _ in 0..count {
            body.extend_from_slice(ATOMS[self.below(ATOMS.len())]);
        }
        if count == 0 {
            return vec![0x45];
        }
        if body.len() + 1 > usize::from(u8::MAX) || count > usize::from(u8::MAX) {
            unreachable!("max_fields and ATOMS keep this inside the narrow form");
        }
        let mut list = vec![0xc0, (body.len() + 1) as u8, count as u8];
        list.extend_from_slice(&body);
        list
    }
}

/// One well-formed encoding of each type a performative field can have,
/// including the forms that are deliberately the *wrong* type for most
/// fields.
const ATOMS: [&[u8]; 16] = [
    &[0x40],                         // null
    &[0x41],                         // true
    &[0x42],                         // false
    &[0x43],                         // uint 0
    &[0x44],                         // ulong 0
    &[0x45],                         // list0
    &[0x50, 0x02],                   // ubyte 2
    &[0x52, 0x07],                   // uint 7
    &[0x53, 0x07],                   // ulong 7
    &[0x60, 0x00, 0x07],             // ushort 7
    &[0x70, 0x00, 0x00, 0x01, 0x00], // uint 256
    &[0xa0, 0x01, 0xff],             // binary
    &[0xa1, 0x01, b'x'],             // string
    &[0xa3, 0x01, b'x'],             // symbol
    &[0xc1, 0x01, 0x00],             // empty map
    &[0x00, 0x53, 0x24, 0x45],       // described: accepted
];

/// Every invariant an accepted value must satisfy, checked recursively.
fn check(value: &Value<'_>, depth: u32) {
    assert!(
        depth < LIMITS.max_depth,
        "an accepted value is inside the depth bound"
    );
    match value {
        Value::List(items) => {
            assert!(items.len() as u64 <= u64::from(LIMITS.max_elements));
            for item in items {
                check(item, depth + 1);
            }
        }
        Value::Map(entries) => {
            // The bound is on the wire count, which is twice the pairs.
            assert!(entries.len() as u64 * 2 <= u64::from(LIMITS.max_elements));
            for (key, val) in entries {
                check(key, depth + 1);
                check(val, depth + 1);
            }
        }
        Value::Array(array) => {
            assert!(array.items().len() as u64 <= u64::from(LIMITS.max_elements));
            for item in array.items() {
                check(item, depth + 1);
            }
        }
        Value::Described(described) => check(&described.value, depth + 1),
        Value::Symbol(text) => assert!(text.is_ascii(), "an accepted symbol is ASCII"),
        _ => {}
    }
}

#[test]
fn fuzz_smoke_value() {
    let mut rng = Rng::new(0xA3_0F_10);
    let mut accepted = 0usize;
    for _ in 0..ITERATIONS {
        let input = rng.plausible(48);
        match decode::value(&input, LIMITS) {
            Ok((value, used)) => {
                accepted += 1;
                assert!(used <= input.len(), "a decoder never reads past its input");
                assert!(used >= 1, "every value has at least a constructor");
                check(&value, 0);

                // The canonical encoding is a fixed point: re-decoding it
                // and re-encoding that gives the same octets back. Stated
                // over octets rather than over values because a value may
                // hold a NaN, and `Value`'s equality is Rust's float
                // equality, under which no NaN equals itself.
                let written = encode::to_vec(&value).expect("an accepted value re-encodes");
                assert!(
                    written.len() <= used,
                    "the canonical form is never wider than what arrived"
                );
                let (again, used2) = decode::value(&written, LIMITS)
                    .expect("the re-encoding decodes under the same limits");
                assert_eq!(used2, written.len());
                check(&again, 0);
                assert_eq!(
                    encode::to_vec(&again).expect("re-encodes"),
                    written,
                    "the canonical encoding is idempotent"
                );
            }
            Err(error) => {
                // Every rejection is either "read more" or a violation, and
                // nothing in between.
                assert_eq!(
                    error.is_violation(),
                    !matches!(error, DecodeError::Incomplete { .. })
                );
            }
        }
    }
    assert!(
        accepted > ITERATIONS / 20,
        "the generator must actually reach the decoders: only {accepted} of \
         {ITERATIONS} inputs decoded"
    );
}

#[test]
fn fuzz_smoke_composite() {
    let mut rng = Rng::new(0x1A_2B_3C);
    for _ in 0..ITERATIONS {
        // A described type is `0x00`, a descriptor, a value. Prefix the
        // generator's bytes with the two shapes a real frame body has, so
        // the composite decoder is reached rather than refused at the first
        // octet.
        let tail = rng.bytes(40);
        let mut input = match rng.below(3) {
            0 => vec![0x00, 0x53, rng.byte()],
            1 => vec![0x00, 0x80, 0, 0, 0, 0, 0, 0, rng.byte()],
            _ => vec![0x00, 0xa3, 0x02, b'a', b'b'],
        };
        input.extend_from_slice(&tail);

        if let Ok(mut composite) = decode::composite(&input, LIMITS) {
            assert!(composite.used <= input.len());
            let declared = composite.fields.remaining();
            assert!(declared <= LIMITS.max_elements);
            let mut seen = 0u32;
            // Walking past the declared count must terminate and yield
            // nulls, not panic and not loop.
            for _ in 0..declared + 4 {
                match composite.fields.next_value() {
                    Ok(value) => {
                        check(&value, 1);
                        if seen < declared {
                            seen += 1;
                        } else {
                            assert!(
                                value.is_null(),
                                "past the declared count every field is null"
                            );
                        }
                    }
                    Err(error) => {
                        assert!(
                            error.is_violation() || matches!(error, DecodeError::Incomplete { .. })
                        );
                        break;
                    }
                }
            }
        }
    }
}

#[test]
fn a_declared_count_is_never_believed() {
    // The four headers that can lie the most, each in a buffer far too small
    // to hold what it claims. None of these may allocate: the test process
    // would be killed rather than the assertion failing.
    for header in [
        vec![0xd0, 0x7f, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff],
        vec![0xd1, 0x7f, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xfe],
        vec![0xf0, 0x7f, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff],
        vec![0xb0, 0x7f, 0xff, 0xff, 0xff],
    ] {
        let error = decode::value(&header, LIMITS).expect_err("a lie is refused");
        assert!(
            matches!(
                error,
                DecodeError::ElementCountExceeded { .. } | DecodeError::Incomplete { .. }
            ),
            "unexpected {error:?} for {header:02x?}"
        );
    }
}

#[test]
fn fuzz_smoke_frame() {
    let mut rng = Rng::new(0xF0_0D);
    let mut accepted = 0usize;
    for _ in 0..ITERATIONS {
        // A frame around a body that is itself well formed most of the
        // time, because a uniformly random SIZE is above the ceiling
        // 99.99% of the time and a uniformly random DOFF overruns the frame
        // nearly always — so the header check would be the only thing ever
        // exercised. One time in four each of DOFF and TYPE is random
        // anyway, which is what keeps the malformed paths covered.
        let body = match rng.below(4) {
            0 => rng.bytes(24),
            _ => {
                let mut described = vec![0x00u8, 0x53, 0x10 + rng.below(9) as u8];
                described.extend_from_slice(&rng.field_list(4));
                described
            }
        };
        let doff = if rng.below(4) == 0 { rng.byte() } else { 2 };
        let kind = if rng.below(4) == 0 {
            rng.byte()
        } else {
            rng.below(2) as u8
        };
        let size = (8 + body.len()) as u32;
        let mut input = size.to_be_bytes().to_vec();
        input.push(doff);
        input.push(kind);
        input.push(rng.byte());
        input.push(rng.byte());
        input.extend_from_slice(&body);

        match frame::decode(&input, MIN_MAX_FRAME_SIZE) {
            Ok(read) => {
                accepted += 1;
                assert!(read.header.size >= 8);
                assert!(read.header.size <= MIN_MAX_FRAME_SIZE);
                assert!(read.header.doff >= 2);
                assert!(read.used() <= input.len());
                assert_eq!(read.body.len(), read.header.body_len());
                assert!(read.header.body_offset() <= read.used());

                match read.header.kind {
                    FrameKind::Amqp => {
                        if let Ok((performative, used)) = Performative::decode(read.body, LIMITS) {
                            assert!(used <= read.body.len());
                            let mut written = Vec::new();
                            performative.encode(&mut written).expect("re-encodes");
                            let (again, used2) =
                                Performative::decode(&written, LIMITS).expect("decodes");
                            assert_eq!(used2, written.len());
                            let mut twice = Vec::new();
                            again.encode(&mut twice).expect("re-encodes");
                            assert_eq!(twice, written, "a fixed point over octets");
                        }
                    }
                    FrameKind::Sasl => {
                        if let Ok((body, used)) = SaslFrame::decode(read.body, LIMITS) {
                            assert!(used <= read.body.len());
                            let mut written = Vec::new();
                            body.encode(&mut written).expect("re-encodes");
                            let (again, used2) =
                                SaslFrame::decode(&written, LIMITS).expect("decodes");
                            assert_eq!(used2, written.len());
                            assert_eq!(again, body);
                        }
                    }
                }
            }
            Err(error) => assert_eq!(
                error.is_violation(),
                !matches!(error, DecodeError::Incomplete { .. })
            ),
        }
    }
    assert!(
        accepted > ITERATIONS / 20,
        "the generator must reach the frame decoder: only {accepted} of \
         {ITERATIONS} inputs decoded"
    );
}

#[test]
fn fuzz_smoke_performative() {
    let mut rng = Rng::new(0x0B_ADC0DE);
    let mut accepted = 0usize;
    for _ in 0..ITERATIONS {
        // A described list with a real performative descriptor in front, so
        // the field-by-field decoding is what gets explored rather than the
        // dispatch.
        let code = 0x10 + rng.below(9) as u8;
        let body = rng.field_list(6);
        let mut input = vec![0x00u8, 0x53, code];
        input.extend_from_slice(&body);

        match Performative::decode(&input, LIMITS) {
            Ok((performative, used)) => {
                accepted += 1;
                assert!(used <= input.len());
                assert_eq!(performative.descriptor(), u64::from(code));
                let mut written = Vec::new();
                performative.encode(&mut written).expect("re-encodes");
                let (again, used2) = Performative::decode(&written, LIMITS).expect("decodes");
                assert_eq!(used2, written.len());
                assert_eq!(again, performative);
                let mut twice = Vec::new();
                again.encode(&mut twice).expect("re-encodes");
                assert_eq!(twice, written, "the canonical encoding is idempotent");
            }
            Err(error) => assert_eq!(
                error.is_violation(),
                !matches!(error, DecodeError::Incomplete { .. })
            ),
        }
    }
    assert!(
        accepted > ITERATIONS / 100,
        "the generator must reach the performatives: only {accepted} of \
         {ITERATIONS} inputs decoded"
    );
}

#[test]
fn a_frame_size_field_is_never_believed() {
    // Four octets claiming four gigabytes, in an eight-octet buffer. Before
    // `open` has been read the ceiling is 512, and this is the check that
    // makes the ceiling an argument rather than a constant nobody passes.
    let hostile = [0xff, 0xff, 0xff, 0xff, 0x02, 0x00, 0x00, 0x00];
    assert_eq!(
        frame::decode(&hostile, MIN_MAX_FRAME_SIZE),
        Err(DecodeError::FrameTooLarge {
            size: u32::MAX,
            max: MIN_MAX_FRAME_SIZE
        })
    );
    // And the same header once the partner has advertised room: still
    // refused, because the octets are not there.
    assert!(matches!(
        frame::decode(&hostile, u32::MAX),
        Err(DecodeError::Incomplete { .. })
    ));
}

/// An `array32` of `count` elements: `constructor` is the element constructor
/// and `elements` the untyped octets of the elements.
fn array32(count: u32, constructor: &[u8], elements: &[u8]) -> Vec<u8> {
    let size = (4 + constructor.len() + elements.len()) as u32;
    let mut bytes = vec![codes::ARRAY32];
    bytes.extend_from_slice(&size.to_be_bytes());
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.extend_from_slice(constructor);
    bytes.extend_from_slice(elements);
    bytes
}

/// A `list32` of already-encoded items.
fn list32(items: &[Vec<u8>]) -> Vec<u8> {
    let body = items.concat();
    let size = (4 + body.len()) as u32;
    let mut bytes = vec![codes::LIST32];
    bytes.extend_from_slice(&size.to_be_bytes());
    bytes.extend_from_slice(&(items.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&body);
    bytes
}

/// `inner` as the element of an enclosing array of arrays: the same octets
/// without the constructor the enclosing array already carries.
fn as_element(inner: &[u8]) -> &[u8] {
    &inner[1..]
}

/// `n` copies of `inner` as the elements of an enclosing array of arrays.
fn elements_of(n: usize, inner: &[u8]) -> Vec<u8> {
    (0..n)
        .flat_map(|_| as_element(inner).iter().copied())
        .collect()
}

/// Every `Value` in a decoded tree, a described value's inner value included.
fn nodes(value: &Value<'_>) -> usize {
    1 + match value {
        Value::List(items) => items.iter().map(nodes).sum(),
        Value::Map(entries) => entries.iter().map(|(k, v)| nodes(k) + nodes(v)).sum(),
        Value::Array(array) => array.items().iter().map(nodes).sum(),
        Value::Described(described) => nodes(&described.value),
        _ => 0,
    }
}

/// Decodes `input`, and if it is accepted checks the node bound and that its
/// canonical encoding is accepted again under the same limits. Returns whether
/// it was accepted.
///
/// The bound: each octet of input owns at most two nodes (a described array
/// element is a wrapper with no octets of its own around an inner value that
/// owns at least one), and each unit of the zero-width allowance admits two
/// more (a zero-width element's wrapper and its inner value). So a tree has at
/// most `2 * input.len() + 2 * max_zero_width_elements` nodes.
fn check_accepted(input: &[u8], limits: Limits) -> bool {
    let Ok((value, _)) = decode::value(input, limits) else {
        return false;
    };
    let count = nodes(&value);
    let bound = 2 * input.len() + 2 * limits.max_zero_width_elements as usize;
    assert!(
        count <= bound,
        "{count} nodes from {} octets exceed {bound} under {limits:?}",
        input.len()
    );
    let written = encode::to_vec(&value).expect("an accepted value re-encodes");
    let (again, used) = decode::value(&written, limits)
        .expect("the canonical form is accepted under the same limits");
    assert_eq!(used, written.len());
    assert_eq!(encode::to_vec(&again).expect("re-encodes"), written);
    true
}

#[test]
fn zero_width_arrays_are_refused_once_the_budget_is_spent() {
    let nulls = array32(4096, &[codes::NULL], &[]);
    let flat = array32(65_536, &[codes::NULL], &[]);
    let nested = array32(64, &[codes::ARRAY32], &elements_of(64, &nulls));
    let leaves = array32(4096, &[codes::NULL], &[]);
    let middle = array32(16, &[codes::ARRAY32], &elements_of(16, &leaves));
    let deep = array32(2, &[codes::ARRAY32], &elements_of(2, &middle));

    // Breadth alone is the header's own bound, not the budget's.
    assert_eq!(
        decode::value(&flat, Limits::DEFAULT),
        Err(DecodeError::ElementCountExceeded {
            declared: 65_536,
            cap: 4096,
        })
    );
    assert_eq!(
        decode::value(&flat, Limits::BODY).map(|(_, used)| used),
        Err(DecodeError::ZeroWidthElementsExceeded {
            declared: 65_536,
            cap: 256,
        })
    );
    for limits in [Limits::DEFAULT, Limits::BODY] {
        for hostile in [&nested, &deep] {
            assert!(
                matches!(
                    decode::value(hostile, limits),
                    Err(DecodeError::ZeroWidthElementsExceeded { declared: 4096, .. })
                ),
                "nesting must not multiply the budget under {limits:?}"
            );
        }
    }
}

#[test]
fn legal_arrays_still_decode_and_round_trip() {
    let booleans: Vec<u8> = (0..1000).map(|i| (i % 2) as u8).collect();
    let symbols: Vec<u8> = (0..4096).flat_map(|_| [1u8, b'a']).collect();
    let described: Vec<u8> = (0..3u32).flat_map(|i| i.to_be_bytes()).collect();
    for (bytes, count) in [
        (array32(1000, &[codes::BOOLEAN], &booleans), 1000),
        (array32(4096, &[codes::SYM8], &symbols), 4096),
        (
            array32(
                3,
                &[codes::DESCRIBED, codes::SMALLULONG, 0x24, codes::UINT],
                &described,
            ),
            3,
        ),
    ] {
        let (value, used) = decode::value(&bytes, Limits::DEFAULT).expect("a legal array");
        assert_eq!(used, bytes.len());
        let Value::Array(array) = &value else {
            panic!("expected an array");
        };
        assert_eq!(array.items().len(), count);
        let written = encode::to_vec(&value).expect("re-encodes");
        assert_eq!(
            decode::value(&written, Limits::DEFAULT).expect("decodes").0,
            value
        );
    }
}

#[test]
fn an_array_of_nulls_up_to_the_budget_round_trips() {
    let nulls = Value::Array(Array::new(
        ElementKind::Primitive(codes::NULL),
        vec![Value::Null; 16],
    ));
    let written = encode::to_vec(&nulls).expect("encodes");
    let (back, used) =
        decode::value(&written, Limits::DEFAULT).expect("sixteen nulls are within the budget");
    assert_eq!(used, written.len());
    assert_eq!(back, nulls);
}

#[test]
fn a_composite_has_one_allowance_across_its_fields() {
    let mut bytes = vec![codes::DESCRIBED, codes::SMALLULONG, 0x10];
    bytes.extend(list32(&[
        array32(10, &[codes::NULL], &[]),
        array32(10, &[codes::NULL], &[]),
    ]));
    let mut composite = decode::composite(&bytes, Limits::DEFAULT).expect("a composite");
    composite
        .fields
        .next_value()
        .expect("ten nulls are within the budget");
    assert_eq!(
        composite.fields.next_value(),
        Err(DecodeError::ZeroWidthElementsExceeded {
            declared: 10,
            cap: 16,
        })
    );
}

#[test]
fn a_message_shares_one_allowance_across_its_sections() {
    // An `amqp-sequence` section whose value is a list of one array of `count`
    // nulls. Each section is within the allowance on its own.
    let section = |count: u32| {
        let mut bytes = vec![codes::DESCRIBED, codes::SMALLULONG, 0x76];
        bytes.extend(list32(&[array32(count, &[codes::NULL], &[])]));
        bytes
    };
    let spread: Vec<u8> = (0..25).flat_map(|_| section(10)).collect();
    assert_eq!(
        Message::decode(&spread, Limits::BODY).map(|decoded| decoded.used),
        Ok(spread.len()),
        "250 nulls in 25 sections are within the 256 allowance"
    );
    let over: Vec<u8> = (0..26).flat_map(|_| section(10)).collect();
    assert_eq!(
        Message::decode(&over, Limits::BODY).map(|decoded| decoded.used),
        Err(DecodeError::ZeroWidthElementsExceeded {
            declared: 10,
            cap: 256,
        }),
        "260 nulls in 26 sections exceed the shared allowance"
    );
    assert!(
        Message::decode(&section(256), Limits::BODY).is_ok(),
        "the same total in one array is within the allowance"
    );
}

/// A random `array32` of zero-width or described elements, nested up to
/// `depth` levels, so the zero-width budget is exercised rather than missed.
fn random_array(rng: &mut Rng, depth: u32) -> Vec<u8> {
    let count = rng.below(40) as u32;
    match rng.below(4) {
        0 => array32(count, &[codes::NULL], &[]),
        1 => array32(
            count,
            &[codes::DESCRIBED, codes::SMALLULONG, 0x24, codes::LIST0],
            &[],
        ),
        2 => {
            let booleans: Vec<u8> = (0..count).map(|_| rng.byte() & 1).collect();
            array32(count, &[codes::BOOLEAN], &booleans)
        }
        _ if depth == 0 => array32(count, &[codes::TRUE], &[]),
        _ => {
            let n = rng.below(4) as u32;
            let mut elements = Vec::new();
            for _ in 0..n {
                elements.extend_from_slice(as_element(&random_array(rng, depth - 1)));
            }
            array32(n, &[codes::ARRAY32], &elements)
        }
    }
}

#[test]
fn the_tree_is_bounded_by_the_input_and_the_budget() {
    let mut rng = Rng::new(0x7EE_51DE);
    let mut accepted = 0usize;
    // The hostile shapes under a budget that admits them, so the bound is
    // checked on a tree that is really built.
    let generous = Limits {
        max_elements: 65_536,
        max_depth: 16,
        max_zero_width_elements: 300_000,
    };
    for input in [
        array32(65_536, &[codes::NULL], &[]),
        array32(
            64,
            &[codes::ARRAY32],
            &elements_of(64, &array32(4096, &[codes::NULL], &[])),
        ),
    ] {
        assert!(check_accepted(&input, generous));
        assert!(!check_accepted(&input, Limits::BODY));
    }
    for round in 0..2000 {
        let input = if round % 2 == 0 {
            random_array(&mut rng, 2)
        } else {
            rng.plausible(48)
        };
        for limits in [LIMITS, Limits::DEFAULT] {
            if check_accepted(&input, limits) {
                accepted += 1;
            }
        }
    }
    assert!(
        accepted > 200,
        "only {accepted} of 4000 decodes were accepted"
    );
}
