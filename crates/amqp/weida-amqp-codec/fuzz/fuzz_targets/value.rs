//! The type-system decoder against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted value is inside both of the
//! caller's bounds; the canonical encoding of an accepted value is never
//! wider than what arrived, is accepted again under the same bounds, and is a
//! fixed point of encode-decode-encode.
//!
//! The bounds are small on purpose. A nine-octet `list32` header can declare
//! 2^32-1 elements and a five-octet `vbin32` header 4 GiB, and AMQP grants no
//! credit before `open` has been read, so the caller's `Limits` is the whole
//! defence: a decoder that reserved a declared count would be killed by the
//! fuzzer's memory limit rather than caught by an assertion, and either way
//! this target is where it shows.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_amqp_codec::{Limits, Value, decode, encode};

const LIMITS: Limits = Limits {
    max_elements: 64,
    max_depth: 8,
    max_zero_width_elements: 16,
};

fn check(value: &Value<'_>, depth: u32) {
    assert!(depth < LIMITS.max_depth);
    match value {
        Value::List(items) => {
            assert!(items.len() as u64 <= u64::from(LIMITS.max_elements));
            for item in items {
                check(item, depth + 1);
            }
        }
        Value::Map(entries) => {
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
        Value::Symbol(text) => assert!(text.is_ascii()),
        _ => {}
    }
}

fuzz_target!(|data: &[u8]| {
    if let Ok((value, used)) = decode::value(data, LIMITS) {
        assert!(used <= data.len());
        assert!(used >= 1);
        check(&value, 0);

        let written = encode::to_vec(&value).expect("an accepted value re-encodes");
        assert!(written.len() <= used, "canonical is never wider");
        let (again, used2) =
            decode::value(&written, LIMITS).expect("the re-encoding decodes under the same limits");
        assert_eq!(used2, written.len());
        check(&again, 0);
        assert_eq!(
            encode::to_vec(&again).expect("re-encodes"),
            written,
            "the canonical encoding is idempotent"
        );
    }
});
