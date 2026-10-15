//! The composite decoder against arbitrary bytes.
//!
//! A composite type is a described `list` read by position, and it is the
//! shape every performative, message section and delivery state arrives in.
//! Properties: the cursor never panics; it never yields more non-null fields
//! than the list header declared; and walking past the declared count
//! terminates with nulls rather than looping or reading past the region.
//!
//! The generator is given a plausible described-type prefix, because a
//! uniformly random first octet is `0x00` one time in 256 and the interesting
//! bugs are all past that point.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_amqp_codec::{Limits, decode};

const LIMITS: Limits = Limits {
    max_elements: 64,
    max_depth: 8,
    max_zero_width_elements: 16,
};

fuzz_target!(|data: &[u8]| {
    // Try the bytes as they came, and again behind each of the three
    // descriptor forms, so that one corpus entry exercises all of them.
    let mut attempts: Vec<Vec<u8>> = vec![data.to_vec()];
    for prefix in [
        vec![0x00u8, 0x53, 0x10],
        vec![0x00, 0x80, 0, 0, 0, 0, 0, 0, 0x10],
        vec![0x00, 0xa3, 0x0e],
    ] {
        let mut framed = prefix;
        framed.extend_from_slice(data);
        attempts.push(framed);
    }

    for input in attempts {
        let Ok(mut composite) = decode::composite(&input, LIMITS) else {
            continue;
        };
        assert!(composite.used <= input.len());
        let declared = composite.fields.remaining();
        assert!(declared <= LIMITS.max_elements);

        let mut seen = 0u32;
        // A field that fails to decode ends the walk early, so the fields the
        // header still owes are not consumed and the count is not zero then.
        let mut cut_short = false;
        for _ in 0..declared.saturating_add(4) {
            match composite.fields.next_value() {
                Ok(value) => {
                    if seen < declared {
                        seen += 1;
                    } else {
                        assert!(value.is_null(), "past the declared count, every field is null");
                    }
                }
                Err(_) => {
                    cut_short = true;
                    break;
                }
            }
        }
        if !cut_short {
            assert_eq!(composite.fields.remaining(), 0);
        }
    }
});
