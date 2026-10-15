//! The nine performatives against arbitrary bytes, reached directly rather
//! than through a frame.
//!
//! The frame target needs a well-formed eight-octet header before it gets
//! anywhere near a performative, which costs the fuzzer most of its budget.
//! This target hands the bytes to the performative decoder with a plausible
//! descriptor in front, so the field-by-field decoding — mandatory fields,
//! restricted types, trailing-null trimming — is what gets explored.
//!
//! Properties: decoding never panics; an accepted performative re-encodes and
//! the re-encoding decodes again, to the same canonical octets. The octets are
//! compared, not the values, because a decoded float may be a NaN, which equals
//! nothing, itself included; the round trip is stable over octets, so a
//! peer's wider encoding of a field becomes this crate's canonical one and
//! then stops changing.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_amqp_codec::performative::DESCRIPTORS;
use weida_amqp_codec::{Limits, Performative};

fn exercise(input: &[u8]) {
    let Ok((performative, used)) = Performative::decode(input, Limits::DEFAULT) else {
        return;
    };
    assert!(used <= input.len());
    assert!(used >= 4, "a performative is at least 0x00 code list0");

    let mut written = Vec::new();
    performative
        .encode(&mut written)
        .expect("an accepted performative re-encodes");
    let (again, used2) =
        Performative::decode(&written, Limits::DEFAULT).expect("the re-encoding decodes");
    assert_eq!(used2, written.len());
    // Compared over octets, for the NaN reason in the module documentation.
    assert_eq!(again.descriptor(), performative.descriptor());

    let mut twice = Vec::new();
    again.encode(&mut twice).expect("re-encodes");
    assert_eq!(twice, written, "the canonical encoding is idempotent");
}

fuzz_target!(|data: &[u8]| {
    exercise(data);
    // Behind each of the nine descriptors, so one corpus entry explores all
    // nine field layouts.
    for (code, _) in DESCRIPTORS {
        let mut framed = vec![0x00u8, 0x53, code as u8];
        framed.extend_from_slice(data);
        exercise(&framed);
    }
});
