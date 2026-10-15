//! The plaintext of a CURVE `INITIATE` box against arbitrary bytes.
//!
//! Property: decoding never panics, an accepted plaintext is at least the
//! fixed part long, its dictionary respects the property cap, and it re-encodes
//! to the same octets and decodes back to the same value. The dictionary is the
//! only variable part, and it is the one a peer can make large.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_zmtp::curve::INITIATE_PLAINTEXT_MIN_LEN;
use weida_zmtp::{InitiatePlaintext, MAX_PROPERTIES};

fuzz_target!(|data: &[u8]| {
    let Ok(plaintext) = InitiatePlaintext::decode(data) else {
        return;
    };
    assert!(data.len() >= INITIATE_PLAINTEXT_MIN_LEN);
    assert!(plaintext.metadata.properties().len() <= MAX_PROPERTIES);

    let mut re = Vec::new();
    plaintext
        .encode(&mut re)
        .expect("an accepted INITIATE plaintext re-encodes");
    assert_eq!(re, data);
    assert_eq!(
        InitiatePlaintext::decode(&re).expect("and re-decodes"),
        plaintext
    );
});
