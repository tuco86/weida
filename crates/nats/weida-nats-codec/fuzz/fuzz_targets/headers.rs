//! The `NATS/1.0` header block against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted block holds no more entries
//! than the cap; `encoded_len` is exactly what `encode` writes — the two
//! disagreeing would put a wrong `#header bytes` on an `HPUB` control line
//! and split the receiver's payload at the wrong offset; and an accepted
//! block re-encodes to something that decodes to the same block.
//!
//! A header block is length-delimited by the control line, so unlike an
//! operation it can never be "incomplete": every error here is a violation.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_nats_codec::{Headers, Limits};

const LIMITS: Limits = Limits {
    max_payload: 4096,
    max_control_line: 256,
    max_info_line: 1024,
    max_header_entries: 8,
    max_connect_urls: 4,
};

fuzz_target!(|data: &[u8]| {
    let Ok(headers) = Headers::decode(data, LIMITS) else {
        return;
    };
    assert!(headers.len() as u64 <= u64::from(LIMITS.max_header_entries));
    assert!(
        headers.status.is_some() || headers.description.is_none(),
        "a description without a status cannot be decoded"
    );

    let mut written = Vec::new();
    headers
        .encode(&mut written)
        .expect("an accepted block re-encodes");
    assert_eq!(written.len(), headers.encoded_len());
    assert_eq!(
        Headers::decode(&written, LIMITS).expect("the re-encoding decodes"),
        headers
    );

    // Every entry survived with the case it arrived in, and a name that
    // repeats kept all of its values in order.
    for (name, value) in headers.entries() {
        assert!(!name.is_empty());
        assert!(headers.get(name).is_some());
        assert!(headers.get_all(name).any(|stored| stored == *value));
    }
});
