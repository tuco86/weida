//! QPACK field sections against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted list stays inside the caps
//! and re-encodes to a section that decodes to the same list.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_http_codec::{Limits, qpack};

const LIMITS: Limits = Limits {
    max_head_bytes: 4096,
    max_fields: 32,
    max_chunk_line: 64,
    max_frame_size: 16_384,
};

fuzz_target!(|data: &[u8]| {
    let mut out = Vec::new();
    if qpack::decode_field_section(data, LIMITS, &mut out).is_err() {
        return;
    }
    assert!(out.len() <= LIMITS.max_fields);
    let fields: Vec<_> = out
        .iter()
        .map(|(n, v)| (n.as_slice(), v.as_slice(), false))
        .collect();
    let mut block = Vec::new();
    qpack::encode_field_section(&fields, &mut block);
    let mut again = Vec::new();
    qpack::decode_field_section(&block, LIMITS, &mut again).expect("the re-encoding decodes");
    assert_eq!(again, out);
});
