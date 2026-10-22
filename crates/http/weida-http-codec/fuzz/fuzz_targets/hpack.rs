//! HPACK field blocks against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted list stays inside the caps
//! and re-encodes, through a fresh encoder, to a block a fresh decoder reads
//! as the same list.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_http_codec::Limits;
use weida_http_codec::hpack::{Decoder, Encoder};

const LIMITS: Limits = Limits {
    max_head_bytes: 4096,
    max_fields: 32,
    max_chunk_line: 64,
    max_frame_size: 16_384,
};

fuzz_target!(|data: &[u8]| {
    let mut out = Vec::new();
    if Decoder::new(4096).decode(data, LIMITS, &mut out).is_err() {
        return;
    }
    assert!(out.len() <= LIMITS.max_fields);
    let fields: Vec<_> = out
        .iter()
        .map(|(n, v)| (n.as_slice(), v.as_slice(), false))
        .collect();
    let mut block = Vec::new();
    Encoder::new().encode(&fields, &mut block);
    let mut again = Vec::new();
    Decoder::new(4096)
        .decode(&block, LIMITS, &mut again)
        .expect("the re-encoding decodes");
    assert_eq!(again, out);
});
