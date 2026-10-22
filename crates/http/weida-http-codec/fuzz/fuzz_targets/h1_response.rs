//! HTTP/1.1 response heads against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted head re-encodes to octets
//! that decode to the same head; its framing decision never panics.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_http_codec::{Limits, h1};

const LIMITS: Limits = Limits {
    max_head_bytes: 1024,
    max_fields: 16,
    max_chunk_line: 64,
    max_frame_size: 16_384,
};

fuzz_target!(|data: &[u8]| {
    let Ok((head, used)) = h1::decode_response(data, LIMITS) else {
        return;
    };
    assert!(used <= data.len());
    let _ = h1::response_framing(b"GET", head.status, &head.fields);

    let pairs: Vec<_> = head.fields.iter().map(|f| (f.name, f.value)).collect();
    let mut out = Vec::new();
    h1::encode_response_head(head.minor, head.status, head.reason, &pairs, &mut out)
        .expect("an accepted head re-encodes");
    let (again, _) = h1::decode_response(&out, LIMITS).expect("the re-encoding decodes");
    assert_eq!(again, head);
});
