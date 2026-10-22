//! HTTP/1.1 request heads against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted head stays inside the caps
//! and re-encodes to octets that decode to the same method, target and fields;
//! its framing decision never panics.

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
    let Ok((head, used)) = h1::decode_request(data, LIMITS) else {
        return;
    };
    assert!(used <= data.len() && used <= LIMITS.max_head_bytes);
    assert!(head.fields.len() <= LIMITS.max_fields);
    let _ = h1::request_framing(&head.fields);

    let pairs: Vec<_> = head.fields.iter().map(|f| (f.name, f.value)).collect();
    let mut out = Vec::new();
    h1::encode_request_head(head.method, head.target, &pairs, &mut out)
        .expect("an accepted head re-encodes");
    let (again, _) = h1::decode_request(&out, LIMITS).expect("the re-encoding decodes");
    assert_eq!(again.method, head.method);
    assert_eq!(again.target, head.target);
    assert_eq!(again.fields, head.fields);
});
