//! Chunked bodies against arbitrary bytes, fed in two pieces at an arbitrary
//! split.
//!
//! Properties: decoding never panics; every step consumes at least one octet
//! and never more than it was given; data is never empty; a body decoded from
//! two pieces equals the body decoded from one.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_http_codec::h1::{Chunk, ChunkedDecoder};
use weida_http_codec::{DecodeError, Limits};

const LIMITS: Limits = Limits {
    max_head_bytes: 1024,
    max_fields: 16,
    max_chunk_line: 64,
    max_frame_size: 16_384,
};

/// Decodes `input` until it ends, is used up, or is refused; returns the
/// octets consumed or the refusal.
fn run(d: &mut ChunkedDecoder, input: &[u8], body: &mut Vec<u8>) -> Result<usize, DecodeError> {
    let mut pos = 0;
    loop {
        match d.step(&input[pos..], LIMITS) {
            Ok((chunk, used)) => {
                assert!(used > 0 && pos + used <= input.len());
                pos += used;
                match chunk {
                    Chunk::Data(data) => {
                        assert!(!data.is_empty());
                        body.extend_from_slice(data);
                    }
                    Chunk::End(_) => return Ok(pos),
                }
            }
            Err(DecodeError::Incomplete { .. }) => return Ok(pos),
            Err(e) => return Err(e),
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let mut whole = Vec::new();
    let mut d = ChunkedDecoder::new();
    let Ok(_) = run(&mut d, data, &mut whole) else {
        return;
    };
    if !d.is_done() || data.is_empty() {
        return;
    }
    let split = usize::from(data[0]) % data.len();
    let mut pieces = Vec::new();
    let mut d = ChunkedDecoder::new();
    let used = run(&mut d, &data[..split], &mut pieces).expect("a prefix is never a violation");
    run(&mut d, &data[used..], &mut pieces).expect("the rest decodes");
    assert!(d.is_done());
    assert_eq!(pieces, whole);
});
