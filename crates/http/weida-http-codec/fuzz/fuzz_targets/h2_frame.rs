//! HTTP/2 frames against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted frame used exactly its
//! header plus declared length, within the cap; and every frame but an
//! unknown one re-encodes to octets that decode to the same frame.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_http_codec::h2::{Frame, HEADER_LEN};

const MAX: u32 = 16_384;

fuzz_target!(|data: &[u8]| {
    let Ok((frame, used)) = Frame::decode(data, MAX) else {
        return;
    };
    let declared = u32::from_be_bytes([0, data[0], data[1], data[2]]);
    assert!(declared <= MAX);
    assert_eq!(used, HEADER_LEN + declared as usize);
    if matches!(frame, Frame::Unknown { .. }) {
        return;
    }
    let mut out = Vec::new();
    frame.encode(&mut out);
    let (again, again_used) = Frame::decode(&out, MAX).expect("the re-encoding decodes");
    assert_eq!(again_used, out.len());
    assert_eq!(again, frame);
});
