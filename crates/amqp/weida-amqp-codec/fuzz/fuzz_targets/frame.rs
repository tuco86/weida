//! The frame header and the performative dispatch against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted frame is inside the
//! ceiling; its body is inside the frame; a performative decoded from that
//! body re-encodes to something a decoder reads back to the same octets.
//!
//! The ceiling is the pre-negotiation 512, which is the one AMQP applies
//! before `open` has been read. A four-byte `SIZE` field can declare 4 GiB
//! and there is no credit at all at that point, so a decoder that trusted it
//! would be killed by the fuzzer's memory limit rather than caught by an
//! assertion.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_amqp_codec::frame::{self, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::{Limits, Performative, SaslFrame, protocol_header};

fuzz_target!(|data: &[u8]| {
    // The eight octets before any frame.
    let _ = protocol_header::decode(data);

    if let Ok(read) = frame::decode(data, MIN_MAX_FRAME_SIZE) {
        assert!(read.header.size <= MIN_MAX_FRAME_SIZE);
        assert!(read.header.size >= 8);
        assert!(read.header.doff >= 2);
        assert!(read.used() <= data.len());
        assert_eq!(read.body.len(), read.header.body_len());
        assert!(read.header.body_offset() <= read.used());

        match read.header.kind {
            frame::FrameKind::Amqp => {
                if let Ok((performative, used)) =
                    Performative::decode(read.body, Limits::DEFAULT)
                {
                    assert!(used <= read.body.len());
                    let mut written = Vec::new();
                    performative
                        .encode(&mut written)
                        .expect("an accepted performative re-encodes");
                    let (again, used2) = Performative::decode(&written, Limits::DEFAULT)
                        .expect("the re-encoding decodes");
                    assert_eq!(used2, written.len());
                    let mut twice = Vec::new();
                    again.encode(&mut twice).expect("re-encodes");
                    // Over octets, not values: a decoded float may be a NaN.
                    assert_eq!(twice, written, "the canonical encoding is a fixed point");
                }
            }
            frame::FrameKind::Sasl => {
                if let Ok((body, used)) = SaslFrame::decode(read.body, Limits::DEFAULT) {
                    assert!(used <= read.body.len());
                    let mut written = Vec::new();
                    body.encode(&mut written).expect("an accepted body re-encodes");
                    let (again, used2) = SaslFrame::decode(&written, Limits::DEFAULT)
                        .expect("the re-encoding decodes");
                    assert_eq!(used2, written.len());
                    assert_eq!(again, body);
                }
            }
        }
    }
});
