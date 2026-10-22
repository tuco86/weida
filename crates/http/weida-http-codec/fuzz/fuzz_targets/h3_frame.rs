//! HTTP/3 frame headers, SETTINGS and GOAWAY payloads against arbitrary
//! bytes.
//!
//! Properties: decoding never panics; accepted SETTINGS hold no duplicate and
//! no HTTP/2 identifier and re-encode to a frame that decodes to the same
//! list; an accepted GOAWAY re-encodes to the same identifier.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_http_codec::Limits;
use weida_http_codec::h3::{self, frame};

fuzz_target!(|data: &[u8]| {
    let Ok((head, used)) = h3::decode_frame_header(data) else {
        return;
    };
    let rest = &data[used..];
    let Ok(len) = usize::try_from(head.len) else {
        return;
    };
    if rest.len() < len {
        return;
    }
    let payload = &rest[..len];
    match head.kind {
        frame::SETTINGS => {
            let Ok(settings) = h3::decode_settings(payload, Limits::DEFAULT) else {
                return;
            };
            for (i, (id, _)) in settings.iter().enumerate() {
                assert!(!(0x2..=0x5).contains(id));
                assert!(settings[..i].iter().all(|(seen, _)| seen != id));
            }
            let mut out = Vec::new();
            h3::encode_settings(&settings, &mut out).expect("decoded values fit");
            let (again, used) = h3::decode_frame_header(&out).unwrap();
            assert_eq!(again.kind, frame::SETTINGS);
            assert_eq!(
                h3::decode_settings(&out[used..], Limits::DEFAULT).unwrap(),
                settings
            );
        }
        frame::GOAWAY => {
            let Ok(id) = h3::decode_goaway(payload) else {
                return;
            };
            let mut out = Vec::new();
            h3::encode_goaway(id, &mut out).unwrap();
            let (_, used) = h3::decode_frame_header(&out).unwrap();
            assert_eq!(h3::decode_goaway(&out[used..]), Ok(id));
        }
        _ => {}
    }
});
