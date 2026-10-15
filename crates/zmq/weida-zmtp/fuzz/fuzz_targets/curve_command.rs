//! The CURVE command decoder against arbitrary bodies.
//!
//! Property: decoding never panics, every accepted command keeps its boxes at
//! or above the minimum the layout gives them, and anything accepted re-encodes
//! and decodes back to the same command. CURVE's commands are laid out by
//! count, so a length rule missing on one side shows up as a command that
//! decodes and will not re-encode. The HELLO padding is not part of the
//! command, so the octets need not match; the decoded value must.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_zmtp::curve::{INITIATE_BOX_MIN_LEN, MESSAGE_BOX_MIN_LEN, READY_BOX_MIN_LEN};
use weida_zmtp::{CurveCommand, frame};

fuzz_target!(|data: &[u8]| {
    let Ok(command) = CurveCommand::decode(data) else {
        return;
    };

    match &command {
        CurveCommand::Hello { .. } | CurveCommand::Welcome { .. } => {}
        CurveCommand::Initiate { initiate_box, .. } => {
            assert!(initiate_box.len() >= INITIATE_BOX_MIN_LEN);
        }
        CurveCommand::Ready { ready_box, .. } => {
            assert!(ready_box.len() >= READY_BOX_MIN_LEN);
        }
        CurveCommand::Message { message_box, .. } => {
            assert!(message_box.len() >= MESSAGE_BOX_MIN_LEN);
        }
    }

    let mut body = Vec::new();
    command
        .encode_body(&mut body)
        .expect("an accepted CURVE command re-encodes");
    assert_eq!(
        CurveCommand::decode(&body).expect("and its body decodes"),
        command
    );

    let bytes = command.encode().expect("an accepted CURVE command frames");
    let (_, framed, _) = frame::decode(&bytes, frame::MAX_BODY).expect("its frame decodes");
    assert_eq!(
        CurveCommand::decode(framed).expect("and the framed body decodes"),
        command
    );
});
