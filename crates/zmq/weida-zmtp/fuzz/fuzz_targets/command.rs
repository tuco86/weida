//! The command decoder against arbitrary bodies.
//!
//! Property: decoding never panics, every accepted command respects the bounds
//! its grammar states, and anything the decoder accepts the encoder can write
//! back - the two halves share their length rules, so one accepting what the
//! other refuses is the bug this target looks for.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_zmtp::{Command, MAX_PING_CONTEXT, MAX_PLAIN_FIELD, MAX_PROPERTIES, frame};

fuzz_target!(|data: &[u8]| {
    let Ok(command) = Command::decode(data) else {
        return;
    };

    match &command {
        Command::Ready(md) | Command::Initiate(md) => {
            assert!(md.properties().len() <= MAX_PROPERTIES);
            for (name, value) in md.properties() {
                assert!(!name.is_empty() && name.len() <= 255);
                assert!(value.len() <= i32::MAX as usize);
            }
        }
        Command::Error(reason) => {
            assert!(reason.len() <= 255);
            assert!(reason.bytes().all(|b| (0x20..=0x7E).contains(&b)));
        }
        Command::Subscribe(_) | Command::Cancel(_) => {}
        Command::Ping { context, .. } | Command::Pong { context } => {
            assert!(context.len() <= MAX_PING_CONTEXT);
        }
        Command::Hello { username, password } => {
            assert!(username.len() <= MAX_PLAIN_FIELD);
            assert!(password.len() <= MAX_PLAIN_FIELD);
        }
        Command::Welcome => {}
    }

    let bytes = command.encode().expect("an accepted command re-encodes");
    let (_, body, _) = frame::decode(&bytes, 64 * 1024).expect("its frame decodes");
    assert_eq!(
        Command::decode(body).expect("and its body decodes"),
        command
    );
});
