//! The narrow JSON reader against arbitrary bytes, through both objects that
//! use it.
//!
//! `INFO` arrives from a server that a client has not authenticated yet, so
//! its JSON is the earliest untrusted input in the protocol — earlier than
//! any subject or payload. The reader is hand-written and must not have a
//! panic, an unbounded array or an unbounded recursion in it.
//!
//! Properties: reading never panics; `connect_urls` never exceeds the cap; a
//! `CONNECT` that reads back writes an object that reads to the same fields
//! and encodes to the same octets.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_nats_codec::{Connect, Limits, ServerInfo};

const LIMITS: Limits = Limits {
    max_payload: 4096,
    max_control_line: 256,
    max_info_line: 1024,
    max_header_entries: 8,
    max_connect_urls: 4,
};

fuzz_target!(|data: &[u8]| {
    if let Ok(info) = ServerInfo::parse(data, LIMITS) {
        if let Some(urls) = &info.connect_urls {
            assert!(urls.len() as u64 <= u64::from(LIMITS.max_connect_urls));
        }
    }

    if let Ok(connect) = Connect::parse(data, LIMITS) {
        let mut written = Vec::new();
        connect.write_json(&mut written);
        let again = Connect::parse(&written, LIMITS).expect("what we wrote reads back");
        assert_eq!(again, connect);
        let mut twice = Vec::new();
        again.write_json(&mut twice);
        assert_eq!(twice, written, "the canonical encoding is idempotent");
        assert!(
            !written.contains(&b'\r') && !written.contains(&b'\n'),
            "a CONNECT object never holds a raw newline"
        );
    }
});
