//! A whole stream of operations, decoded the way a reader does it.
//!
//! The single-operation target cannot see the framing bug that matters most:
//! a decoder that reported the wrong number of octets consumed, or that found
//! a payload's `CRLF` instead of its own, would still decode each operation
//! in isolation and would still round trip. It is only when the next
//! operation is read from `input[used..]` that the error shows up — as a
//! sudden violation in the middle of a stream that was legal so far.
//!
//! Properties: every operation in the stream consumes at least one octet and
//! at most what is left; the stream ends in `Incomplete` or in a violation,
//! never in an infinite loop; re-encoding the whole stream and reading it
//! back yields the same sequence of operations.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_nats_codec::{Limits, Op};

const LIMITS: Limits = Limits {
    max_payload: 4096,
    max_control_line: 256,
    max_info_line: 1024,
    max_header_entries: 8,
    max_connect_urls: 4,
};

fuzz_target!(|data: &[u8]| {
    let mut at = 0;
    let mut ops = Vec::new();
    while at < data.len() {
        let Ok((op, used)) = Op::decode(&data[at..], LIMITS) else {
            break;
        };
        assert!(used > 0, "a decoded operation always consumes octets");
        assert!(at + used <= data.len());
        at += used;
        ops.push(op);
    }

    let mut written = Vec::new();
    for op in &ops {
        op.encode(&mut written)
            .expect("an accepted operation re-encodes");
    }

    let mut at = 0;
    for op in &ops {
        let (again, used) = Op::decode(&written[at..], LIMITS).expect("the re-encoding decodes");
        assert_eq!(&again, op);
        at += used;
    }
    assert_eq!(at, written.len(), "the re-encoded stream is fully consumed");
});
