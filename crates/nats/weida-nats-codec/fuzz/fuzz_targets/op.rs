//! One operation against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted operation's payload is
//! exactly as long as the count its control line declared and is inside the
//! cap; an accepted operation re-encodes, and the re-encoding decodes to the
//! same value with the same octets. The re-encoding is read under the same
//! bounds, except that a `-ERR` line may grow by the two quotes it is always
//! written with (the crate docs, "`-ERR` is read quoted or bare").
//!
//! The cap is deliberately far below the server's documented 1 MiB default,
//! because a NATS control line declares its payload in ASCII: twenty-six
//! octets can announce sixteen exabytes, and a decoder that trusted the
//! number would be killed by the fuzzer's memory limit rather than caught by
//! an assertion.

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

/// The bounds the canonical encoding of `op` is read back under.
fn reread_limits(op: &Op<'_>) -> Limits {
    match op {
        Op::Err { .. } => Limits {
            max_control_line: LIMITS.max_control_line + 2,
            ..LIMITS
        },
        _ => LIMITS,
    }
}

fuzz_target!(|data: &[u8]| {
    let Ok((op, used)) = Op::decode(data, LIMITS) else {
        return;
    };
    assert!(used <= data.len());
    if let Some(payload) = op.payload() {
        assert!(payload.len() as u64 <= LIMITS.max_payload);
        assert!(used >= payload.len() + 2);
    }
    if let Some(headers) = op.headers() {
        assert!(headers.len() as u64 <= u64::from(LIMITS.max_header_entries));
    }

    let mut written = Vec::new();
    op.encode(&mut written)
        .expect("an accepted operation re-encodes");
    let (again, used_again) =
        Op::decode(&written, reread_limits(&op)).expect("the re-encoding decodes");
    assert_eq!(used_again, written.len());
    assert_eq!(again, op);

    let mut twice = Vec::new();
    again.encode(&mut twice).expect("re-encodes");
    assert_eq!(twice, written, "the canonical encoding is idempotent");
});
