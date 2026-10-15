//! Deterministic hostile-input smoke tests for the NATS control line.
//!
//! The always-on companion to the `cargo fuzz` targets under
//! `crates/nats/weida-nats-codec/fuzz`: the same properties, driven by a
//! seeded xorshift generator so they run on stable Rust, in `cargo test`,
//! with no nightly toolchain and no corpus. The libFuzzer targets explore far
//! deeper; these make sure the properties are never left unchecked.
//!
//! The properties:
//!
//! 1. **Decoding never panics.** Every input goes through
//!    [`Op::decode`], and an error is only ever a return value.
//! 2. **An accepted operation's payload is exactly its declared count, and
//!    inside the cap.** The count is re-derived from the octets the encoder
//!    writes and compared against the payload the decoder produced, so an
//!    encoder that declared one length and wrote another fails here.
//! 3. **An accepted operation re-encodes, and the re-encoding decodes to the
//!    same value.** Encoding is a fixed point: encoding twice gives the same
//!    octets.
//! 4. **A declared count is never believed.**
//!    `a_declared_count_is_never_believed` sends control lines announcing
//!    sixteen exabytes in a few dozen octets. None of them may reserve
//!    anything: a decoder that did would have the test process killed by the
//!    allocator rather than fail an assertion.
//!
//! The generator is biased towards well-formed operations on purpose. A
//! uniformly random buffer is refused by the verb match essentially always,
//! so it would exercise nothing but `UnknownVerb`; each test therefore
//! asserts a minimum acceptance rate, and a generator that stops reaching the
//! decoder fails rather than passing vacuously.

use weida_nats_codec::{DecodeError, Headers, Limits, Op};

const ITERATIONS: usize = 100_000;

/// Deliberately small: a declared count above these must be refused from the
/// control line alone, and a few dozen octets can never satisfy one by
/// accident.
const LIMITS: Limits = Limits {
    max_payload: 4096,
    max_control_line: 256,
    max_info_line: 1024,
    max_header_entries: 8,
    max_connect_urls: 4,
};

/// Deterministic xorshift64* generator: no dependency, reproducible failures.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn byte(&mut self) -> u8 {
        (self.next_u64() >> 33) as u8
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() >> 33) as usize % n
    }

    fn bytes(&mut self, max: usize) -> Vec<u8> {
        let len = self.below(max + 1);
        (0..len).map(|_| self.byte()).collect()
    }

    /// A payload that holds the terminator often enough to matter: the whole
    /// point of the length-delimited body is that `CRLF` in a payload is
    /// ordinary application data.
    fn payload(&mut self) -> Vec<u8> {
        let mut payload = self.bytes(24);
        match self.below(4) {
            0 => payload.extend_from_slice(b"\r\n"),
            1 => {
                let at = if payload.is_empty() {
                    0
                } else {
                    self.below(payload.len())
                };
                payload.splice(at..at, *b"\r\nPING\r\n");
            }
            _ => {}
        }
        payload
    }

    fn token(&mut self) -> &'static [u8] {
        const TOKENS: [&[u8]; 8] = [
            b"FOO",
            b"foo.bar.baz",
            b"_INBOX.7Yz2K1.4",
            b"1",
            b"44",
            b"G1",
            b"orders.>",
            b"a",
        ];
        TOKENS[self.below(TOKENS.len())]
    }

    /// A count argument: usually the truth, sometimes a lie, sometimes not a
    /// number at all.
    fn count(&mut self, truth: usize) -> Vec<u8> {
        match self.below(8) {
            0 => self.bytes(4),
            1 => Vec::from(*b"18446744073709551616"),
            2 => truth.wrapping_add(1).to_string().into_bytes(),
            _ => truth.to_string().into_bytes(),
        }
    }

    fn header_block(&mut self) -> Vec<u8> {
        const NAMES: [&[u8]; 5] = [b"Bar", b"Nats-Msg-Id", b"BREAKFAST", b"x", b"Content-Type"];
        const VALUES: [&[u8]; 4] = [b"Baz", b"7", b"donut", b"application/json"];
        let mut block = Vec::from(*b"NATS/1.0");
        match self.below(6) {
            0 => block.extend_from_slice(b" 503"),
            1 => block.extend_from_slice(b" 100 Idle Heartbeat"),
            2 => block.extend_from_slice(b" nonsense"),
            _ => {}
        }
        block.extend_from_slice(b"\r\n");
        for _ in 0..self.below(10) {
            block.extend_from_slice(NAMES[self.below(NAMES.len())]);
            block.extend_from_slice(if self.below(3) == 0 { b":" } else { b": " });
            block.extend_from_slice(VALUES[self.below(VALUES.len())]);
            block.extend_from_slice(b"\r\n");
        }
        if self.below(8) != 0 {
            block.extend_from_slice(b"\r\n");
        }
        block
    }

    /// One operation, well formed most of the time.
    fn operation(&mut self) -> Vec<u8> {
        let mut line: Vec<u8> = Vec::new();
        let payload = self.payload();
        let body: Vec<u8> = match self.below(12) {
            0 => {
                line.extend_from_slice(b"INFO ");
                line.extend_from_slice(
                    br#"{"server_id":"S","proto":1,"max_payload":1048576,"headers":true}"#,
                );
                Vec::new()
            }
            1 => {
                line.extend_from_slice(b"CONNECT ");
                line.extend_from_slice(
                    br#"{"verbose":false,"pedantic":false,"tls_required":false,"lang":"rust","protocol":1}"#,
                );
                Vec::new()
            }
            2 => {
                line.extend_from_slice(b"PUB ");
                line.extend_from_slice(self.token());
                if self.below(2) == 0 {
                    line.push(b' ');
                    line.extend_from_slice(self.token());
                }
                line.push(b' ');
                let count = self.count(payload.len());
                line.extend_from_slice(&count);
                payload
            }
            3 | 4 => {
                let block = self.header_block();
                line.extend_from_slice(if self.below(2) == 0 {
                    b"HPUB "
                } else {
                    b"HMSG "
                });
                line.extend_from_slice(self.token());
                if line.starts_with(b"HMSG") {
                    line.push(b' ');
                    line.extend_from_slice(self.token());
                }
                if self.below(2) == 0 {
                    line.push(b' ');
                    line.extend_from_slice(self.token());
                }
                line.push(b' ');
                line.extend_from_slice(&self.count(block.len()));
                line.push(b' ');
                line.extend_from_slice(&self.count(block.len() + payload.len()));
                block.iter().chain(payload.iter()).copied().collect()
            }
            5 => {
                line.extend_from_slice(b"SUB ");
                line.extend_from_slice(self.token());
                if self.below(2) == 0 {
                    line.push(b' ');
                    line.extend_from_slice(self.token());
                }
                line.push(b' ');
                line.extend_from_slice(self.token());
                Vec::new()
            }
            6 => {
                line.extend_from_slice(b"UNSUB ");
                line.extend_from_slice(self.token());
                if self.below(2) == 0 {
                    line.push(b' ');
                    line.extend_from_slice(&self.count(self.0 as usize % 1000));
                }
                Vec::new()
            }
            7 | 8 => {
                line.extend_from_slice(b"MSG ");
                line.extend_from_slice(self.token());
                line.push(b' ');
                line.extend_from_slice(self.token());
                if self.below(2) == 0 {
                    line.push(b' ');
                    line.extend_from_slice(self.token());
                }
                line.push(b' ');
                line.extend_from_slice(&self.count(payload.len()));
                payload
            }
            9 => {
                line.extend_from_slice([&b"PING"[..], b"PONG", b"+OK", b"ping"][self.below(4)]);
                Vec::new()
            }
            10 => {
                line.extend_from_slice(b"-ERR '");
                line.extend_from_slice(
                    [
                        &b"Stale Connection"[..],
                        b"Unknown Protocol Operation",
                        b"Permissions Violation for Publish to 'a.b'",
                    ][self.below(3)],
                );
                line.push(b'\'');
                Vec::new()
            }
            _ => {
                // A verb-shaped line with entirely random arguments.
                line.extend_from_slice(
                    [&b"PUB"[..], b"HPUB", b"SUB", b"UNSUB", b"MSG", b"HMSG"][self.below(6)],
                );
                for _ in 0..self.below(6) {
                    line.push(b' ');
                    line.extend_from_slice(&self.bytes(4));
                }
                payload
            }
        };
        let mut input = line;
        input.extend_from_slice(b"\r\n");
        input.extend_from_slice(&body);
        if !body.is_empty() || self.below(2) == 0 {
            input.extend_from_slice(b"\r\n");
        }
        input
    }
}

/// The arguments of the control line at the front of `written`.
fn arguments(written: &[u8]) -> Vec<&[u8]> {
    let end = written
        .windows(2)
        .position(|pair| pair == b"\r\n")
        .expect("an encoded operation has a control line");
    written[..end].split(|byte| *byte == b' ').collect()
}

fn number(raw: &[u8]) -> u64 {
    core::str::from_utf8(raw)
        .expect("a count is ASCII")
        .parse()
        .expect("a count is decimal")
}

/// Everything an accepted operation must satisfy.
fn check(op: &Op<'_>, used: usize, input: &[u8]) {
    assert!(used <= input.len());
    if let Some(payload) = op.payload() {
        assert!(
            payload.len() as u64 <= LIMITS.max_payload,
            "an accepted payload is inside the cap"
        );
        assert!(used >= payload.len() + 2, "the payload is inside the input");
    }

    let mut written = Vec::new();
    op.encode(&mut written)
        .expect("an accepted operation re-encodes");

    // The declared count and the payload are the same number, checked off
    // the octets rather than off the struct.
    let arguments = arguments(&written);
    match op {
        Op::Pub { payload, .. } | Op::Msg { payload, .. } => {
            assert_eq!(
                number(arguments[arguments.len() - 1]),
                payload.len() as u64,
                "the declared count is the payload length"
            );
        }
        Op::Hpub {
            headers, payload, ..
        }
        | Op::Hmsg {
            headers, payload, ..
        } => {
            let header_len = headers.encoded_len() as u64;
            assert_eq!(number(arguments[arguments.len() - 2]), header_len);
            assert_eq!(
                number(arguments[arguments.len() - 1]),
                header_len + payload.len() as u64,
                "the total is headers plus payload"
            );
        }
        _ => {}
    }

    let (again, used_again) = Op::decode(&written, LIMITS).expect("the re-encoding decodes");
    assert_eq!(used_again, written.len());
    assert_eq!(&again, op, "the round trip is lossless");
    let mut twice = Vec::new();
    again.encode(&mut twice).expect("re-encodes");
    assert_eq!(twice, written, "the canonical encoding is idempotent");
}

#[test]
fn fuzz_smoke_op() {
    let mut rng = Rng::new(0x4e41_5453);
    let mut accepted = 0usize;
    for _ in 0..ITERATIONS {
        let input = if rng.below(8) == 0 {
            rng.bytes(48)
        } else {
            rng.operation()
        };
        match Op::decode(&input, LIMITS) {
            Ok((op, used)) => {
                accepted += 1;
                check(&op, used, &input);
            }
            Err(error) => assert_eq!(
                error.is_violation(),
                !matches!(error, DecodeError::Incomplete { .. })
            ),
        }
    }
    assert!(
        accepted > ITERATIONS / 4,
        "the generator must reach the decoder: only {accepted} of \
         {ITERATIONS} inputs decoded"
    );
}

#[test]
fn fuzz_smoke_prefixes() {
    // Every prefix of a well-formed operation must be `Incomplete`, never a
    // violation and never a shorter operation: that is what makes it safe for
    // a reader to decode after every read.
    let mut rng = Rng::new(0x5052_4546);
    for _ in 0..ITERATIONS / 10 {
        let whole = rng.operation();
        let Ok((op, used)) = Op::decode(&whole, LIMITS) else {
            continue;
        };
        let cut = rng.below(used.max(1));
        match Op::decode(&whole[..cut], LIMITS) {
            Ok((partial, _)) => {
                panic!("{cut} of {used} octets decoded to {partial:?} (whole: {op:?})")
            }
            Err(error) => assert!(
                !error.is_violation(),
                "a prefix of a legal operation is not a violation: {error}"
            ),
        }
    }
}

#[test]
fn fuzz_smoke_headers() {
    let mut rng = Rng::new(0x4844_5253);
    let mut accepted = 0usize;
    for _ in 0..ITERATIONS {
        let block = if rng.below(8) == 0 {
            rng.bytes(48)
        } else {
            rng.header_block()
        };
        match Headers::decode(&block, LIMITS) {
            Ok(headers) => {
                accepted += 1;
                assert!(headers.len() as u64 <= u64::from(LIMITS.max_header_entries));
                assert!(
                    headers.status.is_some() || headers.description.is_none(),
                    "a description without a status cannot be decoded"
                );
                let mut written = Vec::new();
                headers
                    .encode(&mut written)
                    .expect("an accepted block re-encodes");
                assert_eq!(
                    written.len(),
                    headers.encoded_len(),
                    "the declared header count and the block written must agree"
                );
                assert_eq!(
                    Headers::decode(&written, LIMITS).expect("the re-encoding decodes"),
                    headers
                );
            }
            Err(error) => assert!(
                error.is_violation(),
                "a header block is length-delimited and can never be incomplete: {error}"
            ),
        }
    }
    assert!(
        accepted > ITERATIONS / 4,
        "the generator must reach the header decoder: only {accepted} of \
         {ITERATIONS} inputs decoded"
    );
}

#[test]
fn a_declared_count_is_never_believed() {
    // Control lines announcing up to sixteen exabytes, each in a buffer of a
    // few dozen octets. None of these may reserve anything: the test process
    // would be killed rather than the assertion failing.
    for line in [
        &b"PUB FOO 18446744073709551615\r\n"[..],
        &b"PUB FOO reply 18446744073709551615\r\n"[..],
        &b"MSG FOO.BAR 9 18446744073709551615\r\n"[..],
        &b"MSG FOO.BAR 9 reply 18446744073709551615\r\n"[..],
        &b"HPUB FOO 12 18446744073709551615\r\n"[..],
        &b"HMSG FOO.BAR 9 12 18446744073709551615\r\n"[..],
    ] {
        assert!(line.len() < 64, "the buffer really is tiny");
        assert_eq!(
            Op::decode(line, LIMITS),
            Err(DecodeError::PayloadTooLarge {
                declared: u64::MAX,
                cap: LIMITS.max_payload
            }),
            "{}",
            String::from_utf8_lossy(line)
        );
    }

    // One digit past `u64`: refused from the digits alone, before the number
    // means anything.
    assert_eq!(
        Op::decode(b"PUB FOO 18446744073709551616\r\n", LIMITS),
        Err(DecodeError::NumberTooLarge {
            verb: "PUB",
            argument: "#bytes"
        })
    );

    // And with the cap opened all the way, the same line is merely
    // incomplete — still without reserving the sixteen exabytes it asks for.
    let boundless = LIMITS.with_max_payload(u64::MAX);
    assert!(matches!(
        Op::decode(b"PUB FOO 18446744073709551615\r\n", boundless),
        Err(DecodeError::Incomplete { .. })
    ));

    // A header count above the total is refused before the body is touched,
    // because the payload would be negative.
    assert_eq!(
        Op::decode(b"HPUB FOO 4096 12\r\n", LIMITS),
        Err(DecodeError::HeaderBytesAboveTotal {
            header: 4096,
            total: 12
        })
    );
}
