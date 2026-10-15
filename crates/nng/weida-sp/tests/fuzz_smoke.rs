//! Deterministic hostile-input smoke tests for the SP codec.
//!
//! The always-on companion to the `cargo fuzz` targets under
//! `crates/nng/weida-sp/fuzz`: the same properties, driven by a seeded
//! xorshift generator so they run on stable Rust, in `cargo test`, with no
//! nightly toolchain and no corpus. The libFuzzer targets explore far deeper;
//! these make sure the properties are never left unchecked.
//!
//! The property that matters most is the one SP gives no help with. A message
//! may declare 2^64-1 octets [rfc-tcp §3], there is no credit on the wire
//! [nanomsg-nng §12/P12], and `RECVMAXSZ` is unlimited by default
//! [nanomsg-nng §5], so the decoder's cap is the whole defence: **an accepted
//! message never reports a length above the cap, and a rejected one allocates
//! nothing.** The tests assert the first directly and the second by
//! construction — every input here is a few dozen octets, so a decoder that
//! reserved the declared length would abort the test process rather than fail
//! it.

use weida_sp::error::{HeaderError, MessageError, TagError};
use weida_sp::{Backtrace, EndpointType, ProtocolHeader, backtrace, message, pair};

const ITERATIONS: usize = 100_000;
/// Deliberately small: every declared length above it must be refused from
/// the size field alone, and a 64-octet input can never satisfy it by
/// accident.
const CAP: u64 = 4096;

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

    /// Random bytes, biased towards short inputs where the interesting
    /// truncations live.
    fn bytes(&mut self, max: usize) -> Vec<u8> {
        let len = self.below(max + 1);
        (0..len).map(|_| self.byte()).collect()
    }
}

#[test]
fn fuzz_smoke_protocol_header() {
    let mut rng = Rng::new(0x5091);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(12);
        match ProtocolHeader::decode(&input) {
            Ok(header) => {
                assert_eq!(&header.encode()[..], &input[..8]);
                assert!(header.accepts(header.endpoint.peer()));
            }
            Err(HeaderError::Incomplete) => assert!(input.len() < 8),
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_protocol_header_with_a_valid_prefix() {
    // Random bytes almost never start with the magic, so the interesting
    // inputs have to be built: a real header with one octet corrupted.
    let mut rng = Rng::new(0xC0DE);
    let types = [
        EndpointType::Req,
        EndpointType::Rep,
        EndpointType::Pub,
        EndpointType::Sub,
        EndpointType::Push,
        EndpointType::Pull,
        EndpointType::Surveyor,
        EndpointType::Respondent,
        EndpointType::Bus,
        EndpointType::PairV0,
        EndpointType::PairV1,
    ];
    for _ in 0..ITERATIONS {
        let endpoint = types[rng.below(types.len())];
        let mut header = ProtocolHeader::new(endpoint).encode();
        let index = rng.below(8);
        header[index] ^= rng.byte();
        match ProtocolHeader::decode(&header) {
            Ok(back) => {
                // The only corruption a decode may survive is one that lands
                // on a byte it does not constrain - there is none, so an
                // accepted header must be byte-identical to a real one.
                assert_eq!(back.encode(), header);
            }
            Err(e) => assert!(e.is_violation(), "every header rule is fatal"),
        }
    }
}

#[test]
fn fuzz_smoke_message() {
    let mut rng = Rng::new(0x5A17);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(24);
        match message::decode(&input, CAP) {
            Ok((body, used)) => {
                assert!(body.len() as u64 <= CAP);
                assert_eq!(used, message::SIZE_LEN + body.len());
                assert!(used <= input.len());
            }
            Err(MessageError::Incomplete) => {}
            Err(MessageError::BodyTooLarge { len, max }) => {
                assert!(len > max);
                assert_eq!(max, CAP);
            }
        }
    }
}

#[test]
fn fuzz_smoke_message_stream_always_terminates() {
    let mut rng = Rng::new(0xFEED);
    for _ in 0..ITERATIONS / 10 {
        // A well-formed prefix followed by garbage: the interesting shape,
        // because alignment must survive the good part and stop cleanly at
        // the bad one.
        let mut input = message::encode(&rng.bytes(8));
        input.extend_from_slice(&message::encode(&rng.bytes(4)));
        input.extend_from_slice(&rng.bytes(12));

        let mut rest = &input[..];
        let mut consumed = 0usize;
        let mut messages = 0usize;
        while let Ok((body, used)) = message::decode(rest, CAP) {
            assert!(used >= message::SIZE_LEN);
            assert_eq!(used - message::SIZE_LEN, body.len());
            consumed += used;
            messages += 1;
            rest = &rest[used..];
            assert!(messages <= input.len(), "decode must make progress");
        }
        assert!(messages >= 2, "the two well-formed messages decode");
        assert!(consumed <= input.len());
    }
}

#[test]
fn fuzz_smoke_backtrace() {
    let mut rng = Rng::new(0xBEEF);
    const MAX_HOPS: usize = 4;
    for _ in 0..ITERATIONS {
        let input = rng.bytes(28);
        match backtrace::decode(&input, MAX_HOPS) {
            Ok((stack, payload)) => {
                assert!(stack.peers.len() <= MAX_HOPS);
                assert!(stack.id <= backtrace::MAX_ID);
                assert_eq!(stack.encoded_len() + payload.len(), input.len());
                assert_eq!(stack.encode_message(payload), input);
            }
            Err(TagError::Truncated) => {}
            Err(TagError::NoTerminator { hops, max_hops }) => {
                assert_eq!(hops, MAX_HOPS);
                assert_eq!(max_hops, MAX_HOPS);
                // Reading stopped at the bound, not at the end of the input.
                assert!(input.len() >= (MAX_HOPS + 1) * backtrace::TAG_LEN);
            }
            Err(e) => panic!("a tag stack cannot report {e}"),
        }
    }
}

#[test]
fn fuzz_smoke_measure_agrees_with_decode() {
    let mut rng = Rng::new(0xA11E);
    const MAX_HOPS: usize = 4;
    for _ in 0..ITERATIONS {
        let input = rng.bytes(28);
        match (
            backtrace::measure(&input, MAX_HOPS),
            backtrace::decode(&input, MAX_HOPS),
        ) {
            (Ok(shape), Ok((stack, payload))) => {
                assert_eq!(shape.len, stack.encoded_len());
                assert_eq!(shape.peers, stack.peers.len());
                assert_eq!(shape.id, stack.id);
                assert_eq!(input.len() - shape.len, payload.len());
            }
            (Err(measured), Err(decoded)) => assert_eq!(measured, decoded),
            (measured, decoded) => panic!("{measured:?} against {decoded:?} on {input:02x?}"),
        }
    }
}

#[test]
fn fuzz_smoke_backtrace_round_trips_whatever_it_accepts() {
    let mut rng = Rng::new(0x7AC5);
    for _ in 0..ITERATIONS / 10 {
        let mut stack = Backtrace::direct(rng.next_u64() as u32);
        for _ in 0..rng.below(5) {
            stack.push_peer(rng.next_u64() as u32);
        }
        let payload = rng.bytes(16);
        let body = stack.encode_message(&payload);
        let (back, decoded) = backtrace::decode(&body, 8).expect("what we encoded decodes");
        assert_eq!(back, stack);
        assert_eq!(decoded, &payload[..]);
    }
}

#[test]
fn fuzz_smoke_pair() {
    let mut rng = Rng::new(0x9A17);
    const MAX_HOPS: u32 = 8;
    for _ in 0..ITERATIONS {
        let input = rng.bytes(12);
        match pair::decode(&input, MAX_HOPS) {
            Ok((hops, payload)) => {
                assert!(hops <= MAX_HOPS);
                assert_eq!(pair::HEADER_LEN + payload.len(), input.len());
                assert_eq!(pair::encode(hops, payload), input);
            }
            Err(TagError::Truncated) => assert!(input.len() < pair::HEADER_LEN),
            Err(TagError::TooManyHops { hops, max_hops }) => {
                assert!(hops > max_hops);
                // A hop count past the limit drops the message and keeps the
                // connection [nng-src rep.c].
                assert!(!TagError::TooManyHops { hops, max_hops }.is_violation());
            }
            Err(e) => panic!("a PAIR header cannot report {e}"),
        }
    }
}
