//! Deterministic hostile-input smoke tests for the ZMTP codec.
//!
//! The always-on companion to the `cargo fuzz` targets under
//! `crates/zmq/weida-zmtp/fuzz`: the same properties, driven by a seeded
//! xorshift generator so they run on stable Rust, in `cargo test`, with no
//! nightly toolchain and no corpus. The libFuzzer targets explore far deeper;
//! these make sure the properties are never left unchecked.
//!
//! The property that matters most here is the one ZMTP gives no help with. A
//! frame may declare 2^63-1 octets, there is no credit on the wire, and
//! libzmq's `ZMQ_MAXMSGSIZE` is unlimited by default, so the decoder's cap is
//! the whole defence: **an accepted frame never reports a length above the cap,
//! and a rejected one allocates nothing.** The tests assert the first directly
//! and the second by construction - every input here is a few dozen octets, so
//! a decoder that reserved the declared length would abort the test process
//! rather than fail it.

use weida_zmtp::error::{CommandError, FrameError, GreetingError};
use weida_zmtp::{
    Command, CurveCommand, Greeting, InitiatePlaintext, MAX_PROPERTIES, Mechanism, Metadata,
    SocketType, curve, frame, greeting, z85,
};

const ITERATIONS: usize = 100_000;
/// Deliberately small: every declared length above it must be refused from the
/// header alone, and a 64-octet input can never satisfy it by accident.
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
fn fuzz_smoke_frame_header() {
    let mut rng = Rng::new(0x5A17);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(24);
        match frame::decode(&input, CAP) {
            Ok((header, body, used)) => {
                assert!(header.len <= CAP, "an accepted frame is within the cap");
                assert_eq!(body.len() as u64, header.len);
                assert!(used <= input.len());
                assert!(used >= 2, "a frame is at least a flags and a size octet");
                // Re-encoding an accepted frame yields the canonical form,
                // which decodes to the same header. Byte equality is not the
                // property: a peer may send a long size for a short body.
                let re = frame::encode(header.kind, body);
                let (again, body2, _) = frame::decode(&re, CAP).expect("re-decode");
                assert_eq!(again, header);
                assert_eq!(body2, body);
            }
            Err(e) => assert!(e == FrameError::Incomplete || e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_frame_header_declares_more_than_it_carries() {
    // The adversarial shape, built on purpose rather than stumbled upon: a
    // valid long header with an arbitrary 64-bit length and no body at all.
    let mut rng = Rng::new(0xD00D);
    for _ in 0..ITERATIONS {
        let mut input = vec![if rng.byte() & 1 == 0 { 0x02 } else { 0x06 }];
        input.extend_from_slice(&rng.next_u64().to_be_bytes());
        input.extend_from_slice(&rng.bytes(8));
        match frame::decode(&input, CAP) {
            // Accepted only when the declared length is both within the cap
            // and actually present.
            Ok((header, body, _)) => {
                assert!(header.len <= CAP);
                assert_eq!(body.len() as u64, header.len);
            }
            Err(e) => assert!(e.is_violation() || e == FrameError::Incomplete),
        }
        // And the header alone must decide: no body, no allocation.
        let header_only = &input[..9];
        match frame::decode_header(header_only, CAP) {
            Ok((header, used)) => {
                assert!(header.len <= CAP);
                assert_eq!(used, 9);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_command_bodies() {
    let mut rng = Rng::new(0xC0FFEE);
    for _ in 0..ITERATIONS {
        let body = rng.bytes(40);
        match Command::decode(&body) {
            Ok(command) => {
                check_command(&command);
                // Idempotence: an accepted command re-encodes and decodes to
                // the same value.
                let frame_bytes = command.encode().expect("an accepted command re-encodes");
                let (_, body2, _) = frame::decode(&frame_bytes, CAP).expect("decode");
                assert_eq!(Command::decode(body2).expect("re-decode"), command);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_command_bodies_from_valid_prefixes() {
    // Random bytes rarely start with a valid command name, so this half feeds
    // well-formed names with hostile data behind them.
    let names: [&[u8]; 7] = [
        b"\x05READY",
        b"\x05ERROR",
        b"\x09SUBSCRIBE",
        b"\x06CANCEL",
        b"\x04PING",
        b"\x04PONG",
        b"\x04JOIN",
    ];
    let mut rng = Rng::new(0xBEEF);
    for _ in 0..ITERATIONS {
        let mut body = names[rng.below(names.len())].to_vec();
        body.extend_from_slice(&rng.bytes(32));
        match Command::decode(&body) {
            Ok(command) => {
                check_command(&command);
                let re = command.encode().expect("an accepted command re-encodes");
                let (_, body2, _) = frame::decode(&re, CAP).expect("decode");
                assert_eq!(Command::decode(body2).expect("re-decode"), command);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_curve_command_bodies() {
    // CURVE's commands are laid out by count, so the hostile input that
    // matters is a body one or two octets away from a legal length: every
    // field behind the padding of a short HELLO is displaced, and a decoder
    // that trusted the name would read a signature box out of phase.
    let names: [&[u8]; 6] = [
        b"\x05HELLO",
        b"\x07WELCOME",
        b"\x08INITIATE",
        b"\x05READY",
        b"\x07MESSAGE",
        b"\x05ERROR",
    ];
    let lengths = [
        0usize,
        1,
        curve::MESSAGE_MIN_LEN - 1,
        curve::READY_MIN_LEN,
        curve::WELCOME_LEN - 1,
        curve::WELCOME_LEN,
        curve::HELLO_LEN - 2,
        curve::HELLO_LEN,
        curve::INITIATE_MIN_LEN,
        curve::INITIATE_MIN_LEN + 1,
    ];
    let mut rng = Rng::new(0xC1234);
    let mut accepted = 0usize;
    for _ in 0..ITERATIONS {
        let name = names[rng.below(names.len())];
        let total = lengths[rng.below(lengths.len())];
        let mut body = name.to_vec();
        while body.len() < total {
            body.push(rng.byte());
        }
        body.truncate(total.max(name.len()));
        // A HELLO only ever gets past the length check with its version
        // octets intact, so half the draws set them: without this the
        // interesting path is never reached.
        if body.len() > 8 && rng.below(2) == 0 {
            body[6] = 1;
            body[7] = 0;
        }
        match CurveCommand::decode(&body) {
            Ok(command) => {
                accepted += 1;
                check_curve_command(&command);
                let re = command.encode().expect("an accepted command re-encodes");
                let (_, body2, _) = frame::decode(&re, CAP).expect("decode");
                assert_eq!(CurveCommand::decode(body2).expect("re-decode"), command);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
    // The length table above is built from the real minima, so every command
    // must have been accepted at some point; a table that only ever produced
    // refusals would be a test of nothing.
    assert!(
        accepted > ITERATIONS / 100,
        "only {accepted} of {ITERATIONS} bodies were accepted"
    );
}

#[test]
fn fuzz_smoke_initiate_plaintext() {
    // The plaintext of an INITIATE box: two keys, a vouch and a dictionary.
    // Lengths are drawn up to a little past the minimum, so the accepted path
    // is reached as well as the refusals.
    let mut rng = Rng::new(0x1A17);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(curve::INITIATE_PLAINTEXT_MIN_LEN + 40);
        match InitiatePlaintext::decode(&input) {
            Ok(value) => {
                assert!(input.len() >= curve::INITIATE_PLAINTEXT_MIN_LEN);
                assert!(value.metadata.properties().len() <= MAX_PROPERTIES);
                let mut re = Vec::new();
                value
                    .encode(&mut re)
                    .expect("an accepted INITIATE plaintext re-encodes");
                assert_eq!(re, input);
                assert_eq!(InitiatePlaintext::decode(&re).expect("re-decode"), value);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_z85() {
    // Z85 is where text a human typed becomes a key, so the property is that
    // no input panics and every accepted one round-trips: five legal
    // characters can still name a value above four octets, which is the case
    // a decoder that only checked the alphabet would wrap on.
    let mut rng = Rng::new(0x2585);
    for _ in 0..ITERATIONS {
        let text: String = (0..rng.below(11))
            .map(|_| {
                let pick = rng.below(90);
                char::from(*z85::ALPHABET.get(pick).unwrap_or(&b'"'))
            })
            .collect();
        if let Ok(data) = z85::decode(&text) {
            assert!(data.len().is_multiple_of(4));
            assert_eq!(z85::encode(&data).expect("re-encode"), text);
        }
        let data = rng.bytes(12);
        if let Ok(text) = z85::encode(&data) {
            assert_eq!(text.len(), data.len() / 4 * 5);
            assert_eq!(z85::decode(&text).expect("re-decode"), data);
        }
    }
}

#[test]
fn fuzz_smoke_metadata() {
    let mut rng = Rng::new(0xFEED);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(32);
        match Metadata::decode(&input) {
            Ok(md) => {
                assert!(
                    md.properties().len() <= MAX_PROPERTIES,
                    "an accepted dictionary is within the cap"
                );
                for (name, value) in md.properties() {
                    assert!(!name.is_empty() && name.len() <= 255);
                    assert!(value.len() <= i32::MAX as usize);
                }
                // A decoded dictionary re-encodes byte-for-byte: unlike a
                // weida header there are no unknown keys to drop, so the
                // fixed point here really is byte equality.
                let mut re = Vec::new();
                md.encode(&mut re)
                    .expect("an accepted dictionary re-encodes");
                assert_eq!(re, input);
                let _ = md.socket_type();
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_metadata_property_counts() {
    // Dictionaries of one to two caps' worth of minimal properties: the count
    // alone decides the outcome, and a refusal names the cap instead of
    // panicking or building the list.
    let mut rng = Rng::new(0x6E7A);
    for _ in 0..ITERATIONS / 10 {
        let count = rng.below(2 * MAX_PROPERTIES + 1);
        let mut input = Vec::new();
        for _ in 0..count {
            input.push(1);
            input.push(b'a' + rng.below(3) as u8);
            let value = rng.bytes(3);
            input.extend_from_slice(&(value.len() as u32).to_be_bytes());
            input.extend_from_slice(&value);
        }
        match Metadata::decode(&input) {
            Ok(md) => {
                assert!(count <= MAX_PROPERTIES);
                assert_eq!(md.properties().len(), count);
            }
            Err(e) => {
                assert!(count > MAX_PROPERTIES, "{count} properties refused");
                assert_eq!(e, CommandError::TooManyProperties(MAX_PROPERTIES + 1));
            }
        }
    }
}

#[test]
fn fuzz_smoke_greeting() {
    let mut rng = Rng::new(0x515E);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(70);
        match Greeting::decode(&input) {
            Ok(g) => {
                assert!(input.len() >= 64);
                assert!(!g.mechanism.name().is_empty());
                // Re-encoding is byte-exact over the fields that carry
                // meaning; padding and filler are not among them, so the
                // comparison is the field set, not the octets.
                assert_eq!(Greeting::decode(&g.encode()).expect("re-decode"), g);
                let _ = g.accept(Mechanism::NULL);
            }
            Err(e) => assert!(e == GreetingError::Incomplete || e.is_violation()),
        }
        match greeting::sniff_major(&input) {
            Ok(_) => assert!(input.len() >= 11),
            Err(e) => assert!(e == GreetingError::Incomplete || e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_greeting_from_valid_ones() {
    // A real greeting with one octet corrupted: the shape where a decoder that
    // validates too much or too little shows up.
    let mut rng = Rng::new(0x1234);
    for _ in 0..ITERATIONS {
        let original = Greeting::null().encode();
        let mut bytes = original;
        let at = rng.below(64);
        // Guaranteed to be a different octet, so "decoded to the same
        // greeting" means the field truly carries no meaning rather than that
        // the generator happened to rewrite the same value.
        let mut octet = rng.byte();
        if octet == original[at] {
            octet = octet.wrapping_add(1);
        }
        bytes[at] = octet;
        match Greeting::decode(&bytes) {
            Ok(g) => {
                // The octets that may differ without changing meaning are
                // exactly the padding and the filler. Everything else either
                // changes a field or is refused.
                let cosmetic = (1..9).contains(&at) || at >= 33;
                assert!(
                    cosmetic || g != Greeting::null(),
                    "octet {at} changed nothing and is not padding or filler"
                );
                assert_eq!(Greeting::decode(&g.encode()).expect("re-decode"), g);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

/// Every documented bound an accepted command must respect.
fn check_command(command: &Command<'_>) {
    if let Command::Ready(md) | Command::Initiate(md) = command {
        assert!(
            md.properties().len() <= MAX_PROPERTIES,
            "{} carries more properties than the cap",
            command.name()
        );
    }
    match command {
        Command::Ready(md) => {
            for (name, _) in md.properties() {
                assert!(!name.is_empty());
            }
            if let Some(t) = md.socket_type() {
                assert!(SocketType::parse(t.as_str().as_bytes()) == Some(t));
            }
        }
        Command::Error(reason) => {
            assert!(reason.len() <= 255);
            assert!(reason.bytes().all(|b| (0x20..=0x7E).contains(&b)));
        }
        Command::Subscribe(_) | Command::Cancel(_) => {}
        Command::Ping { context, .. } | Command::Pong { context } => {
            assert!(context.len() <= weida_zmtp::MAX_PING_CONTEXT);
        }
        Command::Hello { username, password } => {
            // Each field's own length octet is its bound, so anything that
            // decoded fits and anything longer could not have been written.
            assert!(username.len() <= weida_zmtp::MAX_PLAIN_FIELD);
            assert!(password.len() <= weida_zmtp::MAX_PLAIN_FIELD);
        }
        Command::Welcome => {}
        Command::Initiate(md) => {
            for (name, _) in md.properties() {
                assert!(!name.is_empty());
            }
        }
    }
    // A command that decoded must also encode; the two halves share their
    // length rules, so one accepting what the other refuses is a bug.
    assert!(
        command.encode().is_ok(),
        "{} decoded but will not encode",
        command.name()
    );
}

/// Every documented bound an accepted CURVE command must respect. The boxes
/// are opaque here as everywhere: what is checkable is their length.
fn check_curve_command(command: &CurveCommand<'_>) {
    match command {
        CurveCommand::Hello { .. } | CurveCommand::Welcome { .. } => {
            // Both are fixed-size, and the array types in the variant make
            // any other length unrepresentable; the encoder's total is the
            // remaining claim.
        }
        CurveCommand::Initiate { initiate_box, .. } => {
            assert!(initiate_box.len() >= curve::INITIATE_BOX_MIN_LEN);
        }
        CurveCommand::Ready { ready_box, .. } => {
            assert!(ready_box.len() >= curve::READY_BOX_MIN_LEN);
        }
        CurveCommand::Message { message_box, .. } => {
            assert!(message_box.len() >= curve::MESSAGE_BOX_MIN_LEN);
        }
    }
    let mut body = Vec::new();
    command
        .encode_body(&mut body)
        .expect("an accepted CURVE command re-encodes");
    let expected = match command {
        CurveCommand::Hello { .. } => curve::HELLO_LEN,
        CurveCommand::Welcome { .. } => curve::WELCOME_LEN,
        CurveCommand::Initiate { .. } => body.len().max(curve::INITIATE_MIN_LEN),
        CurveCommand::Ready { .. } => body.len().max(curve::READY_MIN_LEN),
        CurveCommand::Message { .. } => body.len().max(curve::MESSAGE_MIN_LEN),
    };
    assert_eq!(body.len(), expected, "{}", command.name());
}
