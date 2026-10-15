//! Deterministic hostile-input smoke tests for the MQTT codec.
//!
//! The always-on companion to the `cargo fuzz` targets under
//! `crates/mqtt/weida-mqtt-codec/fuzz`: the same properties, driven by a
//! seeded xorshift generator so they run on stable Rust, in `cargo test`, with
//! no nightly toolchain and no corpus. The libFuzzer targets explore far
//! deeper; these make sure the properties are never left unchecked.
//!
//! Four properties, and the first is the one MQTT gives no help with.
//!
//! 1. **An accepted packet never exceeds the cap, and a rejected one allocates
//!    nothing.** A packet may declare 268,435,455 bytes from the Remaining
//!    Length alone (2.1.4) and `Maximum Packet Size` is absent by default,
//!    meaning no limit below that (3.1.2.11.4) [mqtt5 §5]. So the decoder's
//!    cap is the whole defence. The tests assert the first directly and the
//!    second by construction — every input here is a few dozen bytes, so a
//!    decoder that reserved the declared length would abort the test process
//!    rather than fail it.
//! 2. **`Incomplete` is reachable only while bytes are genuinely missing.**
//!    A reader loop that parks on a packet the peer has finished sending is
//!    deadlocked, so once the whole declared packet is present the answer must
//!    be a packet or a violation and never a request for more. This is the
//!    property that caught the property-block and packet-body length cases,
//!    and it is asserted by feeding every decoder its own exact byte count.
//! 3. **Whatever decodes, re-encodes to something that decodes the same.**
//!    Not byte-for-byte: a peer may send properties in any order and may spell
//!    an all-zero PUBACK long or short, while this codec emits one canonical
//!    form [mqtt5 §3]. Re-decoding is the check that survives that.
//! 4. **Progress.** A stream reader fed arbitrary bytes always either consumes
//!    at least one packet or stops, never loops.

use weida_mqtt_codec::property::{Properties, PropertySet};
use weida_mqtt_codec::{
    Auth, AuthReasonCode, Connack, Connect, ConnectReasonCode, DecodeError, Disconnect,
    DisconnectReasonCode, Packet, PayloadList, Puback, Pubcomp, Publish, Pubrec, Pubrel, QoS,
    Reader, Suback, SubackReasonCode, Subscribe, Subscription, SubscriptionOptions, Unsuback,
    UnsubackReasonCode, Unsubscribe, Will, data, varint,
};

const ITERATIONS: usize = 100_000;

/// Deliberately small: every declared packet above it must be refused from the
/// fixed header alone, and a 64-byte input can never satisfy it by accident.
const CAP: u32 = 4096;

/// Deterministic xorshift64* generator: no dependency, reproducible failures.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn byte(&mut self) -> u8 {
        (self.next_u64() >> 33) as u8
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() >> 33) as usize % bound
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.byte()).collect()
    }
}

#[test]
fn fuzz_smoke_varint_never_panics_and_round_trips_what_it_accepts() {
    let mut rng = Rng::new(0x5150_4D51_5454_0001);
    for _ in 0..ITERATIONS {
        let len = rng.below(7);
        let input = rng.bytes(len);
        match varint::decode(&input) {
            Ok((value, used)) => {
                assert!(value <= varint::MAX);
                assert!((1..=varint::MAX_BYTES).contains(&used));
                assert!(used <= input.len());
                assert_eq!(used, varint::encoded_len(value), "{input:02X?} is minimal");

                let mut out = Vec::new();
                varint::encode(value, &mut out).expect("a decoded value re-encodes");
                assert_eq!(out, &input[..used], "the encoding is canonical");
            }
            Err(error) => assert!(
                matches!(
                    error,
                    DecodeError::Incomplete
                        | DecodeError::VarintNotMinimal
                        | DecodeError::VarintTooLong
                ),
                "{input:02X?}: {error}"
            ),
        }
    }
}

/// A string is length-prefixed, so this is where a decoder that trusts a
/// declared length would be caught: a two-byte prefix can claim 65,535 bytes
/// over a buffer holding six.
#[test]
fn fuzz_smoke_strings_and_binary_never_over_read() {
    let mut rng = Rng::new(0x5150_4D51_5454_0002);
    for _ in 0..ITERATIONS {
        let input = {
            let len = rng.below(12);
            rng.bytes(len)
        };

        let mut reader = Reader::new(&input);
        if let Ok(text) = reader.string() {
            assert!(text.len() <= data::MAX_FIELD_LEN);
            assert!(!text.as_bytes().contains(&0));
            assert_eq!(reader.position(), text.len() + 2);
            let mut out = Vec::new();
            data::put_string(text, &mut out).expect("re-encodes");
            assert_eq!(out, &input[..text.len() + 2]);
        }

        let mut reader = Reader::new(&input);
        if let Ok(bytes) = reader.binary() {
            assert!(bytes.len() <= data::MAX_FIELD_LEN);
            assert_eq!(reader.position(), bytes.len() + 2);
        }
    }
}

/// The property block is the one place where "not enough bytes" must *not* be
/// `Incomplete`: the block's own declared length is authoritative, so the
/// decoder is handed exactly that many bytes and anything short is malformed.
#[test]
fn fuzz_smoke_properties_over_an_exact_block_never_ask_for_more() {
    let mut rng = Rng::new(0x5150_4D51_5454_0003);
    let sets = [
        PropertySet::CONNECT,
        PropertySet::CONNACK,
        PropertySet::WILL,
        PropertySet::PUBLISH,
        PropertySet::NONE,
    ];

    for _ in 0..ITERATIONS {
        let body = {
            let len = rng.below(20);
            rng.bytes(len)
        };
        let allowed = sets[rng.below(sets.len())];

        // A block whose declared length is exactly what follows it: every
        // declared byte is present, so `Incomplete` would be a lie.
        let mut framed = Vec::new();
        varint::encode(body.len() as u32, &mut framed).expect("a small length");
        framed.extend_from_slice(&body);

        let mut reader = Reader::new(&framed);
        match Properties::decode(&mut reader, allowed, None) {
            Ok(properties) => {
                assert!(reader.is_empty(), "the whole block was consumed");
                // Whatever it accepted, it can say how long it is and write
                // it back out as something that decodes the same.
                let len = properties
                    .encoded_len(allowed)
                    .expect("a decoded set re-encodes");
                let mut out = Vec::new();
                properties.encode(allowed, &mut out).expect("re-encodes");
                assert_eq!(out.len() as u32, len);

                let mut again = Reader::new(&out);
                let reparsed =
                    Properties::decode(&mut again, allowed, None).expect("the canonical form");
                assert_eq!(reparsed, properties, "{body:02X?}");
            }
            Err(error) => assert_ne!(
                error,
                DecodeError::Incomplete,
                "{body:02X?}: an exact block never asks for more"
            ),
        }
    }
}

/// What an accepted packet must satisfy, whoever found the input: a random
/// draw below, or one a `cargo fuzz` run once broke.
///
/// `encoded_len` is compared against the **re-encoded** length and not against
/// the bytes consumed, and the difference is the point. An acknowledgement's
/// all-success tail is omissible (3.4.1) and properties may arrive in any
/// order [mqtt5 §3], so a legal input may be longer than this codec's
/// canonical spelling of the same value. The converse holds in exactly one
/// place: a reason code sent without its Property Length (an acknowledgement's
/// `id, code` body, DISCONNECT's bare code, AUTH's bare code) decodes, and
/// re-encodes with the explicit zero length every peer writes, one byte
/// longer. So `encoded_len <= used + 1` is the honest invariant, and the
/// canonical form must be a fixed point: encoding what it decodes to gives it
/// back.
fn check_packet(input: &[u8]) {
    let Ok((packet, used)) = Packet::decode(input, CAP) else {
        return;
    };
    assert!(used <= input.len());
    assert!(used as u32 <= CAP);

    let mut out = Vec::new();
    packet
        .encode(&mut out)
        .expect("a decoded packet re-encodes");
    assert_eq!(packet.encoded_len(), Ok(out.len() as u32));
    assert!(
        out.len() <= used + 1,
        "{input:02X?}: the canonical form is at most the omitted Property Length longer than \
         what arrived"
    );

    let (again, used2) = Packet::decode(&out, CAP).expect("the canonical form");
    assert_eq!(again, packet);
    assert_eq!(used2, out.len());
    let mut twice = Vec::new();
    again
        .encode(&mut twice)
        .expect("the canonical form re-encodes");
    assert_eq!(
        twice, out,
        "{input:02X?}: the canonical form is a fixed point"
    );
}

/// The whole decoder against arbitrary bytes: nothing panics, an accepted
/// packet fits the cap, and no rejection reserves memory.
#[test]
fn fuzz_smoke_packets_never_panic_and_respect_the_cap() {
    let mut rng = Rng::new(0x5150_4D51_5454_0004);
    for _ in 0..ITERATIONS {
        let input = {
            let len = rng.below(48);
            rng.bytes(len)
        };
        check_packet(&input);
    }
}

/// The input a fuzz run broke the length relation with: a PUBACK whose body is
/// the identifier and a non-success reason code, with no Property Length, and
/// the start of another packet behind it. Legal (3.4.2.2), decoded to its
/// value, and re-encoded one byte longer with the explicit zero length.
#[test]
fn a_reason_code_without_a_property_length_canonicalises_one_byte_longer() {
    check_packet(&[0x40, 0x03, 0x40, 0x03, 0x87, 0x40, 0xF3]);

    // The same spelling in the other two families that allow it.
    for bare in [&[0xE0, 0x01, 0x8E][..], &[0xF0, 0x01, 0x18][..]] {
        let (packet, used) = Packet::decode(bare, CAP).expect("a bare reason code decodes");
        assert_eq!(used, bare.len());
        let mut out = Vec::new();
        packet.encode(&mut out).expect("encodes");
        assert_eq!(out.len(), bare.len() + 1, "{bare:02X?}");
        check_packet(bare);
    }
}

/// Mutations of a valid packet, which is how the decoder is reached past its
/// fixed header at all.
///
/// Uniformly random bytes almost never build a CONNECT: the four-byte protocol
/// name alone is a 1-in-2^32 event. So the generator starts from packets that
/// are valid in every field — one per packet type, the golden vectors of
/// `golden_vectors.rs` among them — and flips bytes in the body,
/// which lands the decoder in the property block, the Will Properties, the
/// subscription options byte and the length-prefixed payload fields where the
/// interesting faults live.
///
/// Two counters are asserted rather than one. Without the accept counter the
/// test could pass while rejecting everything, which proves nothing; without
/// the reject counter it could pass while accepting everything, which proves
/// less. A third assertion covers every seed, so a corpus entry that stopped
/// being decodable — because a rule tightened — is caught rather than
/// silently contributing nothing.
#[test]
fn fuzz_smoke_mutations_of_a_valid_packet() {
    let corpus = one_of_each();
    assert_eq!(corpus.len(), 15, "one seed per packet type");
    for seed in &corpus {
        Packet::decode(seed, CAP).unwrap_or_else(|error| {
            panic!("the seed {seed:02X?} must decode, got {error}");
        });
    }

    let mut rng = Rng::new(0x5150_4D51_5454_0005);
    let mut accepted = 0usize;
    let mut rejected = 0usize;

    for _ in 0..ITERATIONS {
        let mut input = corpus[rng.below(corpus.len())].clone();
        if input.len() <= 2 {
            // PINGREQ and PINGRESP have no body to mutate; mutating their
            // fixed header is the `packets_never_panic` target's job.
            continue;
        }

        // Never the Remaining Length byte at index 1: the property this test
        // asserts is that a *complete* packet is never `Incomplete`, and
        // rewriting its declared length is how that stops being true.
        for _ in 0..1 + rng.below(3) {
            let at = 2 + rng.below(input.len() - 2);
            input[at] = rng.byte();
        }

        match Packet::decode(&input, CAP) {
            Ok((packet, used)) => {
                accepted += 1;
                assert_eq!(used, input.len());
                let mut out = Vec::new();
                packet.encode(&mut out).expect("re-encodes");
                let (again, _) = Packet::decode(&out, CAP).expect("the canonical form");
                assert_eq!(again, packet);
            }
            // The fixed header declares exactly the bytes that follow, so a
            // complete packet is present and `Incomplete` is never the answer.
            Err(error) => {
                rejected += 1;
                assert_ne!(error, DecodeError::Incomplete, "{input:02X?}");
                assert!(error.is_violation(), "{input:02X?}: {error}");
                assert!(error.reason_code().is_some(), "{error}");
            }
        }
    }

    assert!(accepted > 0, "no mutation was ever harmless");
    assert!(rejected > 0, "no mutation was ever caught");
}

/// One valid packet of every type, as wire bytes.
fn one_of_each() -> Vec<Vec<u8>> {
    let filters = [Subscription::new("a/+", QoS::AtLeastOnce)];
    let names = ["a/+"];
    let sub_codes = [SubackReasonCode::GrantedQos1];
    let unsub_codes = [UnsubackReasonCode::Success];
    let pairs = [("k", "v")];

    let packets = [
        Packet::Connect(Connect {
            client_id: "c",
            clean_start: true,
            keep_alive: 60,
            will: Some(Box::new(Will {
                topic: "d",
                payload: &[0x01],
                qos: QoS::AtLeastOnce,
                retain: false,
                properties: Properties {
                    will_delay_interval: Some(10),
                    ..Properties::new()
                },
            })),
            user_name: Some("u"),
            password: Some(&[0x70]),
            properties: Properties::new().with_user_properties(&pairs),
        }),
        Packet::Connack(Connack {
            session_present: true,
            reason_code: ConnectReasonCode::Success,
            properties: Properties {
                receive_maximum: Some(10),
                maximum_qos: Some(QoS::AtLeastOnce),
                ..Properties::new()
            },
        }),
        Packet::Publish(Publish {
            topic: "a/b",
            payload: b"hi",
            qos: QoS::ExactlyOnce,
            dup: true,
            retain: true,
            packet_id: Some(10),
            properties: Properties {
                content_type: Some("text/plain"),
                topic_alias: Some(3),
                ..Properties::new()
            },
        }),
        Packet::Puback(Puback::new(10)),
        Packet::Pubrec(Pubrec::new(10)),
        Packet::Pubrel(Pubrel::new(10)),
        Packet::Pubcomp(Pubcomp::new(10)),
        Packet::Subscribe(Subscribe {
            packet_id: 1,
            properties: Properties::new(),
            filters: PayloadList::new(&filters),
        }),
        Packet::Suback(Suback {
            packet_id: 1,
            properties: Properties::new(),
            reason_codes: PayloadList::new(&sub_codes),
        }),
        Packet::Unsubscribe(Unsubscribe {
            packet_id: 2,
            properties: Properties::new(),
            filters: PayloadList::new(&names),
        }),
        Packet::Unsuback(Unsuback {
            packet_id: 2,
            properties: Properties::new(),
            reason_codes: PayloadList::new(&unsub_codes),
        }),
        Packet::Pingreq,
        Packet::Pingresp,
        Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::SessionTakenOver,
            properties: Properties {
                reason_string: Some("taken"),
                ..Properties::new()
            },
        }),
        Packet::Auth(Auth {
            reason_code: AuthReasonCode::ContinueAuthentication,
            properties: Properties {
                authentication_method: Some("SCRAM-SHA-1"),
                authentication_data: Some(&[0x01, 0x02]),
                ..Properties::new()
            },
        }),
    ];

    packets
        .iter()
        .map(|packet| {
            let mut out = Vec::new();
            packet.encode(&mut out).expect("a seed encodes");
            out
        })
        .collect()
}

/// Every one of the 256 subscription-options bytes is either accepted and
/// round-trips, or refused with the verdict 3.8.3.1 gives it.
///
/// Exhaustive rather than sampled, because the byte is only eight bits wide
/// and it carries three fields plus two reserved bits with three *different*
/// verdicts — malformed for the reserved bits and for QoS 3, a Protocol Error
/// for Retain Handling 3.
#[test]
fn fuzz_smoke_every_subscription_options_byte() {
    let mut accepted = 0usize;
    for byte in 0u8..=255 {
        match SubscriptionOptions::from_byte(byte) {
            Ok(options) => {
                accepted += 1;
                assert_eq!(options.as_byte(), byte, "0x{byte:02X} round-trips");
            }
            Err(error) => {
                assert!(error.is_violation());
                assert!(
                    matches!(
                        error,
                        DecodeError::ReservedSubscriptionOptionBits { .. }
                            | DecodeError::InvalidQos { .. }
                            | DecodeError::InvalidRetainHandling { .. }
                    ),
                    "0x{byte:02X}: {error}"
                );
            }
        }
    }
    // 3 QoS levels x 2 No Local x 2 Retain As Published x 3 Retain Handling.
    assert_eq!(accepted, 3 * 2 * 2 * 3);
}

/// A stream reader fed arbitrary bytes always terminates: each round either
/// consumes at least one byte or stops.
#[test]
fn fuzz_smoke_a_stream_reader_always_terminates() {
    let mut rng = Rng::new(0x5150_4D51_5454_0006);
    for _ in 0..ITERATIONS / 10 {
        let stream = {
            let len = rng.below(96);
            rng.bytes(len)
        };
        let mut offset = 0;
        let mut rounds = 0;

        while offset < stream.len() {
            rounds += 1;
            assert!(rounds <= stream.len() + 1, "the reader looped");
            match Packet::decode(&stream[offset..], CAP) {
                Ok((_, used)) => {
                    assert!(used > 0, "a packet consumed nothing");
                    offset += used;
                }
                Err(_) => break,
            }
        }
    }
}
