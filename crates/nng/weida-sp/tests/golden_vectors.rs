//! Conformance: the golden test vectors.
//!
//! These octets are the contract, so an implementation - this one, or a future
//! reimplementation, or a reader checking NNG against them - MUST encode
//! exactly them and MUST decode them back to exactly these values. The
//! vectors are asserted here through the crate's public surface only: the unit
//! tests beside each module cover the halves, and this file is what makes the
//! published bytes binding.
//!
//! Every accept vector is asserted **in both directions**. Encoding alone
//! would not catch a decoder that is wrong in the same way, which is the
//! failure mode that matters when the other end of the wire is NNG and not us.
//! The rejection vectors are asserted once, on the decoder, because there is
//! nothing to encode: they are bytes a peer may send and this codec must
//! refuse.

use weida_sp::error::{HeaderError, MessageError, TagError};
use weida_sp::{Backtrace, EndpointType, ProtocolHeader, backtrace, message, pair};

/// The cap every message vector is decoded under, 8 MiB, and no vector is
/// anywhere near it.
const CAP: u64 = 8 * 1024 * 1024;

/// Vectors 1-11: the protocol header, one per endpoint type.
#[track_caller]
fn assert_header(endpoint: EndpointType, expected: [u8; 8]) {
    let encoded = ProtocolHeader::new(endpoint).encode();
    assert_eq!(encoded, expected, "{endpoint:?}: header bytes");

    let decoded = ProtocolHeader::decode(&expected).expect("decode");
    assert_eq!(decoded.endpoint, endpoint, "{endpoint:?}: decoded type");
    assert_eq!(decoded.version, 0, "{endpoint:?}: version octet");
}

#[test]
fn golden_protocol_headers() {
    assert_header(
        EndpointType::Req,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x30, 0, 0],
    );
    assert_header(
        EndpointType::Rep,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x31, 0, 0],
    );
    assert_header(
        EndpointType::Pub,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x20, 0, 0],
    );
    assert_header(
        EndpointType::Sub,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x21, 0, 0],
    );
    assert_header(
        EndpointType::Push,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x50, 0, 0],
    );
    assert_header(
        EndpointType::Pull,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x51, 0, 0],
    );
    assert_header(
        EndpointType::Surveyor,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x62, 0, 0],
    );
    assert_header(
        EndpointType::Respondent,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x63, 0, 0],
    );
    assert_header(
        EndpointType::Bus,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x70, 0, 0],
    );
    assert_header(
        EndpointType::PairV0,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x10, 0, 0],
    );
    assert_header(
        EndpointType::PairV1,
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x11, 0, 0],
    );
}

/// Vector 12: an empty message is eight zero octets and nothing else.
#[test]
fn golden_empty_message() {
    let expected = [0u8; 8];
    assert_eq!(message::encode(b""), expected);
    let (body, used) = message::decode(&expected, CAP).expect("decode");
    assert!(body.is_empty());
    assert_eq!(used, 8);
}

/// Vector 13: body `hi`.
#[test]
fn golden_message_with_a_body() {
    let expected = [0, 0, 0, 0, 0, 0, 0, 0x02, b'h', b'i'];
    assert_eq!(message::encode(b"hi"), expected);
    let (body, used) = message::decode(&expected, CAP).expect("decode");
    assert_eq!(body, b"hi");
    assert_eq!(used, expected.len());
}

/// Vector 14: a REQ message, request ID 1, body `ping`.
#[test]
fn golden_req_message() {
    #[rustfmt::skip]
    let expected = [
        0, 0, 0, 0, 0, 0, 0, 0x08,
        0x80, 0x00, 0x00, 0x01,
        b'p', b'i', b'n', b'g',
    ];
    let stack = Backtrace::direct(1);
    assert_eq!(message::encode(&stack.encode_message(b"ping")), expected);

    let (body, used) = message::decode(&expected, CAP).expect("decode");
    assert_eq!(used, expected.len());
    let (decoded, payload) = backtrace::decode(body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
    assert_eq!(decoded, stack);
    assert_eq!(payload, b"ping");
}

/// Vector 15: the same request after one device, peer ID 7 pushed in front.
#[test]
fn golden_req_message_through_one_device() {
    #[rustfmt::skip]
    let expected = [
        0, 0, 0, 0, 0, 0, 0, 0x0c,
        0x00, 0x00, 0x00, 0x07,
        0x80, 0x00, 0x00, 0x01,
        b'p', b'i', b'n', b'g',
    ];
    let mut stack = Backtrace::direct(1);
    stack.push_peer(7);
    assert_eq!(message::encode(&stack.encode_message(b"ping")), expected);

    let (body, _) = message::decode(&expected, CAP).expect("decode");
    let (decoded, payload) = backtrace::decode(body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
    assert_eq!(decoded.peers, vec![7]);
    assert_eq!(decoded.id, 1);
    assert_eq!(payload, b"ping");
}

/// Vector 16: the reply carries the request's tag back unchanged.
#[test]
fn golden_rep_reply() {
    #[rustfmt::skip]
    let expected = [
        0, 0, 0, 0, 0, 0, 0, 0x08,
        0x80, 0x00, 0x00, 0x01,
        b'p', b'o', b'n', b'g',
    ];
    let request = message::encode(&Backtrace::direct(1).encode_message(b"ping"));
    let (request_body, _) = message::decode(&request, CAP).expect("decode");
    let (stack, _) = backtrace::decode(request_body, backtrace::DEFAULT_MAX_HOPS).expect("tags");

    // "the processing node attaches the backtrace stack from the request to
    // the reply" [rfc-reqrep §5]: the same object, a different payload.
    assert_eq!(message::encode(&stack.encode_message(b"pong")), expected);

    let (body, _) = message::decode(&expected, CAP).expect("decode");
    let (decoded, payload) = backtrace::decode(body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
    assert_eq!(decoded, stack);
    assert_eq!(payload, b"pong");
}

/// Vector 17: a survey uses the same construction with a survey ID.
#[test]
fn golden_survey_message() {
    #[rustfmt::skip]
    let expected = [
        0, 0, 0, 0, 0, 0, 0, 0x07,
        0x80, 0x00, 0x00, 0x2a,
        b'w', b'h', b'o',
    ];
    let stack = Backtrace::direct(42);
    assert_eq!(message::encode(&stack.encode_message(b"who")), expected);

    let (body, _) = message::decode(&expected, CAP).expect("decode");
    let (decoded, payload) = backtrace::decode(body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
    assert_eq!(decoded.id, 42);
    assert!(decoded.peers.is_empty());
    assert_eq!(payload, b"who");
}

/// Vectors 18 and 19: PAIR v1 as a cooked socket sends it, and after one hop.
#[test]
fn golden_pair_v1_messages() {
    #[rustfmt::skip]
    let initial = [
        0, 0, 0, 0, 0, 0, 0, 0x06,
        0x00, 0x00, 0x00, 0x01,
        b'h', b'i',
    ];
    #[rustfmt::skip]
    let forwarded = [
        0, 0, 0, 0, 0, 0, 0, 0x06,
        0x00, 0x00, 0x00, 0x02,
        b'h', b'i',
    ];
    assert_eq!(message::encode(&pair::encode_initial(b"hi")), initial);

    let (body, _) = message::decode(&initial, CAP).expect("decode");
    let (hops, payload) = pair::decode(body, pair::DEFAULT_MAX_HOPS).expect("hop count");
    assert_eq!(hops, 1);
    assert_eq!(payload, b"hi");

    assert_eq!(
        message::encode(&pair::encode(pair::next_hop(hops), payload)),
        forwarded
    );
    let (body, _) = message::decode(&forwarded, CAP).expect("decode");
    assert_eq!(
        pair::decode(body, pair::DEFAULT_MAX_HOPS).expect("hop count"),
        (2, &b"hi"[..])
    );
}

/// Vector 20: PUB/SUB has no topic field. The topic is the leading bytes of
/// the body and nothing in the framing knows where it ends
/// [nanomsg-nng §3].
#[test]
fn golden_pub_message_carries_its_topic_inline() {
    #[rustfmt::skip]
    let expected = [
        0, 0, 0, 0, 0, 0, 0, 0x09,
        b'p', b'x', b'.', b'e', b'u', b'r', b'1', b'2', b'0',
    ];
    assert_eq!(message::encode(b"px.eur120"), expected);

    let (body, used) = message::decode(&expected, CAP).expect("decode");
    assert_eq!(used, expected.len());
    assert_eq!(body, b"px.eur120");
    // A subscriber's filter is a byte prefix of exactly this body
    // [nanomsg-nng §4]; there is no field to consult.
    assert!(body.starts_with(b"px.eur"));
}

/// R1-R3: the protocol header rules, each of which closes the connection
/// [rfc-tcp §2].
#[test]
fn golden_rejected_protocol_headers() {
    let wrong_magic = [0x00, 0x53, 0x51, 0x00, 0x00, 0x30, 0, 0];
    assert_eq!(
        ProtocolHeader::decode(&wrong_magic),
        Err(HeaderError::BadMagic([0x00, 0x53, 0x51]))
    );

    let reserved = [0x00, 0x53, 0x50, 0x00, 0x00, 0x30, 0x00, 0x01];
    assert_eq!(
        ProtocolHeader::decode(&reserved),
        Err(HeaderError::ReservedNotZero(1))
    );

    let version = [0x00, 0x53, 0x50, 0x01, 0x00, 0x30, 0, 0];
    assert_eq!(
        ProtocolHeader::decode(&version),
        Err(HeaderError::UnsupportedVersion(1))
    );

    for bytes in [wrong_magic, reserved, version] {
        assert!(
            ProtocolHeader::decode(&bytes).unwrap_err().is_violation(),
            "every header rule is fatal [rfc-tcp §2]"
        );
    }
}

/// R4: a declaration beyond the cap, with no body behind it. The assertion
/// that matters is not the error value but that this returns at all: a
/// decoder that reserved 2^64-1 octets first would not.
#[test]
fn golden_rejected_oversized_declaration() {
    let hostile = [0xFFu8; 8];
    assert_eq!(
        message::decode(&hostile, CAP),
        Err(MessageError::BodyTooLarge {
            len: u64::MAX,
            max: CAP
        })
    );
}

/// R5: a tag stack that never terminates.
///
/// Two ways that ends, and the vector shows the first: the body runs out
/// before a tag with the terminal bit appears, which is malformed
/// [rfc-reqrep §5]. The second is a peer that keeps sending peer IDs, which
/// the local `MAXTTL` stops [nanomsg-nng §11] — asserted below by decoding
/// the same shape with the bound already reached.
#[test]
fn golden_rejected_tag_stack_without_terminator() {
    let wire = [0, 0, 0, 0, 0, 0, 0, 0x04, 0x00, 0x00, 0x00, 0x07];
    let (body, _) = message::decode(&wire, CAP).expect("the message itself is well formed");
    assert_eq!(
        backtrace::decode(body, backtrace::DEFAULT_MAX_HOPS),
        Err(TagError::Truncated)
    );
    assert_eq!(
        backtrace::decode(body, 0),
        Err(TagError::NoTerminator {
            hops: 0,
            max_hops: 0
        })
    );

    // A longer stack under a tighter bound: three peer IDs, at most two hops
    // allowed, and the payload behind them is never reached.
    #[rustfmt::skip]
    let deep = [
        0, 0, 0, 0, 0, 0, 0, 0x14,
        0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x03,
        0x80, 0x00, 0x00, 0x09,
        b'p', b'i', b'n', b'g',
    ];
    let (body, _) = message::decode(&deep, CAP).expect("well formed message");
    assert_eq!(
        backtrace::decode(body, 2),
        Err(TagError::NoTerminator {
            hops: 2,
            max_hops: 2
        })
    );
    let (stack, payload) = backtrace::decode(body, 3).expect("within the bound");
    assert_eq!(stack.peers, vec![1, 2, 3]);
    assert_eq!(stack.id, 9);
    assert_eq!(payload, b"ping");
}

/// R6: a reply shorter than one tag. "If the reply is shorter than 32 bits,
/// it is malformed and the endpoint MUST ignore it" [rfc-reqrep §5].
#[test]
fn golden_rejected_reply_shorter_than_a_tag() {
    let wire = [0, 0, 0, 0, 0, 0, 0, 0x02, 0x80, 0x00];
    let (body, used) = message::decode(&wire, CAP).expect("the message itself is well formed");
    assert_eq!(used, wire.len());
    assert_eq!(
        backtrace::decode(body, backtrace::DEFAULT_MAX_HOPS),
        Err(TagError::Truncated)
    );
}
