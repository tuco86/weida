//! HTTP/2 frames, one hand-assembled vector per frame type, laid out as
//! RFC 9113 §6 draws them: decoded, and re-encoded byte for byte where the
//! encoder writes that shape; plus one refusal per rule, named by the rule.

use weida_http_codec::DecodeError;
use weida_http_codec::h2::{Frame, PREFACE, code, setting};

const MAX: u32 = 16_384;

/// `length(24) type(8) flags(8) R+stream(31)`, then the payload.
fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u32).to_be_bytes()[1..].to_vec();
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(&stream.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn round_trip(wire: &[u8], expect: &Frame<'_>) {
    let (decoded, used) = Frame::decode(wire, MAX).unwrap();
    assert_eq!(&decoded, expect);
    assert_eq!(used, wire.len());
    let mut out = Vec::new();
    expect.encode(&mut out);
    assert_eq!(out, wire, "{expect:?}");
}

#[test]
fn golden_h2_preface() {
    assert_eq!(PREFACE, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
}

#[test]
fn golden_h2_every_frame_type() {
    // §6.1 DATA, END_STREAM.
    round_trip(
        &frame(0x0, 0x1, 1, b"hello"),
        &Frame::Data {
            stream: 1,
            end_stream: true,
            data: b"hello",
        },
    );
    // §6.2 HEADERS, END_HEADERS, an HPACK `:method: GET`.
    round_trip(
        &frame(0x1, 0x4, 3, &[0x82]),
        &Frame::Headers {
            stream: 3,
            end_stream: false,
            end_headers: true,
            block: &[0x82],
        },
    );
    // §6.3 PRIORITY: E + dependency(31), weight(8).
    round_trip(
        &frame(0x2, 0, 5, &[0, 0, 0, 0, 15]),
        &Frame::Priority { stream: 5 },
    );
    // §6.4 RST_STREAM, CANCEL.
    round_trip(
        &frame(0x3, 0, 7, &[0, 0, 0, 8]),
        &Frame::RstStream {
            stream: 7,
            code: code::CANCEL,
        },
    );
    // §6.5 SETTINGS: two parameters, then the ACK.
    round_trip(
        &frame(0x4, 0, 0, &[0, 2, 0, 0, 0, 0, 0, 4, 0, 1, 0, 0]),
        &Frame::Settings {
            ack: false,
            params: vec![
                (setting::ENABLE_PUSH, 0),
                (setting::INITIAL_WINDOW_SIZE, 65_536),
            ],
        },
    );
    round_trip(
        &frame(0x4, 0x1, 0, &[]),
        &Frame::Settings {
            ack: true,
            params: vec![],
        },
    );
    // §6.6 PUSH_PROMISE: R + promised stream(31), block.
    round_trip(
        &frame(0x5, 0x4, 1, &[0, 0, 0, 2, 0x82]),
        &Frame::PushPromise {
            stream: 1,
            promised: 2,
            end_headers: true,
            block: &[0x82],
        },
    );
    // §6.7 PING and its ACK.
    round_trip(
        &frame(0x6, 0, 0, b"12345678"),
        &Frame::Ping {
            ack: false,
            data: *b"12345678",
        },
    );
    round_trip(
        &frame(0x6, 0x1, 0, b"12345678"),
        &Frame::Ping {
            ack: true,
            data: *b"12345678",
        },
    );
    // §6.8 GOAWAY: R + last stream(31), code, debug data.
    round_trip(
        &frame(0x7, 0, 0, &[0, 0, 0, 9, 0, 0, 0, 1, b'b', b'y', b'e']),
        &Frame::GoAway {
            last_stream: 9,
            code: code::PROTOCOL_ERROR,
            debug: b"bye",
        },
    );
    // §6.9 WINDOW_UPDATE on the connection and on a stream.
    round_trip(
        &frame(0x8, 0, 0, &[0x7f, 0xff, 0xff, 0xff]),
        &Frame::WindowUpdate {
            stream: 0,
            increment: (1 << 31) - 1,
        },
    );
    round_trip(
        &frame(0x8, 0, 1, &[0, 0, 0, 0]),
        &Frame::WindowUpdate {
            stream: 1,
            increment: 0,
        },
    );
    // §6.10 CONTINUATION.
    round_trip(
        &frame(0x9, 0x4, 3, &[0x84]),
        &Frame::Continuation {
            stream: 3,
            end_headers: true,
            block: &[0x84],
        },
    );
}

#[test]
fn golden_h2_padding_and_priority_are_removed() {
    // DATA, PADDED: pad length 3, "ab", three zero octets.
    let wire = frame(0x0, 0x8, 1, &[3, b'a', b'b', 0, 0, 0]);
    assert_eq!(
        Frame::decode(&wire, MAX).unwrap().0,
        Frame::Data {
            stream: 1,
            end_stream: false,
            data: b"ab"
        }
    );
    // HEADERS, PADDED | PRIORITY | END_STREAM | END_HEADERS.
    let wire = frame(0x1, 0x2d, 1, &[1, 0x80, 0, 0, 3, 200, 0x82, 0]);
    assert_eq!(
        Frame::decode(&wire, MAX).unwrap().0,
        Frame::Headers {
            stream: 1,
            end_stream: true,
            end_headers: true,
            block: &[0x82]
        }
    );
    // A pad that uses the whole remaining payload leaves empty data.
    let wire = frame(0x0, 0x8, 1, &[2, 0, 0]);
    assert_eq!(
        Frame::decode(&wire, MAX).unwrap().0,
        Frame::Data {
            stream: 1,
            end_stream: false,
            data: b""
        }
    );
    // An unknown type is skipped whole.
    let wire = frame(0xfa, 0xff, 9, b"anything");
    assert_eq!(
        Frame::decode(&wire, MAX).unwrap(),
        (
            Frame::Unknown {
                kind: 0xfa,
                stream: 9
            },
            wire.len()
        )
    );
}

fn refusal(wire: &[u8]) -> &'static str {
    match Frame::decode(wire, MAX) {
        Err(DecodeError::Malformed { what }) => what,
        other => panic!("{wire:02x?}: {other:?}"),
    }
}

#[test]
fn golden_h2_refusals() {
    let cases: &[(&str, Vec<u8>)] = &[
        ("padding", frame(0x0, 0x8, 1, &[3, 0, 0])),
        ("frame length", frame(0x0, 0x8, 1, &[])),
        ("padding", frame(0x1, 0x28, 1, &[1, 0, 0, 0, 0, 0])),
        ("frame length", frame(0x1, 0x20, 1, &[0, 0, 0])),
        ("frame length", frame(0x2, 0, 1, &[0, 0, 0, 0])),
        ("frame length", frame(0x3, 0, 1, &[0, 0, 0])),
        ("frame length", frame(0x4, 0, 0, &[0, 2, 0, 0, 0])),
        ("frame length", frame(0x4, 0x1, 0, &[0, 2, 0, 0, 0, 0])),
        ("frame length", frame(0x5, 0, 1, &[0, 0, 0])),
        ("frame length", frame(0x6, 0, 0, b"1234567")),
        ("frame length", frame(0x7, 0, 0, &[0, 0, 0, 0, 0, 0, 0])),
        ("frame length", frame(0x8, 0, 0, &[0, 0, 1])),
        ("stream identifier", frame(0x0, 0, 0, b"x")),
        ("stream identifier", frame(0x1, 0x4, 0, &[0x82])),
        ("stream identifier", frame(0x2, 0, 0, &[0, 0, 0, 0, 15])),
        ("stream identifier", frame(0x3, 0, 0, &[0, 0, 0, 8])),
        ("stream identifier", frame(0x5, 0x4, 0, &[0, 0, 0, 2])),
        ("stream identifier", frame(0x9, 0x4, 0, &[0x82])),
        ("stream identifier", frame(0x4, 0x1, 1, &[])),
        ("stream identifier", frame(0x6, 0, 1, b"12345678")),
        ("stream identifier", frame(0x7, 0, 1, &[0; 8])),
        (
            "SETTINGS_ENABLE_PUSH",
            frame(0x4, 0, 0, &[0, 2, 0, 0, 0, 2]),
        ),
        (
            "SETTINGS_INITIAL_WINDOW_SIZE",
            frame(0x4, 0, 0, &[0, 4, 0x80, 0, 0, 0]),
        ),
        (
            "SETTINGS_MAX_FRAME_SIZE",
            frame(0x4, 0, 0, &[0, 5, 0, 0, 0x3f, 0xff]),
        ),
        (
            "SETTINGS_MAX_FRAME_SIZE",
            frame(0x4, 0, 0, &[0, 5, 0x01, 0, 0, 0]),
        ),
    ];
    for (what, wire) in cases {
        assert_eq!(refusal(wire), *what, "{wire:02x?}");
    }
}

#[test]
fn golden_h2_settings_bounds_are_inclusive() {
    for value in [16_384u32, 16_777_215] {
        let mut p = vec![0, 5];
        p.extend_from_slice(&value.to_be_bytes());
        assert!(Frame::decode(&frame(0x4, 0, 0, &p), MAX).is_ok());
    }
    let mut p = vec![0, 4];
    p.extend_from_slice(&((1u32 << 31) - 1).to_be_bytes());
    assert!(Frame::decode(&frame(0x4, 0, 0, &p), MAX).is_ok());
}
