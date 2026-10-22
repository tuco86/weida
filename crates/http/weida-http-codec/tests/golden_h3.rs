//! QUIC variable-length integers (RFC 9000 Appendix A.1), QPACK field
//! sections (RFC 9204 Appendix B.1) and the static-only rule, HTTP/3 frames.

use weida_http_codec::h3::{self, FrameHeader, frame, setting};
use weida_http_codec::{DecodeError, Limits, qpack, varint};

fn hex(s: &str) -> Vec<u8> {
    let s: String = s.split_whitespace().collect();
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn golden_rfc9000_a1_varints() {
    for (wire, value) in [
        ("c2197c5eff14e88c", 151_288_809_941_952_652u64),
        ("9d7f3e7d", 494_878_333),
        ("7bbd", 15_293),
        ("25", 37),
    ] {
        let wire = hex(wire);
        assert_eq!(varint::decode(&wire), Ok((value, wire.len())));
        let mut out = Vec::new();
        varint::encode(value, &mut out).unwrap();
        assert_eq!(out, wire);
    }
    // "the two-byte sequence 0x40 0x25 also decodes to 37".
    assert_eq!(varint::decode(&[0x40, 0x25]), Ok((37, 2)));
}

#[test]
fn golden_rfc9204_b1_field_section() {
    let mut out = Vec::new();
    qpack::decode_field_section(
        &hex("0000 510b 2f69 6e64 6578 2e68 746d 6c"),
        Limits::DEFAULT,
        &mut out,
    )
    .unwrap();
    assert_eq!(out, [(b":path".to_vec(), b"/index.html".to_vec())]);
}

#[test]
fn golden_qpack_a_required_insert_count_above_zero_is_refused() {
    // RFC 9204 Appendix B.2's first request on the request stream: Required
    // Insert Count 2 (encoded 03), Delta Base 0, two post-base indexed lines.
    let mut out = Vec::new();
    assert_eq!(
        qpack::decode_field_section(&hex("0381 10 11"), Limits::DEFAULT, &mut out),
        Err(DecodeError::Malformed {
            what: "dynamic table reference"
        })
    );
    assert!(out.is_empty());
}

#[test]
fn golden_qpack_round_trip() {
    let fields: &[(&[u8], &[u8], bool)] = &[
        (b":method", b"POST", false),
        (b":scheme", b"https", false),
        (b":authority", b"example.com", false),
        (b":path", b"/upload?x=1", false),
        (b"content-type", b"application/json", false),
        (b"x-trace", b"0123456789abcdef", false),
        (b"cookie", b"a=b", false),
    ];
    let mut block = Vec::new();
    qpack::encode_field_section(fields, &mut block);
    assert_eq!(&block[..2], [0, 0]);
    let mut out = Vec::new();
    qpack::decode_field_section(&block, Limits::DEFAULT, &mut out).unwrap();
    let expect: Vec<_> = fields
        .iter()
        .map(|(n, v, _)| (n.to_vec(), v.to_vec()))
        .collect();
    assert_eq!(out, expect);
}

#[test]
fn golden_h3_frames() {
    // SETTINGS { MAX_FIELD_SECTION_SIZE: 16384, QPACK_MAX_TABLE_CAPACITY: 0,
    // QPACK_BLOCKED_STREAMS: 0 }: type 04, length 09, then the pairs.
    let wire = hex("04 09 06 80004000 01 00 07 00");
    let mut out = Vec::new();
    h3::encode_settings(
        &[
            (setting::MAX_FIELD_SECTION_SIZE, 16_384),
            (setting::QPACK_MAX_TABLE_CAPACITY, 0),
            (setting::QPACK_BLOCKED_STREAMS, 0),
        ],
        &mut out,
    )
    .unwrap();
    assert_eq!(out, wire);
    let (head, used) = h3::decode_frame_header(&wire).unwrap();
    assert_eq!(
        head,
        FrameHeader {
            kind: frame::SETTINGS,
            len: 9
        }
    );
    assert_eq!(
        h3::decode_settings(&wire[used..], Limits::DEFAULT).unwrap(),
        [(6, 16_384), (1, 0), (7, 0)]
    );

    // DATA of 5 octets, HEADERS of 3, GOAWAY naming stream 4.
    let mut out = Vec::new();
    h3::encode_data_header(5, &mut out).unwrap();
    assert_eq!(out, [0x00, 0x05]);
    let mut out = Vec::new();
    h3::encode_headers(&[0, 0, 0xd1], &mut out);
    assert_eq!(out, [0x01, 0x03, 0, 0, 0xd1]);
    let mut out = Vec::new();
    h3::encode_goaway(4, &mut out).unwrap();
    assert_eq!(out, [0x07, 0x01, 0x04]);

    // A header split anywhere is incomplete.
    let wire = hex("4100 4400");
    for cut in 0..wire.len() {
        assert!(matches!(
            h3::decode_frame_header(&wire[..cut]),
            Err(DecodeError::Incomplete { .. })
        ));
    }
    assert_eq!(
        h3::decode_frame_header(&wire),
        Ok((
            FrameHeader {
                kind: 0x100,
                len: 0x400
            },
            4
        ))
    );
}
