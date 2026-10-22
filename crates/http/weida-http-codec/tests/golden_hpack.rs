//! RFC 7541 Appendix C, block by block: every block decodes to the RFC's
//! header list, and every block this crate's encoding strategy writes the
//! same way re-encodes byte for byte (see the `hpack` module doc for the
//! three that it does not, and why).

use weida_http_codec::Limits;
use weida_http_codec::hpack::{Decoder, Encoder};

const L: Limits = Limits::DEFAULT;

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

type List = &'static [(&'static str, &'static str)];

fn decode(d: &mut Decoder, block: &str, expect: List) {
    let mut out = Vec::new();
    d.decode(&hex(block), L, &mut out).unwrap();
    let expect: Vec<_> = expect
        .iter()
        .map(|(n, v)| (n.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect();
    assert_eq!(out, expect, "{block}");
}

fn encode(e: &mut Encoder, list: List) -> Vec<u8> {
    let fields: Vec<_> = list
        .iter()
        .map(|(n, v)| (n.as_bytes(), v.as_bytes(), false))
        .collect();
    let mut out = Vec::new();
    e.encode(&fields, &mut out);
    out
}

#[test]
fn golden_rfc7541_c2_literals_and_index() {
    let blocks: [(&str, List); 4] = [
        (
            "400a637573746f6d2d6b65790d637573746f6d2d686561646572",
            &[("custom-key", "custom-header")],
        ),
        ("040c2f73616d706c652f70617468", &[(":path", "/sample/path")]),
        (
            "100870617373776f726406736563726574",
            &[("password", "secret")],
        ),
        ("82", &[(":method", "GET")]),
    ];
    for (block, list) in blocks {
        decode(&mut Decoder::new(4096), block, list);
    }
}

const REQ1: List = &[
    (":method", "GET"),
    (":scheme", "http"),
    (":path", "/"),
    (":authority", "www.example.com"),
];
const REQ2: List = &[
    (":method", "GET"),
    (":scheme", "http"),
    (":path", "/"),
    (":authority", "www.example.com"),
    ("cache-control", "no-cache"),
];
const REQ3: List = &[
    (":method", "GET"),
    (":scheme", "https"),
    (":path", "/index.html"),
    (":authority", "www.example.com"),
    ("custom-key", "custom-value"),
];

#[test]
fn golden_rfc7541_c3_requests_without_huffman() {
    let blocks = [
        ("828684410f7777772e6578616d706c652e636f6d", REQ1),
        ("828684be58086e6f2d6361636865", REQ2),
        (
            "828785bf400a637573746f6d2d6b65790c637573746f6d2d76616c7565",
            REQ3,
        ),
    ];
    let mut d = Decoder::new(4096);
    let mut e = Encoder::new();
    e.set_huffman(false);
    for (block, list) in blocks {
        decode(&mut d, block, list);
        assert_eq!(encode(&mut e, list), hex(block));
    }
}

#[test]
fn golden_rfc7541_c4_requests_with_huffman() {
    let blocks = [
        ("828684418cf1e3c2e5f23a6ba0ab90f4ff", REQ1),
        ("828684be5886a8eb10649cbf", REQ2),
        ("828785bf408825a849e95ba97d7f8925a849e95bb8e8b4bf", REQ3),
    ];
    let mut d = Decoder::new(4096);
    let mut e = Encoder::new();
    for (block, list) in blocks {
        decode(&mut d, block, list);
        assert_eq!(encode(&mut e, list), hex(block));
    }
}

const RESP1: List = &[
    (":status", "302"),
    ("cache-control", "private"),
    ("date", "Mon, 21 Oct 2013 20:13:21 GMT"),
    ("location", "https://www.example.com"),
];
const RESP2: List = &[
    (":status", "307"),
    ("cache-control", "private"),
    ("date", "Mon, 21 Oct 2013 20:13:21 GMT"),
    ("location", "https://www.example.com"),
];
const RESP3: List = &[
    (":status", "200"),
    ("cache-control", "private"),
    ("date", "Mon, 21 Oct 2013 20:13:22 GMT"),
    ("location", "https://www.example.com"),
    ("content-encoding", "gzip"),
    (
        "set-cookie",
        "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
    ),
];

#[test]
fn golden_rfc7541_c5_responses_without_huffman() {
    let blocks = [
        (
            "4803333032580770726976617465611d4d6f6e2c203231204f637420323031332032303a31333a323120474d546e1768747470733a2f2f7777772e6578616d706c652e636f6d",
            RESP1,
            true,
        ),
        ("4803333037c1c0bf", RESP2, true),
        (
            "88c1611d4d6f6e2c203231204f637420323031332032303a31333a323220474d54c05a04677a69707738666f6f3d4153444a4b48514b425a584f5157454f50495541585157454f49553b206d61782d6167653d333630303b2076657273696f6e3d31",
            RESP3,
            // set-cookie: indexed by the RFC, never-indexed here.
            false,
        ),
    ];
    let mut d = Decoder::new(256);
    let mut e = Encoder::with_table_size(256);
    e.set_huffman(false);
    for (block, list, exact) in blocks {
        decode(&mut d, block, list);
        let ours = encode(&mut e, list);
        if exact {
            assert_eq!(ours, hex(block));
        }
    }
}

/// The C.6 blocks: Huffman-coded.
#[test]
fn golden_rfc7541_c6_responses_with_huffman() {
    let blocks = [
        (
            "488264025885aec3771a4b6196d07abe941054d444a8200595040b8166e082a62d1bff6e919d29ad171863c78f0b97c8e9ae82ae43d3",
            RESP1,
            true,
        ),
        // `307` is as long Huffman-coded as plain; this encoder writes it
        // plain.
        ("4883640effc1c0bf", RESP2, false),
        (
            "88c16196d07abe941054d444a8200595040b8166e084a62d1bffc05a839bd9ab77ad94e7821dd7f2e6c7b335dfdfcd5b3960d5af27087f3672c1ab270fb5291f9587316065c003ed4ee5b1063d5007",
            RESP3,
            false,
        ),
    ];
    let mut d = Decoder::new(256);
    let mut e = Encoder::with_table_size(256);
    for (block, list, exact) in blocks {
        decode(&mut d, block, list);
        let ours = encode(&mut e, list);
        if exact {
            assert_eq!(ours, hex(block));
        }
    }
}

/// Our own C.5/C.6 encodings decode to the same lists through a decoder that
/// saw the same sequence, whether or not they match the RFC octets.
#[test]
fn golden_rfc7541_c5_c6_our_encodings_decode() {
    for huffman in [false, true] {
        let mut d = Decoder::new(256);
        let mut e = Encoder::with_table_size(256);
        e.set_huffman(huffman);
        for list in [RESP1, RESP2, RESP3] {
            let ours = encode(&mut e, list);
            let mut out = Vec::new();
            d.decode(&ours, L, &mut out).unwrap();
            assert_eq!(out.len(), list.len());
            for ((n, v), (en, ev)) in out.iter().zip(list) {
                assert_eq!((n.as_slice(), v.as_slice()), (en.as_bytes(), ev.as_bytes()));
            }
        }
    }
}
