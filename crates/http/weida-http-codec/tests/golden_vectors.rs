//! Golden vectors: the RFCs' own examples, decoded and, where this crate
//! encodes the same shape, re-encoded byte for byte; and one vector per rule
//! the decoders refuse, named by the rule.

use weida_http_codec::fields::Field;
use weida_http_codec::h1::{self, Chunk, ChunkedDecoder, Framing};
use weida_http_codec::{DecodeError, Limits, date};

const L: Limits = Limits::DEFAULT;

fn pairs<'a>(fields: &'a [Field<'a>]) -> Vec<(&'a [u8], &'a [u8])> {
    fields.iter().map(|f| (f.name, f.value)).collect()
}

/// RFC 9110 §3.9, the example request.
const REQUEST: &[u8] = b"GET /hello.txt HTTP/1.1\r\n\
User-Agent: curl/7.64.1\r\n\
Host: www.example.com\r\n\
Accept-Language: en, mi\r\n\
\r\n";

/// RFC 9110 §3.9, the example response.
const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\n\
Date: Mon, 27 Jul 2009 12:28:53 GMT\r\n\
Server: Apache\r\n\
Last-Modified: Wed, 22 Jul 2009 19:15:56 GMT\r\n\
ETag: \"34aa387-d-1568eb00\"\r\n\
Accept-Ranges: bytes\r\n\
Content-Length: 51\r\n\
Vary: Accept-Encoding\r\n\
Content-Type: text/plain\r\n\
\r\n\
Hello World! My content includes a trailing CRLF.\r\n";

#[test]
fn golden_rfc9110_request() {
    let (head, used) = h1::decode_request(REQUEST, L).unwrap();
    assert_eq!(used, REQUEST.len());
    assert_eq!(head.method, b"GET");
    assert_eq!(head.target, b"/hello.txt");
    assert_eq!(head.minor, 1);
    assert_eq!(
        pairs(&head.fields),
        [
            (&b"User-Agent"[..], &b"curl/7.64.1"[..]),
            (b"Host", b"www.example.com"),
            (b"Accept-Language", b"en, mi"),
        ]
    );
    assert_eq!(h1::request_framing(&head.fields), Ok(Framing::Empty));

    let mut out = Vec::new();
    h1::encode_request_head(head.method, head.target, &pairs(&head.fields), &mut out).unwrap();
    assert_eq!(out, REQUEST);
}

#[test]
fn golden_rfc9110_response() {
    let (head, used) = h1::decode_response(RESPONSE, L).unwrap();
    assert_eq!(head.status, 200);
    assert_eq!(head.reason, b"OK");
    assert_eq!(head.fields.len(), 8);
    assert_eq!(head.fields[3].value, b"\"34aa387-d-1568eb00\"");
    let framing = h1::response_framing(b"GET", head.status, &head.fields).unwrap();
    assert_eq!(framing, Framing::Length(51));
    assert_eq!(
        &RESPONSE[used..],
        b"Hello World! My content includes a trailing CRLF.\r\n"
    );

    let mut out = Vec::new();
    h1::encode_response_head(
        head.minor,
        head.status,
        head.reason,
        &pairs(&head.fields),
        &mut out,
    )
    .unwrap();
    assert_eq!(out, &RESPONSE[..used]);
}

/// A chunked body with a trailer (RFC 9112 §7.1 grammar).
const CHUNKED: &[u8] = b"4\r\nWiki\r\n5\r\npedia\r\nE\r\n in\r\n\r\nchunks.\r\n0\r\n\
Expires: Sat, 27 Mar 2004 21:12:00 GMT\r\n\r\n";

#[test]
fn golden_chunked_with_a_trailer() {
    let mut d = ChunkedDecoder::new();
    let mut pos = 0;
    let mut body = Vec::new();
    let mut pieces = Vec::new();
    let trailers = loop {
        let (chunk, used) = d.step(&CHUNKED[pos..], L).unwrap();
        pos += used;
        match chunk {
            Chunk::Data(data) => {
                body.extend_from_slice(data);
                pieces.push(data);
            }
            Chunk::End(trailers) => break trailers,
        }
    };
    assert_eq!(pos, CHUNKED.len());
    assert_eq!(body, b"Wikipedia in\r\n\r\nchunks.");
    assert_eq!(
        pairs(&trailers),
        [(&b"Expires"[..], &b"Sat, 27 Mar 2004 21:12:00 GMT"[..])]
    );

    let mut out = Vec::new();
    for piece in pieces {
        h1::encode_chunk(piece, &mut out);
    }
    h1::encode_last_chunk(&pairs(&trailers), &mut out).unwrap();
    assert_eq!(out, CHUNKED);
}

#[test]
fn golden_rfc9110_date() {
    // RFC 9110 §5.6.7: "Sun, 06 Nov 1994 08:49:37 GMT".
    let mut out = Vec::new();
    date::imf_fixdate(784_111_777, &mut out);
    assert_eq!(out, b"Sun, 06 Nov 1994 08:49:37 GMT");
}

fn request_refusal(input: &[u8]) -> &'static str {
    match h1::decode_request(input, L) {
        Err(DecodeError::Malformed { what }) => what,
        other => panic!("{:?}: {other:?}", String::from_utf8_lossy(input)),
    }
}

fn response_refusal(input: &[u8]) -> &'static str {
    match h1::decode_response(input, L) {
        Err(DecodeError::Malformed { what }) => what,
        other => panic!("{:?}: {other:?}", String::from_utf8_lossy(input)),
    }
}

#[test]
fn golden_head_refusals() {
    let cases: &[(&str, &[u8])] = &[
        ("request line", b"GET /  HTTP/1.1\r\n\r\n"),
        ("request line", b"GET / HTTP/1.1 x\r\n\r\n"),
        ("method", b"G(T / HTTP/1.1\r\n\r\n"),
        ("request target", b"GET  HTTP/1.1\r\n\r\n"),
        ("version", b"GET / HTTP/2.0\r\n\r\n"),
        ("version", b"GET / http/1.1\r\n\r\n"),
        (
            "obsolete line folding",
            b"GET / HTTP/1.1\r\nA: b\r\n c\r\n\r\n",
        ),
        (
            "field line without a colon",
            b"GET / HTTP/1.1\r\nA b\r\n\r\n",
        ),
        (
            "whitespace before the colon",
            b"GET / HTTP/1.1\r\nHost : a\r\n\r\n",
        ),
        ("field name", b"GET / HTTP/1.1\r\nA(b: c\r\n\r\n"),
        ("field name", b"GET / HTTP/1.1\r\n: c\r\n\r\n"),
        ("field value", b"GET / HTTP/1.1\r\nA: b\0c\r\n\r\n"),
        ("field value", b"GET / HTTP/1.1\r\nA: b\rc\r\n\r\n"),
    ];
    for (what, input) in cases {
        assert_eq!(
            request_refusal(input),
            *what,
            "{:?}",
            String::from_utf8_lossy(input)
        );
    }

    let cases: &[(&str, &[u8])] = &[
        ("status line", b"HTTP/1.1\r\n\r\n"),
        ("version", b"HTTP/1.2 200 OK\r\n\r\n"),
        ("status", b"HTTP/1.1 20 OK\r\n\r\n"),
        ("status", b"HTTP/1.1 099 OK\r\n\r\n"),
        ("status", b"HTTP/1.1 2000 OK\r\n\r\n"),
        ("reason phrase", b"HTTP/1.1 200 O\x7fK\r\n\r\n"),
        // The departure: obs-fold is refused in a response too.
        (
            "obsolete line folding",
            b"HTTP/1.1 200 OK\r\nA: b\r\n\tc\r\n\r\n",
        ),
    ];
    for (what, input) in cases {
        assert_eq!(
            response_refusal(input),
            *what,
            "{:?}",
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn golden_framing_refusals() {
    let te = Field {
        name: b"Transfer-Encoding",
        value: b"chunked",
    };
    let cl = Field {
        name: b"Content-Length",
        value: b"3",
    };
    assert_eq!(
        h1::request_framing(&[te, cl]),
        Err(DecodeError::Malformed {
            what: "both Transfer-Encoding and Content-Length"
        })
    );
    for value in [&b"gzip, chunked"[..], b"chunked, chunked", b"identity", b""] {
        let f = Field {
            name: b"Transfer-Encoding",
            value,
        };
        assert_eq!(
            h1::request_framing(&[f]),
            Err(DecodeError::Malformed {
                what: "transfer coding"
            })
        );
    }
    for value in [&b"3, 4"[..], b"-1", b"0x10", b"18446744073709551616"] {
        let f = Field {
            name: b"Content-Length",
            value,
        };
        assert_eq!(
            h1::request_framing(&[f]),
            Err(DecodeError::Malformed {
                what: "Content-Length"
            }),
            "{:?}",
            String::from_utf8_lossy(value)
        );
    }
}

#[test]
fn golden_chunk_refusals() {
    let malformed = |input: &[u8]| {
        let mut d = ChunkedDecoder::new();
        let mut pos = 0;
        loop {
            match d.step(&input[pos..], L) {
                Ok((_, used)) => pos += used,
                Err(DecodeError::Malformed { what }) => return what,
                Err(e) => panic!("{e:?}"),
            }
        }
    };
    assert_eq!(malformed(b"x\r\n"), "chunk size");
    assert_eq!(malformed(b"3x\r\n"), "chunk size");
    assert_eq!(malformed(b"3\r\nabcX\r\n"), "chunk data terminator");
    assert_eq!(
        malformed(b"0\r\nA : b\r\n\r\n"),
        "whitespace before the colon"
    );
    assert_eq!(malformed(b"0\r\n\r\n"), "chunked body already ended");
}

#[test]
fn golden_caps() {
    let small = Limits {
        max_head_bytes: 32,
        max_fields: 1,
        max_chunk_line: 6,
        ..L
    };
    let too_large = |r: Result<_, DecodeError>| match r {
        Err(DecodeError::TooLarge { what, .. }) => what,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        too_large(h1::decode_request(&[b'a'; 33], small).map(|_| ())),
        "head"
    );
    assert_eq!(
        too_large(h1::decode_response(&[b'a'; 32], small).map(|_| ())),
        "head"
    );
    assert_eq!(
        too_large(
            h1::decode_request(
                b"GET / HTTP/1.1\r\na: b\r\nc: d\r\n\r\n",
                Limits { max_fields: 1, ..L }
            )
            .map(|_| ())
        ),
        "field count"
    );
    assert_eq!(
        too_large(
            ChunkedDecoder::new()
                .step(b"1;abcdef\r\n", small)
                .map(|_| ())
        ),
        "chunk line"
    );
    let mut d = ChunkedDecoder::new();
    let trailer = [b"0\r\n".as_slice(), &[b'a'; 40]].concat();
    assert_eq!(
        too_large(d.step(&trailer, small).map(|_| ())),
        "trailer section"
    );
}
