//! Deterministic hostile-input smoke tests.
//!
//! The always-on companion to the `cargo fuzz` targets under
//! `crates/http/weida-http-codec/fuzz`: the same properties, driven by a
//! seeded xorshift generator so they run on stable Rust in the unit tier, with
//! no nightly toolchain and no corpus.
//!
//! The properties:
//!
//! 1. **Decoding never panics.** An error is only ever a return value.
//! 2. **An accepted input re-encodes to octets that decode to the same
//!    value.**
//! 3. **Every proper prefix of a valid input is `Incomplete`**
//!    (`fuzz_smoke_prefixes`), so a reader that gets a violation knows the
//!    peer is wrong and not merely slow.
//!
//! The generator builds well-formed inputs and then mutates a few octets, so
//! the decoders see both valid input and input one edit away from it; each
//! test asserts a minimum acceptance rate, so a generator that stops reaching
//! the decoder fails rather than passing vacuously.

use weida_http_codec::fields::Field;
use weida_http_codec::h1::{self, Chunk, ChunkedDecoder};
use weida_http_codec::hpack;
use weida_http_codec::uri::Target;
use weida_http_codec::{DecodeError, Limits};

const ITERATIONS: usize = 100_000;

/// Small, so that the caps are reached by a few dozen octets.
const LIMITS: Limits = Limits {
    max_head_bytes: 512,
    max_fields: 8,
    max_chunk_line: 32,
    max_frame_size: 16_384,
};

/// Deterministic xorshift64* generator: no dependency, reproducible failures.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    pub fn bytes(&mut self, alphabet: &[u8], max: usize) -> Vec<u8> {
        let n = self.below(max + 1);
        (0..n).map(|_| *self.pick(alphabet)).collect()
    }

    /// Up to three random edits: flip, insert or delete an octet.
    pub fn mutate(&mut self, mut v: Vec<u8>) -> Vec<u8> {
        for _ in 0..self.below(4) {
            let at = self.below(v.len() + 1);
            match self.below(3) {
                0 if at < v.len() => v[at] = self.next() as u8,
                1 => v.insert(at, *self.pick(b"\r\n :\t\0\x7fA/;")),
                2 if at < v.len() => {
                    v.remove(at);
                }
                _ => {}
            }
        }
        v
    }
}

const TOKEN: &[u8] = b"abcXYZ-_09!~";
const VALUE: &[u8] = b"abc XYZ09,;=\"\t\x80";

fn fields(rng: &mut Rng) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..rng.below(6) {
        let mut name = rng.bytes(TOKEN, 8);
        if name.is_empty() {
            name.push(b'x');
        }
        out.extend_from_slice(&name);
        out.extend_from_slice(b":");
        out.extend_from_slice(rng.pick(&[&b" "[..], b"", b"\t"]));
        out.extend_from_slice(&rng.bytes(VALUE, 12));
        out.extend_from_slice(rng.pick(&[&b"\r\n"[..], b"\n"]));
    }
    out
}

pub fn request(rng: &mut Rng) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(rng.pick(&[&b""[..], b"", b"\r\n"]));
    out.extend_from_slice(rng.pick(&[&b"GET"[..], b"POST", b"HEAD", b"PATCH", b"X-Y"]));
    out.push(b' ');
    out.extend_from_slice(rng.pick(&[&b"/"[..], b"/a?b=c", b"*", b"http://h/p"]));
    out.push(b' ');
    out.extend_from_slice(rng.pick(&[&b"HTTP/1.1"[..], b"HTTP/1.0"]));
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&fields(rng));
    out.extend_from_slice(b"\r\n");
    out
}

pub fn response(rng: &mut Rng) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(rng.pick(&[&b"HTTP/1.1 "[..], b"HTTP/1.0 "]));
    out.extend_from_slice(rng.pick(&[&b"200 OK"[..], b"204", b"404 Not Found", b"999 x", b"100 "]));
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&fields(rng));
    out.extend_from_slice(b"\r\n");
    out
}

pub fn chunked(rng: &mut Rng) -> (Vec<u8>, Vec<u8>) {
    let mut wire = Vec::new();
    let mut body = Vec::new();
    for _ in 0..rng.below(4) {
        let data = rng.bytes(b"ab\r\n0", 20);
        if data.is_empty() {
            continue;
        }
        body.extend_from_slice(&data);
        wire.extend_from_slice(format!("{:x}", data.len()).as_bytes());
        wire.extend_from_slice(rng.pick(&[&b""[..], b";ext=1", b" ;x"]));
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(&data);
        wire.extend_from_slice(b"\r\n");
    }
    wire.extend_from_slice(b"0\r\n");
    wire.extend_from_slice(&fields(rng));
    wire.extend_from_slice(b"\r\n");
    (wire, body)
}

fn pairs<'a>(fields: &'a [Field<'a>]) -> Vec<(&'a [u8], &'a [u8])> {
    fields.iter().map(|f| (f.name, f.value)).collect()
}

/// Decodes a whole chunked body; `Ok` holds the body and the trailers'
/// re-encoding input, `Err` the first error.
fn run_chunked(input: &[u8]) -> Result<(Vec<u8>, usize, usize), DecodeError> {
    let mut d = ChunkedDecoder::new();
    let mut pos = 0;
    let mut body = Vec::new();
    loop {
        let (chunk, used) = d.step(&input[pos..], LIMITS)?;
        assert!(used > 0 && pos + used <= input.len());
        pos += used;
        match chunk {
            Chunk::Data(data) => {
                assert!(!data.is_empty());
                body.extend_from_slice(data);
            }
            Chunk::End(trailers) => {
                assert!(trailers.len() <= LIMITS.max_fields);
                return Ok((body, trailers.len(), pos));
            }
        }
    }
}

#[test]
fn fuzz_smoke_h1_request() {
    let mut rng = Rng::new(0x5eed_0001);
    let mut accepted = 0;
    for _ in 0..ITERATIONS {
        let input = request(&mut rng);
        let input = rng.mutate(input);
        let Ok((head, used)) = h1::decode_request(&input, LIMITS) else {
            continue;
        };
        accepted += 1;
        assert!(used <= input.len() && used <= LIMITS.max_head_bytes);
        assert!(head.fields.len() <= LIMITS.max_fields);
        let mut out = Vec::new();
        h1::encode_request_head(head.method, head.target, &pairs(&head.fields), &mut out)
            .expect("an accepted head re-encodes");
        let (again, again_used) =
            h1::decode_request(&out, LIMITS).expect("the re-encoding decodes");
        assert_eq!(again_used, out.len());
        assert_eq!(
            (again.method, again.target, again.fields),
            (head.method, head.target, head.fields)
        );
    }
    assert!(accepted > ITERATIONS / 5, "only {accepted} accepted");
}

#[test]
fn fuzz_smoke_h1_response() {
    let mut rng = Rng::new(0x5eed_0002);
    let mut accepted = 0;
    for _ in 0..ITERATIONS {
        let input = response(&mut rng);
        let input = rng.mutate(input);
        let Ok((head, used)) = h1::decode_response(&input, LIMITS) else {
            continue;
        };
        accepted += 1;
        assert!(used <= input.len());
        let mut out = Vec::new();
        h1::encode_response_head(
            head.minor,
            head.status,
            head.reason,
            &pairs(&head.fields),
            &mut out,
        )
        .expect("an accepted head re-encodes");
        let (again, _) = h1::decode_response(&out, LIMITS).expect("the re-encoding decodes");
        assert_eq!(again, head);
        let _ = h1::response_framing(b"GET", head.status, &head.fields);
    }
    assert!(accepted > ITERATIONS / 5, "only {accepted} accepted");
}

#[test]
fn fuzz_smoke_chunked() {
    let mut rng = Rng::new(0x5eed_0003);
    let mut accepted = 0;
    for i in 0..ITERATIONS {
        let (wire, body) = chunked(&mut rng);
        if i % 2 == 0 {
            let (decoded, _, used) = run_chunked(&wire).expect("a generated body decodes");
            assert_eq!(decoded, body);
            assert_eq!(used, wire.len());
            accepted += 1;
        } else {
            let wire = rng.mutate(wire);
            let _ = run_chunked(&wire);
        }
    }
    assert!(accepted > 0);
}

#[test]
fn fuzz_smoke_uri() {
    let mut rng = Rng::new(0x5eed_0004);
    let mut accepted = 0;
    for _ in 0..ITERATIONS {
        let mut input = Vec::new();
        input.extend_from_slice(rng.pick(&[&b"http://"[..], b"HTTPS://", b"https://"]));
        input.extend_from_slice(rng.pick(&[
            &b"example.com"[..],
            b"[::1]",
            b"10.0.0.1",
            b"a-b.c",
            b"",
        ]));
        input.extend_from_slice(rng.pick(&[&b""[..], b":8080", b":443", b":", b":99999"]));
        input.extend_from_slice(&rng.bytes(b"/?#a=&%20b", 12));
        let input = rng.mutate(input);
        let Ok(text) = core::str::from_utf8(&input) else {
            continue;
        };
        let Ok(t) = Target::parse(text) else {
            continue;
        };
        accepted += 1;
        assert!(t.path_and_query.starts_with('/'));
        assert!(!t.path_and_query.contains('#'));
        assert_ne!(t.port, 0);
        let rebuilt = format!(
            "{}://{}{}",
            t.scheme.as_str(),
            t.authority(),
            t.path_and_query
        );
        assert_eq!(Target::parse(&rebuilt).expect("a rebuilt URL parses"), t);
    }
    assert!(accepted > ITERATIONS / 10, "only {accepted} accepted");
}

fn assert_incomplete<T: core::fmt::Debug>(r: Result<T, DecodeError>, input: &[u8]) {
    assert!(
        matches!(r, Err(DecodeError::Incomplete { .. })),
        "{:?}: {r:?}",
        String::from_utf8_lossy(input)
    );
}

#[test]
fn fuzz_smoke_prefixes() {
    let mut rng = Rng::new(0x5eed_0005);
    for _ in 0..ITERATIONS / 100 {
        let input = request(&mut rng);
        if h1::decode_request(&input, LIMITS).is_ok() {
            for cut in 0..input.len() {
                assert_incomplete(h1::decode_request(&input[..cut], LIMITS), &input[..cut]);
            }
        }
        let input = response(&mut rng);
        if h1::decode_response(&input, LIMITS).is_ok() {
            for cut in 0..input.len() {
                assert_incomplete(h1::decode_response(&input[..cut], LIMITS), &input[..cut]);
            }
        }
        let (wire, _) = chunked(&mut rng);
        for cut in 0..wire.len() {
            assert_incomplete(run_chunked(&wire[..cut]), &wire[..cut]);
        }
    }
}

#[test]
fn fuzz_smoke_hpack() {
    let mut rng = Rng::new(0x5eed_0006);
    // One encoder and decoder pair across every block, so the dynamic table
    // fills, evicts and is resized; a mutated copy of each block goes to a
    // fresh decoder.
    let mut enc = hpack::Encoder::new();
    let mut dec = hpack::Decoder::new(4096);
    let names: &[&[u8]] = &[
        b":path",
        b"x-a",
        b"cookie",
        b"accept",
        b"content-type",
        b"x-long-name",
    ];
    let mut accepted = 0;
    for i in 0..ITERATIONS / 10 {
        if i % 97 == 0 {
            let size = [0, 64, 256, 4096][rng.below(4)];
            enc.set_peer_max_table_size(size);
            dec.set_max_table_size(4096);
        }
        let list: Vec<(Vec<u8>, Vec<u8>, bool)> = (0..rng.below(6))
            .map(|_| {
                let name = rng.pick(names).to_vec();
                let value = rng.bytes(b"abc/09 =;", 16);
                (name, value, rng.below(8) == 0)
            })
            .collect();
        let fields: Vec<_> = list
            .iter()
            .map(|(n, v, s)| (n.as_slice(), v.as_slice(), *s))
            .collect();
        let mut block = Vec::new();
        enc.encode(&fields, &mut block);
        let mut out = Vec::new();
        dec.decode(&block, Limits::DEFAULT, &mut out)
            .expect("our own block decodes");
        let expect: Vec<_> = list
            .iter()
            .map(|(n, v, _)| (n.clone(), v.clone()))
            .collect();
        assert_eq!(out, expect);

        let mutated = rng.mutate(block);
        let mut out = Vec::new();
        if hpack::Decoder::new(4096)
            .decode(&mutated, LIMITS, &mut out)
            .is_err()
        {
            continue;
        }
        accepted += 1;
        assert!(out.len() <= LIMITS.max_fields);
        let fields: Vec<_> = out
            .iter()
            .map(|(n, v)| (n.as_slice(), v.as_slice(), false))
            .collect();
        let mut again = Vec::new();
        hpack::Encoder::new().encode(&fields, &mut again);
        let mut back = Vec::new();
        hpack::Decoder::new(4096)
            .decode(&again, LIMITS, &mut back)
            .expect("the re-encoding decodes");
        assert_eq!(back, out);
    }
    assert!(accepted > ITERATIONS / 100, "only {accepted} accepted");
}
