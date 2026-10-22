//! QPACK, the field compression of HTTP/3 (RFC 9204), with the static table
//! only.
//!
//! An endpoint that advertises `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0` never
//! has a dynamic table, needs neither an encoder nor a decoder stream
//! (RFC 9204 §4.2), and every field section it receives must have a Required
//! Insert Count of 0. So [`decode_field_section`] refuses every reference to a
//! dynamic table as `"dynamic table reference"`, which a connection maps onto
//! `QPACK_DECOMPRESSION_FAILED`, and [`encode_field_section`] writes only
//! static references and literals.
//!
//! Strategy of the encoder, fixed:
//!
//! * a field found whole in the static table is an indexed field line;
//! * a field whose name is in the static table is a literal with a name
//!   reference;
//! * anything else is a literal with a literal name;
//! * a sensitive field (the flag, or [`is_sensitive`]) sets the N bit, so an
//!   intermediary never puts it into a dynamic table (RFC 9204 §7.1.3);
//! * a string is Huffman-coded when that is strictly shorter.

use crate::error::DecodeError;
use crate::fields::is_sensitive;
use crate::limits::Limits;
use crate::prefix::{decode_int, decode_string, encode_int, encode_string};

/// The static table, Appendix A: index 0 is the first entry.
pub const STATIC: [(&[u8], &[u8]); 99] = [
    (b":authority", b""),
    (b":path", b"/"),
    (b"age", b"0"),
    (b"content-disposition", b""),
    (b"content-length", b"0"),
    (b"cookie", b""),
    (b"date", b""),
    (b"etag", b""),
    (b"if-modified-since", b""),
    (b"if-none-match", b""),
    (b"last-modified", b""),
    (b"link", b""),
    (b"location", b""),
    (b"referer", b""),
    (b"set-cookie", b""),
    (b":method", b"CONNECT"),
    (b":method", b"DELETE"),
    (b":method", b"GET"),
    (b":method", b"HEAD"),
    (b":method", b"OPTIONS"),
    (b":method", b"POST"),
    (b":method", b"PUT"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"103"),
    (b":status", b"200"),
    (b":status", b"304"),
    (b":status", b"404"),
    (b":status", b"503"),
    (b"accept", b"*/*"),
    (b"accept", b"application/dns-message"),
    (b"accept-encoding", b"gzip, deflate, br"),
    (b"accept-ranges", b"bytes"),
    (b"access-control-allow-headers", b"cache-control"),
    (b"access-control-allow-headers", b"content-type"),
    (b"access-control-allow-origin", b"*"),
    (b"cache-control", b"max-age=0"),
    (b"cache-control", b"max-age=2592000"),
    (b"cache-control", b"max-age=604800"),
    (b"cache-control", b"no-cache"),
    (b"cache-control", b"no-store"),
    (b"cache-control", b"public, max-age=31536000"),
    (b"content-encoding", b"br"),
    (b"content-encoding", b"gzip"),
    (b"content-type", b"application/dns-message"),
    (b"content-type", b"application/javascript"),
    (b"content-type", b"application/json"),
    (b"content-type", b"application/x-www-form-urlencoded"),
    (b"content-type", b"image/gif"),
    (b"content-type", b"image/jpeg"),
    (b"content-type", b"image/png"),
    (b"content-type", b"text/css"),
    (b"content-type", b"text/html; charset=utf-8"),
    (b"content-type", b"text/plain"),
    (b"content-type", b"text/plain;charset=utf-8"),
    (b"range", b"bytes=0-"),
    (b"strict-transport-security", b"max-age=31536000"),
    (
        b"strict-transport-security",
        b"max-age=31536000; includesubdomains",
    ),
    (
        b"strict-transport-security",
        b"max-age=31536000; includesubdomains; preload",
    ),
    (b"vary", b"accept-encoding"),
    (b"vary", b"origin"),
    (b"x-content-type-options", b"nosniff"),
    (b"x-xss-protection", b"1; mode=block"),
    (b":status", b"100"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"302"),
    (b":status", b"400"),
    (b":status", b"403"),
    (b":status", b"421"),
    (b":status", b"425"),
    (b":status", b"500"),
    (b"accept-language", b""),
    (b"access-control-allow-credentials", b"FALSE"),
    (b"access-control-allow-credentials", b"TRUE"),
    (b"access-control-allow-headers", b"*"),
    (b"access-control-allow-methods", b"get"),
    (b"access-control-allow-methods", b"get, post, options"),
    (b"access-control-allow-methods", b"options"),
    (b"access-control-expose-headers", b"content-length"),
    (b"access-control-request-headers", b"content-type"),
    (b"access-control-request-method", b"get"),
    (b"access-control-request-method", b"post"),
    (b"alt-svc", b"clear"),
    (b"authorization", b""),
    (
        b"content-security-policy",
        b"script-src 'none'; object-src 'none'; base-uri 'none'",
    ),
    (b"early-data", b"1"),
    (b"expect-ct", b""),
    (b"forwarded", b""),
    (b"if-range", b""),
    (b"origin", b""),
    (b"purpose", b"prefetch"),
    (b"server", b""),
    (b"timing-allow-origin", b"*"),
    (b"upgrade-insecure-requests", b"1"),
    (b"user-agent", b""),
    (b"x-forwarded-for", b""),
    (b"x-frame-options", b"deny"),
    (b"x-frame-options", b"sameorigin"),
];

const fn malformed(what: &'static str) -> DecodeError {
    DecodeError::Malformed { what }
}

const DYNAMIC: DecodeError = malformed("dynamic table reference");

fn static_entry(index: u64) -> Result<(&'static [u8], &'static [u8]), DecodeError> {
    usize::try_from(index)
        .ok()
        .and_then(|i| STATIC.get(i))
        .copied()
        .ok_or(malformed("index"))
}

/// Decodes one complete field section, appending `(name, value)` pairs to
/// `out` in order.
///
/// # Errors
///
/// `Malformed` `"dynamic table reference"` for a Required Insert Count other
/// than 0 or any line that refers to a dynamic table; `"index"` past the
/// static table; `"uppercase field name"`; `"truncated field section"`;
/// `"integer"` and the Huffman refusals. `TooLarge` `"field section"` and
/// `"field count"` as HPACK's decoder counts them, `"string"` for one string
/// longer than [`Limits::max_head_bytes`].
pub fn decode_field_section(
    block: &[u8],
    limits: Limits,
    out: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), DecodeError> {
    let truncated = |e: DecodeError| match e {
        DecodeError::Incomplete { .. } => malformed("truncated field section"),
        e => e,
    };
    let cap = limits.max_head_bytes;
    // Encoded Required Insert Count, then the sign bit and Delta Base.
    let (ric, used) = decode_int(block, 8).map_err(truncated)?;
    if ric != 0 {
        return Err(DYNAMIC);
    }
    let mut pos = used;
    let (_, used) = decode_int(&block[pos..], 7).map_err(truncated)?;
    pos += used;

    let mut counted = 0usize;
    let mut count = 0usize;
    while pos < block.len() {
        let b = block[pos];
        let rest = &block[pos..];
        let (name, value) = if b & 0x80 != 0 {
            // 1T: indexed field line.
            if b & 0x40 == 0 {
                return Err(DYNAMIC);
            }
            let (idx, used) = decode_int(rest, 6).map_err(truncated)?;
            pos += used;
            let (n, v) = static_entry(idx)?;
            (n.to_vec(), v.to_vec())
        } else if b & 0x40 != 0 {
            // 01NT: literal with name reference.
            if b & 0x10 == 0 {
                return Err(DYNAMIC);
            }
            let (idx, used) = decode_int(rest, 4).map_err(truncated)?;
            pos += used;
            let name = static_entry(idx)?.0.to_vec();
            let (value, used) = decode_string(&block[pos..], 0x80, 7, cap).map_err(truncated)?;
            pos += used;
            (name, value)
        } else if b & 0x20 != 0 {
            // 001NH: literal with literal name.
            let (name, used) = decode_string(rest, 0x08, 3, cap).map_err(truncated)?;
            pos += used;
            let (value, used) = decode_string(&block[pos..], 0x80, 7, cap).map_err(truncated)?;
            pos += used;
            (name, value)
        } else {
            // 0001: post-base indexed; 0000N: post-base name reference.
            return Err(DYNAMIC);
        };
        if name.iter().any(u8::is_ascii_uppercase) {
            return Err(malformed("uppercase field name"));
        }
        count += 1;
        if count > limits.max_fields {
            return Err(DecodeError::TooLarge {
                what: "field count",
                declared: count as u64,
                cap: limits.max_fields as u64,
            });
        }
        counted += name.len() + value.len() + 32;
        if counted > cap {
            return Err(DecodeError::TooLarge {
                what: "field section",
                declared: counted as u64,
                cap: cap as u64,
            });
        }
        out.push((name, value));
    }
    Ok(())
}

/// Encodes one field section against the static table only. Each field is
/// `(name, value, sensitive)`; names must already be lowercase.
pub fn encode_field_section(fields: &[(&[u8], &[u8], bool)], out: &mut Vec<u8>) {
    // Required Insert Count 0, Delta Base 0.
    out.extend_from_slice(&[0x00, 0x00]);
    for &(name, value, sensitive) in fields {
        let n_bit = sensitive || is_sensitive(name);
        let mut by_name = None;
        let mut exact = None;
        for (i, (n, v)) in STATIC.iter().enumerate() {
            if *n == name {
                by_name.get_or_insert(i);
                if *v == value {
                    exact = Some(i);
                    break;
                }
            }
        }
        if let Some(i) = exact {
            encode_int(i as u64, 6, 0xc0, out);
            continue;
        }
        match by_name {
            Some(i) => {
                let first = 0x50 | if n_bit { 0x20 } else { 0 };
                encode_int(i as u64, 4, first, out);
            }
            None => {
                let first = 0x20 | if n_bit { 0x10 } else { 0 };
                encode_string(name, first, 0x08, 3, true, out);
            }
        }
        encode_string(value, 0, 0x80, 7, true, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Limits = Limits::DEFAULT;

    type Fields = Vec<(Vec<u8>, Vec<u8>)>;

    fn decode(block: &[u8]) -> Result<Fields, DecodeError> {
        let mut out = Vec::new();
        decode_field_section(block, L, &mut out).map(|()| out)
    }

    #[test]
    fn every_line_shape_round_trips() {
        let fields: &[(&[u8], &[u8], bool)] = &[
            (b":method", b"GET", false),
            (b":path", b"/a/b", false),
            (b"x-custom", b"value", false),
            (b"authorization", b"Bearer t", false),
            (b"x-secret", b"s", true),
            (b"accept", b"", false),
        ];
        let mut block = Vec::new();
        encode_field_section(fields, &mut block);
        let out = decode(&block).unwrap();
        let expect: Vec<_> = fields
            .iter()
            .map(|(n, v, _)| (n.to_vec(), v.to_vec()))
            .collect();
        assert_eq!(out, expect);
        // `:method GET` is static index 17: 11 + 010001.
        assert_eq!(block[2], 0xd1);
    }

    #[test]
    fn sensitive_fields_set_the_never_indexed_bit() {
        let mut block = Vec::new();
        encode_field_section(&[(b"authorization", b"x", false)], &mut block);
        // 01NT with N set: name index 84 does not fit 4 bits.
        assert_eq!(block[2], 0x7f);
        let mut block = Vec::new();
        encode_field_section(&[(b"x-secret", b"s", true)], &mut block);
        assert_eq!(block[2] & 0xf0, 0x30);
    }

    #[test]
    fn every_dynamic_reference_is_refused() {
        for block in [
            &[0x01, 0x00][..],         // Required Insert Count 1
            &[0x00, 0x00, 0x80],       // indexed, T = 0
            &[0x00, 0x00, 0x40, 0x00], // name reference, T = 0
            &[0x00, 0x00, 0x10],       // post-base indexed
            &[0x00, 0x00, 0x00, 0x00], // post-base name reference
        ] {
            assert_eq!(decode(block), Err(DYNAMIC), "{block:02x?}");
        }
    }

    #[test]
    fn the_static_table_ends_at_98() {
        let mut block = vec![0, 0];
        encode_int(98, 6, 0xc0, &mut block);
        assert_eq!(decode(&block).unwrap()[0].0, b"x-frame-options");
        let mut block = vec![0, 0];
        encode_int(99, 6, 0xc0, &mut block);
        assert_eq!(decode(&block), Err(malformed("index")));
    }

    #[test]
    fn a_truncated_section_is_malformed() {
        assert_eq!(decode(&[0x00]), Err(malformed("truncated field section")));
        assert_eq!(
            decode(&[0x00, 0x00, 0x51, 0x05, b'a']),
            Err(malformed("truncated field section"))
        );
    }
}
