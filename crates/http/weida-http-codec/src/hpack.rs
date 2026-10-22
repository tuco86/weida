//! HPACK, the field compression of HTTP/2 (RFC 7541).
//!
//! [`Decoder`] reads a complete field block (the payload of HEADERS plus its
//! CONTINUATION frames) against the static table and a dynamic table bounded
//! by the size this side advertised; [`Encoder`] writes one with a fixed,
//! simple strategy:
//!
//! * a field found whole in the static or dynamic table is written as an
//!   index;
//! * a sensitive field (the caller's flag, or a name
//!   [`is_sensitive`] names) is a literal
//!   *never indexed*, so no intermediary puts it into a compression context
//!   (RFC 7541 §7.1.3);
//! * everything else is a literal *with incremental indexing*, its name
//!   indexed when a table holds it;
//! * a string is Huffman-coded when that is strictly shorter.
//!
//! # The Appendix C vectors
//!
//! `tests/golden_vectors.rs` decodes every block of RFC 7541 Appendix C and
//! re-encodes C.3, C.4, C.5.1, C.5.2 and C.6.1 byte for byte. Three blocks
//! are asserted decode-only, each for a stated reason of this strategy:
//!
//! * **C.5.3 and C.6.3** carry `set-cookie`, which the RFC's encoder indexes
//!   and this one writes never-indexed, because it is a credential.
//! * **C.6.2** Huffman-codes `307`, whose Huffman form is exactly as long as
//!   its plain form; this encoder takes Huffman only when it is shorter.
//!
//! The C.3 and C.5 vectors are written without Huffman coding, which is what
//! [`Encoder::set_huffman`] exists for; C.5 and C.6 start from a 256-octet
//! table on both sides, which [`Encoder::with_table_size`] states.

use std::collections::VecDeque;

use crate::error::DecodeError;
use crate::fields::is_sensitive;
use crate::limits::Limits;
use crate::prefix::{decode_int, decode_string, encode_int, encode_string};

/// The static table, Appendix A: index 1 is the first entry.
pub const STATIC: [(&[u8], &[u8]); 61] = [
    (b":authority", b""),
    (b":method", b"GET"),
    (b":method", b"POST"),
    (b":path", b"/"),
    (b":path", b"/index.html"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"200"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"304"),
    (b":status", b"400"),
    (b":status", b"404"),
    (b":status", b"500"),
    (b"accept-charset", b""),
    (b"accept-encoding", b"gzip, deflate"),
    (b"accept-language", b""),
    (b"accept-ranges", b""),
    (b"accept", b""),
    (b"access-control-allow-origin", b""),
    (b"age", b""),
    (b"allow", b""),
    (b"authorization", b""),
    (b"cache-control", b""),
    (b"content-disposition", b""),
    (b"content-encoding", b""),
    (b"content-language", b""),
    (b"content-length", b""),
    (b"content-location", b""),
    (b"content-range", b""),
    (b"content-type", b""),
    (b"cookie", b""),
    (b"date", b""),
    (b"etag", b""),
    (b"expect", b""),
    (b"expires", b""),
    (b"from", b""),
    (b"host", b""),
    (b"if-match", b""),
    (b"if-modified-since", b""),
    (b"if-none-match", b""),
    (b"if-range", b""),
    (b"if-unmodified-since", b""),
    (b"last-modified", b""),
    (b"link", b""),
    (b"location", b""),
    (b"max-forwards", b""),
    (b"proxy-authenticate", b""),
    (b"proxy-authorization", b""),
    (b"range", b""),
    (b"referer", b""),
    (b"refresh", b""),
    (b"retry-after", b""),
    (b"server", b""),
    (b"set-cookie", b""),
    (b"strict-transport-security", b""),
    (b"transfer-encoding", b""),
    (b"user-agent", b""),
    (b"vary", b""),
    (b"via", b""),
    (b"www-authenticate", b""),
];

/// The default `SETTINGS_HEADER_TABLE_SIZE`, and the largest dynamic table
/// this crate's [`Encoder`] uses: 4096 octets (RFC 9113 §6.5.2).
pub const DEFAULT_TABLE_SIZE: u32 = 4096;

/// What one entry costs in a dynamic table (RFC 7541 §4.1).
fn entry_size(name: &[u8], value: &[u8]) -> usize {
    name.len() + value.len() + 32
}

/// The dynamic table: newest first, so index 62 is the front.
#[derive(Debug, Default)]
struct Table {
    entries: VecDeque<(Vec<u8>, Vec<u8>)>,
    size: usize,
    max: usize,
}

impl Table {
    fn new(max: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            size: 0,
            max,
        }
    }

    fn evict_to(&mut self, max: usize) {
        while self.size > max {
            let (n, v) = self
                .entries
                .pop_back()
                .expect("a table over its size has entries");
            self.size -= entry_size(&n, &v);
        }
    }

    fn set_max(&mut self, max: usize) {
        self.max = max;
        self.evict_to(max);
    }

    /// RFC 7541 §4.4: an entry larger than the table empties it and is not
    /// added.
    fn insert(&mut self, name: Vec<u8>, value: Vec<u8>) {
        let size = entry_size(&name, &value);
        if size > self.max {
            self.entries.clear();
            self.size = 0;
            return;
        }
        self.evict_to(self.max - size);
        self.size += size;
        self.entries.push_front((name, value));
    }

    /// Entry `index` of the combined address space (1-based).
    fn get(&self, index: u64) -> Option<(&[u8], &[u8])> {
        let index = usize::try_from(index).ok()?;
        match index {
            0 => None,
            1..=61 => Some(STATIC[index - 1]),
            _ => self
                .entries
                .get(index - 62)
                .map(|(n, v)| (n.as_slice(), v.as_slice())),
        }
    }

    /// The lowest index whose entry is `(name, value)`, and the lowest whose
    /// name is `name`.
    fn find(&self, name: &[u8], value: &[u8]) -> (Option<usize>, Option<usize>) {
        let mut name_only = None;
        let all = STATIC.iter().copied().chain(
            self.entries
                .iter()
                .map(|(n, v)| (n.as_slice(), v.as_slice())),
        );
        for (i, (n, v)) in all.enumerate() {
            if n == name {
                if v == value {
                    return (Some(i + 1), name_only.or(Some(i + 1)));
                }
                name_only.get_or_insert(i + 1);
            }
        }
        (None, name_only)
    }
}

/// An HPACK decoder: one per connection direction.
#[derive(Debug)]
pub struct Decoder {
    table: Table,
    /// The largest size an update may set: our `SETTINGS_HEADER_TABLE_SIZE`.
    limit: usize,
}

const fn malformed(what: &'static str) -> DecodeError {
    DecodeError::Malformed { what }
}

impl Decoder {
    /// A decoder whose table is bounded by `max_table_size`, the
    /// `SETTINGS_HEADER_TABLE_SIZE` this side advertised.
    #[must_use]
    pub fn new(max_table_size: u32) -> Self {
        Self {
            table: Table::new(max_table_size as usize),
            limit: max_table_size as usize,
        }
    }

    /// Follows a change of our `SETTINGS_HEADER_TABLE_SIZE`: later updates
    /// are bounded by it, and the table shrinks to it at once.
    pub fn set_max_table_size(&mut self, max: u32) {
        self.limit = max as usize;
        if self.table.max > self.limit {
            self.table.set_max(self.limit);
        }
    }

    /// Decodes one complete field block, appending `(name, value)` pairs to
    /// `out` in order.
    ///
    /// # Errors
    ///
    /// `TooLarge` `"field section"` when the fields, counted as name plus
    /// value plus 32 each (RFC 9113 §6.5.2), exceed
    /// [`Limits::max_head_bytes`], `"field count"` past
    /// [`Limits::max_fields`], `"string"` for one string longer than
    /// `max_head_bytes`; `Malformed` naming the rule: `"index"`, a table
    /// size update that is `"not at the start of a block"` or
    /// `"above the advertised table size"`, `"uppercase field name"`,
    /// `"truncated field block"`, `"integer"`, the Huffman refusals. A
    /// decoder that returned an error must not be used again: the dynamic
    /// table may hold half a block (RFC 7541 §2.3.3 makes this a connection
    /// error).
    pub fn decode(
        &mut self,
        block: &[u8],
        limits: Limits,
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), DecodeError> {
        let truncated = |e: DecodeError| match e {
            DecodeError::Incomplete { .. } => malformed("truncated field block"),
            e => e,
        };
        let cap = limits.max_head_bytes;
        let mut pos = 0;
        let mut at_start = true;
        let mut counted = 0usize;
        let mut count = 0usize;
        while pos < block.len() {
            let b = block[pos];
            let rest = &block[pos..];
            let (name, value, index) = if b & 0x80 != 0 {
                let (idx, used) = decode_int(rest, 7).map_err(truncated)?;
                pos += used;
                let (n, v) = self.table.get(idx).ok_or(malformed("index"))?;
                (n.to_vec(), v.to_vec(), false)
            } else if b & 0xe0 == 0x20 {
                let (size, used) = decode_int(rest, 5).map_err(truncated)?;
                pos += used;
                if !at_start {
                    return Err(malformed("table size update not at the start of a block"));
                }
                if size > self.limit as u64 {
                    return Err(malformed(
                        "table size update above the advertised table size",
                    ));
                }
                self.table.set_max(size as usize);
                continue;
            } else {
                // 01: incremental indexing, 6-bit name index; 0000 without
                // indexing and 0001 never indexed, 4-bit name index.
                let (prefix, index) = if b & 0x40 != 0 { (6, true) } else { (4, false) };
                let (idx, used) = decode_int(rest, prefix).map_err(truncated)?;
                pos += used;
                let name = if idx == 0 {
                    let (n, used) =
                        decode_string(&block[pos..], 0x80, 7, cap).map_err(truncated)?;
                    pos += used;
                    n
                } else {
                    self.table.get(idx).ok_or(malformed("index"))?.0.to_vec()
                };
                let (value, used) =
                    decode_string(&block[pos..], 0x80, 7, cap).map_err(truncated)?;
                pos += used;
                (name, value, index)
            };
            at_start = false;
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
            counted += entry_size(&name, &value);
            if counted > cap {
                return Err(DecodeError::TooLarge {
                    what: "field section",
                    declared: counted as u64,
                    cap: cap as u64,
                });
            }
            if index {
                self.table.insert(name.clone(), value.clone());
            }
            out.push((name, value));
        }
        Ok(())
    }
}

/// An HPACK encoder: one per connection direction.
#[derive(Debug)]
pub struct Encoder {
    table: Table,
    /// The smallest size the table passed through since the last block, and
    /// whether it changed: a block starts with the updates the decoder needs
    /// (RFC 7541 §4.2).
    pending: Option<usize>,
    huffman: bool,
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoder {
    /// An encoder whose table is [`DEFAULT_TABLE_SIZE`], the size a decoder
    /// starts with in HTTP/2.
    #[must_use]
    pub fn new() -> Self {
        Self::with_table_size(DEFAULT_TABLE_SIZE)
    }

    /// An encoder whose table starts at `size` on both sides, as when the
    /// size was agreed before the first block (RFC 7541 Appendix C.5). Sizes
    /// above [`DEFAULT_TABLE_SIZE`] are capped to it.
    #[must_use]
    pub fn with_table_size(size: u32) -> Self {
        Self {
            table: Table::new(size.min(DEFAULT_TABLE_SIZE) as usize),
            pending: None,
            huffman: true,
        }
    }

    /// Whether strings are Huffman-coded when that is shorter; on by
    /// default.
    pub fn set_huffman(&mut self, on: bool) {
        self.huffman = on;
    }

    /// Follows the peer's `SETTINGS_HEADER_TABLE_SIZE`. This encoder uses at
    /// most [`DEFAULT_TABLE_SIZE`]; a change in what it uses is announced at
    /// the start of the next block.
    pub fn set_peer_max_table_size(&mut self, size: u32) {
        let size = size.min(DEFAULT_TABLE_SIZE) as usize;
        if size == self.table.max {
            return;
        }
        let low = self
            .pending
            .map_or(size.min(self.table.max), |p| p.min(size));
        self.pending = Some(low);
        self.table.set_max(size);
    }

    /// Encodes one field block. Each field is `(name, value, sensitive)`;
    /// names must already be lowercase, which the caller checked.
    pub fn encode(&mut self, fields: &[(&[u8], &[u8], bool)], out: &mut Vec<u8>) {
        if let Some(low) = self.pending.take() {
            encode_int(low as u64, 5, 0x20, out);
            if low != self.table.max {
                encode_int(self.table.max as u64, 5, 0x20, out);
            }
        }
        for &(name, value, sensitive) in fields {
            let sensitive = sensitive || is_sensitive(name);
            let (exact, by_name) = self.table.find(name, value);
            if let Some(i) = exact {
                encode_int(i as u64, 7, 0x80, out);
                continue;
            }
            let (prefix, first) = if sensitive { (4, 0x10) } else { (6, 0x40) };
            match by_name {
                Some(i) => encode_int(i as u64, prefix, first, out),
                None => {
                    out.push(first);
                    encode_string(name, 0, 0x80, 7, self.huffman, out);
                }
            }
            encode_string(value, 0, 0x80, 7, self.huffman, out);
            if !sensitive {
                self.table.insert(name.to_vec(), value.to_vec());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Limits = Limits::DEFAULT;

    fn round_trip(
        enc: &mut Encoder,
        dec: &mut Decoder,
        fields: &[(&[u8], &[u8], bool)],
    ) -> Vec<u8> {
        let mut block = Vec::new();
        enc.encode(fields, &mut block);
        let mut out = Vec::new();
        dec.decode(&block, L, &mut out).unwrap();
        let expect: Vec<_> = fields
            .iter()
            .map(|(n, v, _)| (n.to_vec(), v.to_vec()))
            .collect();
        assert_eq!(out, expect);
        block
    }

    #[test]
    fn a_sensitive_field_is_never_indexed_and_never_enters_the_table() {
        let (mut e, mut d) = (Encoder::new(), Decoder::new(4096));
        let block = round_trip(&mut e, &mut d, &[(b"authorization", b"Bearer x", false)]);
        // 0001 with name index 23 (authorization) does not fit 4 bits.
        assert_eq!(block[0], 0x1f);
        let block = round_trip(&mut e, &mut d, &[(b"x-token", b"s", true)]);
        assert_eq!(block[0], 0x10);
        // Sent again, still literal: nothing was indexed.
        let again = round_trip(&mut e, &mut d, &[(b"x-token", b"s", true)]);
        assert_eq!(again, block);
    }

    #[test]
    fn a_repeated_field_becomes_one_index_octet() {
        let (mut e, mut d) = (Encoder::new(), Decoder::new(4096));
        round_trip(&mut e, &mut d, &[(b"x-a", b"1", false)]);
        let block = round_trip(&mut e, &mut d, &[(b"x-a", b"1", false)]);
        assert_eq!(block, [0x80 | 62]);
    }

    #[test]
    fn a_shrunk_peer_table_is_announced_once() {
        let (mut e, mut d) = (Encoder::new(), Decoder::new(4096));
        round_trip(&mut e, &mut d, &[(b"x-a", b"1", false)]);
        e.set_peer_max_table_size(0);
        e.set_peer_max_table_size(100);
        d.set_max_table_size(100);
        let block = round_trip(&mut e, &mut d, &[(b"x-a", b"1", false)]);
        // Two updates: to the minimum (0) and then to the final size (100).
        assert_eq!(&block[..3], [0x20, 0x3f, 100 - 31]);
        let block = round_trip(&mut e, &mut d, &[(b"x-a", b"1", false)]);
        assert_eq!(block, [0x80 | 62]);
    }

    #[test]
    fn the_decoder_refuses_what_it_did_not_advertise() {
        let mut d = Decoder::new(100);
        let mut out = Vec::new();
        assert_eq!(
            d.decode(&[0x3f, 0xe1, 0x1f], L, &mut out),
            Err(malformed(
                "table size update above the advertised table size"
            ))
        );
        let mut d = Decoder::new(4096);
        assert_eq!(
            d.decode(&[0x82, 0x20], L, &mut out),
            Err(malformed("table size update not at the start of a block"))
        );
        assert_eq!(d.decode(&[0x80], L, &mut out), Err(malformed("index")));
        assert_eq!(d.decode(&[0xbe], L, &mut out), Err(malformed("index")));
        assert_eq!(
            d.decode(&[0x40, 0x01, b'A', 0x00], L, &mut out),
            Err(malformed("uppercase field name"))
        );
        assert_eq!(
            d.decode(&[0x40, 0x05, b'a'], L, &mut out),
            Err(malformed("truncated field block"))
        );
    }

    #[test]
    fn the_field_section_is_counted_as_rfc9113_counts_it() {
        let mut block = Vec::new();
        Encoder::new().encode(&[(b"a", b"b", false), (b"c", b"d", false)], &mut block);
        let mut out = Vec::new();
        let exact = Limits {
            max_head_bytes: 68,
            ..L
        };
        Decoder::new(4096).decode(&block, exact, &mut out).unwrap();
        let short = Limits {
            max_head_bytes: 67,
            ..L
        };
        assert!(matches!(
            Decoder::new(4096).decode(&block, short, &mut Vec::new()),
            Err(DecodeError::TooLarge {
                what: "field section",
                declared: 68,
                cap: 67
            })
        ));
        let one = Limits { max_fields: 1, ..L };
        assert!(matches!(
            Decoder::new(4096).decode(&block, one, &mut Vec::new()),
            Err(DecodeError::TooLarge {
                what: "field count",
                ..
            })
        ));
    }

    #[test]
    fn an_oversized_entry_empties_the_table() {
        let mut t = Table::new(64);
        t.insert(b"a".to_vec(), b"b".to_vec());
        assert_eq!(t.size, 34);
        t.insert(b"a".to_vec(), vec![b'x'; 40]);
        assert_eq!((t.size, t.entries.len()), (0, 0));
    }
}
