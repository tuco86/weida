//! The `NATS/1.0` header block carried by `HPUB` and `HMSG`.
//!
//! The reference describes it in one sentence — "Header version `NATS/1.0␍␊`
//! followed by one or more `name: value` pairs, each separated by `␍␊`" — and
//! then adds the two rules that decide the data structure:
//!
//! > NATS headers are similar, in structure and semantics, to HTTP headers as
//! > `name: value` pairs **including supporting multi-value headers**. Headers
//! > can be mixed case and **NATS will preserve case between message
//! > publisher and message receiver(s)**.
//!
//! So a name may repeat with different values and every value survives in
//! order, and nothing anywhere may lowercase a name. Both rules are properties
//! of *storage*, which is why the entries are a vector of pairs rather than a
//! map: a map would have to choose a key type, and every choice — case-folded,
//! case-sensitive, first-wins, last-wins — loses one of the two rules. Lookup
//! is a separate concern and is offered separately, in both a case-sensitive
//! ([`Headers::get`]) and a case-insensitive ([`Headers::get_ignore_ascii_case`])
//! form, neither of which changes what is stored.
//!
//! The version line may carry a status and a description after it —
//! `NATS/1.0 503` or `NATS/1.0 100 Idle Heartbeat`. That is how the
//! no-responder answer of request-reply arrives (`docs/research/nats.md` §4:
//! "a request with no responders can receive a fast no-responder status when
//! the client enables `no_responders` and headers"), and how a push consumer's
//! idle heartbeat does. Both are decoded as optional fields rather than left
//! in the version line, because a client has to branch on the code.
//!
//! Sources: the client protocol reference, `HPUB`; ADR-4, *NATS Message
//! Headers* (`docs/research/nats.md` §3, the sheet's sources 4 and 12).

use crate::error::{DecodeError, EncodeError};
use crate::limits::Limits;

/// The one header version this protocol has.
pub const VERSION: &str = "NATS/1.0";

/// A decoded `NATS/1.0` header block.
///
/// Borrows every name and value from the block it was decoded from; nothing
/// here is copied, and nothing here is case-folded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Headers<'a> {
    /// The three-digit status after `NATS/1.0`, where the version line
    /// carried one: `503` for no responders, `100` for a flow-control or
    /// idle-heartbeat control message.
    pub status: Option<u16>,
    /// The human-readable text after the status, where there was one —
    /// `Idle Heartbeat` in `NATS/1.0 100 Idle Heartbeat`. Never present
    /// without a [`status`](Self::status): the grammar puts it after the
    /// code.
    pub description: Option<&'a str>,
    /// In arrival order, duplicates kept, case as the publisher wrote it.
    entries: Vec<(&'a str, &'a str)>,
}

impl<'a> Headers<'a> {
    /// An empty block: version line, no status, no entries.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            status: None,
            description: None,
            entries: Vec::new(),
        }
    }

    /// Append an entry, keeping the case and the position it is given.
    ///
    /// A repeat of a name already present is not an error and does not
    /// replace anything: multi-value headers are the reference's own term for
    /// it. Names and values are checked when the block is encoded, not here,
    /// so that a caller building a block does not have to handle an error per
    /// entry.
    pub fn push(&mut self, name: &'a str, value: &'a str) {
        self.entries.push((name, value));
    }

    /// Every entry, in arrival order.
    #[must_use]
    pub fn entries(&self) -> &[(&'a str, &'a str)] {
        &self.entries
    }

    /// How many entries the block holds, duplicates counted separately.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the block holds no entries. A block with a status and no
    /// entries is the ordinary shape of a no-responder message.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The first value stored under exactly this name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&'a str> {
        self.entries
            .iter()
            .find(|(stored, _)| *stored == name)
            .map(|(_, value)| *value)
    }

    /// The first value whose name matches ignoring ASCII case.
    ///
    /// Separate from [`get`](Self::get) on purpose: the protocol promises
    /// only that case is *preserved*, not that it is significant, so a client
    /// looking for `Nats-Msg-Id` in a block written by another language's
    /// client may well have to ignore it. What must never happen is the
    /// storage doing the folding.
    #[must_use]
    pub fn get_ignore_ascii_case(&self, name: &str) -> Option<&'a str> {
        self.entries
            .iter()
            .find(|(stored, _)| stored.eq_ignore_ascii_case(name))
            .map(|(_, value)| *value)
    }

    /// Every value stored under exactly this name, in order.
    pub fn get_all<'s>(&'s self, name: &'s str) -> impl Iterator<Item = &'a str> + 's {
        self.entries
            .iter()
            .filter(move |(stored, _)| *stored == name)
            .map(|(_, value)| *value)
    }

    /// Decode one complete header block.
    ///
    /// `block` is exactly the octets the control line's `#header bytes`
    /// declared: version line, entries, and the blank line that ends it. The
    /// count is the delimiter here — the block is never searched for its own
    /// end — so a block that stops short of its blank line is
    /// [`DecodeError::HeaderBlockNotTerminated`] and octets after it are
    /// [`DecodeError::TrailingHeaderBytes`], rather than either one silently
    /// eating part of the payload.
    pub fn decode(block: &'a [u8], limits: Limits) -> Result<Self, DecodeError> {
        let rest = block
            .strip_prefix(VERSION.as_bytes())
            .ok_or(DecodeError::HeaderVersionMissing)?;
        let line_end = find_crlf(rest).ok_or(DecodeError::HeaderBlockNotTerminated)?;
        let (status, description) = decode_version_tail(&rest[..line_end])?;

        // The version line and the blank line that ends the block are not
        // entries, so `newlines - 2` bounds their count. Reserving that once,
        // capped by the entry limit, avoids growing the vector by doubling.
        let newlines = block.iter().filter(|byte| **byte == b'\n').count();
        let mut headers = Self {
            status,
            description,
            entries: Vec::with_capacity(
                newlines
                    .saturating_sub(2)
                    .min(limits.max_header_entries as usize),
            ),
        };
        let mut at = VERSION.len() + line_end + 2;
        loop {
            let line_end = find_crlf(&block[at..]).ok_or(DecodeError::HeaderBlockNotTerminated)?;
            let line = &block[at..at + line_end];
            at += line_end + 2;
            if line.is_empty() {
                if at != block.len() {
                    return Err(DecodeError::TrailingHeaderBytes {
                        extra: block.len() - at,
                    });
                }
                return Ok(headers);
            }
            if headers.entries.len() as u64 >= u64::from(limits.max_header_entries) {
                return Err(DecodeError::TooManyHeaderEntries {
                    cap: limits.max_header_entries,
                });
            }
            let colon = line
                .iter()
                .position(|byte| *byte == b':')
                .ok_or(DecodeError::MalformedHeaderLine)?;
            let name = decode_name(&line[..colon])?;
            let value = decode_value(&line[colon + 1..])?;
            headers.entries.push((name, value));
        }
    }

    /// Append the encoded block, blank line included.
    ///
    /// Canonical: exactly one space after each colon, no space around the
    /// value, the status written as three digits. Values that could not
    /// survive the round trip are refused rather than mangled — see
    /// [`EncodeError::HeaderValueInvalid`].
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        self.check()?;
        self.encode_checked(out);
        Ok(())
    }

    /// Append the block without checking it first.
    ///
    /// Only for callers that have already run [`check`](Self::check) on this
    /// block. The operation encoders check each block once, before they write
    /// the control line that declares its size.
    pub(crate) fn encode_checked(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(VERSION.as_bytes());
        if let Some(status) = self.status {
            out.push(b' ');
            out.push(b'0' + (status / 100) as u8);
            out.push(b'0' + (status / 10 % 10) as u8);
            out.push(b'0' + (status % 10) as u8);
            if let Some(description) = self.description {
                out.push(b' ');
                out.extend_from_slice(description.as_bytes());
            }
        }
        out.extend_from_slice(b"\r\n");
        for (name, value) in &self.entries {
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(b"\r\n");
    }

    /// How many octets [`encode`](Self::encode) will append.
    ///
    /// `HPUB` and `HMSG` declare the header size *before* the block, so the
    /// length has to be knowable without writing the block into a scratch
    /// buffer first. That the two agree is not left to inspection: every
    /// round trip in the tests decodes the control line this length produced,
    /// and a header block one octet longer or shorter than its declared count
    /// fails to decode.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        let mut len = VERSION.len() + 2;
        if self.status.is_some() {
            len += 4;
            if let Some(description) = self.description {
                len += 1 + description.len();
            }
        }
        for (name, value) in &self.entries {
            len += name.len() + 2 + value.len() + 2;
        }
        len + 2
    }

    /// Everything [`encode`](Self::encode) refuses, in one place, so that
    /// [`encoded_len`](Self::encoded_len) is only ever asked about a block
    /// that can be written.
    pub(crate) fn check(&self) -> Result<(), EncodeError> {
        if let Some(status) = self.status {
            if status > 999 {
                return Err(EncodeError::HeaderStatusOutOfRange { status });
            }
        } else if self.description.is_some() {
            return Err(EncodeError::HeaderDescriptionWithoutStatus);
        }
        if let Some(description) = self.description
            && !is_writable_value(description)
        {
            return Err(EncodeError::HeaderValueInvalid);
        }
        for (name, value) in &self.entries {
            if !is_valid_name(name) {
                return Err(EncodeError::HeaderNameInvalid);
            }
            if !is_writable_value(value) {
                return Err(EncodeError::HeaderValueInvalid);
            }
        }
        Ok(())
    }
}

/// What follows `NATS/1.0` on the version line.
///
/// Empty, or a space and then a three-digit code with an optional
/// description. Anything else is [`DecodeError::InvalidHeaderStatus`]: a
/// version line this crate cannot read is not a line to guess at, since the
/// code is what tells a requester its request had no responder.
fn decode_version_tail(tail: &[u8]) -> Result<(Option<u16>, Option<&str>), DecodeError> {
    if tail.is_empty() {
        return Ok((None, None));
    }
    if tail[0] != b' ' {
        return Err(DecodeError::HeaderVersionMissing);
    }
    let tail = trim(&tail[1..]);
    let code_end = tail
        .iter()
        .position(|byte| *byte == b' ')
        .unwrap_or(tail.len());
    let code = &tail[..code_end];
    if code.len() != 3 || !code.iter().all(u8::is_ascii_digit) {
        return Err(DecodeError::InvalidHeaderStatus);
    }
    let status = u16::from(code[0] - b'0') * 100
        + u16::from(code[1] - b'0') * 10
        + u16::from(code[2] - b'0');
    let description = trim(&tail[code_end..]);
    if description.is_empty() {
        return Ok((Some(status), None));
    }
    Ok((Some(status), Some(decode_value(description)?)))
}

/// A header name: ASCII graphic, non-empty.
///
/// The colon is excluded by construction — the line was split at the first
/// one — and everything below `0x21` or above `0x7e` is excluded because it
/// is either a control, the separating space, or a byte HTTP-like syntax has
/// no meaning for.
fn decode_name(raw: &[u8]) -> Result<&str, DecodeError> {
    if raw.is_empty() || !raw.iter().all(|byte| (0x21..=0x7e).contains(byte)) {
        return Err(DecodeError::InvalidHeaderName);
    }
    core::str::from_utf8(raw).map_err(|_| DecodeError::InvalidHeaderName)
}

/// A header value: UTF-8, no C0 control except the tab, surrounding spaces
/// and tabs removed.
///
/// The trimming is what makes `Bar: Baz` and `Bar:Baz` the same value, which
/// they have to be: the reference writes the space, ADR-4's examples write
/// the space, and nothing in either says a receiver may keep it.
fn decode_value(raw: &[u8]) -> Result<&str, DecodeError> {
    let raw = trim(raw);
    if raw.iter().any(|byte| *byte < 0x20 && *byte != b'\t') {
        return Err(DecodeError::InvalidHeaderValue);
    }
    core::str::from_utf8(raw).map_err(|_| DecodeError::InvalidHeaderValue)
}

fn trim(mut raw: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = raw {
        if *first == b' ' || *first == b'\t' {
            raw = rest;
        } else {
            break;
        }
    }
    while let [rest @ .., last] = raw {
        if *last == b' ' || *last == b'\t' {
            raw = rest;
        } else {
            break;
        }
    }
    raw
}

fn find_crlf(input: &[u8]) -> Option<usize> {
    input.windows(2).position(|pair| pair == b"\r\n")
}

fn is_valid_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

/// Whether a value can be written *and read back unchanged*.
///
/// Stricter than what the decoder accepts, and deliberately so: the decoder
/// trims surrounding whitespace, so a value that carries any would come back
/// different from what was written. A codec whose encoder can produce
/// something its own decoder reads as another value is a codec with a bug in
/// it, so the encoder refuses instead.
fn is_writable_value(value: &str) -> bool {
    if value.bytes().any(|byte| byte < 0x20 && byte != b'\t') {
        return false;
    }
    let edges = [value.as_bytes().first(), value.as_bytes().last()];
    !edges
        .iter()
        .any(|byte| matches!(byte, Some(b' ') | Some(b'\t')))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: Limits = Limits::DEFAULT;

    #[test]
    fn a_name_may_repeat_and_every_value_survives_in_order() {
        // The reference's own example: "To publish a message to subject
        // MORNING MENU with one header BREAKFAST having two values".
        let block = b"NATS/1.0\r\nBREAKFAST: donut\r\nBREAKFAST: eggs\r\n\r\n";
        let headers = Headers::decode(block, LIMITS).expect("decodes");
        assert_eq!(headers.len(), 2);
        assert_eq!(
            headers.get_all("BREAKFAST").collect::<Vec<_>>(),
            ["donut", "eggs"]
        );
        assert_eq!(headers.get("BREAKFAST"), Some("donut"));
        let mut written = Vec::new();
        headers.encode(&mut written).expect("encodes");
        assert_eq!(written, block);
    }

    #[test]
    fn case_is_preserved_and_lookup_is_a_separate_concern() {
        let block = b"NATS/1.0\r\nNats-Msg-Id: 7\r\nfoo-BAR: baz\r\n\r\n";
        let headers = Headers::decode(block, LIMITS).expect("decodes");
        assert_eq!(
            headers.entries(),
            [("Nats-Msg-Id", "7"), ("foo-BAR", "baz")]
        );
        // Stored case is the publisher's, exactly.
        assert_eq!(headers.get("nats-msg-id"), None);
        assert_eq!(headers.get_ignore_ascii_case("nats-msg-id"), Some("7"));
        assert_eq!(headers.get_ignore_ascii_case("FOO-bar"), Some("baz"));
    }

    #[test]
    fn a_status_line_carries_the_no_responder_answer() {
        let no_responders = Headers::decode(b"NATS/1.0 503\r\n\r\n", LIMITS).expect("decodes");
        assert_eq!(no_responders.status, Some(503));
        assert_eq!(no_responders.description, None);
        assert!(no_responders.is_empty(), "the reference says one or more");

        let heartbeat =
            Headers::decode(b"NATS/1.0 100 Idle Heartbeat\r\n\r\n", LIMITS).expect("decodes");
        assert_eq!(heartbeat.status, Some(100));
        assert_eq!(heartbeat.description, Some("Idle Heartbeat"));

        for block in [&no_responders, &heartbeat] {
            let mut written = Vec::new();
            block.encode(&mut written).expect("encodes");
            assert_eq!(&Headers::decode(&written, LIMITS).expect("decodes"), block);
            assert_eq!(written.len(), block.encoded_len());
        }
    }

    #[test]
    fn a_version_line_tail_that_is_not_a_code_is_refused() {
        for block in [
            &b"NATS/1.0 XX\r\n\r\n"[..],
            &b"NATS/1.0 50\r\n\r\n"[..],
            &b"NATS/1.0 5031\r\n\r\n"[..],
        ] {
            assert_eq!(
                Headers::decode(block, LIMITS),
                Err(DecodeError::InvalidHeaderStatus),
                "{:?}",
                core::str::from_utf8(block)
            );
        }
        assert_eq!(
            Headers::decode(b"NATS/1.1\r\n\r\n", LIMITS),
            Err(DecodeError::HeaderVersionMissing)
        );
    }

    #[test]
    fn a_value_is_the_same_with_or_without_the_space() {
        let spaced = Headers::decode(b"NATS/1.0\r\nBar: Baz\r\n\r\n", LIMITS).expect("decodes");
        let tight = Headers::decode(b"NATS/1.0\r\nBar:Baz\r\n\r\n", LIMITS).expect("decodes");
        assert_eq!(spaced, tight);
        assert_eq!(spaced.get("Bar"), Some("Baz"));
    }

    #[test]
    fn a_block_is_delimited_by_its_count_and_not_by_a_search() {
        // One octet short of the blank line.
        assert_eq!(
            Headers::decode(b"NATS/1.0\r\nBar: Baz\r\n", LIMITS),
            Err(DecodeError::HeaderBlockNotTerminated)
        );
        // Payload octets that the declared header count swallowed.
        assert_eq!(
            Headers::decode(b"NATS/1.0\r\n\r\nHello", LIMITS),
            Err(DecodeError::TrailingHeaderBytes { extra: 5 })
        );
    }

    #[test]
    fn the_entry_count_is_bounded_because_the_block_declares_none() {
        let limits = Limits {
            max_header_entries: 2,
            ..Limits::DEFAULT
        };
        let block = b"NATS/1.0\r\na: 1\r\nb: 2\r\nc: 3\r\n\r\n";
        assert_eq!(
            Headers::decode(block, limits),
            Err(DecodeError::TooManyHeaderEntries { cap: 2 })
        );
    }

    #[test]
    fn the_encoder_refuses_what_its_own_decoder_would_change() {
        let mut headers = Headers::new();
        headers.push("Bar", " padded ");
        assert_eq!(
            headers.encode(&mut Vec::new()),
            Err(EncodeError::HeaderValueInvalid)
        );

        let mut broken = Headers::new();
        broken.push("Ba r", "baz");
        assert_eq!(
            broken.encode(&mut Vec::new()),
            Err(EncodeError::HeaderNameInvalid)
        );

        let mut newline = Headers::new();
        newline.push("Bar", "a\r\nInjected: yes");
        assert_eq!(
            newline.encode(&mut Vec::new()),
            Err(EncodeError::HeaderValueInvalid)
        );

        let described = Headers {
            status: None,
            description: Some("Idle Heartbeat"),
            ..Headers::new()
        };
        assert_eq!(
            described.encode(&mut Vec::new()),
            Err(EncodeError::HeaderDescriptionWithoutStatus)
        );
    }
}
