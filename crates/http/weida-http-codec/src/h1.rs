//! HTTP/1.1 message syntax (RFC 9112): heads, framing, chunked bodies.
//!
//! A head is decoded whole or not at all: [`decode_request`] and
//! [`decode_response`] first find the empty line that ends it, within
//! [`Limits::max_head_bytes`], and only then parse, so every proper prefix of
//! a valid head is [`DecodeError::Incomplete`] and a head that never ends is
//! [`DecodeError::TooLarge`] once the cap's worth of octets has been searched.
//!
//! The body's framing is decided from the head by [`request_framing`] and
//! [`response_framing`] (RFC 9112 §6.3); a chunked body is decoded step by
//! step by [`ChunkedDecoder`], which hands out slices of the caller's buffer.

use crate::error::{DecodeError, EncodeError};
use crate::fields::{Field, is_field_value, is_token, trim_ows};
use crate::limits::Limits;

/// A decoded request head, borrowing from the input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestHead<'a> {
    /// The method, a token, case as written (methods are case-sensitive).
    pub method: &'a [u8],
    /// The request target as written: origin, absolute, authority or
    /// asterisk form (RFC 9112 §3.2).
    pub target: &'a [u8],
    /// The minor version: 1 for `HTTP/1.1`, 0 for `HTTP/1.0`.
    pub minor: u8,
    /// The field lines in order.
    pub fields: Vec<Field<'a>>,
}

/// A decoded response head, borrowing from the input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseHead<'a> {
    /// The minor version: 1 for `HTTP/1.1`, 0 for `HTTP/1.0`.
    pub minor: u8,
    /// The status code, `100..=999`.
    pub status: u16,
    /// The reason phrase, possibly empty; a client ignores it (RFC 9112 §4).
    pub reason: &'a [u8],
    /// The field lines in order.
    pub fields: Vec<Field<'a>>,
}

/// How a message body is delimited (RFC 9112 §6.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    /// No body.
    Empty,
    /// Exactly this many octets.
    Length(u64),
    /// `Transfer-Encoding: chunked`.
    Chunked,
    /// Until the connection closes (responses only).
    UntilClose,
}

const fn malformed(what: &'static str) -> DecodeError {
    DecodeError::Malformed { what }
}

/// Finds the empty line that ends a field section starting at `start`,
/// searching no further than `cap` octets from `start`. Returns the index just
/// past it.
fn find_section_end(
    input: &[u8],
    start: usize,
    cap: usize,
    what: &'static str,
) -> Result<usize, DecodeError> {
    let limit = start.saturating_add(cap).min(input.len());
    let window = &input[..limit];
    // The section's first line may itself be empty: `start` is the start of a
    // line, so treat it as following a line feed.
    let mut line_start = start;
    loop {
        match &window[line_start..] {
            [b'\n', ..] => return Ok(line_start + 1),
            [b'\r', b'\n', ..] => return Ok(line_start + 2),
            _ => {}
        }
        match window[line_start..].iter().position(|&b| b == b'\n') {
            Some(lf) => line_start += lf + 1,
            None => break,
        }
    }
    if input.len() - start >= cap {
        Err(DecodeError::TooLarge {
            what,
            declared: (input.len() - start) as u64,
            cap: cap as u64,
        })
    } else {
        Err(DecodeError::Incomplete { needed: 1 })
    }
}

/// The lines of `section` without their line endings; a bare LF ends a line
/// as CRLF does (RFC 9112 §2.2). The final empty line is not yielded.
fn lines(section: &[u8]) -> impl Iterator<Item = &[u8]> {
    section
        .split(|&b| b == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .take_while(|line| !line.is_empty())
}

/// Parses field lines (RFC 9112 §5) with the rules every section shares.
fn parse_fields<'a>(
    lines: impl Iterator<Item = &'a [u8]>,
    limits: Limits,
) -> Result<Vec<Field<'a>>, DecodeError> {
    let mut fields = Vec::new();
    for line in lines {
        if matches!(line.first(), Some(b' ' | b'\t')) {
            return Err(malformed("obsolete line folding"));
        }
        let colon = line
            .iter()
            .position(|&b| b == b':')
            .ok_or(malformed("field line without a colon"))?;
        let name = &line[..colon];
        if matches!(name.last(), Some(b' ' | b'\t')) {
            return Err(malformed("whitespace before the colon"));
        }
        if !is_token(name) {
            return Err(malformed("field name"));
        }
        let value = trim_ows(&line[colon + 1..]);
        if !is_field_value(value) {
            return Err(malformed("field value"));
        }
        if fields.len() == limits.max_fields {
            return Err(DecodeError::TooLarge {
                what: "field count",
                declared: fields.len() as u64 + 1,
                cap: limits.max_fields as u64,
            });
        }
        fields.push(Field { name, value });
    }
    Ok(fields)
}

fn parse_version(v: &[u8]) -> Result<u8, DecodeError> {
    match v {
        b"HTTP/1.1" => Ok(1),
        b"HTTP/1.0" => Ok(0),
        _ => Err(malformed("version")),
    }
}

fn is_visible(b: &[u8]) -> bool {
    b.iter().all(|c| (0x21..=0x7e).contains(c))
}

fn is_reason(b: &[u8]) -> bool {
    b.iter()
        .all(|&c| c == b' ' || c == b'\t' || (0x21..=0x7e).contains(&c) || c >= 0x80)
}

/// Decodes a request head. Returns it and the octets it used, the empty line
/// included; the body, if any, starts there.
///
/// Empty lines before the request line are skipped (RFC 9112 §2.2).
///
/// # Errors
///
/// [`DecodeError::Incomplete`] before the empty line; `TooLarge` `"head"`
/// when none is found within [`Limits::max_head_bytes`] (leading empty lines
/// count), `TooLarge` `"field count"` past [`Limits::max_fields`];
/// `Malformed` naming the rule otherwise (`"version"` for a version other
/// than HTTP/1.1 and HTTP/1.0).
pub fn decode_request(
    input: &[u8],
    limits: Limits,
) -> Result<(RequestHead<'_>, usize), DecodeError> {
    let mut start = 0;
    loop {
        if start >= limits.max_head_bytes {
            return Err(DecodeError::TooLarge {
                what: "head",
                declared: input.len() as u64,
                cap: limits.max_head_bytes as u64,
            });
        }
        match &input[start..] {
            [b'\r', b'\n', ..] => start += 2,
            [b'\n', ..] => start += 1,
            [] | [b'\r'] => return Err(DecodeError::Incomplete { needed: 1 }),
            _ => break,
        }
    }
    let cap = limits.max_head_bytes.saturating_sub(start);
    let end = find_section_end(input, start, cap, "head")?;
    let mut it = lines(&input[start..end]);
    let line = it.next().ok_or(malformed("request line"))?;
    let mut parts = line.split(|&b| b == b' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(malformed("request line"));
    };
    if !is_token(method) {
        return Err(malformed("method"));
    }
    if target.is_empty() || !is_visible(target) {
        return Err(malformed("request target"));
    }
    let minor = parse_version(version)?;
    let fields = parse_fields(it, limits)?;
    Ok((
        RequestHead {
            method,
            target,
            minor,
            fields,
        },
        end,
    ))
}

/// Decodes a response head. Returns it and the octets it used.
///
/// # Errors
///
/// As [`decode_request`], with `"status line"`, `"status"` and
/// `"reason phrase"` for the first line.
pub fn decode_response(
    input: &[u8],
    limits: Limits,
) -> Result<(ResponseHead<'_>, usize), DecodeError> {
    let end = find_section_end(input, 0, limits.max_head_bytes, "head")?;
    let mut it = lines(&input[..end]);
    let line = it.next().ok_or(malformed("status line"))?;
    let sp = line
        .iter()
        .position(|&b| b == b' ')
        .ok_or(malformed("status line"))?;
    let minor = parse_version(&line[..sp])?;
    let rest = &line[sp + 1..];
    let (code, reason) = match rest {
        [a, b, c] => ([*a, *b, *c], &rest[3..]),
        [a, b, c, b' ', reason @ ..] => ([*a, *b, *c], reason),
        _ => return Err(malformed("status")),
    };
    if !(b'1'..=b'9').contains(&code[0]) || !code.iter().all(u8::is_ascii_digit) {
        return Err(malformed("status"));
    }
    let status = code
        .iter()
        .fold(0u16, |acc, d| acc * 10 + u16::from(d - b'0'));
    if !is_reason(reason) {
        return Err(malformed("reason phrase"));
    }
    let fields = parse_fields(it, limits)?;
    Ok((
        ResponseHead {
            minor,
            status,
            reason,
            fields,
        },
        end,
    ))
}

fn values<'a, 'f>(
    fields: &'f [Field<'a>],
    name: &'static str,
) -> impl Iterator<Item = &'a [u8]> + 'f {
    fields
        .iter()
        .filter(move |f| f.name.eq_ignore_ascii_case(name.as_bytes()))
        .flat_map(|f| f.value.split(|&b| b == b','))
        .map(trim_ows)
        .filter(|v| !v.is_empty())
}

fn has(fields: &[Field<'_>], name: &'static str) -> bool {
    fields
        .iter()
        .any(|f| f.name.eq_ignore_ascii_case(name.as_bytes()))
}

/// The value of `Content-Length`, if present: a list of equal values is one
/// value (RFC 9110 §8.6).
///
/// # Errors
///
/// `Malformed` `"Content-Length"` for a value that is not digits, overflows
/// `u64`, or differs from another.
pub fn content_length(fields: &[Field<'_>]) -> Result<Option<u64>, DecodeError> {
    let mut found = None;
    let mut present = false;
    for v in values(fields, "content-length") {
        present = true;
        if !v.iter().all(u8::is_ascii_digit) {
            return Err(malformed("Content-Length"));
        }
        let n = v.iter().try_fold(0u64, |acc, d| {
            acc.checked_mul(10)?.checked_add(u64::from(d - b'0'))
        });
        let n = n.ok_or(malformed("Content-Length"))?;
        match found {
            None => found = Some(n),
            Some(m) if m == n => {}
            Some(_) => return Err(malformed("Content-Length")),
        }
    }
    if present || !has(fields, "content-length") {
        Ok(found)
    } else {
        Err(malformed("Content-Length"))
    }
}

/// Whether `Transfer-Encoding` is present; `Ok(true)` only for exactly
/// `chunked`.
fn chunked(fields: &[Field<'_>]) -> Result<bool, DecodeError> {
    if !has(fields, "transfer-encoding") {
        return Ok(false);
    }
    let mut codings = values(fields, "transfer-encoding");
    match (codings.next(), codings.next()) {
        (Some(c), None) if c.eq_ignore_ascii_case(b"chunked") => Ok(true),
        _ => Err(malformed("transfer coding")),
    }
}

/// Whether the `Connection` field lists `option`, ignoring ASCII case
/// (RFC 9110 §7.6.1).
#[must_use]
pub fn connection_has(fields: &[Field<'_>], option: &str) -> bool {
    values(fields, "connection").any(|v| v.eq_ignore_ascii_case(option.as_bytes()))
}

/// The framing of a request body (RFC 9112 §6.3 rules 3 to 7).
///
/// # Errors
///
/// `Malformed`: `"both Transfer-Encoding and Content-Length"` (a smuggling
/// vector a server must not guess at, RFC 9112 §6.1); `"transfer coding"` for
/// any coding list other than exactly `chunked`; `"Content-Length"`.
pub fn request_framing(fields: &[Field<'_>]) -> Result<Framing, DecodeError> {
    if has(fields, "transfer-encoding") && has(fields, "content-length") {
        return Err(malformed("both Transfer-Encoding and Content-Length"));
    }
    if chunked(fields)? {
        return Ok(Framing::Chunked);
    }
    Ok(match content_length(fields)? {
        Some(n) => Framing::Length(n),
        None => Framing::Empty,
    })
}

/// The framing of a response body (RFC 9112 §6.3 rules 1 to 7), given the
/// method of the request it answers.
///
/// # Errors
///
/// `Malformed` `"transfer coding"` or `"Content-Length"`.
pub fn response_framing(
    request_method: &[u8],
    status: u16,
    fields: &[Field<'_>],
) -> Result<Framing, DecodeError> {
    if request_method == b"HEAD" || (100..200).contains(&status) || status == 204 || status == 304 {
        return Ok(Framing::Empty);
    }
    if chunked(fields)? {
        return Ok(Framing::Chunked);
    }
    Ok(match content_length(fields)? {
        Some(n) => Framing::Length(n),
        None => Framing::UntilClose,
    })
}

/// One step of a chunked body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Chunk<'a> {
    /// Body octets, never empty.
    Data(&'a [u8]),
    /// The last chunk and its trailer fields; the body has ended.
    End(Vec<Field<'a>>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Size,
    Data(u64),
    DataEnd,
    Trailers,
    Done,
}

/// The state of a chunked body decode (RFC 9112 §7.1).
///
/// The decoder owns no buffer: [`ChunkedDecoder::step`] reads the caller's
/// input and returns slices of it. On an error the state is unchanged, so a
/// caller that got [`DecodeError::Incomplete`] calls again with more input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkedDecoder {
    state: State,
}

impl Default for ChunkedDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedDecoder {
    /// A decoder at the start of a body.
    #[must_use]
    pub const fn new() -> Self {
        Self { state: State::Size }
    }

    /// Whether the last chunk and the trailers have been decoded.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// Decodes the next piece of the body from `input`. Returns it and the
    /// octets it used.
    ///
    /// [`Chunk::Data`] is at most the rest of the current chunk that `input`
    /// holds. Chunk extensions are stepped over but count toward
    /// [`Limits::max_chunk_line`]; trailers follow the head's field rules and
    /// [`Limits::max_head_bytes`].
    ///
    /// # Errors
    ///
    /// `Incomplete`; `TooLarge` `"chunk line"` or `"trailer section"`;
    /// `Malformed` `"chunk size"`, `"chunk data terminator"`, or a field rule;
    /// `Malformed` `"chunked body already ended"` after [`Chunk::End`].
    pub fn step<'a>(
        &mut self,
        input: &'a [u8],
        limits: Limits,
    ) -> Result<(Chunk<'a>, usize), DecodeError> {
        let mut pos = 0;
        let mut state = self.state;
        loop {
            match state {
                State::Size => {
                    let (size, used) = chunk_size_line(&input[pos..], limits)?;
                    pos += used;
                    state = if size == 0 {
                        State::Trailers
                    } else {
                        State::Data(size)
                    };
                }
                State::Data(remaining) => {
                    let available = input.len() - pos;
                    if available == 0 {
                        return Err(DecodeError::Incomplete { needed: 1 });
                    }
                    let n = remaining.min(available as u64) as usize;
                    let data = &input[pos..pos + n];
                    let left = remaining - n as u64;
                    self.state = if left == 0 {
                        State::DataEnd
                    } else {
                        State::Data(left)
                    };
                    return Ok((Chunk::Data(data), pos + n));
                }
                State::DataEnd => {
                    match &input[pos..] {
                        [b'\r', b'\n', ..] => pos += 2,
                        [b'\n', ..] => pos += 1,
                        [] | [b'\r'] => return Err(DecodeError::Incomplete { needed: 1 }),
                        _ => return Err(malformed("chunk data terminator")),
                    }
                    state = State::Size;
                }
                State::Trailers => {
                    let end =
                        find_section_end(input, pos, limits.max_head_bytes, "trailer section")?;
                    let fields = parse_fields(lines(&input[pos..end]), limits)?;
                    self.state = State::Done;
                    return Ok((Chunk::End(fields), end));
                }
                State::Done => return Err(malformed("chunked body already ended")),
            }
        }
    }
}

/// `chunk-size [ chunk-ext ] CRLF`: the size and the octets used.
fn chunk_size_line(input: &[u8], limits: Limits) -> Result<(u64, usize), DecodeError> {
    let window = &input[..input.len().min(limits.max_chunk_line)];
    let Some(lf) = window.iter().position(|&b| b == b'\n') else {
        return if input.len() >= limits.max_chunk_line {
            Err(DecodeError::TooLarge {
                what: "chunk line",
                declared: input.len() as u64,
                cap: limits.max_chunk_line as u64,
            })
        } else {
            Err(DecodeError::Incomplete { needed: 1 })
        };
    };
    let line = &window[..lf];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let digits = line.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    if digits == 0 {
        return Err(malformed("chunk size"));
    }
    let size = line[..digits].iter().try_fold(0u64, |acc, &d| {
        let v = (d as char).to_digit(16)?;
        acc.checked_mul(16)?.checked_add(u64::from(v))
    });
    let size = size.ok_or(malformed("chunk size"))?;
    match line.get(digits) {
        None | Some(b';' | b' ' | b'\t') => Ok((size, lf + 1)),
        Some(_) => Err(malformed("chunk size")),
    }
}

fn check_fields(fields: &[(&[u8], &[u8])]) -> Result<(), EncodeError> {
    for (name, value) in fields {
        if !is_token(name) {
            return Err(EncodeError::InvalidName { what: "field name" });
        }
        if !is_field_value(value) {
            return Err(EncodeError::InvalidValue {
                what: "field value",
            });
        }
    }
    Ok(())
}

fn write_fields(fields: &[(&[u8], &[u8])], out: &mut Vec<u8>) {
    for (name, value) in fields {
        out.extend_from_slice(name);
        out.extend_from_slice(b": ");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
}

/// Writes an `HTTP/1.1` request head, the empty line included.
///
/// # Errors
///
/// `InvalidName` `"method"` or `"field name"`; `InvalidValue`
/// `"request target"` or `"field value"`. Nothing is written on error.
pub fn encode_request_head(
    method: &[u8],
    target: &[u8],
    fields: &[(&[u8], &[u8])],
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    if !is_token(method) {
        return Err(EncodeError::InvalidName { what: "method" });
    }
    if target.is_empty() || !is_visible(target) {
        return Err(EncodeError::InvalidValue {
            what: "request target",
        });
    }
    check_fields(fields)?;
    out.extend_from_slice(method);
    out.push(b' ');
    out.extend_from_slice(target);
    out.extend_from_slice(b" HTTP/1.1\r\n");
    write_fields(fields, out);
    Ok(())
}

/// Writes a response head with version `HTTP/1.<minor>`, the empty line
/// included.
///
/// # Errors
///
/// `InvalidValue` `"version"` (minor not 0 or 1), `"status"` (outside
/// `100..=999`), `"reason phrase"`, `"field value"`; `InvalidName`
/// `"field name"`. Nothing is written on error.
pub fn encode_response_head(
    minor: u8,
    status: u16,
    reason: &[u8],
    fields: &[(&[u8], &[u8])],
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    if minor > 1 {
        return Err(EncodeError::InvalidValue { what: "version" });
    }
    if !(100..=999).contains(&status) {
        return Err(EncodeError::InvalidValue { what: "status" });
    }
    if !is_reason(reason) {
        return Err(EncodeError::InvalidValue {
            what: "reason phrase",
        });
    }
    check_fields(fields)?;
    out.extend_from_slice(b"HTTP/1.");
    out.push(b'0' + minor);
    out.push(b' ');
    out.extend_from_slice(status.to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(reason);
    out.extend_from_slice(b"\r\n");
    write_fields(fields, out);
    Ok(())
}

/// Writes one chunk: the size in uppercase hex, CRLF, the data, CRLF. Empty
/// data writes nothing, because a zero-size chunk is the last chunk.
pub fn encode_chunk(data: &[u8], out: &mut Vec<u8>) {
    if data.is_empty() {
        return;
    }
    out.extend_from_slice(format!("{:X}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

/// Writes the last chunk and the trailer section.
///
/// # Errors
///
/// As [`encode_request_head`] for the fields. Nothing is written on error.
pub fn encode_last_chunk(
    trailers: &[(&[u8], &[u8])],
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    check_fields(trailers)?;
    out.extend_from_slice(b"0\r\n");
    write_fields(trailers, out);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Limits = Limits::DEFAULT;

    #[test]
    fn a_head_of_exactly_the_cap_is_accepted_and_one_more_is_not() {
        let head = b"GET / HTTP/1.1\r\nHost: a\r\n\r\n";
        let limits = Limits {
            max_head_bytes: head.len(),
            ..L
        };
        assert_eq!(decode_request(head, limits).unwrap().1, head.len());
        let limits = Limits {
            max_head_bytes: head.len() - 1,
            ..L
        };
        assert!(matches!(
            decode_request(head, limits),
            Err(DecodeError::TooLarge { what: "head", .. })
        ));
    }

    #[test]
    fn leading_empty_lines_before_a_request_are_skipped() {
        let (head, used) = decode_request(b"\r\n\nGET / HTTP/1.0\r\n\r\nrest", L).unwrap();
        assert_eq!(head.method, b"GET");
        assert_eq!(head.minor, 0);
        assert_eq!(used, 21);
    }

    #[test]
    fn a_bare_lf_ends_a_line() {
        let (head, _) = decode_request(b"GET / HTTP/1.1\nHost: a\n\n", L).unwrap();
        assert_eq!(
            head.fields,
            vec![Field {
                name: b"Host",
                value: b"a"
            }]
        );
    }

    #[test]
    fn a_status_line_may_omit_the_reason() {
        let (head, _) = decode_response(b"HTTP/1.1 204\r\n\r\n", L).unwrap();
        assert_eq!(head.status, 204);
        assert_eq!(head.reason, b"");
    }

    #[test]
    fn the_field_count_is_capped() {
        let limits = Limits { max_fields: 1, ..L };
        assert!(matches!(
            decode_request(b"GET / HTTP/1.1\r\na: 1\r\nb: 2\r\n\r\n", limits),
            Err(DecodeError::TooLarge {
                what: "field count",
                declared: 2,
                cap: 1
            })
        ));
    }

    #[test]
    fn chunked_data_is_handed_out_as_it_arrives() {
        let mut d = ChunkedDecoder::new();
        let input = b"5\r\nhel";
        let (chunk, used) = d.step(input, L).unwrap();
        assert_eq!(chunk, Chunk::Data(b"hel"));
        assert_eq!(used, 6);
        let (chunk, used) = d.step(b"lo\r\n0\r\n\r\n", L).unwrap();
        assert_eq!(chunk, Chunk::Data(b"lo"));
        assert_eq!(used, 2);
        let (chunk, used) = d.step(b"\r\n0\r\n\r\n", L).unwrap();
        assert_eq!(chunk, Chunk::End(vec![]));
        assert_eq!(used, 7);
        assert!(d.is_done());
    }

    #[test]
    fn an_incomplete_step_leaves_the_state_alone() {
        let mut d = ChunkedDecoder::new();
        assert_eq!(
            d.step(b"5\r\n", L),
            Err(DecodeError::Incomplete { needed: 1 })
        );
        assert_eq!(d.step(b"5\r\nab", L).unwrap(), (Chunk::Data(b"ab"), 5));
    }

    #[test]
    fn chunk_extensions_are_skipped_but_bounded() {
        let mut d = ChunkedDecoder::new();
        let (chunk, _) = d.step(b"1;name=\"v\"\r\nx", L).unwrap();
        assert_eq!(chunk, Chunk::Data(b"x"));
        let limits = Limits {
            max_chunk_line: 8,
            ..L
        };
        assert!(matches!(
            ChunkedDecoder::new().step(b"1;aaaaaaaaaa\r\nx", limits),
            Err(DecodeError::TooLarge {
                what: "chunk line",
                ..
            })
        ));
    }

    #[test]
    fn a_chunk_size_beyond_u64_is_malformed() {
        assert_eq!(
            ChunkedDecoder::new().step(b"10000000000000000\r\n", L),
            Err(DecodeError::Malformed { what: "chunk size" })
        );
        assert!(
            ChunkedDecoder::new()
                .step(b"FFFFFFFFFFFFFFFF\r\nx", L)
                .is_ok()
        );
    }

    #[test]
    fn content_length_lists_of_equal_values_are_one_value() {
        let f = [
            Field {
                name: b"Content-Length",
                value: b"5, 5",
            },
            Field {
                name: b"content-length",
                value: b"5",
            },
        ];
        assert_eq!(request_framing(&f), Ok(Framing::Length(5)));
        let f = [Field {
            name: b"Content-Length",
            value: b"",
        }];
        assert_eq!(
            request_framing(&f),
            Err(DecodeError::Malformed {
                what: "Content-Length"
            })
        );
    }

    #[test]
    fn response_framing_follows_the_method_and_status() {
        let cl = [Field {
            name: b"Content-Length",
            value: b"9",
        }];
        assert_eq!(response_framing(b"HEAD", 200, &cl), Ok(Framing::Empty));
        assert_eq!(response_framing(b"GET", 204, &cl), Ok(Framing::Empty));
        assert_eq!(response_framing(b"GET", 304, &cl), Ok(Framing::Empty));
        assert_eq!(response_framing(b"GET", 101, &cl), Ok(Framing::Empty));
        assert_eq!(response_framing(b"GET", 200, &cl), Ok(Framing::Length(9)));
        assert_eq!(response_framing(b"GET", 200, &[]), Ok(Framing::UntilClose));
        let both = [
            Field {
                name: b"Transfer-Encoding",
                value: b"chunked",
            },
            Field {
                name: b"Content-Length",
                value: b"9",
            },
        ];
        assert_eq!(response_framing(b"GET", 200, &both), Ok(Framing::Chunked));
    }

    #[test]
    fn connection_options_are_a_case_insensitive_list() {
        let f = [Field {
            name: b"Connection",
            value: b"keep-alive, Close",
        }];
        assert!(connection_has(&f, "close"));
        assert!(connection_has(&f, "keep-alive"));
        assert!(!connection_has(&f, "upgrade"));
    }

    #[test]
    fn encoders_write_nothing_on_error() {
        let mut out = b"x".to_vec();
        assert_eq!(
            encode_request_head(b"GET", b"/", &[(b"a", b"ok"), (b"b", b"bad\n")], &mut out),
            Err(EncodeError::InvalidValue {
                what: "field value"
            })
        );
        assert_eq!(
            encode_response_head(2, 200, b"OK", &[], &mut out),
            Err(EncodeError::InvalidValue { what: "version" })
        );
        assert_eq!(out, b"x");
    }

    #[test]
    fn a_chunk_is_written_in_uppercase_hex() {
        let mut out = Vec::new();
        encode_chunk(&[0u8; 26], &mut out);
        assert!(out.starts_with(b"1A\r\n"));
        assert!(out.ends_with(b"\r\n"));
        let before = out.len();
        encode_chunk(b"", &mut out);
        assert_eq!(out.len(), before);
    }
}
