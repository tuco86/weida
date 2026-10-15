//! The twelve control-line operations and their grammar.
//!
//! The whole protocol is one enum. Every operation is a line of ASCII
//! terminated by `CRLF`, and four of them — `PUB`, `HPUB`, `MSG`, `HMSG` —
//! are followed by a body whose length that line declared.
//!
//! # The argument-count rule
//!
//! The optional middle arguments are positional and are distinguished
//! **only by how many arguments follow the verb**. There is no marker, no
//! keyword and no way to tell from an argument's own bytes what it is:
//!
//! ```text
//! PUB a 5      -> subject a,  no reply-to,   5 octets
//! PUB a b 5    -> subject a,  reply-to b,    5 octets
//! MSG a 1 5    -> subject a,  sid 1,  no reply-to,  5 octets
//! MSG a 1 b 5  -> subject a,  sid 1,  reply-to b,   5 octets
//! ```
//!
//! This is the single most error-prone part of the protocol, and it is the
//! reason the encoder refuses a subject with a space in it: `PUB "a b" 5`
//! would go out as `PUB a b 5`, which every conforming peer reads as a
//! publish with a reply subject. [`EncodeError::ArgumentNotOneToken`] is that
//! rule enforced from the other side.
//!
//! # The payload is never scanned
//!
//! The declared count is read from the control line and the body is then
//! taken *by length*; the trailing `CRLF` is required at exactly the offset
//! the count named and is never searched for. A payload is opaque application
//! bytes and may hold `CRLF` anywhere — a decoder that looked for a
//! terminator would split such a message in the wrong place and then read the
//! rest of it as protocol. [`DecodeError::MissingPayloadTerminator`] is what a
//! count that did not land on a `CRLF` produces, and there is no recovery
//! from it: at that point the reader has lost the framing.
//!
//! # The count is checked before anything is read
//!
//! `PUB x 18446744073709551615` is twenty-six octets that declare sixteen
//! exabytes. The count is checked against [`Limits::max_payload`] from the
//! control line alone — before the body is looked at, before a slice is
//! taken, and before anything at all is reserved. Nothing in this crate
//! reserves memory on behalf of a peer.
//!
//! Source: the client protocol reference (`docs/research/nats.md` §3, the
//! sheet's source 4).

use crate::connect::Connect;
use crate::error::{DecodeError, EncodeError};
use crate::headers::Headers;
use crate::json::push_decimal;
use crate::limits::Limits;

/// The most arguments any verb takes: `HMSG <subject> <sid> <reply-to>
/// <#header bytes> <#total bytes>`.
const MAX_ARGS: usize = 5;

/// One NATS protocol operation.
///
/// Borrows its subjects, identifiers and payload from the buffer it was
/// decoded from: nothing on this path is copied, and a subject keeps its
/// octets exactly as the peer wrote them. Verb *names* are matched ignoring
/// ASCII case — "NATS protocol operation names are case insensitive, thus
/// `SUB foo 1␍␊` and `sub foo 1␍␊` are equivalent" — but nothing else is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op<'a> {
    /// `INFO {json}␍␊`, server to client, the first thing on the connection
    /// and possibly again later.
    ///
    /// Carried as raw octets. `INFO` is the one object whose *fields* a
    /// client reads but whose *text* it never writes, and a server may add a
    /// field at any version; [`ServerInfo::parse`](crate::ServerInfo::parse)
    /// reads the fifteen fields a client acts on and steps over the rest.
    /// Keeping the octets means an `INFO` also survives a round trip
    /// unchanged.
    Info {
        /// The JSON object, `{` through `}`.
        json: &'a [u8],
    },
    /// `CONNECT {json}␍␊`, client to server, the answer to `INFO`.
    Connect(Connect<'a>),
    /// `PUB <subject> [reply-to] <#bytes>␍␊[payload]␍␊`.
    Pub {
        /// "The destination subject to publish to."
        subject: &'a [u8],
        /// "The reply subject that subscribers can use to send a response
        /// back to the publisher/requestor."
        reply_to: Option<&'a [u8]>,
        /// The payload, exactly as long as the control line declared. May be
        /// empty, and may hold `CRLF`.
        payload: &'a [u8],
    },
    /// `HPUB <subject> [reply-to] <#header bytes> <#total bytes>␍␊[headers]␍␊␍␊[payload]␍␊`.
    ///
    /// The two counts are not stored: the header count is what the block
    /// encodes to and the total is that plus the payload, so a decoded
    /// `HPUB` cannot hold counts that disagree with its own contents.
    Hpub {
        /// "The destination subject to publish to."
        subject: &'a [u8],
        /// The optional reply subject.
        reply_to: Option<&'a [u8]>,
        /// The `NATS/1.0` block.
        headers: Headers<'a>,
        /// The payload: total octets minus header octets.
        payload: &'a [u8],
    },
    /// `SUB <subject> [queue group] <sid>␍␊`.
    Sub {
        /// "The subject name to subscribe to."
        subject: &'a [u8],
        /// "If specified, the subscriber will join this queue group."
        queue_group: Option<&'a [u8]>,
        /// "A unique alphanumeric subscription ID, generated by the client."
        sid: &'a [u8],
    },
    /// `UNSUB <sid> [max_msgs]␍␊`.
    Unsub {
        /// The subscription to remove.
        sid: &'a [u8],
        /// "A number of messages to wait for before automatically
        /// unsubscribing."
        max_msgs: Option<u64>,
    },
    /// `MSG <subject> <sid> [reply-to] <#bytes>␍␊[payload]␍␊`.
    Msg {
        /// "Subject name this message was received on."
        subject: &'a [u8],
        /// "The unique alphanumeric subscription ID of the subject."
        sid: &'a [u8],
        /// "The subject on which the publisher is listening for responses."
        reply_to: Option<&'a [u8]>,
        /// The payload, exactly as long as the control line declared.
        payload: &'a [u8],
    },
    /// `HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>␍␊[headers]␍␊␍␊[payload]␍␊`.
    Hmsg {
        /// "Subject name this message was received on."
        subject: &'a [u8],
        /// "The unique alphanumeric subscription ID of the subject."
        sid: &'a [u8],
        /// The optional reply subject.
        reply_to: Option<&'a [u8]>,
        /// The `NATS/1.0` block.
        headers: Headers<'a>,
        /// The payload: total octets minus header octets.
        payload: &'a [u8],
    },
    /// `PING␍␊`, either direction.
    Ping,
    /// `PONG␍␊`, either direction.
    Pong,
    /// `+OK␍␊`, sent by the server for every well-formed client operation
    /// while `verbose` is on.
    Ok,
    /// `-ERR '<reason>'␍␊`, "a protocol, authorization, or other runtime
    /// connection error". Most of them are followed by a close.
    Err {
        /// The reason, without the single quotes that delimit it on the wire.
        /// Kept as octets: an error message is remote text and a client that
        /// could not report a non-UTF-8 one would lose the only explanation
        /// it is going to get.
        reason: &'a [u8],
    },
}

impl<'a> Op<'a> {
    /// Decode one operation, returning it and the octets it consumed.
    ///
    /// Returns [`DecodeError::Incomplete`] where `input` holds only part of
    /// an operation; every other error is a violation from which the only
    /// recovery is dropping the connection.
    ///
    /// `limits` is the caller's bounds, and both of the ones that matter here
    /// are checked before anything is read: the control line is bounded while
    /// the `CRLF` is searched for, and the declared payload count is bounded
    /// from the control line alone. An `INFO` line is bounded by
    /// [`Limits::max_info_line`]; every other line by
    /// [`Limits::max_control_line`].
    pub fn decode(input: &'a [u8], limits: Limits) -> Result<(Self, usize), DecodeError> {
        let max_line = if starts_info_line(input) {
            limits.max_info_line
        } else {
            limits.max_control_line
        };
        let line_end = find_control_line(input, max_line)?;
        let line = &input[..line_end];
        let after = line_end + 2;
        let (verb, rest) = split_verb(line);

        if is(verb, "MSG") {
            let args = split_args(rest, "MSG")?;
            let (subject, sid, reply_to, count) = match args.len {
                3 => (args.items[0], args.items[1], None, args.items[2]),
                4 => (
                    args.items[0],
                    args.items[1],
                    Some(args.items[2]),
                    args.items[3],
                ),
                found => return Err(DecodeError::ArgumentCount { verb: "MSG", found }),
            };
            let declared = number(count, "MSG", "#bytes")?;
            within(declared, limits)?;
            let payload = take_body(input, after, declared)?;
            return Ok((
                Op::Msg {
                    subject,
                    sid,
                    reply_to,
                    payload,
                },
                after + payload.len() + 2,
            ));
        }
        if is(verb, "HMSG") {
            let args = split_args(rest, "HMSG")?;
            let (subject, sid, reply_to, header, total) = match args.len {
                4 => (
                    args.items[0],
                    args.items[1],
                    None,
                    args.items[2],
                    args.items[3],
                ),
                5 => (
                    args.items[0],
                    args.items[1],
                    Some(args.items[2]),
                    args.items[3],
                    args.items[4],
                ),
                found => {
                    return Err(DecodeError::ArgumentCount {
                        verb: "HMSG",
                        found,
                    });
                }
            };
            let (headers, payload, used) =
                decode_with_headers(input, after, header, total, "HMSG", limits)?;
            return Ok((
                Op::Hmsg {
                    subject,
                    sid,
                    reply_to,
                    headers,
                    payload,
                },
                used,
            ));
        }
        if is(verb, "PUB") {
            let args = split_args(rest, "PUB")?;
            let (subject, reply_to, count) = match args.len {
                2 => (args.items[0], None, args.items[1]),
                3 => (args.items[0], Some(args.items[1]), args.items[2]),
                found => return Err(DecodeError::ArgumentCount { verb: "PUB", found }),
            };
            let declared = number(count, "PUB", "#bytes")?;
            within(declared, limits)?;
            let payload = take_body(input, after, declared)?;
            return Ok((
                Op::Pub {
                    subject,
                    reply_to,
                    payload,
                },
                after + payload.len() + 2,
            ));
        }
        if is(verb, "HPUB") {
            let args = split_args(rest, "HPUB")?;
            let (subject, reply_to, header, total) = match args.len {
                3 => (args.items[0], None, args.items[1], args.items[2]),
                4 => (
                    args.items[0],
                    Some(args.items[1]),
                    args.items[2],
                    args.items[3],
                ),
                found => {
                    return Err(DecodeError::ArgumentCount {
                        verb: "HPUB",
                        found,
                    });
                }
            };
            let (headers, payload, used) =
                decode_with_headers(input, after, header, total, "HPUB", limits)?;
            return Ok((
                Op::Hpub {
                    subject,
                    reply_to,
                    headers,
                    payload,
                },
                used,
            ));
        }
        if is(verb, "SUB") {
            let args = split_args(rest, "SUB")?;
            let (subject, queue_group, sid) = match args.len {
                2 => (args.items[0], None, args.items[1]),
                3 => (args.items[0], Some(args.items[1]), args.items[2]),
                found => return Err(DecodeError::ArgumentCount { verb: "SUB", found }),
            };
            return Ok((
                Op::Sub {
                    subject,
                    queue_group,
                    sid,
                },
                after,
            ));
        }
        if is(verb, "UNSUB") {
            let args = split_args(rest, "UNSUB")?;
            let (sid, max_msgs) = match args.len {
                1 => (args.items[0], None),
                2 => (
                    args.items[0],
                    Some(number(args.items[1], "UNSUB", "max_msgs")?),
                ),
                found => {
                    return Err(DecodeError::ArgumentCount {
                        verb: "UNSUB",
                        found,
                    });
                }
            };
            return Ok((Op::Unsub { sid, max_msgs }, after));
        }
        if is(verb, "PING") {
            no_arguments(rest, "PING")?;
            return Ok((Op::Ping, after));
        }
        if is(verb, "PONG") {
            no_arguments(rest, "PONG")?;
            return Ok((Op::Pong, after));
        }
        if is(verb, "+OK") {
            no_arguments(rest, "+OK")?;
            return Ok((Op::Ok, after));
        }
        if is(verb, "-ERR") {
            return Ok((
                Op::Err {
                    reason: unquote(trim(rest)),
                },
                after,
            ));
        }
        if is(verb, "INFO") {
            return Ok((
                Op::Info {
                    json: object(trim(rest))?,
                },
                after,
            ));
        }
        if is(verb, "CONNECT") {
            let connect = Connect::parse(object(trim(rest))?, limits)?;
            return Ok((Op::Connect(connect), after));
        }
        Err(DecodeError::UnknownVerb)
    }

    /// Append the canonical encoding of this operation.
    ///
    /// Canonical: the verb in upper case, exactly one space between
    /// arguments, the counts in decimal with no padding. Nothing is written
    /// to `out` unless the whole operation can be written, so a rejected
    /// encode never leaves half a control line behind for the next one to
    /// continue.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            Op::Info { json } => {
                if json.iter().any(|byte| *byte == b'\r' || *byte == b'\n') {
                    return Err(EncodeError::EmbeddedNewline { field: "INFO" });
                }
                out.extend_from_slice(b"INFO ");
                out.extend_from_slice(json);
                out.extend_from_slice(b"\r\n");
            }
            Op::Connect(connect) => {
                out.extend_from_slice(b"CONNECT ");
                connect.write_json(out);
                out.extend_from_slice(b"\r\n");
            }
            Op::Pub {
                subject,
                reply_to,
                payload,
            } => {
                token("PUB", "subject", subject)?;
                optional_token("PUB", "reply-to", *reply_to)?;
                out.extend_from_slice(b"PUB ");
                out.extend_from_slice(subject);
                push_optional(out, *reply_to);
                out.push(b' ');
                push_decimal(out, payload.len() as u64);
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(payload);
                out.extend_from_slice(b"\r\n");
            }
            Op::Hpub {
                subject,
                reply_to,
                headers,
                payload,
            } => {
                token("HPUB", "subject", subject)?;
                optional_token("HPUB", "reply-to", *reply_to)?;
                headers.check()?;
                out.extend_from_slice(b"HPUB ");
                out.extend_from_slice(subject);
                push_optional(out, *reply_to);
                push_header_counts(out, headers, payload);
                write_body(out, headers, payload);
            }
            Op::Sub {
                subject,
                queue_group,
                sid,
            } => {
                token("SUB", "subject", subject)?;
                optional_token("SUB", "queue group", *queue_group)?;
                token("SUB", "sid", sid)?;
                out.extend_from_slice(b"SUB ");
                out.extend_from_slice(subject);
                push_optional(out, *queue_group);
                out.push(b' ');
                out.extend_from_slice(sid);
                out.extend_from_slice(b"\r\n");
            }
            Op::Unsub { sid, max_msgs } => {
                token("UNSUB", "sid", sid)?;
                out.extend_from_slice(b"UNSUB ");
                out.extend_from_slice(sid);
                if let Some(max_msgs) = max_msgs {
                    out.push(b' ');
                    push_decimal(out, *max_msgs);
                }
                out.extend_from_slice(b"\r\n");
            }
            Op::Msg {
                subject,
                sid,
                reply_to,
                payload,
            } => {
                token("MSG", "subject", subject)?;
                token("MSG", "sid", sid)?;
                optional_token("MSG", "reply-to", *reply_to)?;
                out.extend_from_slice(b"MSG ");
                out.extend_from_slice(subject);
                out.push(b' ');
                out.extend_from_slice(sid);
                push_optional(out, *reply_to);
                out.push(b' ');
                push_decimal(out, payload.len() as u64);
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(payload);
                out.extend_from_slice(b"\r\n");
            }
            Op::Hmsg {
                subject,
                sid,
                reply_to,
                headers,
                payload,
            } => {
                token("HMSG", "subject", subject)?;
                token("HMSG", "sid", sid)?;
                optional_token("HMSG", "reply-to", *reply_to)?;
                headers.check()?;
                out.extend_from_slice(b"HMSG ");
                out.extend_from_slice(subject);
                out.push(b' ');
                out.extend_from_slice(sid);
                push_optional(out, *reply_to);
                push_header_counts(out, headers, payload);
                write_body(out, headers, payload);
            }
            Op::Ping => out.extend_from_slice(b"PING\r\n"),
            Op::Pong => out.extend_from_slice(b"PONG\r\n"),
            Op::Ok => out.extend_from_slice(b"+OK\r\n"),
            Op::Err { reason } => {
                if reason.iter().any(|byte| *byte == b'\r' || *byte == b'\n') {
                    return Err(EncodeError::EmbeddedNewline {
                        field: "-ERR reason",
                    });
                }
                out.extend_from_slice(b"-ERR '");
                out.extend_from_slice(reason);
                out.extend_from_slice(b"'\r\n");
            }
        }
        Ok(())
    }

    /// The verb, as the reference spells it.
    #[must_use]
    pub const fn verb(&self) -> &'static str {
        match self {
            Op::Info { .. } => "INFO",
            Op::Connect(_) => "CONNECT",
            Op::Pub { .. } => "PUB",
            Op::Hpub { .. } => "HPUB",
            Op::Sub { .. } => "SUB",
            Op::Unsub { .. } => "UNSUB",
            Op::Msg { .. } => "MSG",
            Op::Hmsg { .. } => "HMSG",
            Op::Ping => "PING",
            Op::Pong => "PONG",
            Op::Ok => "+OK",
            Op::Err { .. } => "-ERR",
        }
    }

    /// The payload of the four operations that carry one.
    #[must_use]
    pub const fn payload(&self) -> Option<&'a [u8]> {
        match self {
            Op::Pub { payload, .. }
            | Op::Hpub { payload, .. }
            | Op::Msg { payload, .. }
            | Op::Hmsg { payload, .. } => Some(payload),
            _ => None,
        }
    }

    /// The header block of the two operations that carry one.
    #[must_use]
    pub const fn headers(&self) -> Option<&Headers<'a>> {
        match self {
            Op::Hpub { headers, .. } | Op::Hmsg { headers, .. } => Some(headers),
            _ => None,
        }
    }
}

/// The arguments of one control line, without allocating: five is the most
/// any verb takes.
struct Args<'a> {
    items: [&'a [u8]; MAX_ARGS],
    len: usize,
}

/// Split on runs of spaces and tabs.
///
/// The reference writes exactly one space between arguments and the encoder
/// writes exactly one, but a run is accepted: the server's own parser skips
/// whitespace, and an operation whose meaning depended on how many spaces it
/// was written with would be a worse protocol than the one documented.
fn split_args<'a>(rest: &'a [u8], verb: &'static str) -> Result<Args<'a>, DecodeError> {
    let mut args = Args {
        items: [b""; MAX_ARGS],
        len: 0,
    };
    let mut at = 0;
    while at < rest.len() {
        while at < rest.len() && (rest[at] == b' ' || rest[at] == b'\t') {
            at += 1;
        }
        if at >= rest.len() {
            break;
        }
        let start = at;
        while at < rest.len() && rest[at] != b' ' && rest[at] != b'\t' {
            at += 1;
        }
        if args.len == MAX_ARGS {
            return Err(DecodeError::ArgumentCount {
                verb,
                found: MAX_ARGS + 1,
            });
        }
        args.items[args.len] = &rest[start..at];
        args.len += 1;
    }
    Ok(args)
}

/// The header count, the total count, and the body they delimit.
///
/// Both counts come off the control line before the body is touched: the
/// total is checked against the caller's cap, and the header count against
/// the total, because the payload is total minus header and a header count
/// above the total would be a negative payload.
fn decode_with_headers<'a>(
    input: &'a [u8],
    after: usize,
    header: &[u8],
    total: &[u8],
    verb: &'static str,
    limits: Limits,
) -> Result<(Headers<'a>, &'a [u8], usize), DecodeError> {
    let header_len = number(header, verb, "#header bytes")?;
    let total_len = number(total, verb, "#total bytes")?;
    within(total_len, limits)?;
    if header_len > total_len {
        return Err(DecodeError::HeaderBytesAboveTotal {
            header: header_len,
            total: total_len,
        });
    }
    let body = take_body(input, after, total_len)?;
    // `header_len <= total_len` and the body is exactly `total_len` octets,
    // so this cannot exceed the slice.
    let split = header_len as usize;
    let headers = Headers::decode(&body[..split], limits)?;
    Ok((headers, &body[split..], after + body.len() + 2))
}

/// Where the control line's `CRLF` starts, or why it does not.
///
/// The search is bounded by `max`, so a peer that never sends a `CRLF` is
/// refused after `max` octets rather than after the reader's buffer fills.
///
/// A bare `CR` or `LF` inside the line is a violation rather than an
/// argument byte. `nats-server`'s own parser ends an operation at a bare
/// `LF`, so a line holding one means the sender and this reader disagree
/// about where the line ends — and an argument that disagrees about that
/// cannot be written back out either.
fn find_control_line(input: &[u8], max: usize) -> Result<usize, DecodeError> {
    let horizon = input.len().min(max.saturating_add(2));
    let window = &input[..horizon];
    for (at, byte) in window.iter().enumerate() {
        match byte {
            b'\r' => match window.get(at + 1) {
                Some(b'\n') => return Ok(at),
                Some(_) => return Err(DecodeError::ControlLineHasNewline),
                // The input ends on the `CR`, so the `LF` may simply not have
                // arrived yet. The line is `at` octets long: incomplete where
                // that is within the bound, too long where it is not. It is
                // judged by the line and not by how much has been read, or a
                // line of exactly `max` octets would be refused whenever a
                // read ended between its `CR` and its `LF`.
                None => {
                    return Err(if at <= max {
                        DecodeError::Incomplete { needed: 1 }
                    } else {
                        DecodeError::ControlLineTooLong { max }
                    });
                }
            },
            b'\n' => return Err(DecodeError::ControlLineHasNewline),
            _ => {}
        }
    }
    if input.len() > max {
        return Err(DecodeError::ControlLineTooLong { max });
    }
    Err(DecodeError::Incomplete {
        needed: 2usize.saturating_sub(input.len()).max(1),
    })
}

/// Whether the line is an `INFO`, judged from its first five octets.
///
/// The verb is matched ignoring ASCII case and must end at a space, a tab or
/// the `CR` of the `CRLF`, so that `INFOX` keeps the control-line bound. Input
/// shorter than five octets is not judged: the `CRLF` search then reports
/// `Incomplete` under the control-line bound, as it does for any short input.
fn starts_info_line(input: &[u8]) -> bool {
    matches!(
        input.get(..5),
        Some([
            b'I' | b'i',
            b'N' | b'n',
            b'F' | b'f',
            b'O' | b'o',
            b' ' | b'\t' | b'\r'
        ])
    )
}

fn split_verb(line: &[u8]) -> (&[u8], &[u8]) {
    let end = line
        .iter()
        .position(|byte| *byte == b' ' || *byte == b'\t')
        .unwrap_or(line.len());
    (&line[..end], &line[end..])
}

/// "NATS protocol operation names are case insensitive."
fn is(verb: &[u8], name: &str) -> bool {
    verb.eq_ignore_ascii_case(name.as_bytes())
}

fn no_arguments(rest: &[u8], verb: &'static str) -> Result<(), DecodeError> {
    let args = split_args(rest, verb)?;
    if args.len == 0 {
        Ok(())
    } else {
        Err(DecodeError::ArgumentCount {
            verb,
            found: args.len,
        })
    }
}

/// A decimal count: digits and nothing else.
///
/// A sign is refused rather than parsed: these are sizes, and `PUB x -1`
/// declaring a payload is not a shape any reader should have an opinion
/// about.
fn number(raw: &[u8], verb: &'static str, argument: &'static str) -> Result<u64, DecodeError> {
    if raw.is_empty() || !raw.iter().all(u8::is_ascii_digit) {
        return Err(DecodeError::NotANumber { verb, argument });
    }
    let mut value: u64 = 0;
    for &digit in raw {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(digit - b'0')))
            .ok_or(DecodeError::NumberTooLarge { verb, argument })?;
    }
    Ok(value)
}

/// The declared count against the caller's cap, from the control line alone.
fn within(declared: u64, limits: Limits) -> Result<(), DecodeError> {
    if declared > limits.max_payload {
        return Err(DecodeError::PayloadTooLarge {
            declared,
            cap: limits.max_payload,
        });
    }
    Ok(())
}

/// `len` octets from `at`, and the `CRLF` that must follow them.
///
/// By length, never by search: the payload is opaque and may hold `CRLF`.
fn take_body(input: &[u8], at: usize, len: u64) -> Result<&[u8], DecodeError> {
    let available = (input.len() - at) as u64;
    let needed = len.saturating_add(2);
    if needed > available {
        return Err(DecodeError::Incomplete {
            needed: usize::try_from(needed - available).unwrap_or(usize::MAX),
        });
    }
    // `len + 2 <= available <= usize::MAX`, so this is lossless.
    let len = len as usize;
    let end = at + len;
    if &input[end..end + 2] != b"\r\n" {
        return Err(DecodeError::MissingPayloadTerminator);
    }
    Ok(&input[at..end])
}

/// Strip the single quotes the server puts around a `-ERR` reason.
///
/// The reference's syntax block says `-ERR <error message>` and every
/// example, including its own telnet transcript, says `-ERR 'Stale
/// Connection'`. Both are read; only the quoted form is written.
fn unquote(reason: &[u8]) -> &[u8] {
    if reason.len() >= 2 && reason.first() == Some(&b'\'') && reason.last() == Some(&b'\'') {
        &reason[1..reason.len() - 1]
    } else {
        reason
    }
}

/// The argument of `INFO` or `CONNECT`, checked to be an object before it is
/// carried or parsed.
fn object(raw: &[u8]) -> Result<&[u8], DecodeError> {
    if raw.first() == Some(&b'{') && raw.last() == Some(&b'}') {
        Ok(raw)
    } else {
        Err(DecodeError::JsonNotAnObject)
    }
}

fn trim(raw: &[u8]) -> &[u8] {
    let mut raw = raw;
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

/// One argument of a control line: non-empty, and one token.
fn token(verb: &'static str, argument: &'static str, value: &[u8]) -> Result<(), EncodeError> {
    if value.is_empty() {
        return Err(EncodeError::EmptyArgument { verb, argument });
    }
    if value
        .iter()
        .any(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
    {
        return Err(EncodeError::ArgumentNotOneToken { verb, argument });
    }
    Ok(())
}

fn optional_token(
    verb: &'static str,
    argument: &'static str,
    value: Option<&[u8]>,
) -> Result<(), EncodeError> {
    match value {
        Some(value) => token(verb, argument, value),
        None => Ok(()),
    }
}

fn push_optional(out: &mut Vec<u8>, value: Option<&[u8]>) {
    if let Some(value) = value {
        out.push(b' ');
        out.extend_from_slice(value);
    }
}

/// ` <#header bytes> <#total bytes>␍␊`.
///
/// The total is headers *plus* payload: "the total size of headers and
/// payload sections in bytes".
fn push_header_counts(out: &mut Vec<u8>, headers: &Headers<'_>, payload: &[u8]) {
    let header_len = headers.encoded_len();
    out.push(b' ');
    push_decimal(out, header_len as u64);
    out.push(b' ');
    push_decimal(out, (header_len + payload.len()) as u64);
    out.extend_from_slice(b"\r\n");
}

/// Append the header block and then the payload, with the `CRLF` after it.
///
/// The block is not checked again here: the operation encoder has already run
/// [`Headers::check`] on it before writing the control line, which is what
/// keeps a refused operation from leaving bytes behind.
fn write_body(out: &mut Vec<u8>, headers: &Headers<'_>, payload: &[u8]) {
    let before = out.len();
    headers.encode_checked(out);
    debug_assert_eq!(
        out.len() - before,
        headers.encoded_len(),
        "the declared header count and the block written must agree"
    );
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::info::ServerInfo;
    use crate::limits::{DEFAULT_MAX_CONTROL_LINE, DEFAULT_MAX_INFO_LINE};

    const LIMITS: Limits = Limits::DEFAULT;

    fn decode(input: &[u8]) -> Result<(Op<'_>, usize), DecodeError> {
        Op::decode(input, LIMITS)
    }

    #[test]
    fn an_operation_is_consumed_exactly() {
        let stream = b"PING\r\nPONG\r\nPUB a 1\r\nx\r\n";
        let (first, used) = decode(stream).expect("decodes");
        assert_eq!((first, used), (Op::Ping, 6));
        let (second, used) = decode(&stream[6..]).expect("decodes");
        assert_eq!((second, used), (Op::Pong, 6));
        let (third, used) = decode(&stream[12..]).expect("decodes");
        assert_eq!(used, 12);
        assert_eq!(third.payload(), Some(&b"x"[..]));
    }

    #[test]
    fn a_verb_is_case_insensitive_but_its_arguments_are_not() {
        // "NATS protocol operation names are case insensitive, thus
        // `SUB foo 1␍␊` and `sub foo 1␍␊` are equivalent."
        let upper = decode(b"SUB foo 1\r\n").expect("decodes").0;
        let lower = decode(b"sub foo 1\r\n").expect("decodes").0;
        assert_eq!(upper, lower);
        let shouting = decode(b"SUB FOO 1\r\n").expect("decodes").0;
        assert_ne!(upper, shouting, "a subject keeps its octets");
    }

    #[test]
    fn a_split_operation_asks_for_more() {
        let whole = b"PUB FOO 11\r\nHello NATS!\r\n";
        for prefix in 0..whole.len() {
            let error = decode(&whole[..prefix]).expect_err("incomplete");
            assert!(!error.is_violation(), "{prefix}: {error}");
        }
        assert!(decode(whole).is_ok());
    }

    #[test]
    fn the_payload_is_taken_by_length_and_never_searched() {
        // A payload holding the terminator in the middle: a decoder that
        // looked for CRLF would stop after "one" and read "two" as protocol.
        let input = b"PUB FOO 8\r\none\r\ntwo\r\n";
        let (op, used) = decode(input).expect("decodes");
        assert_eq!(op.payload(), Some(&b"one\r\ntwo"[..]));
        assert_eq!(used, input.len());
    }

    #[test]
    fn a_count_that_does_not_land_on_a_terminator_is_a_violation() {
        assert_eq!(
            decode(b"PUB FOO 2\r\nabc\r\n"),
            Err(DecodeError::MissingPayloadTerminator)
        );
    }

    #[test]
    fn an_unterminated_control_line_is_refused_at_the_bound() {
        let limits = Limits {
            max_control_line: 16,
            ..LIMITS
        };
        let flood = vec![b'x'; 64];
        assert_eq!(
            Op::decode(&flood, limits),
            Err(DecodeError::ControlLineTooLong { max: 16 })
        );
        // Short of the bound it is merely incomplete.
        assert!(matches!(
            Op::decode(&flood[..8], limits),
            Err(DecodeError::Incomplete { .. })
        ));
    }

    #[test]
    fn a_reason_reads_with_or_without_its_quotes() {
        assert_eq!(
            decode(b"-ERR 'Stale Connection'\r\n").expect("decodes").0,
            Op::Err {
                reason: b"Stale Connection"
            }
        );
        assert_eq!(
            decode(b"-ERR Unknown Protocol Operation\r\n")
                .expect("decodes")
                .0,
            Op::Err {
                reason: b"Unknown Protocol Operation"
            }
        );
    }

    #[test]
    fn an_encoder_refuses_a_subject_that_would_change_the_argument_count() {
        let smuggled = Op::Pub {
            subject: b"a b",
            reply_to: None,
            payload: b"hello",
        };
        let mut out = Vec::new();
        assert_eq!(
            smuggled.encode(&mut out),
            Err(EncodeError::ArgumentNotOneToken {
                verb: "PUB",
                argument: "subject"
            })
        );
    }

    #[test]
    fn a_refused_operation_writes_nothing() {
        let mut out = Vec::from(*b"PING\r\n");
        let bad = Op::Msg {
            subject: b"a",
            sid: b"",
            reply_to: None,
            payload: b"",
        };
        assert!(bad.encode(&mut out).is_err());
        assert_eq!(out, b"PING\r\n", "no half-written control line");
    }

    /// An `INFO` line carrying `count` server URLs, in the shape a server sends.
    fn info_with_urls(count: usize) -> String {
        let urls: Vec<String> = (0..count)
            .map(|i| format!("\"10.0.0.{i}.cluster.internal.example:4222\""))
            .collect();
        format!("INFO {{\"connect_urls\":[{}]}}\r\n", urls.join(","))
    }

    #[test]
    fn a_large_info_line_is_bounded_by_its_own_limit() {
        // 200 URLs make a line longer than a control line may be, so only the
        // INFO bound lets it through.
        let wire = info_with_urls(200);
        assert!(wire.len() > DEFAULT_MAX_CONTROL_LINE);
        assert!(wire.len() < DEFAULT_MAX_INFO_LINE);
        let (op, used) = decode(wire.as_bytes()).expect("decodes");
        assert_eq!(used, wire.len());
        let Op::Info { json } = op else {
            panic!("an INFO");
        };
        let urls = ServerInfo::parse(json, LIMITS)
            .expect("parses")
            .connect_urls
            .expect("present");
        assert_eq!(urls.len(), 200);
        assert_eq!(urls[199], "10.0.0.199.cluster.internal.example:4222");
    }

    #[test]
    fn an_info_line_above_its_bound_is_refused_at_that_bound() {
        let wire = info_with_urls(200);
        let limits = Limits {
            max_info_line: 1024,
            ..LIMITS
        };
        assert!(wire.len() > 1024);
        assert_eq!(
            Op::decode(wire.as_bytes(), limits),
            Err(DecodeError::ControlLineTooLong { max: 1024 })
        );
    }

    #[test]
    fn a_msg_line_keeps_the_control_line_bound() {
        // Longer than `max_control_line` and shorter than `max_info_line`: the
        // larger bound belongs to INFO alone.
        let wire = format!("MSG {} 1 0\r\n\r\n", "a".repeat(5000));
        assert!(wire.len() > DEFAULT_MAX_CONTROL_LINE);
        assert!(wire.len() < DEFAULT_MAX_INFO_LINE);
        assert_eq!(
            decode(wire.as_bytes()),
            Err(DecodeError::ControlLineTooLong {
                max: DEFAULT_MAX_CONTROL_LINE
            })
        );
    }

    #[test]
    fn the_info_verb_takes_the_info_bound_in_any_case() {
        let wire = info_with_urls(200).replacen("INFO", "info", 1);
        assert!(wire.len() > DEFAULT_MAX_CONTROL_LINE);
        let (op, used) = decode(wire.as_bytes()).expect("decodes");
        assert_eq!(used, wire.len());
        assert!(matches!(op, Op::Info { .. }));
    }

    #[test]
    fn a_verb_that_only_starts_with_info_keeps_the_control_line_bound() {
        let wire = info_with_urls(200).replacen("INFO ", "INFOX ", 1);
        assert!(wire.len() > DEFAULT_MAX_CONTROL_LINE);
        assert_eq!(
            decode(wire.as_bytes()),
            Err(DecodeError::ControlLineTooLong {
                max: DEFAULT_MAX_CONTROL_LINE
            })
        );
    }

    #[test]
    fn debug_of_an_operation_redacts_its_connect_secrets() {
        use std::borrow::Cow;
        let op = Op::Connect(Connect {
            pass: Some(Cow::Borrowed("hunter2")),
            auth_token: Some(Cow::Borrowed("t0k3n")),
            jwt: Some(Cow::Borrowed("ey.J.W.T")),
            sig: Some(Cow::Borrowed("SIGNATURE")),
            user: Some(Cow::Borrowed("alice")),
            ..Connect::default()
        });
        let shown = format!("{op:?}");
        for secret in ["hunter2", "t0k3n", "ey.J.W.T", "SIGNATURE"] {
            assert!(!shown.contains(secret), "{secret} leaked: {shown}");
        }
        assert!(shown.contains("<redacted>"), "{shown}");
        assert!(shown.contains("alice"), "{shown}");
    }

    #[test]
    fn a_refused_header_block_writes_nothing() {
        let mut headers = Headers::new();
        headers.push("Bar", " padded ");
        let refused = Op::Hpub {
            subject: b"a",
            reply_to: None,
            headers,
            payload: b"",
        };
        let mut out = Vec::from(*b"PING\r\n");
        assert_eq!(
            refused.encode(&mut out),
            Err(EncodeError::HeaderValueInvalid)
        );
        assert_eq!(out, b"PING\r\n", "no half-written HPUB");
    }

    /// A line of exactly the bound is legal, and so it is when a read ends
    /// right after its `CR` and the `LF` comes in the next one: the reader
    /// must ask for more, not refuse a line that fits. One octet more is too
    /// long however much of it has arrived.
    #[test]
    fn a_line_of_exactly_the_bound_is_accepted_across_a_split_crlf() {
        let limits = Limits {
            max_control_line: 16,
            ..LIMITS
        };
        // `-ERR '` + nine octets + `'` is sixteen octets before the CRLF.
        let line = b"-ERR 'abcdefghi'";
        assert_eq!(line.len(), 16);

        let mut cut_after_cr = line.to_vec();
        cut_after_cr.push(b'\r');
        assert_eq!(
            Op::decode(&cut_after_cr, limits),
            Err(DecodeError::Incomplete { needed: 1 }),
            "the LF has not arrived yet"
        );

        let mut whole = cut_after_cr.clone();
        whole.push(b'\n');
        assert_eq!(
            Op::decode(&whole, limits).expect("decodes"),
            (
                Op::Err {
                    reason: b"abcdefghi"
                },
                18
            )
        );

        // Every prefix of the legal line is merely incomplete.
        for prefix in 0..whole.len() {
            let error = Op::decode(&whole[..prefix], limits).expect_err("incomplete");
            assert!(!error.is_violation(), "{prefix}: {error}");
        }

        // Seventeen octets before the CR is one too many, with or without the
        // LF having arrived.
        let long = b"-ERR 'abcdefghij'";
        assert_eq!(long.len(), 17);
        let mut long_cr = long.to_vec();
        long_cr.push(b'\r');
        assert_eq!(
            Op::decode(&long_cr, limits),
            Err(DecodeError::ControlLineTooLong { max: 16 })
        );
        assert_eq!(
            Op::decode(long, limits),
            Err(DecodeError::ControlLineTooLong { max: 16 })
        );
    }
}
