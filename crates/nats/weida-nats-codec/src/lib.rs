//! NATS client protocol codec: the twelve control-line verbs and the
//! `NATS/1.0` header block, sans-I/O.
//!
//! This crate is the codec under `weida-nats`, the NATS client library
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.3). It is
//! **sans-I/O and weida-free**: no sockets, no executor, no dependency — not
//! even on `weida-core`, and not even a JSON one. What it exports is a
//! byte-for-byte encoder and decoder for the published client protocol, so
//! that it can be checked against a foreign specification rather than against
//! weida's opinion of it. `weida-nats`, the client built on top, depends on
//! this crate and on `weida-runtime`; this one depends on neither.
//!
//! # Shape
//!
//! * [`Op`] — the twelve operations, `INFO` and `CONNECT` through `+OK` and
//!   `-ERR`, with the control-line grammar and the argument-count rule that
//!   decides what an optional middle argument is.
//! * [`Headers`] — the `NATS/1.0` block: repeated names in order, case
//!   preserved, and the optional status and description that carry the
//!   no-responder answer.
//! * [`ServerInfo`] — the fourteen `INFO` fields a client acts on, read out
//!   of the raw JSON on demand.
//! * [`Connect`] — the one JSON object this crate writes, escaped so that no
//!   credential can end the string it is in.
//! * [`Limits`] — the caller's bounds, an argument to every decode, and the
//!   documented defaults they start from.
//! * [`error`] — why bytes were rejected, split by the direction that
//!   produced them.
//!
//! # Reading a stream
//!
//! ```
//! use weida_nats_codec::{Limits, Op};
//!
//! // What a server sends a subscriber, payload and all.
//! let wire = b"MSG FOO.BAR 9 GREETING.34 11\r\nHello World\r\n";
//! let (op, used) = Op::decode(wire, Limits::DEFAULT).expect("a MSG");
//! assert_eq!(used, wire.len());
//! assert_eq!(
//!     op,
//!     Op::Msg {
//!         subject: b"FOO.BAR",
//!         sid: b"9",
//!         reply_to: Some(b"GREETING.34"),
//!         payload: b"Hello World",
//!     }
//! );
//!
//! // The same line without the reply subject is one argument shorter, and
//! // that is the *only* thing that distinguishes them.
//! let (plain, _) = Op::decode(b"MSG FOO.BAR 9 11\r\nHello World\r\n", Limits::DEFAULT)
//!     .expect("a MSG");
//! assert_eq!(plain.payload(), Some(&b"Hello World"[..]));
//! assert!(matches!(plain, Op::Msg { reply_to: None, .. }));
//! ```
//!
//! # Bounds
//!
//! NATS declares its sizes in ASCII: twenty-six octets of control line can
//! announce sixteen exabytes of payload. Every decode entry point therefore
//! takes [`Limits`] as an **argument**, and the declared count is refused
//! from the control line alone — before the payload is looked at and before
//! anything is reserved. The real ceiling is remote configuration
//! (`INFO.max_payload`), which is the second reason it cannot be a constant.
//!
//! # Where this codec departs from the text
//!
//! * **The JSON reader is not a JSON parser, deliberately.** This crate has
//!   no dependencies, and `INFO` and `CONNECT` are the only JSON in the
//!   protocol. So the reader — private, and it stays private —
//!   accepts exactly the shapes the reference's field tables name: string,
//!   integer, boolean, array of strings, `null`. A *known* field of any other
//!   shape is a named error. An *unknown* field is stepped over, including an
//!   object or an array, to
//!   [`MAX_JSON_DEPTH`](limits::MAX_JSON_DEPTH) — because a server that adds
//!   a field must not make its `INFO` unreadable, and because stepping over
//!   it is the only recursion in the crate. `\uXXXX` is supported, surrogate
//!   pairs included: `nats-server` marshals `INFO` with Go's
//!   `encoding/json`, which escapes `<`, `>` and `&` by default, so a reader
//!   without it would mangle real traffic.
//! * **`INFO` travels as raw octets; `CONNECT` travels as fields.** A client
//!   reads `INFO` and never writes it, and writes `CONNECT` and never reads
//!   it. Making [`Op::Info`] carry the object unparsed means an `INFO` round
//!   trips exactly and that a field this crate does not know is not lost;
//!   [`ServerInfo::parse`] is the reader. `CONNECT` is a struct because the
//!   client chooses every value, and a client assembling JSON by hand is a
//!   client that can put a `CRLF` in the middle of a control line.
//! * **`-ERR` is read quoted or bare, and always written quoted.** The
//!   reference's syntax block says `-ERR <error message>`; its own telnet
//!   transcript and every server build say `-ERR 'Stale Connection'`. A
//!   matched pair of single quotes is stripped on the way in and quotes are
//!   always written on the way out. Only the *outermost* pair is stripped, so
//!   a reason that itself holds a quote — `Permissions Violation for Publish
//!   to 'a.b'` — still survives the round trip unchanged; the alternative,
//!   refusing it, would make a value this decoder produces one its own
//!   encoder cannot write.
//! * **The reference's first `HMSG` example is malformed and is refused.**
//!   `HMSG FOO.BAR 34 45␍␊NATS/1.0␍␊FoodGroup: vegetable␍␊␍␊Hello World␍␊`
//!   has three arguments where `HMSG <subject> <sid> [reply-to]
//!   <#header bytes> <#total bytes>` needs four; the `sid` is missing. The
//!   very next example in the same section, `HMSG FOO.BAR 9 BAZ.69 34 45`,
//!   has it, and the `MSG` section's grammar makes `sid` unconditional. It is
//!   a typo in the document, not a fifth form, and
//!   `tests/golden_vectors.rs` pins both readings.
//! * **A header block with no entries is accepted.** The reference says the
//!   block is "`NATS/1.0␍␊` followed by one or more `name: value` pairs", but
//!   the no-responder answer is `NATS/1.0 503␍␊␍␊` and a push consumer's
//!   idle heartbeat is `NATS/1.0 100 Idle Heartbeat␍␊␍␊`, neither of which
//!   has a single pair. The status forms win.
//! * **A version-line tail must be a three-digit code.** ADR-4 and the status
//!   messages give `NATS/1.0 <code> [description]` and nothing else, so
//!   `NATS/1.0 whatever` is [`DecodeError::InvalidHeaderStatus`] rather than
//!   a description with no code. Guessing here would mean guessing at whether
//!   a request had a responder.
//! * **Header values are trimmed, and the encoder refuses values that would
//!   be.** `Bar: Baz` and `Bar:Baz` decode to the same value, because the
//!   reference writes the space and nothing says a receiver keeps it. That
//!   makes a value with surrounding whitespace unable to survive a round
//!   trip, so [`Headers::encode`] refuses one instead of writing something
//!   its own decoder would read differently.
//! * **`CRLF` is required, and a bare `CR` or `LF` inside a control line is a
//!   violation.** `nats-server`'s own parser ends an operation at a bare
//!   `LF`; the reference documents `␍␊` everywhere. A client decoder is
//!   reading a *server*, which always writes both, so the strict reading
//!   costs nothing and keeps one framing rule instead of two — and a line
//!   holding a bare `LF` is one whose sender and reader disagree about where
//!   it ends, which would let a subject carry an octet this crate's own
//!   encoder could not write back
//!   ([`DecodeError::ControlLineHasNewline`]). The *payload* is never
//!   examined for either.
//! * **Argument separators are runs, not single spaces.** The encoder writes
//!   one space; the decoder accepts spaces and tabs in runs, as the server's
//!   parser does. An operation whose meaning depended on how many spaces it
//!   was written with would be a worse protocol than the documented one.
//! * **`max_control_line` has two published defaults.** The reference's error
//!   table says the option "default is 1024 bytes"; the server has compiled
//!   4096 for several major versions. [`limits::DEFAULT_MAX_CONTROL_LINE`] is
//!   the larger, with the reasoning at its definition — and it is a caller
//!   argument regardless.
//! * **The `HPUB`/`HMSG` cap is on the total, not the payload.** "Headers
//!   count within the `HPUB` total size and therefore within the server's
//!   accepted message size" (`docs/research/nats.md` §3), so
//!   [`Limits::max_payload`] is checked against `#total bytes`.
//! * **Subjects stay octets; header names and values must be UTF-8.**
//!   Subjects, `sid`s, queue groups, reply subjects and the `-ERR` reason are
//!   `&[u8]`: they are remote text this crate has no reason to validate, and
//!   an error message a client could not report because it was not UTF-8
//!   would be the one explanation it loses. Header names and values are
//!   `&str`, because ADR-4's HTTP-like grammar is text and a client compares
//!   them.
//!
//! # Sources
//!
//! Every rule and every number comes from `docs/research/nats.md`, which
//! cites:
//!
//! * [Client Protocol](https://docs.nats.io/reference/protocols/client) — the
//!   twelve verbs, their grammar, their arguments and their errors (the
//!   sheet's source 4).
//! * [ADR-4, NATS Message Headers](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-4.md)
//!   — the `NATS/1.0` block (the sheet's source 12).
//! * [Server Configuration](https://docs.nats.io/running-a-nats-service/configuration)
//!   — `max_payload`, `max_control_line` and the other bounds of §11 (the
//!   sheet's source 7).

#![warn(missing_docs)]

pub mod connect;
pub mod error;
pub mod headers;
pub mod info;
mod json;
pub mod limits;
pub mod op;

pub use connect::Connect;
pub use error::{DecodeError, EncodeError};
pub use headers::Headers;
pub use info::ServerInfo;
pub use limits::Limits;
pub use op::Op;
