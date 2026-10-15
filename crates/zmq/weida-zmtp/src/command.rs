//! Commands: `READY`, `ERROR`, `SUBSCRIBE`, `CANCEL`, `PING`, `PONG`, and
//! PLAIN's `HELLO`, `WELCOME` and `INITIATE`.
//!
//! ```text
//! command-body = command-name command-data
//! command-name = short-size 1*255command-name-char
//! command-name-char = ALPHA
//! ```
//!
//! **The specification contradicts itself here, and this codec follows the
//! grammar.** The prose says commands contain "a printable command name, a
//! null octet separator, and data", but the ABNF - and every per-command
//! grammar under it, e.g. `ready = command-size %d5 "READY" metadata` - gives a
//! **length octet** before the name and no separator at all. libzmq puts
//! `\x05READY` on the wire, so the length form is what interoperates; the
//! null-separator sentence is a leftover and is not implemented.
//!
//! Commands are borrowed from the frame body, so decoding one allocates at most
//! the property list of a `READY` or `INITIATE`, capped at
//! [`MAX_PROPERTIES`](crate::metadata::MAX_PROPERTIES) entries.

use crate::error::CommandError;
use crate::frame::{self, FrameKind};
use crate::metadata::Metadata;

/// Largest `PING`/`PONG` context: "0*16OCTET", echoed verbatim by `PONG`.
pub const MAX_PING_CONTEXT: usize = 16;

/// Longest PLAIN username or password: `*-length = 1OCTET`, so the field's
/// own length octet is the bound (24/ZMTP-PLAIN).
pub const MAX_PLAIN_FIELD: usize = 255;

/// A decoded command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command<'a> {
    /// The NULL mechanism's handshake command, carrying connection metadata.
    /// The client sends it and waits for one in reply; messages may flow once
    /// it has been both sent and received.
    Ready(Metadata<'a>),
    /// A fatal handshake error with a reason to log and no defined semantics.
    /// "The peer SHALL treat an incoming ERROR command as fatal."
    Error(&'a str),
    /// A subscription: a binary prefix, empty meaning everything.
    /// Subscriptions are additive and not idempotent - two `SUBSCRIBE`s need
    /// two `CANCEL`s.
    Subscribe(&'a [u8]),
    /// Cancels one subscription previously sent.
    Cancel(&'a [u8]),
    /// A heartbeat request.
    Ping {
        /// Time to live in tenths of a second, a hint to the peer to
        /// disconnect after that much silence. Zero means no hint; the
        /// maximum is 6553.5 seconds.
        ttl: u16,
        /// Opaque context, at most [`MAX_PING_CONTEXT`] octets, echoed by the
        /// `PONG`.
        context: &'a [u8],
    },
    /// The reply to a `PING`, echoing its context.
    Pong {
        /// The context of the `PING` being answered.
        context: &'a [u8],
    },
    /// PLAIN's first command: the username and password in clear text, sent
    /// by the client (24/ZMTP-PLAIN).
    ///
    /// ```text
    /// hello = command-size %d5 "HELLO" username password
    /// username = username-length username-value
    /// username-length = 1OCTET
    /// password = password-length password-value
    /// ```
    ///
    /// The mechanism is "not robust against even the simplest traffic
    /// snooping or spoofing attacks" and says so in its own RFC; this codec's
    /// job is that the octets are right, not that they are safe.
    Hello {
        /// `username-value = *OCTET`, at most [`MAX_PLAIN_FIELD`] octets.
        username: &'a [u8],
        /// `password-value = *OCTET`, at most [`MAX_PLAIN_FIELD`] octets.
        password: &'a [u8],
    },
    /// PLAIN's answer to an accepted `HELLO`, with no data at all:
    /// `welcome = command-size %d7 "WELCOME"`. A refusal is `ERROR`.
    Welcome,
    /// PLAIN's second client command, carrying the client's metadata the way
    /// `READY` carries it for NULL: `initiate = command-size %d8 "INITIATE"
    /// metadata`.
    Initiate(Metadata<'a>),
}

impl<'a> Command<'a> {
    /// The wire name.
    pub const fn name(&self) -> &'static str {
        match self {
            Command::Ready(_) => "READY",
            Command::Error(_) => "ERROR",
            Command::Subscribe(_) => "SUBSCRIBE",
            Command::Cancel(_) => "CANCEL",
            Command::Ping { .. } => "PING",
            Command::Pong { .. } => "PONG",
            Command::Hello { .. } => "HELLO",
            Command::Welcome => "WELCOME",
            Command::Initiate(_) => "INITIATE",
        }
    }

    /// Decodes a command body: the contents of a frame whose COMMAND flag was
    /// set, without the flags and size octets.
    pub fn decode(body: &'a [u8]) -> Result<Self, CommandError> {
        let name_len = usize::from(*body.first().ok_or(CommandError::Truncated)?);
        if name_len == 0 {
            return Err(CommandError::BadName);
        }
        let name = body.get(1..1 + name_len).ok_or(CommandError::Truncated)?;
        if !name.iter().all(u8::is_ascii_alphabetic) {
            return Err(CommandError::BadName);
        }
        let data = &body[1 + name_len..];

        match name {
            b"READY" => Ok(Command::Ready(Metadata::decode(data)?)),
            b"ERROR" => {
                let len = usize::from(*data.first().ok_or(CommandError::Truncated)?);
                let reason = data.get(1..1 + len).ok_or(CommandError::Truncated)?;
                Ok(Command::Error(check_reason(reason)?))
            }
            b"SUBSCRIBE" => Ok(Command::Subscribe(data)),
            b"CANCEL" => Ok(Command::Cancel(data)),
            b"PING" => {
                let ttl = data.get(..2).ok_or(CommandError::Truncated)?;
                let context = &data[2..];
                check_context(context)?;
                Ok(Command::Ping {
                    ttl: u16::from_be_bytes([ttl[0], ttl[1]]),
                    context,
                })
            }
            b"PONG" => {
                check_context(data)?;
                Ok(Command::Pong { context: data })
            }
            b"HELLO" => {
                let (username, rest) = take_field(data)?;
                let (password, rest) = take_field(rest)?;
                if !rest.is_empty() {
                    // `hello = %d5 "HELLO" username password` and nothing
                    // else: trailing octets are a different command.
                    return Err(CommandError::Truncated);
                }
                Ok(Command::Hello { username, password })
            }
            b"WELCOME" => {
                if !data.is_empty() {
                    return Err(CommandError::Truncated);
                }
                Ok(Command::Welcome)
            }
            b"INITIATE" => Ok(Command::Initiate(Metadata::decode(data)?)),
            _ => Err(CommandError::UnknownName),
        }
    }

    /// Appends the command body - name and data, without frame header.
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), CommandError> {
        let name = self.name().as_bytes();
        out.push(name.len() as u8);
        out.extend_from_slice(name);
        match self {
            Command::Ready(metadata) => metadata.encode(out)?,
            Command::Error(reason) => {
                let reason = check_reason(reason.as_bytes())?.as_bytes();
                out.push(reason.len() as u8);
                out.extend_from_slice(reason);
            }
            Command::Subscribe(prefix) | Command::Cancel(prefix) => {
                out.extend_from_slice(prefix);
            }
            Command::Ping { ttl, context } => {
                check_context(context)?;
                out.extend_from_slice(&ttl.to_be_bytes());
                out.extend_from_slice(context);
            }
            Command::Pong { context } => {
                check_context(context)?;
                out.extend_from_slice(context);
            }
            Command::Hello { username, password } => {
                put_field(username, out)?;
                put_field(password, out)?;
            }
            Command::Welcome => {}
            Command::Initiate(metadata) => metadata.encode(out)?,
        }
        Ok(())
    }

    /// Builds the complete command frame, flags and size included.
    pub fn encode(&self) -> Result<Vec<u8>, CommandError> {
        let mut body = Vec::new();
        self.encode_body(&mut body)?;
        Ok(frame::encode(FrameKind::Command, &body))
    }
}

/// `username = username-length username-value`, and the same shape for the
/// password: one length octet, then that many octets.
fn take_field(data: &[u8]) -> Result<(&[u8], &[u8]), CommandError> {
    let len = usize::from(*data.first().ok_or(CommandError::Truncated)?);
    let value = data.get(1..1 + len).ok_or(CommandError::Truncated)?;
    Ok((value, &data[1 + len..]))
}

/// The other direction, refusing a field the length octet cannot describe —
/// which is the only way a PLAIN field can be malformed.
fn put_field(value: &[u8], out: &mut Vec<u8>) -> Result<(), CommandError> {
    if value.len() > MAX_PLAIN_FIELD {
        return Err(CommandError::FieldTooLong(value.len()));
    }
    out.push(value.len() as u8);
    out.extend_from_slice(value);
    Ok(())
}

/// `ping-context = 0*16OCTET`, for both `PING` and `PONG`.
fn check_context(context: &[u8]) -> Result<(), CommandError> {
    if context.len() > MAX_PING_CONTEXT {
        return Err(CommandError::ContextTooLong(context.len()));
    }
    Ok(())
}

/// `error-reason = short-size 0*255VCHAR`.
///
/// Accepts the space octet, which `VCHAR` excludes: libzmq's own reasons read
/// like "Unknown mechanism", so a strict reading would reject the reference
/// implementation's `ERROR` commands. Everything outside printable ASCII is
/// still refused, in both directions - an error reason is for a log, and a
/// reason carrying control octets is a log injection, not a diagnosis.
fn check_reason(reason: &[u8]) -> Result<&str, CommandError> {
    if reason.len() > 255 {
        return Err(CommandError::ReasonTooLong(reason.len()));
    }
    if !reason.iter().all(|b| (0x20..=0x7E).contains(b)) {
        return Err(CommandError::ReasonNotPrintable);
    }
    Ok(std::str::from_utf8(reason).expect("printable ASCII is UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FrameError;
    use crate::metadata::SocketType;

    /// Encodes, checks the bytes, and decodes the body back.
    #[track_caller]
    fn round_trip(command: Command<'_>, expected: &[u8]) {
        let frame = command.encode().expect("encode");
        assert_eq!(frame, expected, "{} frame bytes", command.name());
        let (header, body, used) = frame::decode(&frame, 4096).expect("decode frame");
        assert_eq!(header.kind, FrameKind::Command);
        assert_eq!(used, frame.len());
        assert_eq!(Command::decode(body).expect("decode body"), command);
    }

    #[test]
    fn ready_carries_a_length_prefixed_name_and_metadata() {
        round_trip(
            Command::Ready(Metadata::new().with_socket_type(SocketType::Req)),
            b"\x04\x19\x05READY\x0BSocket-Type\x00\x00\x00\x03REQ",
        );
    }

    #[test]
    fn an_empty_ready_completes_a_handshake() {
        round_trip(Command::Ready(Metadata::new()), b"\x04\x06\x05READY");
    }

    #[test]
    fn error_carries_a_length_prefixed_reason() {
        round_trip(
            Command::Error("bad socket type"),
            b"\x04\x16\x05ERROR\x0Fbad socket type",
        );
        round_trip(Command::Error(""), b"\x04\x07\x05ERROR\x00");
    }

    #[test]
    fn subscribe_and_cancel_carry_a_raw_prefix() {
        round_trip(
            Command::Subscribe(b"px.eur"),
            b"\x04\x10\x09SUBSCRIBEpx.eur",
        );
        round_trip(Command::Cancel(b"px.eur"), b"\x04\x0D\x06CANCELpx.eur");
        // An empty subscription matches everything, and is not the same thing
        // as no subscription.
        round_trip(Command::Subscribe(b""), b"\x04\x0A\x09SUBSCRIBE");
    }

    #[test]
    fn ping_carries_a_ttl_in_tenths_of_a_second() {
        round_trip(
            Command::Ping {
                ttl: 300,
                context: b"ctx",
            },
            b"\x04\x0A\x04PING\x01\x2Cctx",
        );
        round_trip(Command::Pong { context: b"ctx" }, b"\x04\x08\x04PONGctx");
        // The documented maximum: 6553.5 seconds.
        round_trip(
            Command::Ping {
                ttl: u16::MAX,
                context: b"",
            },
            b"\x04\x07\x04PING\xFF\xFF",
        );
    }

    #[test]
    fn a_context_beyond_16_octets_is_refused_in_both_directions() {
        let long = [b'c'; 17];
        assert_eq!(
            Command::Pong { context: &long }.encode(),
            Err(CommandError::ContextTooLong(17))
        );
        assert_eq!(
            Command::Ping {
                ttl: 0,
                context: &long
            }
            .encode(),
            Err(CommandError::ContextTooLong(17))
        );

        let mut body = b"\x04PONG".to_vec();
        body.extend_from_slice(&long);
        assert_eq!(
            Command::decode(&body),
            Err(CommandError::ContextTooLong(17))
        );

        // Exactly 16 is legal.
        let ok = [b'c'; 16];
        assert!(Command::Pong { context: &ok }.encode().is_ok());
    }

    #[test]
    fn a_ping_without_its_ttl_is_truncated() {
        assert_eq!(Command::decode(b"\x04PING"), Err(CommandError::Truncated));
        assert_eq!(
            Command::decode(b"\x04PING\x01"),
            Err(CommandError::Truncated)
        );
    }

    #[test]
    fn an_unprintable_or_over_long_reason_is_refused() {
        assert_eq!(
            Command::Error("line\nbreak").encode(),
            Err(CommandError::ReasonNotPrintable)
        );
        assert_eq!(
            Command::decode(b"\x05ERROR\x05a\x00b\x01c"),
            Err(CommandError::ReasonNotPrintable)
        );
        let long = "x".repeat(256);
        assert_eq!(
            Command::Error(&long).encode(),
            Err(CommandError::ReasonTooLong(256))
        );
    }

    #[test]
    fn an_unknown_command_is_named_as_such() {
        // JOIN belongs to RADIO/DISH, which this crate does not carry.
        assert_eq!(
            Command::decode(b"\x04JOINgroup"),
            Err(CommandError::UnknownName)
        );
        assert_eq!(
            Command::decode(b"\x07NOSUCHX"),
            Err(CommandError::UnknownName)
        );
    }

    #[test]
    fn a_malformed_command_name_is_rejected() {
        assert_eq!(Command::decode(b""), Err(CommandError::Truncated));
        assert_eq!(Command::decode(b"\x00"), Err(CommandError::BadName));
        assert_eq!(Command::decode(b"\x05READ"), Err(CommandError::Truncated));
        // `command-name-char = ALPHA`: a digit is not a command name.
        assert_eq!(Command::decode(b"\x05RE4DY"), Err(CommandError::BadName));
        // The null-separator form the prose describes is not the wire form.
        assert_eq!(
            Command::decode(b"READY\x00"),
            Err(CommandError::Truncated),
            "the first octet is a length, so 'R' means 82 octets of name"
        );
    }

    #[test]
    fn a_long_ready_uses_a_long_command_frame() {
        // A READY beyond 255 octets must switch to flags 0x06, and the
        // metadata must survive the switch.
        let big = [b'v'; 400];
        let command = Command::Ready(Metadata::new().with("X-Big", &big));
        let frame = command.encode().expect("encode");
        assert_eq!(frame[0], 0x06, "COMMAND | LONG");
        let (header, body, _) = frame::decode(&frame, 4096).expect("decode");
        assert_eq!(header.len, 6 + 1 + 5 + 4 + 400);
        assert_eq!(Command::decode(body).expect("body"), command);
    }

    #[test]
    fn a_command_frame_beyond_the_cap_never_reaches_the_command_decoder() {
        let big = [b'v'; 400];
        let frame = Command::Ready(Metadata::new().with("X-Big", &big))
            .encode()
            .expect("encode");
        assert_eq!(
            frame::decode(&frame, 100),
            Err(FrameError::BodyTooLarge { len: 416, max: 100 })
        );
    }
}
