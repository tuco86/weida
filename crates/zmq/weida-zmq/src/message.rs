//! Messages: frames, multipart, and the size bound that is checked before
//! anything is allocated.
//!
//! **A message is one value.** 37/ZMTP: "A message SHALL be sent or received
//! atomically; that is, all frames or none. On sending, the peer SHALL queue
//! all frames of a message in memory until the final frame is sent", and the
//! zguide's corollary is that "the first part (and all following parts) are
//! only actually sent on the wire when you send the final part"
//! (`docs/research/zeromq.md` §3). So [`Multipart`] is a value handed over
//! whole rather than a builder that writes as it goes: a half-sent message
//! has no representation here, which is also what makes `ZMQ_MAXMSGSIZE`
//! checkable before a body is touched.
//!
//! **The size check is the codec's, not ours.** `weida-zmtp` decodes a frame
//! header and rejects an over-large *declared* length before the body is
//! looked at, let alone reserved — it is the only place that check can
//! happen, and it is already written and fuzzed. This module passes the
//! remaining budget down and translates the refusal into
//! [`Error::EMSGSIZE`]; it does not repeat the arithmetic.

use weida_zmtp::{FrameError, FrameKind, frame};

use crate::error::{Error, Result};

/// Default `ZMQ_MAXMSGSIZE`, in bytes, for a whole message.
///
/// **This is the second default that deliberately differs from libzmq**
/// ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4
/// item 5). libzmq's `ZMQ_MAXMSGSIZE` is `-1`, "no limit", against a grammar
/// where a peer may declare a frame of 2^63-1 octets
/// (`docs/research/zeromq.md` §3, §11) — so the default answer to a hostile
/// declaration is to believe it, and "no remote input can cause unbounded
/// memory allocation" (`docs/INVARIANTS.md`) would be false by default.
///
/// One mebibyte is the ceiling this crate chooses under that rule. It is
/// settable back to anything, including far higher; it is never silent.
///
/// **The exposure is a product, and the product is bounded.** The high-water
/// marks bound a queue in *messages* — libzmq's unit — so one peer's queue
/// could hold `hwm` messages of this size in each direction: 1000 × 1 MiB at
/// the defaults, which is why a message count alone is not a memory bound.
/// Each direction of each peer's queue therefore carries
/// [`crate::DEFAULT_QUEUE_BYTES`] as well (8 MiB), and a queue that is not
/// empty refuses past it (`pipe`'s module documentation). What this number
/// bounds is what remains: the **one** message an empty queue always accepts,
/// whatever its size.
pub const DEFAULT_MAX_MESSAGE_SIZE: u64 = 1024 * 1024;

/// Default ceiling on the frames of one message.
///
/// **37/ZMTP sets none** — "the total number of message parts is unlimited
/// except by available memory" (`docs/research/zeromq.md` §3) — so this
/// number is ours, in the same way the finite close budget and the real
/// `ZMQ_MAXMSGSIZE` default are
/// ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4
/// item 5).
///
/// It is chosen against two products. A frame costs at least two octets on
/// the wire and a `Message` costs a `Vec` header plus its bytes, so 1024
/// frames of nothing are kilobytes rather than the eight megabytes an
/// unbounded count allowed. And the count bounds *work*, not only memory:
/// an incomplete message is re-parsed from the front on every read, so the
/// re-parse is 1024 headers at worst instead of the whole buffer. Every
/// envelope any ZeroMQ pattern defines is a handful of frames — the reply
/// envelope is one delimiter plus addresses, a RADIO message is two — so a
/// legitimate message is orders of magnitude below this, and an application
/// whose own framing is not may raise it.
pub const DEFAULT_MAX_MESSAGE_FRAMES: usize = 1024;

/// What bounds one inbound message.
///
/// Two numbers, because one of them alone bounds nothing: bytes without a
/// frame count lets empty frames accumulate for free, and a frame count
/// without bytes lets one frame declare 2^63-1 octets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageLimits {
    /// `ZMQ_MAXMSGSIZE`, counting frame headers as well as declared bodies.
    pub max_bytes: u64,
    /// Frames one message may have. See [`DEFAULT_MAX_MESSAGE_FRAMES`].
    pub max_frames: usize,
}

impl MessageLimits {
    /// Limits of `max_bytes` and `max_frames`.
    pub const fn new(max_bytes: u64, max_frames: usize) -> MessageLimits {
        MessageLimits {
            max_bytes,
            max_frames,
        }
    }
}

impl Default for MessageLimits {
    fn default() -> MessageLimits {
        MessageLimits::new(DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_MESSAGE_FRAMES)
    }
}

/// One frame: an opaque byte string.
///
/// ZeroMQ has no payload typing — "ZeroMQ strings are length-specified and
/// are sent on the wire *without* a trailing null", and applications own all
/// serialization (`docs/research/zeromq.md` §3). An empty frame is a legal
/// frame and a load-bearing one: it is the envelope delimiter REQ prepends
/// and REP strips.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct Message {
    bytes: Vec<u8>,
}

impl Message {
    /// An empty frame — the envelope delimiter, and a legal message of size
    /// zero.
    pub const fn empty() -> Message {
        Message { bytes: Vec::new() }
    }

    /// The frame's bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// The frame's length in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether this is the empty frame.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Takes the bytes, consuming the frame.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl From<Vec<u8>> for Message {
    fn from(bytes: Vec<u8>) -> Message {
        Message { bytes }
    }
}

impl From<&[u8]> for Message {
    fn from(bytes: &[u8]) -> Message {
        Message {
            bytes: bytes.to_vec(),
        }
    }
}

impl<const N: usize> From<&[u8; N]> for Message {
    fn from(bytes: &[u8; N]) -> Message {
        Message {
            bytes: bytes.to_vec(),
        }
    }
}

impl From<String> for Message {
    fn from(text: String) -> Message {
        Message {
            bytes: text.into_bytes(),
        }
    }
}

impl From<&str> for Message {
    fn from(text: &str) -> Message {
        Message {
            bytes: text.as_bytes().to_vec(),
        }
    }
}

impl AsRef<[u8]> for Message {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A payload is opaque and may be huge or binary, so show what a log
        // line can use: the length, and the bytes only while they are short
        // and printable.
        if self.bytes.len() <= 32
            && self
                .bytes
                .iter()
                .all(|b| b.is_ascii_graphic() || *b == b' ')
        {
            write!(
                f,
                "Message({:?}, {} bytes)",
                String::from_utf8_lossy(&self.bytes),
                self.bytes.len()
            )
        } else {
            write!(f, "Message({} bytes)", self.bytes.len())
        }
    }
}

/// A whole ZeroMQ message: one or more frames, delivered all or none.
///
/// Never empty. A message of zero frames does not exist in ZMTP — the
/// smallest message is one empty frame — and refusing to represent one keeps
/// every consumer from having to handle a case the protocol does not have.
#[derive(Clone, PartialEq, Eq)]
pub struct Multipart {
    frames: Vec<Message>,
}

impl Multipart {
    /// A single-frame message.
    pub fn single(frame: impl Into<Message>) -> Multipart {
        Multipart {
            frames: vec![frame.into()],
        }
    }

    /// A message of `frames`, in order.
    ///
    /// Fails with `EINVAL` on an empty frame list: that is not a message.
    pub fn new(frames: Vec<Message>) -> Result<Multipart> {
        if frames.is_empty() {
            return Err(Error::EINVAL(
                "a message has at least one frame; the smallest one is a single empty frame".into(),
            ));
        }
        Ok(Multipart { frames })
    }

    /// The frames, in order.
    pub fn frames(&self) -> &[Message] {
        &self.frames
    }

    /// How many frames this message has. Never zero.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Always `false`; kept so that `len()` has its usual companion.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// The total payload, in bytes, across every frame — what
    /// `ZMQ_MAXMSGSIZE` bounds and what a queue holds.
    pub fn total_bytes(&self) -> u64 {
        self.frames.iter().map(|frame| frame.len() as u64).sum()
    }

    /// Appends a frame.
    pub fn push(&mut self, frame: impl Into<Message>) {
        self.frames.push(frame.into());
    }

    /// Takes the frames, consuming the message.
    pub fn into_frames(self) -> Vec<Message> {
        self.frames
    }

    /// Encodes the whole message: MORE set on every frame but the last.
    ///
    /// One buffer for the whole message, because that is what atomicity means
    /// on the sending side: "the peer SHALL queue all frames of a message in
    /// memory until the final frame is sent" (§3). A caller cannot hand half
    /// of this to a socket.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.total_bytes() as usize + 2 * self.frames.len());
        let last = self.frames.len() - 1;
        for (index, frame) in self.frames.iter().enumerate() {
            frame::encode_header(
                FrameKind::Message {
                    more: index != last,
                },
                frame.len() as u64,
                &mut out,
            );
            out.extend_from_slice(frame.as_slice());
        }
        out
    }

    /// Decodes whatever is at the front of `input`, under `limits`.
    ///
    /// Nothing is delivered until it is whole: a buffer holding the first two
    /// frames of a three-frame message yields [`Decoded::Incomplete`] and
    /// consumes nothing, which is "all frames or none" on the receiving side.
    /// A command frame is not part of any message, so it is reported
    /// separately for the session layer to consume — and a command *inside* a
    /// multipart sequence is a protocol violation, because MORE "SHALL be
    /// zero on command frames".
    ///
    /// Fails with `EMSGSIZE` when the message exceeds either bound of
    /// [`MessageLimits`], and does so from the **declared** sizes: a header
    /// alone is enough to refuse it, before the body has arrived and before
    /// anything is reserved for it.
    ///
    /// **Both bounds are load-bearing, and the second one was a hole.**
    /// Counting declared bodies alone bounds nothing: an empty frame declares
    /// no body, so a peer sending `01 00` — one empty frame, MORE set —
    /// forever grew a reader's buffer by two octets per frame with
    /// `ZMQ_MAXMSGSIZE` untouched (measured: four million of them are eight
    /// megabytes held against a one-mebibyte ceiling, and still
    /// `Incomplete`). So a frame costs its **header plus its declared body**
    /// against [`MessageLimits::max_bytes`], and the frame count has a
    /// ceiling of its own.
    pub fn decode(input: &[u8], limits: MessageLimits) -> Result<Decoded<'_>> {
        let mut frames: Vec<Message> = Vec::new();
        let mut held: u64 = 0;
        let mut at = 0usize;
        loop {
            let remaining = limits.max_bytes.saturating_sub(held);
            let (header, used) = match frame::decode_header(&input[at..], remaining) {
                Ok(decoded) => decoded,
                Err(FrameError::Incomplete) => return Ok(Decoded::Incomplete),
                Err(e) => return Err(frame_error(e, limits.max_bytes)),
            };
            // The header's own octets count too: this is the check that makes
            // a stream of empty frames finite.
            let cost = used as u64 + header.len;
            if cost > remaining {
                return Err(Error::EMSGSIZE(
                    format!(
                        "this message has reached its {}-byte budget (ZMQ_MAXMSGSIZE), \
                         counting frame headers; the next frame needs {cost} more",
                        limits.max_bytes
                    )
                    .into(),
                ));
            }
            let body_at = at + used;
            let Some(end) = body_at.checked_add(header.len as usize) else {
                return Err(Error::EMSGSIZE(
                    format!("a frame declaring {} bytes cannot be addressed", header.len).into(),
                ));
            };
            if input.len() < end {
                // The declaration is within budget but the body has not
                // arrived. Nothing is consumed and nothing is delivered.
                return Ok(Decoded::Incomplete);
            }
            match header.kind {
                FrameKind::Command => {
                    if !frames.is_empty() {
                        return Err(Error::EINVAL(
                            "a command frame appeared inside a multipart message; MORE is zero \
                             on commands, so a command is never part of one"
                                .into(),
                        ));
                    }
                    return Ok(Decoded::Command {
                        body: &input[body_at..end],
                        consumed: end,
                    });
                }
                FrameKind::Message { more } => {
                    if frames.len() >= limits.max_frames {
                        return Err(Error::EMSGSIZE(
                            format!(
                                "this message already has its ceiling of {} frames; \
                                 37/ZMTP sets no limit, so this one is ours",
                                limits.max_frames
                            )
                            .into(),
                        ));
                    }
                    held += cost;
                    frames.push(Message::from(&input[body_at..end]));
                    at = end;
                    if !more {
                        return Ok(Decoded::Message {
                            message: Multipart { frames },
                            consumed: at,
                        });
                    }
                }
            }
        }
    }
}

/// Translates the codec's refusal into this crate's vocabulary.
fn frame_error(error: FrameError, max_message_bytes: u64) -> Error {
    match error {
        FrameError::BodyTooLarge { len, max } => Error::EMSGSIZE(
            format!(
                "a peer declared a {len}-byte frame against a {max}-byte budget \
                 (ZMQ_MAXMSGSIZE is {max_message_bytes} for the whole message); \
                 nothing was allocated for it"
            )
            .into(),
        ),
        FrameError::Incomplete => Error::EAGAIN("the frame is not complete yet".into()),
        other => Error::EINVAL(format!("malformed frame: {other}").into()),
    }
}

/// What [`Multipart::decode`] found at the front of a buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum Decoded<'a> {
    /// A whole message, and how many octets of the buffer it occupied.
    Message {
        /// The message, every frame of it.
        message: Multipart,
        /// Octets consumed from the front of the buffer.
        consumed: usize,
    },
    /// A command frame, which belongs to the session layer rather than to any
    /// message: `READY`, `PING`, `SUBSCRIBE` and the rest
    /// (`docs/research/zeromq.md` §3). The body is borrowed from the buffer.
    Command {
        /// The command frame's body, still encoded.
        body: &'a [u8],
        /// Octets consumed from the front of the buffer.
        consumed: usize,
    },
    /// Not a whole message yet. Nothing was consumed, and nothing may be
    /// delivered: on the wire a multipart message is only a message once its
    /// last frame has arrived.
    Incomplete,
}

impl std::fmt::Debug for Multipart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Multipart")
            .field("frames", &self.frames)
            .field("total_bytes", &self.total_bytes())
            .finish()
    }
}

impl From<Message> for Multipart {
    fn from(frame: Message) -> Multipart {
        Multipart {
            frames: vec![frame],
        }
    }
}

/// Every one-frame conversion a [`Message`] has, a single-frame message has
/// too: the overwhelmingly common case is one frame, and making a caller
/// write it twice buys nothing.
impl From<Vec<u8>> for Multipart {
    fn from(bytes: Vec<u8>) -> Multipart {
        Multipart::single(Message::from(bytes))
    }
}

impl From<&[u8]> for Multipart {
    fn from(bytes: &[u8]) -> Multipart {
        Multipart::single(Message::from(bytes))
    }
}

impl<const N: usize> From<&[u8; N]> for Multipart {
    fn from(bytes: &[u8; N]) -> Multipart {
        Multipart::single(Message::from(bytes))
    }
}

impl From<String> for Multipart {
    fn from(text: String) -> Multipart {
        Multipart::single(Message::from(text))
    }
}

impl From<&str> for Multipart {
    fn from(text: &str) -> Multipart {
        Multipart::single(Message::from(text))
    }
}

impl IntoIterator for Multipart {
    type Item = Message;
    type IntoIter = std::vec::IntoIter<Message>;

    fn into_iter(self) -> Self::IntoIter {
        self.frames.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(frames: &[&[u8]]) -> Multipart {
        Multipart::new(frames.iter().map(|f| Message::from(*f)).collect()).expect("frames")
    }

    /// Claim: a message of no frames does not exist, and the smallest one
    /// that does is a single empty frame — the envelope delimiter.
    #[test]
    fn a_message_has_at_least_one_frame() {
        let err = Multipart::new(Vec::new()).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");

        let delimiter = Multipart::single(Message::empty());
        assert_eq!(delimiter.len(), 1);
        assert_eq!(delimiter.total_bytes(), 0);
        assert!(delimiter.frames()[0].is_empty());
    }

    /// Claim: MORE is set on every frame but the last, which is the whole of
    /// multipart on the wire.
    #[test]
    fn more_is_set_on_every_frame_but_the_last() {
        let encoded = parts(&[b"a", b"", b"ccc"]).encode();
        let mut at = 0;
        let mut flags = Vec::new();
        while at < encoded.len() {
            let (header, used) = frame::decode_header(&encoded[at..], 1024).expect("header");
            flags.push(header.kind);
            at += used + header.len as usize;
        }
        assert_eq!(
            flags,
            vec![
                FrameKind::Message { more: true },
                FrameKind::Message { more: true },
                FrameKind::Message { more: false },
            ]
        );
    }

    /// Claim: what `encode` writes, `decode` reads back — frame for frame,
    /// including an empty frame in the middle and a long frame that needs the
    /// eight-octet size field.
    #[test]
    fn encode_and_decode_round_trip() {
        let long = vec![7u8; 300];
        let message = parts(&[b"", b"header", &long]);
        let encoded = message.encode();
        match Multipart::decode(&encoded, MessageLimits::default()).expect("decode") {
            Decoded::Message {
                message: back,
                consumed,
            } => {
                assert_eq!(back, message);
                assert_eq!(consumed, encoded.len());
                assert_eq!(back.total_bytes(), 306);
            }
            other => panic!("expected a message, got {other:?}"),
        }
    }

    /// Claim: a message is delivered all or none. Every proper prefix of an
    /// encoded three-frame message decodes to `Incomplete` and consumes
    /// nothing; only the whole buffer yields the message.
    #[test]
    fn nothing_is_delivered_until_the_last_frame_arrives() {
        let message = parts(&[b"one", b"two", b"three"]);
        let encoded = message.encode();
        for cut in 0..encoded.len() {
            assert_eq!(
                Multipart::decode(&encoded[..cut], MessageLimits::default()).expect("decode"),
                Decoded::Incomplete,
                "a {cut}-byte prefix must deliver nothing"
            );
        }
        assert!(matches!(
            Multipart::decode(&encoded, MessageLimits::default()).expect("decode"),
            Decoded::Message { .. }
        ));
    }

    /// Claim: `ZMQ_MAXMSGSIZE` is judged from the **declared** length. The
    /// buffer here holds nine octets — a long header and nothing else — and
    /// the refusal still happens, which is the only order in which a 2^63-1
    /// declaration is survivable.
    #[test]
    fn the_size_bound_is_judged_before_the_body_exists() {
        let mut header = vec![0x02u8];
        header.extend_from_slice(&(i64::MAX as u64).to_be_bytes());
        assert_eq!(header.len(), 9);

        let err = Multipart::decode(&header, MessageLimits::default()).unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");
        assert!(err.cause().contains("nothing was allocated"), "{err}");
    }

    /// Claim: the bound is on the whole message, not on each frame. Three
    /// frames that each fit but together do not are refused — and the refusal
    /// arrives at the frame that crosses the line, so the earlier bodies are
    /// all that was ever held.
    #[test]
    fn the_size_bound_covers_the_whole_message() {
        let frame = vec![9u8; 100];
        let message = parts(&[&frame, &frame, &frame]);
        let encoded = message.encode();

        assert!(matches!(
            Multipart::decode(
                &encoded,
                MessageLimits::new(400, DEFAULT_MAX_MESSAGE_FRAMES)
            )
            .expect("at the budget"),
            Decoded::Message { .. }
        ));

        let err = Multipart::decode(
            &encoded,
            MessageLimits::new(250, DEFAULT_MAX_MESSAGE_FRAMES),
        )
        .unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");
    }

    /// Claim: a command is not part of a message. It is reported on its own
    /// for the session layer, and one appearing inside a multipart sequence
    /// is refused rather than swallowed.
    #[test]
    fn a_command_frame_is_not_part_of_a_message() {
        let command = frame::encode(FrameKind::Command, b"\x05READY");
        match Multipart::decode(&command, MessageLimits::default()).expect("decode") {
            Decoded::Command { body, consumed } => {
                assert_eq!(body, b"\x05READY");
                assert_eq!(consumed, command.len());
            }
            other => panic!("expected a command, got {other:?}"),
        }

        let mut mixed = frame::encode(FrameKind::Message { more: true }, b"first");
        mixed.extend_from_slice(&command);
        let err = Multipart::decode(&mixed, MessageLimits::default()).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
        assert!(err.cause().contains("command"), "{err}");
    }

    /// Claim: a decode consumes exactly one message, so a buffer holding two
    /// back to back yields them in order.
    #[test]
    fn a_decode_consumes_exactly_one_message() {
        let first = parts(&[b"a", b"b"]);
        let second = Multipart::single("c");
        let mut buffer = first.encode();
        buffer.extend_from_slice(&second.encode());

        let Decoded::Message { message, consumed } =
            Multipart::decode(&buffer, MessageLimits::default()).expect("first")
        else {
            panic!("expected the first message");
        };
        assert_eq!(message, first);

        let Decoded::Message {
            message,
            consumed: rest,
        } = Multipart::decode(&buffer[consumed..], MessageLimits::default()).expect("second")
        else {
            panic!("expected the second message");
        };
        assert_eq!(message, second);
        assert_eq!(consumed + rest, buffer.len());
    }

    /// Claim: reserved flag bits are a malformed frame, not a message — the
    /// codec's rule, surfaced in this crate's vocabulary.
    #[test]
    fn a_malformed_frame_is_einval() {
        let err = Multipart::decode(&[0xF8, 0x00], MessageLimits::default()).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
    }

    /// Claim: **a peer cannot hold a reader's memory with frames that
    /// declare nothing.** This is the review's reproduction: `01 00` — one
    /// empty frame with MORE set — repeated. Before the header octets
    /// counted, four million of them were eight megabytes of buffer against
    /// a one-mebibyte ceiling and still `Incomplete`; now the decode refuses
    /// long before that, from the accumulated declared size, and says which
    /// bound it hit.
    #[test]
    fn a_stream_of_empty_frames_is_refused_rather_than_accumulated() {
        // Enough frames to blow a small budget many times over, and far more
        // than the frame ceiling.
        let mut hostile = Vec::new();
        for _ in 0..8_192 {
            hostile.extend_from_slice(&[0x01, 0x00]);
        }

        // Against the byte budget: two octets a frame, so a 512-byte budget
        // is spent after 256 of them.
        let err = Multipart::decode(&hostile, MessageLimits::new(512, 100_000)).unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");
        assert!(err.cause().contains("512"), "{err}");

        // And against the frame ceiling, which is what bounds the re-parse
        // as well as the memory.
        let err = Multipart::decode(&hostile, MessageLimits::new(1 << 30, 64)).unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");
        assert!(err.cause().contains("64"), "{err}");

        // The default limits refuse it too, which is the case a socket runs
        // under: 8192 empty frames is under the default byte budget and over
        // the default frame ceiling, so the frame ceiling is what catches it.
        let err = Multipart::decode(&hostile, MessageLimits::default()).unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");
    }

    /// Claim: a legitimate many-frame message just under the ceiling still
    /// decodes — the bound refuses the excess, not the pattern.
    #[test]
    fn a_many_frame_message_under_the_ceiling_still_decodes() {
        let limits = MessageLimits::new(DEFAULT_MAX_MESSAGE_SIZE, 64);
        let frames: Vec<Message> = (0..64).map(|n| Message::from(format!("{n}"))).collect();
        let message = Multipart::new(frames).expect("frames");
        let encoded = message.encode();
        match Multipart::decode(&encoded, limits).expect("decode") {
            Decoded::Message { message: back, .. } => assert_eq!(back.len(), 64),
            other => panic!("expected a message, got {other:?}"),
        }

        // One more frame than the ceiling is one too many.
        let frames: Vec<Message> = (0..65).map(|n| Message::from(format!("{n}"))).collect();
        let over = Multipart::new(frames).expect("frames").encode();
        let err = Multipart::decode(&over, limits).unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");
    }
}
