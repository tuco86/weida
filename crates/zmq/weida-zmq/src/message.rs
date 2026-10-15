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

#[cfg(test)]
use std::cell::Cell;

use bytes::Bytes;
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
/// the wire and a `Message` costs a handle plus its bytes, so 1024 frames of
/// nothing are tens of kilobytes rather than the eight megabytes an
/// unbounded count allowed. And the count bounds the *per-message work*: a
/// whole-buffer [`Multipart::decode`] walks at most this many headers per
/// call. Every envelope any ZeroMQ pattern defines is a handful of frames —
/// the reply envelope is one delimiter plus addresses, a RADIO message is
/// two — so a legitimate message is orders of magnitude below this, and an
/// application whose own framing is not may raise it.
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
///
/// **Cloning is a reference-count bump**, not a copy: the bytes are shared
/// and immutable, so a publisher hands one frame to every subscriber, a
/// proxy copies a message to its capture socket, and a received frame can be
/// a slice of the buffer it was read into, all without moving a payload
/// byte.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct Message {
    bytes: Bytes,
}

#[cfg(test)]
thread_local! {
    /// Payload bytes copied into a frame on this thread, so that a test can
    /// tell a decoder that copies once from one that copies on every read.
    static COPIED: Cell<u64> = const { Cell::new(0) };
}

/// Payload bytes copied into frames on this thread so far.
#[cfg(test)]
pub(crate) fn copied_bytes() -> u64 {
    COPIED.with(Cell::get)
}

impl Message {
    /// An empty frame — the envelope delimiter, and a legal message of size
    /// zero.
    pub const fn empty() -> Message {
        Message {
            bytes: Bytes::new(),
        }
    }

    /// A frame holding a copy of `bytes`.
    ///
    /// The one place a received payload is copied into a frame of its own;
    /// small frames are copied rather than sliced off the receive buffer so
    /// that a queued frame of a few octets never pins the allocation it was
    /// read into.
    pub(crate) fn copy_of(bytes: &[u8]) -> Message {
        #[cfg(test)]
        COPIED.with(|copied| copied.set(copied.get() + bytes.len() as u64));
        Message {
            bytes: Bytes::copy_from_slice(bytes),
        }
    }

    /// The frame's bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// The frame's shared bytes, for a caller that wants to keep a handle on
    /// them without copying.
    pub const fn as_bytes(&self) -> &Bytes {
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

    /// Takes the bytes as a `Vec`, consuming the frame. Free when this was
    /// the only handle on a frame built from a `Vec`; a copy otherwise.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes.into()
    }

    /// Takes the shared bytes, consuming the frame.
    pub fn into_inner(self) -> Bytes {
        self.bytes
    }
}

impl From<Bytes> for Message {
    fn from(bytes: Bytes) -> Message {
        Message { bytes }
    }
}

impl From<Message> for Bytes {
    fn from(message: Message) -> Bytes {
        message.bytes
    }
}

impl From<Vec<u8>> for Message {
    /// Takes the `Vec` over without copying it.
    fn from(bytes: Vec<u8>) -> Message {
        Message {
            bytes: Bytes::from(bytes),
        }
    }
}

impl From<&[u8]> for Message {
    fn from(bytes: &[u8]) -> Message {
        Message::copy_of(bytes)
    }
}

impl<const N: usize> From<&[u8; N]> for Message {
    fn from(bytes: &[u8; N]) -> Message {
        Message::copy_of(bytes)
    }
}

impl From<String> for Message {
    /// Takes the `String`'s buffer over without copying it.
    fn from(text: String) -> Message {
        Message {
            bytes: Bytes::from(text),
        }
    }
}

impl From<&str> for Message {
    fn from(text: &str) -> Message {
        Message::copy_of(text.as_bytes())
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

    /// The octets [`Self::encode`] writes: every frame's header — two octets
    /// up to 255 bytes of body, nine beyond — and its body.
    pub fn encoded_len(&self) -> usize {
        self.frames
            .iter()
            .map(|frame| header_octets(frame.len() as u64) + frame.len())
            .sum()
    }

    /// Encodes the whole message: MORE set on every frame but the last.
    ///
    /// One buffer for the whole message, because that is what atomicity means
    /// on the sending side: "the peer SHALL queue all frames of a message in
    /// memory until the final frame is sent" (§3). A caller cannot hand half
    /// of this to a socket.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// [`Self::encode`], appended to `out`: one reservation of exactly
    /// [`Self::encoded_len`] octets, so a connection that batches many
    /// messages into one write copies each frame once and reallocates for
    /// none of them.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(self.encoded_len());
        let last = self.frames.len() - 1;
        for (index, frame) in self.frames.iter().enumerate() {
            frame::encode_header(
                FrameKind::Message {
                    more: index != last,
                },
                frame.len() as u64,
                out,
            );
            out.extend_from_slice(frame.as_slice());
        }
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
    /// **Both bounds are load-bearing.** Counting declared bodies alone
    /// bounds nothing: an empty frame declares no body, so a peer sending
    /// `01 00` — one empty frame, MORE set — forever would grow a reader's
    /// buffer by two octets per frame with `ZMQ_MAXMSGSIZE` untouched. So a
    /// frame costs its **header plus its declared body** against
    /// [`MessageLimits::max_bytes`], and the frame count has a ceiling of
    /// its own.
    ///
    /// **A call costs headers, not payload, until the message is whole.** The
    /// first pass walks the frame headers and allocates nothing; only a
    /// message that is complete is copied, once. A caller that re-decodes a
    /// growing buffer on every read — which is what a hostile peer dripping
    /// a large message one octet at a time makes it do — therefore pays at
    /// most [`MessageLimits::max_frames`] header parses per read and never a
    /// copy of a frame it has already seen. A connection does not re-decode
    /// at all: the session assembles frames as they complete.
    pub fn decode(input: &[u8], limits: MessageLimits) -> Result<Decoded<'_>> {
        let mut budget = Budget::default();
        let mut at = 0usize;
        loop {
            let remaining = budget.remaining(&limits);
            let (header, used) = match frame::decode_header(&input[at..], remaining) {
                Ok(decoded) => decoded,
                Err(FrameError::Incomplete) => return Ok(Decoded::Incomplete),
                Err(e) => return Err(frame_error(e, limits.max_bytes)),
            };
            let cost = budget.check(used, header, &limits)?;
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
                    if budget.frames > 0 {
                        return Err(command_in_multipart());
                    }
                    return Ok(Decoded::Command {
                        body: &input[body_at..end],
                        consumed: end,
                    });
                }
                FrameKind::Message { more } => {
                    budget.commit(cost, true);
                    at = end;
                    if !more {
                        return Ok(Decoded::Message {
                            message: copy_frames(&input[..at], budget.frames),
                            consumed: at,
                        });
                    }
                }
            }
        }
    }
}

/// The second pass of [`Multipart::decode`]: `input` is exactly the frames of
/// one whole message, already validated by the first, so this only copies.
fn copy_frames(input: &[u8], frames: usize) -> Multipart {
    let mut out = Vec::with_capacity(frames);
    let mut at = 0usize;
    while at < input.len() {
        // The first pass parsed these same octets under a tighter cap.
        let Ok((header, used)) = frame::decode_header(&input[at..], frame::MAX_BODY) else {
            break;
        };
        let body_at = at + used;
        let end = body_at + header.len as usize;
        out.push(Message::copy_of(&input[body_at..end]));
        at = end;
    }
    Multipart { frames: out }
}

/// The octets a frame header takes for a body of `len`: the shortest form,
/// which is the one this library writes.
pub(crate) const fn header_octets(len: u64) -> usize {
    if len > frame::MAX_SHORT_BODY { 9 } else { 2 }
}

pub(crate) fn command_in_multipart() -> Error {
    Error::EINVAL(
        "a command frame appeared inside a multipart message; MORE is zero on commands, so a \
         command is never part of one"
            .into(),
    )
}

/// What one inbound message has spent of [`MessageLimits`] so far.
///
/// Shared by the whole-buffer [`Multipart::decode`] and by a connection that
/// assembles frames as they complete, so both refuse the same messages at the
/// same header.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Budget {
    /// Octets charged: every message frame's header plus its declared body.
    held: u64,
    /// Message frames charged.
    frames: usize,
}

impl Budget {
    /// Octets the message may still add.
    pub(crate) fn remaining(&self, limits: &MessageLimits) -> u64 {
        limits.max_bytes.saturating_sub(self.held)
    }

    /// Whether `header`, which took `header_octets` on the wire, still fits:
    /// the cost of the frame, or `EMSGSIZE`. Pure, so a frame whose body has
    /// not arrived can be checked again on the next read at no charge.
    ///
    /// The header's own octets count too: this is the check that makes a
    /// stream of empty frames finite.
    pub(crate) fn check(
        &self,
        header_octets: usize,
        header: frame::FrameHeader,
        limits: &MessageLimits,
    ) -> Result<u64> {
        let remaining = self.remaining(limits);
        let cost = header_octets as u64 + header.len;
        if cost > remaining {
            return Err(Error::EMSGSIZE(
                format!(
                    "this message has reached its {}-byte budget (ZMQ_MAXMSGSIZE), counting \
                     frame headers; the next frame needs {cost} more",
                    limits.max_bytes
                )
                .into(),
            ));
        }
        if matches!(header.kind, FrameKind::Message { .. }) && self.frames >= limits.max_frames {
            return Err(Error::EMSGSIZE(
                format!(
                    "this message already has its ceiling of {} frames; 37/ZMTP sets no limit, \
                     so this one is ours",
                    limits.max_frames
                )
                .into(),
            ));
        }
        Ok(cost)
    }

    /// Charges a frame that [`Self::check`] accepted.
    pub(crate) fn commit(&mut self, cost: u64, message_frame: bool) {
        if message_frame {
            self.held += cost;
            self.frames += 1;
        }
    }
}

/// The frames of one inbound message that have arrived, held until its last
/// one has.
///
/// This is "all frames or none" for a connection that reads a stream: each
/// frame is moved in as it completes and never looked at again, which is
/// what makes receiving linear in the octets received, and a partial message
/// is bounded by the same [`Budget`] a whole one is.
#[derive(Debug, Default)]
pub(crate) struct Assembly {
    budget: Budget,
    frames: Vec<Message>,
}

impl Assembly {
    /// What the message has spent so far.
    pub(crate) const fn budget(&self) -> &Budget {
        &self.budget
    }

    /// Whether no frame of an unfinished message is held, which is the only
    /// state a command may arrive in.
    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Adds a frame that [`Budget::check`] accepted at `cost`. Returns the
    /// message when this was its last frame, leaving the assembly empty for
    /// the next one.
    pub(crate) fn push(&mut self, frame: Message, cost: u64, more: bool) -> Option<Multipart> {
        if !more && self.frames.is_empty() {
            // The common case, and one allocation of exactly one handle.
            self.budget = Budget::default();
            return Some(Multipart::single(frame));
        }
        self.budget.commit(cost, true);
        self.frames.push(frame);
        if more {
            return None;
        }
        self.budget = Budget::default();
        Some(Multipart {
            frames: std::mem::take(&mut self.frames),
        })
    }
}

/// Translates the codec's refusal into this crate's vocabulary.
pub(crate) fn frame_error(error: FrameError, max_message_bytes: u64) -> Error {
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

    /// Claim: **a peer cannot make a reader copy a frame again by sending the
    /// rest of its message slowly.** A decode of a buffer that does not yet
    /// hold the whole message reads headers only, so re-decoding a growing
    /// prefix on every read — which is what dripping one octet at a time makes
    /// a reader do — copies nothing until the last octet arrives, and the
    /// whole message is then copied once.
    #[test]
    fn a_partial_message_is_never_copied() {
        let limits = MessageLimits::new(2 * 1024 * 1024, DEFAULT_MAX_MESSAGE_FRAMES);
        let first = vec![1u8; 512 * 1024];
        let second = vec![2u8; 400 * 1024];
        let encoded = Multipart::new(vec![
            Message::from(first.clone()),
            Message::from(second.clone()),
        ])
        .expect("frames")
        .encode();

        let baseline = copied_bytes();
        // From the moment the first frame is whole, every prefix is one more
        // read of the second.
        let drip_from = 9 + first.len();
        for cut in (drip_from..encoded.len()).step_by(97) {
            assert_eq!(
                Multipart::decode(&encoded[..cut], limits).expect("decode"),
                Decoded::Incomplete,
                "a {cut}-byte prefix must deliver nothing"
            );
        }
        assert_eq!(
            copied_bytes(),
            baseline,
            "decoding a partial message copied payload"
        );

        match Multipart::decode(&encoded, limits).expect("decode") {
            Decoded::Message { message, consumed } => {
                assert_eq!(consumed, encoded.len());
                assert_eq!(message.total_bytes(), (first.len() + second.len()) as u64);
            }
            other => panic!("expected a message, got {other:?}"),
        }
        assert_eq!(
            copied_bytes() - baseline,
            (first.len() + second.len()) as u64,
            "a whole message is copied exactly once"
        );
    }

    /// Claim: a clone shares the payload rather than copying it, which is what
    /// lets a publisher hand one message to every subscriber, and a `Vec` or a
    /// `String` becomes a frame without being copied at all.
    #[test]
    fn cloning_a_message_copies_no_payload() {
        let message = Multipart::new(vec![Message::from(vec![7u8; 4096]), Message::from("tail")])
            .expect("frames");
        let copy = message.clone();
        for (original, cloned) in message.frames().iter().zip(copy.frames()) {
            assert_eq!(
                original.as_slice().as_ptr(),
                cloned.as_slice().as_ptr(),
                "a clone must share the bytes"
            );
        }
        assert_eq!(copy, message);

        let bytes = vec![3u8; 1000];
        let at = bytes.as_ptr();
        let frame = Message::from(bytes);
        assert_eq!(
            frame.as_slice().as_ptr(),
            at,
            "a Vec is taken over, not copied"
        );
        assert_eq!(frame.into_bytes().len(), 1000);
    }

    /// Claim: `encoded_len` is what `encode` writes — two octets of header up
    /// to 255 bytes of body and nine beyond — and `encode_into` appends.
    #[test]
    fn encoded_len_is_what_encode_writes() {
        for frames in [vec![0usize], vec![255], vec![256], vec![0, 300, 1, 70_000]] {
            let message = Multipart::new(
                frames
                    .iter()
                    .map(|len| Message::from(vec![5u8; *len]))
                    .collect(),
            )
            .expect("frames");
            assert_eq!(message.encoded_len(), message.encode().len(), "{frames:?}");
        }

        let message = parts(&[b"abc", b"de"]);
        let mut out = b"prefix".to_vec();
        message.encode_into(&mut out);
        assert_eq!(&out[..6], b"prefix");
        assert_eq!(&out[6..], message.encode().as_slice());
    }

    /// Claim: a connection that assembles frames as they arrive holds them
    /// until the last one — all or none — refuses the frame that crosses the
    /// budget from its header alone, and starts over for the next message.
    #[test]
    fn an_assembly_delivers_all_frames_or_none() {
        let limits = MessageLimits::new(250, DEFAULT_MAX_MESSAGE_FRAMES);
        let mut assembly = Assembly::default();
        let message_header = |more, len| frame::FrameHeader {
            kind: FrameKind::Message { more },
            len,
        };

        let cost = assembly
            .budget()
            .check(2, message_header(true, 100), &limits)
            .expect("fits");
        assert!(assembly.push(Message::from("one"), cost, true).is_none());
        assert!(!assembly.is_empty());

        // 102 octets are spent; the next 148 fit and 149 do not.
        assert!(
            assembly
                .budget()
                .check(2, message_header(true, 146), &limits)
                .is_ok()
        );
        let err = assembly
            .budget()
            .check(2, message_header(true, 147), &limits)
            .unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");

        let cost = assembly
            .budget()
            .check(2, message_header(false, 10), &limits)
            .expect("fits");
        let done = assembly
            .push(Message::from("two"), cost, false)
            .expect("the last frame completes the message");
        assert_eq!(done.len(), 2);
        assert!(assembly.is_empty());
        assert_eq!(assembly.budget().remaining(&limits), 250, "a fresh budget");

        // One frame, which is most of what a socket receives.
        let single = assembly
            .push(Message::from("solo"), 6, false)
            .expect("a lone frame is a message");
        assert_eq!(single.frames()[0].as_slice(), b"solo");
    }
}
