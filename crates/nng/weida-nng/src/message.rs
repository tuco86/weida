//! The message: a protocol header and an application body, in separate
//! storage, delivered whole or not at all.
//!
//! "An `nng_msg` has separate application body and protocol header storage;
//! application data is opaque to most protocols" (`docs/research/nanomsg-nng.md`
//! §2), and "NNG delivers a message wholly or not at all; it does not expose
//! partial-message delivery or streaming bodies" (§3). Both sentences are
//! this type: a header and a body kept apart, and no API anywhere that hands
//! out half of one.
//!
//! **Why two parts rather than one with an offset.** Every protocol that
//! has a header builds it on one side and consumes it on the other: a REQ
//! pushes a request ID, a device pushes its own peer ID in front of that, a
//! PAIR v1 increments a hop count, a raw BUS reads the pipe a message came
//! in on. All of those edit the header while the body stays untouched and
//! unexamined — "application data is opaque to most protocols" — so the
//! split is where the protocols already put it, and a body handed to an
//! application never has to be re-sliced past a header it does not know
//! about.
//!
//! **What is shared and what is not.** The body is a [`Bytes`]: cloning a
//! message bumps a reference count and copies no octet, so a broadcast hands
//! every peer the same allocation instead of one deep copy each, and moving a
//! tag stack from a received body into the header is a pointer move. The
//! header is a few dozen octets at most — a default tag stack is 36 — and
//! lives inline in the message, so a request and its reply allocate their
//! bodies and nothing else.
//!
//! **On the wire there is no split.** A message is a big-endian 64-bit octet
//! count followed by exactly that many octets: header then body, with no
//! type tag, no flags and no continuation bit (§3). [`Message::encode`]
//! writes that, and [`declared_len`] reads the count — refusing an
//! over-large declaration from the size field alone, before a single body
//! octet is allocated, which is the only place `NNG_OPT_RECVMAXSZ` can be
//! enforced at all.

use bytes::Bytes;
use weida_sp::message;

use crate::error::{Error, Result};

/// Largest message this library accepts from a peer by default, in bytes.
///
/// **This default deliberately differs from NNG's**
/// ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 item
/// 5). `NNG_OPT_RECVMAXSZ` is "unlimited at zero" (§11) and SP's grammar
/// permits a declared body of 2^64-1 octets (§3), so the two together are an
/// invitation to allocate whatever a stranger asks for, and
/// `docs/INVARIANTS.md` forbids exactly that. One mebibyte is the number
/// NNG itself shipped as its default before it was relaxed, so a program
/// ported from NNG meets a familiar number rather than a novel one.
///
/// It is settable, including to [`RECV_MAX_SIZE_UNLIMITED`], and it is never
/// silent: an over-large message is `NNG_EMSGSIZE` with both numbers in the
/// message.
pub const DEFAULT_RECV_MAX_SIZE: u64 = 1024 * 1024;

/// The value NNG gives `NNG_OPT_RECVMAXSZ` to mean "no limit" (§11).
///
/// Available, because a program ported from NNG may depend on it and a
/// library that refused it would be lying about its parity. Setting it opts
/// out of the only inbound size defence SP has: from then on a peer chooses
/// the allocation.
pub const RECV_MAX_SIZE_UNLIMITED: u64 = 0;

/// Header octets a [`Message`] keeps inline, without a heap block of their
/// own.
///
/// A tag stack is four octets per tag and `NNG_OPT_MAXTTL` defaults to 8,
/// so a request that crossed one device carries at most ten tags: forty
/// octets. A longer header — a raised `MAXTTL` — is kept as a shared
/// allocation instead of being refused.
const INLINE_HEADER_BYTES: usize = 40;

/// A message's protocol header.
#[derive(Clone)]
enum Header {
    Inline {
        len: u8,
        octets: [u8; INLINE_HEADER_BYTES],
    },
    Shared(Bytes),
}

impl Header {
    const EMPTY: Header = Header::Inline {
        len: 0,
        octets: [0; INLINE_HEADER_BYTES],
    };

    fn from_slice(octets: &[u8]) -> Header {
        if octets.len() > INLINE_HEADER_BYTES {
            return Header::Shared(Bytes::copy_from_slice(octets));
        }
        let mut inline = [0u8; INLINE_HEADER_BYTES];
        inline[..octets.len()].copy_from_slice(octets);
        Header::Inline {
            len: octets.len() as u8,
            octets: inline,
        }
    }

    /// A header made of `octets` without copying them when they are too
    /// long to be kept inline.
    fn from_bytes(octets: Bytes) -> Header {
        if octets.len() > INLINE_HEADER_BYTES {
            Header::Shared(octets)
        } else {
            Header::from_slice(&octets)
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Header::Inline { len, octets } => &octets[..usize::from(*len)],
            Header::Shared(octets) => octets,
        }
    }

    fn into_bytes(self) -> Bytes {
        match self {
            Header::Inline { len, octets } => Bytes::copy_from_slice(&octets[..usize::from(len)]),
            Header::Shared(octets) => octets,
        }
    }
}

impl std::fmt::Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_slice(), f)
    }
}

/// One SP message: the protocol header the protocols write and read, and the
/// application body they do not look at.
///
/// Cloning is cheap and shares the body; see the module documentation.
#[derive(Clone, Debug)]
pub struct Message {
    header: Header,
    body: Bytes,
}

impl Message {
    /// An empty message: no header, no body.
    ///
    /// An empty message is a real message in SP — "an empty message is the
    /// eight zero octets and nothing after them" (§3) — and REQ/REP use one
    /// routinely.
    pub const fn new() -> Message {
        Message {
            header: Header::EMPTY,
            body: Bytes::new(),
        }
    }

    /// A message carrying `body` and no protocol header.
    ///
    /// A `Vec<u8>` is taken over without a copy, and so is a [`Bytes`].
    pub fn from_body(body: impl Into<Bytes>) -> Message {
        Message {
            header: Header::EMPTY,
            body: body.into(),
        }
    }

    /// A message with both halves given.
    pub fn from_parts(header: impl AsRef<[u8]>, body: impl Into<Bytes>) -> Message {
        Message {
            header: Header::from_slice(header.as_ref()),
            body: body.into(),
        }
    }

    /// The protocol header: the tag stack, the hop count, the pipe ID —
    /// whatever this protocol puts in front of the body, or nothing.
    pub fn header(&self) -> &[u8] {
        self.header.as_slice()
    }

    /// The application body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// The application body as the shared allocation it is, for a caller
    /// that wants to keep it past the message or hand it on without a copy.
    pub const fn body_bytes(&self) -> &Bytes {
        &self.body
    }

    /// Body octets. `nng_msg_len()`, which counts the body and not the
    /// header.
    pub fn len(&self) -> usize {
        self.body.len()
    }

    /// Whether the body is empty. The header may not be.
    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    /// Octets this message occupies on the wire, size field excluded.
    pub fn wire_len(&self) -> usize {
        self.header().len() + self.body.len()
    }

    /// The body alone, consuming the message.
    pub fn into_body(self) -> Bytes {
        self.body
    }

    /// Both halves, consuming the message.
    pub fn into_parts(self) -> (Bytes, Bytes) {
        (self.header.into_bytes(), self.body)
    }

    /// Rewrites the header through a scratch copy: the four word edits below
    /// are `nng_msg_header_*` for a caller, and no protocol of this library
    /// takes this path per message, so they are written for clarity.
    fn edit_header<T>(&mut self, edit: impl FnOnce(&mut Vec<u8>) -> T) -> T {
        let mut octets = self.header().to_vec();
        let out = edit(&mut octets);
        self.header = Header::from_slice(&octets);
        out
    }

    /// Appends a 32-bit word to the end of the header.
    /// `nng_msg_header_append_u32()`.
    pub fn append_header_u32(&mut self, word: u32) {
        self.edit_header(|header| header.extend_from_slice(&word.to_be_bytes()));
    }

    /// Inserts a 32-bit word at the front of the header.
    /// `nng_msg_header_insert_u32()` — what a device does with its own peer
    /// ID on the way in (§4).
    pub fn insert_header_u32(&mut self, word: u32) {
        self.edit_header(|header| {
            let mut fronted = word.to_be_bytes().to_vec();
            fronted.extend_from_slice(header);
            *header = fronted;
        });
    }

    /// Removes a 32-bit word from the front of the header.
    /// `nng_msg_header_trim_u32()` — what a device does when routing a reply
    /// back (§4). `None` when fewer than four octets remain.
    pub fn trim_header_u32(&mut self) -> Option<u32> {
        let head = self.header().first_chunk::<4>().copied()?;
        self.edit_header(|header| header.drain(..4).for_each(drop));
        Some(u32::from_be_bytes(head))
    }

    /// Removes a 32-bit word from the end of the header.
    /// `nng_msg_header_chop_u32()`.
    pub fn chop_header_u32(&mut self) -> Option<u32> {
        let tail = self.header().last_chunk::<4>().copied()?;
        self.edit_header(|header| header.truncate(header.len() - 4));
        Some(u32::from_be_bytes(tail))
    }

    /// Moves the first `len` octets of the body into the header.
    ///
    /// This is what a cooked socket does with an arriving message: the wire
    /// carries one run of octets and only the protocol knows how many of
    /// them in front are its own — four per tag for a REQ stack, four for a
    /// PAIR v1 hop count, none for PUB/SUB (§3). Doing it here rather than
    /// in each protocol means the header is always the protocol's and the
    /// body is always the application's, whichever socket type is holding
    /// the message.
    ///
    /// The body is not copied: the header octets are taken from its front
    /// and the rest stays where it is.
    ///
    /// Fails with `NNG_EPROTO` when the body is shorter than `len`: a
    /// message whose declared protocol header runs off its own end is
    /// malformed, which is the case the RFC says MUST be ignored.
    pub fn split_header_off_body(&mut self, len: usize) -> Result<()> {
        if self.body.len() < len {
            return Err(Error::EPROTO(
                format!(
                    "the protocol header claims {len} octets and the whole message is {}",
                    self.body.len()
                )
                .into(),
            ));
        }
        let moved = self.body.split_to(len);
        self.header = if self.header().is_empty() {
            Header::from_bytes(moved)
        } else {
            let mut joined = self.header().to_vec();
            joined.extend_from_slice(&moved);
            Header::from_slice(&joined)
        };
        Ok(())
    }

    /// Appends the whole wire form — the 64-bit size field, the header, the
    /// body — to `out`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.reserve(message::SIZE_LEN + self.wire_len());
        message::encode_size(self.wire_len() as u64, out);
        out.extend_from_slice(self.header());
        out.extend_from_slice(&self.body);
    }

    /// The whole wire form as its own buffer.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(message::SIZE_LEN + self.wire_len());
        self.write_to(&mut out);
        out
    }
}

impl Default for Message {
    fn default() -> Message {
        Message::new()
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Message) -> bool {
        self.header() == other.header() && self.body == other.body
    }
}

impl Eq for Message {}

/// Reads the size field of one message and judges it against
/// `max_body_bytes`.
///
/// `Ok(length)` — how many octets follow, header and body together, since
/// the wire does not tell them apart.
///
/// `Err(NNG_EMSGSIZE)` — the peer declared more than `max_body_bytes`.
/// Judged **from the size field alone**, before the body is looked at, let
/// alone reserved: SP grants no credit, a declaration may be 2^64-1 octets
/// (§3), and this is the only point at which the allocation is still ours
/// to refuse.
///
/// `max_body_bytes` of [`RECV_MAX_SIZE_UNLIMITED`] is NNG's "no limit".
pub fn declared_len(size_field: [u8; message::SIZE_LEN], max_body_bytes: u64) -> Result<usize> {
    let cap = if max_body_bytes == RECV_MAX_SIZE_UNLIMITED {
        u64::MAX
    } else {
        max_body_bytes
    };
    let too_large =
        |err: weida_sp::MessageError| Error::EMSGSIZE(format!("{err} (NNG_OPT_RECVMAXSZ)").into());
    let (declared, _) = message::decode_size(&size_field, cap).map_err(too_large)?;
    // The cap has bounded `declared`; the narrowing can only fail on a
    // 32-bit target, where a body that large could never be held anyway.
    usize::try_from(declared).map_err(|_| {
        too_large(weida_sp::MessageError::BodyTooLarge {
            len: declared,
            max: usize::MAX as u64,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the header and the body are separate storage — editing one
    /// never touches the other — and the body is what `nng_msg_len()`
    /// counts.
    #[test]
    fn the_header_and_the_body_are_separate() {
        let mut msg = Message::from_body(b"payload".to_vec());
        assert!(msg.header().is_empty());
        assert_eq!(msg.len(), 7);

        msg.append_header_u32(0x8000_0001);
        assert_eq!(msg.header(), [0x80, 0x00, 0x00, 0x01]);
        assert_eq!(msg.body(), b"payload");
        assert_eq!(msg.len(), 7, "a header is not body length");
        assert_eq!(msg.wire_len(), 11);
    }

    /// Claim: the four header edits are NNG's four, and front and back are
    /// distinct — a device pushes its peer ID at the front of a stack whose
    /// last word is the request ID, and pops it from the front again.
    #[test]
    fn header_words_go_on_and_come_off_at_both_ends() {
        let mut msg = Message::new();
        msg.append_header_u32(0x8000_0009); // the request ID, terminal
        msg.insert_header_u32(7); // a forwarder's peer ID, in front
        assert_eq!(msg.header().len(), 8);

        assert_eq!(msg.trim_header_u32(), Some(7));
        assert_eq!(msg.chop_header_u32(), Some(0x8000_0009));
        assert_eq!(msg.trim_header_u32(), None);
        assert_eq!(msg.chop_header_u32(), None);

        // A header with fewer than four octets left yields nothing rather
        // than a truncated word.
        let mut short = Message::from_parts([1, 2, 3], Vec::new());
        assert_eq!(short.trim_header_u32(), None);
        assert_eq!(short.chop_header_u32(), None);
        assert_eq!(short.header(), [1, 2, 3]);
    }

    /// Claim: claiming header octets off the body moves them, and a claim
    /// longer than the message is `NNG_EPROTO` rather than a panic or a
    /// silent short header.
    #[test]
    fn claiming_header_octets_off_the_body_moves_them() {
        let mut msg = Message::from_body(b"\x00\x00\x00\x01hello".to_vec());
        msg.split_header_off_body(4).expect("four octets");
        assert_eq!(msg.header(), [0, 0, 0, 1]);
        assert_eq!(msg.body(), b"hello");

        let mut short = Message::from_body(b"ab".to_vec());
        let err = short.split_header_off_body(4).unwrap_err();
        assert!(matches!(err, Error::EPROTO(_)), "{err:?}");
        assert_eq!(short.body(), b"ab", "a refused split changes nothing");
    }

    /// Claim: a header longer than the inline budget — a raised `MAXTTL`
    /// makes one — is kept whole rather than refused or cut, and splitting
    /// it off a body moves the same octets as splitting a short one.
    #[test]
    fn a_header_longer_than_the_inline_budget_is_kept_whole() {
        let stack: Vec<u8> = (0..=u8::MAX).collect();
        let mut wire = stack.clone();
        wire.extend_from_slice(b"tail");
        let mut msg = Message::from_body(wire);
        msg.split_header_off_body(stack.len())
            .expect("a long stack");
        assert_eq!(msg.header(), stack);
        assert_eq!(msg.body(), b"tail");
        assert_eq!(msg.wire_len(), stack.len() + 4);

        let direct = Message::from_parts(&stack, b"tail".to_vec());
        assert_eq!(direct, msg);
    }

    /// Claim: a clone shares the body instead of copying it, which is what
    /// makes a broadcast cost a reference count per peer and not a copy.
    #[test]
    fn a_clone_shares_the_body() {
        let original = Message::from_parts([0, 0, 0, 1], vec![1u8; 1024]);
        let copy = original.clone();
        assert_eq!(original.body().as_ptr(), copy.body().as_ptr());
        assert_eq!(original, copy);

        // And splitting a header off a body moves a pointer, not octets.
        let mut wire = Message::from_body(vec![7u8; 1024]);
        let before = wire.body().as_ptr();
        wire.split_header_off_body(4).expect("split");
        assert_eq!(wire.body().as_ptr(), before.wrapping_add(4));
    }

    /// Claim: the wire form is the 64-bit size followed by header then body,
    /// and the size field reads back the same octet count.
    #[test]
    fn the_wire_form_is_a_size_and_one_run_of_octets() {
        let msg = Message::from_parts(b"\x80\x00\x00\x05", b"ping".to_vec());
        let wire = msg.encode();
        assert_eq!(&wire[..8], &[0, 0, 0, 0, 0, 0, 0, 8]);
        assert_eq!(&wire[8..], b"\x80\x00\x00\x05ping");

        let mut size_field = [0u8; 8];
        size_field.copy_from_slice(&wire[..8]);
        assert_eq!(
            declared_len(size_field, DEFAULT_RECV_MAX_SIZE).expect("a legal size"),
            8
        );

        // An empty message is eight zero octets and nothing after them.
        assert_eq!(Message::new().encode(), [0u8; 8]);
    }

    /// Claim: `RECVMAXSZ` is judged from the declared 64-bit length alone.
    /// Eight octets of `0xFF` declare 2^64-1 and nothing follows them; a
    /// reader that reserved before checking would die on this input, and
    /// this one answers `NNG_EMSGSIZE` with both numbers.
    #[test]
    fn an_oversized_declaration_is_refused_before_anything_is_allocated() {
        let err = declared_len([0xFF; 8], DEFAULT_RECV_MAX_SIZE).unwrap_err();
        assert!(matches!(err, Error::EMSGSIZE(_)), "{err:?}");
        assert!(err.cause().contains(&DEFAULT_RECV_MAX_SIZE.to_string()));
        assert!(err.cause().contains("NNG_OPT_RECVMAXSZ"));

        // Exactly at the cap is accepted; one over is not.
        assert_eq!(declared_len(64u64.to_be_bytes(), 64).expect("legal"), 64);
        assert!(matches!(
            declared_len(64u64.to_be_bytes(), 63),
            Err(Error::EMSGSIZE(_))
        ));
    }

    /// Claim: NNG's own "no limit" value is available and means what it says
    /// there — the declaration is believed — which is the parity price of
    /// letting a ported program keep its configuration.
    #[test]
    fn nngs_unlimited_is_available_and_means_unlimited() {
        assert_eq!(
            declared_len(4096u64.to_be_bytes(), RECV_MAX_SIZE_UNLIMITED).expect("legal"),
            4096
        );
        // Believed, not satisfied: a size no machine could hold is still
        // refused as `NNG_EMSGSIZE` rather than narrowed.
        if usize::BITS < 64 {
            assert!(matches!(
                declared_len([0xFF; 8], RECV_MAX_SIZE_UNLIMITED),
                Err(Error::EMSGSIZE(_))
            ));
        }
    }
}
