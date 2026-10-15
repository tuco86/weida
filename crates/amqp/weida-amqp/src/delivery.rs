//! A delivery: one message, however many `transfer` frames it took.
//!
//! A delivery is the unit link credit is counted in and the unit a disposition
//! names, and it is *not* a frame. "A single transfer MAY span multiple frames
//! with `more=true` on all but the last" (Part 2 §2.6.14), so what arrives is
//! fragments and what the application wants is the message. The reassembly
//! itself belongs to the codec — [`weida_amqp_codec::message::Reassembly`]
//! bounds the payload by the link's `max-message-size` *before* it appends, so
//! a peer cannot hand a receiver an unbounded message one bounded frame at a
//! time.
//!
//! # The two identities
//!
//! * `delivery-id` is session-scoped and is what a `disposition` names.
//! * `delivery-tag` is up to 32 octets chosen by the sending application, and
//!   is what identifies a delivery across a link that was detached and
//!   re-attached — the unsettled map is keyed on it, not on the id.
//!
//! Both are here because settling needs the first and recovering needs the
//! second.

use bytes::Bytes;
use weida_amqp_codec::message::{Decoded, Message};
use weida_amqp_codec::{DecodeError, Limits};

/// What the sender put on the wire, reassembled.
///
/// The payload is shared, not copied: a delivery that arrived in one `transfer`
/// frame is a slice of the buffer that frame was read into, and one that took
/// several is the buffer they were gathered in. Either way it outlives the
/// frame by holding the buffer, which is why a delivery the application keeps
/// pins up to [`READ_CHUNK`](crate::transport::READ_CHUNK) octets beyond its
/// own. [`Delivery::message`] decodes it and borrows from *this*, which is why
/// it takes `&self` and hands back a value tied to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    /// The session-scoped id a `disposition` names.
    pub delivery_id: u32,
    /// The sender's tag, up to 32 octets. Empty where the sender sent none,
    /// which is legal only for a delivery it settled itself.
    pub delivery_tag: Vec<u8>,
    /// Part 3 defines exactly one format and numbers it zero.
    pub message_format: u32,
    /// Whether the sender settled the delivery as it sent it. `true` means no
    /// disposition is expected, wanted, or answerable: the sender has already
    /// forgotten it.
    pub settled: bool,
    /// Our own handle for the link it arrived on, so an application holding
    /// several can tell them apart.
    pub handle: u32,
    payload: Bytes,
}

impl Delivery {
    pub(crate) const fn new(
        delivery_id: u32,
        delivery_tag: Vec<u8>,
        message_format: u32,
        settled: bool,
        handle: u32,
        payload: Bytes,
    ) -> Self {
        Self {
            delivery_id,
            delivery_tag,
            message_format,
            settled,
            handle,
            payload,
        }
    }

    /// The message's octets: every section, in the order they arrived.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// How many octets arrived.
    #[must_use]
    pub fn len(&self) -> usize {
        self.payload.len()
    }

    /// Whether the delivery carried no payload at all, which a resumed
    /// delivery legitimately may.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    /// Decodes the sections into a message.
    ///
    /// Separate from arrival on purpose: a receiver may want the octets
    /// without paying for the decode — to forward them, to store them, to
    /// hand them to a `data` consumer that never looks at the annotations.
    pub fn message(&self) -> Result<Message<'_>, DecodeError> {
        Ok(Message::decode(&self.payload, Limits::BODY)?.message)
    }

    /// Decodes the sections and also reports the bare message's octets, which
    /// is what a signature is computed over.
    pub fn decoded(&self) -> Result<Decoded<'_>, DecodeError> {
        Message::decode(&self.payload, Limits::BODY)
    }
}

/// What a sender learned about the message it sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sent {
    /// The id this end assigned, and the one a `disposition` from the peer
    /// will name.
    pub delivery_id: u32,
    /// The tag this end chose.
    pub delivery_tag: Vec<u8>,
    /// How many `transfer` frames it took. More than one means the session
    /// window was spent more than once on this single message, which is the
    /// difference between the two credit schemes made visible.
    pub frames: usize,
    /// Whether it went out settled, in which case no disposition is coming
    /// and none is needed.
    pub settled: bool,
}
