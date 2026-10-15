//! A message as an application sees it, and the owned form of the
//! `NATS/1.0` header block.
//!
//! `weida-nats-codec`'s `Op` and `Headers` borrow from the buffer they were
//! decoded from, which is exactly right for a codec and exactly wrong for a
//! message that travels down a channel to another task. A [`Message`] gets
//! around that without copying: its `subject`, `reply_to` and `payload` are
//! [`Bytes`] — reference-counted slices of the buffer the server's octets were
//! read into — so receiving a message allocates nothing per message and moves
//! no payload. The header block is the one part that is copied out, because
//! its entries are text the application compares and the block is small.
//!
//! # What a slice keeps alive
//!
//! A [`Bytes`] slice keeps the buffer chunk it was cut from alive, however
//! little of it the slice covers. A message that is held for a long time holds
//! up to one read chunk (16 KiB) of memory its neighbours have long released;
//! an application that keeps small payloads around for good should copy them
//! (`Bytes::copy_from_slice`) rather than keep the message. While a message is
//! waiting for its reader the connection accounts for this: every queued
//! message is charged at least a quarter chunk against
//! [`ConnectionOptions::max_queued_bytes`](crate::options::ConnectionOptions::max_queued_bytes).
//!
//! # Octets, not text
//!
//! The payload is opaque by definition: "the protocol assigns no payload
//! schema or content type" (`docs/research/nats.md` §3). The subject and the
//! reply subject are remote input, and a client that refused a non-UTF-8
//! subject would refuse a message a conforming server delivered — the same
//! reason the codec keeps them as octets. [`Message::subject_str`] and
//! [`Message::reply_to_str`] are there for the ordinary case where they are
//! text.
//!
//! Header names and values are text, because ADR-4's grammar is HTTP-like and
//! a client compares them.

use std::fmt;
use std::ops::Range;

use bytes::Bytes;
use weida_nats_codec::Headers;

use crate::queue::PER_MESSAGE_OVERHEAD;

/// The `NATS/1.0` status a server sends when a request found no responder.
///
/// "A request with no responders can receive a fast no-responder status when
/// the client enables `no_responders` and headers"
/// (`docs/research/nats.md` §4).
pub const NO_RESPONDERS: u16 = 503;

/// One delivered message.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Message {
    /// "Subject name this message was received on" — the literal subject the
    /// publisher used, not the pattern that matched it.
    pub subject: Bytes,
    /// The `sid` of the subscription this copy was delivered to.
    ///
    /// Kept because it is the one field that says *which* subscription
    /// received it, and two subscriptions on this connection can match the
    /// same publication and each get their own copy.
    pub sid: u64,
    /// "The subject on which the publisher is listening for responses", where
    /// there was one. This is what a responder publishes its answer to, and
    /// it is the whole of request-reply's correlation.
    pub reply_to: Option<Bytes>,
    /// The header block, where the message arrived as `HMSG`.
    pub headers: Option<OwnedHeaders>,
    /// The payload, exactly as long as the control line declared. May be
    /// empty, and may hold `CRLF`.
    pub payload: Bytes,
}

impl Message {
    /// The subject as text, where it is UTF-8.
    #[must_use]
    pub fn subject_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.subject).ok()
    }

    /// The reply subject as text, where there is one and it is UTF-8.
    #[must_use]
    pub fn reply_to_str(&self) -> Option<&str> {
        self.reply_to
            .as_deref()
            .and_then(|r| std::str::from_utf8(r).ok())
    }

    /// The `NATS/1.0` status, where the block carried one.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        self.headers.as_ref().and_then(|headers| headers.status)
    }

    /// Whether this is the no-responder answer to a request.
    ///
    /// A `503` with no payload, which is what the server sends the instant it
    /// finds no interest in a request's subject. Distinguished from a
    /// responder that happened to answer with a 503-shaped block by nothing
    /// at all — the protocol offers no distinction, and none is invented
    /// here.
    #[must_use]
    pub fn is_no_responders(&self) -> bool {
        self.status() == Some(NO_RESPONDERS)
    }

    /// What this message costs while it waits for its reader: its octets plus
    /// [`PER_MESSAGE_OVERHEAD`], so that a flood of empty messages is bounded
    /// by the same budget as a flood of large ones.
    pub(crate) fn queued_len(&self) -> usize {
        PER_MESSAGE_OVERHEAD
            .saturating_add(self.subject.len())
            .saturating_add(self.reply_to.as_ref().map_or(0, Bytes::len))
            .saturating_add(self.headers.as_ref().map_or(0, OwnedHeaders::byte_len))
            .saturating_add(self.payload.len())
    }
}

/// A `NATS/1.0` header block that owns its names and values.
///
/// A sequence of pairs rather than a map, for the two reasons the codec gives:
/// names may repeat with different values ("including supporting multi-value
/// headers") and case is preserved between publisher and receiver. A map
/// would have to fold one of the two away.
///
/// Every name and value lives in **one** string, back to back, with the pairs
/// kept as offsets into it. A block of `n` entries is therefore two
/// allocations however large `n` is, where a `String` per name and per value
/// would be `2n + 1`, and a message may carry `max_header_entries` of them.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct OwnedHeaders {
    /// The three-digit status after `NATS/1.0`, where the version line
    /// carried one.
    pub status: Option<u16>,
    /// The text after the status, where there was one.
    pub description: Option<String>,
    /// Every name and value, in order, with nothing between them.
    text: String,
    /// Where each entry's name and value sit in `text`.
    entries: Vec<(Range<usize>, Range<usize>)>,
}

impl OwnedHeaders {
    /// An empty block.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            status: None,
            description: None,
            text: String::new(),
            entries: Vec::new(),
        }
    }

    /// Append an entry, keeping its case and its position.
    pub fn push(&mut self, name: impl AsRef<str>, value: impl AsRef<str>) {
        let name_at = self.text.len();
        self.text.push_str(name.as_ref());
        let value_at = self.text.len();
        self.text.push_str(value.as_ref());
        self.entries
            .push((name_at..value_at, value_at..self.text.len()));
    }

    /// Every entry, in order, duplicates kept.
    pub fn entries(&self) -> impl ExactSizeIterator<Item = (&str, &str)> + '_ {
        self.entries.iter().map(|(name, value)| {
            (
                &self.text[name.start..name.end],
                &self.text[value.start..value.end],
            )
        })
    }

    /// Whether the block holds no entries. A block with a status and no
    /// entries is the ordinary shape of a no-responder message.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How many entries the block holds, duplicates counted separately.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The first value stored under exactly this name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries()
            .find(|(stored, _)| *stored == name)
            .map(|(_, value)| value)
    }

    /// The first value whose name matches ignoring ASCII case.
    ///
    /// The protocol promises case is *preserved*, not that it is significant,
    /// so a client reading a block another language's client wrote may well
    /// have to ignore it. What must not happen is the storage folding it.
    #[must_use]
    pub fn get_ignore_ascii_case(&self, name: &str) -> Option<&str> {
        self.entries()
            .find(|(stored, _)| stored.eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
    }

    /// Copies a decoded block out of the read buffer it borrows from.
    #[must_use]
    pub fn from_borrowed(headers: &Headers<'_>) -> Self {
        let size = headers
            .entries()
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum();
        let mut block = Self {
            status: headers.status,
            description: headers.description.map(str::to_owned),
            text: String::with_capacity(size),
            entries: Vec::with_capacity(headers.len()),
        };
        for (name, value) in headers.entries() {
            block.push(name, value);
        }
        block
    }

    /// The borrowed form, for putting this block back on the wire.
    #[must_use]
    pub fn as_borrowed(&self) -> Headers<'_> {
        let mut headers = Headers::new();
        headers.status = self.status;
        headers.description = self.description.as_deref();
        for (name, value) in self.entries() {
            headers.push(name, value);
        }
        headers
    }

    /// The octets of text this block holds, for what it costs while queued.
    pub(crate) fn byte_len(&self) -> usize {
        self.text.len() + self.description.as_ref().map_or(0, String::len)
    }
}

impl fmt::Debug for OwnedHeaders {
    /// The entries as pairs: the offsets are how they are stored, not what
    /// they are.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedHeaders")
            .field("status", &self.status)
            .field("description", &self.description)
            .field("entries", &self.entries().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_nats_codec::Limits;

    /// The round trip is what makes `as_borrowed` trustworthy: a block copied
    /// out of a decode and written back must produce the octets it came from,
    /// repeated names and case included.
    #[test]
    fn a_block_survives_the_copy_out_and_back() {
        let wire = b"NATS/1.0\r\nFoodGroup: vegetable\r\nfoodgroup: fruit\r\n\r\n";
        let decoded = Headers::decode(wire, Limits::DEFAULT).expect("a block");
        let owned = OwnedHeaders::from_borrowed(&decoded);
        assert_eq!(owned.len(), 2, "a repeated name is two entries");
        assert_eq!(owned.get("FoodGroup"), Some("vegetable"));
        assert_eq!(
            owned.get("foodgroup"),
            Some("fruit"),
            "case is significant to `get`, because it is preserved on the wire"
        );
        assert_eq!(owned.get_ignore_ascii_case("FOODGROUP"), Some("vegetable"));

        let mut out = Vec::new();
        owned.as_borrowed().encode(&mut out).expect("writable");
        assert_eq!(out, wire);
    }

    /// Entries keep their order and their duplicates, and a name that is the
    /// tail of another is not confused with it: they share one string.
    #[test]
    fn entries_keep_order_and_duplicates_in_one_string() {
        let mut block = OwnedHeaders::new();
        block.push("Link", "a");
        block.push("Link", "b");
        block.push("k", "Link");
        block.push("", "");
        assert_eq!(
            block.entries().collect::<Vec<_>>(),
            [("Link", "a"), ("Link", "b"), ("k", "Link"), ("", "")]
        );
        assert_eq!(block.get("Link"), Some("a"), "the first of a repeated name");
        assert_eq!(block.get("k"), Some("Link"));
        assert_eq!(block.len(), 4);
    }

    /// A block built by hand and one copied out of a decode are the same
    /// block when they hold the same entries.
    #[test]
    fn a_built_block_equals_a_decoded_one() {
        let decoded = Headers::decode(
            b"NATS/1.0 100 Idle Heartbeat\r\nA: 1\r\nB: 2\r\n\r\n",
            Limits::DEFAULT,
        )
        .expect("a block");
        let mut built = OwnedHeaders::new();
        built.status = Some(100);
        built.description = Some("Idle Heartbeat".to_owned());
        built.push("A", "1");
        built.push("B", "2");
        assert_eq!(OwnedHeaders::from_borrowed(&decoded), built);
    }

    /// The no-responder answer is a status and nothing else, and that is what
    /// `is_no_responders` keys on.
    #[test]
    fn the_no_responder_answer_is_a_status_with_no_entries() {
        let decoded = Headers::decode(b"NATS/1.0 503\r\n\r\n", Limits::DEFAULT).expect("a block");
        let message = Message {
            headers: Some(OwnedHeaders::from_borrowed(&decoded)),
            ..Message::default()
        };
        assert_eq!(message.status(), Some(NO_RESPONDERS));
        assert!(message.is_no_responders());
        assert!(message.headers.as_ref().expect("headers").is_empty());
        assert!(!Message::default().is_no_responders());
    }

    /// What a queued message costs is its octets plus the floor, so an empty
    /// message is not free.
    #[test]
    fn an_empty_message_still_costs_the_floor() {
        assert_eq!(Message::default().queued_len(), PER_MESSAGE_OVERHEAD);
        let message = Message {
            subject: Bytes::from_static(b"a.b"),
            reply_to: Some(Bytes::from_static(b"_INBOX.x")),
            payload: Bytes::from_static(b"12345"),
            ..Message::default()
        };
        assert_eq!(message.queued_len(), PER_MESSAGE_OVERHEAD + 3 + 8 + 5);
    }
}
