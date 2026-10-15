//! What an application publishes, what it receives, and what a completion
//! actually certifies.
//!
//! # The completion vocabulary, and why it is four values
//!
//! MQTT's delivery protocol is "concerned solely with the delivery of an
//! application message from a single sender to a single receiver" and there is
//! "no end-to-end guarantee from publisher to subscriber" (4.3) [mqtt5 §6]. So
//! [`Completion`] says what the *hop* certified and nothing more, and it has a
//! distinct value per QoS because the three levels certify genuinely different
//! things:
//!
//! * [`Completion::Sent`] — QoS 0. "The message arrives at the receiver either
//!   once or not at all" (4.3.1) [mqtt5 §6]; there is no response, no retry
//!   and no stored state, so the strongest true statement is that the bytes
//!   were handed to the transport.
//! * [`Completion::Acknowledged`] — QoS 1's PUBACK: "transfer of ownership for
//!   that hop, nothing else. Not that a subscriber received the message, not
//!   that it was persisted, not that it was processed" [mqtt5 §6]. The
//!   receiver "does not need to complete delivery of the Application Message
//!   before sending the PUBACK".
//! * [`Completion::Refused`] — a PUBREC of 0x80 or above. The message "counts
//!   as acknowledged and MUST NOT be retransmitted" ([MQTT-4.4.0-2])
//!   [mqtt5 §6]: it is dead with no protocol recourse, which is a different
//!   outcome from either success or a retry and deserves its own value.
//! * [`Completion::Complete`] — QoS 2's PUBCOMP: "publication of QoS 2 message
//!   is complete" (3.7.2.1), meaning the handshake terminated and the
//!   identifier is released — "not that any subscriber saw anything"
//!   [mqtt5 §6].
//!
//! **Durability is certified by none of them.** "No acknowledgement certifies
//! durability": the specification puts the choice on the solution developer
//! and 4.1.1 explicitly contemplates loss [mqtt5 §12/P7]. That is why nothing
//! here is named `Stored`.
//!
//! # Success codes that look like failures
//!
//! `0x10 No matching subscribers` is a **success**, and it is the only
//! in-protocol signal that a message reached nobody — the server "MAY use this
//! Reason Code instead of 0x00" [mqtt5 §12/P16]. [`Completion::is_error`] is
//! the 0x80 comparison (2.4) and nothing more, so a caller that wants to know
//! about 0x10 asks for the code.

use std::time::Duration;

use bytes::Bytes;
use weida_mqtt_codec::{
    PayloadFormat, Properties, PubackReasonCode, PubcompReasonCode, Publish, PubrecReasonCode, QoS,
};

use crate::error::Result;
use crate::limits::{Limits, ServerLimits};
use crate::session::StoredPublish;

/// An Application Message to publish.
///
/// Owned, because a QoS > 0 message outlives the call that sent it: it becomes
/// session state until the exchange completes, and may have to be resent on a
/// later connection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Message {
    /// Topic Name. Matched "character for character with no normalization"
    /// ([MQTT-4.7.3-4]) [mqtt5 §3], and never a filter — no `+` or `#`.
    pub topic: String,
    /// The payload. Opaque bytes; zero length is valid (3.3.3), and zero
    /// length with RETAIN is the delete of [MQTT-3.3.1-6].
    ///
    /// `Bytes`, so that the copies a message goes through — a clone to publish
    /// it, the stored shape a QoS > 0 message becomes, a resend on a later
    /// connection — share one allocation instead of each copying the payload.
    pub payload: Bytes,
    /// Quality of Service. Refused above the server's `Maximum QoS`
    /// (3.2.2.3.4) before the packet reaches the wire.
    pub qos: QoS,
    /// RETAIN. Refused where the server declared `Retain Available` 0
    /// (3.2.2.3.5).
    pub retain: bool,
    /// `Payload Format Indicator` (3.3.2.3.1).
    pub payload_format_indicator: Option<PayloadFormat>,
    /// `Message Expiry Interval` (3.3.2.3.3): how long the server holds an
    /// undelivered copy.
    pub message_expiry: Option<Duration>,
    /// `Content Type` (3.3.2.3.9). "MQTT performs no validation of the string"
    /// beyond UTF-8 well-formedness [mqtt5 §3].
    pub content_type: Option<String>,
    /// `Response Topic` (3.3.2.3.5), for request/response (4.10).
    pub response_topic: Option<String>,
    /// `Correlation Data` (3.3.2.3.6), copied into the reply by the responder.
    pub correlation_data: Option<Vec<u8>>,
    /// `User Property` pairs, which the server MUST forward unaltered and in
    /// order ([MQTT-3.3.2-17]) [mqtt5 §3].
    pub user_properties: Vec<(String, String)>,
}

impl Message {
    /// A QoS 0 message on `topic`.
    ///
    /// `payload` is anything that is `Bytes` already or becomes it without a
    /// copy: a `Vec<u8>`, a `String`, a `&'static str` or `&'static [u8]`, or
    /// a `Bytes` itself. A borrowed slice that is not `'static` is
    /// `Bytes::copy_from_slice`d by the caller, once, where the copy is
    /// visible.
    #[must_use]
    pub fn new(topic: impl Into<String>, payload: impl Into<Bytes>) -> Message {
        Message {
            topic: topic.into(),
            payload: payload.into(),
            ..Message::default()
        }
    }

    /// The same at `qos`.
    #[must_use]
    pub fn at(mut self, qos: QoS) -> Message {
        self.qos = qos;
        self
    }

    /// The same with RETAIN set.
    #[must_use]
    pub fn retained(mut self) -> Message {
        self.retain = true;
        self
    }

    /// The **delete** of a topic's retained message: RETAIN 1 and a
    /// zero-length payload.
    ///
    /// "A PUBLISH with RETAIN 1 and a zero-byte payload removes the retained
    /// message for that topic, is delivered to current subscribers as a
    /// normal message, and **is itself not stored**"
    /// ([MQTT-3.3.1-6], [MQTT-3.3.1-7], [MQTT-3.3.1-10], [MQTT-3.3.1-11])
    /// (3.3.1.3) [mqtt5 §8]. So the delete is a message and not an
    /// operation: current subscribers see an empty payload arrive, and a
    /// later subscriber sees nothing at all.
    ///
    /// It exists as its own constructor because `Message::new(topic, "")`
    /// `.retained()` reads like an oversight and this reads like the delete
    /// it is. The QoS is the caller's - a delete at QoS 0 "MAY be discarded
    /// at any time" like any other QoS 0 retained message (3.3.1.3), so a
    /// delete that must happen is a delete at QoS 1 or above.
    #[must_use]
    pub fn delete_retained(topic: impl Into<String>) -> Message {
        Message {
            topic: topic.into(),
            payload: Bytes::new(),
            retain: true,
            ..Message::default()
        }
    }

    /// Refuses what the server said it will not accept, **before the packet
    /// reaches the wire**.
    ///
    /// Two of the five availability flags apply to a publish, and each has its
    /// own Protocol Error code: `Maximum QoS` is 0x9B and `Retain Available`
    /// is 0x9A [mqtt5 §11].
    ///
    /// # Errors
    ///
    /// [`crate::Error::Unavailable`] carrying the code the server would have
    /// sent, and [`crate::Error::Configuration`] for a `Message Expiry Interval` a Four
    /// Byte Integer cannot carry.
    pub fn check(&self, limits: &ServerLimits) -> Result<()> {
        // A Topic Name and never a filter ([MQTT-3.3.2-2]). The zero-length
        // case is legal only with an established Topic Alias (3.3.2.1),
        // which is the alias table's to decide, so it is refused here.
        crate::filter::check_topic_name(&self.topic, false)?;
        limits.require(crate::error::Feature::Qos(self.qos))?;
        if self.retain {
            limits.require(crate::error::Feature::Retain)?;
        }
        crate::options::interval_seconds("message_expiry", self.message_expiry)?;
        Ok(())
    }

    /// The stored shape a QoS > 0 message becomes: session state until the
    /// exchange completes.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Configuration`] for an interval a Four Byte Integer cannot
    /// carry.
    pub fn into_stored(self) -> Result<StoredPublish> {
        let message_expiry_interval =
            crate::options::interval_seconds("message_expiry", self.message_expiry)?;
        Ok(StoredPublish {
            topic: self.topic,
            payload: self.payload,
            qos: self.qos,
            retain: self.retain,
            payload_format_indicator: self.payload_format_indicator,
            message_expiry_interval,
            content_type: self.content_type,
            response_topic: self.response_topic,
            correlation_data: self.correlation_data,
            user_properties: self.user_properties,
        })
    }
}

/// What a hop certified.
///
/// See the module documentation for what each value does and does not mean.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Completion {
    /// QoS 0: the bytes were handed to the transport. Nothing certifies more,
    /// because QoS 0 has no response (4.3.1) [mqtt5 §6].
    Sent,
    /// QoS 1: the PUBACK's reason code — ownership transferred for this hop.
    Acknowledged(PubackReasonCode),
    /// QoS 2 ended by a PUBREC of 0x80 or above: the message "MUST NOT be
    /// retransmitted" ([MQTT-4.4.0-2]) and is dead with no protocol recourse.
    Refused(PubrecReasonCode),
    /// QoS 2: the PUBCOMP's reason code — the handshake terminated and the
    /// identifier is released.
    Complete(PubcompReasonCode),
}

impl Completion {
    /// Whether the hop reported a failure: the 0x80 line of 2.4 [mqtt5 §1].
    ///
    /// `0x10 No matching subscribers` is therefore **not** an error: it is the
    /// only in-protocol signal that a message reached nobody, and it is a
    /// success code [mqtt5 §12/P16].
    #[must_use]
    pub const fn is_error(self) -> bool {
        match self {
            Completion::Sent => false,
            Completion::Acknowledged(code) => code.is_error(),
            Completion::Refused(code) => code.is_error(),
            Completion::Complete(code) => code.is_error(),
        }
    }

    /// The reason code, where the hop sent one.
    #[must_use]
    pub const fn reason_code(self) -> Option<u8> {
        match self {
            Completion::Sent => None,
            Completion::Acknowledged(code) | Completion::Refused(code) => Some(code.as_byte()),
            Completion::Complete(code) => Some(code.as_byte()),
        }
    }
}

/// The properties of a received message, owned.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeliveryProperties {
    /// `Payload Format Indicator`.
    pub payload_format_indicator: Option<PayloadFormat>,
    /// `Message Expiry Interval`, which the server rewrites downwards for
    /// waiting time ([MQTT-3.3.2-6]) [mqtt5 §3], so this is what is left.
    pub message_expiry_interval: Option<u32>,
    /// `Content Type`.
    pub content_type: Option<String>,
    /// `Response Topic`.
    pub response_topic: Option<String>,
    /// `Correlation Data`.
    pub correlation_data: Option<Vec<u8>>,
    /// `User Property` pairs, in the order the publisher sent them.
    ///
    /// `User Property` "is allowed to appear multiple times" (3.3.2.3.7)
    /// [mqtt5 §3] and the protocol puts **no ceiling** on how many one
    /// delivery may carry, so [`Limits::max_user_properties`] does: past it
    /// the first pairs are kept and [`DeliveryProperties::truncated`] says so.
    pub user_properties: Vec<(String, String)>,
    /// The Subscription Identifiers this delivery was caused by.
    ///
    /// "A single copy carries all matching identifiers ([MQTT-3.3.4-4]);
    /// multiple copies one each ([MQTT-3.3.4-5])" [mqtt5 §4.1], which is how a
    /// client tells which of its own subscriptions produced a message. The
    /// protocol puts **no ceiling** on how many one delivery may carry, so
    /// [`Limits::max_subscription_identifiers`] does: past it the first are
    /// kept and [`DeliveryProperties::truncated`] says so.
    pub subscription_identifiers: Vec<u32>,
    /// Whether this delivery carried more `User Property` pairs or
    /// Subscription Identifiers than this client keeps, so that the two lists
    /// above hold the first of them and not all.
    ///
    /// Everything else about the delivery is as sent. A caller that routes on
    /// a property that might be past the cut checks this before concluding
    /// that the property is absent.
    pub truncated: bool,
}

impl DeliveryProperties {
    /// Reads a PUBLISH's properties, bounded.
    ///
    /// **The bound is on what is kept, and it is a verdict on the list and not
    /// on the delivery.** At most [`Limits::max_subscription_identifiers`]
    /// identifiers and [`Limits::max_user_properties`] pairs are copied out,
    /// in order, and [`DeliveryProperties::truncated`] records that more were
    /// there. The protocol has neither ceiling: the only limit on the wire is
    /// the packet size, so at a 268 MB `Maximum Packet Size` a single delivery
    /// could carry tens of millions of identifiers, or — a pair's minimal form
    /// being five octets — tens of millions of pairs. But any publisher that
    /// can reach a subscribed topic can send such a message, and the broker
    /// must forward its User Properties unaltered ([MQTT-3.3.2-17]); refusing
    /// the connection over one would let every publisher disconnect every
    /// subscriber, so the delivery is made and the excess is left unread.
    #[must_use]
    pub fn read(properties: &Properties<'_>, limits: &Limits) -> DeliveryProperties {
        let mut identifiers = properties.subscription_identifiers();
        let subscription_identifiers: Vec<u32> = identifiers
            .by_ref()
            .take(limits.max_subscription_identifiers)
            .collect();
        let mut pairs = properties.user_properties();
        let user_properties: Vec<(String, String)> = pairs
            .by_ref()
            .take(limits.max_user_properties)
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        // One more step of each walk answers whether anything was left, and
        // is the whole cost of knowing.
        let truncated = identifiers.next().is_some() || pairs.next().is_some();
        DeliveryProperties {
            payload_format_indicator: properties.payload_format_indicator,
            message_expiry_interval: properties.message_expiry_interval,
            content_type: properties.content_type.map(str::to_owned),
            response_topic: properties.response_topic.map(str::to_owned),
            correlation_data: properties.correlation_data.map(<[u8]>::to_vec),
            user_properties,
            subscription_identifiers,
            truncated,
        }
    }
}

/// Where a delivery came from, as far as the protocol can say.
///
/// The third value is not a gap in this library: it is the protocol's, and
/// naming it is the difference between reporting what happened and guessing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RetainedOrigin {
    /// The server's retained-message cache: one message per exact Topic Name,
    /// sent because a subscription was made (3.3.1.3) [mqtt5 §8].
    ///
    /// **A last-value cache and not a replay log**: no history, no
    /// snapshot-plus-delta, no offset, no "last N messages". A late joiner
    /// needing more must have subscribed with Clean Start 0 and a non-zero
    /// Session Expiry Interval *before* the messages were published, so the
    /// server queued them (4.1) [mqtt5 §8].
    Retained,
    /// Forwarded live from a publisher while this subscription was already in
    /// place.
    Live,
    /// The subscription set Retain As Published, so RETAIN reports the
    /// publisher's flag rather than this copy's origin ([MQTT-3.3.1-13]).
    /// Nothing in the packet distinguishes the two cases.
    Unknowable,
}

/// A message the server delivered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    /// Topic Name.
    pub topic: String,
    /// The payload, copied once out of the receive buffer.
    ///
    /// The packet is decoded where it was read, so this is the only copy a
    /// payload costs after the one the operating system makes into that
    /// buffer — and an owned one, so that holding a delivery pins nothing of
    /// the connection's.
    pub payload: Vec<u8>,
    /// The QoS it was **delivered** at, which need not be the QoS it was
    /// published at: "the QoS level used to deliver an Application Message
    /// outbound to the Client could differ from that of the inbound
    /// Application Message" (4.3), and the rule is the minimum of the two
    /// ([MQTT-3.8.4-8]) [mqtt5 §6]. Downgraded, never upgraded.
    pub qos: QoS,
    /// RETAIN as the client sees it, which under `Retain As Published` 0 is
    /// cleared by the server ([MQTT-3.3.1-12]) and under 1 is the flag as
    /// published ([MQTT-3.3.1-13]) [mqtt5 §4.4].
    pub retain: bool,
    /// DUP. Useless for deduplication and known to be: it is not propagated,
    /// a receiver of 1 "cannot assume that it has seen an earlier copy", and
    /// the same message can arrive twice with DUP 0 under different
    /// identifiers [mqtt5 §6]. Reported because it is on the wire, not
    /// because it means anything.
    pub dup: bool,
    /// The Packet Identifier, present only at QoS 1 and 2 (3.3.2.2).
    pub packet_id: Option<u16>,
    /// The properties, bounded.
    pub properties: DeliveryProperties,
}

impl Delivery {
    /// Reads a PUBLISH.
    ///
    /// `topic` is the Topic Name the delivery is about, which is not always
    /// the one the PUBLISH carried: a Topic Alias is resolved before an
    /// application sees one. The payload is copied out of the packet.
    ///
    /// The properties are bounded as [`DeliveryProperties::read`] describes.
    #[must_use]
    pub fn read(publish: &Publish<'_>, topic: String, limits: &Limits) -> Delivery {
        Delivery {
            topic,
            payload: publish.payload.to_vec(),
            qos: publish.qos,
            retain: publish.retain,
            dup: publish.dup,
            packet_id: publish.packet_id,
            properties: DeliveryProperties::read(&publish.properties, limits),
        }
    }

    /// Whether this delivery came out of the server's retained-message cache
    /// or off the wire live - and where the protocol makes that unanswerable,
    /// says so.
    ///
    /// `retain_as_published` is the option of the subscription that matched,
    /// which is what decides whether [`Delivery::retain`] is an answer:
    ///
    /// * **0** (the default): the server sets RETAIN 1 only on a message it
    ///   sends "as a result of a new subscription being made"
    ///   ([MQTT-3.3.1-8]) and clears it on every forwarded live message
    ///   ([MQTT-3.3.1-12]) [mqtt5 §8]. So the flag *is* the verdict.
    /// * **1**: the flag is forwarded "as published" ([MQTT-3.3.1-13]), so it
    ///   reports what the publisher asked the server to store and says
    ///   nothing about where this copy came from. A live message published
    ///   with RETAIN 1 and a cached one are then indistinguishable, which is
    ///   [`RetainedOrigin::Unknowable`].
    ///
    /// Retain As Published exists for bridges, which need the publisher's
    /// flag preserved so the far side stores what the near side stored
    /// [mqtt5 §4.6]. The cost is exactly this: a bridge cannot also tell
    /// which of its inbound messages were snapshots.
    #[must_use]
    pub fn origin(&self, retain_as_published: bool) -> RetainedOrigin {
        match (retain_as_published, self.retain) {
            (true, _) => RetainedOrigin::Unknowable,
            (false, true) => RetainedOrigin::Retained,
            (false, false) => RetainedOrigin::Live,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_mqtt_codec::PubrelReasonCode;

    #[test]
    fn the_success_codes_that_look_like_failures_are_successes() {
        // 0x10, the only in-protocol "delivered to nobody" signal.
        let nobody = Completion::Acknowledged(PubackReasonCode::NoMatchingSubscribers);
        assert!(!nobody.is_error());
        assert_eq!(nobody.reason_code(), Some(0x10));

        // And the ones that are failures.
        let refused = Completion::Refused(PubackReasonCode::NotAuthorized);
        assert!(refused.is_error());
        assert_eq!(refused.reason_code(), Some(0x87));

        // QoS 0 certifies nothing and therefore reports no code.
        assert!(!Completion::Sent.is_error());
        assert_eq!(Completion::Sent.reason_code(), None);

        let complete = Completion::Complete(PubrelReasonCode::Success);
        assert!(!complete.is_error());
        assert_eq!(complete.reason_code(), Some(0x00));
        let lost = Completion::Complete(PubrelReasonCode::PacketIdentifierNotFound);
        assert!(lost.is_error());
        assert_eq!(lost.reason_code(), Some(0x92));
    }

    /// The two availability flags a publish can trip, each refused with the
    /// server's own code before anything is sent.
    #[test]
    fn a_publish_is_refused_against_what_the_server_declined() {
        let strict = ServerLimits {
            maximum_qos: QoS::AtLeastOnce,
            retain_available: false,
            ..ServerLimits::default()
        };

        let too_high = Message::new("a", "b").at(QoS::ExactlyOnce);
        assert_eq!(
            too_high.check(&strict).unwrap_err().reason_code(),
            Some(0x9B)
        );

        let retained = Message::new("a", "b").at(QoS::AtLeastOnce).retained();
        assert_eq!(
            retained.check(&strict).unwrap_err().reason_code(),
            Some(0x9A)
        );

        // At or below the ceiling, and without RETAIN, it passes.
        assert!(
            Message::new("a", "b")
                .at(QoS::AtLeastOnce)
                .check(&strict)
                .is_ok()
        );
        // And a permissive server permits both.
        assert!(
            Message::new("a", "b")
                .at(QoS::ExactlyOnce)
                .retained()
                .check(&ServerLimits::default())
                .is_ok()
        );
    }

    /// The ceiling the protocol does not provide: a delivery carrying more
    /// Subscription Identifiers than this client will hold keeps the first
    /// of them and says so, rather than growing a vector a peer controls — or
    /// costing the connection.
    #[test]
    fn a_delivery_past_the_subscription_identifier_ceiling_is_truncated() {
        let limits = Limits {
            max_subscription_identifiers: 2,
            ..Limits::default()
        };
        let ids = [1u32, 2, 3];
        let properties = Properties::new().with_subscription_identifiers(&ids);
        let read = DeliveryProperties::read(&properties, &limits);
        assert_eq!(read.subscription_identifiers, [1, 2], "the first, in order");
        assert!(read.truncated);

        // At the ceiling it is read whole, and in order: which subscription
        // caused a delivery is the whole reason the property exists.
        let ids = [7u32, 9];
        let properties = Properties::new().with_subscription_identifiers(&ids);
        let read = DeliveryProperties::read(&properties, &limits);
        assert_eq!(read.subscription_identifiers, [7, 9]);
        assert!(!read.truncated, "nothing was left out");
    }

    /// The other ceiling the protocol does not provide: `User Property` is
    /// equally repeatable (3.3.2.3.7), so a delivery carrying more pairs than
    /// this client will hold is cut on the same policy as its neighbour,
    /// rather than growing a vector of peer-controlled strings.
    #[test]
    fn a_delivery_past_the_user_property_ceiling_is_truncated() {
        let limits = Limits {
            max_user_properties: 2,
            ..Limits::default()
        };
        let pairs = [("a", "1"), ("b", "2"), ("c", "3")];
        let properties = Properties::new().with_user_properties(&pairs);
        let read = DeliveryProperties::read(&properties, &limits);
        assert_eq!(
            read.user_properties,
            [
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]
        );
        assert!(read.truncated);

        // At the ceiling it is read, and in the publisher's order: the pairs
        // are an ordered list and not a map ([MQTT-3.3.2-17]).
        let pairs = [("b", "2"), ("a", "1")];
        let properties = Properties::new().with_user_properties(&pairs);
        let read = DeliveryProperties::read(&properties, &limits);
        let seen: Vec<(&str, &str)> = read
            .user_properties
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        assert_eq!(seen, [("b", "2"), ("a", "1")]);
        assert!(!read.truncated);
    }

    #[test]
    fn a_stored_message_keeps_every_forwarded_property() {
        let message = Message {
            topic: "a/b".into(),
            payload: Bytes::from_static(b"body"),
            qos: QoS::ExactlyOnce,
            retain: true,
            payload_format_indicator: Some(PayloadFormat::Utf8),
            message_expiry: Some(Duration::from_secs(60)),
            content_type: Some("text/plain".into()),
            response_topic: Some("r".into()),
            correlation_data: Some(vec![1, 2]),
            user_properties: vec![("k".into(), "v".into())],
        };
        let stored = message.clone().into_stored().expect("stores");
        assert_eq!(stored.topic, "a/b");
        assert_eq!(stored.message_expiry_interval, Some(60));
        assert_eq!(stored.content_type.as_deref(), Some("text/plain"));
        assert_eq!(stored.correlation_data.as_deref(), Some(&[1u8, 2][..]));
        assert_eq!(stored.user_properties, [("k".to_string(), "v".to_string())]);
        assert!(stored.retain);
    }

    /// The delivered QoS is what arrived, not what was published: "the QoS of
    /// Application Messages sent in response to a Subscription MUST be the
    /// minimum of the QoS of the originally published message and the Maximum
    /// QoS granted" ([MQTT-3.8.4-8]).
    #[test]
    fn a_delivery_reports_the_qos_it_arrived_at() {
        let publish = Publish {
            topic: "a/b",
            payload: b"hi",
            qos: QoS::AtMostOnce,
            dup: false,
            retain: true,
            packet_id: None,
            properties: Properties::new(),
        };
        let delivery = Delivery::read(&publish, "a/b".to_owned(), &Limits::default());
        assert_eq!(delivery.qos, QoS::AtMostOnce);
        assert_eq!(delivery.packet_id, None, "no identifier at QoS 0");
        assert!(delivery.retain);
        assert!(!delivery.dup);
    }

    /// Publishing clones the message and a QoS > 0 publish stores it, and
    /// neither may copy the payload: it is shared, so a large message costs
    /// one allocation however many times it is held.
    #[test]
    fn a_clone_and_the_stored_shape_share_the_payload() {
        let message = Message::new("a/b", vec![7u8; 4096]).at(QoS::AtLeastOnce);
        let cloned = message.clone();
        assert_eq!(message.payload.as_ptr(), cloned.payload.as_ptr());
        let stored = message.into_stored().expect("stores");
        assert_eq!(stored.payload.as_ptr(), cloned.payload.as_ptr());
    }
}
