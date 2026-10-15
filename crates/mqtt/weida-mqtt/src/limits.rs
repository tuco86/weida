//! Every ceiling, named: ours, and the server's own.
//!
//! MQTT's §11 is a table of limits and a list of four resources the protocol
//! does not bound at all [mqtt5 §11]. A client is on the exposed side of two
//! of them, so this module is where "no remote input can cause unbounded
//! memory allocation" ([INVARIANTS.md]) becomes a number with a name and a
//! source.
//!
//! **Three tables grow from what a peer sends, and each one's ceiling is the
//! protocol's own rather than invented here.**
//!
//! | Table | Bounded by | Whose number |
//! | --- | --- | --- |
//! | Topic Alias mappings the server may establish toward us | `Topic Alias Maximum` we declare in CONNECT (3.1.2.11.5) | ours, declared |
//! | Topic Alias mappings we may establish toward the server | `Topic Alias Maximum` the server declares in CONNACK (3.2.2.3.8) | the server's, honoured |
//! | Unacknowledged QoS 1 and 2 PUBLISH packets we have in flight | `Receive Maximum` the server declares in CONNACK (3.2.2.3.3) | the server's, honoured |
//! | Unacknowledged QoS 1 and 2 PUBLISH packets the server has in flight toward us | `Receive Maximum` we declare in CONNECT (3.1.2.11.3) | ours, declared |
//!
//! Those four are not policy: "the sender sets an initial send quota, non-zero
//! and not exceeding the peer's Receive Maximum ([MQTT-4.9.0-1])" and "the
//! sender MUST NOT exceed [Topic Alias Maximum] ([MQTT-3.3.2-9])" [mqtt5 §5],
//! so using the peer's declared value *is* the bound, and saying so here is
//! what stops a later reader from inventing a second one beside it.
//!
//! **More tables have no protocol ceiling, so [`Limits`] supplies one.**
//! The Subscription Identifiers and the User Properties a single delivery may
//! carry — "a single copy carries all matching identifiers
//! ([MQTT-3.3.4-4])" [mqtt5 §4.1] — are bounded only by the packet size; the
//! queue of deliveries waiting for the application, and the QoS 1 and 2
//! messages this client keeps while the server has not acknowledged them, are
//! bounded by a count the protocol does not set and by a number of **bytes**
//! it cannot, because a count is not a memory bound; and the Topic Alias
//! tables are bounded in entries by the peer's declaration and in bytes by
//! nothing but the packet size. Each gets a named field, and each says in its
//! own documentation that the number is ours because the protocol has none.

use std::time::Duration;

use weida_mqtt_codec::{Connack, QoS, varint};

use crate::error::{Error, Feature, Result};

/// `Receive Maximum` when the property is absent (3.1.2.11.3) [mqtt5 §11].
pub const DEFAULT_RECEIVE_MAXIMUM: u16 = 65_535;

/// `Topic Alias Maximum` when the property is absent: zero, "so no aliases
/// unless requested" (3.1.2.11.5) [mqtt5 §5].
pub const DEFAULT_TOPIC_ALIAS_MAXIMUM: u16 = 0;

/// What this client declares about itself, and the two bounds the protocol
/// does not supply.
///
/// Every field is refused rather than corrected when it is unusable, which is
/// the rule [0006](../../../docs/decisions/0006-guarantee-sets.md) §4.7 gives
/// an edge and [0013](../../../docs/decisions/0013-competitor-libraries.md)
/// §4.4 item 4 gives an option.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// `Receive Maximum` we put in CONNECT: unacknowledged QoS 1 and 2
    /// PUBLISH packets we will accept from the server at once (3.1.2.11.3).
    /// Must be non-zero — 0 is a Protocol Error [mqtt5 §11].
    ///
    /// This is the only bound on what the server may have in flight toward
    /// us, and it counts packets and nothing else: QoS 0 is entirely
    /// uncovered, which the specification states twice [mqtt5 §5]. So it is
    /// not a defence against a QoS 0 flood, and `incoming_queue` is.
    pub receive_maximum: u16,
    /// `Maximum Packet Size` we put in CONNECT: the largest packet we will
    /// accept, fixed header included (3.1.2.11.4). Must be non-zero and at
    /// most [`varint::MAX`] plus the five bytes of the largest fixed header.
    ///
    /// It is also the cap every decode in this client passes to
    /// `weida-mqtt-codec`, so an over-large declaration is refused from the
    /// fixed header alone, before a body is reserved.
    pub maximum_packet_size: u32,
    /// `Topic Alias Maximum` we put in CONNECT: the highest alias the server
    /// may use toward us, and therefore the size of the alias table we must
    /// hold (3.1.2.11.5). Zero refuses aliases altogether, which is the
    /// protocol's own default.
    pub topic_alias_maximum: u16,
    /// Subscription Identifiers this client will keep on one delivery.
    ///
    /// **The protocol has no ceiling for this one.** A server MAY send one
    /// copy carrying every matching identifier ([MQTT-3.3.4-4]) [mqtt5 §4.1],
    /// and the only limit is the packet size, so at `maximum_packet_size` of
    /// 268 MB a single delivery could carry tens of millions. The number is
    /// ours, and a delivery that carries more keeps the first of them and
    /// says so ([`crate::DeliveryProperties::truncated`]): the cap bounds what
    /// is *kept*, and a delivery is not worth the connection.
    pub max_subscription_identifiers: usize,
    /// `User Property` pairs this client will keep on one delivery.
    ///
    /// **The protocol has no ceiling for this one either.** `User Property`
    /// "is allowed to appear multiple times to represent multiple name, value
    /// pairs" (3.3.2.3.7) [mqtt5 §3], and the only limit on the wire is again
    /// the packet size: a pair's minimal form is five octets, so even the
    /// default `maximum_packet_size` of 1 MiB admits over 200,000 of them and
    /// a 268 MB one admits over 50 million. The number is ours, and a
    /// delivery that carries more keeps the first of them and says so
    /// ([`crate::DeliveryProperties::truncated`]): any publisher who can reach
    /// a subscribed topic can send one, so a verdict on the connection would
    /// hand every publisher the power to disconnect every subscriber.
    pub max_user_properties: usize,
    /// Deliveries held for the application before the reader stops reading.
    ///
    /// **The protocol has no ceiling for this one either**, and QoS 0 has no
    /// credit at all [mqtt5 §5], so this queue is the only thing between a
    /// QoS 0 flood and this process's memory. Reaching it stops the socket
    /// being read, which is backpressure rather than a drop — and only the
    /// socket: keep-alive, commands and `disconnect` keep running while it is
    /// full. A count is not a memory bound, so see
    /// [`Limits::incoming_queue_bytes`], which is the one that is.
    pub incoming_queue: usize,
    /// Bytes of deliveries held for the application before the reader stops
    /// reading.
    ///
    /// Each delivery is charged the size of the packet that carried it plus a
    /// fixed overhead, so a flood of large messages, or of empty ones, meets
    /// this ceiling and not only [`Limits::incoming_queue`]. **A queue that is
    /// empty still accepts one message of any size**, because a bound that
    /// refused the only message would be a deadlock; the most this can hold is
    /// therefore this number less one plus [`Limits::maximum_packet_size`].
    pub incoming_queue_bytes: usize,
    /// QoS 1 and 2 messages this client keeps unacknowledged at once,
    /// whatever the server's `Receive Maximum` allows.
    ///
    /// The server's number is the protocol's bound on the send quota
    /// ([MQTT-4.9.0-1]) and is the **peer's**: a server that declares nothing
    /// means 65,535, and a silent one never acknowledges any of them. The send
    /// quota is the smaller of the two, and a publish past it stalls like one
    /// past the server's own.
    pub max_inflight: usize,
    /// Bytes of QoS 1 and 2 messages this client keeps unacknowledged at once.
    ///
    /// Each stored message is charged its topic, payload, properties and a
    /// fixed overhead. A publish that would pass it stalls until
    /// acknowledgements free room, and **a session holding nothing still
    /// accepts one message of any size**, for the reason
    /// [`Limits::incoming_queue_bytes`] gives.
    pub max_inflight_bytes: usize,
    /// Bytes of Topic Names one Topic Alias table may hold, charged per entry
    /// with a fixed overhead.
    ///
    /// The tables are bounded in entries by the peer's `Topic Alias Maximum`
    /// — what we declared for the inbound one, what the server declared for
    /// the outbound one — and an entry may be a 65,535-byte Topic Name, so the
    /// entry count is not a memory bound either. An inbound alias that would
    /// pass this is refused with 0x94; an outbound topic that would pass it is
    /// simply sent without an alias.
    pub max_alias_bytes: usize,
    /// Addresses a resolver answer may contribute, since a resolver answer is
    /// remote input ([INVARIANTS.md]).
    pub max_addresses: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            // Not 65,535: a default that admits 65,535 concurrent
            // unacknowledged deliveries is a default nobody sized. 64 is one
            // order above Mosquitto's `max_inflight_messages` of 20 and
            // EMQX's `mqtt.max_inflight` of 32 [mqtt5 §5], so it is generous
            // against what brokers themselves ship.
            receive_maximum: 64,
            // Mosquitto's `max_packet_size` since 2.1 is 2,000,000 bytes and
            // EMQX's is 1 MB [mqtt5 §11]; 1 MiB is that scale, and it is four
            // orders below the 268 MB the encoding would otherwise allow.
            maximum_packet_size: 1024 * 1024,
            // The protocol's default is 0, and so is this one: an alias table
            // is memory a peer controls, and asking for one should be a
            // decision.
            topic_alias_maximum: DEFAULT_TOPIC_ALIAS_MAXIMUM,
            // A client with more than 32 overlapping subscriptions matching
            // one topic has a topic design problem, not a limits problem.
            max_subscription_identifiers: 32,
            // Properties are metadata, not payload: a delivery that needs
            // more than 64 named pairs to describe itself is carrying its
            // payload in its header. 64 is an order above the handful that
            // tracing and content-negotiation conventions use, which is the
            // same reasoning that sizes `max_subscription_identifiers`.
            max_user_properties: 64,
            incoming_queue: 1024,
            // The bound that is a bound: 1024 messages is a count, and a
            // count at the 1 MiB default packet size is a gigabyte. 8 MiB is
            // the figure `weida-zmq` gives the same queue, and a client that
            // is draining at all never meets it.
            incoming_queue_bytes: 8 * 1024 * 1024,
            // Mosquitto ships 20 and EMQX 32 as their own in-flight windows,
            // so 1024 is two orders above what a broker lets a client have
            // outstanding and still caps the 65,535 a silent one would allow.
            max_inflight: 1024,
            max_inflight_bytes: 8 * 1024 * 1024,
            // 256 KiB holds thousands of aliases for ordinary topic names at
            // the per-entry overhead, and a peer that wants more than that
            // from this table is not describing a topic set.
            max_alias_bytes: 256 * 1024,
            max_addresses: 8,
        }
    }
}

impl Limits {
    /// The largest packet the encoding can express, fixed header included:
    /// the 268,435,455-byte Remaining Length plus its own five bytes (2.1.4)
    /// [mqtt5 §3].
    pub const MAX_PACKET_SIZE: u32 = varint::MAX + 5;

    /// Refuses an unusable configuration at configuration time.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for a `Receive Maximum` of 0 or a
    /// `Maximum Packet Size` of 0, both Protocol Errors on the wire
    /// [mqtt5 §11]; for a `Maximum Packet Size` above what the encoding can
    /// express; and for a zero per-delivery ceiling, queue or address budget,
    /// which would make progress impossible.
    pub fn validate(&self) -> Result<()> {
        if self.receive_maximum == 0 {
            return Err(Error::Configuration(
                "Limits::receive_maximum must be non-zero: 0 is a Protocol Error (3.1.2.11.3)"
                    .into(),
            ));
        }
        if self.maximum_packet_size == 0 {
            return Err(Error::Configuration(
                "Limits::maximum_packet_size must be non-zero: 0 is a Protocol Error (3.1.2.11.4)"
                    .into(),
            ));
        }
        if self.maximum_packet_size > Limits::MAX_PACKET_SIZE {
            return Err(Error::Configuration(format!(
                "Limits::maximum_packet_size {} exceeds the {} bytes the encoding can express",
                self.maximum_packet_size,
                Limits::MAX_PACKET_SIZE
            )));
        }
        if self.max_subscription_identifiers == 0 {
            return Err(Error::Configuration(
                "Limits::max_subscription_identifiers must be at least 1".into(),
            ));
        }
        if self.max_user_properties == 0 {
            return Err(Error::Configuration(
                "Limits::max_user_properties must be at least 1".into(),
            ));
        }
        for (name, value) in [
            ("incoming_queue", self.incoming_queue),
            ("incoming_queue_bytes", self.incoming_queue_bytes),
            ("max_inflight", self.max_inflight),
            ("max_inflight_bytes", self.max_inflight_bytes),
            ("max_alias_bytes", self.max_alias_bytes),
            ("max_addresses", self.max_addresses),
        ] {
            if value == 0 {
                return Err(Error::Configuration(format!(
                    "Limits::{name} must be at least 1"
                )));
            }
        }
        Ok(())
    }
}

/// What the server declared about itself in CONNACK, with the absent
/// properties replaced by the defaults §11 gives them.
///
/// **Absence is meaningful on the wire and is resolved exactly once, here.**
/// An absent `Maximum QoS` means 2, an absent `Retain Available` means
/// available, an absent `Receive Maximum` means 65,535, an absent `Topic
/// Alias Maximum` means 0 [mqtt5 §11]. Resolving that in one place is what
/// lets every other module ask a plain question and get a plain answer;
/// resolving it at each call site is how a client ends up refusing RETAIN
/// against a server that never said anything about it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerLimits {
    /// `Receive Maximum` (3.2.2.3.3): unacknowledged QoS 1 and 2 PUBLISH
    /// packets we may have in flight toward the server. **This is the send
    /// quota's ceiling**, per [MQTT-4.9.0-1], and it is the server's number,
    /// not ours.
    pub receive_maximum: u16,
    /// `Maximum QoS` (3.2.2.3.4): the highest QoS we may publish at. Absent
    /// means 2.
    pub maximum_qos: QoS,
    /// `Retain Available` (3.2.2.3.5). Absent means available.
    pub retain_available: bool,
    /// `Maximum Packet Size` (3.2.2.3.6): the largest packet we may send.
    /// `None` means no limit below what the encoding allows.
    pub maximum_packet_size: Option<u32>,
    /// `Topic Alias Maximum` (3.2.2.3.8): the highest alias we may use toward
    /// the server, and therefore the size of our outbound alias table. Absent
    /// means 0 — no aliases.
    pub topic_alias_maximum: u16,
    /// `Wildcard Subscription Available` (3.2.2.3.11). Absent means
    /// available.
    pub wildcard_subscription_available: bool,
    /// `Subscription Identifiers Available` (3.2.2.3.12). Absent means
    /// available.
    pub subscription_identifiers_available: bool,
    /// `Shared Subscription Available` (3.2.2.3.13). Absent means available.
    pub shared_subscription_available: bool,
    /// `Server Keep Alive` (3.2.2.3.14): present, the client MUST use it
    /// ([MQTT-3.2.2-21]); absent, the client's own value stands
    /// ([MQTT-3.2.2-22]) [mqtt5 §1]. The only property that overrides the
    /// client.
    pub server_keep_alive: Option<Duration>,
    /// `Session Expiry Interval` (3.2.2.3.2), where the server revised what
    /// the client asked for.
    pub session_expiry_interval: Option<Duration>,
    /// `Assigned Client Identifier` (3.2.2.3.7), when the client sent a
    /// zero-length one ([MQTT-3.2.2-16]).
    pub assigned_client_identifier: Option<String>,
    /// `Response Information` (3.2.2.3.15): the namespace to build a Response
    /// Topic from, which the server MAY decline even when asked
    /// (3.1.2.11.6) [mqtt5 §4.3].
    pub response_information: Option<String>,
    /// `Server Reference` (3.2.2.3.16), for a redirection.
    pub server_reference: Option<String>,
    /// `Reason String` (3.2.2.3.9), which "SHOULD NOT be parsed"
    /// [mqtt5 §12/P16].
    pub reason_string: Option<String>,
}

impl Default for ServerLimits {
    /// Every default is §11's meaning of the property being absent, which is
    /// also what a server that sends an empty CONNACK property set means.
    fn default() -> ServerLimits {
        ServerLimits {
            receive_maximum: DEFAULT_RECEIVE_MAXIMUM,
            maximum_qos: QoS::ExactlyOnce,
            retain_available: true,
            maximum_packet_size: None,
            topic_alias_maximum: DEFAULT_TOPIC_ALIAS_MAXIMUM,
            wildcard_subscription_available: true,
            subscription_identifiers_available: true,
            shared_subscription_available: true,
            server_keep_alive: None,
            session_expiry_interval: None,
            assigned_client_identifier: None,
            response_information: None,
            server_reference: None,
            reason_string: None,
        }
    }
}

impl ServerLimits {
    /// Reads a CONNACK's property set, substituting §11's defaults for what
    /// is absent.
    #[must_use]
    pub fn from_connack(connack: &Connack<'_>) -> ServerLimits {
        let properties = &connack.properties;
        ServerLimits {
            receive_maximum: properties
                .receive_maximum
                .unwrap_or(DEFAULT_RECEIVE_MAXIMUM),
            maximum_qos: properties.maximum_qos.unwrap_or(QoS::ExactlyOnce),
            retain_available: properties.retain_available.unwrap_or(true),
            maximum_packet_size: properties.maximum_packet_size,
            topic_alias_maximum: properties
                .topic_alias_maximum
                .unwrap_or(DEFAULT_TOPIC_ALIAS_MAXIMUM),
            wildcard_subscription_available: properties
                .wildcard_subscription_available
                .unwrap_or(true),
            subscription_identifiers_available: properties
                .subscription_identifier_available
                .unwrap_or(true),
            shared_subscription_available: properties.shared_subscription_available.unwrap_or(true),
            server_keep_alive: properties
                .server_keep_alive
                .map(|seconds| Duration::from_secs(u64::from(seconds))),
            session_expiry_interval: properties
                .session_expiry_interval
                .map(|seconds| Duration::from_secs(u64::from(seconds))),
            assigned_client_identifier: properties.assigned_client_identifier.map(str::to_owned),
            response_information: properties.response_information.map(str::to_owned),
            server_reference: properties.server_reference.map(str::to_owned),
            reason_string: properties.reason_string.map(str::to_owned),
        }
    }

    /// Refuses `feature` locally when the server declared it unavailable.
    ///
    /// Using an unavailable feature is this client's own error and never
    /// reaches the wire, and the error carries the code the server would have
    /// sent.
    ///
    /// # Errors
    ///
    /// [`Error::Unavailable`] with `feature`'s reason code.
    pub fn require(&self, feature: Feature) -> Result<()> {
        let available = match feature {
            Feature::Retain => self.retain_available,
            Feature::Qos(qos) => qos <= self.maximum_qos,
            Feature::WildcardSubscription => self.wildcard_subscription_available,
            Feature::SubscriptionIdentifier => self.subscription_identifiers_available,
            Feature::SharedSubscription => self.shared_subscription_available,
            Feature::TopicAlias => self.topic_alias_maximum > 0,
        };
        if available {
            return Ok(());
        }
        Err(Error::Unavailable {
            feature,
            reason_code: feature.reason_code(),
        })
    }

    /// The alias this client may use toward the server, refused above the
    /// server's `Topic Alias Maximum` ([MQTT-3.3.2-9]) [mqtt5 §5].
    ///
    /// # Errors
    ///
    /// [`Error::Unavailable`] with 0x94 for an alias of 0 or one above the
    /// server's maximum.
    pub fn check_topic_alias(&self, alias: u16) -> Result<()> {
        if alias == 0 || alias > self.topic_alias_maximum {
            return Err(Error::Unavailable {
                feature: Feature::TopicAlias,
                reason_code: Feature::TopicAlias.reason_code(),
            });
        }
        Ok(())
    }

    /// The cap to pass an encoder: the server's `Maximum Packet Size`, or the
    /// encoding's own ceiling where the server declared none.
    #[must_use]
    pub fn send_packet_ceiling(&self) -> u32 {
        self.maximum_packet_size.unwrap_or(Limits::MAX_PACKET_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_mqtt_codec::{ConnectReasonCode, Properties};

    #[test]
    fn an_empty_connack_means_the_documented_defaults() {
        let limits = ServerLimits::from_connack(&Connack::default());
        assert_eq!(limits, ServerLimits::default());
        // §11's four defaults, spelled out because every one of them is a
        // place a client could plausibly assume zero instead.
        assert_eq!(limits.receive_maximum, 65_535);
        assert_eq!(limits.maximum_qos, QoS::ExactlyOnce);
        assert!(limits.retain_available);
        assert_eq!(limits.topic_alias_maximum, 0);
        assert_eq!(limits.maximum_packet_size, None);
        assert_eq!(limits.send_packet_ceiling(), Limits::MAX_PACKET_SIZE);
    }

    #[test]
    fn every_declared_property_is_read() {
        let connack = Connack {
            session_present: false,
            reason_code: ConnectReasonCode::Success,
            properties: Properties {
                receive_maximum: Some(10),
                maximum_qos: Some(QoS::AtLeastOnce),
                retain_available: Some(false),
                maximum_packet_size: Some(2_000_000),
                topic_alias_maximum: Some(10),
                wildcard_subscription_available: Some(false),
                subscription_identifier_available: Some(false),
                shared_subscription_available: Some(false),
                server_keep_alive: Some(30),
                session_expiry_interval: Some(3600),
                assigned_client_identifier: Some("auto-1"),
                response_information: Some("reply/auto-1"),
                server_reference: Some("other:1883"),
                reason_string: Some("welcome"),
                ..Properties::new()
            },
        };
        let limits = ServerLimits::from_connack(&connack);
        assert_eq!(limits.receive_maximum, 10);
        assert_eq!(limits.maximum_qos, QoS::AtLeastOnce);
        assert!(!limits.retain_available);
        assert_eq!(limits.send_packet_ceiling(), 2_000_000);
        assert_eq!(limits.topic_alias_maximum, 10);
        assert_eq!(limits.server_keep_alive, Some(Duration::from_secs(30)));
        assert_eq!(
            limits.session_expiry_interval,
            Some(Duration::from_secs(3600))
        );
        assert_eq!(limits.assigned_client_identifier.as_deref(), Some("auto-1"));
        assert_eq!(limits.response_information.as_deref(), Some("reply/auto-1"));
        assert_eq!(limits.server_reference.as_deref(), Some("other:1883"));
        assert_eq!(limits.reason_string.as_deref(), Some("welcome"));
    }

    /// The five flags, each refused with the code 3.2.2.3 gives it and each
    /// permitted when the server said nothing.
    #[test]
    fn an_unavailable_feature_is_refused_with_the_servers_code() {
        let permissive = ServerLimits::default();
        for feature in [
            Feature::Retain,
            Feature::Qos(QoS::ExactlyOnce),
            Feature::WildcardSubscription,
            Feature::SubscriptionIdentifier,
            Feature::SharedSubscription,
        ] {
            assert!(permissive.require(feature).is_ok(), "{feature}");
        }

        let strict = ServerLimits {
            retain_available: false,
            maximum_qos: QoS::AtMostOnce,
            wildcard_subscription_available: false,
            subscription_identifiers_available: false,
            shared_subscription_available: false,
            ..ServerLimits::default()
        };
        for (feature, code) in [
            (Feature::Retain, 0x9A),
            (Feature::Qos(QoS::AtLeastOnce), 0x9B),
            (Feature::SharedSubscription, 0x9E),
            (Feature::SubscriptionIdentifier, 0xA1),
            (Feature::WildcardSubscription, 0xA2),
        ] {
            let error = strict.require(feature).expect_err("refused");
            assert_eq!(error.reason_code(), Some(code), "{feature}");
        }
        // QoS 0 is still permitted against Maximum QoS 0: the rule is a
        // ceiling, not a ban ([MQTT-3.2.2-9]).
        assert!(strict.require(Feature::Qos(QoS::AtMostOnce)).is_ok());
    }

    /// Topic Alias Maximum 0 is the default, so an alias against a silent
    /// server is refused rather than sent and rejected.
    #[test]
    fn an_alias_above_the_servers_maximum_is_refused() {
        let silent = ServerLimits::default();
        assert_eq!(
            silent.check_topic_alias(1).unwrap_err().reason_code(),
            Some(0x94)
        );

        let ten = ServerLimits {
            topic_alias_maximum: 10,
            ..ServerLimits::default()
        };
        assert!(ten.check_topic_alias(1).is_ok());
        assert!(ten.check_topic_alias(10).is_ok());
        // 0 is forbidden outright ([MQTT-3.3.2-8]), and 11 is over the bound.
        assert!(ten.check_topic_alias(0).is_err());
        assert!(ten.check_topic_alias(11).is_err());
    }

    #[test]
    fn the_two_protocol_error_zeroes_are_refused_at_configuration_time() {
        assert!(Limits::default().validate().is_ok());
        for limits in [
            Limits {
                receive_maximum: 0,
                ..Limits::default()
            },
            Limits {
                maximum_packet_size: 0,
                ..Limits::default()
            },
            Limits {
                maximum_packet_size: Limits::MAX_PACKET_SIZE + 1,
                ..Limits::default()
            },
            Limits {
                incoming_queue: 0,
                ..Limits::default()
            },
            Limits {
                max_subscription_identifiers: 0,
                ..Limits::default()
            },
            Limits {
                max_user_properties: 0,
                ..Limits::default()
            },
            Limits {
                max_addresses: 0,
                ..Limits::default()
            },
            Limits {
                incoming_queue_bytes: 0,
                ..Limits::default()
            },
            Limits {
                max_inflight: 0,
                ..Limits::default()
            },
            Limits {
                max_inflight_bytes: 0,
                ..Limits::default()
            },
            Limits {
                max_alias_bytes: 0,
                ..Limits::default()
            },
        ] {
            assert!(limits.validate().is_err(), "{limits:?}");
        }
    }

    /// The default is deliberately not 65,535: a client that admits every
    /// identifier the space has is a client nobody sized.
    #[test]
    fn the_default_receive_maximum_is_sized_against_what_brokers_ship() {
        let limits = Limits::default();
        assert!(limits.receive_maximum > 32, "above EMQX's max_inflight");
        assert!(limits.receive_maximum < DEFAULT_RECEIVE_MAXIMUM);
        assert!(limits.maximum_packet_size < Limits::MAX_PACKET_SIZE);
    }
}
