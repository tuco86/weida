//! PUBLISH: the only packet that carries an Application Message, and the only
//! one that uses its fixed-header flags.
//!
//! ```text
//! fixed header flags   DUP(3) QoS(2-1) RETAIN(0)
//! variable header
//!   topic name         UTF-8 Encoded String; may be zero-length with a
//!                      Topic Alias (3.3.2.1)
//!   packet identifier  Two Byte Integer, **only** when QoS > 0 (3.3.2.2)
//!   properties         3.3.2.3
//! payload              the rest of the packet; zero length is valid (3.3.3)
//! ```
//!
//! **The Packet Identifier's presence is decided by the QoS bits and nothing
//! else.** "The Packet Identifier field is only present in PUBLISH packets
//! where the QoS level is 1 or 2" (3.3.2.2) [mqtt5 §3], so the two bits in the
//! fixed header change the shape of the variable header. That makes
//! [`Publish::packet_id`] an `Option` whose `Some`-ness is not free: the
//! encoder derives it from the QoS, so a QoS 0 publish with an identifier set
//! cannot be built by accident.
//!
//! **The payload is the remainder, and there is no length field for it.** One
//! PUBLISH carries one whole Application Message — no multipart, no
//! fragmentation, no continuation packet [mqtt5 §12/P13] — so the payload
//! runs from the end of the properties to the end of the packet. It is
//! borrowed, never copied, which is what lets a client hand it on without a
//! second copy.
//!
//! Two rules are enforced here; three deliberately are not.
//!
//! Enforced, because both are facts about the bytes: DUP is 0 on a QoS 0
//! message ([MQTT-3.3.1-2]), and a Packet Identifier is non-zero
//! ([MQTT-2.2.1-3]).
//!
//! Not enforced, because each needs state a sans-I/O codec does not hold:
//!
//! * **A zero-length Topic Name is legal only with an established Topic
//!   Alias**, and an unestablished one earns DISCONNECT 0x82 [mqtt5 §8]. The
//!   alias table is per direction and per connection ([MQTT-3.3.2-7]), so it
//!   belongs to the client's alias table in `weida-mqtt`.
//! * **`Subscription Identifier` MUST NOT appear in a client-to-server
//!   PUBLISH** ([MQTT-3.3.4-6]) [mqtt5 §3]. The property is legal in the same
//!   packet travelling the other way — that is how a client learns which
//!   subscription caused a delivery — so the rule is about direction, which
//!   this crate does not know. [`crate::PacketType::from_client`] is what a
//!   client checks it against.
//! * **`Topic Alias` above the peer's `Topic Alias Maximum`** is a negotiated
//!   bound, not a grammatical one: the client checks it against the CONNACK.

use crate::data::{self, Reader};
use crate::error::{DecodeError, EncodeError};
use crate::property::{Properties, PropertySet};
use crate::types::QoS;

/// Bit positions in a PUBLISH fixed header's low nibble (3.3.1) [mqtt5 §6].
const DUP: u8 = 0b1000;
const QOS: u8 = 0b0110;
const RETAIN: u8 = 0b0001;

/// A PUBLISH packet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Publish<'a> {
    /// Topic Name: matched character for character with no normalization
    /// ([MQTT-4.7.3-4]) [mqtt5 §3], and never a filter — no wildcards.
    /// Zero-length only with a `Topic Alias`.
    pub topic: &'a str,
    /// The Application Message. Opaque bytes; zero length is valid (3.3.3)
    /// [mqtt5 §3].
    pub payload: &'a [u8],
    /// Quality of Service, from bits 2-1 of the fixed header.
    pub qos: QoS,
    /// DUP: "might be re-delivery of an earlier attempt to send the packet",
    /// and MUST be 1 when re-delivering ([MQTT-3.3.1-1]) [mqtt5 §6]. It is not
    /// a deduplication key and is not propagated ([MQTT-3.3.1-3]).
    pub dup: bool,
    /// RETAIN: store this as the topic's retained message ([MQTT-3.3.1-5]),
    /// and a zero-byte payload with it deletes rather than stores
    /// ([MQTT-3.3.1-6]) [mqtt5 §4.4].
    pub retain: bool,
    /// The Packet Identifier, present exactly when `qos` is 1 or 2
    /// (3.3.2.2) [mqtt5 §3]. Set by [`Publish::encode_body`] from the QoS, so
    /// a mismatch is not expressible on the wire.
    pub packet_id: Option<u16>,
    /// The PUBLISH properties of 3.3.2.3.
    pub properties: Properties<'a>,
}

impl<'a> Publish<'a> {
    /// The low nibble of the fixed header this packet encodes to.
    #[must_use]
    pub const fn flags(&self) -> u8 {
        let mut flags = self.qos.as_bits() << 1;
        if self.dup {
            flags |= DUP;
        }
        if self.retain {
            flags |= RETAIN;
        }
        flags
    }

    /// Decodes the variable header and payload, given the fixed header's low
    /// nibble.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidQos`] for QoS 3, [`DecodeError::DupOnQos0`],
    /// [`DecodeError::InvalidPacketIdentifier`] for 0, and anything the
    /// property or data readers report.
    pub fn decode_body(reader: &mut Reader<'a>, flags: u8) -> Result<Publish<'a>, DecodeError> {
        let qos = QoS::from_bits((flags & QOS) >> 1)?;
        let dup = flags & DUP != 0;
        if dup && qos == QoS::AtMostOnce {
            return Err(DecodeError::DupOnQos0);
        }

        let topic = reader.string()?;
        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            Some(non_zero(reader.u16()?)?)
        };
        let properties = Properties::decode(
            reader,
            PropertySet::PUBLISH,
            Some(crate::types::PacketType::Publish),
        )?;

        Ok(Publish {
            topic,
            payload: reader.rest(),
            qos,
            dup,
            retain: flags & RETAIN != 0,
            packet_id,
            properties,
        })
    }

    /// Bytes the variable header and payload occupy.
    ///
    /// # Errors
    ///
    /// [`EncodeError::DupOnQos0`],
    /// [`EncodeError::InvalidPacketIdentifier`] for an identifier of 0, plus
    /// whatever the fields and properties report.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        self.check()?;
        let mut len = data::field_len(self.topic.len())?;
        if self.qos != QoS::AtMostOnce {
            len += 2;
        }
        len += self.properties.encoded_len(PropertySet::PUBLISH)?;
        len += u32::try_from(self.payload.len()).map_err(|_| EncodeError::PacketTooLong {
            len: self.payload.len() as u64,
        })?;
        Ok(len)
    }

    /// Appends the variable header and payload.
    ///
    /// # Errors
    ///
    /// As [`Publish::body_len`].
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        self.check()?;
        data::put_string(self.topic, out)?;
        if self.qos != QoS::AtMostOnce {
            // The identifier's presence follows the QoS, not the `Option`:
            // 0 is not a valid identifier, so `check` has already refused a
            // QoS > 0 publish without one.
            let packet_id = self.packet_id.unwrap_or(0);
            out.extend_from_slice(&packet_id.to_be_bytes());
        }
        self.properties.encode(PropertySet::PUBLISH, out)?;
        out.extend_from_slice(self.payload);
        Ok(())
    }

    /// The two rules the encoder refuses to put on the wire.
    fn check(&self) -> Result<(), EncodeError> {
        if self.dup && self.qos == QoS::AtMostOnce {
            return Err(EncodeError::DupOnQos0);
        }
        if self.qos != QoS::AtMostOnce && self.packet_id.unwrap_or(0) == 0 {
            return Err(EncodeError::InvalidPacketIdentifier);
        }
        Ok(())
    }
}

/// A Packet Identifier of 0 is not one ([MQTT-2.2.1-3]) (2.2.1) [mqtt5 §2].
pub(crate) fn non_zero(packet_id: u16) -> Result<u16, DecodeError> {
    if packet_id == 0 {
        return Err(DecodeError::InvalidPacketIdentifier);
    }
    Ok(packet_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::property::PayloadFormat;
    use crate::varint;
    use crate::{Packet, PropertyId};

    fn round_trip(publish: &Publish<'_>) -> Vec<u8> {
        let mut out = Vec::new();
        Packet::Publish(publish.clone())
            .encode(&mut out)
            .expect("encodes");
        let (packet, used) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(used, out.len());
        assert_eq!(packet, Packet::Publish(publish.clone()));
        out
    }

    /// Vector 5: QoS 0 has no Packet Identifier at all, and the property
    /// length byte is still there.
    #[test]
    fn qos_zero_has_no_packet_identifier() {
        assert_eq!(
            round_trip(&Publish {
                topic: "a/b",
                payload: b"hi",
                ..Publish::default()
            }),
            [0x30, 0x08, 0x00, 0x03, b'a', b'/', b'b', 0x00, b'h', b'i']
        );
    }

    /// Vector 6: QoS 1 puts the identifier after the Topic Name and before the
    /// properties.
    #[test]
    fn qos_one_carries_the_identifier_after_the_topic() {
        assert_eq!(
            round_trip(&Publish {
                topic: "a/b",
                payload: b"hi",
                qos: QoS::AtLeastOnce,
                packet_id: Some(10),
                ..Publish::default()
            }),
            [
                0x32, 0x0A, 0x00, 0x03, b'a', b'/', b'b', 0x00, 0x0A, 0x00, b'h', b'i'
            ]
        );
    }

    /// Vector 7: a zero-length Topic Name plus Topic Alias 1. The codec
    /// accepts it because the alias table is the client's, not the codec's.
    #[test]
    fn a_zero_length_topic_with_an_alias_is_accepted() {
        assert_eq!(
            round_trip(&Publish {
                topic: "",
                payload: b"hi",
                properties: Properties {
                    topic_alias: Some(1),
                    ..Properties::new()
                },
                ..Publish::default()
            }),
            [0x30, 0x08, 0x00, 0x00, 0x03, 0x23, 0x00, 0x01, b'h', b'i']
        );
    }

    #[test]
    fn the_flag_bits_are_dup_qos_and_retain() {
        // DUP 1, QoS 2, RETAIN 1.
        let wire = round_trip(&Publish {
            topic: "t",
            payload: &[],
            qos: QoS::ExactlyOnce,
            dup: true,
            retain: true,
            packet_id: Some(1),
            ..Publish::default()
        });
        assert_eq!(wire[0], 0x3D);
    }

    /// [MQTT-3.3.1-2]: nothing at QoS 0 can be a retransmission, so DUP on one
    /// is a Protocol Error in both directions.
    #[test]
    fn dup_on_qos_zero_is_refused_both_ways() {
        let wire = [0x38, 0x05, 0x00, 0x01, b't', 0x00, b'x'];
        let error = Packet::decode(&wire, varint::MAX).expect_err("refused");
        assert_eq!(error, DecodeError::DupOnQos0);
        assert_eq!(error.reason_code(), Some(crate::error::PROTOCOL_ERROR));

        let mut out = Vec::new();
        assert_eq!(
            Packet::Publish(Publish {
                topic: "t",
                dup: true,
                ..Publish::default()
            })
            .encode(&mut out),
            Err(EncodeError::DupOnQos0)
        );
        assert!(out.is_empty());
    }

    #[test]
    fn a_zero_packet_identifier_is_refused_both_ways() {
        let wire = [0x32, 0x06, 0x00, 0x01, b't', 0x00, 0x00, 0x00];
        assert_eq!(
            Packet::decode(&wire, varint::MAX),
            Err(DecodeError::InvalidPacketIdentifier)
        );

        let mut out = Vec::new();
        assert_eq!(
            Packet::Publish(Publish {
                topic: "t",
                qos: QoS::AtLeastOnce,
                packet_id: None,
                ..Publish::default()
            })
            .encode(&mut out),
            Err(EncodeError::InvalidPacketIdentifier)
        );
    }

    /// A zero-length payload is valid (3.3.3) and is the shape a retained
    /// delete takes ([MQTT-3.3.1-6]).
    #[test]
    fn a_zero_length_payload_is_valid() {
        let wire = round_trip(&Publish {
            topic: "t",
            payload: &[],
            retain: true,
            ..Publish::default()
        });
        assert_eq!(wire, [0x31, 0x04, 0x00, 0x01, b't', 0x00]);
    }

    #[test]
    fn every_publish_property_round_trips() {
        let pairs = [("k", "v")];
        let ids = [1u32, 268_435_455];
        round_trip(&Publish {
            topic: "t/u",
            payload: b"body",
            qos: QoS::ExactlyOnce,
            dup: true,
            retain: true,
            packet_id: Some(0xFFFF),
            properties: Properties {
                payload_format_indicator: Some(PayloadFormat::Utf8),
                message_expiry_interval: Some(60),
                topic_alias: Some(7),
                response_topic: Some("r/s"),
                correlation_data: Some(&[0x01, 0x02]),
                content_type: Some("application/json"),
                ..Properties::new()
            }
            .with_user_properties(&pairs)
            .with_subscription_identifiers(&ids),
        });
    }

    /// The direction rule the codec deliberately does not enforce: a
    /// Subscription Identifier is legal in a PUBLISH the *server* sends and
    /// forbidden in one a client sends ([MQTT-3.3.4-6]), and only a client
    /// knows which it is building.
    #[test]
    fn a_subscription_identifier_decodes_because_direction_is_not_the_codecs() {
        let ids = [5u32];
        let wire = round_trip(&Publish {
            topic: "t",
            properties: Properties::new().with_subscription_identifiers(&ids),
            ..Publish::default()
        });
        let (packet, _) = Packet::decode(&wire, varint::MAX).expect("decodes");
        let Packet::Publish(publish) = packet else {
            panic!("a publish")
        };
        assert_eq!(
            publish
                .properties
                .subscription_identifiers()
                .collect::<Vec<_>>(),
            [5]
        );
        // And the property is refused where it does not belong, which is how
        // a client will check the direction rule.
        assert!(!PropertySet::PUBACK.contains(PropertyId::SubscriptionIdentifier));
    }
}
