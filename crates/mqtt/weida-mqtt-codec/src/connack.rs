//! CONNACK: the server's answer, and the whole of what it declares about
//! itself.
//!
//! ```text
//! variable header
//!   acknowledge flags  seven reserved bits, then Session Present in bit 0
//!   reason code        3.2.2.2
//!   properties         3.2.2.3
//! no payload
//! ```
//!
//! There is exactly one per connection, and the server MUST send it with code
//! 0x00 before any packet other than AUTH ([MQTT-3.2.0-1], [MQTT-3.2.0-2])
//! (3.2) [mqtt5 §1].
//!
//! **Negotiation is this packet, and it is one declaration rather than a round
//! trip.** Each side states its own limits in its own packet and there is no
//! counter-offer [mqtt5 §1]. So everything a client must honour for the rest
//! of the connection — `Receive Maximum`, `Maximum QoS`, `Maximum Packet
//! Size`, `Topic Alias Maximum`, `Server Keep Alive` and the five availability
//! flags — arrives here, in [`Connack::properties`], and their *absence* is
//! meaningful: the defaults of [mqtt5 §11] are what an absent property means,
//! not zero. Applying them is the client's job in `weida-mqtt`; this module's
//! job is to make the difference between absent and present visible, which is
//! why every one of them is an `Option` and none has a default substituted at
//! decode time.
//!
//! Two rules are enforced here because both are facts about the bytes:
//!
//! * the seven reserved bits of the flags byte are 0 ([MQTT-3.2.2-1]);
//! * a non-zero reason code comes with Session Present 0 ([MQTT-3.2.2-6]) —
//!   the combination is a Protocol Error, and a client that trusted it would
//!   believe it had resumed a session the server just refused.
//!
//! The rule this module does **not** enforce is the one that needs client
//! state rather than bytes: "a client with no session state that receives
//! Session Present 1 MUST close the connection" ([MQTT-3.2.2-4]) and one with
//! state that receives 0 MUST discard it ([MQTT-3.2.2-5]) (3.2.2.1.1)
//! [mqtt5 §1]. A sans-I/O codec has no session to compare against; that is
//! the client's session in `weida-mqtt`, and `docs/libraries/mqtt.md` §9.5
//! records how it reads the two rules.

use crate::data::Reader;
use crate::error::{DecodeError, EncodeError};
use crate::property::{Properties, PropertySet};
use crate::reason::ConnectReasonCode;
use crate::types::PacketType;

/// Session Present, bit 0 of the acknowledge flags (3.2.2.1) [mqtt5 §1].
const SESSION_PRESENT: u8 = 0b0000_0001;

/// A CONNACK packet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connack<'a> {
    /// Whether the server is resuming a session for this Client Identifier
    /// (3.2.2.1.1) [mqtt5 §1].
    pub session_present: bool,
    /// The Connect Reason Code (3.2.2.2) [mqtt5 §1].
    pub reason_code: ConnectReasonCode,
    /// The CONNACK properties of 3.2.2.3 — the server's whole declaration.
    pub properties: Properties<'a>,
}

impl Default for Connack<'_> {
    fn default() -> Self {
        Connack {
            session_present: false,
            reason_code: ConnectReasonCode::Success,
            properties: Properties::new(),
        }
    }
}

impl<'a> Connack<'a> {
    /// Decodes the variable header, the fixed header already consumed.
    ///
    /// # Errors
    ///
    /// [`DecodeError::ReservedConnackFlag`],
    /// [`DecodeError::InvalidReasonCode`],
    /// [`DecodeError::SessionPresentWithError`], and anything the property
    /// reader reports.
    pub fn decode_body(reader: &mut Reader<'a>) -> Result<Connack<'a>, DecodeError> {
        let flags = reader.u8()?;
        if flags & !SESSION_PRESENT != 0 {
            return Err(DecodeError::ReservedConnackFlag { flags });
        }
        let session_present = flags & SESSION_PRESENT != 0;

        let reason_code = ConnectReasonCode::from_byte(reader.u8()?)?;
        if session_present && reason_code.is_error() {
            return Err(DecodeError::SessionPresentWithError {
                reason_code: reason_code.as_byte(),
            });
        }

        let properties =
            Properties::decode(reader, PropertySet::CONNACK, Some(PacketType::Connack))?;

        Ok(Connack {
            session_present,
            reason_code,
            properties,
        })
    }

    /// Bytes the variable header occupies, which is the packet's Remaining
    /// Length: CONNACK has no payload.
    ///
    /// # Errors
    ///
    /// Whatever [`Properties::encoded_len`] reports.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        Ok(2 + self.properties.encoded_len(PropertySet::CONNACK)?)
    }

    /// Appends the variable header, the fixed header already written.
    ///
    /// # Errors
    ///
    /// Whatever [`Properties::encode`] reports.
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.push(u8::from(self.session_present));
        out.push(self.reason_code.as_byte());
        self.properties.encode(PropertySet::CONNACK, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::QoS;
    use crate::varint;
    use crate::{Packet, reason};

    fn round_trip(connack: &Connack<'_>) -> Vec<u8> {
        let mut out = Vec::new();
        Packet::Connack(connack.clone())
            .encode(&mut out)
            .expect("encodes");
        let (packet, used) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(used, out.len());
        assert_eq!(packet, Packet::Connack(connack.clone()));
        out
    }

    #[test]
    fn the_minimal_connack_is_vector_three() {
        assert_eq!(
            round_trip(&Connack::default()),
            [0x20, 0x03, 0x00, 0x00, 0x00]
        );
    }

    /// Vector 4: Session Present 1 with a Receive Maximum of 10.
    #[test]
    fn session_present_with_a_property_is_vector_four() {
        let connack = Connack {
            session_present: true,
            properties: Properties {
                receive_maximum: Some(10),
                ..Properties::new()
            },
            ..Connack::default()
        };
        assert_eq!(
            round_trip(&connack),
            [0x20, 0x06, 0x01, 0x00, 0x03, 0x21, 0x00, 0x0A]
        );
    }

    #[test]
    fn the_reserved_flag_bits_are_validated() {
        assert_eq!(
            Packet::decode(&[0x20, 0x03, 0x02, 0x00, 0x00], varint::MAX),
            Err(DecodeError::ReservedConnackFlag { flags: 0x02 })
        );
    }

    /// [MQTT-3.2.2-6]. A client that believed this would think it had resumed
    /// a session the server had in fact refused.
    #[test]
    fn session_present_with_an_error_code_is_a_protocol_error() {
        let error =
            Packet::decode(&[0x20, 0x03, 0x01, 0x87, 0x00], varint::MAX).expect_err("refused");
        assert_eq!(
            error,
            DecodeError::SessionPresentWithError { reason_code: 0x87 }
        );
        assert_eq!(error.reason_code(), Some(crate::error::PROTOCOL_ERROR));

        // Session Present 0 with the same code is fine.
        let (packet, _) = Packet::decode(&[0x20, 0x03, 0x00, 0x87, 0x00], varint::MAX)
            .expect("a refusal decodes");
        let Packet::Connack(connack) = packet else {
            panic!("a connack");
        };
        assert_eq!(connack.reason_code, ConnectReasonCode::NotAuthorized);
        assert!(connack.reason_code.is_error());
    }

    #[test]
    fn an_unlisted_reason_code_is_refused() {
        assert_eq!(
            Packet::decode(&[0x20, 0x03, 0x00, 0x8B, 0x00], varint::MAX),
            Err(DecodeError::InvalidReasonCode {
                packet_type: PacketType::Connack,
                code: 0x8B
            })
        );
    }

    #[test]
    fn every_connack_property_round_trips() {
        let pairs = [("k", "v"), ("k", "w")];
        let connack = Connack {
            session_present: true,
            reason_code: ConnectReasonCode::Success,
            properties: Properties {
                session_expiry_interval: Some(3600),
                receive_maximum: Some(20),
                maximum_qos: Some(QoS::AtLeastOnce),
                retain_available: Some(false),
                maximum_packet_size: Some(2_000_000),
                assigned_client_identifier: Some("auto-1"),
                topic_alias_maximum: Some(10),
                reason_string: Some("welcome"),
                wildcard_subscription_available: Some(true),
                subscription_identifier_available: Some(false),
                shared_subscription_available: Some(true),
                server_keep_alive: Some(30),
                response_information: Some("reply/auto-1"),
                server_reference: Some("other.example:8883"),
                authentication_method: Some("SCRAM-SHA-1"),
                authentication_data: Some(&[0xDE, 0xAD]),
                ..Properties::new()
            }
            .with_user_properties(&pairs),
        };
        round_trip(&connack);
    }

    /// Absence is meaningful: a decoded CONNACK with no properties reports
    /// `None` everywhere rather than substituting the defaults of §11, so a
    /// client can tell "the server said 1" from "the server said nothing".
    #[test]
    fn an_absent_property_stays_absent() {
        let (packet, _) =
            Packet::decode(&[0x20, 0x03, 0x00, 0x00, 0x00], varint::MAX).expect("decodes");
        let Packet::Connack(connack) = packet else {
            panic!("a connack");
        };
        assert_eq!(connack.properties.receive_maximum, None);
        assert_eq!(connack.properties.maximum_qos, None);
        assert_eq!(connack.properties.retain_available, None);
        assert_eq!(connack.properties.server_keep_alive, None);
        assert_eq!(connack.properties.maximum_packet_size, None);
        assert_eq!(connack.properties.topic_alias_maximum, None);
    }

    /// Every code the specification lists survives a round trip in a real
    /// packet, not just through `from_byte`.
    #[test]
    fn every_reason_code_survives_a_packet_round_trip() {
        for &reason_code in reason::CONNECT_REASON_CODES {
            round_trip(&Connack {
                session_present: false,
                reason_code,
                properties: Properties::new(),
            });
        }
    }
}
