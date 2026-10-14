//! The four packets with no payload: PINGREQ, PINGRESP, DISCONNECT and AUTH.
//!
//! ```text
//! PINGREQ / PINGRESP   fixed header only; Remaining Length 0 (3.12, 3.13)
//!
//! DISCONNECT
//!   reason code        one byte   -- omissible with the properties (3.14.2.1)
//!   properties         3.14.2.2
//!
//! AUTH
//!   reason code        one byte
//!   properties         3.15.2.2
//! ```
//!
//! **PINGREQ and PINGRESP have no variable header, no properties and no
//! payload**, which makes them the two packets in the protocol with no
//! property field at all (2.2.2) [mqtt5 §3] and the reason
//! [`crate::PropertySet::NONE`] exists. They are also the only asymmetric
//! liveness mechanism in MQTT: PINGREQ is client-to-server only, there is no
//! server-initiated ping, and a client with no PINGRESP is told to close
//! "within a reasonable amount of time" with no number given [mqtt5 §1].
//!
//! **DISCONNECT is the packet 5.0 changed most.** In 3.1.1 it flowed
//! client-to-server only and a server reported errors by closing the socket,
//! leaving the client to guess [mqtt5 §1.9]. Now it flows both ways with
//! twenty-nine reason codes, and two of them are the client's own instruction
//! rather than a complaint: `0x00` makes the server discard the Will without
//! publishing it ([MQTT-3.14.4-3]), and `0x04` asks for the Will anyway. A
//! client may also revise its `Session Expiry Interval` here — "so a session's
//! lifetime can be shortened or extended at close" — with two constraints the
//! client owns rather than the codec: a non-zero value when CONNECT carried
//! zero is a Protocol Error, and the *server* MUST NOT send the property at
//! all ([MQTT-3.14.2-2]) [mqtt5 §1]. Both need the CONNECT that came before
//! and the direction of travel, so they belong to the client's session in
//! `weida-mqtt`, which refuses them before sending.
//!
//! **AUTH has no short form here, and that is a decision.** DISCONNECT's
//! reason code and property length are omissible together (3.14.2.1), and this
//! codec both accepts and emits that. AUTH's reason code is always written,
//! because every AUTH MUST carry the same `Authentication Method` as the
//! CONNECT ([MQTT-4.12.0-5]) [mqtt5 §10] — so an AUTH with no properties is a
//! packet that cannot be conformant anyway, and spelling it in two bytes would
//! be spelling something unusable. A peer that sends a zero-length AUTH is
//! answered with [`crate::DecodeError::PacketLengthMismatch`] rather than
//! guessed at; the asymmetry with DISCONNECT is deliberate.

use crate::data::Reader;
use crate::error::{DecodeError, EncodeError};
use crate::property::{Properties, PropertySet};
use crate::reason::{AuthReasonCode, DisconnectReasonCode};
use crate::types::PacketType;

/// A DISCONNECT packet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Disconnect<'a> {
    /// The reason code (3.14.2.1) [mqtt5 §1].
    pub reason_code: DisconnectReasonCode,
    /// The DISCONNECT properties: `Session Expiry Interval`, `Reason String`,
    /// `User Property` and `Server Reference` (3.14.2.2).
    pub properties: Properties<'a>,
}

impl Default for Disconnect<'_> {
    fn default() -> Self {
        Disconnect {
            reason_code: DisconnectReasonCode::NormalDisconnection,
            properties: Properties::new(),
        }
    }
}

impl<'a> Disconnect<'a> {
    /// Whether this value encodes to the empty form: normal disconnection with
    /// no properties, which is a Remaining Length of 0 (3.14.2.1) [mqtt5 §1].
    #[must_use]
    pub fn is_short_form(&self) -> bool {
        self.reason_code == DisconnectReasonCode::NormalDisconnection
            && self.properties == Properties::new()
    }

    /// Decodes the variable header, the fixed header already consumed.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidReasonCode`] and anything the property reader
    /// reports.
    pub fn decode_body(reader: &mut Reader<'a>) -> Result<Disconnect<'a>, DecodeError> {
        if reader.is_empty() {
            return Ok(Disconnect::default());
        }
        let reason_code = DisconnectReasonCode::from_byte(reader.u8()?)?;
        let properties = if reader.is_empty() {
            Properties::new()
        } else {
            Properties::decode(
                reader,
                PropertySet::DISCONNECT,
                Some(PacketType::Disconnect),
            )?
        };
        Ok(Disconnect {
            reason_code,
            properties,
        })
    }

    /// Bytes the variable header occupies.
    ///
    /// # Errors
    ///
    /// Whatever the properties report.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        if self.is_short_form() {
            return Ok(0);
        }
        Ok(1 + self.properties.encoded_len(PropertySet::DISCONNECT)?)
    }

    /// Appends the variable header, emitting the empty form where it applies.
    ///
    /// # Errors
    ///
    /// Whatever the properties report.
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        if self.is_short_form() {
            return Ok(());
        }
        out.push(self.reason_code.as_byte());
        self.properties.encode(PropertySet::DISCONNECT, out)
    }
}

/// An AUTH packet: the enhanced-authentication exchange of 4.12.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Auth<'a> {
    /// The reason code: `Success`, `ContinueAuthentication` or
    /// `ReAuthenticate` (3.15.2.1) [mqtt5 §10].
    pub reason_code: AuthReasonCode,
    /// The AUTH properties: `Authentication Method`, `Authentication Data`,
    /// `Reason String` and `User Property` (3.15.2.2). The method is
    /// effectively mandatory ([MQTT-4.12.0-5]).
    pub properties: Properties<'a>,
}

impl Default for Auth<'_> {
    fn default() -> Self {
        Auth {
            reason_code: AuthReasonCode::ContinueAuthentication,
            properties: Properties::new(),
        }
    }
}

impl<'a> Auth<'a> {
    /// Decodes the variable header, the fixed header already consumed.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidReasonCode`], and
    /// [`DecodeError::PacketLengthMismatch`] for a zero-length AUTH — see the
    /// module documentation for why that is not read as a success.
    pub fn decode_body(reader: &mut Reader<'a>) -> Result<Auth<'a>, DecodeError> {
        let reason_code = AuthReasonCode::from_byte(reader.u8()?)?;
        let properties = if reader.is_empty() {
            Properties::new()
        } else {
            Properties::decode(reader, PropertySet::AUTH, Some(PacketType::Auth))?
        };
        Ok(Auth {
            reason_code,
            properties,
        })
    }

    /// Bytes the variable header occupies.
    ///
    /// # Errors
    ///
    /// Whatever the properties report.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        Ok(1 + self.properties.encoded_len(PropertySet::AUTH)?)
    }

    /// Appends the variable header.
    ///
    /// # Errors
    ///
    /// Whatever the properties report.
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.push(self.reason_code.as_byte());
        self.properties.encode(PropertySet::AUTH, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint;
    use crate::{Packet, PropertyId, reason};

    #[track_caller]
    fn round_trip(packet: &Packet<'_>, expected: &[u8]) {
        let mut out = Vec::new();
        packet.encode(&mut out).expect("encodes");
        assert_eq!(out, expected, "{:?}", packet.packet_type());
        let (decoded, used) = Packet::decode(expected, varint::MAX).expect("decodes");
        assert_eq!(used, expected.len());
        assert_eq!(&decoded, packet);
    }

    /// Vector 16: two bytes each, and no property field at all.
    #[test]
    fn vector_16_the_pings_are_two_bytes() {
        round_trip(&Packet::Pingreq, &[0xC0, 0x00]);
        round_trip(&Packet::Pingresp, &[0xD0, 0x00]);
        assert_eq!(
            PropertySet::for_packet(PacketType::Pingreq),
            PropertySet::NONE
        );
        assert_eq!(
            PropertySet::for_packet(PacketType::Pingresp),
            PropertySet::NONE
        );
    }

    /// The asymmetry: a client pings, a server answers, and neither direction
    /// is reversible (3.12, 3.13).
    #[test]
    fn the_ping_directions_are_fixed() {
        assert!(PacketType::Pingreq.from_client() && !PacketType::Pingreq.from_server());
        assert!(PacketType::Pingresp.from_server() && !PacketType::Pingresp.from_client());
    }

    /// Vector 17: the form that discards the Will ([MQTT-3.14.4-3]).
    #[test]
    fn vector_17_a_normal_disconnect_is_two_bytes() {
        round_trip(&Packet::Disconnect(Disconnect::default()), &[0xE0, 0x00]);
    }

    /// Vector 18: 0x04 asks for the Will anyway, and the Session Expiry
    /// Interval revises the session's lifetime at close.
    #[test]
    fn vector_18_disconnect_with_will_and_a_revised_expiry() {
        round_trip(
            &Packet::Disconnect(Disconnect {
                reason_code: DisconnectReasonCode::DisconnectWithWillMessage,
                properties: Properties {
                    session_expiry_interval: Some(30),
                    ..Properties::new()
                },
            }),
            &[0xE0, 0x07, 0x04, 0x05, 0x11, 0x00, 0x00, 0x00, 0x1E],
        );
    }

    /// The middle form: a reason code with the property length omitted.
    #[test]
    fn a_disconnect_reason_code_alone_decodes() {
        let (decoded, used) = Packet::decode(&[0xE0, 0x01, 0x8E], varint::MAX).expect("decodes");
        assert_eq!(used, 3);
        assert_eq!(
            decoded,
            Packet::Disconnect(Disconnect {
                reason_code: DisconnectReasonCode::SessionTakenOver,
                properties: Properties::new(),
            })
        );
        // It canonicalises to the explicit property length, because the code
        // is not Normal disconnection.
        let mut out = Vec::new();
        decoded.encode(&mut out).expect("encodes");
        assert_eq!(out, [0xE0, 0x02, 0x8E, 0x00]);
    }

    #[test]
    fn every_disconnect_reason_code_survives_a_round_trip() {
        for &reason_code in reason::DISCONNECT_REASON_CODES {
            let packet = Packet::Disconnect(Disconnect {
                reason_code,
                properties: Properties::new(),
            });
            let mut out = Vec::new();
            packet.encode(&mut out).expect("encodes");
            let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
            assert_eq!(decoded, packet, "0x{:02X}", reason_code.as_byte());
        }
    }

    #[test]
    fn every_disconnect_property_round_trips() {
        let pairs = [("k", "v")];
        round_trip(
            &Packet::Disconnect(Disconnect {
                reason_code: DisconnectReasonCode::ServerMoved,
                properties: Properties {
                    session_expiry_interval: Some(0),
                    reason_string: Some("moved"),
                    server_reference: Some("[fe80::1]:1883"),
                    ..Properties::new()
                }
                .with_user_properties(&pairs),
            }),
            &[
                0xE0, 0x27, 0x9D, 0x25, 0x11, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x00, 0x0E, b'[', b'f',
                b'e', b'8', b'0', b':', b':', b'1', b']', b':', b'1', b'8', b'8', b'3', 0x1F, 0x00,
                0x05, b'm', b'o', b'v', b'e', b'd', 0x26, 0x00, 0x01, b'k', 0x00, 0x01, b'v',
            ],
        );
    }

    /// Vector 19: packet type 15, Reserved and Forbidden in 3.1.1, carrying
    /// the method every AUTH must repeat ([MQTT-4.12.0-5]).
    #[test]
    fn vector_19_an_auth_carries_its_method() {
        round_trip(
            &Packet::Auth(Auth {
                reason_code: AuthReasonCode::ContinueAuthentication,
                properties: Properties {
                    authentication_method: Some("SCRAM-SHA-1"),
                    ..Properties::new()
                },
            }),
            &[
                0xF0, 0x10, 0x18, 0x0E, 0x15, 0x00, 0x0B, b'S', b'C', b'R', b'A', b'M', b'-', b'S',
                b'H', b'A', b'-', b'1',
            ],
        );
    }

    #[test]
    fn every_auth_reason_code_survives_a_round_trip() {
        for &reason_code in reason::AUTH_REASON_CODES {
            let packet = Packet::Auth(Auth {
                reason_code,
                properties: Properties {
                    authentication_method: Some("K"),
                    authentication_data: Some(&[0x01]),
                    ..Properties::new()
                },
            });
            let mut out = Vec::new();
            packet.encode(&mut out).expect("encodes");
            let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
            assert_eq!(decoded, packet, "0x{:02X}", reason_code.as_byte());
        }
    }

    /// The decision the module documentation records: AUTH always writes its
    /// reason code, and a zero-length AUTH is refused rather than guessed at.
    #[test]
    fn an_auth_has_no_short_form() {
        let error = Packet::decode(&[0xF0, 0x00], varint::MAX).expect_err("no short form");
        assert_eq!(error, DecodeError::PacketLengthMismatch);
        assert_eq!(error.reason_code(), Some(crate::error::MALFORMED_PACKET));

        // Even an all-default AUTH writes the code, unlike DISCONNECT.
        let mut out = Vec::new();
        Packet::Auth(Auth {
            reason_code: AuthReasonCode::Success,
            properties: Properties::new(),
        })
        .encode(&mut out)
        .expect("encodes");
        assert_eq!(out, [0xF0, 0x02, 0x00, 0x00]);
    }

    /// AUTH's property set is its own: the method and data belong to it,
    /// `Session Expiry Interval` does not.
    #[test]
    fn the_auth_property_set_is_narrow() {
        assert!(PropertySet::AUTH.contains(PropertyId::AuthenticationMethod));
        assert!(PropertySet::AUTH.contains(PropertyId::AuthenticationData));
        assert!(!PropertySet::AUTH.contains(PropertyId::SessionExpiryInterval));
        assert!(PropertySet::DISCONNECT.contains(PropertyId::SessionExpiryInterval));
        assert!(!PropertySet::DISCONNECT.contains(PropertyId::AuthenticationMethod));
    }

    /// A PING with a body is malformed: the fixed header says the packet is
    /// two bytes, so anything after it is not part of this packet.
    #[test]
    fn a_ping_with_a_body_is_refused() {
        let error = Packet::decode(&[0xC0, 0x01, 0x00], varint::MAX).expect_err("refused");
        assert_eq!(error, DecodeError::TrailingBytes { len: 1 });
    }
}
