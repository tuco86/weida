//! CONNECT: the client's first packet, and the Will it carries.
//!
//! ```text
//! variable header
//!   protocol name      "MQTT" as a UTF-8 Encoded String, at a fixed offset
//!   protocol version   0x05
//!   connect flags      UserName Password WillRetain WillQoS(2) Will Clean 0
//!   keep alive         Two Byte Integer, seconds
//!   properties         3.1.2.11
//! payload, in this order (3.1.3)
//!   client identifier  always present, may be zero-length
//!   will properties    if the Will Flag is set (3.1.3.2)
//!   will topic         if the Will Flag is set
//!   will payload       if the Will Flag is set, Binary Data
//!   user name          if the User Name Flag is set
//!   password           if the Password Flag is set, Binary Data
//! ```
//!
//! **The payload order above is the normative body's, not Appendix B's.** This
//! is the one place the specification contradicts itself about this packet:
//! 3.1.3 lists Client Identifier, Will Properties, Will Topic, Will Payload,
//! User Name, Password, and Appendix B's non-normative example puts the Will
//! fields elsewhere. The golden vectors (`tests/golden_vectors.rs`) follow
//! 3.1.3, which is what every implementation on the wire does, and this
//! paragraph keeps the contradiction on the record rather than resolving it
//! silently.
//!
//! **The protocol name is a fixed six bytes and is checked as such.** "The
//! string, its offset and length will not be changed by future versions of the
//! MQTT specification" (3.1.2.1) [mqtt5 §0], so a mismatch is not a version
//! question: it means the peer is not speaking MQTT, and the answer is a
//! Malformed Packet rather than CONNACK 0x84.
//!
//! Three rules 3.1.2 states as MUSTs are enforced here rather than left to a
//! caller, because each is a fact about the bytes:
//!
//! * the reserved bit 0 of the flags byte is 0 ([MQTT-3.1.2-3]);
//! * with the Will Flag clear, Will QoS and Will Retain are both 0
//!   ([MQTT-3.1.2-11], [MQTT-3.1.2-13]);
//! * `Authentication Data` does not appear without `Authentication Method`
//!   ([MQTT-3.1.2-33]).
//!
//! One 3.1.1 restriction is deliberately *not* enforced: 5.0 permits a
//! Password with no User Name, which 3.1.1 forbade (3.1.2.9) [mqtt5 §10], so a
//! Password-only CONNECT encodes and decodes here.

use crate::data::{self, Reader};
use crate::error::{DecodeError, EncodeError};
use crate::property::{Properties, PropertySet};
use crate::types::QoS;

/// The protocol name, length prefix included: the six bytes at a fixed offset
/// that identify MQTT (3.1.2.1) [mqtt5 §0].
pub const PROTOCOL_NAME: [u8; 6] = [0x00, 0x04, b'M', b'Q', b'T', b'T'];

/// The Protocol Version byte for MQTT 5.0 (3.1.2.2) [mqtt5 §0].
pub const PROTOCOL_VERSION: u8 = 5;

/// The Will: an Application Message the server publishes when the connection
/// closes abnormally (3.1.2.5, 3.1.3.2) [mqtt5 §4.5].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Will<'a> {
    /// Will Topic; a Topic Name, so no wildcards.
    pub topic: &'a str,
    /// Will Payload, opaque bytes.
    pub payload: &'a [u8],
    /// Will QoS, from bits 4-3 of the connect flags.
    pub qos: QoS,
    /// Will Retain, from bit 5 of the connect flags.
    pub retain: bool,
    /// Will Properties, including `Will Delay Interval` (3.1.3.2.2).
    pub properties: Properties<'a>,
}

/// A CONNECT packet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Connect<'a> {
    /// Client Identifier: the key to session state, 1..=65,535 bytes, and
    /// zero-length to ask the server to assign one ([MQTT-3.1.3-6])
    /// [mqtt5 §2].
    pub client_id: &'a str,
    /// Clean Start: 1 discards any existing session ([MQTT-3.1.2-4]), 0
    /// resumes one if it exists ([MQTT-3.1.2-5]) [mqtt5 §1].
    pub clean_start: bool,
    /// Keep Alive in seconds; 0 disables (3.1.2.10) [mqtt5 §1].
    pub keep_alive: u16,
    /// The CONNECT properties of 3.1.2.11.
    pub properties: Properties<'a>,
    /// The Will, if the Will Flag is set.
    ///
    /// Boxed, and the reason is arithmetic rather than taste: a `Will`
    /// carries a second [`Properties`] set, so inlining it made `Connect`
    /// — and therefore every [`crate::Packet`] variant, including the
    /// PUBLISH on the hot path — 336 bytes larger than the next largest
    /// packet. A Will is declared at most once per connection and is read
    /// by nothing afterwards, so one allocation buys back that width
    /// everywhere else.
    pub will: Option<Box<Will<'a>>>,
    /// User Name, if the User Name Flag is set.
    pub user_name: Option<&'a str>,
    /// Password, if the Password Flag is set. Binary Data, and "can be used to
    /// carry any credential information" (3.1.3.6) [mqtt5 §10].
    pub password: Option<&'a [u8]>,
}

/// Bit positions in the connect flags byte (3.1.2.3) [mqtt5 §1].
mod flag {
    pub const USER_NAME: u8 = 0b1000_0000;
    pub const PASSWORD: u8 = 0b0100_0000;
    pub const WILL_RETAIN: u8 = 0b0010_0000;
    pub const WILL_QOS: u8 = 0b0001_1000;
    pub const WILL: u8 = 0b0000_0100;
    pub const CLEAN_START: u8 = 0b0000_0010;
    pub const RESERVED: u8 = 0b0000_0001;
}

impl<'a> Connect<'a> {
    /// Decodes the variable header and payload, the fixed header already
    /// consumed.
    ///
    /// # Errors
    ///
    /// [`DecodeError::ProtocolNameInvalid`],
    /// [`DecodeError::UnsupportedProtocolVersion`],
    /// [`DecodeError::ReservedConnectFlag`], [`DecodeError::InvalidQos`],
    /// [`DecodeError::WillFlagsWithoutWill`],
    /// [`DecodeError::AuthenticationDataWithoutMethod`], and anything the
    /// property or data readers report.
    pub fn decode_body(reader: &mut Reader<'a>) -> Result<Connect<'a>, DecodeError> {
        if reader.take(PROTOCOL_NAME.len())? != PROTOCOL_NAME {
            return Err(DecodeError::ProtocolNameInvalid);
        }
        let version = reader.u8()?;
        if version != PROTOCOL_VERSION {
            return Err(DecodeError::UnsupportedProtocolVersion { version });
        }

        let flags = reader.u8()?;
        if flags & flag::RESERVED != 0 {
            return Err(DecodeError::ReservedConnectFlag);
        }
        let has_will = flags & flag::WILL != 0;
        let will_qos = QoS::from_bits((flags & flag::WILL_QOS) >> 3)?;
        let will_retain = flags & flag::WILL_RETAIN != 0;
        if !has_will && (will_qos != QoS::AtMostOnce || will_retain) {
            return Err(DecodeError::WillFlagsWithoutWill);
        }

        let keep_alive = reader.u16()?;
        let properties = Properties::decode(
            reader,
            PropertySet::CONNECT,
            Some(crate::types::PacketType::Connect),
        )?;
        if properties.authentication_data.is_some() && properties.authentication_method.is_none() {
            return Err(DecodeError::AuthenticationDataWithoutMethod);
        }

        let client_id = reader.string()?;
        let will = if has_will {
            Some(Box::new(Will {
                properties: Properties::decode(reader, PropertySet::WILL, None)?,
                topic: reader.string()?,
                payload: reader.binary()?,
                qos: will_qos,
                retain: will_retain,
            }))
        } else {
            None
        };
        let user_name = if flags & flag::USER_NAME != 0 {
            Some(reader.string()?)
        } else {
            None
        };
        let password = if flags & flag::PASSWORD != 0 {
            Some(reader.binary()?)
        } else {
            None
        };

        Ok(Connect {
            client_id,
            clean_start: flags & flag::CLEAN_START != 0,
            keep_alive,
            properties,
            will,
            user_name,
            password,
        })
    }

    /// The connect flags byte this packet encodes to.
    #[must_use]
    pub fn flags(&self) -> u8 {
        let mut flags = 0;
        if self.user_name.is_some() {
            flags |= flag::USER_NAME;
        }
        if self.password.is_some() {
            flags |= flag::PASSWORD;
        }
        if let Some(will) = &self.will {
            flags |= flag::WILL | (will.qos.as_bits() << 3);
            if will.retain {
                flags |= flag::WILL_RETAIN;
            }
        }
        if self.clean_start {
            flags |= flag::CLEAN_START;
        }
        flags
    }

    /// Bytes the variable header and payload occupy, which is the packet's
    /// Remaining Length.
    ///
    /// # Errors
    ///
    /// Whatever [`Connect::encode_body`] would report, before a byte is
    /// written.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        // protocol name (6) + version (1) + flags (1) + keep alive (2).
        let mut len: u32 = 10;
        len += self.properties.encoded_len(PropertySet::CONNECT)?;
        len += data::field_len(self.client_id.len())?;
        if let Some(will) = &self.will {
            len += will.properties.encoded_len(PropertySet::WILL)?;
            len += data::field_len(will.topic.len())?;
            len += data::field_len(will.payload.len())?;
        }
        if let Some(user_name) = self.user_name {
            len += data::field_len(user_name.len())?;
        }
        if let Some(password) = self.password {
            len += data::field_len(password.len())?;
        }
        Ok(len)
    }

    /// Appends the variable header and payload, the fixed header already
    /// written.
    ///
    /// # Errors
    ///
    /// [`EncodeError::FieldTooLong`] for a field above 65,535 bytes, plus
    /// whatever [`Properties::encode`] reports.
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.extend_from_slice(&PROTOCOL_NAME);
        out.push(PROTOCOL_VERSION);
        out.push(self.flags());
        out.extend_from_slice(&self.keep_alive.to_be_bytes());
        self.properties.encode(PropertySet::CONNECT, out)?;

        data::put_string(self.client_id, out)?;
        if let Some(will) = &self.will {
            will.properties.encode(PropertySet::WILL, out)?;
            data::put_string(will.topic, out)?;
            data::put_binary(will.payload, out)?;
        }
        if let Some(user_name) = self.user_name {
            data::put_string(user_name, out)?;
        }
        if let Some(password) = self.password {
            data::put_binary(password, out)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Packet;
    use crate::varint;

    fn round_trip(connect: &Connect<'_>) -> Vec<u8> {
        let mut out = Vec::new();
        Packet::Connect(connect.clone())
            .encode(&mut out)
            .expect("encodes");
        let (packet, used) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(used, out.len());
        assert_eq!(packet, Packet::Connect(connect.clone()));
        out
    }

    #[test]
    fn the_minimal_connect_is_vector_one() {
        let connect = Connect {
            client_id: "a",
            clean_start: true,
            keep_alive: 60,
            ..Connect::default()
        };
        assert_eq!(
            round_trip(&connect),
            [
                0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
                0x01, b'a'
            ]
        );
    }

    #[test]
    fn a_zero_length_client_identifier_is_legal() {
        let connect = Connect {
            client_id: "",
            clean_start: true,
            ..Connect::default()
        };
        round_trip(&connect);
    }

    /// The reserved bit is validated rather than ignored ([MQTT-3.1.2-3]).
    #[test]
    fn the_reserved_flag_is_refused() {
        let mut wire = vec![
            0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x03, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a',
        ];
        assert_eq!(
            Packet::decode(&wire, varint::MAX),
            Err(DecodeError::ReservedConnectFlag)
        );
        // Clearing it makes the same bytes decode.
        wire[9] = 0x02;
        assert!(Packet::decode(&wire, varint::MAX).is_ok());
    }

    /// [MQTT-3.1.2-11] and [MQTT-3.1.2-13]: with no Will, Will QoS and Will
    /// Retain must be zero. This is the check a decoder most easily forgets,
    /// because both fields are simply unread when there is no Will.
    #[test]
    fn will_flags_without_a_will_are_a_protocol_error() {
        for flags in [0x0A /* Will QoS 1 */, 0x22 /* Will Retain */] {
            let wire = [
                0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, flags, 0x00, 0x3C, 0x00,
                0x00, 0x01, b'a',
            ];
            assert_eq!(
                Packet::decode(&wire, varint::MAX),
                Err(DecodeError::WillFlagsWithoutWill),
                "flags 0x{flags:02X}"
            );
        }
    }

    #[test]
    fn a_will_qos_of_three_is_malformed() {
        let wire = [
            0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x1E, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a',
        ];
        assert_eq!(
            Packet::decode(&wire, varint::MAX),
            Err(DecodeError::InvalidQos { qos: 3 })
        );
    }

    #[test]
    fn a_wrong_protocol_name_or_version_is_distinguished() {
        let mut wire = vec![
            0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a',
        ];
        wire[7] = b'X';
        assert_eq!(
            Packet::decode(&wire, varint::MAX),
            Err(DecodeError::ProtocolNameInvalid)
        );

        wire[7] = b'T';
        wire[8] = 4;
        let error = Packet::decode(&wire, varint::MAX).expect_err("3.1.1 is refused");
        assert_eq!(
            error,
            DecodeError::UnsupportedProtocolVersion { version: 4 }
        );
        assert_eq!(
            error.reason_code(),
            Some(crate::error::UNSUPPORTED_PROTOCOL_VERSION)
        );
    }

    /// [MQTT-3.1.2-33]: data without a method.
    #[test]
    fn authentication_data_without_a_method_is_refused() {
        let connect = Connect {
            client_id: "a",
            properties: Properties {
                authentication_data: Some(b"x"),
                ..Properties::new()
            },
            ..Connect::default()
        };
        let mut out = Vec::new();
        Packet::Connect(connect).encode(&mut out).expect("encodes");
        assert_eq!(
            Packet::decode(&out, varint::MAX),
            Err(DecodeError::AuthenticationDataWithoutMethod)
        );
    }

    /// 5.0 dropped 3.1.1's rule that a Password needs a User Name (3.1.2.9).
    #[test]
    fn a_password_without_a_user_name_is_legal_in_five() {
        let connect = Connect {
            client_id: "a",
            password: Some(b"secret"),
            ..Connect::default()
        };
        let wire = round_trip(&connect);
        assert_eq!(wire[9] & flag::USER_NAME, 0);
        assert_ne!(wire[9] & flag::PASSWORD, 0);
    }

    #[test]
    fn every_connect_property_round_trips() {
        let pairs = [("k", "v")];
        let connect = Connect {
            client_id: "c",
            clean_start: false,
            keep_alive: 65_535,
            properties: Properties {
                session_expiry_interval: Some(0xFFFF_FFFF),
                receive_maximum: Some(1),
                maximum_packet_size: Some(268_435_455),
                topic_alias_maximum: Some(10),
                request_response_information: Some(true),
                request_problem_information: Some(false),
                authentication_method: Some("SCRAM-SHA-1"),
                authentication_data: Some(&[0x01, 0x02]),
                ..Properties::new()
            }
            .with_user_properties(&pairs),
            ..Connect::default()
        };
        round_trip(&connect);
    }

    #[test]
    fn every_will_property_round_trips() {
        let pairs = [("wk", "wv")];
        let connect = Connect {
            client_id: "c",
            will: Some(Box::new(Will {
                topic: "d/e",
                payload: b"gone",
                qos: QoS::ExactlyOnce,
                retain: true,
                properties: Properties {
                    will_delay_interval: Some(10),
                    payload_format_indicator: Some(crate::PayloadFormat::Utf8),
                    message_expiry_interval: Some(60),
                    content_type: Some("text/plain"),
                    response_topic: Some("r"),
                    correlation_data: Some(&[0xAA]),
                    ..Properties::new()
                }
                .with_user_properties(&pairs),
            })),
            ..Connect::default()
        };
        let wire = round_trip(&connect);
        // Will Retain (0x20) | Will QoS 2 (0x10) | Will Flag (0x04).
        assert_eq!(wire[9], 0x34);
    }

    /// Golden vector 2 (`tests/golden_vectors.rs`): the payload order of 3.1.3,
    /// with all six fields present.
    #[test]
    fn the_payload_order_is_the_normative_bodys() {
        let connect = Connect {
            client_id: "c",
            clean_start: true,
            keep_alive: 60,
            will: Some(Box::new(Will {
                topic: "d",
                payload: &[0x01],
                qos: QoS::AtLeastOnce,
                retain: false,
                properties: Properties {
                    will_delay_interval: Some(10),
                    ..Properties::new()
                },
            })),
            user_name: Some("u"),
            password: Some(&[0x70]),
            ..Connect::default()
        };
        assert_eq!(
            round_trip(&connect),
            [
                0x10, 0x20, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0xCE, 0x00, 0x3C, 0x00, 0x00,
                0x01, b'c', 0x05, 0x18, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x01, b'd', 0x00, 0x01, 0x01,
                0x00, 0x01, b'u', 0x00, 0x01, 0x70
            ]
        );
    }
}
