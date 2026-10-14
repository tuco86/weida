//! The four delivery acknowledgements: PUBACK, PUBREC, PUBREL and PUBCOMP.
//!
//! ```text
//! variable header
//!   packet identifier  Two Byte Integer
//!   reason code        one byte      -- omissible
//!   properties         3.4.2.2       -- omissible
//! no payload
//! ```
//!
//! **Two shapes, not four.** PUBREC's reason codes "are identical to PUBACK's"
//! (3.5.2.1) and PUBCOMP's to PUBREL's (3.7.2.1) [mqtt5 §6], so this module
//! has [`Puback`] and [`Pubrel`] with [`Pubrec`] and [`Pubcomp`] as aliases.
//! That is the specification's own economy rather than ours, and it keeps the
//! QoS 2 handshake's four packets from becoming four near-copies of the same
//! twelve lines.
//!
//! **The short form is not an optimization, it is the wire.** "The Reason Code
//! and Property Length can be omitted if the Reason Code is 0x00 (Success) and
//! there are no Properties. In this case the PUBACK has a Remaining Length of
//! 2" (3.4.1) [mqtt5 §6]. Both forms are legal and mean the same thing, so a
//! decoder MUST accept either and an encoder has to pick: this one emits the
//! short form whenever it applies, which is what makes golden vectors 8 and 10
//! (`tests/golden_vectors.rs`) binding and vector 9 — the long form of the
//! same value — a decode-only vector.
//!
//! What each packet certifies is [mqtt5 §6], and it is worth having in the
//! module that encodes them because it is the one thing about them that is not
//! obvious from the bytes:
//!
//! * **PUBACK** — ownership transfer for this hop. Not that a subscriber
//!   received the message, not that it was persisted, not that it was
//!   processed; the receiver "does not need to complete delivery of the
//!   Application Message before sending the PUBACK".
//! * **PUBREC** — ownership *and* every check that could cause a forwarding
//!   failure, done before accepting. Strictly more than PUBACK, still not
//!   onward delivery.
//! * **PUBREL** — the sender will never send this PUBLISH again, so the
//!   receiver may release its duplicate-suppression state.
//! * **PUBCOMP** — the identifier is released. The handshake terminated, not
//!   that any subscriber saw anything.
//!
//! A reason code of 0x80 or above on PUBACK or PUBREC means the PUBLISH counts
//! as acknowledged and MUST NOT be retransmitted ([MQTT-4.4.0-2]) [mqtt5 §6]:
//! the message is dead with no protocol recourse, which is why
//! [`crate::PubackReasonCode::is_error`] is a decision point and not a
//! diagnostic.

use crate::data::Reader;
use crate::error::{DecodeError, EncodeError};
use crate::property::{Properties, PropertySet};
use crate::publish::non_zero;
use crate::reason::{PubackReasonCode, PubrelReasonCode};
use crate::types::PacketType;

/// Defines one acknowledgement shape: packet identifier, an omissible reason
/// code, and omissible properties.
macro_rules! ack {
    (
        $(#[$meta:meta])*
        $name:ident, $code:ty, $success:expr, $set:expr;
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name<'a> {
            /// The Packet Identifier being acknowledged; non-zero
            /// ([MQTT-2.2.1-3]).
            pub packet_id: u16,
            /// The reason code.
            pub reason_code: $code,
            /// The properties: `Reason String` and `User Property` only.
            pub properties: Properties<'a>,
        }

        impl $name<'_> {
            /// An acknowledgement of `packet_id` with the success code and no
            /// properties — the short-form case.
            #[must_use]
            pub fn new(packet_id: u16) -> Self {
                $name {
                    packet_id,
                    reason_code: $success,
                    properties: Properties::new(),
                }
            }

            /// Whether this value encodes to the two-byte short form: success
            /// and no properties (3.4.1) [mqtt5 §6].
            #[must_use]
            pub fn is_short_form(&self) -> bool {
                self.reason_code == $success && self.properties == Properties::new()
            }
        }

        impl<'a> $name<'a> {
            /// Decodes the variable header, the fixed header already consumed.
            ///
            /// # Errors
            ///
            /// [`DecodeError::InvalidPacketIdentifier`] for 0,
            /// [`DecodeError::InvalidReasonCode`] for a byte outside the
            /// packet's subset, and anything the property reader reports.
            pub fn decode_body(reader: &mut Reader<'a>) -> Result<$name<'a>, DecodeError> {
                let packet_id = non_zero(reader.u16()?)?;

                // The short form: Remaining Length 2, so the reader is spent.
                if reader.is_empty() {
                    return Ok($name {
                        packet_id,
                        reason_code: $success,
                        properties: Properties::new(),
                    });
                }

                let reason_code = <$code>::from_byte(reader.u8()?)?;
                // A reason code with no property length is also legal: 3.4.2.2
                // makes the Property Length omissible along with the code, and
                // a three-byte body is the case where only the code was sent.
                let properties = if reader.is_empty() {
                    Properties::new()
                } else {
                    Properties::decode(reader, $set, Some(PacketType::$name))?
                };

                Ok($name {
                    packet_id,
                    reason_code,
                    properties,
                })
            }

            /// Bytes the variable header occupies.
            ///
            /// # Errors
            ///
            /// [`EncodeError::InvalidPacketIdentifier`] for 0, plus whatever
            /// the properties report.
            pub fn body_len(&self) -> Result<u32, EncodeError> {
                if self.packet_id == 0 {
                    return Err(EncodeError::InvalidPacketIdentifier);
                }
                if self.is_short_form() {
                    return Ok(2);
                }
                Ok(3 + self.properties.encoded_len($set)?)
            }

            /// Appends the variable header, emitting the short form where it
            /// applies.
            ///
            /// # Errors
            ///
            /// As this type's `body_len`.
            pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
                if self.packet_id == 0 {
                    return Err(EncodeError::InvalidPacketIdentifier);
                }
                out.extend_from_slice(&self.packet_id.to_be_bytes());
                if self.is_short_form() {
                    return Ok(());
                }
                out.push(self.reason_code.as_byte());
                self.properties.encode($set, out)
            }
        }
    };
}

ack! {
    /// PUBACK (3.4), the QoS 1 acknowledgement. Also the shape of PUBREC
    /// (3.5), which shares its reason-code list verbatim.
    Puback, PubackReasonCode, PubackReasonCode::Success, PropertySet::PUBACK;
}

ack! {
    /// PUBREL (3.6), the second packet of the QoS 2 handshake. Also the shape
    /// of PUBCOMP (3.7), which shares its reason-code list verbatim.
    Pubrel, PubrelReasonCode, PubrelReasonCode::Success, PropertySet::PUBACK;
}

/// PUBREC has PUBACK's shape and PUBACK's reason codes (3.5.2.1) [mqtt5 §6].
pub type Pubrec<'a> = Puback<'a>;

/// PUBCOMP has PUBREL's shape and PUBREL's reason codes (3.7.2.1) [mqtt5 §6].
pub type Pubcomp<'a> = Pubrel<'a>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint;
    use crate::{Packet, reason};

    #[track_caller]
    fn round_trip(packet: &Packet<'_>, expected: &[u8]) {
        let mut out = Vec::new();
        packet.encode(&mut out).expect("encodes");
        assert_eq!(out, expected, "{:?}", packet.packet_type());
        let (decoded, used) = Packet::decode(expected, varint::MAX).expect("decodes");
        assert_eq!(used, expected.len());
        assert_eq!(&decoded, packet);
    }

    /// Vectors 8 and 10: the short form, for all four packets. PUBREL's
    /// reserved flags `0010` are part of it and are not optional.
    #[test]
    fn the_short_form_is_two_bytes_for_all_four() {
        round_trip(&Packet::Puback(Puback::new(10)), &[0x40, 0x02, 0x00, 0x0A]);
        round_trip(&Packet::Pubrec(Pubrec::new(10)), &[0x50, 0x02, 0x00, 0x0A]);
        round_trip(&Packet::Pubrel(Pubrel::new(10)), &[0x62, 0x02, 0x00, 0x0A]);
        round_trip(
            &Packet::Pubcomp(Pubcomp::new(10)),
            &[0x70, 0x02, 0x00, 0x0A],
        );
    }

    /// Vector 9: the long form of the same value decodes to it, and the
    /// encoder canonicalises back to the short form. Both spellings are legal
    /// (3.4.1), so this is the one vector that is decode-only.
    #[test]
    fn the_long_form_decodes_to_the_same_value_as_the_short_one() {
        let (long, used) = Packet::decode(&[0x40, 0x04, 0x00, 0x0A, 0x00, 0x00], varint::MAX)
            .expect("the long form");
        assert_eq!(used, 6);
        let (short, _) =
            Packet::decode(&[0x40, 0x02, 0x00, 0x0A], varint::MAX).expect("the short form");
        assert_eq!(long, short);

        // And the three-byte middle form: a reason code with no property
        // length at all.
        let (middle, used) =
            Packet::decode(&[0x40, 0x03, 0x00, 0x0A, 0x00], varint::MAX).expect("the middle form");
        assert_eq!(used, 5);
        assert_eq!(middle, short);

        let mut out = Vec::new();
        short.encode(&mut out).expect("encodes");
        assert_eq!(out, [0x40, 0x02, 0x00, 0x0A], "canonicalised to short");
    }

    /// A non-success code, or any property, forces the long form.
    #[test]
    fn a_reason_code_or_a_property_forces_the_long_form() {
        round_trip(
            &Packet::Puback(Puback {
                packet_id: 1,
                reason_code: PubackReasonCode::NoMatchingSubscribers,
                properties: Properties::new(),
            }),
            &[0x40, 0x04, 0x00, 0x01, 0x10, 0x00],
        );

        round_trip(
            &Packet::Pubrec(Pubrec {
                packet_id: 1,
                reason_code: PubackReasonCode::Success,
                properties: Properties {
                    reason_string: Some("ok"),
                    ..Properties::new()
                },
            }),
            &[
                0x50, 0x09, 0x00, 0x01, 0x00, 0x05, 0x1F, 0x00, 0x02, b'o', b'k',
            ],
        );
    }

    #[test]
    fn every_reason_code_survives_a_round_trip() {
        for &reason_code in reason::PUBACK_REASON_CODES {
            let packet = Packet::Puback(Puback {
                packet_id: 7,
                reason_code,
                properties: Properties::new(),
            });
            let mut out = Vec::new();
            packet.encode(&mut out).expect("encodes");
            let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
            assert_eq!(decoded, packet, "0x{:02X}", reason_code.as_byte());
        }
        for &reason_code in reason::PUBREL_REASON_CODES {
            let packet = Packet::Pubcomp(Pubcomp {
                packet_id: 7,
                reason_code,
                properties: Properties::new(),
            });
            let mut out = Vec::new();
            packet.encode(&mut out).expect("encodes");
            let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
            assert_eq!(decoded, packet, "0x{:02X}", reason_code.as_byte());
        }
    }

    /// The subsets are per packet and are enforced: 0x92 is a PUBREL code and
    /// not a PUBACK one, 0x10 the reverse.
    #[test]
    fn a_code_from_the_wrong_subset_is_refused() {
        assert_eq!(
            Packet::decode(&[0x40, 0x04, 0x00, 0x01, 0x92, 0x00], varint::MAX),
            Err(DecodeError::InvalidReasonCode {
                packet_type: PacketType::Puback,
                code: 0x92
            })
        );
        assert_eq!(
            Packet::decode(&[0x62, 0x04, 0x00, 0x01, 0x10, 0x00], varint::MAX),
            Err(DecodeError::InvalidReasonCode {
                packet_type: PacketType::Pubrel,
                code: 0x10
            })
        );
    }

    #[test]
    fn a_zero_packet_identifier_is_refused_both_ways() {
        assert_eq!(
            Packet::decode(&[0x40, 0x02, 0x00, 0x00], varint::MAX),
            Err(DecodeError::InvalidPacketIdentifier)
        );
        let mut out = Vec::new();
        assert_eq!(
            Packet::Puback(Puback::new(0)).encode(&mut out),
            Err(EncodeError::InvalidPacketIdentifier)
        );
    }

    /// PUBREL and PUBCOMP carry only two codes, so the `Reason String` a
    /// server might want to attach still has to ride on one of them.
    #[test]
    fn pubrel_carries_properties_too() {
        round_trip(
            &Packet::Pubrel(Pubrel {
                packet_id: 3,
                reason_code: PubrelReasonCode::PacketIdentifierNotFound,
                properties: Properties {
                    reason_string: Some("no"),
                    ..Properties::new()
                },
            }),
            &[
                0x62, 0x09, 0x00, 0x03, 0x92, 0x05, 0x1F, 0x00, 0x02, b'n', b'o',
            ],
        );
    }
}
