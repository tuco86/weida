//! SUBSCRIBE and SUBACK: the filters, the per-filter options, and the
//! per-filter verdicts.
//!
//! ```text
//! SUBSCRIBE (fixed-header flags 0010)
//!   packet identifier  Two Byte Integer
//!   properties         Subscription Identifier, User Property (3.8.2.1)
//!   payload            one or more (Topic Filter, Subscription Options)
//!
//! SUBACK
//!   packet identifier  Two Byte Integer
//!   properties         Reason String, User Property (3.9.2.1)
//!   payload            one Reason Code per filter, in request order (3.9.3)
//! ```
//!
//! **The options byte is where 5.0 put everything 3.1.1 could not say.** A
//! 3.1.1 SUBSCRIBE payload byte is maximum QoS and nothing else, which is why
//! bridging "cannot be expressed in the protocol" there and needs
//! broker-specific loop suppression [mqtt5 §1.9]. 5.0's byte adds No Local and
//! Retain As Published — "primarily defined to allow for message bridge
//! applications" (Appendix C) — and Retain Handling.
//!
//! ```text
//! bit    7   6   5   4   3   2   1   0
//!      +---+---+---+---+---+---+---+---+
//!      | Reserved  | RetainH   |RAP|NL | Maximum QoS |
//!      +---+---+---+---+---+---+---+---+
//! ```
//!
//! Four rules live here because all four are facts about that byte
//! (3.8.3.1) [mqtt5 §4.4]:
//!
//! * bits 6 and 7 are reserved and a non-zero value makes the packet
//!   **malformed** ([MQTT-3.8.3-5]);
//! * a Maximum QoS of 3 is malformed, as everywhere;
//! * a Retain Handling of 3 is a **Protocol Error** — the three defined values
//!   are 0 send retained at subscribe, 1 send only if the subscription did not
//!   already exist, 2 never send at subscribe;
//! * No Local on a Shared Subscription is a **Protocol Error**
//!   ([MQTT-3.8.3-4]), and this is the one of the four that needs the filter
//!   rather than the byte — `$share/` is visible in the same packet, so the
//!   codec checks it rather than deferring a rule it can see.
//!
//! **A SUBSCRIBE with no filters is a Protocol Error** ([MQTT-3.8.3-2]), and
//! SUBACK inherits the rule because one code per filter over zero filters
//! answers nothing.
//!
//! What is *not* here: matching. A filter's `+`/`#` grammar and the
//! specification's worked examples belong to the client that subscribes with
//! them (`weida-mqtt`'s `filter` module), because a codec that rejected an
//! unmatched filter would be refusing bytes a broker is free to accept.

use crate::data::{self, Reader};
use crate::error::{DecodeError, EncodeError};
use crate::list::{PayloadItem, PayloadList};
use crate::property::{Properties, PropertySet};
use crate::publish::non_zero;
use crate::reason::SubackReasonCode;
use crate::types::{PacketType, QoS};

/// The `$share/` prefix that makes a filter a Shared Subscription
/// ([MQTT-4.8.2-1]) [mqtt5 §4.2].
pub const SHARE_PREFIX: &str = "$share/";

/// Retain Handling: what the server sends at subscribe time (3.8.3.1)
/// [mqtt5 §4.4].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RetainHandling {
    /// 0 — send the retained messages at the time of the subscribe.
    #[default]
    SendAtSubscribe = 0,
    /// 1 — send them only if the subscription did not already exist. "Useful
    /// when a reconnect is done and the Client is not certain whether the
    /// subscriptions were completed in the previous connection."
    SendIfNew = 1,
    /// 2 — do not send retained messages at subscribe. For a client that
    /// "wishes to receive change notifications and does not need to know the
    /// initial state".
    DoNotSend = 2,
}

impl RetainHandling {
    /// The value from two bits.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidRetainHandling`] for 3, which "is a Protocol
    /// Error to send" (3.8.3.1) [mqtt5 §4.4].
    pub const fn from_bits(bits: u8) -> Result<RetainHandling, DecodeError> {
        Ok(match bits {
            0 => RetainHandling::SendAtSubscribe,
            1 => RetainHandling::SendIfNew,
            2 => RetainHandling::DoNotSend,
            value => return Err(DecodeError::InvalidRetainHandling { value }),
        })
    }

    /// The two bits this value occupies.
    #[must_use]
    pub const fn as_bits(self) -> u8 {
        self as u8
    }
}

/// One filter's Subscription Options (3.8.3.1) [mqtt5 §4.4].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SubscriptionOptions {
    /// Maximum QoS: the ceiling on delivery to this subscription. The server
    /// reports what it actually granted in the SUBACK code, which may be less
    /// ([MQTT-3.8.4-8]) [mqtt5 §6].
    pub maximum_qos: QoS,
    /// No Local: messages MUST NOT be forwarded to a connection whose Client
    /// Identifier equals the publishing connection's ([MQTT-3.8.3-3]). A
    /// Protocol Error on a Shared Subscription.
    pub no_local: bool,
    /// Retain As Published: 0 the server clears RETAIN when forwarding
    /// ([MQTT-3.3.1-12]), 1 it forwards the flag as published
    /// ([MQTT-3.3.1-13]) [mqtt5 §4.4].
    pub retain_as_published: bool,
    /// Retain Handling.
    pub retain_handling: RetainHandling,
}

impl SubscriptionOptions {
    /// Options requesting `maximum_qos` and the defaults for everything else.
    #[must_use]
    pub const fn new(maximum_qos: QoS) -> SubscriptionOptions {
        SubscriptionOptions {
            maximum_qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAtSubscribe,
        }
    }

    /// The options byte.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        let mut byte = self.maximum_qos.as_bits();
        if self.no_local {
            byte |= 0b0000_0100;
        }
        if self.retain_as_published {
            byte |= 0b0000_1000;
        }
        byte |= self.retain_handling.as_bits() << 4;
        byte
    }

    /// The options from a byte.
    ///
    /// # Errors
    ///
    /// [`DecodeError::ReservedSubscriptionOptionBits`] when bit 6 or 7 is set,
    /// [`DecodeError::InvalidQos`] for a Maximum QoS of 3, and
    /// [`DecodeError::InvalidRetainHandling`] for a Retain Handling of 3.
    pub const fn from_byte(byte: u8) -> Result<SubscriptionOptions, DecodeError> {
        if byte & 0b1100_0000 != 0 {
            return Err(DecodeError::ReservedSubscriptionOptionBits { bits: byte });
        }
        Ok(SubscriptionOptions {
            maximum_qos: match QoS::from_bits(byte & 0b11) {
                Ok(qos) => qos,
                Err(error) => return Err(error),
            },
            no_local: byte & 0b0000_0100 != 0,
            retain_as_published: byte & 0b0000_1000 != 0,
            retain_handling: match RetainHandling::from_bits((byte >> 4) & 0b11) {
                Ok(handling) => handling,
                Err(error) => return Err(error),
            },
        })
    }
}

/// One entry of a SUBSCRIBE payload: a Topic Filter and its options.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Subscription<'a> {
    /// The Topic Filter, which may contain `+` and `#`, and may be a
    /// `$share/{ShareName}/{filter}`.
    pub filter: &'a str,
    /// The filter's options.
    pub options: SubscriptionOptions,
}

impl<'a> Subscription<'a> {
    /// A subscription to `filter` at `maximum_qos`, defaults elsewhere.
    #[must_use]
    pub const fn new(filter: &'a str, maximum_qos: QoS) -> Subscription<'a> {
        Subscription {
            filter,
            options: SubscriptionOptions::new(maximum_qos),
        }
    }

    /// Whether this is a Shared Subscription: the filter begins `$share/`
    /// ([MQTT-4.8.2-1]) [mqtt5 §4.2].
    #[must_use]
    pub fn is_shared(&self) -> bool {
        self.filter.starts_with(SHARE_PREFIX)
    }

    /// [MQTT-3.8.3-4]: No Local MUST NOT be set on a Shared Subscription.
    fn check(&self) -> Result<(), DecodeError> {
        if self.options.no_local && self.is_shared() {
            return Err(DecodeError::NoLocalOnSharedSubscription);
        }
        Ok(())
    }
}

impl<'a> PayloadItem<'a> for Subscription<'a> {
    fn read(reader: &mut Reader<'a>) -> Result<Self, DecodeError> {
        let subscription = Subscription {
            filter: reader.string()?,
            options: SubscriptionOptions::from_byte(reader.u8()?)?,
        };
        subscription.check()?;
        Ok(subscription)
    }

    fn item_len(&self) -> Result<u32, EncodeError> {
        Ok(data::field_len(self.filter.len())? + 1)
    }

    fn put(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        data::put_string(self.filter, out)?;
        out.push(self.options.as_byte());
        Ok(())
    }
}

/// A SUBSCRIBE packet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Subscribe<'a> {
    /// The Packet Identifier; non-zero ([MQTT-2.2.1-3]). Freed on SUBACK
    /// [mqtt5 §2].
    pub packet_id: u16,
    /// The SUBSCRIBE properties: `Subscription Identifier` and
    /// `User Property` (3.8.2.1).
    pub properties: Properties<'a>,
    /// The filters, one or more ([MQTT-3.8.3-2]).
    pub filters: PayloadList<'a, Subscription<'a>>,
}

impl<'a> Subscribe<'a> {
    /// Decodes the variable header and payload.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidPacketIdentifier`], [`DecodeError::EmptyPayload`],
    /// the four options-byte errors of this module, and anything the property
    /// reader reports.
    pub fn decode_body(reader: &mut Reader<'a>) -> Result<Subscribe<'a>, DecodeError> {
        let packet_id = non_zero(reader.u16()?)?;
        let properties =
            Properties::decode(reader, PropertySet::SUBSCRIBE, Some(PacketType::Subscribe))?;
        let filters = PayloadList::decode(reader, PacketType::Subscribe)?;
        Ok(Subscribe {
            packet_id,
            properties,
            filters,
        })
    }

    /// Bytes the variable header and payload occupy.
    ///
    /// # Errors
    ///
    /// [`EncodeError::InvalidPacketIdentifier`], [`EncodeError::EmptyPayload`],
    /// plus whatever the properties and filters report.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        if self.packet_id == 0 {
            return Err(EncodeError::InvalidPacketIdentifier);
        }
        if self.filters.is_empty() {
            return Err(EncodeError::EmptyPayload {
                packet_type: PacketType::Subscribe,
            });
        }
        Ok(2 + self.properties.encoded_len(PropertySet::SUBSCRIBE)? + self.filters.body_len()?)
    }

    /// Appends the variable header and payload.
    ///
    /// # Errors
    ///
    /// As [`Subscribe::body_len`].
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        self.body_len()?;
        out.extend_from_slice(&self.packet_id.to_be_bytes());
        self.properties.encode(PropertySet::SUBSCRIBE, out)?;
        self.filters.encode(out)
    }
}

impl<'a> PayloadItem<'a> for SubackReasonCode {
    fn read(reader: &mut Reader<'a>) -> Result<Self, DecodeError> {
        SubackReasonCode::from_byte(reader.u8()?)
    }

    fn item_len(&self) -> Result<u32, EncodeError> {
        Ok(1)
    }

    fn put(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.push(self.as_byte());
        Ok(())
    }
}

/// A SUBACK packet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Suback<'a> {
    /// The Packet Identifier of the SUBSCRIBE being answered.
    pub packet_id: u16,
    /// The SUBACK properties: `Reason String` and `User Property` (3.9.2.1).
    pub properties: Properties<'a>,
    /// One reason code per filter, in the order the SUBSCRIBE listed them
    /// (3.9.3) [mqtt5 §6]. A failure for one filter leaves the others
    /// unaffected, which is why this is a list and not one code.
    pub reason_codes: PayloadList<'a, SubackReasonCode>,
}

impl<'a> Suback<'a> {
    /// Decodes the variable header and payload.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidPacketIdentifier`], [`DecodeError::EmptyPayload`],
    /// [`DecodeError::InvalidReasonCode`], and anything the property reader
    /// reports.
    pub fn decode_body(reader: &mut Reader<'a>) -> Result<Suback<'a>, DecodeError> {
        let packet_id = non_zero(reader.u16()?)?;
        let properties = Properties::decode(reader, PropertySet::SUBACK, Some(PacketType::Suback))?;
        let reason_codes = PayloadList::decode(reader, PacketType::Suback)?;
        Ok(Suback {
            packet_id,
            properties,
            reason_codes,
        })
    }

    /// Bytes the variable header and payload occupy.
    ///
    /// # Errors
    ///
    /// [`EncodeError::InvalidPacketIdentifier`] or
    /// [`EncodeError::EmptyPayload`], plus whatever the properties report.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        if self.packet_id == 0 {
            return Err(EncodeError::InvalidPacketIdentifier);
        }
        if self.reason_codes.is_empty() {
            return Err(EncodeError::EmptyPayload {
                packet_type: PacketType::Suback,
            });
        }
        Ok(2 + self.properties.encoded_len(PropertySet::SUBACK)? + self.reason_codes.body_len()?)
    }

    /// Appends the variable header and payload.
    ///
    /// # Errors
    ///
    /// As [`Suback::body_len`].
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        self.body_len()?;
        out.extend_from_slice(&self.packet_id.to_be_bytes());
        self.properties.encode(PropertySet::SUBACK, out)?;
        self.reason_codes.encode(out)
    }
}

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

    /// Vector 11: one filter at maximum QoS 1, no options set. The reserved
    /// fixed-header flags `0010` are part of it.
    #[test]
    fn vector_11_a_single_filter() {
        let filters = [Subscription::new("a/+", QoS::AtLeastOnce)];
        round_trip(
            &Packet::Subscribe(Subscribe {
                packet_id: 1,
                properties: Properties::new(),
                filters: PayloadList::new(&filters),
            }),
            &[
                0x82, 0x09, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'+', 0x01,
            ],
        );
    }

    /// Vector 12: a Subscription Identifier, which is a Variable Byte Integer
    /// in the range 1..=268,435,455.
    #[test]
    fn vector_12_a_subscription_identifier() {
        let filters = [Subscription::new("a/+", QoS::AtLeastOnce)];
        let ids = [5u32];
        round_trip(
            &Packet::Subscribe(Subscribe {
                packet_id: 1,
                properties: Properties::new().with_subscription_identifiers(&ids),
                filters: PayloadList::new(&filters),
            }),
            &[
                0x82, 0x0B, 0x00, 0x01, 0x02, 0x0B, 0x05, 0x00, 0x03, b'a', b'/', b'+', 0x01,
            ],
        );
    }

    /// Vector 13: one granted QoS per filter, in request order.
    #[test]
    fn vector_13_a_suback_grants_per_filter() {
        let codes = [SubackReasonCode::GrantedQos1];
        round_trip(
            &Packet::Suback(Suback {
                packet_id: 1,
                properties: Properties::new(),
                reason_codes: PayloadList::new(&codes),
            }),
            &[0x90, 0x04, 0x00, 0x01, 0x00, 0x01],
        );
    }

    /// The options byte, bit by bit, and the round trip that proves every
    /// combination survives.
    #[test]
    fn every_legal_options_byte_round_trips() {
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            for no_local in [false, true] {
                for retain_as_published in [false, true] {
                    for retain_handling in [
                        RetainHandling::SendAtSubscribe,
                        RetainHandling::SendIfNew,
                        RetainHandling::DoNotSend,
                    ] {
                        let options = SubscriptionOptions {
                            maximum_qos: qos,
                            no_local,
                            retain_as_published,
                            retain_handling,
                        };
                        let byte = options.as_byte();
                        assert_eq!(byte & 0b1100_0000, 0, "reserved bits stay clear");
                        assert_eq!(
                            SubscriptionOptions::from_byte(byte),
                            Ok(options),
                            "0x{byte:02X}"
                        );
                    }
                }
            }
        }
    }

    /// [MQTT-3.8.3-5]: reserved bits make the packet malformed, which is a
    /// harder verdict than the Protocol Error the other options faults draw.
    #[test]
    fn reserved_option_bits_are_malformed() {
        for bits in [0b0100_0000u8, 0b1000_0000, 0b1100_0000] {
            let error = SubscriptionOptions::from_byte(bits).expect_err("reserved");
            assert_eq!(
                error,
                DecodeError::ReservedSubscriptionOptionBits { bits },
                "0x{bits:02X}"
            );
            assert_eq!(error.reason_code(), Some(crate::error::MALFORMED_PACKET));
        }
    }

    /// Retain Handling 3 is a Protocol Error, and Maximum QoS 3 is malformed:
    /// two faults in one byte with two different verdicts, which is exactly
    /// what 3.8.3.1 says.
    #[test]
    fn retain_handling_three_and_qos_three_differ_in_verdict() {
        let error = SubscriptionOptions::from_byte(0b0011_0000).expect_err("RH 3");
        assert_eq!(error, DecodeError::InvalidRetainHandling { value: 3 });
        assert_eq!(error.reason_code(), Some(crate::error::PROTOCOL_ERROR));

        let error = SubscriptionOptions::from_byte(0b0000_0011).expect_err("QoS 3");
        assert_eq!(error, DecodeError::InvalidQos { qos: 3 });
        assert_eq!(error.reason_code(), Some(crate::error::MALFORMED_PACKET));
    }

    /// [MQTT-3.8.3-4]: the one options rule that needs the filter, which the
    /// codec has in the same packet.
    #[test]
    fn no_local_on_a_shared_subscription_is_refused() {
        let wire = [
            0x82, 0x12, 0x00, 0x01, 0x00, 0x00, 0x0C, b'$', b's', b'h', b'a', b'r', b'e', b'/',
            b'g', b'/', b'a', b'/', b'+', 0x05,
        ];
        let error = Packet::decode(&wire, varint::MAX).expect_err("refused");
        assert_eq!(error, DecodeError::NoLocalOnSharedSubscription);
        assert_eq!(error.reason_code(), Some(crate::error::PROTOCOL_ERROR));

        // The same filter without No Local is fine, and is recognised as
        // shared.
        let filters = [Subscription::new("$share/g/a/+", QoS::AtLeastOnce)];
        assert!(filters[0].is_shared());
        round_trip(
            &Packet::Subscribe(Subscribe {
                packet_id: 1,
                properties: Properties::new(),
                filters: PayloadList::new(&filters),
            }),
            &[
                0x82, 0x12, 0x00, 0x01, 0x00, 0x00, 0x0C, b'$', b's', b'h', b'a', b'r', b'e', b'/',
                b'g', b'/', b'a', b'/', b'+', 0x01,
            ],
        );
        // And No Local on a non-shared filter is legal, which is what makes
        // the rule about `$share/` rather than about the bit.
        let filters = [Subscription {
            filter: "a/+",
            options: SubscriptionOptions {
                no_local: true,
                ..SubscriptionOptions::new(QoS::AtLeastOnce)
            },
        }];
        round_trip(
            &Packet::Subscribe(Subscribe {
                packet_id: 1,
                properties: Properties::new(),
                filters: PayloadList::new(&filters),
            }),
            &[
                0x82, 0x09, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'+', 0x05,
            ],
        );
    }

    /// [MQTT-3.8.3-2]: no filters is a Protocol Error, in both directions.
    #[test]
    fn an_empty_subscribe_is_refused_both_ways() {
        let error =
            Packet::decode(&[0x82, 0x03, 0x00, 0x01, 0x00], varint::MAX).expect_err("no filters");
        assert_eq!(
            error,
            DecodeError::EmptyPayload {
                packet_type: PacketType::Subscribe
            }
        );
        assert_eq!(error.reason_code(), Some(crate::error::PROTOCOL_ERROR));

        let mut out = Vec::new();
        assert_eq!(
            Packet::Subscribe(Subscribe {
                packet_id: 1,
                ..Subscribe::default()
            })
            .encode(&mut out),
            Err(EncodeError::EmptyPayload {
                packet_type: PacketType::Subscribe
            })
        );
        assert!(out.is_empty());
    }

    #[test]
    fn an_empty_suback_is_refused_both_ways() {
        assert_eq!(
            Packet::decode(&[0x90, 0x03, 0x00, 0x01, 0x00], varint::MAX),
            Err(DecodeError::EmptyPayload {
                packet_type: PacketType::Suback
            })
        );
        let mut out = Vec::new();
        assert_eq!(
            Packet::Suback(Suback {
                packet_id: 1,
                ..Suback::default()
            })
            .encode(&mut out),
            Err(EncodeError::EmptyPayload {
                packet_type: PacketType::Suback
            })
        );
    }

    /// Several filters in one packet, each with its own options and its own
    /// verdict — and one verdict being a failure leaves the others alone
    /// (3.9.3).
    #[test]
    fn many_filters_and_many_verdicts_keep_their_order() {
        let filters = [
            Subscription::new("a", QoS::AtMostOnce),
            Subscription {
                filter: "b/#",
                options: SubscriptionOptions {
                    maximum_qos: QoS::ExactlyOnce,
                    no_local: true,
                    retain_as_published: true,
                    retain_handling: RetainHandling::DoNotSend,
                },
            },
            Subscription::new("$share/g/c/+", QoS::AtLeastOnce),
        ];
        let subscribe = Packet::Subscribe(Subscribe {
            packet_id: 0xBEEF,
            properties: Properties::new(),
            filters: PayloadList::new(&filters),
        });
        let mut out = Vec::new();
        subscribe.encode(&mut out).expect("encodes");
        let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(decoded, subscribe);
        let Packet::Subscribe(decoded) = decoded else {
            panic!("a subscribe")
        };
        assert_eq!(decoded.filters.iter().collect::<Vec<_>>(), filters);

        let codes = [
            SubackReasonCode::GrantedQos0,
            SubackReasonCode::NotAuthorized,
            SubackReasonCode::SharedSubscriptionsNotSupported,
        ];
        let suback = Packet::Suback(Suback {
            packet_id: 0xBEEF,
            properties: Properties::new(),
            reason_codes: PayloadList::new(&codes),
        });
        let mut out = Vec::new();
        suback.encode(&mut out).expect("encodes");
        let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(decoded, suback);
        let Packet::Suback(decoded) = decoded else {
            panic!("a suback")
        };
        assert_eq!(decoded.reason_codes.iter().collect::<Vec<_>>(), codes);
        assert!(!codes[0].is_error() && codes[1].is_error());
    }

    #[test]
    fn every_suback_reason_code_survives_a_round_trip() {
        let codes: Vec<SubackReasonCode> = reason::SUBACK_REASON_CODES.to_vec();
        let suback = Packet::Suback(Suback {
            packet_id: 1,
            properties: Properties::new(),
            reason_codes: PayloadList::new(&codes),
        });
        let mut out = Vec::new();
        suback.encode(&mut out).expect("encodes");
        let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(decoded, suback);
    }

    #[test]
    fn a_zero_packet_identifier_is_refused() {
        assert_eq!(
            Packet::decode(
                &[0x82, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
                varint::MAX
            ),
            Err(DecodeError::InvalidPacketIdentifier)
        );
    }
}
