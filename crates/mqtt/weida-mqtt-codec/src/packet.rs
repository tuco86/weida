//! One control packet, fixed header included — all fifteen of them.
//!
//! [`Packet::decode`] is the entry point a reader loop calls with whatever it
//! has: it returns [`DecodeError::Incomplete`] until the whole packet is
//! present, then the packet and the bytes it consumed. [`Packet::encode`] is
//! the mirror. [`Packet`] is the list of what this codec speaks, so a caller's
//! exhaustive `match` is that list.
//!
//! **The order of operations is the bound.** `decode` reads the fixed header
//! first and therefore learns the declared size before it looks at the body;
//! the caller's `max_packet_size` is applied there, so an over-large
//! declaration is refused having consumed nothing and reserved nothing. Only
//! then is the body required to be present, and the body is *borrowed* rather
//! than copied. Between those two facts, "no remote input can cause unbounded
//! memory allocation" holds for this codec by construction and not by
//! inspection: a decoded packet's size is a few `Option`s and some slices of
//! the caller's own buffer.
//!
//! **A body that does not match its Remaining Length is a Malformed Packet**,
//! not a request for more bytes. The declared length is the peer's own
//! statement about where this packet ends, so bytes left over inside it are
//! [`DecodeError::TrailingBytes`] and a field that runs past it is
//! [`DecodeError::PacketLengthMismatch`]. Getting this backwards is how a
//! decoder deadlocks a connection: it waits for bytes the peer already
//! finished sending.
//!
//! **Three encodings are canonical choices rather than the only legal ones.**
//! PUBACK, PUBREC, PUBREL and PUBCOMP may omit an all-success tail and
//! DISCONNECT may omit everything (3.4.1, 3.14.2.1) [mqtt5 §6], and properties
//! may appear in any order [mqtt5 §3]. This codec decodes every spelling and
//! emits one, which is what makes the golden vectors
//! (`tests/golden_vectors.rs`) binding — and why the fuzz targets assert
//! that a re-encoded packet *decodes* to the same value rather than that it
//! reproduces the same bytes.

use crate::ack::{Puback, Pubcomp, Pubrec, Pubrel};
use crate::connack::Connack;
use crate::connect::Connect;
use crate::control::{Auth, Disconnect};
use crate::data::Reader;
use crate::error::{DecodeError, EncodeError};
use crate::publish::Publish;
use crate::subscribe::{Suback, Subscribe};
use crate::types::{FixedHeader, PacketType};
use crate::unsubscribe::{Unsuback, Unsubscribe};

/// A decoded control packet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet<'a> {
    /// CONNECT (3.1) — the client's first packet.
    Connect(Connect<'a>),
    /// CONNACK (3.2) — exactly one per connection.
    Connack(Connack<'a>),
    /// PUBLISH (3.3) — the only packet carrying an Application Message.
    Publish(Publish<'a>),
    /// PUBACK (3.4) — the QoS 1 acknowledgement.
    Puback(Puback<'a>),
    /// PUBREC (3.5) — QoS 2, part one. Shares PUBACK's shape and codes.
    Pubrec(Pubrec<'a>),
    /// PUBREL (3.6) — QoS 2, part two.
    Pubrel(Pubrel<'a>),
    /// PUBCOMP (3.7) — QoS 2, part three. Shares PUBREL's shape and codes.
    Pubcomp(Pubcomp<'a>),
    /// SUBSCRIBE (3.8).
    Subscribe(Subscribe<'a>),
    /// SUBACK (3.9) — one reason code per filter.
    Suback(Suback<'a>),
    /// UNSUBSCRIBE (3.10).
    Unsubscribe(Unsubscribe<'a>),
    /// UNSUBACK (3.11) — one reason code per filter; new in 5.0.
    Unsuback(Unsuback<'a>),
    /// PINGREQ (3.12) — client to server only; no body at all.
    Pingreq,
    /// PINGRESP (3.13) — server to client only; no body at all.
    Pingresp,
    /// DISCONNECT (3.14) — either direction in 5.0.
    Disconnect(Disconnect<'a>),
    /// AUTH (3.15) — the enhanced-authentication exchange; Reserved in 3.1.1.
    Auth(Auth<'a>),
}

impl<'a> Packet<'a> {
    /// This packet's type.
    #[must_use]
    pub const fn packet_type(&self) -> PacketType {
        match self {
            Packet::Connect(_) => PacketType::Connect,
            Packet::Connack(_) => PacketType::Connack,
            Packet::Publish(_) => PacketType::Publish,
            Packet::Puback(_) => PacketType::Puback,
            Packet::Pubrec(_) => PacketType::Pubrec,
            Packet::Pubrel(_) => PacketType::Pubrel,
            Packet::Pubcomp(_) => PacketType::Pubcomp,
            Packet::Subscribe(_) => PacketType::Subscribe,
            Packet::Suback(_) => PacketType::Suback,
            Packet::Unsubscribe(_) => PacketType::Unsubscribe,
            Packet::Unsuback(_) => PacketType::Unsuback,
            Packet::Pingreq => PacketType::Pingreq,
            Packet::Pingresp => PacketType::Pingresp,
            Packet::Disconnect(_) => PacketType::Disconnect,
            Packet::Auth(_) => PacketType::Auth,
        }
    }

    /// The low nibble of the fixed header this packet encodes to.
    ///
    /// Fixed for fourteen of the fifteen types; PUBLISH derives it from DUP,
    /// QoS and RETAIN (2.1.3) [mqtt5 §3].
    #[must_use]
    pub fn flags(&self) -> u8 {
        match self {
            Packet::Publish(publish) => publish.flags(),
            other => other.packet_type().required_flags().unwrap_or(0),
        }
    }

    /// Decodes one packet from the front of `input`.
    ///
    /// Returns the packet and the bytes it occupied, so a reader loop can
    /// advance its buffer. `max_packet_size` bounds the **whole packet**
    /// (2.1.4) [mqtt5 §3] and is checked from the fixed header alone.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] while bytes are still missing — the one
    /// variant that is not a verdict about the connection. Once the whole
    /// declared packet is present, a field that runs off its end is
    /// [`DecodeError::PacketLengthMismatch`] and bytes left inside it are
    /// [`DecodeError::TrailingBytes`]; both are Malformed Packets. Plus
    /// anything the fixed header, the properties or the packet body report.
    pub fn decode(
        input: &'a [u8],
        max_packet_size: u32,
    ) -> Result<(Packet<'a>, usize), DecodeError> {
        let (header, header_len) = FixedHeader::decode(input, max_packet_size)?;

        let end = header_len
            .checked_add(header.remaining_length as usize)
            .ok_or(DecodeError::Incomplete)?;
        let body = input.get(header_len..end).ok_or(DecodeError::Incomplete)?;

        // Inside `body` the length is settled: the peer said this packet ends
        // here and every one of those bytes has arrived. So running out is the
        // packet contradicting its own header, and reporting `Incomplete` for
        // it would park a reader loop waiting for bytes the peer has already
        // finished sending.
        let mut reader = Reader::new(body);
        let packet =
            Self::decode_body(header.packet_type, header.flags, &mut reader).map_err(exhausted)?;

        if !reader.is_empty() {
            return Err(DecodeError::TrailingBytes {
                len: reader.remaining(),
            });
        }

        Ok((packet, end))
    }

    /// Dispatches the body decoder for `packet_type`.
    fn decode_body(
        packet_type: PacketType,
        flags: u8,
        reader: &mut Reader<'a>,
    ) -> Result<Packet<'a>, DecodeError> {
        Ok(match packet_type {
            PacketType::Connect => Packet::Connect(Connect::decode_body(reader)?),
            PacketType::Connack => Packet::Connack(Connack::decode_body(reader)?),
            PacketType::Publish => Packet::Publish(Publish::decode_body(reader, flags)?),
            PacketType::Puback => Packet::Puback(Puback::decode_body(reader)?),
            PacketType::Pubrec => Packet::Pubrec(Pubrec::decode_body(reader)?),
            PacketType::Pubrel => Packet::Pubrel(Pubrel::decode_body(reader)?),
            PacketType::Pubcomp => Packet::Pubcomp(Pubcomp::decode_body(reader)?),
            PacketType::Subscribe => Packet::Subscribe(Subscribe::decode_body(reader)?),
            PacketType::Suback => Packet::Suback(Suback::decode_body(reader)?),
            PacketType::Unsubscribe => Packet::Unsubscribe(Unsubscribe::decode_body(reader)?),
            PacketType::Unsuback => Packet::Unsuback(Unsuback::decode_body(reader)?),
            PacketType::Pingreq => Packet::Pingreq,
            PacketType::Pingresp => Packet::Pingresp,
            PacketType::Disconnect => Packet::Disconnect(Disconnect::decode_body(reader)?),
            PacketType::Auth => Packet::Auth(Auth::decode_body(reader)?),
        })
    }

    /// Bytes the packet's variable header and payload occupy: its Remaining
    /// Length.
    ///
    /// # Errors
    ///
    /// Whatever the packet body's own length computation reports, before a
    /// byte is written.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        match self {
            Packet::Connect(body) => body.body_len(),
            Packet::Connack(body) => body.body_len(),
            Packet::Publish(body) => body.body_len(),
            Packet::Puback(body) | Packet::Pubrec(body) => body.body_len(),
            Packet::Pubrel(body) | Packet::Pubcomp(body) => body.body_len(),
            Packet::Subscribe(body) => body.body_len(),
            Packet::Suback(body) => body.body_len(),
            Packet::Unsubscribe(body) => body.body_len(),
            Packet::Unsuback(body) => body.body_len(),
            Packet::Pingreq | Packet::Pingresp => Ok(0),
            Packet::Disconnect(body) => body.body_len(),
            Packet::Auth(body) => body.body_len(),
        }
    }

    /// Bytes the whole packet occupies, fixed header included. This is the
    /// number `Maximum Packet Size` bounds.
    ///
    /// # Errors
    ///
    /// As [`Packet::body_len`].
    pub fn encoded_len(&self) -> Result<u32, EncodeError> {
        let body = self.body_len()?;
        Ok(FixedHeader::new(self.packet_type(), body).packet_len() as u32)
    }

    /// Appends the whole packet to `out`.
    ///
    /// # Errors
    ///
    /// Everything [`EncodeError`] lists, each reported before anything is
    /// written.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        let body = self.body_len()?;
        FixedHeader {
            packet_type: self.packet_type(),
            flags: self.flags(),
            remaining_length: body,
        }
        .encode(out)?;

        let before = out.len();
        match self {
            Packet::Connect(body) => body.encode_body(out)?,
            Packet::Connack(body) => body.encode_body(out)?,
            Packet::Publish(body) => body.encode_body(out)?,
            Packet::Puback(body) | Packet::Pubrec(body) => body.encode_body(out)?,
            Packet::Pubrel(body) | Packet::Pubcomp(body) => body.encode_body(out)?,
            Packet::Subscribe(body) => body.encode_body(out)?,
            Packet::Suback(body) => body.encode_body(out)?,
            Packet::Unsubscribe(body) => body.encode_body(out)?,
            Packet::Unsuback(body) => body.encode_body(out)?,
            Packet::Pingreq | Packet::Pingresp => {}
            Packet::Disconnect(body) => body.encode_body(out)?,
            Packet::Auth(body) => body.encode_body(out)?,
        }
        debug_assert_eq!(out.len() - before, body as usize, "body_len agrees");
        Ok(())
    }

    /// Appends the whole packet, refusing one above the peer's declared
    /// `Maximum Packet Size`.
    ///
    /// "The Client MUST NOT send packets exceeding Maximum Packet Size to the
    /// Server" ([MQTT-3.2.2-15]) (3.2.2.3.6) [mqtt5 §5], so a client that uses
    /// this method cannot earn a DISCONNECT 0x95 by accident. The limit is a
    /// parameter rather than state because it is negotiated per connection and
    /// this crate holds none.
    ///
    /// # Errors
    ///
    /// [`EncodeError::PacketTooLarge`] above `max_packet_size`, plus anything
    /// [`Packet::encode`] reports.
    pub fn encode_within(
        &self,
        max_packet_size: u32,
        out: &mut Vec<u8>,
    ) -> Result<(), EncodeError> {
        let size = self.encoded_len()?;
        if size > max_packet_size {
            return Err(EncodeError::PacketTooLarge {
                size,
                max: max_packet_size,
            });
        }
        self.encode(out)
    }
}

/// Inside a packet body whose declared length has fully arrived, "not enough
/// bytes" is the packet contradicting its own Remaining Length: a Malformed
/// Packet, never a request for more.
fn exhausted(error: DecodeError) -> DecodeError {
    match error {
        DecodeError::Incomplete => DecodeError::PacketLengthMismatch,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint;

    /// One packet of every type, for the tests that must cover all fifteen.
    fn one_of_each() -> Vec<Vec<u8>> {
        let mut wires = Vec::new();
        for packet in [
            Packet::Connect(Connect {
                client_id: "a",
                clean_start: true,
                keep_alive: 60,
                ..Connect::default()
            }),
            Packet::Connack(Connack::default()),
            Packet::Publish(Publish {
                topic: "a/b",
                payload: b"hi",
                ..Publish::default()
            }),
            Packet::Puback(Puback::new(10)),
            Packet::Pubrec(Pubrec::new(10)),
            Packet::Pubrel(Pubrel::new(10)),
            Packet::Pubcomp(Pubcomp::new(10)),
            Packet::Pingreq,
            Packet::Pingresp,
            Packet::Disconnect(Disconnect::default()),
        ] {
            let mut out = Vec::new();
            packet.encode(&mut out).expect("encodes");
            wires.push(out);
        }

        // The four with payload lists and AUTH need borrowed operands, so they
        // are built here rather than in the array above.
        use crate::list::PayloadList;
        use crate::property::Properties;
        use crate::reason::{AuthReasonCode, SubackReasonCode, UnsubackReasonCode};
        use crate::subscribe::Subscription;
        use crate::types::QoS;

        let filters = [Subscription::new("a/+", QoS::AtLeastOnce)];
        let names = ["a/+"];
        let sub_codes = [SubackReasonCode::GrantedQos1];
        let unsub_codes = [UnsubackReasonCode::Success];
        for packet in [
            Packet::Subscribe(Subscribe {
                packet_id: 1,
                properties: Properties::new(),
                filters: PayloadList::new(&filters),
            }),
            Packet::Suback(Suback {
                packet_id: 1,
                properties: Properties::new(),
                reason_codes: PayloadList::new(&sub_codes),
            }),
            Packet::Unsubscribe(Unsubscribe {
                packet_id: 2,
                properties: Properties::new(),
                filters: PayloadList::new(&names),
            }),
            Packet::Unsuback(Unsuback {
                packet_id: 2,
                properties: Properties::new(),
                reason_codes: PayloadList::new(&unsub_codes),
            }),
            Packet::Auth(Auth {
                reason_code: AuthReasonCode::ContinueAuthentication,
                properties: Properties {
                    authentication_method: Some("K"),
                    ..Properties::new()
                },
            }),
        ] {
            let mut out = Vec::new();
            packet.encode(&mut out).expect("encodes");
            wires.push(out);
        }

        wires
    }

    /// All fifteen types are reachable, and each reports the type its fixed
    /// header carries. This is the test that catches a dispatch table wired to
    /// the wrong variant, which no per-packet test can see.
    #[test]
    fn all_fifteen_types_decode_to_their_own_variant() {
        let wires = one_of_each();
        assert_eq!(wires.len(), 15, "one packet per type");

        let mut seen = [false; 16];
        for wire in &wires {
            let (packet, used) = Packet::decode(wire, varint::MAX).expect("decodes");
            assert_eq!(used, wire.len());
            let packet_type = packet.packet_type();
            assert_eq!(
                packet_type.as_bits(),
                wire[0] >> 4,
                "{packet_type}: the fixed header agrees with the variant"
            );
            assert!(
                !seen[usize::from(packet_type.as_bits())],
                "{packet_type} appeared twice"
            );
            seen[usize::from(packet_type.as_bits())] = true;
        }
        for bits in 1u8..=15 {
            assert!(seen[usize::from(bits)], "packet type {bits} is not covered");
        }
    }

    /// Every prefix of every packet asks for more bytes and condemns nothing.
    /// That is what a socket does, and over WebSocket the specification makes
    /// it explicit ([MQTT-6.0.0-2]) [mqtt5 §3].
    #[test]
    fn every_short_prefix_of_every_type_is_incomplete() {
        for wire in one_of_each() {
            for len in 0..wire.len() {
                let error = Packet::decode(&wire[..len], varint::MAX).expect_err("short");
                assert_eq!(
                    error,
                    DecodeError::Incomplete,
                    "{:02X?}: prefix of {len} bytes",
                    &wire[..len.min(2)]
                );
                assert!(!error.is_violation());
                assert_eq!(error.reason_code(), None);
            }
        }
    }

    /// The canonical form is a fixed point: encoding what was decoded from a
    /// canonical encoding reproduces it byte for byte.
    #[test]
    fn the_canonical_encoding_is_a_fixed_point_for_every_type() {
        for wire in one_of_each() {
            let (packet, _) = Packet::decode(&wire, varint::MAX).expect("decodes");
            let mut out = Vec::new();
            packet.encode(&mut out).expect("re-encodes");
            assert_eq!(out, wire, "{}", packet.packet_type());
            assert_eq!(packet.encoded_len(), Ok(wire.len() as u32));
        }
    }

    /// Trailing bytes past the fixed header's declared end are left for the
    /// next packet rather than consumed by this one.
    #[test]
    fn bytes_after_a_packet_are_left_for_the_next_one() {
        let mut wire = vec![0xC0, 0x00];
        wire.extend_from_slice(&[0xD0, 0x00]);
        let (first, used) = Packet::decode(&wire, varint::MAX).expect("the first packet");
        assert_eq!((first, used), (Packet::Pingreq, 2));
        let (second, used) = Packet::decode(&wire[2..], varint::MAX).expect("the second");
        assert_eq!((second, used), (Packet::Pingresp, 2));
    }

    /// A Remaining Length longer than the body needs leaves bytes inside the
    /// packet, which is malformed and deliberately not `Incomplete`.
    #[test]
    fn a_body_shorter_than_its_declared_length_is_malformed() {
        let wire = [
            0x10, 0x0F, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a', 0x00,
        ];
        let error = Packet::decode(&wire, varint::MAX).expect_err("refused");
        assert_eq!(error, DecodeError::TrailingBytes { len: 1 });
        assert_eq!(error.reason_code(), Some(crate::error::MALFORMED_PACKET));
    }

    /// A field that runs past the declared end is the same fault from the
    /// other side, and is reported without waiting for bytes the peer has
    /// already stopped sending.
    #[test]
    fn a_field_running_past_the_declared_end_does_not_wait_for_more() {
        let wire = [
            0x10, 0x0D, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a',
        ];
        let error = Packet::decode(&wire, varint::MAX).expect_err("refused");
        assert_eq!(error, DecodeError::PacketLengthMismatch);
        assert_eq!(error.reason_code(), Some(crate::error::MALFORMED_PACKET));
        assert!(error.is_violation());
    }

    #[test]
    fn the_declared_size_is_checked_against_the_callers_maximum() {
        let wire = [
            0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a',
        ];
        assert_eq!(
            Packet::decode(&wire, 15),
            Err(DecodeError::PacketTooLarge { size: 16, max: 15 })
        );
        assert!(Packet::decode(&wire, 16).is_ok());
    }

    /// The mirror on the way out: a packet above the peer's declared maximum
    /// is refused here rather than earning a DISCONNECT 0x95 there.
    #[test]
    fn encoding_refuses_a_packet_above_the_peers_maximum() {
        let packet = Packet::Connect(Connect {
            client_id: "a",
            clean_start: true,
            keep_alive: 60,
            ..Connect::default()
        });
        assert_eq!(packet.encoded_len(), Ok(16));

        let mut out = Vec::new();
        assert_eq!(
            packet.encode_within(15, &mut out),
            Err(EncodeError::PacketTooLarge { size: 16, max: 15 })
        );
        assert!(out.is_empty(), "nothing was written");
        assert!(packet.encode_within(16, &mut out).is_ok());
        assert_eq!(out.len(), 16);
    }

    /// A large declaration over a tiny buffer allocates nothing: if it did,
    /// this test would abort the process rather than fail.
    ///
    /// The ceiling here is 268,435,460 and not [`varint::MAX`], which is the
    /// arithmetic worth pinning: `Maximum Packet Size` bounds the **whole
    /// packet**, so the largest legal packet is the 268,435,455-byte Remaining
    /// Length plus its own five bytes of fixed header (2.1.4).
    #[test]
    fn a_quarter_gigabyte_declaration_over_five_bytes_allocates_nothing() {
        let wire = [0x30, 0xFF, 0xFF, 0xFF, 0x7F];
        assert_eq!(
            Packet::decode(&wire, 268_435_460),
            Err(DecodeError::Incomplete)
        );
        assert_eq!(
            Packet::decode(&wire, varint::MAX),
            Err(DecodeError::PacketTooLarge {
                size: 268_435_460,
                max: varint::MAX
            })
        );
        assert_eq!(
            Packet::decode(&wire, 1024),
            Err(DecodeError::PacketTooLarge {
                size: 268_435_460,
                max: 1024
            })
        );
    }
}
