//! MQTT 5.0 wire codec: the fixed header, the Variable Byte Integer, the data
//! representations, the property framework and the control packets.
//!
//! This crate is the codec under `weida-mqtt`, the MQTT 5.0 client library. It
//! is **sans-I/O and weida-free**: no sockets, no executor, no dependency — not
//! even on `weida-core`
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.3). What it
//! exports is a byte-for-byte encoder and decoder for what an MQTT 5.0 client
//! and server put on a connection, so that it can be checked against the OASIS
//! standard rather than against weida's opinion of it. `weida-mqtt`, the
//! client, is built on top and depends on both this crate and `weida-runtime`;
//! this one depends on neither.
//!
//! # Shape
//!
//! * [`varint`] — the Variable Byte Integer, 1 to 4 bytes, refused when not
//!   minimally encoded.
//! * [`data`] — the five data representations of chapter 1.5 and the borrowing
//!   [`Reader`] over them, with the 65,535-byte bound that the two-byte length
//!   prefix imposes on strings and binary data.
//! * [`types`] — the fifteen packet types, [`QoS`], and the [`FixedHeader`]
//!   where the maximum-packet-size check lives.
//! * [`property`] — the twenty-seven property identifiers of table 2-4, which
//!   packet carries which, and the two that may repeat.
//! * [`reason`] — a reason-code type per packet, each a closed subset, with
//!   PUBREC's list shared with PUBACK's and PUBCOMP's with PUBREL's.
//! * [`list`] — the payload of the four packets that carry a homogeneous
//!   list: SUBSCRIBE's filters, UNSUBSCRIBE's, and SUBACK's and UNSUBACK's
//!   per-filter verdicts.
//! * [`connect`] / [`connack`] — CONNECT with its Will, and CONNACK with the
//!   whole of what a server declares about itself.
//! * [`publish`] — the only packet carrying an Application Message, and the
//!   only one that uses its fixed-header flags.
//! * [`ack`] — PUBACK, PUBREC, PUBREL and PUBCOMP: two shapes, four packets.
//! * [`subscribe`] / [`unsubscribe`] — the filters and their per-filter
//!   options and verdicts.
//! * [`control`] — PINGREQ, PINGRESP, DISCONNECT and AUTH.
//! * [`packet`] — [`Packet`], one variant per packet type, and the stream
//!   entry points [`Packet::decode`] and [`Packet::encode`].
//!
//! # Reading a connection
//!
//! ```
//! use weida_mqtt_codec::{Connack, Connect, ConnectReasonCode, Packet, Properties};
//!
//! // 1. The client's first packet MUST be CONNECT ([MQTT-3.1.0-1]).
//! let connect = Connect {
//!     client_id: "sensor-1",
//!     clean_start: true,
//!     keep_alive: 60,
//!     properties: Properties {
//!         receive_maximum: Some(20),
//!         ..Properties::new()
//!     },
//!     ..Connect::default()
//! };
//! let mut wire = Vec::new();
//! // 65,536 is this client's own declared ceiling, honoured on the way out.
//! Packet::Connect(connect).encode_within(65_536, &mut wire)?;
//!
//! // 2. The server answers with exactly one CONNACK, and everything the
//! //    client must honour for the rest of the connection is in it.
//! let mut answer = Vec::new();
//! Packet::Connack(Connack {
//!     session_present: false,
//!     reason_code: ConnectReasonCode::Success,
//!     properties: Properties {
//!         receive_maximum: Some(10),
//!         server_keep_alive: Some(30),
//!         ..Properties::new()
//!     },
//!     ..Connack::default()
//! })
//! .encode(&mut answer)?;
//!
//! // 3. A reader decodes with its own ceiling, and learns how far to advance.
//! let (packet, used) = Packet::decode(&answer, 65_536)?;
//! assert_eq!(used, answer.len());
//! let Packet::Connack(connack) = packet else { unreachable!() };
//! assert_eq!(connack.properties.receive_maximum, Some(10));
//! // Server Keep Alive overrides the client's value when present
//! // ([MQTT-3.2.2-21]); absent, the client's stands ([MQTT-3.2.2-22]).
//! assert_eq!(connack.properties.server_keep_alive, Some(30));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Bounds
//!
//! A packet may declare 268,435,455 bytes from the Remaining Length encoding
//! alone (2.1.4) [mqtt5 §3], and `Maximum Packet Size` is absent by default,
//! meaning no limit below that (3.1.2.11.4) [mqtt5 §5]. So the only defence is
//! the local one, and it is a **parameter of every decode** rather than a
//! constant: [`Packet::decode`] and [`FixedHeader::decode`] take
//! `max_packet_size` and refuse an over-large declaration from the fixed
//! header alone, before the body is looked at, let alone reserved.
//!
//! Past that check the decode path makes **one** allocation, and only for a
//! CONNECT that carries a Will: [`Connect::will`] is boxed, because a `Will`
//! holds a second property set and inlining it widened every
//! [`Packet`] variant — the PUBLISH on the hot path included — by 336 bytes
//! for a field declared at most once per connection. Nothing else allocates.
//! Decoding borrows: every string is a `&str` and every binary value a
//! `&[u8]` into the caller's buffer, and the two repeatable properties —
//! `User Property` and `Subscription Identifier` — are not collected but
//! re-walked on demand by [`Properties::user_properties`] and
//! [`Properties::subscription_identifiers`]. A decoded packet is otherwise a
//! fixed-size stack value whatever the peer sent: a thousand user properties
//! cost the same as none.
//!
//! The other bounds are the protocol's own and need no configuration: a UTF-8
//! Encoded String or Binary Data field is capped at 65,535 bytes by its
//! two-byte length prefix (1.5.4, 1.5.6), and a Variable Byte Integer at four
//! bytes (1.5.5) [mqtt5 §3].
//!
//! # Where this codec makes a choice the specification leaves open
//!
//! Each of these is recorded here and in the module it names, with the vectors
//! in `tests/golden_vectors.rs`; what an interop run measured against real
//! brokers is in `docs/libraries/mqtt.md` §10. So a disagreement with a real
//! broker is traceable to a decision rather than to a bug.
//!
//! * **CONNECT's payload order is the normative body's, not Appendix B's.**
//!   3.1.3 lists Client Identifier, Will Properties, Will Topic, Will Payload,
//!   User Name, Password; Appendix B's non-normative example disagrees. See
//!   [`connect`].
//! * **A property a packet type does not carry is a Malformed Packet.** Table
//!   2-4 assigns each identifier to a set of packets without stating the
//!   verdict in one place. See [`property`].
//! * **Disallowed Unicode code points are carried, not refused.** 1.5.3 makes
//!   them a SHOULD NOT, and 5.4.9.2 documents a denial of service against a
//!   *strict* subscriber; a client library is that subscriber, so it is
//!   tolerant. See [`data`].
//! * **An unlisted reason code is refused rather than passed through.** A
//!   client that cannot name a code cannot act on it, and each packet type's
//!   list is closed. See [`reason`].
//! * **Encoding emits properties in ascending identifier order**, though
//!   order between different identifiers is insignificant on the wire. That is
//!   what makes a golden vector binding and every round trip byte-exact; the
//!   repeatable properties keep the caller's order among themselves, which
//!   [MQTT-3.3.2-17] requires.

#![warn(missing_docs)]

pub mod ack;
pub mod connack;
pub mod connect;
pub mod control;
pub mod data;
pub mod error;
pub mod list;
pub mod packet;
pub mod property;
pub mod publish;
pub mod reason;
pub mod subscribe;
pub mod types;
pub mod unsubscribe;
pub mod varint;

pub use ack::{Puback, Pubcomp, Pubrec, Pubrel};
pub use connack::Connack;
pub use connect::{Connect, PROTOCOL_NAME, PROTOCOL_VERSION, Will};
pub use control::{Auth, Disconnect};
pub use data::Reader;
pub use error::{DecodeError, EncodeError};
pub use list::{PayloadItem, PayloadList};
pub use packet::Packet;
pub use property::{PayloadFormat, Properties, PropertyId, PropertySet, Repeated, ValueKind};
pub use publish::Publish;
pub use reason::{
    AuthReasonCode, ConnectReasonCode, DisconnectReasonCode, PubackReasonCode, PubcompReasonCode,
    PubrecReasonCode, PubrelReasonCode, SubackReasonCode, UnsubackReasonCode,
};
pub use subscribe::{RetainHandling, Suback, Subscribe, Subscription, SubscriptionOptions};
pub use types::{FixedHeader, PacketType, QoS};
pub use unsubscribe::{Unsuback, Unsubscribe};
