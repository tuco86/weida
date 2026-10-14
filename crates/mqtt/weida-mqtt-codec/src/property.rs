//! The property framework: the twenty-seven identifiers of table 2-4, their
//! value types, which packet carries which, and the two that may repeat.
//!
//! Thirteen of the fifteen packet types end their variable header with a
//! Variable Byte Integer Property Length — zero if there are none
//! ([MQTT-2.2.2-1]) — followed by identifier/value pairs, and CONNECT carries
//! a second such set, the Will Properties, inside its payload (2.2.2)
//! [mqtt5 §3]. PINGREQ and PINGRESP have no property field at all.
//!
//! **Identifiers are Variable Byte Integers even though all twenty-seven
//! defined ones are a single byte** (2.2.2.2) [mqtt5 §3]. This module decodes
//! and encodes them as Variable Byte Integers rather than as bytes, because
//! the two differ for an unknown identifier: `0x80 0x01` is the identifier
//! 128, and a byte-oriented reader would see `0x80` and then misread the
//! value. Reading it correctly is what makes the rejection of an unknown
//! identifier a *decision* rather than an accident of framing.
//!
//! # The three rules this module enforces
//!
//! 1. **An unknown identifier, or a wrong value type, is a Malformed Packet**,
//!    answered with reason code 0x81 (2.2.2) [mqtt5 §3]. A type is "wrong"
//!    when the value cannot be read at the width the identifier requires,
//!    which inside a property block means the block ends too early — reported
//!    as [`DecodeError::PropertyLengthMismatch`] and never as
//!    [`DecodeError::Incomplete`], because no number of further bytes from the
//!    socket can fix a block whose own declared length is exhausted.
//! 2. **A property the packet type does not carry is a Malformed Packet.**
//!    Table 2-4 assigns each identifier to a set of packets, and this module
//!    turns that table into a [`PropertySet`] bitmask per packet type. The
//!    specification states the assignment without stating the verdict in one
//!    place; this codec's reading is Malformed Packet, recorded here so that a
//!    disagreement with a real broker is traceable to a decision rather than
//!    to a bug (`docs/libraries/mqtt.md` §3).
//! 3. **Repetition is a Protocol Error for every property except `User
//!    Property` and `Subscription Identifier`** (2.2.2, 3.1.2.11.8, 3.3.2.3.8)
//!    [mqtt5 §3], answered with 0x82.
//!
//! Order between different identifiers is insignificant on the wire
//! [mqtt5 §3], so decoding accepts any order. Encoding emits **ascending
//! identifier order**, which makes the golden vectors
//! (`tests/golden_vectors.rs`) binding and every round trip byte-exact;
//! the repeatable properties keep the caller's order among themselves, which
//! is required of User Property ([MQTT-3.3.2-17]) [mqtt5 §3].
//!
//! # Where the allocation bound is, and is not
//!
//! Nowhere in this module. Decoding borrows: every string and every binary
//! value is a slice of the caller's buffer, and the two repeatable properties
//! are not collected into a vector but re-walked on demand by
//! [`Properties::user_properties`] and
//! [`Properties::subscription_identifiers`]. A decoded property set is a
//! fixed-size stack value whatever the peer sent, so the only bound that
//! matters is the one [`crate::FixedHeader::decode`] already applied to the
//! whole packet.

use crate::data::{self, Reader};
use crate::error::{DecodeError, EncodeError};
use crate::types::{PacketType, QoS};
use crate::varint;

/// What a property's value looks like on the wire (2.2.2.2) [mqtt5 §3].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueKind {
    /// One octet, and for most of these only 0 or 1 is legal.
    Byte,
    /// Two Byte Integer.
    TwoByte,
    /// Four Byte Integer.
    FourByte,
    /// Variable Byte Integer; `Subscription Identifier` alone.
    Varint,
    /// UTF-8 Encoded String.
    Utf8,
    /// Binary Data.
    Binary,
    /// UTF-8 String Pair; `User Property` alone.
    Utf8Pair,
}

/// The twenty-seven property identifiers of table 2-4 (2.2.2.2) [mqtt5 §3].
///
/// The discriminant is the wire identifier, which is why the values are not
/// contiguous: the specification leaves gaps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PropertyId {
    /// 0x01, Byte: 0 unspecified bytes, 1 UTF-8 character data (3.3.2.3.2).
    PayloadFormatIndicator = 0x01,
    /// 0x02, Four Byte Integer: seconds the server holds an undelivered copy.
    MessageExpiryInterval = 0x02,
    /// 0x03, UTF-8: MIME by convention, unvalidated beyond well-formedness.
    ContentType = 0x03,
    /// 0x08, UTF-8: where a reply goes (4.10).
    ResponseTopic = 0x08,
    /// 0x09, Binary: opaque, copied into the reply by the responder (4.10).
    CorrelationData = 0x09,
    /// 0x0B, Variable Byte Integer, 1..=268,435,455. **Repeatable.**
    SubscriptionIdentifier = 0x0B,
    /// 0x11, Four Byte Integer: how long session state outlives the
    /// connection (3.1.2.11.2).
    SessionExpiryInterval = 0x11,
    /// 0x12, UTF-8: the identifier the server chose for a zero-length one.
    AssignedClientIdentifier = 0x12,
    /// 0x13, Two Byte Integer: overrides the client's Keep Alive
    /// ([MQTT-3.2.2-21]).
    ServerKeepAlive = 0x13,
    /// 0x15, UTF-8: the enhanced-authentication scheme name (4.12).
    AuthenticationMethod = 0x15,
    /// 0x16, Binary: the challenge or response (4.12).
    AuthenticationData = 0x16,
    /// 0x17, Byte: 0 suppresses Reason String and User Property
    /// ([MQTT-3.1.2-29]).
    RequestProblemInformation = 0x17,
    /// 0x18, Four Byte Integer: delay before the Will is published
    /// (3.1.3.2.2).
    WillDelayInterval = 0x18,
    /// 0x19, Byte: 1 asks the server for Response Information
    /// ([MQTT-3.1.2-28]).
    RequestResponseInformation = 0x19,
    /// 0x1A, UTF-8: the namespace a client builds a Response Topic from
    /// (4.10.2).
    ResponseInformation = 0x1A,
    /// 0x1C, UTF-8: a space-separated redirection list (4.11).
    ServerReference = 0x1C,
    /// 0x1F, UTF-8: human diagnostics, which "SHOULD NOT be parsed".
    ReasonString = 0x1F,
    /// 0x21, Two Byte Integer: the send quota, and 0 is a Protocol Error
    /// (4.9).
    ReceiveMaximum = 0x21,
    /// 0x22, Two Byte Integer: the highest alias the sender may use; default
    /// 0.
    TopicAliasMaximum = 0x22,
    /// 0x23, Two Byte Integer: the alias itself, and 0 is forbidden
    /// ([MQTT-3.3.2-8]).
    TopicAlias = 0x23,
    /// 0x24, Byte: 0 or 1; absence means 2 (3.2.2.3.4).
    MaximumQos = 0x24,
    /// 0x25, Byte: whether the client may set RETAIN (3.2.2.3.5).
    RetainAvailable = 0x25,
    /// 0x26, UTF-8 String Pair. **Repeatable**, and order is preserved
    /// ([MQTT-3.3.2-17]).
    UserProperty = 0x26,
    /// 0x27, Four Byte Integer: total packet bytes, and 0 is a Protocol Error.
    MaximumPacketSize = 0x27,
    /// 0x28, Byte: whether filters may contain `+` or `#` (3.2.2.3.11).
    WildcardSubscriptionAvailable = 0x28,
    /// 0x29, Byte: whether SUBSCRIBE may carry an identifier (3.2.2.3.12).
    SubscriptionIdentifierAvailable = 0x29,
    /// 0x2A, Byte: whether `$share/` filters are accepted (3.2.2.3.13).
    SharedSubscriptionAvailable = 0x2A,
}

/// Every identifier, in ascending wire order. The encoder walks this order, so
/// it is also the order bytes come out in.
pub const ALL: [PropertyId; 27] = [
    PropertyId::PayloadFormatIndicator,
    PropertyId::MessageExpiryInterval,
    PropertyId::ContentType,
    PropertyId::ResponseTopic,
    PropertyId::CorrelationData,
    PropertyId::SubscriptionIdentifier,
    PropertyId::SessionExpiryInterval,
    PropertyId::AssignedClientIdentifier,
    PropertyId::ServerKeepAlive,
    PropertyId::AuthenticationMethod,
    PropertyId::AuthenticationData,
    PropertyId::RequestProblemInformation,
    PropertyId::WillDelayInterval,
    PropertyId::RequestResponseInformation,
    PropertyId::ResponseInformation,
    PropertyId::ServerReference,
    PropertyId::ReasonString,
    PropertyId::ReceiveMaximum,
    PropertyId::TopicAliasMaximum,
    PropertyId::TopicAlias,
    PropertyId::MaximumQos,
    PropertyId::RetainAvailable,
    PropertyId::UserProperty,
    PropertyId::MaximumPacketSize,
    PropertyId::WildcardSubscriptionAvailable,
    PropertyId::SubscriptionIdentifierAvailable,
    PropertyId::SharedSubscriptionAvailable,
];

impl PropertyId {
    /// The identifier this property has on the wire.
    #[must_use]
    pub const fn id(self) -> u32 {
        self as u32
    }

    /// The property with wire identifier `id`.
    ///
    /// # Errors
    ///
    /// [`DecodeError::UnknownProperty`] for anything table 2-4 does not
    /// define, which is a Malformed Packet (2.2.2) [mqtt5 §3].
    pub const fn from_id(id: u32) -> Result<PropertyId, DecodeError> {
        Ok(match id {
            0x01 => PropertyId::PayloadFormatIndicator,
            0x02 => PropertyId::MessageExpiryInterval,
            0x03 => PropertyId::ContentType,
            0x08 => PropertyId::ResponseTopic,
            0x09 => PropertyId::CorrelationData,
            0x0B => PropertyId::SubscriptionIdentifier,
            0x11 => PropertyId::SessionExpiryInterval,
            0x12 => PropertyId::AssignedClientIdentifier,
            0x13 => PropertyId::ServerKeepAlive,
            0x15 => PropertyId::AuthenticationMethod,
            0x16 => PropertyId::AuthenticationData,
            0x17 => PropertyId::RequestProblemInformation,
            0x18 => PropertyId::WillDelayInterval,
            0x19 => PropertyId::RequestResponseInformation,
            0x1A => PropertyId::ResponseInformation,
            0x1C => PropertyId::ServerReference,
            0x1F => PropertyId::ReasonString,
            0x21 => PropertyId::ReceiveMaximum,
            0x22 => PropertyId::TopicAliasMaximum,
            0x23 => PropertyId::TopicAlias,
            0x24 => PropertyId::MaximumQos,
            0x25 => PropertyId::RetainAvailable,
            0x26 => PropertyId::UserProperty,
            0x27 => PropertyId::MaximumPacketSize,
            0x28 => PropertyId::WildcardSubscriptionAvailable,
            0x29 => PropertyId::SubscriptionIdentifierAvailable,
            0x2A => PropertyId::SharedSubscriptionAvailable,
            _ => return Err(DecodeError::UnknownProperty { id }),
        })
    }

    /// The value's wire shape.
    #[must_use]
    pub const fn value_kind(self) -> ValueKind {
        match self {
            PropertyId::PayloadFormatIndicator
            | PropertyId::RequestProblemInformation
            | PropertyId::RequestResponseInformation
            | PropertyId::MaximumQos
            | PropertyId::RetainAvailable
            | PropertyId::WildcardSubscriptionAvailable
            | PropertyId::SubscriptionIdentifierAvailable
            | PropertyId::SharedSubscriptionAvailable => ValueKind::Byte,
            PropertyId::ServerKeepAlive
            | PropertyId::ReceiveMaximum
            | PropertyId::TopicAliasMaximum
            | PropertyId::TopicAlias => ValueKind::TwoByte,
            PropertyId::MessageExpiryInterval
            | PropertyId::SessionExpiryInterval
            | PropertyId::WillDelayInterval
            | PropertyId::MaximumPacketSize => ValueKind::FourByte,
            PropertyId::SubscriptionIdentifier => ValueKind::Varint,
            PropertyId::ContentType
            | PropertyId::ResponseTopic
            | PropertyId::AssignedClientIdentifier
            | PropertyId::AuthenticationMethod
            | PropertyId::ResponseInformation
            | PropertyId::ServerReference
            | PropertyId::ReasonString => ValueKind::Utf8,
            PropertyId::CorrelationData | PropertyId::AuthenticationData => ValueKind::Binary,
            PropertyId::UserProperty => ValueKind::Utf8Pair,
        }
    }

    /// Whether the property may appear more than once. True only for
    /// `User Property` and `Subscription Identifier` [mqtt5 §3].
    #[must_use]
    pub const fn repeatable(self) -> bool {
        matches!(
            self,
            PropertyId::UserProperty | PropertyId::SubscriptionIdentifier
        )
    }

    /// The bit this property occupies in a [`PropertySet`] and in the
    /// duplicate-detection mask. Dense, unlike [`PropertyId::id`].
    const fn bit(self) -> u32 {
        let mut index = 0;
        while index < ALL.len() {
            if ALL[index] as u32 == self as u32 {
                return 1u32 << index;
            }
            index += 1;
        }
        unreachable!()
    }

    /// The specification's name, as it appears in table 2-4.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            PropertyId::PayloadFormatIndicator => "Payload Format Indicator",
            PropertyId::MessageExpiryInterval => "Message Expiry Interval",
            PropertyId::ContentType => "Content Type",
            PropertyId::ResponseTopic => "Response Topic",
            PropertyId::CorrelationData => "Correlation Data",
            PropertyId::SubscriptionIdentifier => "Subscription Identifier",
            PropertyId::SessionExpiryInterval => "Session Expiry Interval",
            PropertyId::AssignedClientIdentifier => "Assigned Client Identifier",
            PropertyId::ServerKeepAlive => "Server Keep Alive",
            PropertyId::AuthenticationMethod => "Authentication Method",
            PropertyId::AuthenticationData => "Authentication Data",
            PropertyId::RequestProblemInformation => "Request Problem Information",
            PropertyId::WillDelayInterval => "Will Delay Interval",
            PropertyId::RequestResponseInformation => "Request Response Information",
            PropertyId::ResponseInformation => "Response Information",
            PropertyId::ServerReference => "Server Reference",
            PropertyId::ReasonString => "Reason String",
            PropertyId::ReceiveMaximum => "Receive Maximum",
            PropertyId::TopicAliasMaximum => "Topic Alias Maximum",
            PropertyId::TopicAlias => "Topic Alias",
            PropertyId::MaximumQos => "Maximum QoS",
            PropertyId::RetainAvailable => "Retain Available",
            PropertyId::UserProperty => "User Property",
            PropertyId::MaximumPacketSize => "Maximum Packet Size",
            PropertyId::WildcardSubscriptionAvailable => "Wildcard Subscription Available",
            PropertyId::SubscriptionIdentifierAvailable => "Subscription Identifier Available",
            PropertyId::SharedSubscriptionAvailable => "Shared Subscription Available",
        }
    }
}

impl core::fmt::Display for PropertyId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{} (0x{:02X})", self.name(), self.id())
    }
}

/// Which properties a packet type carries: table 2-4, as a bitmask.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PropertySet(u32);

macro_rules! set {
    ($($id:ident),* $(,)?) => {
        PropertySet(0 $(| PropertyId::$id.bit())*)
    };
}

impl PropertySet {
    /// No properties at all: PINGREQ and PINGRESP, which have no property
    /// field (3.12, 3.13) [mqtt5 §3].
    pub const NONE: PropertySet = PropertySet(0);

    /// CONNECT (3.1.2.11) [mqtt5 §1].
    pub const CONNECT: PropertySet = set![
        SessionExpiryInterval,
        ReceiveMaximum,
        MaximumPacketSize,
        TopicAliasMaximum,
        RequestResponseInformation,
        RequestProblemInformation,
        UserProperty,
        AuthenticationMethod,
        AuthenticationData,
    ];

    /// The Will Properties inside the CONNECT payload (3.1.3.2) [mqtt5 §1].
    /// Not a packet type, which is why [`DecodeError::PropertyNotAllowed`]
    /// carries `None` for it.
    pub const WILL: PropertySet = set![
        WillDelayInterval,
        PayloadFormatIndicator,
        MessageExpiryInterval,
        ContentType,
        ResponseTopic,
        CorrelationData,
        UserProperty,
    ];

    /// CONNACK (3.2.2.3) [mqtt5 §1].
    pub const CONNACK: PropertySet = set![
        SessionExpiryInterval,
        ReceiveMaximum,
        MaximumQos,
        RetainAvailable,
        MaximumPacketSize,
        AssignedClientIdentifier,
        TopicAliasMaximum,
        ReasonString,
        UserProperty,
        WildcardSubscriptionAvailable,
        SubscriptionIdentifierAvailable,
        SharedSubscriptionAvailable,
        ServerKeepAlive,
        ResponseInformation,
        ServerReference,
        AuthenticationMethod,
        AuthenticationData,
    ];

    /// PUBLISH (3.3.2.3) [mqtt5 §3].
    pub const PUBLISH: PropertySet = set![
        PayloadFormatIndicator,
        MessageExpiryInterval,
        TopicAlias,
        ResponseTopic,
        CorrelationData,
        UserProperty,
        SubscriptionIdentifier,
        ContentType,
    ];

    /// PUBACK, PUBREC, PUBREL and PUBCOMP, which share one set (3.4.2.2 and
    /// its siblings) [mqtt5 §6].
    pub const PUBACK: PropertySet = set![ReasonString, UserProperty];

    /// SUBSCRIBE (3.8.2.1) [mqtt5 §3].
    pub const SUBSCRIBE: PropertySet = set![SubscriptionIdentifier, UserProperty];

    /// SUBACK and UNSUBACK (3.9.2.1, 3.11.2.1) [mqtt5 §3].
    pub const SUBACK: PropertySet = set![ReasonString, UserProperty];

    /// UNSUBSCRIBE (3.10.2.1) [mqtt5 §3].
    pub const UNSUBSCRIBE: PropertySet = set![UserProperty];

    /// DISCONNECT (3.14.2.2) [mqtt5 §1].
    pub const DISCONNECT: PropertySet = set![
        SessionExpiryInterval,
        ReasonString,
        UserProperty,
        ServerReference,
    ];

    /// AUTH (3.15.2.2) [mqtt5 §10].
    pub const AUTH: PropertySet = set![
        AuthenticationMethod,
        AuthenticationData,
        ReasonString,
        UserProperty,
    ];

    /// The set a packet type carries.
    #[must_use]
    pub const fn for_packet(packet_type: PacketType) -> PropertySet {
        match packet_type {
            PacketType::Connect => PropertySet::CONNECT,
            PacketType::Connack => PropertySet::CONNACK,
            PacketType::Publish => PropertySet::PUBLISH,
            PacketType::Puback | PacketType::Pubrec | PacketType::Pubrel | PacketType::Pubcomp => {
                PropertySet::PUBACK
            }
            PacketType::Subscribe => PropertySet::SUBSCRIBE,
            PacketType::Suback | PacketType::Unsuback => PropertySet::SUBACK,
            PacketType::Unsubscribe => PropertySet::UNSUBSCRIBE,
            PacketType::Pingreq | PacketType::Pingresp => PropertySet::NONE,
            PacketType::Disconnect => PropertySet::DISCONNECT,
            PacketType::Auth => PropertySet::AUTH,
        }
    }

    /// Whether the set contains `id`.
    #[must_use]
    pub const fn contains(self, id: PropertyId) -> bool {
        self.0 & id.bit() != 0
    }
}

/// 0 unspecified bytes, 1 UTF-8 character data (3.3.2.3.2) [mqtt5 §3].
///
/// A value other than 0 or 1 is a Protocol Error, so the wire byte is modelled
/// as this enum rather than as a `u8` a caller could put 7 in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PayloadFormat {
    /// 0: unspecified bytes, which "MAY be equivalent to not sending a Payload
    /// Format Indicator".
    #[default]
    Unspecified = 0,
    /// 1: UTF-8 encoded character data. A receiver MAY validate and reject
    /// with 0x99, or MAY not — this codec does not, per `data`'s module
    /// documentation.
    Utf8 = 1,
}

/// Where the two repeatable properties live: `User Property` and
/// `Subscription Identifier`.
///
/// A decoded set holds the property block itself here and re-walks it, so it
/// allocates nothing for however many pairs a peer sent; a set a caller built
/// holds slices the caller owns. The two representations are deliberately not
/// mixable — a value holds one or the other — which is why the fields are
/// private and [`Properties::with_user_properties`] clears the wire block.
#[derive(Clone, Copy, Debug, Default)]
pub struct Repeated<'a> {
    /// The property block a decode borrowed. Empty for a set a caller built.
    wire: &'a [u8],
    /// User properties a caller supplied, in order.
    user: &'a [(&'a str, &'a str)],
    /// Subscription identifiers a caller supplied, in order.
    ids: &'a [u32],
}

impl<'a> Repeated<'a> {
    /// No repeatable properties.
    #[must_use]
    pub const fn new() -> Repeated<'a> {
        Repeated {
            wire: &[],
            user: &[],
            ids: &[],
        }
    }
}

/// A decoded or to-be-encoded property set.
///
/// The twenty-five non-repeatable properties are plain public fields, because
/// they carry no invariant beyond the value ranges that
/// [`Properties::encode`] checks. The two repeatable ones are behind
/// [`Properties::user_properties`] and
/// [`Properties::subscription_identifiers`], because a decoded set re-walks
/// the wire bytes for them and allocates nothing, while a set a caller builds
/// reads them from slices the caller owns.
#[derive(Clone, Debug, Default)]
pub struct Properties<'a> {
    /// 0x01.
    pub payload_format_indicator: Option<PayloadFormat>,
    /// 0x02.
    pub message_expiry_interval: Option<u32>,
    /// 0x03.
    pub content_type: Option<&'a str>,
    /// 0x08.
    pub response_topic: Option<&'a str>,
    /// 0x09.
    pub correlation_data: Option<&'a [u8]>,
    /// 0x11.
    pub session_expiry_interval: Option<u32>,
    /// 0x12.
    pub assigned_client_identifier: Option<&'a str>,
    /// 0x13.
    pub server_keep_alive: Option<u16>,
    /// 0x15.
    pub authentication_method: Option<&'a str>,
    /// 0x16.
    pub authentication_data: Option<&'a [u8]>,
    /// 0x17.
    pub request_problem_information: Option<bool>,
    /// 0x18.
    pub will_delay_interval: Option<u32>,
    /// 0x19.
    pub request_response_information: Option<bool>,
    /// 0x1A.
    pub response_information: Option<&'a str>,
    /// 0x1C.
    pub server_reference: Option<&'a str>,
    /// 0x1F.
    pub reason_string: Option<&'a str>,
    /// 0x21. Non-zero: 0 is a Protocol Error (3.1.2.11.3).
    pub receive_maximum: Option<u16>,
    /// 0x22.
    pub topic_alias_maximum: Option<u16>,
    /// 0x23. Non-zero: alias 0 is forbidden ([MQTT-3.3.2-8]).
    pub topic_alias: Option<u16>,
    /// 0x24. Only [`QoS::AtMostOnce`] or [`QoS::AtLeastOnce`]; absence means 2
    /// (3.2.2.3.4).
    pub maximum_qos: Option<QoS>,
    /// 0x25.
    pub retain_available: Option<bool>,
    /// 0x27. Non-zero: 0 is a Protocol Error (3.1.2.11.4).
    pub maximum_packet_size: Option<u32>,
    /// 0x28.
    pub wildcard_subscription_available: Option<bool>,
    /// 0x29.
    pub subscription_identifier_available: Option<bool>,
    /// 0x2A.
    pub shared_subscription_available: Option<bool>,

    /// The two repeatable properties.
    ///
    /// This field is public so that `..Properties::new()` works in a struct
    /// literal; its contents are not, because a decoded set holds a borrowed
    /// wire block there and a hand-built one holds slices, and mixing the two
    /// would report a property twice. Use
    /// [`Properties::with_user_properties`] and
    /// [`Properties::with_subscription_identifiers`] to set them.
    pub repeated: Repeated<'a>,
}

impl<'a> Properties<'a> {
    /// An empty set.
    #[must_use]
    pub fn new() -> Properties<'a> {
        Properties::default()
    }

    /// The user properties, in wire order.
    #[must_use]
    pub fn user_properties(&self) -> UserProperties<'a> {
        if self.repeated.wire.is_empty() {
            UserProperties(UserPropertiesInner::Slice(self.repeated.user.iter()))
        } else {
            UserProperties(UserPropertiesInner::Wire(Reader::new(self.repeated.wire)))
        }
    }

    /// The subscription identifiers, in wire order.
    #[must_use]
    pub fn subscription_identifiers(&self) -> SubscriptionIdentifiers<'a> {
        if self.repeated.wire.is_empty() {
            SubscriptionIdentifiers(SubscriptionIdentifiersInner::Slice(
                self.repeated.ids.iter(),
            ))
        } else {
            SubscriptionIdentifiers(SubscriptionIdentifiersInner::Wire(Reader::new(
                self.repeated.wire,
            )))
        }
    }

    /// Sets the user properties, in the order they will be encoded.
    ///
    /// This drops any borrowed wire block, so a decoded set whose user
    /// properties are replaced reports the new ones and not both.
    #[must_use]
    pub fn with_user_properties(mut self, pairs: &'a [(&'a str, &'a str)]) -> Properties<'a> {
        self.repeated.wire = &[];
        self.repeated.user = pairs;
        self
    }

    /// Sets the subscription identifiers, in the order they will be encoded.
    ///
    /// Drops any borrowed wire block, for the reason
    /// [`Properties::with_user_properties`] gives.
    #[must_use]
    pub fn with_subscription_identifiers(mut self, ids: &'a [u32]) -> Properties<'a> {
        self.repeated.wire = &[];
        self.repeated.ids = ids;
        self
    }

    /// Decodes a property block from `reader`: a Variable Byte Integer length
    /// followed by that many bytes of identifier/value pairs.
    ///
    /// `allowed` is the packet type's set from table 2-4, and `context` names
    /// the packet type for the error message — `None` for the Will Properties,
    /// which belong to no packet type.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] while the block is still arriving;
    /// [`DecodeError::UnknownProperty`], [`DecodeError::PropertyNotAllowed`]
    /// or [`DecodeError::PropertyLengthMismatch`] for a Malformed Packet; and
    /// [`DecodeError::DuplicateProperty`] or
    /// [`DecodeError::InvalidPropertyValue`] for a Protocol Error.
    pub fn decode(
        reader: &mut Reader<'a>,
        allowed: PropertySet,
        context: Option<PacketType>,
    ) -> Result<Properties<'a>, DecodeError> {
        let len = reader.varint()?;
        let block = reader.take(len as usize)?;

        let mut properties = Properties {
            repeated: Repeated {
                wire: block,
                ..Repeated::new()
            },
            ..Properties::default()
        };
        let mut seen = 0u32;
        let mut inner = Reader::new(block);

        while !inner.is_empty() {
            // Inside the block, running out of bytes is the block lying about
            // its own length: no further socket read can fix it, so it is
            // malformed rather than incomplete.
            let id = PropertyId::from_id(inner.varint().map_err(exhausted)?)?;

            if !allowed.contains(id) {
                return Err(DecodeError::PropertyNotAllowed {
                    id,
                    packet_type: context,
                });
            }
            if !id.repeatable() {
                if seen & id.bit() != 0 {
                    return Err(DecodeError::DuplicateProperty { id });
                }
                seen |= id.bit();
            }

            properties.read_value(id, &mut inner)?;
        }

        Ok(properties)
    }

    /// Reads one property's value and stores it.
    fn read_value(&mut self, id: PropertyId, inner: &mut Reader<'a>) -> Result<(), DecodeError> {
        match id {
            PropertyId::PayloadFormatIndicator => {
                self.payload_format_indicator = Some(match byte(inner)? {
                    0 => PayloadFormat::Unspecified,
                    1 => PayloadFormat::Utf8,
                    value => {
                        return Err(DecodeError::InvalidPropertyValue {
                            id,
                            value: u32::from(value),
                        });
                    }
                });
            }
            PropertyId::MessageExpiryInterval => {
                self.message_expiry_interval = Some(inner.u32().map_err(exhausted)?);
            }
            PropertyId::ContentType => {
                self.content_type = Some(inner.string().map_err(exhausted)?);
            }
            PropertyId::ResponseTopic => {
                self.response_topic = Some(inner.string().map_err(exhausted)?);
            }
            PropertyId::CorrelationData => {
                self.correlation_data = Some(inner.binary().map_err(exhausted)?);
            }
            PropertyId::SubscriptionIdentifier => {
                let value = inner.varint().map_err(exhausted)?;
                if value == 0 {
                    return Err(DecodeError::InvalidPropertyValue { id, value });
                }
                // Repeatable: the value stays in `wire` and is re-read by
                // `subscription_identifiers`, so nothing is stored here.
            }
            PropertyId::SessionExpiryInterval => {
                self.session_expiry_interval = Some(inner.u32().map_err(exhausted)?);
            }
            PropertyId::AssignedClientIdentifier => {
                self.assigned_client_identifier = Some(inner.string().map_err(exhausted)?);
            }
            PropertyId::ServerKeepAlive => {
                self.server_keep_alive = Some(inner.u16().map_err(exhausted)?);
            }
            PropertyId::AuthenticationMethod => {
                self.authentication_method = Some(inner.string().map_err(exhausted)?);
            }
            PropertyId::AuthenticationData => {
                self.authentication_data = Some(inner.binary().map_err(exhausted)?);
            }
            PropertyId::RequestProblemInformation => {
                self.request_problem_information = Some(boolean(id, inner)?);
            }
            PropertyId::WillDelayInterval => {
                self.will_delay_interval = Some(inner.u32().map_err(exhausted)?);
            }
            PropertyId::RequestResponseInformation => {
                self.request_response_information = Some(boolean(id, inner)?);
            }
            PropertyId::ResponseInformation => {
                self.response_information = Some(inner.string().map_err(exhausted)?);
            }
            PropertyId::ServerReference => {
                self.server_reference = Some(inner.string().map_err(exhausted)?);
            }
            PropertyId::ReasonString => {
                self.reason_string = Some(inner.string().map_err(exhausted)?);
            }
            PropertyId::ReceiveMaximum => {
                self.receive_maximum = Some(non_zero_u16(id, inner)?);
            }
            PropertyId::TopicAliasMaximum => {
                self.topic_alias_maximum = Some(inner.u16().map_err(exhausted)?);
            }
            PropertyId::TopicAlias => {
                self.topic_alias = Some(non_zero_u16(id, inner)?);
            }
            PropertyId::MaximumQos => {
                self.maximum_qos = Some(match byte(inner)? {
                    0 => QoS::AtMostOnce,
                    1 => QoS::AtLeastOnce,
                    value => {
                        return Err(DecodeError::InvalidPropertyValue {
                            id,
                            value: u32::from(value),
                        });
                    }
                });
            }
            PropertyId::RetainAvailable => {
                self.retain_available = Some(boolean(id, inner)?);
            }
            PropertyId::UserProperty => {
                // Repeatable: validate both halves, store neither.
                inner.string().map_err(exhausted)?;
                inner.string().map_err(exhausted)?;
            }
            PropertyId::MaximumPacketSize => {
                let value = inner.u32().map_err(exhausted)?;
                if value == 0 {
                    return Err(DecodeError::InvalidPropertyValue { id, value });
                }
                self.maximum_packet_size = Some(value);
            }
            PropertyId::WildcardSubscriptionAvailable => {
                self.wildcard_subscription_available = Some(boolean(id, inner)?);
            }
            PropertyId::SubscriptionIdentifierAvailable => {
                self.subscription_identifier_available = Some(boolean(id, inner)?);
            }
            PropertyId::SharedSubscriptionAvailable => {
                self.shared_subscription_available = Some(boolean(id, inner)?);
            }
        }
        Ok(())
    }

    /// Bytes the property block's **body** occupies, excluding its own length
    /// prefix.
    ///
    /// # Errors
    ///
    /// The same conditions [`Properties::encode`] reports, checked before a
    /// byte is written.
    pub fn body_len(&self, allowed: PropertySet) -> Result<u32, EncodeError> {
        self.write_body(allowed, None)
    }

    /// Bytes the whole property block occupies, length prefix included.
    ///
    /// # Errors
    ///
    /// As [`Properties::body_len`].
    pub fn encoded_len(&self, allowed: PropertySet) -> Result<u32, EncodeError> {
        let body = self.body_len(allowed)?;
        Ok(body + varint::encoded_len(body) as u32)
    }

    /// Appends the property block: the Variable Byte Integer length, then the
    /// identifier/value pairs in ascending identifier order.
    ///
    /// # Errors
    ///
    /// [`EncodeError::PropertyNotAllowed`] for a property `allowed` does not
    /// contain, [`EncodeError::InvalidPropertyValue`] for a value outside its
    /// range, [`EncodeError::FieldTooLong`] for a string or binary value above
    /// 65,535 bytes, and [`EncodeError::InvalidSubscriptionIdentifier`] for
    /// one outside 1..=268,435,455.
    pub fn encode(&self, allowed: PropertySet, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        let body = self.body_len(allowed)?;
        varint::encode(body, out)?;
        let before = out.len();
        self.write_body(allowed, Some(out))?;
        debug_assert_eq!(out.len() - before, body as usize, "body_len agrees");
        Ok(())
    }

    /// The single table both the length computation and the encoder walk.
    ///
    /// `out` is `None` to measure and `Some` to write, so the order of
    /// properties and the set of checks cannot drift apart between the two.
    fn write_body(
        &self,
        allowed: PropertySet,
        mut out: Option<&mut Vec<u8>>,
    ) -> Result<u32, EncodeError> {
        let mut len: u32 = 0;

        macro_rules! emit {
            ($id:expr, $size:expr, $write:expr) => {{
                let id: PropertyId = $id;
                if !allowed.contains(id) {
                    return Err(EncodeError::PropertyNotAllowed { id });
                }
                len += varint::encoded_len(id.id()) as u32 + $size;
                if let Some(out) = out.as_deref_mut() {
                    varint::encode(id.id(), out)?;
                    let write = $write;
                    write(out)?;
                }
            }};
        }

        if let Some(value) = self.payload_format_indicator {
            emit!(PropertyId::PayloadFormatIndicator, 1, |out: &mut Vec<
                u8,
            >| {
                out.push(value as u8);
                Ok(())
            });
        }
        if let Some(value) = self.message_expiry_interval {
            emit!(PropertyId::MessageExpiryInterval, 4, |out: &mut Vec<u8>| {
                out.extend_from_slice(&value.to_be_bytes());
                Ok(())
            });
        }
        if let Some(value) = self.content_type {
            emit!(
                PropertyId::ContentType,
                data::field_len(value.len())?,
                |out: &mut Vec<u8>| data::put_string(value, out)
            );
        }
        if let Some(value) = self.response_topic {
            emit!(
                PropertyId::ResponseTopic,
                data::field_len(value.len())?,
                |out: &mut Vec<u8>| data::put_string(value, out)
            );
        }
        if let Some(value) = self.correlation_data {
            emit!(
                PropertyId::CorrelationData,
                data::field_len(value.len())?,
                |out: &mut Vec<u8>| data::put_binary(value, out)
            );
        }
        for value in self.subscription_identifiers() {
            if value == 0 || value > varint::MAX {
                return Err(EncodeError::InvalidSubscriptionIdentifier { value });
            }
            emit!(
                PropertyId::SubscriptionIdentifier,
                varint::encoded_len(value) as u32,
                |out: &mut Vec<u8>| varint::encode(value, out)
            );
        }
        if let Some(value) = self.session_expiry_interval {
            emit!(PropertyId::SessionExpiryInterval, 4, |out: &mut Vec<u8>| {
                out.extend_from_slice(&value.to_be_bytes());
                Ok(())
            });
        }
        if let Some(value) = self.assigned_client_identifier {
            emit!(
                PropertyId::AssignedClientIdentifier,
                data::field_len(value.len())?,
                |out: &mut Vec<u8>| data::put_string(value, out)
            );
        }
        if let Some(value) = self.server_keep_alive {
            emit!(PropertyId::ServerKeepAlive, 2, |out: &mut Vec<u8>| {
                out.extend_from_slice(&value.to_be_bytes());
                Ok(())
            });
        }
        if let Some(value) = self.authentication_method {
            emit!(
                PropertyId::AuthenticationMethod,
                data::field_len(value.len())?,
                |out: &mut Vec<u8>| data::put_string(value, out)
            );
        }
        if let Some(value) = self.authentication_data {
            emit!(
                PropertyId::AuthenticationData,
                data::field_len(value.len())?,
                |out: &mut Vec<u8>| data::put_binary(value, out)
            );
        }
        if let Some(value) = self.request_problem_information {
            emit!(PropertyId::RequestProblemInformation, 1, |out: &mut Vec<
                u8,
            >| {
                out.push(u8::from(value));
                Ok(())
            });
        }
        if let Some(value) = self.will_delay_interval {
            emit!(PropertyId::WillDelayInterval, 4, |out: &mut Vec<u8>| {
                out.extend_from_slice(&value.to_be_bytes());
                Ok(())
            });
        }
        if let Some(value) = self.request_response_information {
            emit!(
                PropertyId::RequestResponseInformation,
                1,
                |out: &mut Vec<u8>| {
                    out.push(u8::from(value));
                    Ok(())
                }
            );
        }
        if let Some(value) = self.response_information {
            emit!(
                PropertyId::ResponseInformation,
                data::field_len(value.len())?,
                |out: &mut Vec<u8>| data::put_string(value, out)
            );
        }
        if let Some(value) = self.server_reference {
            emit!(
                PropertyId::ServerReference,
                data::field_len(value.len())?,
                |out: &mut Vec<u8>| data::put_string(value, out)
            );
        }
        if let Some(value) = self.reason_string {
            emit!(
                PropertyId::ReasonString,
                data::field_len(value.len())?,
                |out: &mut Vec<u8>| data::put_string(value, out)
            );
        }
        if let Some(value) = self.receive_maximum {
            if value == 0 {
                return Err(EncodeError::InvalidPropertyValue {
                    id: PropertyId::ReceiveMaximum,
                    value: 0,
                });
            }
            emit!(PropertyId::ReceiveMaximum, 2, |out: &mut Vec<u8>| {
                out.extend_from_slice(&value.to_be_bytes());
                Ok(())
            });
        }
        if let Some(value) = self.topic_alias_maximum {
            emit!(PropertyId::TopicAliasMaximum, 2, |out: &mut Vec<u8>| {
                out.extend_from_slice(&value.to_be_bytes());
                Ok(())
            });
        }
        if let Some(value) = self.topic_alias {
            if value == 0 {
                return Err(EncodeError::InvalidPropertyValue {
                    id: PropertyId::TopicAlias,
                    value: 0,
                });
            }
            emit!(PropertyId::TopicAlias, 2, |out: &mut Vec<u8>| {
                out.extend_from_slice(&value.to_be_bytes());
                Ok(())
            });
        }
        if let Some(value) = self.maximum_qos {
            if value == QoS::ExactlyOnce {
                return Err(EncodeError::InvalidPropertyValue {
                    id: PropertyId::MaximumQos,
                    value: 2,
                });
            }
            emit!(PropertyId::MaximumQos, 1, |out: &mut Vec<u8>| {
                out.push(value.as_bits());
                Ok(())
            });
        }
        if let Some(value) = self.retain_available {
            emit!(PropertyId::RetainAvailable, 1, |out: &mut Vec<u8>| {
                out.push(u8::from(value));
                Ok(())
            });
        }
        for (key, value) in self.user_properties() {
            emit!(
                PropertyId::UserProperty,
                data::field_len(key.len())? + data::field_len(value.len())?,
                |out: &mut Vec<u8>| {
                    data::put_string(key, out)?;
                    data::put_string(value, out)
                }
            );
        }
        if let Some(value) = self.maximum_packet_size {
            if value == 0 {
                return Err(EncodeError::InvalidPropertyValue {
                    id: PropertyId::MaximumPacketSize,
                    value: 0,
                });
            }
            emit!(PropertyId::MaximumPacketSize, 4, |out: &mut Vec<u8>| {
                out.extend_from_slice(&value.to_be_bytes());
                Ok(())
            });
        }
        if let Some(value) = self.wildcard_subscription_available {
            emit!(
                PropertyId::WildcardSubscriptionAvailable,
                1,
                |out: &mut Vec<u8>| {
                    out.push(u8::from(value));
                    Ok(())
                }
            );
        }
        if let Some(value) = self.subscription_identifier_available {
            emit!(
                PropertyId::SubscriptionIdentifierAvailable,
                1,
                |out: &mut Vec<u8>| {
                    out.push(u8::from(value));
                    Ok(())
                }
            );
        }
        if let Some(value) = self.shared_subscription_available {
            emit!(
                PropertyId::SharedSubscriptionAvailable,
                1,
                |out: &mut Vec<u8>| {
                    out.push(u8::from(value));
                    Ok(())
                }
            );
        }

        Ok(len)
    }
}

/// Inside a property block, "not enough bytes" is the block contradicting its
/// own declared length, which is malformed and not a request for more.
fn exhausted(error: DecodeError) -> DecodeError {
    match error {
        DecodeError::Incomplete => DecodeError::PropertyLengthMismatch,
        other => other,
    }
}

fn byte(inner: &mut Reader<'_>) -> Result<u8, DecodeError> {
    inner.u8().map_err(exhausted)
}

fn boolean(id: PropertyId, inner: &mut Reader<'_>) -> Result<bool, DecodeError> {
    match byte(inner)? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(DecodeError::InvalidPropertyValue {
            id,
            value: u32::from(value),
        }),
    }
}

fn non_zero_u16(id: PropertyId, inner: &mut Reader<'_>) -> Result<u16, DecodeError> {
    let value = inner.u16().map_err(exhausted)?;
    if value == 0 {
        return Err(DecodeError::InvalidPropertyValue {
            id,
            value: u32::from(value),
        });
    }
    Ok(value)
}

/// Two property sets are equal when their scalars and their repeatable
/// properties are, whichever representation each is holding.
impl PartialEq for Properties<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.payload_format_indicator == other.payload_format_indicator
            && self.message_expiry_interval == other.message_expiry_interval
            && self.content_type == other.content_type
            && self.response_topic == other.response_topic
            && self.correlation_data == other.correlation_data
            && self.session_expiry_interval == other.session_expiry_interval
            && self.assigned_client_identifier == other.assigned_client_identifier
            && self.server_keep_alive == other.server_keep_alive
            && self.authentication_method == other.authentication_method
            && self.authentication_data == other.authentication_data
            && self.request_problem_information == other.request_problem_information
            && self.will_delay_interval == other.will_delay_interval
            && self.request_response_information == other.request_response_information
            && self.response_information == other.response_information
            && self.server_reference == other.server_reference
            && self.reason_string == other.reason_string
            && self.receive_maximum == other.receive_maximum
            && self.topic_alias_maximum == other.topic_alias_maximum
            && self.topic_alias == other.topic_alias
            && self.maximum_qos == other.maximum_qos
            && self.retain_available == other.retain_available
            && self.maximum_packet_size == other.maximum_packet_size
            && self.wildcard_subscription_available == other.wildcard_subscription_available
            && self.subscription_identifier_available == other.subscription_identifier_available
            && self.shared_subscription_available == other.shared_subscription_available
            && self.user_properties().eq(other.user_properties())
            && self
                .subscription_identifiers()
                .eq(other.subscription_identifiers())
    }
}

impl Eq for Properties<'_> {}

enum UserPropertiesInner<'a> {
    Wire(Reader<'a>),
    Slice(core::slice::Iter<'a, (&'a str, &'a str)>),
}

/// The user properties of a property set, in wire order.
pub struct UserProperties<'a>(UserPropertiesInner<'a>);

impl<'a> Iterator for UserProperties<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<(&'a str, &'a str)> {
        match &mut self.0 {
            UserPropertiesInner::Slice(iter) => iter.next().copied(),
            UserPropertiesInner::Wire(reader) => walk(reader, PropertyId::UserProperty, |reader| {
                Some((reader.string().ok()?, reader.string().ok()?))
            }),
        }
    }
}

enum SubscriptionIdentifiersInner<'a> {
    Wire(Reader<'a>),
    Slice(core::slice::Iter<'a, u32>),
}

/// The subscription identifiers of a property set, in wire order.
pub struct SubscriptionIdentifiers<'a>(SubscriptionIdentifiersInner<'a>);

impl Iterator for SubscriptionIdentifiers<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        match &mut self.0 {
            SubscriptionIdentifiersInner::Slice(iter) => iter.next().copied(),
            SubscriptionIdentifiersInner::Wire(reader) => {
                walk(reader, PropertyId::SubscriptionIdentifier, |reader| {
                    reader.varint().ok()
                })
            }
        }
    }
}

/// Walks a validated property block to the next occurrence of `wanted`.
///
/// The block came from [`Properties::decode`], which already proved every
/// identifier known, permitted and well-formed, so a read that fails here can
/// only mean the end of the block: the walk stops rather than panicking, which
/// keeps the iterators total.
fn walk<'a, T>(
    reader: &mut Reader<'a>,
    wanted: PropertyId,
    take: impl Fn(&mut Reader<'a>) -> Option<T>,
) -> Option<T> {
    while !reader.is_empty() {
        let id = PropertyId::from_id(reader.varint().ok()?).ok()?;
        if id == wanted {
            return take(reader);
        }
        skip(reader, id)?;
    }
    None
}

/// Steps over one property's value.
fn skip(reader: &mut Reader<'_>, id: PropertyId) -> Option<()> {
    match id.value_kind() {
        ValueKind::Byte => {
            reader.u8().ok()?;
        }
        ValueKind::TwoByte => {
            reader.u16().ok()?;
        }
        ValueKind::FourByte => {
            reader.u32().ok()?;
        }
        ValueKind::Varint => {
            reader.varint().ok()?;
        }
        ValueKind::Utf8 => {
            reader.string().ok()?;
        }
        ValueKind::Binary => {
            reader.binary().ok()?;
        }
        ValueKind::Utf8Pair => {
            reader.string().ok()?;
            reader.string().ok()?;
        }
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8], allowed: PropertySet) -> Result<Properties<'_>, DecodeError> {
        let mut reader = Reader::new(bytes);
        let properties = Properties::decode(&mut reader, allowed, Some(PacketType::Connect))?;
        assert!(reader.is_empty(), "the whole block was consumed");
        Ok(properties)
    }

    #[test]
    fn an_empty_block_is_one_zero_byte() {
        let properties = decode(&[0x00], PropertySet::CONNECT).expect("an empty set");
        assert_eq!(properties, Properties::new());

        let mut out = Vec::new();
        Properties::new()
            .encode(PropertySet::CONNECT, &mut out)
            .expect("encodes");
        assert_eq!(out, [0x00]);
    }

    /// Table 2-4 has twenty-seven rows, each with a distinct identifier and a
    /// distinct dense bit. A duplicate in either would silently merge two
    /// properties.
    #[test]
    fn the_twenty_seven_identifiers_are_distinct_in_both_numberings() {
        let mut ids: Vec<u32> = ALL.iter().map(|id| id.id()).collect();
        let mut bits: Vec<u32> = ALL.iter().map(|id| id.bit()).collect();
        assert_eq!(ids.len(), 27);
        ids.sort_unstable();
        let sorted = ids.clone();
        ids.dedup();
        assert_eq!(ids.len(), 27, "identifiers are distinct");
        assert_eq!(sorted, ids, "ALL is in ascending identifier order");
        bits.sort_unstable();
        bits.dedup();
        assert_eq!(bits.len(), 27, "dense bits are distinct");

        for id in ALL {
            assert_eq!(PropertyId::from_id(id.id()), Ok(id), "{id}");
        }
    }

    #[test]
    fn an_unknown_identifier_is_malformed() {
        assert_eq!(
            decode(&[0x02, 0x7F, 0x00], PropertySet::CONNECT),
            Err(DecodeError::UnknownProperty { id: 0x7F })
        );
        // 0x04 sits in one of table 2-4's gaps.
        assert_eq!(
            PropertyId::from_id(0x04),
            Err(DecodeError::UnknownProperty { id: 0x04 })
        );
    }

    /// A multi-byte identifier is read as a Variable Byte Integer, so 128 is
    /// rejected as the identifier 128 rather than misparsed as 0x80 plus a
    /// stray value byte.
    #[test]
    fn an_identifier_is_a_variable_byte_integer() {
        assert_eq!(
            decode(&[0x02, 0x80, 0x01], PropertySet::CONNECT),
            Err(DecodeError::UnknownProperty { id: 128 })
        );
    }

    #[test]
    fn a_property_the_packet_does_not_carry_is_malformed() {
        // Will Delay Interval belongs to the Will, not to CONNECT itself.
        assert_eq!(
            decode(&[0x05, 0x18, 0x00, 0x00, 0x00, 0x0A], PropertySet::CONNECT),
            Err(DecodeError::PropertyNotAllowed {
                id: PropertyId::WillDelayInterval,
                packet_type: Some(PacketType::Connect),
            })
        );
        // And it is accepted in the Will set.
        let properties =
            decode(&[0x05, 0x18, 0x00, 0x00, 0x00, 0x0A], PropertySet::WILL).expect("a will set");
        assert_eq!(properties.will_delay_interval, Some(10));
    }

    #[test]
    fn repetition_is_a_protocol_error_except_for_the_two() {
        let twice = [
            0x0A, 0x11, 0x00, 0x00, 0x00, 0x1E, 0x11, 0x00, 0x00, 0x00, 0x1F,
        ];
        let error = decode(&twice, PropertySet::CONNECT).expect_err("refused");
        assert_eq!(
            error,
            DecodeError::DuplicateProperty {
                id: PropertyId::SessionExpiryInterval
            }
        );
        assert_eq!(error.reason_code(), Some(crate::error::PROTOCOL_ERROR));

        // Two user properties are fine, and both are reported in order.
        let pairs = [
            0x0E, 0x26, 0x00, 0x01, b'a', 0x00, 0x01, b'1', 0x26, 0x00, 0x01, b'b', 0x00, 0x01,
            b'2',
        ];
        let properties = decode(&pairs, PropertySet::CONNECT).expect("two pairs");
        assert_eq!(
            properties.user_properties().collect::<Vec<_>>(),
            [("a", "1"), ("b", "2")]
        );
    }

    /// The block's declared length is authoritative: a value that runs off its
    /// end is malformed, and is deliberately not `Incomplete`, because waiting
    /// for more socket bytes would be waiting forever.
    #[test]
    fn a_value_running_past_the_block_is_a_length_mismatch() {
        assert_eq!(
            decode(&[0x02, 0x11, 0x00], PropertySet::CONNECT),
            Err(DecodeError::PropertyLengthMismatch)
        );
    }

    /// A block whose length exceeds the bytes present is still `Incomplete`:
    /// that one a further read can fix.
    #[test]
    fn a_block_longer_than_the_input_asks_for_bytes() {
        let mut reader = Reader::new(&[0x08, 0x11, 0x00]);
        assert_eq!(
            Properties::decode(&mut reader, PropertySet::CONNECT, None),
            Err(DecodeError::Incomplete)
        );
    }

    #[test]
    fn the_zero_and_range_rules_are_enforced() {
        for (bytes, id, value) in [
            (
                &[0x03u8, 0x21, 0x00, 0x00][..],
                PropertyId::ReceiveMaximum,
                0,
            ),
            (
                &[0x05, 0x27, 0x00, 0x00, 0x00, 0x00][..],
                PropertyId::MaximumPacketSize,
                0,
            ),
        ] {
            assert_eq!(
                decode(bytes, PropertySet::CONNECT),
                Err(DecodeError::InvalidPropertyValue { id, value }),
                "{id}"
            );
        }

        // A boolean-valued property that is neither 0 nor 1.
        assert_eq!(
            decode(&[0x02, 0x17, 0x02], PropertySet::CONNECT),
            Err(DecodeError::InvalidPropertyValue {
                id: PropertyId::RequestProblemInformation,
                value: 2
            })
        );
        // Maximum QoS 2 is a Protocol Error: a server that supports 2 omits it.
        let mut reader = Reader::new(&[0x02, 0x24, 0x02]);
        assert_eq!(
            Properties::decode(&mut reader, PropertySet::CONNACK, Some(PacketType::Connack)),
            Err(DecodeError::InvalidPropertyValue {
                id: PropertyId::MaximumQos,
                value: 2
            })
        );
        // Topic Alias 0 and Subscription Identifier 0.
        let mut reader = Reader::new(&[0x03, 0x23, 0x00, 0x00]);
        assert_eq!(
            Properties::decode(&mut reader, PropertySet::PUBLISH, Some(PacketType::Publish)),
            Err(DecodeError::InvalidPropertyValue {
                id: PropertyId::TopicAlias,
                value: 0
            })
        );
        let mut reader = Reader::new(&[0x02, 0x0B, 0x00]);
        assert_eq!(
            Properties::decode(&mut reader, PropertySet::PUBLISH, Some(PacketType::Publish)),
            Err(DecodeError::InvalidPropertyValue {
                id: PropertyId::SubscriptionIdentifier,
                value: 0
            })
        );
    }

    /// Decoding accepts any order; encoding emits ascending identifier order,
    /// which is what makes a golden vector binding.
    #[test]
    fn order_is_insignificant_inbound_and_canonical_outbound() {
        // Receive Maximum (0x21) before Session Expiry (0x11) on the wire.
        let wire = [
            0x0D, 0x21, 0x00, 0x0A, 0x27, 0x00, 0x01, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x1E,
        ];
        let mut reader = Reader::new(&wire);
        let properties =
            Properties::decode(&mut reader, PropertySet::CONNECT, None).expect("any order decodes");
        assert_eq!(properties.receive_maximum, Some(10));
        assert_eq!(properties.session_expiry_interval, Some(30));
        assert_eq!(properties.maximum_packet_size, Some(65_536));

        let mut out = Vec::new();
        properties
            .encode(PropertySet::CONNECT, &mut out)
            .expect("encodes");
        assert_eq!(
            out,
            [
                0x0D, 0x11, 0x00, 0x00, 0x00, 0x1E, 0x21, 0x00, 0x0A, 0x27, 0x00, 0x01, 0x00, 0x00
            ],
            "ascending identifier order"
        );
        // And that canonical form decodes to the same value.
        let mut reader = Reader::new(&out);
        assert_eq!(
            Properties::decode(&mut reader, PropertySet::CONNECT, None),
            Ok(properties)
        );
    }

    #[test]
    fn encoding_refuses_a_property_the_packet_does_not_carry() {
        let properties = Properties {
            will_delay_interval: Some(10),
            ..Properties::new()
        };
        let mut out = Vec::new();
        assert_eq!(
            properties.encode(PropertySet::CONNECT, &mut out),
            Err(EncodeError::PropertyNotAllowed {
                id: PropertyId::WillDelayInterval
            })
        );
    }

    #[test]
    fn encoded_len_agrees_with_the_bytes_written() {
        let pairs = [("k", "v"), ("k2", "v2")];
        let properties = Properties {
            session_expiry_interval: Some(30),
            receive_maximum: Some(20),
            ..Properties::new()
        }
        .with_user_properties(&pairs);

        let mut out = Vec::new();
        properties
            .encode(PropertySet::CONNECT, &mut out)
            .expect("encodes");
        assert_eq!(
            properties.encoded_len(PropertySet::CONNECT),
            Ok(out.len() as u32)
        );
    }

    /// Replacing the repeatable properties of a decoded set replaces them
    /// rather than appending to what the wire held.
    #[test]
    fn setting_user_properties_drops_the_borrowed_block() {
        let wire = [0x07, 0x26, 0x00, 0x01, b'a', 0x00, 0x01, b'1'];
        let decoded = decode(&wire, PropertySet::CONNECT).expect("one pair");
        assert_eq!(decoded.user_properties().count(), 1);

        let replaced = decoded.clone().with_user_properties(&[]);
        assert_eq!(replaced.user_properties().count(), 0);
    }

    /// The subscription-identifier iterator has to step over other properties
    /// to find its own, which is what `skip` exists for.
    #[test]
    fn the_repeatable_iterators_step_over_other_properties() {
        let wire = [
            0x0E, // length
            0x0B, 0x05, // Subscription Identifier 5
            0x01, 0x01, // Payload Format Indicator 1
            0x23, 0x00, 0x07, // Topic Alias 7
            0x0B, 0x81, 0x01, // Subscription Identifier 129
            0x03, 0x00, 0x01, b'x', // Content Type "x"
        ];
        let mut reader = Reader::new(&wire);
        let properties =
            Properties::decode(&mut reader, PropertySet::PUBLISH, None).expect("a publish set");
        assert_eq!(
            properties.subscription_identifiers().collect::<Vec<_>>(),
            [5, 129]
        );
        assert_eq!(properties.content_type, Some("x"));
        assert_eq!(properties.topic_alias, Some(7));
        assert_eq!(
            properties.payload_format_indicator,
            Some(PayloadFormat::Utf8)
        );
    }

    /// Every packet type's set is table 2-4's row for it, and the two that
    /// have no property field have an empty one.
    #[test]
    fn the_packet_sets_match_table_two_four() {
        assert_eq!(
            PropertySet::for_packet(PacketType::Pingreq),
            PropertySet::NONE
        );
        assert_eq!(
            PropertySet::for_packet(PacketType::Pingresp),
            PropertySet::NONE
        );
        assert!(PropertySet::CONNACK.contains(PropertyId::AssignedClientIdentifier));
        assert!(!PropertySet::CONNECT.contains(PropertyId::AssignedClientIdentifier));
        assert!(PropertySet::PUBLISH.contains(PropertyId::TopicAlias));
        assert!(!PropertySet::PUBACK.contains(PropertyId::TopicAlias));
        // User Property is the only one every property-carrying packet has.
        for packet_type in [
            PacketType::Connect,
            PacketType::Connack,
            PacketType::Publish,
            PacketType::Puback,
            PacketType::Subscribe,
            PacketType::Suback,
            PacketType::Unsubscribe,
            PacketType::Unsuback,
            PacketType::Disconnect,
            PacketType::Auth,
        ] {
            assert!(
                PropertySet::for_packet(packet_type).contains(PropertyId::UserProperty),
                "{packet_type} carries User Property"
            );
        }
        assert!(PropertySet::WILL.contains(PropertyId::UserProperty));
    }
}
