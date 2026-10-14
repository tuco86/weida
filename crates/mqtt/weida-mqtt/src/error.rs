//! The client's error vocabulary, in MQTT's own terms.
//!
//! The rule this enum exists to keep, in one sentence: **a server
//! DISCONNECT's reason code is surfaced as a named error rather than as a
//! closed socket.** In 3.1.1 a server reported every fault by
//! closing the connection and the client had to guess [mqtt5 §1.9]; 5.0 puts
//! a reason code on the refusal, and a client that threw it away would be
//! back where 3.1.1 was. So [`Error::ServerDisconnected`] and
//! [`Error::ConnectionRefused`] carry the code, and
//! [`Error::Unavailable`] carries the code the *server* would have sent had
//! the packet been allowed to leave.
//!
//! The other rule is that a feature the server declared unavailable is
//! **this client's own error and never reaches the wire**. Each of the five
//! availability flags of 3.2.2.3 has a Protocol Error code — 0x9A, 0x9B,
//! 0x9E, 0xA1, 0xA2 [mqtt5 §11] — and the honest thing is to report exactly
//! that code locally rather than to send the packet and let the server close
//! the connection. [`Feature`] is that mapping.

use std::fmt;
use std::io;

use weida_mqtt_codec::{
    ConnectReasonCode, DecodeError, DisconnectReasonCode, EncodeError, PacketType, QoS,
};

/// A feature whose availability the server declares in CONNACK (3.2.2.3)
/// [mqtt5 §11].
///
/// The five flags are new in 5.0 and replace the 3.1.1 habit of refusing a
/// feature by claiming the client is not authorized [mqtt5 §1.9]. Honouring
/// them locally is what makes that replacement worth anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Feature {
    /// `Retain Available` 0: the client may not set RETAIN (3.2.2.3.5).
    Retain,
    /// A QoS above `Maximum QoS` (3.2.2.3.4).
    Qos(QoS),
    /// `Wildcard Subscription Available` 0: filters may not contain `+` or
    /// `#` (3.2.2.3.11).
    WildcardSubscription,
    /// `Subscription Identifiers Available` 0 (3.2.2.3.12).
    SubscriptionIdentifier,
    /// `Shared Subscription Available` 0: `$share/` filters are not accepted
    /// (3.2.2.3.13).
    SharedSubscription,
    /// A Topic Alias above the server's `Topic Alias Maximum`
    /// ([MQTT-3.3.2-9]).
    TopicAlias,
}

impl Feature {
    /// The reason code the server would have answered with.
    ///
    /// This is the whole point of reporting locally: the caller gets the same
    /// byte it would have got from a DISCONNECT, without spending the
    /// connection to learn it.
    #[must_use]
    pub const fn reason_code(self) -> u8 {
        match self {
            Feature::Retain => 0x9A,
            Feature::Qos(_) => 0x9B,
            Feature::SharedSubscription => 0x9E,
            Feature::SubscriptionIdentifier => 0xA1,
            Feature::WildcardSubscription => 0xA2,
            Feature::TopicAlias => 0x94,
        }
    }

    /// The specification's name for the flag, for a log line.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Feature::Retain => "Retain Available",
            Feature::Qos(_) => "Maximum QoS",
            Feature::SharedSubscription => "Shared Subscription Available",
            Feature::SubscriptionIdentifier => "Subscription Identifiers Available",
            Feature::WildcardSubscription => "Wildcard Subscription Available",
            Feature::TopicAlias => "Topic Alias Maximum",
        }
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Feature::Qos(qos) => write!(f, "{:?} above Maximum QoS", qos),
            other => f.write_str(other.name()),
        }
    }
}

/// What went wrong.
#[derive(Debug)]
pub enum Error {
    /// The server refused the CONNECT, with its code (3.2.2.2) [mqtt5 §1].
    ConnectionRefused(ConnectReasonCode),
    /// The server sent DISCONNECT, with its code (3.14.2.1) [mqtt5 §1]. This
    /// is the variant that keeps 5.0's improvement over 3.1.1 visible.
    ServerDisconnected(DisconnectReasonCode),
    /// The peer closed the transport with no DISCONNECT at all, which is
    /// always available to it and is what 3.1.1 did for everything
    /// [mqtt5 §1].
    ConnectionClosed,
    /// A feature the server declared unavailable was used. Reported here,
    /// before the packet reached the wire.
    Unavailable {
        /// Which flag refused it.
        feature: Feature,
        /// The code the server would have sent.
        reason_code: u8,
    },
    /// The server sent a packet the protocol does not permit at this point.
    /// Before CONNACK that is anything but CONNACK or AUTH
    /// ([MQTT-3.2.0-2]), and an AUTH at all where the client named no
    /// `Authentication Method` ([MQTT-4.12.0-6]) [mqtt5 §10].
    UnexpectedPacket {
        /// What arrived.
        packet_type: weida_mqtt_codec::PacketType,
    },
    /// An AUTH or a successful CONNACK named a different
    /// `Authentication Method` than the CONNECT did. "All AUTH packets and
    /// any successful CONNACK MUST repeat the same method"
    /// ([MQTT-4.12.0-5]) (4.12) [mqtt5 §10].
    AuthenticationMethodMismatch,
    /// The CONNACK said `Session Present` 1 and this client holds no session
    /// state. "A Client that receives Session Present 1 where it has no
    /// Session State MUST close the Network Connection"
    /// ([MQTT-3.2.2-4]) (3.2.2.1.1) [mqtt5 §1] — believing it would mean
    /// answering acknowledgements for exchanges this client has no record of.
    SessionPresentWithoutState,
    /// As many QoS 1 and 2 messages are unacknowledged as the server's
    /// `Receive Maximum` permits, so no Packet Identifier may be spent.
    ///
    /// "At zero the sender MUST NOT send further QoS > 0 PUBLISH packets"
    /// ([MQTT-4.9.0-2]) [mqtt5 §5]. Exhaustion "stalls the sender rather than
    /// exceeding it", so a publish path waits for room; this variant is for a
    /// caller that asked not to wait.
    QuotaExhausted {
        /// The ceiling in force: the server's `Receive Maximum`.
        quota: u16,
    },
    /// The server has more unacknowledged QoS 2 messages in flight toward
    /// this client than the `Receive Maximum` it was told, which "earns
    /// DISCONNECT 0x93 (Receive Maximum exceeded)" [mqtt5 §5]. Reported here
    /// rather than letting the receive table grow.
    ReceiveMaximumExceeded {
        /// The ceiling this client declared.
        quota: u16,
    },
    /// A delivery carried more Subscription Identifiers than
    /// [`crate::Limits::max_subscription_identifiers`].
    ///
    /// **The protocol has no ceiling for this.** A server MAY send one copy
    /// carrying every matching identifier ([MQTT-3.3.4-4]) [mqtt5 §4.1] and
    /// the only limit on the wire is the packet size, so the number is this
    /// client's and is named in [`crate::Limits`].
    TooManySubscriptionIdentifiers {
        /// The ceiling this client set.
        max: usize,
    },
    /// A delivery carried more `User Property` pairs than
    /// [`crate::Limits::max_user_properties`].
    ///
    /// **The protocol has no ceiling for this either.** `User Property` "is
    /// allowed to appear multiple times" (3.3.2.3.7) [mqtt5 §3] and the only
    /// limit on the wire is the packet size, so the number is this client's
    /// and is named in [`crate::Limits`].
    TooManyUserProperties {
        /// The ceiling this client set.
        max: usize,
    },
    /// A Topic Name or Topic Filter breaks the grammar of 4.7 [mqtt5 §4.1],
    /// refused before the packet reaches the wire.
    ///
    /// **On the wire this would be a Malformed Packet or a SUBACK 0x8F**
    /// ([MQTT-4.7.1-1], [MQTT-4.7.1-2], [MQTT-4.7.3-1], [MQTT-3.3.2-2]), so
    /// refusing it locally costs a round trip less and names which rule was
    /// broken rather than handing back one code for every fault.
    InvalidTopic {
        /// What was offered.
        topic: String,
        /// Which rule it broke, with the conformance statement.
        reason: &'static str,
    },
    /// A SUBACK or UNSUBACK carried a different number of reason codes than
    /// the packet it answers carried filters.
    ///
    /// "The SUBACK MUST contain one reason code for each Topic Filter, in the
    /// same order" ([MQTT-3.9.3-1], [MQTT-3.9.3-2]), and the same for
    /// UNSUBACK ([MQTT-3.11.3-1]) [mqtt5 §8]. **Position is the only binding
    /// between a code and a filter** - there is no filter in the
    /// acknowledgement - so a different count leaves no way to say which
    /// filter each code is about, and reporting that is the only honest
    /// answer.
    AcknowledgementLengthMismatch {
        /// Which acknowledgement.
        packet_type: PacketType,
        /// How many filters were sent.
        sent: usize,
        /// How many codes came back.
        received: usize,
    },
    /// The server broke the protocol. The client's own answer is to close,
    /// which "a Client SHOULD" do ([MQTT-4.13.1-1]) (4.13.1) [mqtt5 §1].
    Protocol(DecodeError),
    /// The client was asked to send something that cannot be encoded, or that
    /// exceeds the server's `Maximum Packet Size`.
    Encode(EncodeError),
    /// A configuration value is unusable. Refused at configuration time,
    /// which is the rule every option follows.
    Configuration(String),
    /// Nothing arrived within a bound the client set: the CONNACK deadline,
    /// or the PINGRESP deadline the specification leaves unquantified.
    Timeout(&'static str),
    /// The connection is gone; its cause was reported to whoever was holding
    /// the events.
    NotConnected,
    /// The transport failed.
    Io(io::Error),
    /// The runtime or the resolver failed.
    Runtime(String),
}

impl Error {
    /// The reason code a caller may log or act on, where one exists.
    #[must_use]
    pub fn reason_code(&self) -> Option<u8> {
        match self {
            Error::ConnectionRefused(code) => Some(code.as_byte()),
            Error::ServerDisconnected(code) => Some(code.as_byte()),
            Error::Unavailable { reason_code, .. } => Some(*reason_code),
            Error::Protocol(error) => error.reason_code(),
            // 0x93 is the code a peer that broke the quota earns [mqtt5 §5].
            Error::ReceiveMaximumExceeded { .. } => Some(0x93),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ConnectionRefused(code) => write!(
                f,
                "the server refused the connection: {code:?} (0x{:02X})",
                code.as_byte()
            ),
            Error::ServerDisconnected(code) => write!(
                f,
                "the server disconnected: {code:?} (0x{:02X})",
                code.as_byte()
            ),
            Error::ConnectionClosed => {
                f.write_str("the peer closed the connection without a DISCONNECT")
            }
            Error::Unavailable {
                feature,
                reason_code,
            } => write!(
                f,
                "the server does not offer {feature} (0x{reason_code:02X}), so the packet was not sent"
            ),
            Error::UnexpectedPacket { packet_type } => write!(
                f,
                "the server sent {packet_type} where the protocol does not permit it"
            ),
            Error::AuthenticationMethodMismatch => f.write_str(
                "the server named a different authentication method than the CONNECT did",
            ),
            Error::SessionPresentWithoutState => f.write_str(
                "the server resumed a session this client has no state for, so the connection \
                 must close ([MQTT-3.2.2-4])",
            ),
            Error::QuotaExhausted { quota } => write!(
                f,
                "{quota} QoS 1 or 2 messages are already unacknowledged, which is the server's \
                 Receive Maximum"
            ),
            Error::ReceiveMaximumExceeded { quota } => write!(
                f,
                "the server exceeded the Receive Maximum of {quota} this client declared"
            ),
            Error::InvalidTopic { topic, reason } => {
                write!(f, "the topic or filter {topic:?} is not valid: {reason}")
            }
            Error::AcknowledgementLengthMismatch {
                packet_type,
                sent,
                received,
            } => write!(
                f,
                "the {packet_type} carried {received} reason codes for {sent} filters, so no \
                 code can be matched to a filter ([MQTT-3.9.3-1])"
            ),
            Error::TooManySubscriptionIdentifiers { max } => write!(
                f,
                "a delivery carried more than {max} subscription identifiers, which is this \
                 client's ceiling because the protocol has none"
            ),
            Error::TooManyUserProperties { max } => write!(
                f,
                "a delivery carried more than {max} user properties, which is this client's \
                 ceiling because the protocol has none"
            ),
            Error::Protocol(error) => write!(f, "the server broke the protocol: {error}"),
            Error::Encode(error) => write!(f, "cannot send this packet: {error}"),
            Error::Configuration(message) => write!(f, "invalid configuration: {message}"),
            Error::Timeout(what) => write!(f, "timed out waiting for {what}"),
            Error::NotConnected => f.write_str("not connected"),
            Error::Io(error) => write!(f, "transport failed: {error}"),
            Error::Runtime(message) => write!(f, "runtime failed: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Protocol(error) => Some(error),
            Error::Encode(error) => Some(error),
            Error::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DecodeError> for Error {
    fn from(error: DecodeError) -> Error {
        Error::Protocol(error)
    }
}

impl From<EncodeError> for Error {
    fn from(error: EncodeError) -> Error {
        Error::Encode(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::Io(error)
    }
}

impl From<weida_core::Error> for Error {
    fn from(error: weida_core::Error) -> Error {
        match error {
            weida_core::Error::Io(io) => Error::Io(io),
            other => Error::Runtime(other.to_string()),
        }
    }
}

/// This crate's result type, with the error defaulted so a call site that
/// wants a different one keeps the escape hatch.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Each of the five availability flags reports the code 3.2.2.3 gives it,
    /// so a caller sees the same byte a DISCONNECT would have carried.
    #[test]
    fn every_feature_reports_the_specifications_code() {
        for (feature, code) in [
            (Feature::Retain, 0x9A),
            (Feature::Qos(QoS::ExactlyOnce), 0x9B),
            (Feature::SharedSubscription, 0x9E),
            (Feature::SubscriptionIdentifier, 0xA1),
            (Feature::WildcardSubscription, 0xA2),
            (Feature::TopicAlias, 0x94),
        ] {
            assert_eq!(feature.reason_code(), code, "{feature}");
            let error = Error::Unavailable {
                feature,
                reason_code: feature.reason_code(),
            };
            assert_eq!(error.reason_code(), Some(code));
            // Every one of these codes is a failure by 2.4's 0x80 line.
            assert!(code >= 0x80);
        }
    }

    /// The distinction this vocabulary exists to make: a refusal carries its
    /// code, and a bare close is its own variant rather than being conflated
    /// with one.
    #[test]
    fn a_refusal_carries_its_code_and_a_bare_close_does_not() {
        let refused = Error::ConnectionRefused(ConnectReasonCode::BadUserNameOrPassword);
        assert_eq!(refused.reason_code(), Some(0x86));
        assert!(refused.to_string().contains("0x86"));

        let taken_over = Error::ServerDisconnected(DisconnectReasonCode::SessionTakenOver);
        assert_eq!(taken_over.reason_code(), Some(0x8E));

        assert_eq!(Error::ConnectionClosed.reason_code(), None);
    }
}
