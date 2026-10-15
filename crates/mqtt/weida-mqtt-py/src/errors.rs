//! MQTT's reason codes, as this module's exception classes.
//!
//! Every acknowledgement in 5.0 carries a reason code, which is the protocol's
//! largest single improvement over 3.1.1 — where "a client learned why a
//! server objected only by watching it close the socket"
//! (`docs/research/mqtt5.md` §1.9). A binding that folded them into
//! `RuntimeError` would throw that away, so each code is its own class:
//!
//! ```text
//! Exception
//!  └── weida_mqtt.MqttError              # `except MqttError` catches all
//!       ├── weida_mqtt.NotAuthorized     # one class per reason-code name
//!       ├── weida_mqtt.QosNotSupported
//!       ├── weida_mqtt.NotConnected      # and per failure that has no code
//!       └── ...
//! ```
//!
//! and every instance carries `errno` (the name), `cause` (why) and
//! `reason_code` — the byte, or `None` where the failure has none. The byte is
//! what a caller logs and the class is what a caller branches on, which is why
//! both are there.
//!
//! **One class per name, not per packet.** `NotAuthorized` is 0x87 in CONNACK,
//! PUBACK, SUBACK, UNSUBACK and DISCONNECT alike, so `except NotAuthorized`
//! catches it wherever it arrived — which is what a caller means by it.
//!
//! **A name is a name, not a packet.** `ProtocolError` is 0x82 and appears in
//! this list once, even though it is both a CONNACK code and a DISCONNECT
//! code — and even though a decode failure in this client also earns it. The
//! foundation refuses a duplicated name at import, which is what caught the
//! first draft of this file listing it twice.
//!
//! **The table cannot drift.** [`connect_name`] and [`disconnect_name`] are
//! exhaustive matches over the codec's own enums, so a code added to the
//! library that is not added here is a compile error in this file rather than
//! a reason code that silently arrives as the base class.

use pyo3::prelude::*;
use pyo3::types::PyInt;
use weida_mqtt::{ConnectReasonCode, DisconnectReasonCode, Error, Feature};
use weida_py_core::{Errno, ErrorClasses};

/// The module's exception classes, built once at import.
static ERRORS: ErrorClasses = ErrorClasses::new();

/// The base class every failure of this module derives from.
const BASE: &str = "MqttError";

/// Writes the class list and the name-to-byte table from one source.
macro_rules! codes {
    ($(($name:ident, $code:expr)),+ $(,)?) => {
        /// Every reason-code class, with the byte it carries.
        const CODED: &[(&str, u8)] = &[$((stringify!($name), $code)),+];
    };
}

/// Writes the class list for the failures MQTT gives no reason code.
macro_rules! uncoded {
    ($(($name:ident)),+ $(,)?) => {
        /// Every class for a failure with no byte behind it.
        const UNCODED: &[&str] = &[$(stringify!($name)),+];
    };
}

codes!(
    (AdministrativeAction, 0x98),
    (BadAuthenticationMethod, 0x8C),
    (BadUserNameOrPassword, 0x86),
    (Banned, 0x8A),
    (ClientIdentifierNotValid, 0x85),
    (ConnectionRateExceeded, 0x9F),
    (DisconnectWithWillMessage, 0x04),
    (ImplementationSpecificError, 0x83),
    (KeepAliveTimeout, 0x8D),
    (MalformedPacket, 0x81),
    (MaximumConnectTime, 0xA0),
    (MessageRateTooHigh, 0x96),
    (NormalDisconnection, 0x00),
    (NotAuthorized, 0x87),
    (PacketTooLarge, 0x95),
    (PayloadFormatInvalid, 0x99),
    (ProtocolError, 0x82),
    (QosNotSupported, 0x9B),
    (QuotaExceeded, 0x97),
    (ReceiveMaximumExceeded, 0x93),
    (RetainNotSupported, 0x9A),
    (ServerBusy, 0x89),
    (ServerMoved, 0x9D),
    (ServerShuttingDown, 0x8B),
    (ServerUnavailable, 0x88),
    (SessionTakenOver, 0x8E),
    (SharedSubscriptionsNotSupported, 0x9E),
    (SubscriptionIdentifiersNotSupported, 0xA1),
    (Success, 0x00),
    (TopicAliasInvalid, 0x94),
    (TopicFilterInvalid, 0x8F),
    (TopicNameInvalid, 0x90),
    (UnspecifiedError, 0x80),
    (UnsupportedProtocolVersion, 0x84),
    (UseAnotherServer, 0x9C),
    (WildcardSubscriptionsNotSupported, 0xA2),
);

uncoded!(
    (ConnectionClosed),
    (NotConnected),
    (Timeout),
    (Configuration),
    (InvalidTopic),
    (UnexpectedPacket),
    (AuthenticationMethodMismatch),
    (AuthenticationFailed),
    (SessionPresentWithoutState),
    (QuotaExhausted),
    (AcknowledgementLengthMismatch),
    (EncodeError),
    (Io),
    (Runtime),
);

/// Creates the classes and adds them to the module. Called once, at import.
pub fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let mut names: Vec<&str> = CODED.iter().map(|(name, _)| *name).collect();
    names.extend_from_slice(UNCODED);
    ERRORS.install(module, BASE, &names)
}

/// The byte behind a class name, where there is one.
fn code_of(name: &str) -> Option<u8> {
    CODED
        .iter()
        .find(|(known, _)| *known == name)
        .map(|(_, code)| *code)
}

/// The name of a CONNACK reason code. Exhaustive, so the codec cannot add one
/// without this file failing to compile.
fn connect_name(code: ConnectReasonCode) -> &'static str {
    match code {
        ConnectReasonCode::Success => "Success",
        ConnectReasonCode::UnspecifiedError => "UnspecifiedError",
        ConnectReasonCode::MalformedPacket => "MalformedPacket",
        ConnectReasonCode::ProtocolError => "ProtocolError",
        ConnectReasonCode::ImplementationSpecificError => "ImplementationSpecificError",
        ConnectReasonCode::UnsupportedProtocolVersion => "UnsupportedProtocolVersion",
        ConnectReasonCode::ClientIdentifierNotValid => "ClientIdentifierNotValid",
        ConnectReasonCode::BadUserNameOrPassword => "BadUserNameOrPassword",
        ConnectReasonCode::NotAuthorized => "NotAuthorized",
        ConnectReasonCode::ServerUnavailable => "ServerUnavailable",
        ConnectReasonCode::ServerBusy => "ServerBusy",
        ConnectReasonCode::Banned => "Banned",
        ConnectReasonCode::BadAuthenticationMethod => "BadAuthenticationMethod",
        ConnectReasonCode::TopicNameInvalid => "TopicNameInvalid",
        ConnectReasonCode::PacketTooLarge => "PacketTooLarge",
        ConnectReasonCode::QuotaExceeded => "QuotaExceeded",
        ConnectReasonCode::PayloadFormatInvalid => "PayloadFormatInvalid",
        ConnectReasonCode::RetainNotSupported => "RetainNotSupported",
        ConnectReasonCode::QosNotSupported => "QosNotSupported",
        ConnectReasonCode::UseAnotherServer => "UseAnotherServer",
        ConnectReasonCode::ServerMoved => "ServerMoved",
        ConnectReasonCode::ConnectionRateExceeded => "ConnectionRateExceeded",
    }
}

/// The same for DISCONNECT, whose set is the largest of the seven.
fn disconnect_name(code: DisconnectReasonCode) -> &'static str {
    match code {
        DisconnectReasonCode::NormalDisconnection => "NormalDisconnection",
        DisconnectReasonCode::DisconnectWithWillMessage => "DisconnectWithWillMessage",
        DisconnectReasonCode::UnspecifiedError => "UnspecifiedError",
        DisconnectReasonCode::MalformedPacket => "MalformedPacket",
        DisconnectReasonCode::ProtocolError => "ProtocolError",
        DisconnectReasonCode::ImplementationSpecificError => "ImplementationSpecificError",
        DisconnectReasonCode::NotAuthorized => "NotAuthorized",
        DisconnectReasonCode::ServerBusy => "ServerBusy",
        DisconnectReasonCode::ServerShuttingDown => "ServerShuttingDown",
        DisconnectReasonCode::KeepAliveTimeout => "KeepAliveTimeout",
        DisconnectReasonCode::SessionTakenOver => "SessionTakenOver",
        DisconnectReasonCode::TopicFilterInvalid => "TopicFilterInvalid",
        DisconnectReasonCode::TopicNameInvalid => "TopicNameInvalid",
        DisconnectReasonCode::ReceiveMaximumExceeded => "ReceiveMaximumExceeded",
        DisconnectReasonCode::TopicAliasInvalid => "TopicAliasInvalid",
        DisconnectReasonCode::PacketTooLarge => "PacketTooLarge",
        DisconnectReasonCode::MessageRateTooHigh => "MessageRateTooHigh",
        DisconnectReasonCode::QuotaExceeded => "QuotaExceeded",
        DisconnectReasonCode::AdministrativeAction => "AdministrativeAction",
        DisconnectReasonCode::PayloadFormatInvalid => "PayloadFormatInvalid",
        DisconnectReasonCode::RetainNotSupported => "RetainNotSupported",
        DisconnectReasonCode::QosNotSupported => "QosNotSupported",
        DisconnectReasonCode::UseAnotherServer => "UseAnotherServer",
        DisconnectReasonCode::ServerMoved => "ServerMoved",
        DisconnectReasonCode::SharedSubscriptionsNotSupported => "SharedSubscriptionsNotSupported",
        DisconnectReasonCode::ConnectionRateExceeded => "ConnectionRateExceeded",
        DisconnectReasonCode::MaximumConnectTime => "MaximumConnectTime",
        DisconnectReasonCode::SubscriptionIdentifiersNotSupported => {
            "SubscriptionIdentifiersNotSupported"
        }
        DisconnectReasonCode::WildcardSubscriptionsNotSupported => {
            "WildcardSubscriptionsNotSupported"
        }
    }
}

/// The name a refused feature earns, which is the name of the code the server
/// would have sent had the packet been allowed to leave.
fn feature_name(feature: Feature) -> &'static str {
    match feature {
        Feature::Retain => "RetainNotSupported",
        Feature::Qos(_) => "QosNotSupported",
        Feature::WildcardSubscription => "WildcardSubscriptionsNotSupported",
        Feature::SubscriptionIdentifier => "SubscriptionIdentifiersNotSupported",
        Feature::SharedSubscription => "SharedSubscriptionsNotSupported",
        Feature::TopicAlias => "TopicAliasInvalid",
    }
}

/// The protocol-neutral form the bridge carries: the class name and why.
///
/// Called on a reactor thread with no GIL held, which is why it does not build
/// the exception itself.
pub fn errno_of(error: Error) -> Errno {
    let cause = error.to_string();
    let name = match &error {
        Error::ConnectionRefused(code) => connect_name(*code),
        Error::ServerDisconnected(code) => disconnect_name(*code),
        Error::Unavailable { feature, .. } => feature_name(*feature),
        Error::ReceiveMaximumExceeded { .. } => "ReceiveMaximumExceeded",
        Error::ConnectionClosed => "ConnectionClosed",
        Error::NotConnected => "NotConnected",
        Error::Timeout(_) => "Timeout",
        Error::Configuration(_) => "Configuration",
        Error::InvalidTopic { .. } => "InvalidTopic",
        Error::UnexpectedPacket { .. } => "UnexpectedPacket",
        Error::AuthenticationMethodMismatch => "AuthenticationMethodMismatch",
        Error::AuthenticationFailed(_) => "AuthenticationFailed",
        Error::SessionPresentWithoutState => "SessionPresentWithoutState",
        Error::QuotaExhausted { .. } => "QuotaExhausted",
        Error::AcknowledgementLengthMismatch { .. } => "AcknowledgementLengthMismatch",
        // The client's judgement that the *server* broke the protocol, and
        // the class is the code this client would send in its DISCONNECT:
        // 0x81 `MalformedPacket` where the bytes could not be parsed, 0x82
        // `ProtocolError` where they parsed and said something illegal. The
        // codec already decides which, so this does not decide it twice.
        Error::Protocol(error) => error
            .reason_code()
            .and_then(|code| DisconnectReasonCode::from_byte(code).ok())
            .map_or("ProtocolError", disconnect_name),
        Error::Encode(_) => "EncodeError",
        Error::Io(_) => "Io",
        Error::Runtime(_) => "Runtime",
    };
    Errno::new(name, cause)
}

/// Turns an [`Errno`] into this module's exception, with `reason_code` on it.
///
/// One name is not a failure at all: [`STOP_ASYNC_ITERATION`], which the end of
/// a delivery stream uses. An `async for` that ends is not an error, and Python
/// spells the end of an async iteration with an exception, so this is where the
/// two meet.
pub fn to_py(py: Python<'_>, errno: &Errno) -> PyErr {
    if errno.name() == STOP_ASYNC_ITERATION {
        return pyo3::exceptions::PyStopAsyncIteration::new_err(errno.cause().to_owned());
    }
    let error = ERRORS.error(py, errno);
    // The byte is set here rather than in `weida-py-core`, because a reason
    // code is MQTT's and the foundation must not know a protocol. A failure to
    // set it is not worth turning a raise into a panic over, so it is ignored:
    // the class and the cause, which are the branch and the message, are
    // already on the instance.
    let value = error.value(py);
    let code = code_of(errno.name());
    let _ = match code {
        Some(code) => value.setattr("reason_code", PyInt::new(py, i64::from(code))),
        None => value.setattr("reason_code", py.None()),
    };
    error
}

/// The name [`to_py`] turns into `StopAsyncIteration`.
///
/// Not a reason code: it is the marker for "this stream has ended", and it
/// exists because an iteration's end is not a failure.
pub const STOP_ASYNC_ITERATION: &str = "StopAsyncIteration";

/// Raises a library error from a synchronous call.
pub fn raise<T>(py: Python<'_>, result: Result<T, Error>) -> PyResult<T> {
    result.map_err(|error| to_py(py, &errno_of(error)))
}
