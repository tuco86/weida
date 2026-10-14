//! What a client declares in CONNECT, and the two timers the specification
//! leaves to it.
//!
//! Negotiation in MQTT is "one exchange of declarative properties, not a round
//! trip; each side states its own limits and there is no counter-offer"
//! [mqtt5 §1]. So this type is one half of that exchange — the client's — and
//! [`crate::ServerLimits`] is the other. Nothing here is negotiated; it is
//! declared, and the server either lives with it or refuses the connection.
//!
//! **Two values are the client's own invention, because the specification
//! declines to give a number.**
//!
//! * `keep_alive` is the client's, and the server's threshold is fixed at 1.5
//!   times it ([MQTT-3.1.2-22]) [mqtt5 §1] unless the server overrides the
//!   value with `Server Keep Alive`.
//! * `ping_timeout` has **no** specification value at all: a client seeing no
//!   PINGRESP "within a reasonable amount of time" SHOULD close, with no
//!   number given, and HiveMQ's guide repeats the same non-quantified advice
//!   [mqtt5 §1]. Every wait is bounded, so it is a field with a real default
//!   rather than a silent forever. The default is the Keep Alive interval
//!   itself: the symmetric choice against the server's 1.5x would be more
//!   patient than the server is, and a client that outlasts its own server's
//!   timeout learns nothing by waiting.
//! * `connect_timeout` likewise: "missing CONNECT or CONNACK within a
//!   reasonable amount of time SHOULD cause a close; neither timeout is
//!   quantified" [mqtt5 §1].

use std::time::Duration;

use weida_mqtt_codec::QoS;

use crate::error::{Error, Result};
use crate::limits::Limits;

/// Keep Alive's ceiling: a Two Byte Integer of seconds, 18h 12m 15s
/// (3.1.2.10) [mqtt5 §1].
pub const MAX_KEEP_ALIVE: Duration = Duration::from_secs(u16::MAX as u64);

/// `Session Expiry Interval` and `Will Delay Interval` are Four Byte
/// Integers of seconds, about 136 years [mqtt5 §11].
pub const MAX_INTERVAL: Duration = Duration::from_secs(u32::MAX as u64);

/// The Will: an Application Message the server publishes when the connection
/// closes abnormally (3.1.2.5) [mqtt5 §4.5].
///
/// Owned rather than borrowed, because it outlives the CONNECT: the server
/// keeps it as session state, and a client that reconnects declares it again.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WillMessage {
    /// Will Topic; a Topic Name, so no wildcards.
    pub topic: String,
    /// Will Payload, opaque bytes.
    pub payload: Vec<u8>,
    /// Will QoS.
    pub qos: QoS,
    /// Will Retain. Refused at configuration time against a server that
    /// declared `Retain Available` 0.
    pub retain: bool,
    /// `Will Delay Interval` (3.1.3.2.2): the server publishes the Will after
    /// this interval or at session end, whichever is first, and MUST NOT
    /// publish it at all if a new connection to that session arrives first
    /// ([MQTT-3.1.3-9]) [mqtt5 §4.5]. Absent means 0 — immediately.
    pub delay: Option<Duration>,
    /// `Message Expiry Interval` on the Will.
    pub message_expiry: Option<Duration>,
    /// `Content Type` on the Will.
    pub content_type: Option<String>,
    /// `Response Topic` on the Will.
    pub response_topic: Option<String>,
    /// `Correlation Data` on the Will.
    pub correlation_data: Option<Vec<u8>>,
}

/// What this client puts in CONNECT.
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// Client Identifier: the key to session state [mqtt5 §2]. Zero-length
    /// asks the server to assign one and return it as
    /// `Assigned Client Identifier` ([MQTT-3.1.3-6]).
    pub client_id: String,
    /// Clean Start: 1 discards any existing session ([MQTT-3.1.2-4]), 0
    /// resumes one if it exists ([MQTT-3.1.2-5]) [mqtt5 §1].
    pub clean_start: bool,
    /// Keep Alive. `Duration::ZERO` disables the mechanism entirely
    /// (3.1.2.10), which also disables the server's liveness detection.
    pub keep_alive: Duration,
    /// `Session Expiry Interval` (3.1.2.11.2). Absent means 0: the session
    /// ends with the connection, which with Clean Start 1 "is exactly
    /// CleanSession 1" [mqtt5 §1.9].
    pub session_expiry: Option<Duration>,
    /// `Request Response Information` (3.1.2.11.6): 1 asks the server for a
    /// namespace to build a Response Topic from. Default 0 [mqtt5 §11].
    pub request_response_information: bool,
    /// `Request Problem Information` (3.1.2.11.7): 0 suppresses `Reason
    /// String` and `User Property` everywhere except PUBLISH, CONNACK and
    /// DISCONNECT ([MQTT-3.1.2-29]). Default 1 [mqtt5 §11].
    pub request_problem_information: bool,
    /// User Name (3.1.3.5).
    pub user_name: Option<String>,
    /// Password (3.1.3.6). 5.0 permits one with no User Name, which 3.1.1
    /// forbade (3.1.2.9) [mqtt5 §10].
    pub password: Option<Vec<u8>>,
    /// `Authentication Method` (3.1.2.11.9): naming one starts the AUTH
    /// exchange of 4.12, after which the client "MUST send nothing but AUTH
    /// or DISCONNECT until CONNACK" ([MQTT-3.1.2-30]) [mqtt5 §1].
    pub authentication_method: Option<String>,
    /// `Authentication Data` (3.1.2.11.10). A Protocol Error without a
    /// method ([MQTT-3.1.2-33]), refused at configuration time here.
    pub authentication_data: Option<Vec<u8>>,
    /// The Will.
    pub will: Option<WillMessage>,
    /// TLS, where the connection is to run under it. `None` is plain TCP on
    /// 1883; `Some` is `mqtts` on 8883 and the handshake completes before the
    /// CONNECT is written.
    ///
    /// It is `Option` and not a flag because the configuration *is* the
    /// decision: there is no default set of trust anchors a messaging library
    /// could pick that an application should not have had to choose
    /// ([`crate::TlsOptions`]).
    #[cfg(feature = "tls")]
    pub tls: Option<crate::transport::TlsOptions>,
    /// `User Property` pairs, forwarded unaltered and in order.
    pub user_properties: Vec<(String, String)>,
    /// What this client declares about itself, and the two bounds the
    /// protocol does not supply.
    pub limits: Limits,
    /// How long to wait for the CONNACK. Unquantified by the specification
    /// [mqtt5 §1]; finite here.
    pub connect_timeout: Duration,
    /// How long to wait for a PINGRESP before treating the connection as
    /// dead. Unquantified by the specification [mqtt5 §1]; see the module
    /// documentation for why the default is the Keep Alive interval.
    pub ping_timeout: Option<Duration>,
}

impl Default for ConnectOptions {
    fn default() -> ConnectOptions {
        ConnectOptions {
            client_id: String::new(),
            clean_start: true,
            // 60 s puts the server's detection at 90 s, which is the scale
            // every client library in the sheet uses [mqtt5 §1].
            keep_alive: Duration::from_secs(60),
            session_expiry: None,
            request_response_information: false,
            request_problem_information: true,
            user_name: None,
            password: None,
            authentication_method: None,
            authentication_data: None,
            will: None,
            #[cfg(feature = "tls")]
            tls: None,
            user_properties: Vec::new(),
            limits: Limits::default(),
            connect_timeout: Duration::from_secs(10),
            ping_timeout: None,
        }
    }
}

impl ConnectOptions {
    /// Options for `client_id`, everything else defaulted.
    #[must_use]
    pub fn new(client_id: impl Into<String>) -> ConnectOptions {
        ConnectOptions {
            client_id: client_id.into(),
            ..ConnectOptions::default()
        }
    }

    /// The Keep Alive as the Two Byte Integer the wire carries.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] above [`MAX_KEEP_ALIVE`], and for a
    /// sub-second non-zero value, which would round to 0 and silently
    /// disable the mechanism.
    pub fn keep_alive_seconds(&self) -> Result<u16> {
        if self.keep_alive > MAX_KEEP_ALIVE {
            return Err(Error::Configuration(format!(
                "keep_alive {:?} exceeds the 65,535 seconds a Two Byte Integer carries",
                self.keep_alive
            )));
        }
        let seconds = self.keep_alive.as_secs();
        if seconds == 0 && !self.keep_alive.is_zero() {
            return Err(Error::Configuration(
                "keep_alive below one second would round to 0, which disables keep-alive \
                 altogether; use Duration::ZERO to mean that deliberately"
                    .into(),
            ));
        }
        Ok(seconds as u16)
    }

    /// How long to wait for a PINGRESP: the caller's value, or the Keep Alive
    /// interval.
    ///
    /// `None` when Keep Alive is zero *and* no explicit timeout was set:
    /// nothing is being sent on a timer, so there is nothing to time out.
    #[must_use]
    pub fn effective_ping_timeout(&self) -> Option<Duration> {
        self.ping_timeout.or({
            if self.keep_alive.is_zero() {
                None
            } else {
                Some(self.keep_alive)
            }
        })
    }

    /// Refuses an unusable configuration before a byte is sent.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for [`Limits`] faults, a Keep Alive that
    /// does not fit, an interval above [`MAX_INTERVAL`], and
    /// `Authentication Data` without `Authentication Method`, which is a
    /// Protocol Error on the wire ([MQTT-3.1.2-33]) [mqtt5 §1].
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.keep_alive_seconds()?;

        if self.authentication_data.is_some() && self.authentication_method.is_none() {
            return Err(Error::Configuration(
                "authentication_data without authentication_method is a Protocol Error \
                 ([MQTT-3.1.2-33])"
                    .into(),
            ));
        }
        // A `server_name` that is not a DNS name or an IP address can never
        // validate a certificate, so it is refused here rather than after a
        // socket has been opened: "refused at configuration time" is the rule
        // every option in this crate follows.
        #[cfg(feature = "tls")]
        if let Some(tls) = &self.tls
            && let Some(name) = &tls.server_name
            && tokio_rustls::rustls::pki_types::ServerName::try_from(name.clone()).is_err()
        {
            return Err(Error::Configuration(format!(
                "{name} is not a valid server name to validate a certificate against"
            )));
        }
        check_interval("session_expiry", self.session_expiry)?;
        if let Some(will) = &self.will {
            // The Will Topic is a Topic **Name**, so no wildcards and never
            // zero-length ([MQTT-3.1.3-10]) - and the Topic Alias exception
            // cannot apply, because no alias exists before the CONNACK.
            crate::filter::check_topic_name(&will.topic, false)?;
            check_interval("will.delay", will.delay)?;
            check_interval("will.message_expiry", will.message_expiry)?;
        }
        // "Servers MUST accept 1..23 bytes of [0-9a-zA-Z] and MAY accept
        // more" ([MQTT-3.1.3-5]) [mqtt5 §2], so a longer or stranger
        // identifier is legal and only the 65,535-byte field bound is ours to
        // refuse.
        if self.client_id.len() > u16::MAX as usize {
            return Err(Error::Configuration(
                "client_id exceeds the 65,535 bytes a UTF-8 Encoded String carries".into(),
            ));
        }
        Ok(())
    }
}

/// A Four Byte Integer of seconds, or a refusal.
pub(crate) fn interval_seconds(name: &str, interval: Option<Duration>) -> Result<Option<u32>> {
    match interval {
        None => Ok(None),
        Some(interval) if interval > MAX_INTERVAL => Err(Error::Configuration(format!(
            "{name} {interval:?} exceeds the 4,294,967,295 seconds a Four Byte Integer carries"
        ))),
        Some(interval) => Ok(Some(interval.as_secs() as u32)),
    }
}

fn check_interval(name: &str, interval: Option<Duration>) -> Result<()> {
    interval_seconds(name, interval).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keep_alive_boundaries_are_the_two_byte_integers() {
        let mut options = ConnectOptions::new("a");
        options.keep_alive = MAX_KEEP_ALIVE;
        assert_eq!(options.keep_alive_seconds().unwrap(), 65_535);

        options.keep_alive = MAX_KEEP_ALIVE + Duration::from_secs(1);
        assert!(options.keep_alive_seconds().is_err());

        // Zero is legal and means "disabled" (3.1.2.10).
        options.keep_alive = Duration::ZERO;
        assert_eq!(options.keep_alive_seconds().unwrap(), 0);
        assert_eq!(options.effective_ping_timeout(), None);

        // A sub-second value would round to zero and silently disable the
        // mechanism, so it is refused rather than rounded.
        options.keep_alive = Duration::from_millis(500);
        assert!(options.keep_alive_seconds().is_err());
    }

    /// The unquantified number made explicit: the default is the Keep Alive
    /// interval, and an explicit value wins over it.
    #[test]
    fn the_ping_timeout_has_a_real_default() {
        let mut options = ConnectOptions::new("a");
        assert_eq!(
            options.effective_ping_timeout(),
            Some(Duration::from_secs(60))
        );
        options.ping_timeout = Some(Duration::from_secs(5));
        assert_eq!(
            options.effective_ping_timeout(),
            Some(Duration::from_secs(5))
        );
        // An explicit timeout stands even with keep-alive disabled, because a
        // client may still be waiting on a PINGRESP it sent by hand.
        options.keep_alive = Duration::ZERO;
        assert_eq!(
            options.effective_ping_timeout(),
            Some(Duration::from_secs(5))
        );
    }

    /// [MQTT-3.1.2-33], refused before a byte is sent rather than discovered
    /// from the server's DISCONNECT.
    #[test]
    fn authentication_data_without_a_method_is_refused() {
        let mut options = ConnectOptions::new("a");
        options.authentication_data = Some(vec![1]);
        assert!(options.validate().is_err());
        options.authentication_method = Some("SCRAM-SHA-1".into());
        assert!(options.validate().is_ok());
    }

    #[test]
    fn an_interval_above_a_four_byte_integer_is_refused() {
        let mut options = ConnectOptions::new("a");
        options.session_expiry = Some(MAX_INTERVAL);
        assert!(options.validate().is_ok());
        options.session_expiry = Some(MAX_INTERVAL + Duration::from_secs(1));
        assert!(options.validate().is_err());
    }

    /// The defaults are §11's: Request Response Information 0, Request
    /// Problem Information 1, Clean Start 1.
    #[test]
    fn the_defaults_are_the_specifications() {
        let options = ConnectOptions::default();
        assert!(!options.request_response_information);
        assert!(options.request_problem_information);
        assert!(options.clean_start);
        assert_eq!(options.session_expiry, None);
        assert!(options.validate().is_ok());
    }
}
