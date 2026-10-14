//! An MQTT 5.0 **client**, on `weida-runtime` and on the sans-I/O
//! `weida-mqtt-codec`.
//!
//! It is a client and deliberately not a server: MQTT's topology is strictly
//! asymmetric, "the server binds and accepts, the client connects", and a
//! client "can never listen" [mqtt5 §12/P11]. An MQTT server is a broker —
//! sessions that outlive connections, retained-message storage,
//! shared-subscription dispatch — and a broker is not this crate
//! ([0014](../../../docs/decisions/0014-parallel-libraries.md) §2). What a
//! client library *may* hold is the client half of a server-side concept where
//! the protocol defines it for a client, which is what the [`Session`] and the
//! joined shared subscription are.
//!
//! **No weida in the picture.** This crate depends on `weida-core`,
//! `weida-runtime` and `weida-mqtt-codec`, and on neither `weida` nor
//! `weida-protocol`
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.2). It is
//! usable on its own, by a caller who has never heard of weida.
//!
//! # Connecting
//!
//! ```no_run
//! use std::time::Duration;
//! use weida_mqtt::{ConnectOptions, Context, DisconnectReasonCode};
//!
//! # async fn run() -> Result<(), weida_mqtt::Error> {
//! let context = Context::owned(1)?;
//!
//! let mut options = ConnectOptions::new("sensor-1");
//! options.keep_alive = Duration::from_secs(30);
//! options.limits.receive_maximum = 20;
//!
//! let (client, mut events) = weida_mqtt::Client::connect(
//!     &context,
//!     "127.0.0.1:1883",
//!     options,
//! )
//! .await?;
//!
//! // Everything the client must honour for the rest of the connection came
//! // back in the CONNACK, with §11's defaults applied to what the server
//! // left out.
//! let limits = client.server_limits();
//! if limits.retain_available {
//!     // ... publishing with RETAIN is permitted
//! }
//!
//! // DISCONNECT 0x00 makes the server discard the Will without publishing
//! // it ([MQTT-3.14.4-3]).
//! client
//!     .disconnect(DisconnectReasonCode::NormalDisconnection)
//!     .await?;
//! while let Some(event) = events.next().await {
//!     eprintln!("{event:?}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # What this client honours, and where
//!
//! Negotiation is "one exchange of declarative properties, not a round trip"
//! [mqtt5 §1], so there are exactly two halves and each has a module.
//!
//! * [`ConnectOptions`] is the client's half, and [`Limits`] the part of it
//!   that bounds what a peer can make this process hold.
//! * [`ServerLimits`] is the server's half, read once from the CONNACK with
//!   §11's defaults substituted for every absent property — because absence is
//!   meaningful and resolving it per call site is how a client ends up
//!   refusing RETAIN against a server that never mentioned it.
//! * **A feature the server declared unavailable is this client's own error
//!   and never reaches the wire.** [`ServerLimits::require`] answers with the
//!   reason code the server would have sent — 0x9A, 0x9B, 0x9E, 0xA1, 0xA2
//!   [mqtt5 §11] — so the caller learns the same byte without spending the
//!   connection on it.
//! * **A server DISCONNECT's reason code is a named error**, not a closed
//!   socket: [`Error::ServerDisconnected`] carries it. In 3.1.1 a server
//!   reported every fault by closing and the client had to guess
//!   [mqtt5 §1.9]; throwing the code away would put this client back there.
//! * `Server Keep Alive` overrides the client's value where the server sends
//!   one ([MQTT-3.2.2-21]) and the client's stands where it does not
//!   ([MQTT-3.2.2-22]); [`Client::keep_alive`] reports which.
//!
//! # Where the numbers come from
//!
//! Two of the limits a client needs are the protocol's own and are used as
//! such: the send quota's ceiling is the server's `Receive Maximum`
//! ([MQTT-4.9.0-1]) and the outbound alias table's is the server's
//! `Topic Alias Maximum` ([MQTT-3.3.2-9]) [mqtt5 §5]. Two more have no
//! protocol ceiling at all — the Subscription Identifiers one delivery may
//! carry, and the queue of deliveries waiting for the application — so
//! [`Limits`] names a number for each and says in its own documentation that
//! the number is ours because the protocol has none. §11's four unbounded
//! resources are the reason that distinction is worth making.
//!
//! # The two timers the specification declines to quantify
//!
//! A missing CONNACK and a missing PINGRESP both get "a reasonable amount of
//! time" and no number [mqtt5 §1]. Both are fields with finite defaults here
//! ([`ConnectOptions::connect_timeout`],
//! [`ConnectOptions::effective_ping_timeout`]), because every wait is bounded.
//!
//! # The session, and the one place retransmission happens
//!
//! A [`Session`] is the client half of MQTT's session state and is held by
//! the **application**, not by the connection: that is what makes it outlive
//! the connection, which is the whole difference between the two. weida
//! deliberately has no counterpart — "there is no L0 session state, nothing
//! is retained across a connection"
//! ([0008](../../../docs/decisions/0008-session-identity.md) §4.5) — so the
//! state has an
//! owner here rather than being smuggled into a runtime that declined it.
//!
//! [`Session::resend`] is the only retransmission in this crate, and it is
//! called in exactly one place: just after a CONNACK whose `Session Present`
//! is set. "This is the only circumstance where a Client or Server is
//! REQUIRED to resend messages. Clients and Servers MUST NOT resend messages
//! at any other time" ([MQTT-4.4.0-1]) [mqtt5 §6]. There is no retry timer
//! anywhere, and that absence is the behaviour — the ecosystem disagrees
//! loudly enough (EMQX's 30-second `retry_interval`, Paho Python's
//! self-declared non-compliant republish [mqtt5 §6]) to make it worth a test
//! that asserts nothing is resent inside a live connection.

#![warn(missing_docs)]

pub mod alias;
#[cfg(feature = "blocking")]
pub mod blocking;
pub mod client;
pub mod connection;
pub mod error;
pub mod filter;
pub mod limits;
pub mod message;
pub mod options;
pub mod session;
pub mod transport;

pub use alias::{Aliased, InboundAliases, OutboundAliases};
pub use client::{Client, Context, Event, Events};
pub use connection::Authenticator;
pub use error::{Error, Feature, Result};
pub use filter::{
    MAX_TOPIC_BYTES, SHARE_PREFIX, Shared, Subscription, SubscriptionRecord, Subscriptions,
    check_topic_filter, check_topic_name, matches, split_shared,
};
pub use limits::{DEFAULT_RECEIVE_MAXIMUM, DEFAULT_TOPIC_ALIAS_MAXIMUM, Limits, ServerLimits};
pub use message::{Completion, Delivery, DeliveryProperties, Message, RetainedOrigin};
pub use options::{ConnectOptions, MAX_INTERVAL, MAX_KEEP_ALIVE, WillMessage};
pub use session::{InFlight, Resend, Resumption, Session, Stage, StoredPublish};
pub use transport::Stream;
#[cfg(feature = "tls")]
pub use transport::TlsOptions;

// The protocol vocabulary a caller needs in order to talk to this client at
// all, re-exported so that `weida-mqtt` is one dependency rather than two.
pub use weida_mqtt_codec::{
    ConnectReasonCode, DisconnectReasonCode, Packet, PacketType, PayloadFormat, QoS,
    RetainHandling, SubackReasonCode, SubscriptionOptions, UnsubackReasonCode,
};
