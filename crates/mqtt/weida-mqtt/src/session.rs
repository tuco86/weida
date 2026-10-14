//! The client half of the session, and the one place retransmission is
//! permitted.
//!
//! MQTT's session is "stateful client/server interaction keyed by Client
//! Identifier, spanning zero or more consecutive connections", and it "lasts
//! as long as the latest Network Connection plus the Session Expiry Interval"
//! (4.1) [mqtt5 §2]. The specification splits the state in two, and this
//! module holds exactly the client's half [mqtt5 §2]:
//!
//! * QoS 1 and QoS 2 messages **sent** but not fully acknowledged;
//! * QoS 2 messages **received** but not fully acknowledged.
//!
//! The server's half — the session's existence, the subscriptions, the queued
//! messages, the Will and the session-end time — is a broker's, which this
//! crate is not ([0014](../../../docs/decisions/0014-parallel-libraries.md)
//! §2). Holding the client half is explicitly not being a broker: "a client
//! library may hold the *client half* of a server-side concept where the
//! protocol defines it for a client — an MQTT session with its expiry"
//! [0014 §2].
//!
//! **weida has nothing like this, and that is the point of the type.** "There
//! is no L0 session state. Nothing is retained across a connection… no expiry
//! interval, no session table and no reconnect logic enters the runtime"
//! ([0008](../../../docs/decisions/0008-session-identity.md) §4.5), because "a
//! retained session is remote-controlled state that the stream core has no
//! owner for" ([INVARIANTS.md]). So the state has an owner here — the
//! application holds the [`Session`] and decides how long it lives — rather
//! than being smuggled into a runtime that declined it.
//!
//! # Retransmission, and the one circumstance that permits it
//!
//! "This is the only circumstance where a Client or Server is REQUIRED to
//! resend messages. Clients and Servers MUST NOT resend messages at any other
//! time" ([MQTT-4.4.0-1]) (4.4) [mqtt5 §6]. So [`Session::resend`] exists and
//! nothing else in this crate resends anything: there is no timer, no retry
//! interval and no redelivery budget, and that absence is the behaviour. The
//! ecosystem disagrees loudly — EMQX ships `mqtt.retry_interval` at 30 s, and
//! Paho's Python client republishes after a reconnect even with
//! `clean_session=True`, calls that non-compliant itself, and warns that QoS 2
//! messages can therefore arrive twice [mqtt5 §6] — which is exactly why the
//! rule is worth a test that asserts the *absence* of a retransmission inside
//! a live connection.
//!
//! What is resent, and in what shape (4.4, [MQTT-4.6.0-1]) [mqtt5 §6]:
//!
//! * unacknowledged PUBLISH packets with QoS > 0, **with their original
//!   Packet Identifiers and DUP set to 1**, in the order the originals were
//!   sent;
//! * PUBREL packets not yet answered by PUBCOMP.
//!
//! What is **not** resent, ever: QoS 0 PUBLISH packets; a PUBLISH whose PUBREL
//! has already been sent ([MQTT-4.3.3-6]); a PUBLISH already answered with a
//! code of 0x80 or above ([MQTT-4.4.0-2]); and SUBSCRIBE or UNSUBSCRIBE, which
//! are not in the retransmission list at all.
//!
//! # What is deliberately not session state
//!
//! Two things a reader might expect here are per *connection* and are
//! re-initialised each time, so they live with the connection and not with the
//! session: the send quota and `Receive Maximum`, which are "explicitly not
//! session state" (4.9) [mqtt5 §5], and the Topic Alias mappings, which a
//! receiver "MUST NOT carry across connections" ([MQTT-3.3.2-7]) [mqtt5 §2].
//! [`StoredPublish`] therefore has no `topic_alias` field at all — a stored
//! message keeps its full Topic Name, because the alias it was sent under is
//! meaningless on the next connection.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use weida_mqtt_codec::{PayloadFormat, Properties, Publish, QoS, SubackReasonCode};

use crate::error::{Error, Result};
use crate::filter::{Subscription, Subscriptions};
use crate::limits::Limits;

/// A PUBLISH held for possible retransmission.
///
/// Owned, because it outlives the connection it was first sent on. The
/// property subset is what the specification requires a server to forward
/// unaltered ([MQTT-3.3.2-4], [MQTT-3.3.2-15] to [MQTT-3.3.2-20])
/// [mqtt5 §3], minus `Topic Alias`, which is per connection and is rebuilt —
/// or not — by the connection that resends this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredPublish {
    /// The full Topic Name. Never an alias: see the module documentation.
    pub topic: String,
    /// The Application Message.
    pub payload: Vec<u8>,
    /// QoS 1 or 2. QoS 0 is never stored, because it is never resent.
    pub qos: QoS,
    /// RETAIN as published.
    pub retain: bool,
    /// `Payload Format Indicator`.
    pub payload_format_indicator: Option<PayloadFormat>,
    /// `Message Expiry Interval`.
    ///
    /// Not decremented here. "Once a QoS 2 PUBLISH has been sent, expiry MUST
    /// NOT be applied ([MQTT-4.3.3-7])" and the handshake MUST complete anyway
    /// ([MQTT-4.3.3-13]) [mqtt5 §6]; rewriting the interval downwards for
    /// waiting time is a *server's* obligation ([MQTT-3.3.2-6]), not a
    /// publisher's.
    pub message_expiry_interval: Option<u32>,
    /// `Content Type`.
    pub content_type: Option<String>,
    /// `Response Topic`.
    pub response_topic: Option<String>,
    /// `Correlation Data`.
    pub correlation_data: Option<Vec<u8>>,
    /// `User Property` pairs, in order.
    pub user_properties: Vec<(String, String)>,
}

impl StoredPublish {
    /// The borrowed shape the codec encodes, with `dup` as the caller states
    /// it.
    ///
    /// "An outgoing DUP value MUST be determined solely by whether the
    /// outgoing PUBLISH packet is a retransmission" ([MQTT-3.3.1-3])
    /// [mqtt5 §6], so it is a parameter here and never a stored field.
    #[must_use]
    pub fn to_publish<'a>(
        &'a self,
        packet_id: u16,
        dup: bool,
        user_properties: &'a [(&'a str, &'a str)],
    ) -> Publish<'a> {
        Publish {
            topic: &self.topic,
            payload: &self.payload,
            qos: self.qos,
            dup,
            retain: self.retain,
            packet_id: Some(packet_id),
            properties: Properties {
                payload_format_indicator: self.payload_format_indicator,
                message_expiry_interval: self.message_expiry_interval,
                content_type: self.content_type.as_deref(),
                response_topic: self.response_topic.as_deref(),
                correlation_data: self.correlation_data.as_deref(),
                ..Properties::new()
            }
            .with_user_properties(user_properties),
        }
    }

    /// The user properties as the borrowed pairs [`StoredPublish::to_publish`]
    /// takes.
    #[must_use]
    pub fn borrowed_user_properties(&self) -> Vec<(&str, &str)> {
        self.user_properties
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect()
    }
}

/// How far an outbound QoS > 0 message has got.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// QoS 1: the PUBLISH is out, the PUBACK is not in ([MQTT-4.3.2-3]).
    AwaitingPuback,
    /// QoS 2: the PUBLISH is out, the PUBREC is not in ([MQTT-4.3.3-3]).
    AwaitingPubrec,
    /// QoS 2: the PUBREL is out, the PUBCOMP is not in ([MQTT-4.3.3-5]).
    ///
    /// Once here the PUBLISH "MUST NOT" be resent ([MQTT-4.3.3-6]) — only the
    /// PUBREL is — which is the distinction that makes this a separate stage
    /// rather than a flag.
    AwaitingPubcomp,
}

/// One outbound message in flight.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InFlight {
    /// The Packet Identifier, held until the exchange completes.
    pub packet_id: u16,
    /// How far it has got.
    pub stage: Stage,
    /// The message, for as long as it may still have to be resent.
    ///
    /// Dropped on the transition to [`Stage::AwaitingPubcomp`], because from
    /// there only the PUBREL is resent and keeping the payload would be
    /// holding a copy of every in-flight QoS 2 body for a round trip longer
    /// than the protocol needs it.
    pub message: Option<StoredPublish>,
    /// The order this was first sent in, so [`Session::resend`] can honour
    /// "in the order the originals were sent" ([MQTT-4.6.0-1]).
    sequence: u64,
}

/// What to put on the wire when a session resumes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resend {
    /// An unacknowledged PUBLISH, to be sent with **DUP 1** and its original
    /// Packet Identifier.
    Publish {
        /// The original identifier.
        packet_id: u16,
        /// The stored message.
        message: StoredPublish,
    },
    /// A PUBREL not yet answered by PUBCOMP.
    Pubrel {
        /// The original identifier.
        packet_id: u16,
    },
}

/// What a CONNACK's `Session Present` obliges the client to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resumption {
    /// The server resumed the session and the client has state to resume
    /// with: the exchanges of [`Session::resend`] go back on the wire.
    Resumed,
    /// Neither side has anything: a fresh session.
    Fresh,
    /// The server had no session and the client did, so the client "MUST
    /// discard its Session State" ([MQTT-3.2.2-5]) (3.2.2.1.1) [mqtt5 §1].
    /// [`Session::accept_connack`] has already discarded it.
    Discarded,
}

/// The client half of one session, keyed by its Client Identifier.
///
/// Cloning shares the state, because the connection task records
/// acknowledgements into the same session the application holds. The
/// application's clone is what makes the session outlive the connection,
/// which is the whole difference between a session and a connection.
#[derive(Clone, Debug)]
pub struct Session {
    client_id: String,
    state: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {
    outbound: BTreeMap<u16, InFlight>,
    /// QoS 2 Packet Identifiers received, answered with PUBREC, and not yet
    /// released by a PUBREL. Holding these is what lets the receiver "answer
    /// any repeat PUBLISH with the same identifier by another PUBREC and MUST
    /// NOT cause a duplicate onward delivery" ([MQTT-4.3.3-10]) [mqtt5 §6].
    inbound_qos2: BTreeSet<u16>,
    /// Packet Identifiers spent on a SUBSCRIBE or UNSUBSCRIBE awaiting its
    /// acknowledgement. They occupy the **same** space as the publish
    /// identifiers ([MQTT-2.2.1-3]) and are never resent, because "SUBSCRIBE
    /// and UNSUBSCRIBE are not in the retransmission list at all" (4.4)
    /// [mqtt5 §6].
    control: BTreeSet<u16>,
    /// Whether a session for this Client Identifier outlives the connection,
    /// because the last CONNECT declared a non-zero `Session Expiry
    /// Interval`.
    ///
    /// **The piece of client-side session state 4.1's list omits.** Without
    /// it [MQTT-3.2.2-4] cannot be applied: a correct resumption of a session
    /// that happens to hold no unacknowledged messages is indistinguishable
    /// from a server inventing one.
    server_holds_session: bool,
    /// The client's mirror of the subscriptions the server holds.
    subscriptions: Subscriptions,
    /// Where the next identifier search starts, so allocation walks the space
    /// rather than always offering the lowest free number.
    next_packet_id: u16,
    sequence: u64,
    /// The `Session Expiry Interval` the last CONNECT declared, in seconds.
    /// `None` until a CONNECT has been built from this session.
    declared_expiry: Option<u32>,
    /// The ceiling on what **this client** may have in flight toward the
    /// server: the peer's `Receive Maximum` once a CONNACK has said so
    /// ([MQTT-4.9.0-1]), and the client's own configured value as a
    /// placeholder before that.
    send_quota: u16,
    /// The ceiling on what the **server** may have in flight toward this
    /// client: the `Receive Maximum` this client declared in CONNECT. A
    /// different number from `send_quota`, and never overwritten by the
    /// server's.
    receive_maximum: u16,
}

impl Session {
    /// A fresh, empty session for `client_id`.
    ///
    /// `limits.receive_maximum` is the ceiling until a CONNACK replaces it
    /// with the server's, which is the number [MQTT-4.9.0-1] actually
    /// requires.
    #[must_use]
    pub fn new(client_id: impl Into<String>, limits: &Limits) -> Session {
        Session {
            client_id: client_id.into(),
            state: Arc::new(Mutex::new(State {
                outbound: BTreeMap::new(),
                inbound_qos2: BTreeSet::new(),
                control: BTreeSet::new(),
                server_holds_session: false,
                subscriptions: Subscriptions::new(),
                next_packet_id: 1,
                sequence: 0,
                declared_expiry: None,
                send_quota: limits.receive_maximum,
                receive_maximum: limits.receive_maximum,
            })),
        }
    }

    /// The Client Identifier this session is keyed by. "It is the key to
    /// session state" [mqtt5 §2].
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // A panic while holding this lock would be a bug in this crate, and
        // poisoning would then hide it behind a second, unrelated error.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether there is nothing to resume: no unacknowledged outbound message
    /// and no unreleased inbound QoS 2 identifier.
    ///
    /// This is the question [MQTT-3.2.2-4] turns on — "a Client with no
    /// Session State that receives Session Present 1 MUST close the Network
    /// Connection" — so it is the session's own answer and not a guess made by
    /// the connection.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let state = self.lock();
        state.outbound.is_empty() && state.inbound_qos2.is_empty()
    }

    /// Unacknowledged outbound messages.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.lock().outbound.len()
    }

    /// Received QoS 2 identifiers awaiting a PUBREL.
    #[must_use]
    pub fn awaiting_pubrel(&self) -> usize {
        self.lock().inbound_qos2.len()
    }

    /// Throws the whole session away.
    ///
    /// Called for Clean Start 1 before connecting ([MQTT-3.1.2-4]) and for a
    /// CONNACK with `Session Present` 0 where the client had state
    /// ([MQTT-3.2.2-5]) [mqtt5 §1].
    pub fn clear(&self) {
        let mut state = self.lock();
        state.outbound.clear();
        state.inbound_qos2.clear();
        state.control.clear();
        // "Subscriptions do not survive Clean Start 1" [mqtt5 §1], so the
        // mirror goes with the rest of the session.
        state.subscriptions.clear();
        // Clean Start 1 discards the server's half too ([MQTT-3.1.2-4]), so
        // after this nothing claims a session exists until the next CONNACK.
        state.server_holds_session = false;
        state.next_packet_id = 1;
        state.sequence = 0;
    }

    /// Records what a CONNECT declared, and refuses the DISCONNECT rule's
    /// precondition being lost.
    ///
    /// # Errors
    ///
    /// Never; the signature is a `Result` because
    /// [`Session::revise_expiry_on_disconnect`] reads what this stored and a
    /// caller that skipped it would get a silent zero.
    pub fn declare_expiry(&self, seconds: Option<u32>) {
        self.lock().declared_expiry = Some(seconds.unwrap_or(0));
    }

    /// Checks a `Session Expiry Interval` a DISCONNECT is about to carry.
    ///
    /// **The rule the codec deliberately deferred.** "The Client MAY set a new
    /// Session Expiry Interval on DISCONNECT, so a session's lifetime can be
    /// shortened or extended at close. A non-zero value when CONNECT carried
    /// zero is a Protocol Error" (3.14.2.2.2) [mqtt5 §1]. It needs the CONNECT
    /// that came before, which a sans-I/O codec does not have and a session
    /// does, which is why `weida-mqtt-codec`'s `control` module names this item
    /// as its owner.
    ///
    /// Enforced **before sending**, so the client never earns the server's
    /// DISCONNECT 0x82 for it.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for a non-zero interval where the CONNECT
    /// declared zero or none.
    pub fn revise_expiry_on_disconnect(&self, seconds: Option<u32>) -> Result<()> {
        let Some(new) = seconds else {
            return Ok(());
        };
        if new == 0 {
            // Shortening to zero is always permitted, and is what the
            // specification advises a client that is finished to do, so that a
            // session it will never return to is not orphaned (3.1.2.11.2)
            // [mqtt5 §9].
            return Ok(());
        }
        let declared = self.lock().declared_expiry.unwrap_or(0);
        if declared == 0 {
            return Err(Error::Configuration(format!(
                "a Session Expiry Interval of {new} on DISCONNECT is a Protocol Error when \
                 CONNECT declared zero or none (3.14.2.2.2)"
            )));
        }
        Ok(())
    }

    /// Applies a CONNACK's `Session Present` to this session.
    ///
    /// The three cases 3.2.2.1.1 defines, and the fourth it forbids:
    ///
    /// | Clean Start | `Session Present` | local state | outcome |
    /// | --- | --- | --- | --- |
    /// | 1 | 0 | discarded before connecting | [`Resumption::Fresh`] |
    /// | 1 | 1 | — | **refused**: the client has no state, and [MQTT-3.2.2-4] says close |
    /// | 0 | 1 | kept | [`Resumption::Resumed`] |
    /// | 0 | 0 | discarded now ([MQTT-3.2.2-5]) | [`Resumption::Discarded`] |
    ///
    /// # Errors
    ///
    /// [`Error::SessionPresentWithoutState`] for the forbidden row, which
    /// obliges the client to close the connection.
    pub fn accept_connack(&self, session_present: bool) -> Result<Resumption> {
        if !session_present {
            // "A Client that receives Session Present 0 and has Session State
            // MUST discard it" ([MQTT-3.2.2-5]).
            let fresh = self.is_empty();
            if !fresh {
                self.clear();
            }
            self.note_session_exists();
            return Ok(if fresh {
                Resumption::Fresh
            } else {
                Resumption::Discarded
            });
        }
        // "A Client with no Session State that receives Session Present 1 MUST
        // close the Network Connection" ([MQTT-3.2.2-4]). Trusting it instead
        // would mean believing the server holds exchanges this client has no
        // record of, and then answering acknowledgements it cannot match.
        //
        // **"No Session State" is not the same as "holds no messages"**, and
        // reading it that way makes the rule unusable: 4.1's client-side list
        // is only the unacknowledged QoS 1 and 2 exchanges [mqtt5 §2], so a
        // client that connected with Clean Start 0 and a non-zero Session
        // Expiry Interval and then published nothing would be obliged to close
        // on its own correct resumption. What this client tracks instead is
        // whether a session exists on the server at all - which is what the
        // server's flag is actually claiming - and that is the third piece of
        // client-side session state the specification's list omits.
        if self.is_empty() && !self.lock().server_holds_session {
            return Err(Error::SessionPresentWithoutState);
        }
        self.note_session_exists();
        Ok(Resumption::Resumed)
    }

    /// Records whether the session this client just declared outlives the
    /// connection, which is what the next CONNACK's `Session Present` can
    /// legitimately be 1 about.
    ///
    /// A `Session Expiry Interval` of zero or absent means "the session ends
    /// when the connection ends" (3.1.2.11.2) [mqtt5 §1], so after such a
    /// connection a `Session Present` 1 really is the forbidden row.
    fn note_session_exists(&self) {
        let mut state = self.lock();
        state.server_holds_session = state.declared_expiry.unwrap_or(0) > 0;
    }

    /// The exchanges to put back on the wire, in the order the originals were
    /// sent ([MQTT-4.6.0-1]).
    ///
    /// **The only retransmission this crate performs**, and it is called from
    /// exactly one place: just after a CONNACK with `Session Present` 1.
    #[must_use]
    pub fn resend(&self) -> Vec<Resend> {
        let state = self.lock();
        let mut ordered: Vec<&InFlight> = state.outbound.values().collect();
        ordered.sort_by_key(|entry| entry.sequence);
        ordered
            .into_iter()
            .map(|entry| match (entry.stage, &entry.message) {
                // "A PUBLISH whose PUBREL has already been sent MUST NOT be
                // resent" ([MQTT-4.3.3-6]); the PUBREL is.
                (Stage::AwaitingPubcomp, _) | (_, None) => Resend::Pubrel {
                    packet_id: entry.packet_id,
                },
                (_, Some(message)) => Resend::Publish {
                    packet_id: entry.packet_id,
                    message: message.clone(),
                },
            })
            .collect()
    }

    /// Replaces the ceiling on concurrent unacknowledged messages with the
    /// server's `Receive Maximum`.
    ///
    /// "The sender sets an initial send quota, non-zero and not exceeding the
    /// peer's Receive Maximum" ([MQTT-4.9.0-1]) [mqtt5 §5], so this is the
    /// protocol's own number and not a second one invented beside it. The
    /// quota itself is per connection and re-initialised each time — it is
    /// "explicitly not session state" (4.9) — but the *count of stored
    /// exchanges* is session state, so the two meet here.
    ///
    /// **This replaces the send side only.** The two ceilings are different
    /// numbers and conflating them is a real bug: the send quota is the
    /// *server's* `Receive Maximum` and bounds what this client may have in
    /// flight toward it, while [`Session::receive_maximum`] is the *client's*
    /// own declared value and bounds what the server may have in flight
    /// toward this client (3.1.2.11.3, 3.2.2.3.3) [mqtt5 §11]. A server that
    /// declares nothing means 65,535 for the first and changes nothing about
    /// the second.
    pub fn set_send_quota(&self, server_receive_maximum: u16) {
        self.lock().send_quota = server_receive_maximum;
    }

    /// The send quota's ceiling: the server's `Receive Maximum`
    /// ([MQTT-4.9.0-1]).
    #[must_use]
    pub fn send_quota(&self) -> u16 {
        self.lock().send_quota
    }

    /// This client's own declared `Receive Maximum`: how many unacknowledged
    /// QoS 2 messages the server may have in flight toward it.
    #[must_use]
    pub fn receive_maximum(&self) -> u16 {
        self.lock().receive_maximum
    }

    /// Allocates a Packet Identifier for a SUBSCRIBE or UNSUBSCRIBE.
    ///
    /// **The identifier space is one unified space per session, "shared
    /// across PUBLISH (QoS > 0), SUBSCRIBE and UNSUBSCRIBE"**
    /// ([MQTT-2.2.1-3]) [mqtt5 §2], so this competes with
    /// [`Session::allocate`] for the same 65,535 numbers and is not a second
    /// counter beside it.
    ///
    /// Two things it does **not** do, and both are the specification's:
    ///
    /// * it does not count against the send quota, whose unit is "one QoS 1
    ///   or QoS 2 PUBLISH packet — not bytes, not QoS 0, not any other packet
    ///   type" (4.9) [mqtt5 §5];
    /// * it stores nothing, because "SUBSCRIBE and UNSUBSCRIBE are not in the
    ///   retransmission list at all" (4.4) [mqtt5 §6] — a subscription that
    ///   goes unanswered is the caller's to retry, and retrying it is a new
    ///   SUBSCRIBE with a new identifier rather than a resend.
    ///
    /// # Errors
    ///
    /// [`Error::QuotaExhausted`] with the 65,535 hard ceiling once the whole
    /// space is in use.
    pub fn allocate_control(&self) -> Result<u16> {
        let mut state = self.lock();
        let packet_id = next_free(&state.outbound, &state.control, state.next_packet_id)
            .ok_or(Error::QuotaExhausted { quota: u16::MAX })?;
        state.next_packet_id = packet_id.wrapping_add(1).max(1);
        state.control.insert(packet_id);
        Ok(packet_id)
    }

    /// Releases a SUBSCRIBE or UNSUBSCRIBE identifier, which happens "on
    /// SUBACK/UNSUBACK" [mqtt5 §2].
    ///
    /// Returns whether it was held.
    pub fn release_control(&self, packet_id: u16) -> bool {
        self.lock().control.remove(&packet_id)
    }

    /// The client's mirror of the subscriptions the server holds for this
    /// session.
    ///
    /// 4.1 lists the subscriptions under the **server's** half of the session
    /// state, not the client's [mqtt5 §2], so this is a mirror and not an
    /// authority: the server is the one that replaces a re-subscribed filter
    /// and grants a maximum QoS. The client keeps it for two things it cannot
    /// otherwise do — telling which of its own filters a delivery matched
    /// where the server sends no `Subscription Identifier`, and knowing what
    /// to re-subscribe after a Clean Start — and it is discarded with the
    /// session, because "subscriptions do not survive Clean Start 1"
    /// [mqtt5 §1].
    pub fn subscriptions(&self) -> Subscriptions {
        self.lock().subscriptions.clone()
    }

    /// Records what a SUBACK granted, replacing any entry with the same
    /// filter.
    ///
    /// "A session cannot hold two subscriptions with the same filter, so the
    /// filter is the key", and re-subscribing "MUST replace the subscription
    /// without losing messages" ([MQTT-3.8.4-3], [MQTT-3.8.4-4]) (3.8.4)
    /// [mqtt5 §2]. The replacement is the server's to perform; this keeps the
    /// mirror from growing a second entry for one subscription.
    pub fn record_subscription(
        &self,
        subscription: Subscription,
        identifier: Option<u32>,
        granted: SubackReasonCode,
    ) {
        self.lock()
            .subscriptions
            .record(subscription, identifier, granted);
    }

    /// Forgets a filter, on an UNSUBACK that reported success.
    pub fn forget_subscription(&self, filter: &str) -> bool {
        self.lock().subscriptions.remove(filter)
    }

    /// Allocates an unused Packet Identifier and stores `message` against it.
    ///
    /// The space is "a single unified space per session, shared across PUBLISH
    /// (QoS > 0), SUBSCRIBE and UNSUBSCRIBE", two-byte and non-zero
    /// ([MQTT-2.2.1-3]) [mqtt5 §2], so 65,535 is the hard ceiling. The soft
    /// one is the peer's `Receive Maximum`, and it bites first.
    ///
    /// # Errors
    ///
    /// [`Error::QuotaExhausted`] once as many QoS > 0 messages are
    /// unacknowledged as the peer's `Receive Maximum` permits. Exhausting the
    /// quota "stalls the sender rather than exceeding it" [mqtt5 §5], and
    /// stalling is the caller's to do — this reports, and
    /// [`crate::Client::publish`] waits.
    pub fn allocate(&self, message: StoredPublish) -> Result<u16> {
        debug_assert_ne!(message.qos, QoS::AtMostOnce, "QoS 0 is never stored");
        let mut state = self.lock();
        if state.outbound.len() >= usize::from(state.send_quota) {
            return Err(Error::QuotaExhausted {
                quota: state.send_quota,
            });
        }

        let packet_id = next_free(&state.outbound, &state.control, state.next_packet_id)
            .ok_or(Error::QuotaExhausted { quota: u16::MAX })?;
        state.next_packet_id = packet_id.wrapping_add(1).max(1);

        let sequence = state.sequence;
        state.sequence += 1;
        let stage = match message.qos {
            QoS::AtLeastOnce => Stage::AwaitingPuback,
            _ => Stage::AwaitingPubrec,
        };
        state.outbound.insert(
            packet_id,
            InFlight {
                packet_id,
                stage,
                message: Some(message),
                sequence,
            },
        );
        Ok(packet_id)
    }

    /// How far `packet_id` has got, for a caller matching an acknowledgement.
    #[must_use]
    pub fn stage_of(&self, packet_id: u16) -> Option<Stage> {
        self.lock()
            .outbound
            .get(&packet_id)
            .map(|entry| entry.stage)
    }

    /// Releases `packet_id` and forgets its message.
    ///
    /// An identifier frees "on PUBACK, PUBCOMP, a PUBREC with code >= 0x80, or
    /// SUBACK/UNSUBACK" [mqtt5 §2]. Returns whether it was held, so a caller
    /// can tell an acknowledgement it was waiting for from one it was not —
    /// the latter being the mismatch 0x92 and 0x91 report [mqtt5 §8].
    pub fn release(&self, packet_id: u16) -> bool {
        self.lock().outbound.remove(&packet_id).is_some()
    }

    /// Moves `packet_id` from awaiting PUBREC to awaiting PUBCOMP, dropping
    /// the stored message.
    ///
    /// "On a PUBREC with code < 0x80 send PUBREL with the same identifier"
    /// ([MQTT-4.3.3-4]) and from then on the PUBLISH "MUST NOT" be resent
    /// ([MQTT-4.3.3-6]) [mqtt5 §6], so the body is released here rather than
    /// held for a round trip nothing can use it in.
    ///
    /// Returns whether the identifier was in [`Stage::AwaitingPubrec`].
    pub fn pubrec_received(&self, packet_id: u16) -> bool {
        let mut state = self.lock();
        match state.outbound.get_mut(&packet_id) {
            Some(entry) if entry.stage == Stage::AwaitingPubrec => {
                entry.stage = Stage::AwaitingPubcomp;
                entry.message = None;
                true
            }
            _ => false,
        }
    }

    /// Records a received QoS 2 Packet Identifier, answered with PUBREC.
    ///
    /// Returns `true` for a first sight and `false` for a repeat. A repeat
    /// must be answered with another PUBREC and MUST NOT cause a duplicate
    /// onward delivery ([MQTT-4.3.3-10]) [mqtt5 §6], so the boolean *is* the
    /// duplicate suppression.
    ///
    /// # Errors
    ///
    /// [`Error::ReceiveMaximumExceeded`] once as many identifiers are
    /// unreleased as this client declared it would accept. "Exceeding the
    /// peer's Receive Maximum earns DISCONNECT 0x93" [mqtt5 §5], and this is
    /// the client's side of that: the server broke the quota it was told.
    pub fn inbound_qos2_received(&self, packet_id: u16) -> Result<bool> {
        let mut state = self.lock();
        if state.inbound_qos2.contains(&packet_id) {
            return Ok(false);
        }
        // Our own declared `Receive Maximum`, not the peer's: this table is
        // what the **server** may have in flight toward us.
        if state.inbound_qos2.len() >= usize::from(state.receive_maximum) {
            return Err(Error::ReceiveMaximumExceeded {
                quota: state.receive_maximum,
            });
        }
        state.inbound_qos2.insert(packet_id);
        Ok(true)
    }

    /// Releases a received QoS 2 identifier on PUBREL.
    ///
    /// Returns whether it was held. `false` is what a PUBCOMP of 0x92 (Packet
    /// Identifier not found) reports, which the specification itself declines
    /// to call an error: it "is not an error during recovery, but at other
    /// times indicates a mismatch between the Session State on the Client and
    /// Server" (3.6.2.1) [mqtt5 §6].
    pub fn inbound_qos2_released(&self, packet_id: u16) -> bool {
        self.lock().inbound_qos2.remove(&packet_id)
    }

    /// Whether `packet_id` is a received QoS 2 identifier awaiting release.
    #[must_use]
    pub fn holds_inbound_qos2(&self, packet_id: u16) -> bool {
        self.lock().inbound_qos2.contains(&packet_id)
    }
}

/// The first identifier at or after `from` that is in use by neither a stored
/// publish nor an unacknowledged SUBSCRIBE or UNSUBSCRIBE, wrapping once and
/// skipping 0. One space, three packet types ([MQTT-2.2.1-3]).
fn next_free(
    outbound: &BTreeMap<u16, InFlight>,
    control: &BTreeSet<u16>,
    from: u16,
) -> Option<u16> {
    let start = from.max(1);
    for offset in 0..u32::from(u16::MAX) {
        let candidate = (u32::from(start - 1) + offset) % u32::from(u16::MAX) + 1;
        let candidate = candidate as u16;
        if !outbound.contains_key(&candidate) && !control.contains(&candidate) {
            return Some(candidate);
        }
    }
    None
}

/// Seconds, for a `Session Expiry Interval`.
#[must_use]
pub(crate) fn expiry_seconds(interval: Option<Duration>) -> Option<u32> {
    interval.map(|interval| interval.as_secs().min(u64::from(u32::MAX)) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(topic: &str, qos: QoS) -> StoredPublish {
        StoredPublish {
            topic: topic.into(),
            payload: topic.as_bytes().to_vec(),
            qos,
            retain: false,
            payload_format_indicator: None,
            message_expiry_interval: None,
            content_type: None,
            response_topic: None,
            correlation_data: None,
            user_properties: Vec::new(),
        }
    }

    fn session(quota: u16) -> Session {
        let limits = Limits {
            receive_maximum: quota,
            ..Limits::default()
        };
        Session::new("c", &limits)
    }

    /// The three cases 3.2.2.1.1 defines and the one it forbids.
    #[test]
    fn the_session_present_matrix_is_the_specifications() {
        // Clean Start 1: the client cleared its state, so Session Present 0 is
        // a fresh session.
        let fresh = session(10);
        assert_eq!(fresh.accept_connack(false).unwrap(), Resumption::Fresh);

        // Clean Start 0 with state, Session Present 1: resume.
        let resumed = session(10);
        resumed.allocate(message("a", QoS::AtLeastOnce)).unwrap();
        assert_eq!(resumed.accept_connack(true).unwrap(), Resumption::Resumed);
        assert_eq!(resumed.in_flight(), 1, "the state survived");

        // Clean Start 0 with state, Session Present 0: [MQTT-3.2.2-5], the
        // client discards.
        let discarded = session(10);
        discarded.allocate(message("a", QoS::AtLeastOnce)).unwrap();
        assert_eq!(
            discarded.accept_connack(false).unwrap(),
            Resumption::Discarded
        );
        assert!(discarded.is_empty(), "the state was discarded");

        // No state, Session Present 1: [MQTT-3.2.2-4], the client MUST close.
        let empty = session(10);
        assert!(matches!(
            empty.accept_connack(true),
            Err(Error::SessionPresentWithoutState)
        ));
    }

    /// The forbidden row of [MQTT-3.2.2-4] is "no Session State", and reading
    /// that as "holds no unacknowledged messages" makes every correct
    /// resumption of an idle session illegal.
    ///
    /// So what decides it is whether a session exists on the server at all:
    /// a CONNECT that declared a non-zero `Session Expiry Interval` leaves
    /// one behind (3.1.2.11.2), and one that declared zero or none does not.
    #[test]
    fn an_idle_session_that_outlives_its_connection_may_be_resumed() {
        // First connection: Clean Start 0, Session Expiry 300, and nothing
        // ever published.
        let session = session(10);
        session.declare_expiry(Some(300));
        assert_eq!(session.accept_connack(false).unwrap(), Resumption::Fresh);
        assert!(session.is_empty(), "no exchange ever happened");

        // The reconnect: Session Present 1 is now the truth, and refusing it
        // would be refusing our own session back.
        session.declare_expiry(Some(300));
        assert_eq!(session.accept_connack(true).unwrap(), Resumption::Resumed);

        // A session that declared no expiry ends with its connection, so
        // Session Present 1 afterwards really is the forbidden row.
        let ephemeral = Session::new("c", &Limits::default());
        ephemeral.declare_expiry(None);
        assert_eq!(ephemeral.accept_connack(false).unwrap(), Resumption::Fresh);
        ephemeral.declare_expiry(None);
        assert!(matches!(
            ephemeral.accept_connack(true),
            Err(Error::SessionPresentWithoutState)
        ));

        // And Clean Start 1 throws the claim away with the rest of it.
        session.declare_expiry(Some(300));
        session.clear();
        assert!(matches!(
            session.accept_connack(true),
            Err(Error::SessionPresentWithoutState)
        ));
    }

    /// [MQTT-4.6.0-1]: resends go out in the order the originals were sent,
    /// which a map keyed by identifier does not give for free.
    #[test]
    fn resends_keep_the_original_order_and_identifiers() {
        let session = session(10);
        // Allocate and release so that the next identifiers are not in
        // ascending order relative to the send order.
        let first = session.allocate(message("a", QoS::AtLeastOnce)).unwrap();
        let second = session.allocate(message("b", QoS::AtLeastOnce)).unwrap();
        let third = session.allocate(message("c", QoS::AtLeastOnce)).unwrap();
        assert!(session.release(second));
        let fourth = session.allocate(message("d", QoS::AtLeastOnce)).unwrap();

        let resent = session.resend();
        let ids: Vec<u16> = resent
            .iter()
            .map(|item| match item {
                Resend::Publish { packet_id, .. } | Resend::Pubrel { packet_id } => *packet_id,
            })
            .collect();
        assert_eq!(
            ids,
            [first, third, fourth],
            "send order, not identifier order"
        );
        let topics: Vec<&str> = resent
            .iter()
            .map(|item| match item {
                Resend::Publish { message, .. } => message.topic.as_str(),
                Resend::Pubrel { .. } => "pubrel",
            })
            .collect();
        assert_eq!(topics, ["a", "c", "d"]);
    }

    /// [MQTT-4.3.3-6]: once the PUBREL is out the PUBLISH is never resent, and
    /// the body is dropped rather than held for a round trip nothing can use
    /// it in.
    #[test]
    fn a_published_message_past_pubrec_resends_only_the_pubrel() {
        let session = session(10);
        let id = session.allocate(message("a", QoS::ExactlyOnce)).unwrap();
        assert_eq!(session.stage_of(id), Some(Stage::AwaitingPubrec));
        assert_eq!(
            session.resend(),
            [Resend::Publish {
                packet_id: id,
                message: message("a", QoS::ExactlyOnce)
            }]
        );

        assert!(session.pubrec_received(id));
        assert_eq!(session.stage_of(id), Some(Stage::AwaitingPubcomp));
        assert_eq!(session.resend(), [Resend::Pubrel { packet_id: id }]);

        // A second PUBREC for the same identifier is not a transition.
        assert!(!session.pubrec_received(id));

        assert!(session.release(id));
        assert!(session.resend().is_empty());
    }

    /// QoS 0 is never stored, so it can never be resent — "not resent: QoS 0
    /// PUBLISH packets, ever" [mqtt5 §6]. The store simply has no room for
    /// one.
    #[test]
    fn qos_zero_never_enters_the_session() {
        let session = session(10);
        assert!(session.is_empty());
        // A QoS 0 publish allocates no identifier at all (3.3.2.2), so there
        // is nothing to store and nothing to resend.
        assert!(session.resend().is_empty());
    }

    /// [MQTT-4.9.0-1]: the ceiling is the peer's `Receive Maximum`, and
    /// exhausting it reports rather than exceeding it.
    #[test]
    fn allocation_stops_at_the_peers_receive_maximum() {
        let session = session(2);
        let first = session.allocate(message("a", QoS::AtLeastOnce)).unwrap();
        let second = session.allocate(message("b", QoS::AtLeastOnce)).unwrap();
        assert!(matches!(
            session.allocate(message("c", QoS::AtLeastOnce)),
            Err(Error::QuotaExhausted { quota: 2 })
        ));

        // Releasing one makes room for exactly one more.
        assert!(session.release(first));
        let third = session.allocate(message("c", QoS::AtLeastOnce)).unwrap();
        assert_ne!(third, second);
        assert!(matches!(
            session.allocate(message("d", QoS::AtLeastOnce)),
            Err(Error::QuotaExhausted { quota: 2 })
        ));

        // And the server's number replaces the client's once CONNACK says so
        // — on the **send** side only, which is the distinction a single
        // `quota` field got wrong.
        session.set_send_quota(4);
        assert_eq!(session.send_quota(), 4);
        assert_eq!(
            session.receive_maximum(),
            2,
            "the client's own declared Receive Maximum is untouched"
        );
        assert!(session.allocate(message("d", QoS::AtLeastOnce)).is_ok());
    }

    /// Identifiers are non-zero and are not reused while in flight
    /// ([MQTT-2.2.1-3]).
    #[test]
    fn identifiers_are_non_zero_and_distinct_while_in_flight() {
        let session = session(64);
        let mut seen = BTreeSet::new();
        for index in 0..64 {
            let id = session
                .allocate(message(&format!("t{index}"), QoS::AtLeastOnce))
                .unwrap();
            assert_ne!(id, 0);
            assert!(seen.insert(id), "{id} was handed out twice");
        }
    }

    /// The allocator walks the space rather than always offering the lowest
    /// free number, which is what keeps an identifier from being reused the
    /// instant it is freed — the case a server answers with 0x91 (Packet
    /// identifier in use) when its own state disagrees [mqtt5 §8].
    #[test]
    fn a_freed_identifier_is_not_handed_straight_back() {
        let session = session(10);
        let first = session.allocate(message("a", QoS::AtLeastOnce)).unwrap();
        assert!(session.release(first));
        let second = session.allocate(message("b", QoS::AtLeastOnce)).unwrap();
        assert_ne!(second, first);
    }

    /// [MQTT-4.3.3-10]: a repeated QoS 2 identifier is answered again and
    /// delivered once. The boolean is that suppression.
    #[test]
    fn a_repeated_inbound_qos2_identifier_is_not_delivered_twice() {
        let session = session(10);
        assert!(session.inbound_qos2_received(7).unwrap(), "first sight");
        assert!(!session.inbound_qos2_received(7).unwrap(), "a repeat");
        assert!(session.holds_inbound_qos2(7));
        assert_eq!(session.awaiting_pubrel(), 1);

        // PUBREL releases it, and "after PUBCOMP treat a later PUBLISH with
        // that identifier as new" ([MQTT-4.3.3-12]).
        assert!(session.inbound_qos2_released(7));
        assert!(!session.inbound_qos2_released(7), "0x92's case");
        assert!(session.inbound_qos2_received(7).unwrap(), "new again");
    }

    /// The client's own `Receive Maximum` bounds what the server may have in
    /// flight toward it, and a server that exceeds it is reported rather than
    /// letting the table grow.
    #[test]
    fn an_inbound_flood_past_receive_maximum_is_refused() {
        let session = session(2);
        assert!(session.inbound_qos2_received(1).unwrap());
        assert!(session.inbound_qos2_received(2).unwrap());
        assert!(matches!(
            session.inbound_qos2_received(3),
            Err(Error::ReceiveMaximumExceeded { quota: 2 })
        ));
        // A repeat of one already held is still answered, because answering it
        // is required and costs no new state.
        assert!(!session.inbound_qos2_received(1).unwrap());
    }

    /// The rule the codec deferred: "a non-zero value when CONNECT carried
    /// zero is a Protocol Error" (3.14.2.2.2), enforced before sending.
    #[test]
    fn zero_then_non_zero_session_expiry_is_refused() {
        let session = session(10);

        // CONNECT declared nothing, which means zero.
        session.declare_expiry(None);
        assert!(session.revise_expiry_on_disconnect(None).is_ok());
        assert!(
            session.revise_expiry_on_disconnect(Some(0)).is_ok(),
            "shortening to zero is always permitted"
        );
        assert!(session.revise_expiry_on_disconnect(Some(30)).is_err());

        // CONNECT declared zero explicitly: the same.
        session.declare_expiry(Some(0));
        assert!(session.revise_expiry_on_disconnect(Some(1)).is_err());

        // CONNECT declared non-zero: the interval may be revised either way.
        session.declare_expiry(Some(60));
        assert!(session.revise_expiry_on_disconnect(Some(30)).is_ok());
        assert!(session.revise_expiry_on_disconnect(Some(120)).is_ok());
        assert!(session.revise_expiry_on_disconnect(Some(0)).is_ok());
    }

    /// Clean Start 1 discards ([MQTT-3.1.2-4]), including the identifier
    /// counter — a resumed identifier space in a session the server threw away
    /// would collide with nothing and confuse everything.
    #[test]
    fn clear_discards_both_halves_of_the_state() {
        let session = session(10);
        session.allocate(message("a", QoS::ExactlyOnce)).unwrap();
        session.inbound_qos2_received(9).unwrap();
        assert!(!session.is_empty());

        session.clear();
        assert!(session.is_empty());
        assert_eq!(session.in_flight(), 0);
        assert_eq!(session.awaiting_pubrel(), 0);
        assert_eq!(
            session.allocate(message("b", QoS::AtLeastOnce)).unwrap(),
            1,
            "the identifier space restarts"
        );
    }

    /// A stored message keeps its full Topic Name and carries no Topic Alias:
    /// "a receiver MUST NOT carry mappings across connections"
    /// ([MQTT-3.3.2-7]) [mqtt5 §2], so an alias from the previous connection
    /// is meaningless.
    #[test]
    fn a_resent_publish_carries_the_topic_and_no_alias() {
        let stored = StoredPublish {
            content_type: Some("text/plain".into()),
            user_properties: vec![("k".into(), "v".into())],
            ..message("a/b", QoS::AtLeastOnce)
        };
        let pairs = stored.borrowed_user_properties();
        let publish = stored.to_publish(5, true, &pairs);
        assert_eq!(publish.topic, "a/b");
        assert_eq!(publish.properties.topic_alias, None);
        assert_eq!(publish.packet_id, Some(5));
        assert!(publish.dup, "a retransmission sets DUP 1 ([MQTT-3.3.1-1])");
        assert_eq!(publish.properties.content_type, Some("text/plain"));
        assert_eq!(
            publish.properties.user_properties().collect::<Vec<_>>(),
            [("k", "v")]
        );

        // And the same message sent for the first time has DUP 0: the flag is
        // "determined solely by whether the outgoing packet is a
        // retransmission" ([MQTT-3.3.1-3]), so it is never stored.
        let first = stored.to_publish(5, false, &pairs);
        assert!(!first.dup);
    }

    /// Cloning a session shares the state, which is what lets the connection
    /// task record acknowledgements into the session the application holds.
    #[test]
    fn a_clone_shares_the_state() {
        let session = session(10);
        let task_side = session.clone();
        let id = session.allocate(message("a", QoS::AtLeastOnce)).unwrap();
        assert!(task_side.release(id));
        assert!(session.is_empty(), "the application sees the release");
        assert_eq!(task_side.client_id(), "c");
    }
}
