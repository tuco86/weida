//! The context, the client handle, and the one task that owns the socket.
//!
//! **Three constructors, mirroring `weida-runtime`'s `Exec`.** A protocol
//! library's audience is overwhelmingly synchronous and must not have to stand
//! in a reactor to call it, which is why
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 gives
//! `weida-zmq` a context with three, and this one copies the shape:
//! [`Context::new`] borrows the ambient runtime, [`Context::with_handle`]
//! takes a handle to somebody else's, and [`Context::owned`] creates one the
//! context keeps alive.
//!
//! **One task owns the socket, and the handle is a channel.** Every ordering
//! obligation MQTT states is per connection — PUBACK in receipt order, PUBREC
//! in receipt order, PUBREL in PUBREC-receipt order ([MQTT-4.6.0-2] to
//! [MQTT-4.6.0-4]) [mqtt5 §7] — and the specification says nothing about
//! threads [mqtt5 §2]. A single reader and a single writer make those
//! obligations fall out of the structure instead of being reimposed by a
//! sorter, which is the cheapest correct answer.
//!
//! **The keep-alive timer is the client's own liveness, and both halves of it
//! are bounded.** Keep Alive "bounds the gap from finishing one client packet
//! to starting the next; absent other traffic the client MUST send PINGREQ"
//! ([MQTT-3.1.2-20]), and a server receiving nothing for 1.5 times the
//! interval MUST close ([MQTT-3.1.2-22]) [mqtt5 §1]. So the task sends PINGREQ
//! only when nothing else has been sent within the interval — a client that
//! pinged on a timer regardless would be spending the bandwidth MQTT exists to
//! save. The other half, waiting for the PINGRESP, has **no** specification
//! number at all [mqtt5 §1]; it is
//! [`crate::ConnectOptions::effective_ping_timeout`] here, because every wait
//! is bounded.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use weida_mqtt_codec::{
    self as codec, AuthReasonCode, DecodeError, Disconnect, DisconnectReasonCode, Packet,
    PacketType, PayloadList, Properties, Puback, Pubcomp, PubcompReasonCode, Publish, Pubrec,
    PubrecReasonCode, Pubrel, QoS, SubackReasonCode, UnsubackReasonCode, varint,
};
use weida_runtime::{Exec, OwnedReactor};

use crate::alias::{Aliased, InboundAliases, OutboundAliases};
use crate::connection::{Authenticator, NoAuthenticator, Reader, Writer, handshake};
use crate::error::{Error, Feature, Result};
use crate::filter::{Subscription, Subscriptions, check_topic_filter};
use crate::limits::{Limits, ServerLimits};
use crate::message::{Completion, Delivery, Message};
use crate::options::{ConnectOptions, interval_seconds};
use crate::session::{Resend, Resumption, Session, expiry_seconds};

/// Who is waiting for a SUBACK's per-filter verdicts.
type SubackWaiter = oneshot::Sender<Result<Vec<SubackReasonCode>>>;
/// Who is waiting for an UNSUBACK's.
type UnsubackWaiter = oneshot::Sender<Result<Vec<UnsubackReasonCode>>>;

/// The reactor every client of this context runs on.
///
/// Cloning is a handle clone; the reactor [`Context::owned`] created dies with
/// the last clone.
#[derive(Clone)]
pub struct Context {
    exec: Exec,
    /// Kept alive for as long as any clone of this context is, and dropped
    /// last. `Arc` because a context is cloneable and the reactor must
    /// outlive every clone.
    reactor: Option<Arc<OwnedReactor>>,
}

impl Context {
    /// A context on the ambient Tokio runtime.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] when the calling thread is not inside a runtime.
    /// Failing here — at construction — beats failing later at a connect.
    pub fn new() -> Result<Context> {
        Ok(Context {
            exec: Exec::current()?,
            reactor: None,
        })
    }

    /// A context on the runtime `handle` names, for a process whose reactor
    /// runs somewhere other than the calling thread.
    #[must_use]
    pub fn with_handle(handle: tokio::runtime::Handle) -> Context {
        Context {
            exec: Exec::from_handle(handle),
            reactor: None,
        }
    }

    /// A context that owns a reactor with `worker_threads` workers.
    ///
    /// For a caller with no reactor at all, which is most of a protocol
    /// library's audience: the futures this context hands back may be driven
    /// on any executor, `futures::executor::block_on` included.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] for `worker_threads` of 0, or [`Error::Io`] when the
    /// OS refuses the threads.
    pub fn owned(worker_threads: usize) -> Result<Context> {
        let (exec, reactor) = Exec::owned(worker_threads, "weida-mqtt")?;
        Ok(Context {
            exec,
            reactor: Some(Arc::new(reactor)),
        })
    }

    /// The executor, for a caller composing this client with something else on
    /// the same reactor.
    #[must_use]
    pub fn exec(&self) -> &Exec {
        &self.exec
    }
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("owns_reactor", &self.reactor.is_some())
            .finish()
    }
}

/// What the connection task reports to the application.
#[derive(Debug)]
#[non_exhaustive]
pub enum Event {
    /// The connection ended, with the reason. A server DISCONNECT arrives
    /// here as [`Error::ServerDisconnected`] carrying its code: a named error
    /// rather than a closed socket.
    Disconnected(Error),
    /// The server delivered a message.
    ///
    /// At QoS 1 and 2 the acknowledgement has **already been sent** when this
    /// arrives, which is what the protocol requires and not an optimization:
    /// the receiver responds "having accepted ownership of the Application
    /// Message" and "does not need to complete delivery before sending the
    /// PUBACK" ([MQTT-4.3.2-4]) [mqtt5 §6]. So this event is a delivery, not
    /// a request for one, and dropping it loses the message.
    Delivered(Delivery),
    /// An exchange that outlived the connection it started on completed.
    ///
    /// The future [`Client::publish`] returns belongs to one connection; the
    /// session
    /// belongs to the application. So an exchange resumed by
    /// [`Client::connect_session`] has no handle left to resolve, and its
    /// completion is reported here instead of being dropped on the floor.
    Completed {
        /// The original Packet Identifier.
        packet_id: u16,
        /// What the hop certified.
        completion: Completion,
    },
}

/// A command for the connection task.
enum Command {
    /// Send DISCONNECT and close.
    Disconnect {
        reason_code: DisconnectReasonCode,
        /// A revised `Session Expiry Interval`, which a client MAY set at
        /// close (3.14.2.2.2) [mqtt5 §1].
        session_expiry: Option<Duration>,
        done: oneshot::Sender<Result<()>>,
    },
    /// Send PINGREQ now, whatever the timer thinks.
    Ping { done: oneshot::Sender<Result<()>> },
    /// Publish one message.
    ///
    /// `done` resolves when the hop has certified what it is going to: at
    /// once for QoS 0, on the PUBACK for QoS 1, on the PUBCOMP — or an
    /// early PUBREC of 0x80 or above — for QoS 2.
    Publish {
        message: Box<Message>,
        done: oneshot::Sender<Result<Completion>>,
    },
    /// Send SUBSCRIBE, resolving on its SUBACK.
    Subscribe {
        subscriptions: Vec<Subscription>,
        identifier: Option<u32>,
        done: oneshot::Sender<Result<Vec<SubackReasonCode>>>,
    },
    /// Send UNSUBSCRIBE, resolving on its UNSUBACK.
    Unsubscribe {
        filters: Vec<String>,
        done: oneshot::Sender<Result<Vec<UnsubackReasonCode>>>,
    },
    /// Start re-authentication on the live connection, resolving on the
    /// server's answer (4.12.1) [mqtt5 §10].
    Reauthenticate { done: oneshot::Sender<Result<()>> },
}

/// The application's end of one connection.
#[derive(Debug)]
pub struct Client {
    commands: mpsc::Sender<Command>,
    limits: Arc<ServerLimits>,
    /// The client half of the session. A clone, so it outlives this handle:
    /// the application's copy is what makes a reconnect a *resumption*.
    session: Session,
    session_present: bool,
    resumption: Resumption,
    client_id: String,
    keep_alive: Option<Duration>,
    /// Whether the transport is encrypted, which is what the credentials in
    /// CONNECT travelled over.
    encrypted: bool,
    /// The `Authentication Method` this connection was opened with, which is
    /// the only method re-authentication may use ([MQTT-4.12.1-1]).
    authentication_method: Option<String>,
}

/// The application's end of the event stream.
#[derive(Debug)]
pub struct Events {
    events: mpsc::Receiver<Event>,
}

impl Events {
    /// The next event, or `None` once the connection task has finished and
    /// every event has been taken.
    pub async fn next(&mut self) -> Option<Event> {
        self.events.recv().await
    }
}

impl Client {
    /// Connects to `address` (`host:port`, or `[v6]:port`), sends CONNECT and
    /// awaits CONNACK, on a session of its own.
    ///
    /// For a caller that does not intend to reconnect: the session is created
    /// here and dies with the returned handle, so nothing is carried across
    /// connections. A caller that *does* intend to reconnect holds its own
    /// [`Session`] and uses [`Client::connect_session`], because the session
    /// is what outlives the connection.
    ///
    /// # Errors
    ///
    /// Everything [`crate::connection`]'s handshake reports: a configuration
    /// refusal, a transport failure, [`Error::ConnectionRefused`] with the
    /// server's code, or [`Error::Timeout`].
    pub async fn connect(
        context: &Context,
        address: &str,
        options: ConnectOptions,
    ) -> Result<(Client, Events)> {
        let session = Session::new(options.client_id.clone(), &options.limits);
        Client::connect_session(context, address, options, &session).await
    }

    /// The same, on a session the caller owns and reuses.
    ///
    /// This is the reconnect path, and it is where the three rules of
    /// 3.2.2.1.1 are applied: Clean Start 1 discards the session before
    /// connecting ([MQTT-3.1.2-4]), `Session Present` 0 with local state
    /// discards it on arrival ([MQTT-3.2.2-5]), and `Session Present` 1 with
    /// no local state refuses the connection ([MQTT-3.2.2-4]).
    ///
    /// It is also the **only** place retransmission happens: on
    /// [`Resumption::Resumed`] the unacknowledged exchanges of
    /// [`Session::resend`] go back on the wire with their original Packet
    /// Identifiers and DUP 1, in the order the originals were sent
    /// ([MQTT-4.6.0-1]), before the connection task starts.
    ///
    /// # Errors
    ///
    /// As [`Client::connect`], plus [`Error::SessionPresentWithoutState`].
    pub async fn connect_session(
        context: &Context,
        address: &str,
        options: ConnectOptions,
        session: &Session,
    ) -> Result<(Client, Events)> {
        Client::connect_full(
            context,
            address,
            options,
            session,
            Arc::new(NoAuthenticator),
        )
        .await
    }

    /// The same, answering the server's AUTH challenges through
    /// `authenticator` (4.12) [mqtt5 §10].
    ///
    /// # Errors
    ///
    /// As [`Client::connect`], plus [`Error::AuthenticationMethodMismatch`]
    /// where the server changes method mid-exchange and whatever the
    /// authenticator itself reports.
    pub async fn connect_with(
        context: &Context,
        address: &str,
        options: ConnectOptions,
        authenticator: Arc<dyn Authenticator>,
    ) -> Result<(Client, Events)> {
        let session = Session::new(options.client_id.clone(), &options.limits);
        Client::connect_full(context, address, options, &session, authenticator).await
    }

    /// The one implementation the four constructors funnel through.
    ///
    /// # Errors
    ///
    /// The union of what the other three report.
    pub async fn connect_full(
        context: &Context,
        address: &str,
        options: ConnectOptions,
        session: &Session,
        authenticator: Arc<dyn Authenticator>,
    ) -> Result<(Client, Events)> {
        options.validate()?;

        // "Clean Start 1 discards any existing Session" ([MQTT-3.1.2-4]), so
        // the client's half goes before the CONNECT rather than after the
        // CONNACK: a resumed identifier space in a session the server threw
        // away would collide with nothing and confuse everything.
        if options.clean_start {
            session.clear();
        }
        // Recorded now because the DISCONNECT rule of 3.14.2.2.2 needs to
        // know what this CONNECT declared, and after the handshake there is
        // nothing left to read it from.
        session.declare_expiry(expiry_seconds(options.session_expiry));

        let options_authentication_method = options.authentication_method.clone();
        let mut handshake =
            handshake(&context.exec, address, &options, authenticator.as_ref()).await?;

        // "The sender sets an initial send quota, non-zero and not exceeding
        // the peer's Receive Maximum" ([MQTT-4.9.0-1]): the server's number
        // replaces the client's placeholder here and nowhere else.
        session.set_send_quota(handshake.limits.receive_maximum);

        let resumption = session.accept_connack(handshake.session_present)?;
        if resumption == Resumption::Resumed {
            retransmit(&mut handshake.writer, session).await?;
        }

        let limits = Arc::new(handshake.limits);
        let (commands_tx, commands_rx) = mpsc::channel(options.limits.incoming_queue);
        let (events_tx, events_rx) = mpsc::channel(options.limits.incoming_queue);

        let task = Task {
            reader: handshake.reader,
            writer: handshake.writer,
            commands: commands_rx,
            events: events_tx,
            keep_alive: handshake.keep_alive,
            ping_timeout: options.effective_ping_timeout(),
            max_packet_size: options.limits.maximum_packet_size,
            exec: context.exec.clone(),
            session: session.clone(),
            limits: options.limits,
            waiters: HashMap::new(),
            // Two bounds, two directions: what the server said it accepts,
            // and what this client said it accepts ([MQTT-3.3.2-8]).
            outbound_aliases: OutboundAliases::new(limits.topic_alias_maximum),
            inbound_aliases: InboundAliases::new(options.limits.topic_alias_maximum),
            subscribe_waiters: HashMap::new(),
            unsubscribe_waiters: HashMap::new(),
            reauthenticating: None,
            authentication_method: options_authentication_method.clone(),
            stalled: VecDeque::new(),
            authenticator,
        };
        context.exec.spawn(task.run());

        Ok((
            Client {
                commands: commands_tx,
                limits,
                session: session.clone(),
                session_present: handshake.session_present,
                resumption,
                client_id: handshake.client_id,
                keep_alive: handshake.keep_alive,
                encrypted: handshake.encrypted,
                authentication_method: options_authentication_method,
            },
            Events { events: events_rx },
        ))
    }

    /// What the server declared in CONNACK, with §11's defaults applied to
    /// what it left out.
    #[must_use]
    pub fn server_limits(&self) -> &ServerLimits {
        &self.limits
    }

    /// `Session Present` from CONNACK (3.2.2.1.1) [mqtt5 §1].
    ///
    /// What a client must *do* about it — close when it has no state and sees
    /// 1 ([MQTT-3.2.2-4]), discard when it has state and sees 0
    /// ([MQTT-3.2.2-5]) — needs a session to compare against and is
    /// [`crate::Session::accept_connack`]'s.
    #[must_use]
    pub const fn session_present(&self) -> bool {
        self.session_present
    }

    /// The Client Identifier in force: the one that was sent, or the
    /// `Assigned Client Identifier` the server chose for a zero-length one
    /// ([MQTT-3.2.2-16]) [mqtt5 §2].
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// `Response Information` from CONNACK (3.2.2.3.15), where the server
    /// offered one.
    ///
    /// A client sets [`ConnectOptions::request_response_information`] to ask
    /// for it, and a server MAY answer with one anyway or refuse even when
    /// asked ([MQTT-3.1.2-28]) [mqtt5 §4.3]. It is "used as the basis for
    /// creating a Response Topic" and its contents are the server's to
    /// define - the specification "does not define how it is used", only that
    /// a common pattern is to use it as a topic prefix.
    #[must_use]
    pub fn response_information(&self) -> Option<&str> {
        self.limits.response_information.as_deref()
    }

    /// A Response Topic under the namespace the server offered, or `None`
    /// where it offered none.
    ///
    /// `None` is the answer and not an omission: **there is no fallback that
    /// would be honest.** Without `Response Information` a client has no
    /// protocol-level way to learn which topics it may publish replies on,
    /// and inventing one - `$response/{client_id}`, say - would be inventing
    /// a namespace the server never granted and that its authorization rules
    /// will very likely refuse. A caller in that position knows its reply
    /// topic out of band and sets [`Message::response_topic`] directly.
    ///
    /// Where the server did offer one, this joins it to `suffix` with a
    /// single `/`, which is the pattern 4.10's non-normative text describes,
    /// and checks the result is a Topic Name: the join is where a suffix
    /// containing `+` or `#` would otherwise become an unusable Response
    /// Topic that only the responder's publish would reject.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidTopic`] where the joined topic is not a Topic Name.
    pub fn response_topic(&self, suffix: &str) -> Result<Option<String>> {
        let Some(base) = self.response_information() else {
            return Ok(None);
        };
        let topic = if suffix.is_empty() {
            base.to_owned()
        } else {
            format!("{}/{suffix}", base.trim_end_matches('/'))
        };
        crate::filter::check_topic_name(&topic, false)?;
        Ok(Some(topic))
    }

    /// Whether this connection's transport is encrypted.
    ///
    /// The User Name, the Password and every byte of `Authentication Data`
    /// travel in the CONNECT, and on a plain connection they travel in the
    /// clear. "The MQTT protocol is not trust symmetrical... there is no
    /// mechanism for the Client to authenticate the Server" with basic
    /// authentication (5.4.3) [mqtt5 §10], so this is the only question a
    /// client can ask about what protected them.
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        self.encrypted
    }

    /// Re-authenticates on the live connection, resolving when the server
    /// accepts or refuses.
    ///
    /// "A Client that has named an Authentication Method MAY send AUTH 0x19
    /// at any time after CONNACK using the same method" ([MQTT-4.12.1-1])
    /// (4.12.1) [mqtt5 §10], which is the protocol's answer to credential
    /// rotation on a long-lived connection - 5.4.10 also suggests periodic
    /// forced re-authentication.
    ///
    /// **Other traffic continues during the exchange.** The specification is
    /// explicit that the previous authentication stays in force while it runs,
    /// so this does not quiesce the connection: publishes and deliveries flow
    /// through the same event loop, and only the re-authentication's own
    /// outcome waits here. On failure "both sides SHOULD send DISCONNECT and
    /// MUST close" ([MQTT-4.12.1-2]), which arrives as
    /// [`Error::ServerDisconnected`] on the event stream.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] where this client named no
    /// `Authentication Method` - there is nothing to re-authenticate with, and
    /// a server "MUST NOT send AUTH" to such a client ([MQTT-4.12.0-6]);
    /// [`Error::AuthenticationMethodMismatch`] where the server answers with
    /// a different method; and [`Error::NotConnected`] where the connection
    /// ended.
    pub async fn reauthenticate(&self) -> Result<()> {
        if self.authentication_method.is_none() {
            return Err(Error::Configuration(
                "re-authentication needs the Authentication Method this connection was \
                 opened with; a client that named none has nothing to re-authenticate \
                 ([MQTT-4.12.1-1])"
                    .into(),
            ));
        }
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Command::Reauthenticate { done })
            .await
            .map_err(|_| Error::NotConnected)?;
        wait.await.map_err(|_| Error::NotConnected)?
    }

    /// The Keep Alive in force: the client's, or `Server Keep Alive` where
    /// the server sent one ([MQTT-3.2.2-21]). `None` means the mechanism is
    /// disabled and the server will not time this connection out.
    #[must_use]
    pub const fn keep_alive(&self) -> Option<Duration> {
        self.keep_alive
    }

    /// The session this connection runs on. Cloning it is how an application
    /// keeps it across a reconnect.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// What `Session Present` obliged this client to do: resume, start
    /// fresh, or discard what it held (3.2.2.1.1) [mqtt5 §1].
    ///
    /// The fourth case — `Session Present` 1 with no local state — is not a
    /// value here because it is [`Error::SessionPresentWithoutState`]:
    /// [MQTT-3.2.2-4] obliges the client to close, so there is no connection
    /// to report it on.
    #[must_use]
    pub const fn resumption(&self) -> Resumption {
        self.resumption
    }

    /// Sends DISCONNECT and closes.
    ///
    /// `DisconnectReasonCode::NormalDisconnection` makes the server discard
    /// the Will without publishing it ([MQTT-3.14.4-3]);
    /// `DisconnectWithWillMessage` asks for the Will anyway [mqtt5 §1].
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`] where the task has already finished, and
    /// whatever the write reported.
    pub async fn disconnect(&self, reason_code: DisconnectReasonCode) -> Result<()> {
        self.disconnect_with(reason_code, None).await
    }

    /// The same, revising the `Session Expiry Interval` at close, which a
    /// client MAY do — "so a session's lifetime can be shortened or extended
    /// at close" (3.14.2.2.2) [mqtt5 §1].
    ///
    /// **The rule the codec deferred is enforced here, before sending**: "a
    /// non-zero value when CONNECT carried zero is a Protocol Error"
    /// (3.14.2.2.2) [mqtt5 §1], which needs the CONNECT that came before and
    /// therefore needs the session. Shortening to zero is always permitted,
    /// and is what the specification advises a client that is finished to do
    /// so that a session it will never return to is not orphaned
    /// (3.1.2.11.2) [mqtt5 §9].
    ///
    /// # Errors
    ///
    /// As [`Client::disconnect`], plus [`Error::Configuration`] for an
    /// interval a Four Byte Integer cannot carry or for the zero-then-non-zero
    /// Protocol Error.
    pub async fn disconnect_with(
        &self,
        reason_code: DisconnectReasonCode,
        session_expiry: Option<Duration>,
    ) -> Result<()> {
        interval_seconds("session_expiry", session_expiry)?;
        self.session
            .revise_expiry_on_disconnect(expiry_seconds(session_expiry))?;
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Command::Disconnect {
                reason_code,
                session_expiry,
                done,
            })
            .await
            .map_err(|_| Error::NotConnected)?;
        wait.await.map_err(|_| Error::NotConnected)?
    }

    /// Sends a PINGREQ now.
    ///
    /// The timer sends one by itself when nothing else has been sent within
    /// the interval; this is for a caller that wants to probe liveness on its
    /// own schedule.
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`], or whatever the write reported.
    pub async fn ping(&self) -> Result<()> {
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Command::Ping { done })
            .await
            .map_err(|_| Error::NotConnected)?;
        wait.await.map_err(|_| Error::NotConnected)?
    }

    /// Publishes `message`, resolving when the hop has certified what it is
    /// going to.
    ///
    /// QoS 0 resolves at once with [`Completion::Sent`]; QoS 1 on the PUBACK;
    /// QoS 2 on the PUBCOMP, or early with [`Completion::Refused`] on a
    /// PUBREC of 0x80 or above. See [`Completion`] for what each of those
    /// does and does not certify — none of them certifies durability, and
    /// none reaches past this hop.
    ///
    /// **The send quota stalls this call rather than being exceeded.** The
    /// quota is the server's `Receive Maximum` counted in QoS 1 and 2 PUBLISH
    /// packets and nothing else ([MQTT-4.9.0-1], 4.9) [mqtt5 §5]; with it
    /// spent, the message waits here until an exchange completes. QoS 0 is
    /// never counted and never waits, which is the same asymmetry that leaves
    /// QoS 0 with no flow control at all.
    ///
    /// # Errors
    ///
    /// [`Error::Unavailable`] where the server declined the QoS or RETAIN,
    /// refused before the packet reaches the wire; [`Error::NotConnected`]
    /// where the connection ended; and whatever the write or the encode
    /// reported.
    pub async fn publish(&self, message: Message) -> Result<Completion> {
        message.check(&self.limits)?;
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Command::Publish {
                message: Box::new(message),
                done,
            })
            .await
            .map_err(|_| Error::NotConnected)?;
        wait.await.map_err(|_| Error::NotConnected)?
    }

    /// Subscribes to `subscriptions`, resolving on the SUBACK.
    ///
    /// **One SUBACK reason code per filter, in the order the filters were
    /// sent** ([MQTT-3.9.3-1], [MQTT-3.9.3-2]) (3.9.3) [mqtt5 §8], and the
    /// returned vector is exactly that list: a failure for one filter leaves
    /// the others granted, so the call as a whole does **not** fail when one
    /// filter is refused. A granted maximum QoS may be lower than what was
    /// asked for — "the minimum of the QoS of the originally published
    /// message and the Maximum QoS granted" ([MQTT-3.8.4-8]) [mqtt5 §6] — and
    /// each code is recorded against its filter in the session's mirror
    /// ([`Session::subscriptions`]), where a refused one is not recorded at
    /// all.
    ///
    /// **Re-subscribing a filter replaces the subscription without losing
    /// messages** ([MQTT-3.8.4-3]): the server performs the replacement and
    /// "any existing retained messages matching the filter are sent again
    /// unless Retain Handling says otherwise" ([MQTT-3.8.4-4]) (3.8.4)
    /// [mqtt5 §2]. So changing one filter's options is one SUBSCRIBE and not
    /// an UNSUBSCRIBE followed by a SUBSCRIBE, which would open a window in
    /// which messages are lost.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for an empty list — "a SUBSCRIBE with no
    /// payload entry is a Protocol Error" ([MQTT-3.8.3-2]);
    /// [`Error::InvalidTopic`] for a filter that breaks the grammar of 4.7.1;
    /// [`Error::Unavailable`] where the server declined wildcards, shared
    /// subscriptions or subscription identifiers; and
    /// [`Error::NotConnected`] where the connection ended.
    pub async fn subscribe(
        &self,
        subscriptions: Vec<Subscription>,
    ) -> Result<Vec<SubackReasonCode>> {
        self.subscribe_with(subscriptions, None).await
    }

    /// The same, with a `Subscription Identifier` the server reports back "on
    /// every delivery it caused" ([MQTT-3.3.4-4]) [mqtt5 §4.1].
    ///
    /// The identifier is one per SUBSCRIBE packet, not per filter (3.8.2.1.2)
    /// [mqtt5 §4.1], so every filter in this call shares it. It ranges from 1
    /// to 268,435,455 — a Variable Byte Integer, and 0 "is a Protocol Error"
    /// ([MQTT-3.8.3-4]).
    ///
    /// **A server may decline the whole feature** with `Subscription
    /// Identifiers Available` 0 (3.2.2.3.12) [mqtt5 §11], which is refused
    /// here with 0xA1 rather than being sent and rejected. Against such a
    /// server the way to tell which filter matched a delivery is
    /// [`Subscriptions::matching`], which is why that exists.
    ///
    /// # Errors
    ///
    /// As [`Client::subscribe`], plus [`Error::Configuration`] for an
    /// identifier of 0 or above 268,435,455.
    pub async fn subscribe_with(
        &self,
        subscriptions: Vec<Subscription>,
        identifier: Option<u32>,
    ) -> Result<Vec<SubackReasonCode>> {
        if subscriptions.is_empty() {
            return Err(Error::Configuration(
                "a SUBSCRIBE must carry at least one filter ([MQTT-3.8.3-2])".into(),
            ));
        }
        for subscription in &subscriptions {
            subscription.check(&self.limits)?;
        }
        if let Some(identifier) = identifier {
            self.limits.require(Feature::SubscriptionIdentifier)?;
            if identifier == 0 || identifier > varint::MAX {
                return Err(Error::Configuration(format!(
                    "a Subscription Identifier is 1 to 268,435,455; {identifier} is not \
                     ([MQTT-3.8.3-4])"
                )));
            }
        }
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Command::Subscribe {
                subscriptions,
                identifier,
                done,
            })
            .await
            .map_err(|_| Error::NotConnected)?;
        wait.await.map_err(|_| Error::NotConnected)?
    }

    /// Unsubscribes from `filters`, resolving on the UNSUBACK.
    ///
    /// One reason code per filter again ([MQTT-3.11.3-1]) (3.11.3), and
    /// [`UnsubackReasonCode::NoSubscriptionExisted`] is a **success**: the end
    /// state the client asked for holds either way, and 3.1.1's UNSUBACK
    /// carried no status at all [mqtt5 §1.9]. Each filter reported success is
    /// dropped from the session's mirror.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for an empty list — "an UNSUBSCRIBE with no
    /// payload entry is a Protocol Error" ([MQTT-3.10.3-2]);
    /// [`Error::InvalidTopic`] for a filter that breaks the grammar; and
    /// [`Error::NotConnected`] where the connection ended.
    pub async fn unsubscribe(&self, filters: Vec<String>) -> Result<Vec<UnsubackReasonCode>> {
        if filters.is_empty() {
            return Err(Error::Configuration(
                "an UNSUBSCRIBE must carry at least one filter ([MQTT-3.10.3-2])".into(),
            ));
        }
        for filter in &filters {
            check_topic_filter(filter)?;
        }
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Command::Unsubscribe { filters, done })
            .await
            .map_err(|_| Error::NotConnected)?;
        wait.await.map_err(|_| Error::NotConnected)?
    }

    /// This client's own view of what it is subscribed to.
    ///
    /// A mirror of the server's state and not an authority; see
    /// [`Session::subscriptions`].
    #[must_use]
    pub fn subscriptions(&self) -> Subscriptions {
        self.session.subscriptions()
    }
}

/// The task that owns the socket.
struct Task {
    reader: Reader,
    writer: Writer,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
    /// The aliases this client hands out, bounded by the server's `Topic
    /// Alias Maximum`. Per connection: this field's lifetime *is* the
    /// mapping's lifetime ([MQTT-3.3.2-7]).
    outbound_aliases: OutboundAliases,
    /// The aliases the server establishes toward this client, bounded by the
    /// `Topic Alias Maximum` this client declared.
    inbound_aliases: InboundAliases,
    keep_alive: Option<Duration>,
    ping_timeout: Option<Duration>,
    max_packet_size: u32,
    /// Who is waiting for each SUBACK, with the filters the SUBSCRIBE
    /// carried: the codes are matched to filters by position (3.9.3), so the
    /// list has to be kept to read the answer.
    subscribe_waiters: HashMap<u16, (Vec<Subscription>, Option<u32>, SubackWaiter)>,
    /// The same for UNSUBACK.
    unsubscribe_waiters: HashMap<u16, (Vec<String>, UnsubackWaiter)>,
    exec: Exec,
    /// The session, shared with the application's handle.
    session: Session,
    /// This client's own declared limits, for reading a delivery under the
    /// Subscription Identifier ceiling the protocol does not provide.
    limits: Limits,
    /// Who is waiting for each in-flight exchange to complete, keyed by
    /// Packet Identifier. An exchange resumed from a previous connection has
    /// no entry, and its completion becomes [`Event::Completed`].
    waiters: HashMap<u16, oneshot::Sender<Result<Completion>>>,
    /// Publishes that arrived with the send quota already spent.
    ///
    /// "At zero the sender MUST NOT send further QoS > 0 PUBLISH packets"
    /// ([MQTT-4.9.0-2]) and exhaustion "stalls the sender rather than
    /// exceeding it" [mqtt5 §5]. This queue **is** that stall: the caller's
    /// future is still pending, the packet is not on the wire, and the moment
    /// an identifier frees the oldest waiting publish takes it. Bounded by
    /// the command channel, which is `Limits::incoming_queue` deep.
    stalled: VecDeque<(Box<Message>, oneshot::Sender<Result<Completion>>)>,
    /// How this connection answers an AUTH challenge, kept because
    /// re-authentication happens on the **live** connection and not in the
    /// handshake (4.12.1) [mqtt5 §10].
    authenticator: Arc<dyn Authenticator>,
    /// Who is waiting for a re-authentication to finish.
    ///
    /// `None` means none is in progress, which is also what makes an
    /// unrequested AUTH detectable: after CONNACK a server may only send AUTH
    /// inside an exchange the client started ([MQTT-4.12.1-1]).
    reauthenticating: Option<oneshot::Sender<Result<()>>>,
    /// The method every AUTH of this connection repeats ([MQTT-4.12.0-5]).
    authentication_method: Option<String>,
}

impl Task {
    async fn run(mut self) {
        let outcome = self.drive().await;
        let error = match outcome {
            Ok(()) => Error::NotConnected,
            Err(error) => error,
        };
        // A send failure here means the application dropped its `Events`,
        // which is its right: the connection is over either way.
        let _ = self.events.send(Event::Disconnected(error)).await;
    }

    /// Reads, answers and pings until something ends the connection.
    async fn drive(&mut self) -> Result<()> {
        // Whether a PINGREQ is outstanding, and therefore whether the
        // unquantified PINGRESP deadline is running.
        let mut awaiting_pingresp = false;
        // Keep Alive "bounds the gap from finishing one Client packet to
        // starting the next" ([MQTT-3.1.2-20]) [mqtt5 §1], so the interval is
        // measured from the last **write** and not from the last loop turn.
        // Measuring it per turn would let a chatty server keep resetting a
        // silent client's timer until the server itself closed the connection
        // at 1.5x — the exact failure the mechanism exists to prevent.
        let mut last_write = std::time::Instant::now();

        loop {
            // The idle interval: what is left of Keep Alive while nothing is
            // outstanding, the PINGRESP deadline once one is. Both may be
            // absent, in which case nothing is timed and the loop waits on
            // traffic alone.
            let idle = if awaiting_pingresp {
                self.ping_timeout
            } else {
                self.keep_alive
                    .map(|interval| interval.saturating_sub(last_write.elapsed()))
            };

            let step = {
                let read = self.reader.next();
                let command = self.commands.recv();
                match idle {
                    Some(idle) => {
                        let timer = self.exec.sleep(idle);
                        tokio::select! {
                            packet = read => Step::Packet(packet.map(<[u8]>::to_vec)),
                            command = command => Step::Command(command),
                            () = timer => Step::Idle,
                        }
                    }
                    None => tokio::select! {
                        packet = read => Step::Packet(packet.map(<[u8]>::to_vec)),
                        command = command => Step::Command(command),
                    },
                }
            };

            match step {
                Step::Packet(bytes) => {
                    let bytes = bytes?;
                    let (packet, _) = Packet::decode(&bytes, self.max_packet_size)?;
                    match packet {
                        Packet::Pingresp => awaiting_pingresp = false,
                        Packet::Disconnect(disconnect) => {
                            // The whole point of 5.0's server-to-client
                            // DISCONNECT: the code reaches the application.
                            return Err(Error::ServerDisconnected(disconnect.reason_code));
                        }
                        Packet::Pingreq
                        | Packet::Connect(_)
                        | Packet::Subscribe(_)
                        | Packet::Unsubscribe(_) => {
                            // Client-to-server only (2.1.2) [mqtt5 §12/P11].
                            return Err(Error::UnexpectedPacket {
                                packet_type: packet.packet_type(),
                            });
                        }
                        Packet::Connack(_) => {
                            // "The Server MUST NOT send more than one CONNACK"
                            // ([MQTT-3.2.0-1]); a second one is a violation.
                            return Err(Error::UnexpectedPacket {
                                packet_type: PacketType::Connack,
                            });
                        }
                        Packet::Publish(publish) => {
                            // Only a QoS 1 or 2 PUBLISH answers with a PUBACK
                            // or PUBREC; a QoS 0 one writes nothing, so it
                            // must not reset the Keep Alive clock. Resetting
                            // it there would let a stream of QoS 0
                            // publications keep a silent client from ever
                            // reaching the idle step, so no PINGREQ would go
                            // out and the server would close the connection
                            // at 1.5x Keep Alive ([MQTT-3.1.2-22]).
                            if self.receive(&publish).await? {
                                last_write = std::time::Instant::now();
                            }
                        }
                        Packet::Puback(puback) => {
                            self.settle(
                                puback.packet_id,
                                Completion::Acknowledged(puback.reason_code),
                            )
                            .await;
                        }
                        Packet::Pubrec(pubrec) => {
                            self.pubrec(pubrec.packet_id, pubrec.reason_code).await?;
                            last_write = std::time::Instant::now();
                        }
                        Packet::Pubcomp(pubcomp) => {
                            self.settle(
                                pubcomp.packet_id,
                                Completion::Complete(pubcomp.reason_code),
                            )
                            .await;
                        }
                        Packet::Pubrel(pubrel) => {
                            self.pubrel(pubrel.packet_id).await?;
                            last_write = std::time::Instant::now();
                        }
                        Packet::Suback(suback) => self.suback(&suback),
                        Packet::Unsuback(unsuback) => self.unsuback(&unsuback),
                        Packet::Auth(auth) => {
                            if self.auth(&auth).await? {
                                last_write = std::time::Instant::now();
                            }
                        }
                    }

                    // Something may have freed an identifier, so the oldest
                    // stalled publish can go.
                    if self.drain_stalled().await? {
                        last_write = std::time::Instant::now();
                    }
                }
                Step::Command(None) => {
                    // Every `Client` handle is gone and nobody asked for a
                    // DISCONNECT. Closing without one is what triggers the
                    // Will [mqtt5 §4.5], and inventing an orderly close on the
                    // application's behalf would suppress it.
                    return Ok(());
                }
                Step::Command(Some(Command::Publish { message, done })) => {
                    self.publish(message, done).await?;
                    last_write = std::time::Instant::now();
                }
                Step::Command(Some(Command::Ping { done })) => {
                    let sent = self.writer.send(&Packet::Pingreq).await;
                    let failed = sent.is_err();
                    let _ = done.send(sent);
                    if failed {
                        return Err(Error::ConnectionClosed);
                    }
                    last_write = std::time::Instant::now();
                    awaiting_pingresp = true;
                }
                Step::Command(Some(Command::Subscribe {
                    subscriptions,
                    identifier,
                    done,
                })) => {
                    self.send_subscribe(subscriptions, identifier, done).await?;
                    last_write = std::time::Instant::now();
                }
                Step::Command(Some(Command::Unsubscribe { filters, done })) => {
                    self.send_unsubscribe(filters, done).await?;
                    last_write = std::time::Instant::now();
                }
                Step::Command(Some(Command::Reauthenticate { done })) => {
                    self.send_reauthenticate(done).await?;
                    last_write = std::time::Instant::now();
                }
                Step::Command(Some(Command::Disconnect {
                    reason_code,
                    session_expiry,
                    done,
                })) => {
                    let sent = self.send_disconnect(reason_code, session_expiry).await;
                    let _ = done.send(sent);
                    // "After sending DISCONNECT the sender MUST send nothing
                    // more and MUST close" ([MQTT-3.14.4-1],
                    // [MQTT-3.14.4-2]) [mqtt5 §12/P17].
                    return Ok(());
                }
                Step::Idle if awaiting_pingresp => {
                    // The number the specification declines to give, spent.
                    return Err(Error::Timeout("PINGRESP"));
                }
                Step::Idle => {
                    // "Absent other traffic the Client MUST send a PINGREQ"
                    // ([MQTT-3.1.2-20]). The timer runs from the last write,
                    // so reaching here *is* "absent other traffic".
                    self.writer.send(&Packet::Pingreq).await?;
                    last_write = std::time::Instant::now();
                    awaiting_pingresp = true;
                }
            }
        }
    }

    /// Sends SUBSCRIBE and remembers who is waiting for its SUBACK.
    ///
    /// The identifier comes from the session's **one** space, shared with
    /// PUBLISH ([MQTT-2.2.1-3]), and is released when the SUBACK arrives.
    /// Nothing is stored for retransmission: "SUBSCRIBE and UNSUBSCRIBE are
    /// not in the retransmission list at all" (4.4) [mqtt5 §6].
    async fn send_subscribe(
        &mut self,
        subscriptions: Vec<Subscription>,
        identifier: Option<u32>,
        done: oneshot::Sender<Result<Vec<SubackReasonCode>>>,
    ) -> Result<()> {
        let packet_id = match self.session.allocate_control() {
            Ok(packet_id) => packet_id,
            Err(error) => {
                let _ = done.send(Err(error));
                return Ok(());
            }
        };
        let filters: Vec<codec::Subscription<'_>> = subscriptions
            .iter()
            .map(|subscription| codec::Subscription {
                filter: &subscription.filter,
                options: subscription.options,
            })
            .collect();
        // The property is repeatable on the wire, but SUBSCRIBE carries "at
        // most one" (3.8.2.1.2) [mqtt5 §4.1], so the slice is zero or one
        // long and its bound is the option's.
        let ids: Vec<u32> = identifier.into_iter().collect();
        let packet = Packet::Subscribe(codec::Subscribe {
            packet_id,
            properties: Properties::new().with_subscription_identifiers(&ids),
            filters: PayloadList::new(&filters),
        });
        if let Err(error) = self.writer.send(&packet).await {
            self.session.release_control(packet_id);
            let _ = done.send(Err(error));
            return Err(Error::ConnectionClosed);
        }
        self.subscribe_waiters
            .insert(packet_id, (subscriptions, identifier, done));
        Ok(())
    }

    /// Sends UNSUBSCRIBE and remembers who is waiting for its UNSUBACK.
    async fn send_unsubscribe(
        &mut self,
        filters: Vec<String>,
        done: oneshot::Sender<Result<Vec<UnsubackReasonCode>>>,
    ) -> Result<()> {
        let packet_id = match self.session.allocate_control() {
            Ok(packet_id) => packet_id,
            Err(error) => {
                let _ = done.send(Err(error));
                return Ok(());
            }
        };
        let borrowed: Vec<&str> = filters.iter().map(String::as_str).collect();
        let packet = Packet::Unsubscribe(codec::Unsubscribe {
            packet_id,
            properties: Properties::new(),
            filters: PayloadList::new(&borrowed),
        });
        if let Err(error) = self.writer.send(&packet).await {
            self.session.release_control(packet_id);
            let _ = done.send(Err(error));
            return Err(Error::ConnectionClosed);
        }
        self.unsubscribe_waiters.insert(packet_id, (filters, done));
        Ok(())
    }

    /// Resolves a SUBSCRIBE with its per-filter verdicts, recording the
    /// granted ones in the session's mirror.
    fn suback(&mut self, suback: &codec::Suback<'_>) {
        self.session.release_control(suback.packet_id);
        let Some((subscriptions, identifier, done)) =
            self.subscribe_waiters.remove(&suback.packet_id)
        else {
            // A SUBACK for an identifier this client never sent. Dropped
            // rather than fatal: the identifier is free either way, and
            // closing the connection over a stray acknowledgement would lose
            // every other subscription with it.
            return;
        };
        let codes: Vec<SubackReasonCode> = suback.reason_codes.iter().collect();
        if codes.len() != subscriptions.len() {
            // "The SUBACK MUST contain one reason code for each filter, in
            // the same order" ([MQTT-3.9.3-1], [MQTT-3.9.3-2]). A different
            // count leaves no way to say which filter each code is about, so
            // it is reported rather than guessed at.
            let _ = done.send(Err(Error::AcknowledgementLengthMismatch {
                packet_type: PacketType::Suback,
                sent: subscriptions.len(),
                received: codes.len(),
            }));
            return;
        }
        for (subscription, granted) in subscriptions.into_iter().zip(codes.iter().copied()) {
            self.session
                .record_subscription(subscription, identifier, granted);
        }
        let _ = done.send(Ok(codes));
    }

    /// Resolves an UNSUBSCRIBE, forgetting every filter it reported gone.
    fn unsuback(&mut self, unsuback: &codec::Unsuback<'_>) {
        self.session.release_control(unsuback.packet_id);
        let Some((filters, done)) = self.unsubscribe_waiters.remove(&unsuback.packet_id) else {
            return;
        };
        let codes: Vec<UnsubackReasonCode> = unsuback.reason_codes.iter().collect();
        if codes.len() != filters.len() {
            let _ = done.send(Err(Error::AcknowledgementLengthMismatch {
                packet_type: PacketType::Unsuback,
                sent: filters.len(),
                received: codes.len(),
            }));
            return;
        }
        for (filter, code) in filters.iter().zip(codes.iter()) {
            // `NoSubscriptionExisted` is a success: the end state the caller
            // asked for holds [mqtt5 §1.9].
            if !code.is_error() {
                self.session.forget_subscription(filter);
            }
        }
        let _ = done.send(Ok(codes));
    }

    /// Starts re-authentication: AUTH 0x19 with the CONNECT's method and the
    /// authenticator's opening data ([MQTT-4.12.1-1]).
    ///
    /// The authenticator is asked with `None` for the opening move, which is
    /// the same shape as a challenge with no `Authentication Data`: the server
    /// has said nothing yet, so there is nothing to answer.
    async fn send_reauthenticate(&mut self, done: oneshot::Sender<Result<()>>) -> Result<()> {
        if let Some(previous) = self.reauthenticating.take() {
            // A second re-authentication while one is running would make two
            // exchanges indistinguishable on the wire - the AUTH packets carry
            // no identifier - so the older waiter is told rather than left
            // pending forever.
            let _ = previous.send(Err(Error::UnexpectedPacket {
                packet_type: PacketType::Auth,
            }));
        }
        let data = match self.authenticator.challenge(None) {
            Ok(data) => data,
            Err(error) => {
                let _ = done.send(Err(error));
                return Ok(());
            }
        };
        let packet = Packet::Auth(codec::Auth {
            reason_code: AuthReasonCode::ReAuthenticate,
            properties: Properties {
                authentication_method: self.authentication_method.as_deref(),
                authentication_data: Some(&data),
                ..Properties::new()
            },
        });
        if let Err(error) = self.writer.send(&packet).await {
            let _ = done.send(Err(error));
            return Err(Error::ConnectionClosed);
        }
        self.reauthenticating = Some(done);
        Ok(())
    }

    /// Answers a server AUTH during re-authentication.
    ///
    /// Returns whether anything was written, so the caller can reset the
    /// keep-alive timer.
    async fn auth(&mut self, auth: &codec::Auth<'_>) -> Result<bool> {
        // "Absent a client-named method the Server MUST NOT send AUTH"
        // ([MQTT-4.12.0-6]), and after CONNACK it may only send one inside an
        // exchange this client started ([MQTT-4.12.1-1]). Either way an AUTH
        // arriving outside one is the server breaking the protocol, and it is
        // checked before the method so an unrequested AUTH is not misreported
        // as a mismatch with a method that was never in play.
        if self.reauthenticating.is_none() {
            return Err(Error::UnexpectedPacket {
                packet_type: PacketType::Auth,
            });
        }
        // [MQTT-4.12.0-5]: every AUTH of an exchange repeats the same method.
        // A server that omits it is tolerated on the packet that *ends* the
        // exchange, as in the handshake; naming a different one never is.
        if let Some(theirs) = auth.properties.authentication_method
            && Some(theirs) != self.authentication_method.as_deref()
        {
            let done = self.reauthenticating.take().expect("checked above");
            let _ = done.send(Err(Error::AuthenticationMethodMismatch));
            return Err(Error::AuthenticationMethodMismatch);
        }
        match auth.reason_code {
            AuthReasonCode::Success => {
                // The exchange is over and the new credentials are in force.
                let done = self.reauthenticating.take().expect("checked above");
                let _ = done.send(Ok(()));
                Ok(false)
            }
            AuthReasonCode::ContinueAuthentication => {
                let data = match self
                    .authenticator
                    .challenge(auth.properties.authentication_data)
                {
                    Ok(data) => data,
                    Err(error) => {
                        let done = self.reauthenticating.take().expect("checked above");
                        // "On failure both sides SHOULD send DISCONNECT and
                        // MUST close" ([MQTT-4.12.1-2]). Returning ends the
                        // connection, which is the MUST; the SHOULD is not
                        // attempted, because a client that cannot answer a
                        // challenge has nothing to say about it. The cause
                        // goes to the caller who asked for the
                        // re-authentication, and the event stream learns why
                        // the connection ended.
                        let reported =
                            Error::Configuration(format!("re-authentication failed: {error}"));
                        let _ = done.send(Err(error));
                        return Err(reported);
                    }
                };
                let packet = Packet::Auth(codec::Auth {
                    reason_code: AuthReasonCode::ContinueAuthentication,
                    properties: Properties {
                        authentication_method: self.authentication_method.as_deref(),
                        authentication_data: Some(&data),
                        ..Properties::new()
                    },
                });
                self.writer.send(&packet).await?;
                Ok(true)
            }
            // 0x19 is the client's own opening move; a server that sends it is
            // asking the client to re-authenticate, which the protocol does
            // not provide for ([MQTT-4.12.1-1] is one-directional).
            AuthReasonCode::ReAuthenticate => Err(Error::UnexpectedPacket {
                packet_type: PacketType::Auth,
            }),
        }
    }

    async fn send_disconnect(
        &mut self,
        reason_code: DisconnectReasonCode,
        session_expiry: Option<Duration>,
    ) -> Result<()> {
        let properties = Properties {
            session_expiry_interval: interval_seconds("session_expiry", session_expiry)?,
            ..Properties::new()
        };
        self.writer
            .send(&Packet::Disconnect(Disconnect {
                reason_code,
                properties,
            }))
            .await
    }

    /// Sends one message, or stalls it if the quota is spent.
    ///
    /// QoS 0 goes straight out and resolves at once: it is "not any other
    /// packet type" in the quota's unit, so it is never counted and never
    /// waits [mqtt5 §5].
    async fn publish(
        &mut self,
        message: Box<Message>,
        done: oneshot::Sender<Result<Completion>>,
    ) -> Result<()> {
        if message.qos == QoS::AtMostOnce {
            let pairs = borrowed(&message.user_properties);
            let mut publish = qos0_publish(&message, &pairs)?;
            alias(&mut self.outbound_aliases, &mut publish);
            let sent = self.writer.send(&Packet::Publish(publish)).await;
            let failed = sent.is_err();
            let _ = done.send(sent.map(|()| Completion::Sent));
            return if failed {
                Err(Error::ConnectionClosed)
            } else {
                Ok(())
            };
        }

        let stored = match message.as_ref().clone().into_stored() {
            Ok(stored) => stored,
            Err(error) => {
                let _ = done.send(Err(error));
                return Ok(());
            }
        };
        match self.session.allocate(stored.clone()) {
            Ok(packet_id) => {
                let pairs = stored.borrowed_user_properties();
                // First attempt, so DUP is 0 ([MQTT-4.3.2-2],
                // [MQTT-4.3.3-2]).
                let mut publish = stored.to_publish(packet_id, false, &pairs);
                // The wire copy may be aliased; the **stored** copy keeps its
                // full Topic Name, because a mapping "MUST NOT be carried
                // across Network Connections" ([MQTT-3.3.2-7]) and a
                // retransmission happens on the next one.
                alias(&mut self.outbound_aliases, &mut publish);
                if let Err(error) = self.writer.send(&Packet::Publish(publish)).await {
                    self.session.release(packet_id);
                    let _ = done.send(Err(error));
                    return Err(Error::ConnectionClosed);
                }
                self.waiters.insert(packet_id, done);
                Ok(())
            }
            Err(Error::QuotaExhausted { .. }) => {
                // The stall of [MQTT-4.9.0-2]: the caller's future stays
                // pending and the packet stays off the wire.
                self.stalled.push_back((message, done));
                Ok(())
            }
            Err(error) => {
                let _ = done.send(Err(error));
                Ok(())
            }
        }
    }

    /// Sends as many stalled publishes as the quota now admits.
    ///
    /// Returns whether anything went out, so the caller can reset the
    /// keep-alive timer.
    async fn drain_stalled(&mut self) -> Result<bool> {
        let mut sent_any = false;
        while let Some((message, done)) = self.stalled.pop_front() {
            let before = self.stalled.len();
            self.publish(message, done).await?;
            if self.stalled.len() > before {
                // It stalled again: the quota is spent, so stop.
                break;
            }
            sent_any = true;
        }
        Ok(sent_any)
    }

    /// Resolves an exchange and releases its identifier.
    ///
    /// An identifier frees "on PUBACK, PUBCOMP, a PUBREC with code >= 0x80,
    /// or SUBACK/UNSUBACK" [mqtt5 §2]. An acknowledgement for an identifier
    /// this client is not holding is not an error: it is the recovery case
    /// 0x92 describes, which "is not an error during recovery, but at other
    /// times indicates a mismatch" (3.6.2.1) [mqtt5 §6], and a client cannot
    /// tell the two apart.
    async fn settle(&mut self, packet_id: u16, completion: Completion) {
        self.session.release(packet_id);
        match self.waiters.remove(&packet_id) {
            Some(done) => {
                let _ = done.send(Ok(completion));
            }
            // No waiter: the exchange was resumed from a previous connection,
            // so its completion goes to the events rather than nowhere.
            None => {
                let _ = self
                    .events
                    .send(Event::Completed {
                        packet_id,
                        completion,
                    })
                    .await;
            }
        }
    }

    /// QoS 2, part two: answer a PUBREC.
    ///
    /// "On a PUBREC with code < 0x80 send PUBREL with the same identifier"
    /// ([MQTT-4.3.3-4]); at 0x80 or above the message "counts as acknowledged
    /// and MUST NOT be retransmitted" ([MQTT-4.4.0-2]) and the identifier
    /// frees [mqtt5 §6]. Those are two different outcomes, and conflating
    /// them would either leak an identifier or resend a dead message.
    async fn pubrec(&mut self, packet_id: u16, reason_code: PubrecReasonCode) -> Result<()> {
        if reason_code.is_error() {
            self.settle(packet_id, Completion::Refused(reason_code))
                .await;
            return Ok(());
        }
        // False means this client was not awaiting a PUBREC for it — a
        // duplicate PUBREC after the PUBREL went out, which the specification
        // expects during recovery. The PUBREL is sent again either way,
        // because the server is still waiting for one.
        self.session.pubrec_received(packet_id);
        self.writer
            .send(&Packet::Pubrel(Pubrel::new(packet_id)))
            .await
    }

    /// QoS 2 inbound, part three: answer a PUBREL with a PUBCOMP.
    ///
    /// "Respond to PUBREL with PUBCOMP" ([MQTT-4.3.3-11]) and "after PUBCOMP
    /// treat the identifier as free and a later PUBLISH with it as new"
    /// ([MQTT-4.3.3-12]) [mqtt5 §6]. A PUBREL for an identifier this client
    /// does not hold is answered 0x92, which the specification explicitly
    /// declines to call an error during recovery (3.6.2.1).
    async fn pubrel(&mut self, packet_id: u16) -> Result<()> {
        let held = self.session.inbound_qos2_released(packet_id);
        let reason_code = if held {
            PubcompReasonCode::Success
        } else {
            PubcompReasonCode::PacketIdentifierNotFound
        };
        self.writer
            .send(&Packet::Pubcomp(Pubcomp {
                packet_id,
                reason_code,
                properties: Properties::new(),
            }))
            .await
    }

    /// A received PUBLISH: deliver it, and acknowledge it at QoS 1 and 2.
    ///
    /// The order is the protocol's. At QoS 1 the receiver responds "having
    /// accepted ownership" and "need not have completed onward delivery
    /// first" ([MQTT-4.3.2-4]); at QoS 2 the PUBREC follows "all checks for
    /// conditions which might result in a forwarding failure" [mqtt5 §6],
    /// and the check this client can make is the Subscription Identifier
    /// ceiling — so the delivery is *read* before the acknowledgement is
    /// sent, and a delivery that cannot be read is not acknowledged.
    ///
    /// **QoS 2's duplicate suppression is here and only here.** A repeat of a
    /// Packet Identifier already awaiting its PUBREL is answered with another
    /// PUBREC and MUST NOT be delivered again ([MQTT-4.3.3-10]) [mqtt5 §6].
    ///
    /// Returns whether anything was written, so the caller can reset the
    /// keep-alive timer — as [`Task::auth`] does. A QoS 0 delivery only
    /// reaches the application, so it answers `false`: Keep Alive "bounds the
    /// gap from finishing one Client packet to starting the next"
    /// ([MQTT-3.1.2-20]), and counting an inbound packet as a write would let
    /// QoS 0 traffic suppress every PINGREQ until the server closed the
    /// connection at 1.5x ([MQTT-3.1.2-22]).
    async fn receive(&mut self, publish: &Publish<'_>) -> Result<bool> {
        let mut delivery = Delivery::read(publish, &self.limits)?;
        // The Topic Alias is resolved **before** the delivery reaches the
        // application, so nothing above this line ever sees a zero-length
        // Topic Name or an alias number. The mapping is per connection and
        // per direction, bounded by what this client declared in CONNECT
        // ([MQTT-3.3.2-7], [MQTT-3.3.2-8]).
        delivery.topic = self
            .inbound_aliases
            .resolve(publish.properties.topic_alias, publish.topic)?;

        match (publish.qos, publish.packet_id) {
            (QoS::AtMostOnce, _) => {
                self.deliver(delivery).await;
                Ok(false)
            }
            (QoS::AtLeastOnce, Some(packet_id)) => {
                self.deliver(delivery).await;
                self.writer
                    .send(&Packet::Puback(Puback::new(packet_id)))
                    .await?;
                Ok(true)
            }
            (QoS::ExactlyOnce, Some(packet_id)) => {
                let first_sight = self.session.inbound_qos2_received(packet_id)?;
                if first_sight {
                    self.deliver(delivery).await;
                }
                self.writer
                    .send(&Packet::Pubrec(Pubrec::new(packet_id)))
                    .await?;
                Ok(true)
            }
            // The codec refuses a QoS > 0 PUBLISH without an identifier
            // ([MQTT-2.2.1-3]), so this is unreachable from the wire.
            (_, None) => Err(Error::Protocol(DecodeError::InvalidPacketIdentifier)),
        }
    }

    async fn deliver(&mut self, delivery: Delivery) {
        let _ = self.events.send(Event::Delivered(delivery)).await;
    }
}

/// Rewrites a PUBLISH to carry a Topic Alias, where the server accepts one.
///
/// Three outcomes, all of 3.3.2.3.4's: the Topic Name in full and no alias,
/// the Topic Name in full **with** the alias that establishes it, or a
/// zero-length Topic Name and the established alias - which is the only place
/// a zero-length Topic Name is legal (3.3.2.1) [mqtt5 §3].
fn alias(aliases: &mut OutboundAliases, publish: &mut Publish<'_>) {
    match aliases.publish(publish.topic) {
        Aliased::Full => {}
        Aliased::Establish(alias) => publish.properties.topic_alias = Some(alias),
        Aliased::Use(alias) => {
            publish.properties.topic_alias = Some(alias);
            publish.topic = "";
        }
    }
}

/// The borrowed pairs the codec takes.
fn borrowed(pairs: &[(String, String)]) -> Vec<(&str, &str)> {
    pairs
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

/// A QoS 0 PUBLISH.
///
/// Never stored and never given a Packet Identifier: "the Packet Identifier
/// field is only present in PUBLISH packets where the QoS level is 1 or 2"
/// (3.3.2.2), and DUP "MUST be set to 0 for all QoS 0 messages"
/// ([MQTT-3.3.1-2]) [mqtt5 §6] — there is nothing at QoS 0 that could be a
/// retransmission.
fn qos0_publish<'a>(
    message: &'a Message,
    user_properties: &'a [(&'a str, &'a str)],
) -> Result<Publish<'a>> {
    Ok(Publish {
        topic: &message.topic,
        payload: &message.payload,
        qos: QoS::AtMostOnce,
        dup: false,
        retain: message.retain,
        packet_id: None,
        properties: Properties {
            payload_format_indicator: message.payload_format_indicator,
            message_expiry_interval: interval_seconds("message_expiry", message.message_expiry)?,
            content_type: message.content_type.as_deref(),
            response_topic: message.response_topic.as_deref(),
            correlation_data: message.correlation_data.as_deref(),
            ..Properties::new()
        }
        .with_user_properties(user_properties),
    })
}

/// Puts the session's unacknowledged exchanges back on the wire.
///
/// **The only retransmission in this crate**, called from exactly one place:
/// just after a CONNACK with `Session Present` 1. "This is the only
/// circumstance where a Client or Server is REQUIRED to resend messages.
/// Clients and Servers MUST NOT resend messages at any other time"
/// ([MQTT-4.4.0-1]) [mqtt5 §6].
///
/// The order is the order the originals were sent ([MQTT-4.6.0-1]), the
/// identifiers are the originals, and every resent PUBLISH carries DUP 1
/// ([MQTT-3.3.1-1]) — while a PUBREL, which has no DUP flag, carries nothing
/// extra.
async fn retransmit(writer: &mut Writer, session: &Session) -> Result<()> {
    for item in session.resend() {
        match item {
            Resend::Publish { packet_id, message } => {
                let pairs = message.borrowed_user_properties();
                let publish = message.to_publish(packet_id, true, &pairs);
                writer.send(&Packet::Publish(publish)).await?;
            }
            Resend::Pubrel { packet_id } => {
                writer.send(&Packet::Pubrel(Pubrel::new(packet_id))).await?;
            }
        }
    }
    Ok(())
}

/// What woke the connection task.
enum Step {
    Packet(Result<Vec<u8>>),
    Command(Option<Command>),
    Idle,
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_mqtt_codec::varint;

    /// The three constructors, and the one that needs no reactor at all.
    #[test]
    fn an_owned_context_needs_no_ambient_runtime() {
        // Not inside a runtime: `new` must fail here and `owned` must not.
        assert!(Context::new().is_err());
        let context = Context::owned(1).expect("owns a reactor");
        assert!(format!("{context:?}").contains("owns_reactor: true"));
        // A clone keeps the reactor alive after the original is dropped: a
        // task spawned on it still runs, which is the only way to observe
        // that the reactor was not shut down with the original.
        let clone = context.clone();
        drop(context);
        let ran = clone.exec().spawn(async { 7u8 });
        assert_eq!(futures::executor::block_on(ran).expect("the task ran"), 7);
    }

    #[tokio::test]
    async fn an_ambient_context_borrows_the_running_runtime() {
        let context = Context::new().expect("ambient");
        assert!(format!("{context:?}").contains("owns_reactor: false"));
    }

    /// A `Maximum Packet Size` a Four Byte Integer cannot carry is refused at
    /// configuration time, which is also what keeps the decoder's cap sane.
    #[test]
    fn the_decoder_cap_is_the_declared_maximum() {
        let options = ConnectOptions::new("a");
        assert!(options.limits.maximum_packet_size <= varint::MAX + 5);
        assert!(options.validate().is_ok());
    }
}
