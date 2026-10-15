//! The context, the client handle, and the stream of events.
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
//! **One task owns the socket, and the handle is a channel.** The task, and
//! the reasons its loop never waits for the peer or for the application, are
//! the `task` module's; this one is the application's side of the channel and
//! of the event queue behind [`Events`].

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use weida_mqtt_codec::{DisconnectReasonCode, SubackReasonCode, UnsubackReasonCode, varint};
use weida_runtime::{Exec, OwnedReactor};

use crate::connection::{Authenticator, NoAuthenticator, handshake};
use crate::error::{Error, Feature, Result};
use crate::filter::{Subscription, Subscriptions, check_topic_filter};
use crate::limits::ServerLimits;
use crate::message::{Completion, Delivery, Message};
use crate::options::{ConnectOptions, interval_seconds};
use crate::queue::{self, Consumer};
use crate::session::{Resumption, Session, expiry_seconds};
use crate::task::{Parts, Task};

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
    ///
    /// Always the last event, and always delivered: it does not wait for room
    /// in the queue, so an application that has not read a single event still
    /// learns why the connection ended.
    Disconnected(Error),
    /// The server delivered a message.
    ///
    /// At QoS 1 and 2 the acknowledgement is sent **once this event is
    /// queued**, which is what the protocol requires and not an optimization:
    /// the receiver responds "having accepted ownership of the Application
    /// Message" and "does not need to complete delivery before sending the
    /// PUBACK" ([MQTT-4.3.2-4]) [mqtt5 §6]. So this event is a delivery, not
    /// a request for one, and dropping it loses the message. While the queue
    /// is full the acknowledgement waits for room, which is what makes the
    /// queue a flow control for the server's QoS 1 and 2 window.
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
    /// A message the session held from an earlier connection was given up on,
    /// and will not be sent.
    ///
    /// A resumed exchange is put back on the wire, and a message that no longer
    /// fits what the server now accepts — a lower `Maximum Packet Size` than
    /// it was stored under — can be sent on no connection to that server.
    /// Failing the connect would fail every later one the same way, so the
    /// message is released instead, and this event is how the application
    /// learns it was lost.
    Abandoned {
        /// The original Packet Identifier.
        packet_id: u16,
        /// Why it could not be sent.
        error: Error,
    },
}

/// A command for the connection task.
pub(crate) enum Command {
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
    /// `done` resolves when the hop has certified what it is going to: when
    /// the bytes have gone to the transport for QoS 0, on the PUBACK for
    /// QoS 1, on the PUBCOMP — or an early PUBREC of 0x80 or above — for QoS 2.
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
    events: Consumer<Event>,
}

impl Events {
    /// The next event, or `None` once the connection task has finished and
    /// every event has been taken.
    ///
    /// Taking an event is what makes room for the next: the connection stops
    /// reading its socket while [`crate::Limits::incoming_queue`] events or
    /// [`crate::Limits::incoming_queue_bytes`] bytes are waiting here, and
    /// nothing else stops.
    pub async fn next(&mut self) -> Option<Event> {
        self.events.next().await
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
    /// ([MQTT-4.6.0-1]) — the connection task's first act, ahead of anything
    /// this client is asked to send. They go **through the server's `Receive
    /// Maximum`**: past it they wait for acknowledgements, and one that no
    /// longer fits the server's `Maximum Packet Size` is released and reported
    /// as [`Event::Abandoned`] rather than failing the connection.
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
        // The numbers that bound a session are the options' of the connection
        // being made, which a caller may have changed since the session was
        // created.
        session.apply_limits(&options.limits);

        let options_authentication_method = options.authentication_method.clone();
        let handshake = handshake(&context.exec, address, &options, authenticator.as_ref()).await?;

        // "The sender sets an initial send quota, non-zero and not exceeding
        // the peer's Receive Maximum" ([MQTT-4.9.0-1]): the server's number
        // replaces the client's placeholder here and nowhere else.
        session.set_send_quota(handshake.limits.receive_maximum);

        let resumption = session.accept_connack(handshake.session_present)?;
        // What a resumed session puts back on the wire is the task's first
        // act, under its deadlines and alongside its reads.
        let resend = if resumption == Resumption::Resumed {
            session.resend()
        } else {
            Vec::new()
        };

        let limits = Arc::new(handshake.limits);
        let (commands_tx, commands_rx) = mpsc::channel(options.limits.incoming_queue);
        let (events_tx, events_rx) = queue::channel(
            options.limits.incoming_queue,
            options.limits.incoming_queue_bytes,
        );

        let reader = handshake.reader;
        let task = Task::new(Parts {
            writer: handshake.writer,
            commands: commands_rx,
            events: events_tx,
            exec: context.exec.clone(),
            session: session.clone(),
            limits: options.limits,
            keep_alive: handshake.keep_alive,
            // Both bounds follow the Keep Alive in force, which is the
            // server's where it overrode the client's.
            ping_timeout: options.effective_ping_timeout(handshake.keep_alive),
            write_timeout: options.effective_write_timeout(handshake.keep_alive),
            auth_timeout: options.connect_timeout,
            server_topic_alias_maximum: limits.topic_alias_maximum,
            authentication_method: options_authentication_method.clone(),
            authenticator,
            resend,
        });
        context.exec.spawn(task.run(reader));

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
