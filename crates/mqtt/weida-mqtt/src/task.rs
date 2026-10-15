//! The one task that owns the socket.
//!
//! **One task owns the connection, and the handle is a channel.** Every
//! ordering obligation MQTT states is per connection — PUBACK in receipt
//! order, PUBREC in receipt order, PUBREL in PUBREC-receipt order
//! ([MQTT-4.6.0-2] to [MQTT-4.6.0-4]) [mqtt5 §7] — and the specification says
//! nothing about threads [mqtt5 §2]. A single reader and a single writer make
//! those obligations fall out of the structure instead of being reimposed by a
//! sorter, which is the cheapest correct answer.
//!
//! **Nothing in the loop waits for the application or for the peer.** A loop
//! that awaits a write, or a send into a full event queue, stops reading, stops
//! answering commands and stops its timers for as long as the one thing it is
//! waiting for takes — and a peer that stops reading, or an application that
//! stops taking events, can make that for ever. So every wait is a branch of
//! one `select!`:
//!
//! * the **outbox** drains as the socket takes it ([`Writer::step`]), and a
//!   socket that takes nothing for the write timeout ends the connection with
//!   `Timeout("write")`;
//! * the **reader** is enabled only while a delivery could be queued and the
//!   peer is taking the acknowledgements already owed it, so back-pressure
//!   stops the *socket* being read and nothing else;
//! * **commands** are served throughout, `disconnect` included;
//! * one **timer** is armed for the earliest of the deadlines below.
//!
//! **The keep-alive timer is the client's own liveness, and both halves of it
//! are bounded.** Keep Alive "bounds the gap from finishing one client packet
//! to starting the next; absent other traffic the client MUST send PINGREQ"
//! ([MQTT-3.1.2-20]), and a server receiving nothing for 1.5 times the
//! interval MUST close ([MQTT-3.1.2-22]) [mqtt5 §1]. So the task sends PINGREQ
//! only when nothing else has been sent within the interval, measured from the
//! last packet *finished* — a client that pinged on a timer regardless would be
//! spending the bandwidth MQTT exists to save. The other half, waiting for the
//! PINGRESP, has **no** specification number at all [mqtt5 §1]; it is
//! [`crate::ConnectOptions::effective_ping_timeout`] here, measured from the
//! PINGREQ and from nothing else, because every wait is bounded. It does not
//! run while the reader is held back for the application's sake: the peer
//! cannot have answered something this client is not reading.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use weida_mqtt_codec::{
    self as codec, AuthReasonCode, DecodeError, Disconnect, DisconnectReasonCode, Packet,
    PacketType, PayloadList, Properties, Puback, Pubcomp, PubcompReasonCode, Publish, Pubrec,
    PubrecReasonCode, Pubrel, PubrelReasonCode, QoS, SubackReasonCode, UnsubackReasonCode,
};
use weida_runtime::Exec;

use crate::alias::{Aliased, InboundAliases, OutboundAliases};
use crate::client::{Command, Event};
use crate::connection::{AUTH_EXCHANGE, Authenticator, MAX_AUTH_ROUNDS, Reader, Writer};
use crate::error::{Error, Result};
use crate::filter::Subscription;
use crate::limits::Limits;
use crate::message::{Completion, Delivery, Message};
use crate::options::interval_seconds;
use crate::queue::Producer;
use crate::session::{Resend, Session, Stage, StoredPublish};

/// What a queued delivery costs beyond the packet that carried it: the event,
/// the owned topic and property strings, and the queue's own entry. Charged on
/// top of the packet's length so that a flood of empty messages meets the byte
/// ceiling as well as the count.
const DELIVERY_OVERHEAD: usize = 256;

/// Acknowledgements this task will have queued for a peer that has not taken
/// them before it stops reading.
///
/// Each inbound PUBLISH, PUBREL or PUBREC makes this client write a few bytes
/// back, so a peer that never reads what it is sent could otherwise make the
/// outbox grow by a packet for every packet it sends. Past this the reader
/// waits until the socket has taken some, and a socket that takes none ends
/// the connection at the write timeout.
const MAX_UNSENT_ACKS: usize = 4096;

/// Who is waiting for a SUBACK's per-filter verdicts.
type SubackWaiter = oneshot::Sender<Result<Vec<SubackReasonCode>>>;
/// Who is waiting for an UNSUBACK's.
type UnsubackWaiter = oneshot::Sender<Result<Vec<UnsubackReasonCode>>>;
/// Who is waiting for a publish to complete.
type PublishWaiter = oneshot::Sender<Result<Completion>>;

/// A publish that arrived with the send quota already spent.
struct Stalled {
    message: Arc<StoredPublish>,
    done: PublishWaiter,
}

/// A delivery the event queue had no room for, and what its acknowledgement is
/// waiting on.
///
/// **The acknowledgement follows the queueing**, which is what makes the queue
/// a flow control and not a place to lose messages: a QoS 1 or 2 PUBLISH is
/// answered once the application has somewhere to take it from, so a server
/// whose window fills with unanswered PUBLISHes stops sending them — and
/// "having accepted ownership" ([MQTT-4.3.2-4]) is then true.
struct Held {
    event: Event,
    charge: usize,
    qos: QoS,
    packet_id: Option<u16>,
}

/// What resolves when the bytes of a queued packet have gone out.
enum FlushDone {
    /// A QoS 0 publish: "handed to the transport" is its whole certificate.
    Publish(PublishWaiter),
    /// A PINGREQ. Whoever asked for it is told when it has gone, and the
    /// PINGRESP wait starts then and not when it was queued.
    Ping(Option<oneshot::Sender<Result<()>>>),
}

/// An enhanced re-authentication in progress.
struct Reauthentication {
    done: oneshot::Sender<Result<()>>,
    /// AUTH packets answered so far, against [`MAX_AUTH_ROUNDS`].
    rounds: u32,
    /// When it must be over, so that a server that never ends it cannot hold
    /// the connection — and the caller — for ever.
    deadline: Instant,
}

/// The connection is ending, and is waiting for its last packet to go out.
struct Closing {
    /// The outbox offset the last packet ends at.
    end: u64,
    /// Whoever asked for the DISCONNECT, told when it has gone.
    done: Option<oneshot::Sender<Result<()>>>,
    /// What the connection ends with, where it is ending because of a failure.
    cause: Option<Error>,
}

/// What woke the loop.
enum Step<'r> {
    Written(Result<()>),
    Command(Option<Command>),
    Space,
    Packet(Result<&'r [u8]>),
    Timer,
}

/// What the loop does next.
enum Flow {
    Continue,
    Stop,
}

/// What a connection task is made from.
pub(crate) struct Parts {
    pub(crate) writer: Writer,
    pub(crate) commands: mpsc::Receiver<Command>,
    pub(crate) events: Producer<Event>,
    pub(crate) exec: Exec,
    pub(crate) session: Session,
    /// This client's own declared limits, for reading a delivery under the
    /// ceilings the protocol does not provide.
    pub(crate) limits: Limits,
    pub(crate) keep_alive: Option<Duration>,
    pub(crate) ping_timeout: Option<Duration>,
    pub(crate) write_timeout: Duration,
    /// How long a re-authentication may take.
    pub(crate) auth_timeout: Duration,
    /// The server's `Topic Alias Maximum`, which bounds the aliases this
    /// client hands out.
    pub(crate) server_topic_alias_maximum: u16,
    pub(crate) authentication_method: Option<String>,
    /// How this connection answers an AUTH challenge, kept because
    /// re-authentication happens on the **live** connection and not in the
    /// handshake (4.12.1) [mqtt5 §10].
    pub(crate) authenticator: Arc<dyn Authenticator>,
    /// What the session held from an earlier connection, to put back on the
    /// wire.
    pub(crate) resend: Vec<Resend>,
}

/// The task that owns the socket.
pub(crate) struct Task {
    writer: Writer,
    commands: mpsc::Receiver<Command>,
    events: Producer<Event>,
    exec: Exec,
    session: Session,
    limits: Limits,
    keep_alive: Option<Duration>,
    ping_timeout: Option<Duration>,
    write_timeout: Duration,
    auth_timeout: Duration,
    /// The aliases this client hands out, bounded by the server's `Topic
    /// Alias Maximum`. Per connection: this field's lifetime *is* the
    /// mapping's lifetime ([MQTT-3.3.2-7]).
    outbound_aliases: OutboundAliases,
    /// The aliases the server establishes toward this client, bounded by the
    /// `Topic Alias Maximum` this client declared.
    inbound_aliases: InboundAliases,
    /// Who is waiting for each SUBACK, with the filters the SUBSCRIBE
    /// carried: the codes are matched to filters by position (3.9.3), so the
    /// list has to be kept to read the answer.
    subscribe_waiters: HashMap<u16, (Vec<Subscription>, Option<u32>, SubackWaiter)>,
    /// The same for UNSUBACK.
    unsubscribe_waiters: HashMap<u16, (Vec<String>, UnsubackWaiter)>,
    /// Who is waiting for each in-flight exchange to complete, keyed by
    /// Packet Identifier. An exchange resumed from a previous connection has
    /// no entry, and its completion becomes [`Event::Completed`].
    waiters: HashMap<u16, PublishWaiter>,
    /// Publishes that arrived with the send quota already spent.
    ///
    /// "At zero the sender MUST NOT send further QoS > 0 PUBLISH packets"
    /// ([MQTT-4.9.0-2]) and exhaustion "stalls the sender rather than
    /// exceeding it" [mqtt5 §5]. This queue **is** that stall: the caller's
    /// future is still pending, the packet is not on the wire, and the moment
    /// an identifier frees the oldest waiting publish takes it — **from the
    /// front, and back to the front** where it still does not fit, so the
    /// order the application published in is the order of the wire. Bounded by
    /// the publishes the application has in flight, which is the number of
    /// `publish()` futures it holds, and by the session's own ceilings on
    /// what it stores.
    stalled: VecDeque<Stalled>,
    /// Identifiers of messages the session held from an earlier connection
    /// whose PUBLISH is not on the wire yet because the server's `Receive
    /// Maximum` is lower than what was in flight, in the order they were
    /// first sent ([MQTT-4.6.0-1]). They go before anything in `stalled`.
    resumed: VecDeque<u16>,
    /// Who is waiting for a re-authentication to finish.
    ///
    /// `None` means none is in progress, which is also what makes an
    /// unrequested AUTH detectable: after CONNACK a server may only send AUTH
    /// inside an exchange the client started ([MQTT-4.12.1-1]).
    reauthenticating: Option<Reauthentication>,
    authenticator: Arc<dyn Authenticator>,
    /// The method every AUTH of this connection repeats ([MQTT-4.12.0-5]).
    authentication_method: Option<String>,
    /// A delivery the event queue had no room for. While it is held the
    /// socket is not read.
    held: Option<Held>,
    /// Queued packets whose completion is "it went out", by the outbox offset
    /// they end at.
    flush_waiters: VecDeque<(u64, FlushDone)>,
    /// The outbox offsets of acknowledgements queued for the peer, not yet
    /// taken by the transport. Its length is what [`MAX_UNSENT_ACKS`] bounds.
    unsent_acks: VecDeque<u64>,
    /// When the PINGREQ the PINGRESP is awaited for finished going out.
    ping_sent_at: Option<Instant>,
    /// Since when the reader has been held back, for the PINGRESP clock.
    paused_since: Option<Instant>,
    closing: Option<Closing>,
    /// The session's exchanges to put back on the wire, until `drive` begins.
    resend: Vec<Resend>,
}

impl Task {
    pub(crate) fn new(parts: Parts) -> Task {
        Task {
            writer: parts.writer,
            commands: parts.commands,
            events: parts.events,
            exec: parts.exec,
            session: parts.session,
            keep_alive: parts.keep_alive,
            ping_timeout: parts.ping_timeout,
            write_timeout: parts.write_timeout,
            auth_timeout: parts.auth_timeout,
            // Two bounds, two directions: what the server said it accepts,
            // and what this client said it accepts ([MQTT-3.3.2-8]).
            outbound_aliases: OutboundAliases::new(
                parts.server_topic_alias_maximum,
                parts.limits.max_alias_bytes,
            ),
            inbound_aliases: InboundAliases::new(
                parts.limits.topic_alias_maximum,
                parts.limits.max_alias_bytes,
            ),
            limits: parts.limits,
            subscribe_waiters: HashMap::new(),
            unsubscribe_waiters: HashMap::new(),
            waiters: HashMap::new(),
            stalled: VecDeque::new(),
            resumed: VecDeque::new(),
            reauthenticating: None,
            authenticator: parts.authenticator,
            authentication_method: parts.authentication_method,
            held: None,
            flush_waiters: VecDeque::new(),
            unsent_acks: VecDeque::new(),
            ping_sent_at: None,
            paused_since: None,
            closing: None,
            resend: parts.resend,
        }
    }

    pub(crate) async fn run(mut self, mut reader: Reader) {
        let outcome = self.drive(&mut reader).await;
        let error = self.conclude(outcome);

        // The socket closes **first**, and what was waiting on it is released
        // with it: an application that sees the connection end is not left
        // holding futures that still belong to a task parked on its event
        // queue.
        for id in self
            .subscribe_waiters
            .keys()
            .chain(self.unsubscribe_waiters.keys())
        {
            // A SUBSCRIBE that went unanswered leaves its identifier in the
            // session the application keeps, and sixty-five thousand of them
            // would exhaust the space for every connection that follows.
            self.session.release_control(*id);
        }
        let Task {
            writer,
            events,
            waiters,
            subscribe_waiters,
            unsubscribe_waiters,
            stalled,
            flush_waiters,
            held,
            ..
        } = self;
        drop((reader, writer));
        drop((
            waiters,
            subscribe_waiters,
            unsubscribe_waiters,
            stalled,
            flush_waiters,
            held,
        ));
        // Never blocks, and never fails for want of room: the last event is
        // not subject to the queue's ceilings. A send to an application that
        // dropped its stream is its right, and the connection is over either
        // way.
        events.push(Event::Disconnected(error));
    }

    /// What a finished connection reports, and who is owed an answer for it.
    fn conclude(&mut self, outcome: Result<()>) -> Error {
        let closing = self.closing.take();
        match outcome {
            Ok(()) => match closing {
                Some(Closing {
                    cause: Some(cause), ..
                }) => cause,
                _ => Error::NotConnected,
            },
            Err(error) => {
                if let Some(closing) = closing {
                    if let Some(done) = closing.done {
                        let _ = done.send(Err(error.replicate()));
                    }
                    if let Some(cause) = closing.cause {
                        return cause;
                    }
                }
                error
            }
        }
    }

    /// Reads, answers and pings until something ends the connection.
    ///
    /// The reader is the caller's and not a field, because the packet it lends
    /// is handled by methods that borrow the rest of the task.
    async fn drive(&mut self, reader: &mut Reader) -> Result<()> {
        self.resume()?;

        // One timer, armed for the earliest deadline and moved only when that
        // changes: a `Sleep` made per turn of the loop would be one timer-wheel
        // insertion and removal per packet.
        let mut timer = std::pin::pin!(self.exec.sleep(Duration::ZERO));
        let mut armed: Option<Instant> = None;

        loop {
            let due = self.next_due();
            if due != armed {
                if let Some(at) = due {
                    timer.as_mut().reset(tokio::time::Instant::from_std(at));
                }
                armed = due;
            }
            let reading = self.may_read();
            self.track_pause(reading);
            let held_charge = self.held.as_ref().map_or(0, |held| held.charge);

            let step = tokio::select! {
                result = self.writer.step(), if self.writer.has_work() => Step::Written(result),
                command = self.commands.recv(), if self.closing.is_none() => Step::Command(command),
                () = self.events.space(held_charge), if self.held.is_some() => Step::Space,
                packet = reader.next(), if reading => Step::Packet(packet),
                () = timer.as_mut(), if due.is_some() => Step::Timer,
            };

            let flow = match step {
                Step::Written(result) => {
                    result?;
                    self.check_closing()?
                }
                Step::Command(command) => self.on_command(command)?,
                Step::Space => {
                    self.retry_held()?;
                    Flow::Continue
                }
                Step::Packet(packet) => {
                    self.on_packet(packet?)?;
                    Flow::Continue
                }
                Step::Timer => {
                    // Re-armed from the state as it stands after this.
                    armed = None;
                    self.on_timer()?
                }
            };
            if let Flow::Stop = flow {
                return Ok(());
            }
        }
    }

    /// Whether the socket is read this turn.
    fn may_read(&self) -> bool {
        self.held.is_none() && self.closing.is_none() && self.unsent_acks.len() < MAX_UNSENT_ACKS
    }

    /// Keeps the PINGRESP clock honest about time the reader was held back.
    ///
    /// A PINGRESP this client is not reading cannot have arrived in a way it
    /// can see, so the time spent not reading is not time the peer was silent:
    /// the deadline moves later by exactly that, and does not run meanwhile.
    fn track_pause(&mut self, reading: bool) {
        match (reading, self.paused_since) {
            (false, None) => self.paused_since = Some(Instant::now()),
            (true, Some(since)) => {
                if let Some(sent) = &mut self.ping_sent_at {
                    *sent += since.elapsed();
                }
                self.paused_since = None;
            }
            _ => {}
        }
    }

    /// The earliest moment something has to be done about the clock.
    fn next_due(&self) -> Option<Instant> {
        let mut due: Option<Instant> = None;
        let mut consider = |at: Instant| due = Some(due.map_or(at, |best| best.min(at)));

        // A write the socket is not taking, measured from the last byte it did.
        if self.writer.has_work() {
            consider(self.writer.progress_at() + self.write_timeout);
        }
        if self.closing.is_none() {
            // The PINGRESP, measured from the PINGREQ that asked for it.
            if let (Some(sent), Some(timeout)) = (self.ping_sent_at, self.ping_timeout)
                && self.paused_since.is_none()
            {
                consider(sent + timeout);
            }
            // Keep Alive, measured from the end of the last packet, and only
            // while nothing is being written and nothing is awaited.
            if self.ping_sent_at.is_none()
                && !self.writer.has_work()
                && let Some(interval) = self.keep_alive
            {
                consider(self.writer.confirmed_at() + interval);
            }
            if let Some(reauthentication) = &self.reauthenticating {
                consider(reauthentication.deadline);
            }
        }
        due
    }

    /// Acts on whichever deadline has passed.
    fn on_timer(&mut self) -> Result<Flow> {
        let now = Instant::now();
        if self.writer.has_work() && now >= self.writer.progress_at() + self.write_timeout {
            return Err(Error::Timeout("write"));
        }
        if self.closing.is_some() {
            return Ok(Flow::Continue);
        }
        if self
            .reauthenticating
            .as_ref()
            .is_some_and(|reauthentication| now >= reauthentication.deadline)
        {
            if let Some(reauthentication) = self.reauthenticating.take() {
                let _ = reauthentication
                    .done
                    .send(Err(Error::Timeout(AUTH_EXCHANGE)));
            }
            return Err(Error::Timeout(AUTH_EXCHANGE));
        }
        if let (Some(sent), Some(timeout)) = (self.ping_sent_at, self.ping_timeout)
            && self.paused_since.is_none()
            && now >= sent + timeout
        {
            // The number the specification declines to give, spent.
            return Err(Error::Timeout("PINGRESP"));
        }
        if self.ping_sent_at.is_none()
            && !self.writer.has_work()
            && let Some(interval) = self.keep_alive
            && now >= self.writer.confirmed_at() + interval
        {
            // "Absent other traffic the Client MUST send a PINGREQ"
            // ([MQTT-3.1.2-20]). The timer runs from the last packet finished,
            // so reaching here *is* "absent other traffic".
            let end = self.writer.queue(&Packet::Pingreq)?;
            self.flush_waiters.push_back((end, FlushDone::Ping(None)));
        }
        Ok(Flow::Continue)
    }

    /// What follows a write that made progress: the completions it earned, and
    /// the end of a connection that was waiting for its last packet.
    fn check_closing(&mut self) -> Result<Flow> {
        let confirmed = self.writer.confirmed();
        while self
            .flush_waiters
            .front()
            .is_some_and(|(end, _)| *end <= confirmed)
        {
            let Some((_, done)) = self.flush_waiters.pop_front() else {
                break;
            };
            match done {
                FlushDone::Publish(done) => {
                    let _ = done.send(Ok(Completion::Sent));
                }
                FlushDone::Ping(done) => {
                    self.ping_sent_at = Some(Instant::now());
                    if let Some(done) = done {
                        let _ = done.send(Ok(()));
                    }
                }
            }
        }
        let written = self.writer.written();
        while self.unsent_acks.front().is_some_and(|end| *end <= written) {
            self.unsent_acks.pop_front();
        }

        if self
            .closing
            .as_ref()
            .is_some_and(|closing| confirmed >= closing.end)
        {
            let Some(closing) = self.closing.take() else {
                return Ok(Flow::Continue);
            };
            if let Some(done) = closing.done {
                let _ = done.send(Ok(()));
            }
            return match closing.cause {
                Some(cause) => Err(cause),
                None => Ok(Flow::Stop),
            };
        }
        Ok(Flow::Continue)
    }

    fn on_command(&mut self, command: Option<Command>) -> Result<Flow> {
        let Some(command) = command else {
            // Every `Client` handle is gone and nobody asked for a
            // DISCONNECT. Closing without one is what triggers the Will
            // [mqtt5 §4.5], and inventing an orderly close on the
            // application's behalf would suppress it. What the application had
            // already handed over is still written first.
            self.closing = Some(Closing {
                end: self.writer.queued(),
                done: None,
                cause: None,
            });
            return self.check_closing();
        };
        match command {
            Command::Publish { message, done } => self.publish(message, done),
            Command::Ping { done } => match self.writer.queue(&Packet::Pingreq) {
                Ok(end) => self
                    .flush_waiters
                    .push_back((end, FlushDone::Ping(Some(done)))),
                Err(error) => {
                    let _ = done.send(Err(error));
                }
            },
            Command::Subscribe {
                subscriptions,
                identifier,
                done,
            } => self.send_subscribe(subscriptions, identifier, done),
            Command::Unsubscribe { filters, done } => self.send_unsubscribe(filters, done),
            Command::Reauthenticate { done } => self.send_reauthenticate(done),
            Command::Disconnect {
                reason_code,
                session_expiry,
                done,
            } => {
                self.begin_disconnect(reason_code, session_expiry, done);
                // Nothing may follow it, and what is already queued goes out
                // first: "after sending DISCONNECT the sender MUST send
                // nothing more and MUST close" ([MQTT-3.14.4-1],
                // [MQTT-3.14.4-2]) [mqtt5 §12/P17].
                return self.check_closing();
            }
        }
        Ok(Flow::Continue)
    }

    /// Queues the DISCONNECT and starts waiting for it to go.
    fn begin_disconnect(
        &mut self,
        reason_code: DisconnectReasonCode,
        session_expiry: Option<Duration>,
        done: oneshot::Sender<Result<()>>,
    ) {
        let packet = interval_seconds("session_expiry", session_expiry).map(|seconds| {
            Packet::Disconnect(Disconnect {
                reason_code,
                properties: Properties {
                    session_expiry_interval: seconds,
                    ..Properties::new()
                },
            })
        });
        let queued = packet.and_then(|packet| self.writer.queue(&packet));
        self.closing = Some(match queued {
            Ok(end) => Closing {
                end,
                done: Some(done),
                cause: None,
            },
            // A DISCONNECT that cannot be built still ends the connection:
            // the application asked for it to end.
            Err(error) => {
                let _ = done.send(Err(error));
                Closing {
                    end: self.writer.queued(),
                    done: None,
                    cause: None,
                }
            }
        });
    }

    /// Puts what the session held back on the wire, in the order the originals
    /// were sent ([MQTT-4.6.0-1]).
    ///
    /// **The only retransmission in this crate**, called from exactly one
    /// place: just after a CONNACK with `Session Present` 1. "This is the only
    /// circumstance where a Client or Server is REQUIRED to resend messages.
    /// Clients and Servers MUST NOT resend messages at any other time"
    /// ([MQTT-4.4.0-1]) [mqtt5 §6]. The identifiers are the originals, every
    /// resent PUBLISH carries DUP 1 ([MQTT-3.3.1-1]), and a PUBREL — which has
    /// no DUP flag — carries nothing extra.
    ///
    /// **Through the quota, not around it.** The server's `Receive Maximum`
    /// may be lower now than it was when these were first sent, and a server
    /// answers more PUBLISHes than it declared with DISCONNECT 0x93 — and again
    /// on the next connection, for ever. So a PUBLISH goes out while the quota
    /// has room and waits in `resumed` for the next acknowledgement where it
    /// does not; a PUBREL is not a PUBLISH and never waits.
    fn resume(&mut self) -> Result<()> {
        let items = std::mem::take(&mut self.resend);
        for item in &items {
            if let Resend::Publish { packet_id, .. } = item {
                self.resumed.push_back(*packet_id);
            }
        }
        for item in items {
            match item {
                Resend::Pubrel { packet_id } => {
                    self.writer.queue(&Packet::Pubrel(Pubrel::new(packet_id)))?;
                }
                Resend::Publish { packet_id, .. } => {
                    if self.resumed.front() == Some(&packet_id) && self.wire_quota_free() {
                        self.resumed.pop_front();
                        self.retransmit(packet_id);
                    }
                }
            }
        }
        Ok(())
    }

    /// Whether the server's `Receive Maximum` has room for one more PUBLISH on
    /// the wire: every exchange the session holds counts, except the resumed
    /// ones that have not been sent.
    fn wire_quota_free(&self) -> bool {
        self.session.in_flight().saturating_sub(self.resumed.len())
            < usize::from(self.session.send_quota())
    }

    /// Sends a stored message again, DUP 1, under its original identifier.
    ///
    /// A message that no longer fits what the server now accepts — a lower
    /// `Maximum Packet Size` than the connection it was stored on — cannot be
    /// sent on this connection or any like it, and is let go **by itself**: its
    /// identifier is released and the application is told. Failing the connect
    /// instead would fail every later one the same way, with the session
    /// holding the message for good.
    fn retransmit(&mut self, packet_id: u16) {
        let Some(message) = self.session.message_of(packet_id) else {
            // Completed while it waited.
            return;
        };
        if let Err(error) = self.transmit(packet_id, &message, true) {
            self.session.release(packet_id);
            self.events.push(Event::Abandoned { packet_id, error });
        }
    }

    /// Encodes one QoS 1 or 2 PUBLISH into the outbox.
    ///
    /// The wire copy may be aliased; the **stored** copy keeps its full Topic
    /// Name, because a mapping "MUST NOT be carried across Network
    /// Connections" ([MQTT-3.3.2-7]) and a retransmission happens on the next
    /// one — so a retransmission carries none.
    fn transmit(&mut self, packet_id: u16, message: &StoredPublish, dup: bool) -> Result<u64> {
        let pairs = message.borrowed_user_properties();
        let mut publish = message.to_publish(packet_id, dup, &pairs);
        let verdict = if dup {
            Aliased::Full
        } else {
            self.outbound_aliases.publish(&message.topic)
        };
        apply_alias(&mut publish, verdict);
        self.writer
            .queue(&Packet::Publish(publish))
            .inspect_err(|_| self.outbound_aliases.revoke(verdict, &message.topic))
    }

    /// Sends one message, or stalls it if the quota is spent.
    fn publish(&mut self, message: Box<Message>, done: PublishWaiter) {
        if message.qos == QoS::AtMostOnce {
            self.publish_unacknowledged(&message, done);
            return;
        }
        // By move: the message is the application's to give and is not wanted
        // again, so it becomes the stored copy without a second payload.
        let stored = match (*message).into_stored() {
            Ok(stored) => Arc::new(stored),
            Err(error) => {
                let _ = done.send(Err(error));
                return;
            }
        };
        // A new publish never overtakes one that is already waiting.
        if !self.stalled.is_empty() || !self.resumed.is_empty() {
            self.stalled.push_back(Stalled {
                message: stored,
                done,
            });
            return;
        }
        if let Some(stalled) = self.send_new(Stalled {
            message: stored,
            done,
        }) {
            self.stalled.push_back(stalled);
        }
    }

    /// Stores and sends a message, or hands it back where the quota is spent.
    fn send_new(&mut self, stalled: Stalled) -> Option<Stalled> {
        let Stalled { message, done } = stalled;
        match self.session.allocate(Arc::clone(&message)) {
            Ok(packet_id) => {
                match self.transmit(packet_id, &message, false) {
                    Ok(_) => {
                        self.waiters.insert(packet_id, done);
                    }
                    // Refused here — over the server's `Maximum Packet Size` —
                    // and the stream is intact: this publish fails and the
                    // connection does not.
                    Err(error) => {
                        self.session.release(packet_id);
                        let _ = done.send(Err(error));
                    }
                }
                None
            }
            // The stall of [MQTT-4.9.0-2]: the caller's future stays pending
            // and the packet stays off the wire.
            Err((_, message)) => Some(Stalled { message, done }),
        }
    }

    /// QoS 0 goes straight into the outbox and resolves when it has gone: it is
    /// "not any other packet type" in the quota's unit, so it is never counted
    /// and never waits for an acknowledgement [mqtt5 §5].
    ///
    /// Never stored and never given a Packet Identifier: "the Packet
    /// Identifier field is only present in PUBLISH packets where the QoS level
    /// is 1 or 2" (3.3.2.2), and DUP "MUST be set to 0 for all QoS 0 messages"
    /// ([MQTT-3.3.1-2]) [mqtt5 §6].
    fn publish_unacknowledged(&mut self, message: &Message, done: PublishWaiter) {
        let pairs = borrowed(&message.user_properties);
        let mut publish = match qos0_publish(message, &pairs) {
            Ok(publish) => publish,
            Err(error) => {
                let _ = done.send(Err(error));
                return;
            }
        };
        let verdict = self.outbound_aliases.publish(publish.topic);
        apply_alias(&mut publish, verdict);
        match self.writer.queue(&Packet::Publish(publish)) {
            Ok(end) => self
                .flush_waiters
                .push_back((end, FlushDone::Publish(done))),
            Err(error) => {
                self.outbound_aliases.revoke(verdict, &message.topic);
                let _ = done.send(Err(error));
            }
        }
    }

    /// Sends what the quota now admits: the resumed messages first, then the
    /// stalled ones, each in order.
    ///
    /// Cheap when nothing can go — one look at the session — and never
    /// reorders: a head that does not fit goes back to the *front*.
    fn drain_pending(&mut self) {
        loop {
            if let Some(&packet_id) = self.resumed.front() {
                if !self.wire_quota_free() {
                    return;
                }
                self.resumed.pop_front();
                self.retransmit(packet_id);
                continue;
            }
            let Some(stalled) = self.stalled.pop_front() else {
                return;
            };
            if let Some(stalled) = self.send_new(stalled) {
                self.stalled.push_front(stalled);
                return;
            }
        }
    }

    /// Sends SUBSCRIBE and remembers who is waiting for its SUBACK.
    ///
    /// The identifier comes from the session's **one** space, shared with
    /// PUBLISH ([MQTT-2.2.1-3]), and is released when the SUBACK arrives.
    /// Nothing is stored for retransmission: "SUBSCRIBE and UNSUBSCRIBE are
    /// not in the retransmission list at all" (4.4) [mqtt5 §6].
    fn send_subscribe(
        &mut self,
        subscriptions: Vec<Subscription>,
        identifier: Option<u32>,
        done: SubackWaiter,
    ) {
        let packet_id = match self.session.allocate_control() {
            Ok(packet_id) => packet_id,
            Err(error) => {
                let _ = done.send(Err(error));
                return;
            }
        };
        let queued = {
            let filters: Vec<codec::Subscription<'_>> = subscriptions
                .iter()
                .map(|subscription| codec::Subscription {
                    filter: &subscription.filter,
                    options: subscription.options,
                })
                .collect();
            // The property is repeatable on the wire, but SUBSCRIBE carries
            // "at most one" (3.8.2.1.2) [mqtt5 §4.1], so the slice is zero or
            // one long and its bound is the option's.
            let ids: Vec<u32> = identifier.into_iter().collect();
            self.writer.queue(&Packet::Subscribe(codec::Subscribe {
                packet_id,
                properties: Properties::new().with_subscription_identifiers(&ids),
                filters: PayloadList::new(&filters),
            }))
        };
        match queued {
            Ok(_) => {
                self.subscribe_waiters
                    .insert(packet_id, (subscriptions, identifier, done));
            }
            Err(error) => {
                self.session.release_control(packet_id);
                let _ = done.send(Err(error));
            }
        }
    }

    /// Sends UNSUBSCRIBE and remembers who is waiting for its UNSUBACK.
    fn send_unsubscribe(&mut self, filters: Vec<String>, done: UnsubackWaiter) {
        let packet_id = match self.session.allocate_control() {
            Ok(packet_id) => packet_id,
            Err(error) => {
                let _ = done.send(Err(error));
                return;
            }
        };
        let queued = {
            let borrowed: Vec<&str> = filters.iter().map(String::as_str).collect();
            self.writer.queue(&Packet::Unsubscribe(codec::Unsubscribe {
                packet_id,
                properties: Properties::new(),
                filters: PayloadList::new(&borrowed),
            }))
        };
        match queued {
            Ok(_) => {
                self.unsubscribe_waiters.insert(packet_id, (filters, done));
            }
            Err(error) => {
                self.session.release_control(packet_id);
                let _ = done.send(Err(error));
            }
        }
    }

    /// Handles one packet from the server.
    fn on_packet(&mut self, frame: &[u8]) -> Result<()> {
        let (packet, _) = Packet::decode(frame, self.limits.maximum_packet_size)?;
        match packet {
            Packet::Pingresp => self.ping_sent_at = None,
            Packet::Disconnect(disconnect) => {
                // The whole point of 5.0's server-to-client DISCONNECT: the
                // code reaches the application.
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
            Packet::Publish(publish) => self.on_publish(frame.len(), &publish)?,
            Packet::Puback(puback) => self.settle(
                puback.packet_id,
                Stage::AwaitingPuback,
                Completion::Acknowledged(puback.reason_code),
            ),
            Packet::Pubrec(pubrec) => self.on_pubrec(pubrec.packet_id, pubrec.reason_code)?,
            Packet::Pubcomp(pubcomp) => self.settle(
                pubcomp.packet_id,
                Stage::AwaitingPubcomp,
                Completion::Complete(pubcomp.reason_code),
            ),
            Packet::Pubrel(pubrel) => self.on_pubrel(pubrel.packet_id)?,
            Packet::Suback(suback) => self.on_suback(&suback),
            Packet::Unsuback(unsuback) => self.on_unsuback(&unsuback),
            Packet::Auth(auth) => self.on_auth(&auth)?,
        }

        // Something may have freed an identifier, so the oldest waiting
        // publish can go.
        if !self.resumed.is_empty() || !self.stalled.is_empty() {
            self.drain_pending();
        }
        Ok(())
    }

    /// Queues an answer to a packet the peer sent, and counts it as owed.
    fn respond(&mut self, packet: Packet<'_>) -> Result<()> {
        let end = self.writer.queue(&packet)?;
        self.unsent_acks.push_back(end);
        Ok(())
    }

    /// A received PUBLISH: deliver it, and acknowledge it at QoS 1 and 2.
    ///
    /// The order is the protocol's. At QoS 1 the receiver responds "having
    /// accepted ownership" and "need not have completed onward delivery
    /// first" ([MQTT-4.3.2-4]); at QoS 2 the PUBREC follows "all checks for
    /// conditions which might result in a forwarding failure" [mqtt5 §6], and
    /// the check this client can make is the Receive Maximum it declared. So
    /// the delivery is **queued** before the acknowledgement is sent, and an
    /// acknowledgement for a delivery that has no room waits for it.
    ///
    /// **QoS 2's duplicate suppression is here and only here.** A repeat of a
    /// Packet Identifier already awaiting its PUBREL is answered with another
    /// PUBREC and MUST NOT be delivered again ([MQTT-4.3.3-10]) [mqtt5 §6], so
    /// it is recognised before a delivery is built for it.
    fn on_publish(&mut self, frame_len: usize, publish: &Publish<'_>) -> Result<()> {
        if publish.qos == QoS::ExactlyOnce {
            let Some(packet_id) = publish.packet_id else {
                return Err(Error::Protocol(DecodeError::InvalidPacketIdentifier));
            };
            if self.session.holds_inbound_qos2(packet_id) {
                // A repeat may still carry the alias a first copy did not.
                self.inbound_aliases
                    .resolve(publish.properties.topic_alias, publish.topic)?;
                return self.respond(Packet::Pubrec(Pubrec::new(packet_id)));
            }
            if self.session.awaiting_pubrel() >= usize::from(self.session.receive_maximum()) {
                return Err(Error::ReceiveMaximumExceeded {
                    quota: self.session.receive_maximum(),
                });
            }
        }

        // The Topic Alias is resolved **before** the delivery reaches the
        // application, so nothing above this line ever sees a zero-length
        // Topic Name or an alias number. The mapping is per connection and
        // per direction, bounded by what this client declared in CONNECT
        // ([MQTT-3.3.2-7], [MQTT-3.3.2-8]).
        let topic = self
            .inbound_aliases
            .resolve(publish.properties.topic_alias, publish.topic)?;
        let delivery = Delivery::read(publish, topic, &self.limits);

        let charge = frame_len + DELIVERY_OVERHEAD;
        match self.events.offer(Event::Delivered(delivery), charge) {
            Ok(()) => self.acknowledge(publish.qos, publish.packet_id),
            Err(event) => {
                self.held = Some(Held {
                    event,
                    charge,
                    qos: publish.qos,
                    packet_id: publish.packet_id,
                });
                Ok(())
            }
        }
    }

    /// Answers a delivery that has been queued for the application.
    ///
    /// Returns an error only for what the codec's own rules already refuse:
    /// a QoS > 0 PUBLISH without an identifier cannot arrive from the wire
    /// ([MQTT-2.2.1-3]).
    fn acknowledge(&mut self, qos: QoS, packet_id: Option<u16>) -> Result<()> {
        match (qos, packet_id) {
            (QoS::AtMostOnce, _) => Ok(()),
            (QoS::AtLeastOnce, Some(packet_id)) => {
                self.respond(Packet::Puback(Puback::new(packet_id)))
            }
            (QoS::ExactlyOnce, Some(packet_id)) => {
                // Recorded **now** and not on receipt: a delivery that was
                // held and never queued must still be a first sight when the
                // server sends it again.
                self.session.inbound_qos2_received(packet_id)?;
                self.respond(Packet::Pubrec(Pubrec::new(packet_id)))
            }
            (_, None) => Err(Error::Protocol(DecodeError::InvalidPacketIdentifier)),
        }
    }

    /// The event queue has room for the delivery that was waiting for it.
    fn retry_held(&mut self) -> Result<()> {
        let Some(held) = self.held.take() else {
            return Ok(());
        };
        let Held {
            event,
            charge,
            qos,
            packet_id,
        } = held;
        match self.events.offer(event, charge) {
            Ok(()) => self.acknowledge(qos, packet_id),
            Err(event) => {
                self.held = Some(Held {
                    event,
                    charge,
                    qos,
                    packet_id,
                });
                Ok(())
            }
        }
    }

    /// Resolves an exchange and releases its identifier.
    ///
    /// An identifier frees "on PUBACK, PUBCOMP, a PUBREC with code >= 0x80,
    /// or SUBACK/UNSUBACK" [mqtt5 §2] — **when the acknowledgement is the one
    /// the exchange is waiting for.** A PUBACK for a QoS 2 message that has
    /// not seen its PUBREC, or an identifier nothing holds, is not that: it
    /// settles nothing and tells the application nothing, because otherwise
    /// any packet a peer chose to send would drop the only stored copy of a
    /// message and report it complete. An acknowledgement for an identifier
    /// this client is not holding is the recovery case 0x92 describes, which
    /// "is not an error during recovery, but at other times indicates a
    /// mismatch" (3.6.2.1) [mqtt5 §6], and a client cannot tell the two apart.
    fn settle(&mut self, packet_id: u16, stage: Stage, completion: Completion) {
        if !self.session.release_awaiting(packet_id, stage) {
            return;
        }
        match self.waiters.remove(&packet_id) {
            Some(done) => {
                let _ = done.send(Ok(completion));
            }
            // No waiter: the exchange was resumed from a previous connection,
            // so its completion goes to the events rather than nowhere.
            None => self.events.push(Event::Completed {
                packet_id,
                completion,
            }),
        }
    }

    /// QoS 2, part two: answer a PUBREC.
    ///
    /// "On a PUBREC with code < 0x80 send PUBREL with the same identifier"
    /// ([MQTT-4.3.3-4]); at 0x80 or above the message "counts as acknowledged
    /// and MUST NOT be retransmitted" ([MQTT-4.4.0-2]) and the identifier
    /// frees [mqtt5 §6]. Those are two different outcomes, and conflating
    /// them would either leak an identifier or resend a dead message.
    fn on_pubrec(&mut self, packet_id: u16, reason_code: PubrecReasonCode) -> Result<()> {
        if reason_code.is_error() {
            self.settle(
                packet_id,
                Stage::AwaitingPubrec,
                Completion::Refused(reason_code),
            );
            return Ok(());
        }
        // A duplicate PUBREC after the PUBREL went out is what the
        // specification expects during recovery, and the PUBREL is sent again
        // because the server is still waiting for one. An identifier this
        // client holds no QoS 2 exchange for gets the answer the protocol has
        // for it, 0x92, and not a success: nothing was released here.
        let reason_code = if self.session.pubrec_received(packet_id)
            || self.session.stage_of(packet_id) == Some(Stage::AwaitingPubcomp)
        {
            PubrelReasonCode::Success
        } else {
            PubrelReasonCode::PacketIdentifierNotFound
        };
        self.respond(Packet::Pubrel(Pubrel {
            packet_id,
            reason_code,
            properties: Properties::new(),
        }))
    }

    /// QoS 2 inbound, part three: answer a PUBREL with a PUBCOMP.
    ///
    /// "Respond to PUBREL with PUBCOMP" ([MQTT-4.3.3-11]) and "after PUBCOMP
    /// treat the identifier as free and a later PUBLISH with it as new"
    /// ([MQTT-4.3.3-12]) [mqtt5 §6]. A PUBREL for an identifier this client
    /// does not hold is answered 0x92, which the specification explicitly
    /// declines to call an error during recovery (3.6.2.1).
    fn on_pubrel(&mut self, packet_id: u16) -> Result<()> {
        let held = self.session.inbound_qos2_released(packet_id);
        let reason_code = if held {
            PubcompReasonCode::Success
        } else {
            PubcompReasonCode::PacketIdentifierNotFound
        };
        self.respond(Packet::Pubcomp(Pubcomp {
            packet_id,
            reason_code,
            properties: Properties::new(),
        }))
    }

    /// Resolves a SUBSCRIBE with its per-filter verdicts, recording the
    /// granted ones in the session's mirror.
    fn on_suback(&mut self, suback: &codec::Suback<'_>) {
        // A SUBACK for an identifier this client is not waiting on a SUBSCRIBE
        // for is dropped and releases nothing: it may be the identifier of an
        // UNSUBSCRIBE, or of a PUBLISH, and freeing it would let the next
        // exchange reuse an identifier that is still in use.
        let Some((subscriptions, identifier, done)) =
            self.subscribe_waiters.remove(&suback.packet_id)
        else {
            return;
        };
        self.session.release_control(suback.packet_id);
        let received = suback.reason_codes.iter().count();
        if received != subscriptions.len() {
            // "The SUBACK MUST contain one reason code for each filter, in
            // the same order" ([MQTT-3.9.3-1], [MQTT-3.9.3-2]). A different
            // count leaves no way to say which filter each code is about, so
            // it is reported rather than guessed at.
            let _ = done.send(Err(Error::AcknowledgementLengthMismatch {
                packet_type: PacketType::Suback,
                sent: subscriptions.len(),
                received,
            }));
            return;
        }
        let codes: Vec<SubackReasonCode> = suback.reason_codes.iter().collect();
        for (subscription, granted) in subscriptions.into_iter().zip(codes.iter().copied()) {
            self.session
                .record_subscription(subscription, identifier, granted);
        }
        let _ = done.send(Ok(codes));
    }

    /// Resolves an UNSUBSCRIBE, forgetting every filter it reported gone.
    fn on_unsuback(&mut self, unsuback: &codec::Unsuback<'_>) {
        let Some((filters, done)) = self.unsubscribe_waiters.remove(&unsuback.packet_id) else {
            return;
        };
        self.session.release_control(unsuback.packet_id);
        let received = unsuback.reason_codes.iter().count();
        if received != filters.len() {
            let _ = done.send(Err(Error::AcknowledgementLengthMismatch {
                packet_type: PacketType::Unsuback,
                sent: filters.len(),
                received,
            }));
            return;
        }
        let codes: Vec<UnsubackReasonCode> = unsuback.reason_codes.iter().collect();
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
    fn send_reauthenticate(&mut self, done: oneshot::Sender<Result<()>>) {
        if let Some(previous) = self.reauthenticating.take() {
            // A second re-authentication while one is running would make two
            // exchanges indistinguishable on the wire - the AUTH packets carry
            // no identifier - so the older waiter is told rather than left
            // pending forever.
            let _ = previous.done.send(Err(Error::UnexpectedPacket {
                packet_type: PacketType::Auth,
            }));
        }
        let data = match self.authenticator.challenge(None) {
            Ok(data) => data,
            Err(error) => {
                let _ = done.send(Err(error));
                return;
            }
        };
        let queued = self.writer.queue(&Packet::Auth(codec::Auth {
            reason_code: AuthReasonCode::ReAuthenticate,
            properties: Properties {
                authentication_method: self.authentication_method.as_deref(),
                authentication_data: Some(&data),
                ..Properties::new()
            },
        }));
        match queued {
            Ok(_) => {
                self.reauthenticating = Some(Reauthentication {
                    done,
                    rounds: 0,
                    deadline: Instant::now() + self.auth_timeout,
                });
            }
            Err(error) => {
                let _ = done.send(Err(error));
            }
        }
    }

    /// Answers a server AUTH during re-authentication.
    fn on_auth(&mut self, auth: &codec::Auth<'_>) -> Result<()> {
        // "Absent a client-named method the Server MUST NOT send AUTH"
        // ([MQTT-4.12.0-6]), and after CONNACK it may only send one inside an
        // exchange this client started ([MQTT-4.12.1-1]). Either way an AUTH
        // arriving outside one is the server breaking the protocol, and it is
        // checked before the method so an unrequested AUTH is not misreported
        // as a mismatch with a method that was never in play.
        let Some(mut reauthentication) = self.reauthenticating.take() else {
            return Err(Error::UnexpectedPacket {
                packet_type: PacketType::Auth,
            });
        };
        // [MQTT-4.12.0-5]: every AUTH of an exchange repeats the same method.
        // A server that omits it is tolerated on the packet that *ends* the
        // exchange, as in the handshake; naming a different one never is.
        if let Some(theirs) = auth.properties.authentication_method
            && Some(theirs) != self.authentication_method.as_deref()
        {
            let _ = reauthentication
                .done
                .send(Err(Error::AuthenticationMethodMismatch));
            return Err(Error::AuthenticationMethodMismatch);
        }
        match auth.reason_code {
            AuthReasonCode::Success => {
                // The exchange is over, and the new credentials are in force
                // only if the server proved it holds the secret they were made
                // from: that proof is the data of this packet.
                match self
                    .authenticator
                    .finish(auth.properties.authentication_data)
                {
                    Ok(()) => {
                        let _ = reauthentication.done.send(Ok(()));
                        Ok(())
                    }
                    Err(error) => self.fail_authentication(reauthentication.done, error),
                }
            }
            AuthReasonCode::ContinueAuthentication => {
                reauthentication.rounds += 1;
                if reauthentication.rounds > MAX_AUTH_ROUNDS {
                    let _ = reauthentication
                        .done
                        .send(Err(Error::Timeout(AUTH_EXCHANGE)));
                    return Err(Error::Timeout(AUTH_EXCHANGE));
                }
                let data = match self
                    .authenticator
                    .challenge(auth.properties.authentication_data)
                {
                    Ok(data) => data,
                    Err(error) => return self.fail_authentication(reauthentication.done, error),
                };
                let packet = Packet::Auth(codec::Auth {
                    reason_code: AuthReasonCode::ContinueAuthentication,
                    properties: Properties {
                        authentication_method: self.authentication_method.as_deref(),
                        authentication_data: Some(&data),
                        ..Properties::new()
                    },
                });
                // Field by field, because `packet` borrows the method.
                let end = self.writer.queue(&packet)?;
                self.unsent_acks.push_back(end);
                self.reauthenticating = Some(reauthentication);
                Ok(())
            }
            // 0x19 is the client's own opening move; a server that sends it is
            // asking the client to re-authenticate, which the protocol does
            // not provide for ([MQTT-4.12.1-1] is one-directional).
            AuthReasonCode::ReAuthenticate => Err(Error::UnexpectedPacket {
                packet_type: PacketType::Auth,
            }),
        }
    }

    /// A re-authentication that failed, because the authenticator could not
    /// answer or would not accept the server.
    ///
    /// "On failure both sides SHOULD send DISCONNECT and MUST close"
    /// ([MQTT-4.12.1-2]): the DISCONNECT is queued and the connection ends
    /// once it has gone, with the cause. The cause goes to the caller who asked
    /// for the re-authentication, and the event stream learns why the
    /// connection ended.
    fn fail_authentication(
        &mut self,
        done: oneshot::Sender<Result<()>>,
        error: Error,
    ) -> Result<()> {
        let reported = Error::AuthenticationFailed(format!("re-authentication failed: {error}"));
        let _ = done.send(Err(error));
        let disconnect = Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::UnspecifiedError,
            properties: Properties::new(),
        });
        match self.writer.queue(&disconnect) {
            Ok(end) => {
                self.closing = Some(Closing {
                    end,
                    done: None,
                    cause: Some(reported),
                });
                Ok(())
            }
            Err(_) => Err(reported),
        }
    }
}

/// Rewrites a PUBLISH to carry the Topic Alias [`OutboundAliases`] decided on.
///
/// Three outcomes, all of 3.3.2.3.4's: the Topic Name in full and no alias,
/// the Topic Name in full **with** the alias that establishes it, or a
/// zero-length Topic Name and the established alias - which is the only place
/// a zero-length Topic Name is legal (3.3.2.1) [mqtt5 §3].
fn apply_alias(publish: &mut Publish<'_>, verdict: Aliased) {
    match verdict {
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
