//! The blocking facade: weida for a caller with no executor.
//!
//! Behind the `blocking` feature, which is **not** default. Everything else
//! in this crate is `async`, because the transport is, and that is right for
//! an application that already has a reactor. It is wrong for a script, a
//! test harness, a CLI or a thread pool that has none — which is why
//! `weida_zmq::blocking`, `weida_mqtt::blocking` and `weida_nng::blocking`
//! exist. This is the same shape for weida's own patterns — for **every role
//! of every pattern**: Req/Rep, Push/Pull, Pub/Sub, PAIR, SURVEY and BUS —
//! and for the cursor surface of
//! [0023](../../../docs/decisions/0023-completion-is-a-cursor.md) as well:
//! [`Cursors`] is a report read by its caller, [`Reporter`] one written by it.
//! RADIO/DISH of [0034](../../../docs/decisions/0034-late-is-lost.md) is here
//! too ([`Radio`], [`Dish`]). The datagram **flow** is not: it is L0, and this
//! facade mirrors the patterns, not `Peer` and `Acceptor`, so a flow stays on
//! the asynchronous surface — where `Flow::send` never blocks anyway.
//!
//! One design question came with SURVEY rather than a translation, and it is
//! answered in [`Survey`]: a deadline-bounded fan-out of exchanges yields
//! answers as they arrive on the asynchronous surface, which is what a
//! reactor is for, while a synchronous caller has nothing to do between two
//! answers — so the blocking shape collects and returns counts.
//!
//! # It is a wrapper, and nothing else
//!
//! Every method here is a `block_on` around the asynchronous endpoint's own
//! method. **No protocol behaviour is decided twice**: HELLO, negotiation,
//! the dispatch rules, the drain, the limits and the guarantee sets belong to
//! the asynchronous surface, and this module cannot disagree with them
//! because it does not implement them. Two weidas in one crate would be a bug
//! that only shows up in the one the tests do not cover.
//!
//! What it adds is the shape a synchronous caller needs: a payload is a
//! `Vec<u8>` rather than a stream, and every receive takes an explicit
//! ceiling in bytes — [`IncomingTransfer::read_capped`](crate::IncomingTransfer::read_capped)'s
//! argument, passed through rather than defaulted, because a caller who
//! wants the whole payload in memory is the one who knows how much of it
//! there may be ([INVARIANTS.md](../../../docs/INVARIANTS.md)).
//!
//! A caller who needs the streaming surface — a payload too large to hold, a
//! reply written while the request is still arriving — wants the
//! asynchronous API, and every wrapper here hands it out
//! ([`Requester::endpoint`], [`Replier::endpoint`], and so on).
//!
//! # A receive is bounded
//!
//! A transfer is handed over as soon as its header is read; its body is
//! collected after that. Every receive here that collects a body does so under
//! [`RuntimeConfig::receive_timeout`], measured on the runtime's clock from the
//! header to the end of the body, so one peer that stalls cannot hold a
//! single-threaded caller past that bound. Past it the transfer is dropped,
//! which stops its stream, and the call returns [`Error::Expired`]; the next
//! receive is unaffected. A `None` limit collects without a timer.
//!
//! # The reactor is the library's, and blocking a worker is refused
//!
//! [`Runtime::new`] builds a [`crate::Runtime::owned`] reactor: its threads
//! belong to this crate, are named, are sized by
//! [`RuntimeConfig::worker_threads`](crate::RuntimeConfig::worker_threads)
//! and die with the handle. The futures are then driven on the **calling**
//! thread with `futures::executor::block_on`, so two threads may each block
//! on their own endpoint, which is how every synchronous program is written.
//!
//! A caller who is already inside a Tokio runtime must not use this facade:
//! blocking a reactor worker parks the thread that has to drive what the
//! caller is waiting for, and the result is a deadlock with no message. That
//! mistake is **detected rather than documented** — every call checks for an
//! ambient runtime first and fails with [`Error::Runtime`] naming the fix —
//! because a deadlock is the one failure a user cannot debug from the
//! outside.
//!
//! ```no_run
//! use weida::blocking::Runtime;
//! use weida::{RuntimeConfig, Trust};
//!
//! # fn main() -> Result<(), weida::Error> {
//! let runtime = Runtime::new(RuntimeConfig::default())?;
//! let requester = runtime.requester(Trust::by_address());
//! requester.connect("weida://sha256:…@127.0.0.1:7443/echo")?;
//! let reply = requester.request(b"ping", 1024 * 1024)?;
//! println!("{}", String::from_utf8_lossy(&reply));
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use weida_core::{Error, Limits};

use crate::{
    ClientTls, CursorLevel, CursorSet, Drained, Identity, IncomingMeta, ReportMode, Reported,
    RuntimeConfig, ServerTls, TransferMeta, Trust,
};

/// A runtime that owns its reactor, for a synchronous caller.
///
/// The counterpart of [`crate::Runtime::owned`], and the reason that
/// constructor exists: a caller with no executor of its own gets one it never
/// sees, and a caller that *has* one wants [`crate::Runtime::new`] and the
/// asynchronous endpoints instead.
pub struct Runtime {
    inner: crate::Runtime,
}

impl Runtime {
    /// A runtime with its own reactor.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] when `worker_threads` is `0` or the OS refuses the
    /// threads, and when this is called from inside a Tokio runtime — see the
    /// [module documentation](self).
    pub fn new(config: RuntimeConfig) -> Result<Runtime, Error> {
        outside_a_reactor()?;
        Ok(Runtime {
            inner: crate::Runtime::owned(config)?,
        })
    }

    /// The asynchronous runtime underneath, for everything this facade does
    /// not wrap.
    pub fn runtime(&self) -> &crate::Runtime {
        &self.inner
    }

    /// A requester on this runtime.
    pub fn requester(&self, trust: impl Into<ClientTls>) -> Requester {
        Requester {
            endpoint: self.inner.requester(trust),
            deadline: BodyDeadline::of(&self.inner),
        }
    }

    /// A pusher on this runtime.
    pub fn pusher(&self, trust: impl Into<ClientTls>) -> Pusher {
        Pusher {
            endpoint: self.inner.pusher(trust),
        }
    }

    /// A subscriber on this runtime.
    pub fn subscriber(&self, trust: impl Into<ClientTls>) -> Subscriber {
        Subscriber {
            endpoint: self.inner.subscriber(trust),
            deadline: BodyDeadline::of(&self.inner),
        }
    }

    /// A paired endpoint on this runtime, dialling.
    ///
    /// The bound half is [`Binding::pair`]; one type serves both roles here
    /// because one type serves both on the asynchronous surface.
    pub fn pair(&self, trust: impl Into<ClientTls>) -> Paired {
        Paired {
            endpoint: self.inner.pair(trust),
            deadline: BodyDeadline::of(&self.inner),
        }
    }

    /// A surveyor on this runtime.
    pub fn surveyor(&self, trust: impl Into<ClientTls>) -> Surveyor {
        Surveyor {
            endpoint: self.inner.surveyor(trust),
        }
    }

    /// A dish on this runtime.
    pub fn dish(&self, trust: impl Into<ClientTls>) -> Dish {
        Dish {
            endpoint: self.inner.dish(trust),
            deadline: BodyDeadline::of(&self.inner),
        }
    }

    /// Binds a QUIC socket and returns the listener's endpoints.
    ///
    /// The address includes the port the kernel chose, which is how a
    /// wildcard port is read back.
    ///
    /// # Errors
    ///
    /// As [`crate::Listener::bind_quic`]; plus [`Error::Runtime`] from inside
    /// a reactor.
    pub fn bind_quic(
        &self,
        addr: std::net::SocketAddr,
        tls: impl Into<ServerTls>,
    ) -> Result<Binding, Error> {
        outside_a_reactor()?;
        let listener = self.inner.listener();
        let binding = drive(listener.bind_quic(addr, tls))?;
        Ok(Binding {
            listener,
            kind: BindingKind::Quic(binding),
            deadline: BodyDeadline::of(&self.inner),
        })
    }

    /// Binds an in-process bus; a client in this process dials
    /// `weida+inproc://<bus>/<path>`.
    ///
    /// # Errors
    ///
    /// As [`crate::Listener::bind_inproc`]; plus [`Error::Runtime`] from
    /// inside a reactor.
    pub fn bind_inproc(&self, bus: &str) -> Result<Binding, Error> {
        outside_a_reactor()?;
        let listener = self.inner.listener();
        let binding = listener.bind_inproc(bus)?;
        Ok(Binding {
            listener,
            kind: BindingKind::Inproc(binding),
            deadline: BodyDeadline::of(&self.inner),
        })
    }

    /// The runtime's limits, as configured.
    pub fn limits(&self) -> &Limits {
        &self.inner.config().limits
    }

    /// Stops admitting work and waits for finished transfers to reach the
    /// peer's transport, bounded by `deadline`.
    ///
    /// The deadline is mandatory and finite for the reason
    /// [`crate::Runtime::drain`] gives.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] from inside a reactor.
    pub fn drain(self, deadline: Duration) -> Result<Drained, Error> {
        outside_a_reactor()?;
        Ok(drive(self.inner.drain(deadline)))
    }

    /// Closes every connection without waiting for anything in flight.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] from inside a reactor.
    pub fn shutdown(self) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.inner.shutdown());
        Ok(())
    }
}

/// A bound QUIC socket or in-process bus, and the endpoints registered on it.
pub struct Binding {
    listener: crate::Listener,
    kind: BindingKind,
    deadline: BodyDeadline,
}

/// What a [`Binding`] holds bound.
enum BindingKind {
    Quic(crate::Binding),
    Inproc(crate::LocalBinding),
}

impl Binding {
    /// The address the socket is bound to, with the port the kernel chose;
    /// `None` for an in-process bus.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.kind {
            BindingKind::Quic(binding) => Some(binding.local_addr()),
            BindingKind::Inproc(_) => None,
        }
    }

    /// The bus an in-process binding answers on; `None` for QUIC.
    pub fn inproc_bus(&self) -> Option<&str> {
        match &self.kind {
            BindingKind::Quic(_) => None,
            BindingKind::Inproc(binding) => Some(binding.bus()),
        }
    }

    /// Closes every live connection this binding holds from `peer`; see
    /// [`crate::Binding::disconnect`]. Returns how many it closed; an
    /// in-process bus proves no fingerprint, so there it is always 0.
    pub fn disconnect(&self, peer: crate::Fingerprint) -> usize {
        match &self.kind {
            BindingKind::Quic(binding) => binding.disconnect(peer),
            BindingKind::Inproc(_) => 0,
        }
    }

    /// Registers a replier at `path`.
    ///
    /// # Errors
    ///
    /// [`Error::AlreadyRegistered`] when the path is taken,
    /// [`Error::InvalidEndpointPath`] for a malformed one.
    pub fn replier(&self, path: &str) -> Result<Replier, Error> {
        Ok(Replier {
            endpoint: self.listener.replier(path)?,
            deadline: self.deadline.clone(),
        })
    }

    /// Registers a puller at `path`.
    ///
    /// # Errors
    ///
    /// As [`Binding::replier`].
    pub fn puller(&self, path: &str) -> Result<Puller, Error> {
        Ok(Puller {
            endpoint: self.listener.puller(path)?,
            deadline: self.deadline.clone(),
        })
    }

    /// Registers a publisher at `path`.
    ///
    /// # Errors
    ///
    /// As [`Binding::replier`].
    pub fn publisher(&self, path: &str) -> Result<Publisher, Error> {
        Ok(Publisher {
            endpoint: self.listener.publisher(path)?,
        })
    }

    /// Registers a paired endpoint at `path`, bound.
    ///
    /// Exactly one peer, and the **first** one is kept: a stream from any
    /// other connection is refused with `LIMIT_EXCEEDED` while the first
    /// keeps working ([decisions/0005](../../../docs/decisions/0005-refusal-race.md)).
    ///
    /// # Errors
    ///
    /// As [`Binding::replier`].
    pub fn pair(&self, path: &str) -> Result<Paired, Error> {
        Ok(Paired {
            endpoint: self.listener.pair(path)?,
            deadline: self.deadline.clone(),
        })
    }

    /// Registers a respondent at `path`, for surveys.
    ///
    /// A survey question is an exchange, so a respondent's route is a
    /// replier's and its accepted request is the same [`Request`] type.
    ///
    /// # Errors
    ///
    /// As [`Binding::replier`].
    pub fn respondent(&self, path: &str) -> Result<Respondent, Error> {
        Ok(Respondent {
            endpoint: self.listener.respondent(path)?,
            deadline: self.deadline.clone(),
        })
    }

    /// Registers a bus member at `path`, which dials the other members on
    /// `trust`'s terms.
    ///
    /// The one registration that takes both a path and dialling terms,
    /// because a bus member is the one role that is bound and dialling at
    /// once.
    ///
    /// # Errors
    ///
    /// As [`Binding::replier`].
    pub fn bus(&self, path: &str, trust: impl Into<ClientTls>) -> Result<BusMember, Error> {
        Ok(BusMember {
            endpoint: self.listener.bus(path, trust)?,
            deadline: self.deadline.clone(),
        })
    }

    /// Registers a radio at `path`.
    ///
    /// # Errors
    ///
    /// As [`Binding::replier`].
    pub fn radio(&self, path: &str) -> Result<Radio, Error> {
        Ok(Radio {
            endpoint: self.listener.radio(path)?,
        })
    }

    /// The listener underneath, for the registrations this facade does not
    /// wrap.
    pub fn listener(&self) -> &crate::Listener {
        &self.listener
    }
}

/// A received payload with the metadata that described it.
///
/// A `Vec<u8>` rather than a stream, which is the whole difference between
/// this facade and the asynchronous surface: the bytes are already here, and
/// the ceiling the caller passed is what bounds them.
#[derive(Clone, Debug)]
pub struct Message {
    /// The payload, at most the ceiling the caller passed.
    pub payload: Vec<u8>,
    /// What the DATA header said, including the topic and the peer.
    pub meta: IncomingMeta,
}

/// The reader's end of one transfer's report, waited on by its caller.
///
/// Independent of the transfer that ordered it, deliberately: the terminal
/// cursor arrives **after** the payload's FIN, so a handle tied to the send
/// would be gone exactly when the interesting record lands
/// ([0023](../../../docs/decisions/0023-completion-is-a-cursor.md) §4.3b).
/// A cursor is never load-bearing, so nothing here fails for a transport
/// reason: "no more cursors" is the one fact a reader acts on.
pub struct Cursors {
    inner: crate::Cursors,
}

impl Cursors {
    /// The latest set, without waiting.
    pub fn snapshot(&self) -> CursorSet {
        self.inner.snapshot()
    }

    /// The latest offset for `level`, without waiting.
    pub fn offset(&self, level: CursorLevel) -> Option<u64> {
        self.inner.offset(level)
    }

    /// Waits up to `deadline` for the next change.
    ///
    /// The deadline is **mandatory**, for the reason
    /// [`Runtime::drain`]'s is: a parked thread is interrupted by nothing, a
    /// peer that never reports opens no stream, and a connection both sides
    /// keep alive never closes — so an unbounded wait here is a hang with a
    /// rationale. The three outcomes are [`Reported`]'s, and
    /// [`Reported::Waiting`] is not the end of anything: a caller that wants
    /// to keep waiting calls again.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] from inside a reactor, and nothing else: there is
    /// no transport failure a cursor may report.
    pub fn changed(&mut self, deadline: Duration) -> Result<Reported, Error> {
        outside_a_reactor()?;
        Ok(drive(self.inner.changed_within(deadline)))
    }

    /// The asynchronous handle underneath.
    pub fn cursors(&mut self) -> &mut crate::Cursors {
        &mut self.inner
    }
}

/// The writer's end: what a receiver uses to report on a transfer it got.
///
/// Every write is best effort, so nothing here fails for a transport reason
/// either — a peer that reset the cursor stream must not fail the application
/// that is doing the reporting. The granularity is this side's own number and
/// is never negotiated: more often is always allowed, less often never.
pub struct Reporter {
    inner: crate::Reporter,
}

impl Reporter {
    /// The levels the sender ordered, ascending.
    pub fn levels(&self) -> &[CursorLevel] {
        self.inner.levels()
    }

    /// The mode the sender asked for.
    pub fn mode(&self) -> ReportMode {
        self.inner.mode()
    }

    /// Reports that `level` has reached `offset`.
    ///
    /// A level the sender did not order is ignored rather than refused.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] from inside a reactor.
    pub fn report(&mut self, level: CursorLevel, offset: u64) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.inner.report(level, offset))
    }

    /// Flushes the latest offset per level and ends the cursor stream.
    ///
    /// The flush is what makes coalescing lossless: whatever the granularity
    /// suppressed, the last number each level reached is on the wire before
    /// the end.
    ///
    /// # Errors
    ///
    /// As [`Reporter::report`].
    pub fn finish(self) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.inner.finish())
    }
}

/// Drives one of this crate's futures on the **calling** thread.
///
/// Not on the reactor: its threads are driving the connections, and a caller
/// that parked one of them would be waiting for the worker that has to
/// complete what it is waiting for.
fn drive<F: Future>(future: F) -> F::Output {
    futures::executor::block_on(future)
}

/// Refuses the one mistake that produces a deadlock instead of an error.
///
/// `Handle::try_current` is `Ok` exactly on a thread a Tokio runtime is
/// driving, which is where `block_on` must never be called: the future this
/// facade blocks on needs that runtime to make progress. Checked at every
/// entry point rather than documented, because the symptom is a hang with no
/// message and no stack anyone can read.
fn outside_a_reactor() -> Result<(), Error> {
    match tokio::runtime::Handle::try_current() {
        Err(_) => Ok(()),
        Ok(_) => Err(Error::Runtime(
            "weida::blocking blocks the calling thread and was called from inside a Tokio \
             runtime, which would deadlock: use the asynchronous API there, or call this from \
             a thread the runtime does not own"
                .to_owned(),
        )),
    }
}

/// The receive deadline of one blocking wrapper: the runtime's clock and the
/// `RuntimeConfig::receive_timeout` it was built with. Captured when the
/// wrapper is made, so every receive on it is timed by the runtime that owns
/// the connection, whichever thread drives it.
#[derive(Clone)]
struct BodyDeadline {
    exec: crate::runtime::Exec,
    limit: Option<Duration>,
}

impl BodyDeadline {
    fn of(runtime: &crate::Runtime) -> BodyDeadline {
        BodyDeadline {
            exec: runtime.exec().clone(),
            limit: runtime.config().receive_timeout,
        }
    }

    /// Awaits one body under the limit. Past it `body` is dropped, which is
    /// what stops a transfer's stream, and the result is [`Error::Expired`].
    async fn bounded<T>(&self, body: impl Future<Output = Result<T, Error>>) -> Result<T, Error> {
        match self.limit {
            None => body.await,
            Some(limit) => self
                .exec
                .within(limit, body)
                .await
                .unwrap_or(Err(Error::Expired)),
        }
    }
}

/// Writes the endpoint accessor, the connect call and the connection
/// statistics every dialling wrapper has.
macro_rules! dialling {
    ($name:ident, $inner:ty) => {
        impl $name {
            /// Connects to a `weida://`, `weida+unix://`, `weida+pipe://` or
            /// `weida+inproc://` address.
            ///
            /// # Errors
            ///
            /// As the asynchronous `connect`; plus [`Error::Runtime`] from
            /// inside a reactor.
            pub fn connect(&self, url: &str) -> Result<(), Error> {
                outside_a_reactor()?;
                drive(self.endpoint.connect(url))
            }

            /// One record per live connection, labelled by the URL as
            /// dialled; see [`crate::Peer::connection_stats`]. Never blocks.
            pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
                self.endpoint.connection_stats()
            }

            /// The asynchronous endpoint underneath, for the streaming
            /// surface this facade does not wrap.
            pub fn endpoint(&self) -> &$inner {
                &self.endpoint
            }
        }
    };
}

/// A requester: one exchange at a time, whole payloads in and out.
pub struct Requester {
    endpoint: crate::Requester,
    deadline: BodyDeadline,
}

dialling!(Requester, crate::Requester);

impl Requester {
    /// Sends `body` and returns the reply, at most `max_reply_bytes`.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`], [`Error::UnknownEndpoint`], [`Error::NoReply`] and
    /// [`Error::Indeterminate`] as the asynchronous `request`;
    /// [`Error::LimitExceeded`] when the reply exceeds the ceiling;
    /// [`Error::Runtime`] from inside a reactor.
    ///
    /// [`Error::Expired`] when the reply is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn request(&self, body: &[u8], max_reply_bytes: usize) -> Result<Vec<u8>, Error> {
        self.request_with(TransferMeta::default(), body, max_reply_bytes)
    }

    /// [`Requester::request`] with explicit metadata.
    ///
    /// # Errors
    ///
    /// As [`Requester::request`].
    ///
    /// [`Error::Expired`] when the reply is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn request_with(
        &self,
        meta: TransferMeta,
        body: &[u8],
        max_reply_bytes: usize,
    ) -> Result<Vec<u8>, Error> {
        outside_a_reactor()?;
        drive(async {
            let reply = self.endpoint.request_with(meta, body).await?;
            self.deadline.bounded(reply.collect(max_reply_bytes)).await
        })
    }
}

/// A pusher: fire-and-forget transfers, with the receipt awaited.
pub struct Pusher {
    endpoint: crate::Pusher,
}

dialling!(Pusher, crate::Pusher);

impl Pusher {
    /// Sends `body` and waits for the peer's transport to acknowledge it.
    ///
    /// Waiting is the honest synchronous shape: a caller that returns before
    /// the receipt has nothing to check and nothing to retry on. It is the
    /// transport's acknowledgement and **not** an application one
    /// ([GUARANTEES.md](../../../docs/GUARANTEES.md) §1). Because it waits
    /// for the receipt it is a stream in the terms of
    /// [0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md):
    /// with every peer down it waits for the runtime's redial, bounded by
    /// `RuntimeConfig::send_timeout`, and is never held in the outbox.
    ///
    /// # Errors
    ///
    /// [`Error::ConnectionLost`] before the FIN, [`Error::Indeterminate`]
    /// when the connection died with the receipt outstanding,
    /// [`Error::Rejected`] when the peer refused the payload,
    /// [`Error::Runtime`] from inside a reactor.
    pub fn send(&self, body: &[u8]) -> Result<(), Error> {
        self.send_with(
            TransferMeta::default().with_content_len(body.len() as u64),
            body,
        )
    }

    /// [`Pusher::send`] with explicit metadata.
    ///
    /// # Errors
    ///
    /// As [`Pusher::send`].
    pub fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(async {
            let mut transfer = self.endpoint.open(meta).await?;
            transfer.write_all(body).await?;
            transfer.finish()?.delivered().await
        })
    }

    /// [`Pusher::send_with`], keeping the report `meta` ordered.
    ///
    /// `send_with` discards it, exactly as the asynchronous `send_with` does:
    /// this is the same send with the reader's end of the report handed back.
    /// `None` when `meta` ordered no levels — a caller that asked for nothing
    /// gets nothing to read rather than an empty handle.
    ///
    /// **This is the call a fire-and-forget producer wants a verdict from.**
    /// A Push has no reply to carry one, and the receipt this returns after is
    /// the transport's; `Accepted` and everything above it arrive on the
    /// cursor stream, after the FIN
    /// ([0023](../../../docs/decisions/0023-completion-is-a-cursor.md) §4.1).
    ///
    /// # Errors
    ///
    /// As [`Pusher::send`].
    pub fn send_reporting(
        &self,
        meta: TransferMeta,
        body: &[u8],
    ) -> Result<Option<Cursors>, Error> {
        outside_a_reactor()?;
        drive(async {
            let mut transfer = self.endpoint.open(meta).await?;
            // Taken before the payload, because `finish` consumes the
            // transfer and the report outlives it by design.
            let cursors = transfer.cursors();
            transfer.write_all(body).await?;
            transfer.finish()?.delivered().await?;
            Ok(cursors.map(|inner| Cursors { inner }))
        })
    }
}

/// A subscriber: whole published messages, one call each.
pub struct Subscriber {
    endpoint: crate::Subscriber,
    deadline: BodyDeadline,
}

dialling!(Subscriber, crate::Subscriber);

impl Subscriber {
    /// Subscribes to `filter`, the empty filter taking every topic.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] for a filter the grammar of
    /// [`PROTOCOL.md`](../../../docs/PROTOCOL.md) §6.4 rejects,
    /// [`Error::LimitExceeded`] past `max_subscriptions`, [`Error::Runtime`]
    /// from inside a reactor.
    pub fn subscribe(&self, filter: &str) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.endpoint.subscribe(filter))
    }

    /// Withdraws one filter.
    ///
    /// # Errors
    ///
    /// As [`Subscriber::subscribe`].
    pub fn unsubscribe(&self, filter: &str) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.endpoint.unsubscribe(filter))
    }

    /// Waits for the next published message, at most `max_bytes`.
    ///
    /// The topic is on [`Message::meta`]'s `topic`, and a gap — under
    /// `PerProducer` ordering — on its `gap`.
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`] when the subscription is gone,
    /// [`Error::LimitExceeded`] past the ceiling, [`Error::Runtime`] from
    /// inside a reactor.
    ///
    /// [`Error::Expired`] when the body is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn recv(&self, max_bytes: usize) -> Result<Message, Error> {
        outside_a_reactor()?;
        drive(async {
            let transfer = self.endpoint.recv().await?;
            let meta = transfer.meta().clone();
            let payload = self.deadline.bounded(transfer.collect(max_bytes)).await?;
            Ok(Message { payload, meta })
        })
    }
}

/// A replier: accept a request, answer it, whole payloads both ways.
pub struct Replier {
    endpoint: crate::Replier,
    deadline: BodyDeadline,
}

impl Replier {
    /// The endpoint path this replier serves.
    pub fn path(&self) -> &str {
        self.endpoint.path()
    }

    /// Waits for the next request and reads its body, at most `max_bytes`.
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`] when the listener is gone,
    /// [`Error::LimitExceeded`] past the ceiling, [`Error::Runtime`] from
    /// inside a reactor.
    ///
    /// [`Error::Expired`] when the body is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn accept(&self, max_bytes: usize) -> Result<Request, Error> {
        outside_a_reactor()?;
        drive(async {
            let mut request = self.endpoint.accept().await?;
            let meta = request.meta().clone();
            let payload = self
                .deadline
                .bounded(request.take_body().collect(max_bytes))
                .await?;
            Ok(Request {
                request,
                message: Message { payload, meta },
            })
        })
    }

    /// The asynchronous replier underneath.
    pub fn endpoint(&self) -> &crate::Replier {
        &self.endpoint
    }
}

/// An accepted request whose body is already read, and the reply it owes.
///
/// Dropping it without [`Request::reply`] causes ERROR `{NO_REPLY}` on the
/// reply half, exactly as it does on the asynchronous surface: a requester
/// never waits out an idle timeout for an answer nobody will send.
pub struct Request {
    request: crate::IncomingRequest,
    message: Message,
}

impl Request {
    /// The request payload and its metadata.
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// The payload, taken.
    pub fn into_payload(self) -> Vec<u8> {
        self.message.payload
    }

    /// Answers with `body`.
    ///
    /// # Errors
    ///
    /// [`Error::Canceled`] when the requester walked away,
    /// [`Error::ConnectionLost`], [`Error::Runtime`] from inside a reactor.
    pub fn reply(self, body: &[u8]) -> Result<(), Error> {
        self.reply_with(
            TransferMeta::default().with_content_len(body.len() as u64),
            body,
        )
    }

    /// [`Request::reply`] with explicit metadata.
    ///
    /// # Errors
    ///
    /// As [`Request::reply`].
    pub fn reply_with(self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(async {
            let mut out = self.request.reply(meta).await?;
            out.write_all(body).await?;
            out.finish()?;
            Ok(())
        })
    }

    /// Declines the request with `code`, which the requester observes as the
    /// matching error.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] from inside a reactor.
    pub fn refuse(self, code: weida_core::ErrorCode) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.request.refuse(code));
        Ok(())
    }

    /// The asynchronous request underneath, for a streamed reply.
    pub fn into_inner(self) -> crate::IncomingRequest {
        self.request
    }
}

/// A puller: whole transfers, one call each.
pub struct Puller {
    endpoint: crate::Puller,
    deadline: BodyDeadline,
}

impl Puller {
    /// The endpoint path this puller serves.
    pub fn path(&self) -> &str {
        self.endpoint.path()
    }

    /// Waits for the next transfer and reads it, at most `max_bytes`.
    ///
    /// # Errors
    ///
    /// As [`Subscriber::recv`].
    ///
    /// [`Error::Expired`] when the body is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn recv(&self, max_bytes: usize) -> Result<Message, Error> {
        outside_a_reactor()?;
        drive(async {
            let transfer = self.endpoint.recv().await?;
            let meta = transfer.meta().clone();
            let payload = self.deadline.bounded(transfer.collect(max_bytes)).await?;
            Ok(Message { payload, meta })
        })
    }

    /// [`Puller::recv`], with the reporter the sender ordered.
    ///
    /// `None` when the sender ordered nothing, which is the ordinary case:
    /// a reporter is only there when a producer asked to be told.
    ///
    /// **This is the call a staged receiver wants.** `recv` collects the
    /// payload and forgets the transfer, so a receiver that wants to say
    /// "accepted", "stored", "processed" as it gets there needs the handle
    /// that outlives the payload
    /// ([0023](../../../docs/decisions/0023-completion-is-a-cursor.md) §4.1).
    ///
    /// # Errors
    ///
    /// As [`Puller::recv`].
    ///
    /// [`Error::Expired`] when the body is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn recv_reporting(&self, max_bytes: usize) -> Result<(Message, Option<Reporter>), Error> {
        outside_a_reactor()?;
        drive(async {
            let transfer = self.endpoint.recv().await?;
            let meta = transfer.meta().clone();
            // Taken before the payload: `collect` consumes the transfer.
            let reporter = transfer.reporter().map(|inner| Reporter { inner });
            let payload = self.deadline.bounded(transfer.collect(max_bytes)).await?;
            Ok((Message { payload, meta }, reporter))
        })
    }

    /// The asynchronous puller underneath.
    pub fn endpoint(&self) -> &crate::Puller {
        &self.endpoint
    }
}

/// A publisher: fan-out that never waits, exactly as the asynchronous one.
pub struct Publisher {
    endpoint: crate::Publisher,
}

impl Publisher {
    /// The endpoint path this publisher serves.
    pub fn path(&self) -> &str {
        self.endpoint.path()
    }

    /// Fans `payload` out to every matching subscriber, returning how many it
    /// was enqueued for.
    ///
    /// Synchronous on the asynchronous surface too — a publish never waits
    /// for a subscriber — so this is the one call in the facade that does not
    /// block at all, and it is here so that a synchronous caller does not
    /// have to reach for the asynchronous type to publish.
    ///
    /// # Errors
    ///
    /// [`Error::LimitExceeded`] for a payload above
    /// `Limits::subscriber_buffer_bytes`, which a streamed publish
    /// ([`crate::Publisher::open`]) has no ceiling for.
    pub fn publish(&self, topic: &str, payload: impl Into<bytes::Bytes>) -> Result<usize, Error> {
        self.endpoint.publish(topic, payload)
    }

    /// Subscribers currently connected.
    pub fn subscriber_count(&self) -> usize {
        self.endpoint.subscriber_count()
    }

    /// Copies dropped because a subscriber could not take them.
    pub fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }

    /// The asynchronous publisher underneath, for the streaming fan-out.
    pub fn endpoint(&self) -> &crate::Publisher {
        &self.endpoint
    }
}

/// A paired endpoint: one peer, transfers in both directions, whole payloads.
///
/// One type for both roles, as on the asynchronous surface: a bound pair comes
/// from [`Binding::pair`] and a dialling one from [`Runtime::pair`], and what
/// distinguishes them is which of them calls [`Paired::connect`].
pub struct Paired {
    endpoint: crate::Paired,
    deadline: BodyDeadline,
}

dialling!(Paired, crate::Paired);

impl Paired {
    /// The endpoint path this pair uses; empty on a dialling pair that has
    /// not connected yet.
    pub fn path(&self) -> &str {
        self.endpoint.path()
    }

    /// Peers connected: `0` or `1`.
    pub fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Sends `body` and waits for the peer's transport to acknowledge it.
    ///
    /// Waiting is the honest synchronous shape, for [`Pusher::send`]'s reason.
    ///
    /// # Errors
    ///
    /// [`Error::LimitExceeded`] when the peer's pair already has a different
    /// peer — the refusal is per stream, so the connection survives it —
    /// plus everything [`Pusher::send`] reports.
    pub fn send(&self, body: &[u8]) -> Result<(), Error> {
        self.send_with(
            TransferMeta::default().with_content_len(body.len() as u64),
            body,
        )
    }

    /// [`Paired::send`] with explicit metadata.
    ///
    /// # Errors
    ///
    /// As [`Paired::send`].
    pub fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(async {
            let mut transfer = self.endpoint.open(meta).await?;
            transfer.write_all(body).await?;
            transfer.finish()?.delivered().await
        })
    }

    /// [`Paired::send_with`], keeping the report `meta` ordered.
    ///
    /// As [`Pusher::send_reporting`]: a pair carries one-way transfers in
    /// each direction, so a verdict from the far end is a cursor here too.
    ///
    /// # Errors
    ///
    /// As [`Paired::send`].
    pub fn send_reporting(
        &self,
        meta: TransferMeta,
        body: &[u8],
    ) -> Result<Option<Cursors>, Error> {
        outside_a_reactor()?;
        drive(async {
            let mut transfer = self.endpoint.open(meta).await?;
            let cursors = transfer.cursors();
            transfer.write_all(body).await?;
            transfer.finish()?.delivered().await?;
            Ok(cursors.map(|inner| Cursors { inner }))
        })
    }

    /// Waits for the next transfer from the peer, at most `max_bytes`.
    ///
    /// # Errors
    ///
    /// As [`Puller::recv`].
    ///
    /// [`Error::Expired`] when the body is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn recv(&self, max_bytes: usize) -> Result<Message, Error> {
        outside_a_reactor()?;
        drive(async {
            let transfer = self.endpoint.recv().await?;
            let meta = transfer.meta().clone();
            let payload = self.deadline.bounded(transfer.collect(max_bytes)).await?;
            Ok(Message { payload, meta })
        })
    }

    /// [`Paired::recv`], with the reporter the peer ordered.
    ///
    /// # Errors
    ///
    /// As [`Paired::recv`].
    ///
    /// [`Error::Expired`] when the body is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn recv_reporting(&self, max_bytes: usize) -> Result<(Message, Option<Reporter>), Error> {
        outside_a_reactor()?;
        drive(async {
            let transfer = self.endpoint.recv().await?;
            let meta = transfer.meta().clone();
            let reporter = transfer.reporter().map(|inner| Reporter { inner });
            let payload = self.deadline.bounded(transfer.collect(max_bytes)).await?;
            Ok((Message { payload, meta }, reporter))
        })
    }
}

/// What one survey collected before its deadline.
///
/// **The deadline is why this is a value rather than an iterator.** On the
/// asynchronous surface a [`crate::SurveyRun`] yields answers as they arrive,
/// which is what a reactor is for; a synchronous caller has nothing to do
/// between two answers, so the blocking shape is "ask, wait out the deadline,
/// here is what came" — and the counts that make the silence readable come
/// with it rather than needing a second call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Survey {
    /// The answers, in arrival order.
    pub replies: Vec<Vec<u8>>,
    /// Respondents the question went to.
    pub asked: usize,
    /// Respondents that answered with an error — a refusal, a reset, a reply
    /// past the ceiling. Counted rather than returned, because a survey's
    /// result is the answers and its failures are a number
    /// ([PATTERNS.md](../../../docs/PATTERNS.md) §5).
    pub failed: usize,
    /// Answers that arrived after the deadline: dropped, and counted.
    pub late: u64,
}

impl Survey {
    /// Respondents that said nothing at all before the deadline.
    ///
    /// `asked` minus what answered, one way or the other. "Nobody answered" is
    /// an answer, so this is a number and never an error.
    pub fn silent(&self) -> usize {
        self.asked
            .saturating_sub(self.replies.len())
            .saturating_sub(self.failed)
    }
}

/// A surveyor: one question to every respondent, bounded by a deadline.
pub struct Surveyor {
    endpoint: crate::Surveyor,
}

dialling!(Surveyor, crate::Surveyor);

impl Surveyor {
    /// Respondents currently connected.
    pub fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Asks every connected respondent and collects what arrives within
    /// `deadline`, each answer at most `max_reply_bytes`.
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`] when no respondent is connected — an empty
    /// survey is **not** an error, but having nobody to ask is — plus
    /// [`Error::Runtime`] from inside a reactor.
    pub fn survey(
        &self,
        body: &[u8],
        deadline: Duration,
        max_reply_bytes: usize,
    ) -> Result<Survey, Error> {
        self.survey_with(TransferMeta::default(), body, deadline, max_reply_bytes)
    }

    /// [`Surveyor::survey`] with explicit metadata.
    ///
    /// # Errors
    ///
    /// As [`Surveyor::survey`].
    pub fn survey_with(
        &self,
        meta: TransferMeta,
        body: &[u8],
        deadline: Duration,
        max_reply_bytes: usize,
    ) -> Result<Survey, Error> {
        outside_a_reactor()?;
        drive(async {
            let mut run = self.endpoint.survey_with(meta, body, deadline).await?;
            let asked = run.respondents();
            let mut replies = Vec::new();
            let mut failed = 0usize;
            while let Some(answer) = run.next(max_reply_bytes).await {
                match answer {
                    Ok(reply) => replies.push(reply),
                    Err(_) => failed += 1,
                }
            }
            Ok(Survey {
                replies,
                asked,
                failed,
                late: run.late(),
            })
        })
    }
}

/// A respondent: accept a survey question, answer it, whole payloads both
/// ways.
///
/// A question is an exchange, so this is a [`Replier`] in every respect and
/// hands out the same [`Request`].
pub struct Respondent {
    endpoint: crate::Respondent,
    deadline: BodyDeadline,
}

impl Respondent {
    /// The endpoint path this respondent serves.
    pub fn path(&self) -> &str {
        self.endpoint.path()
    }

    /// Waits for the next question and reads it, at most `max_bytes`.
    ///
    /// # Errors
    ///
    /// As [`Replier::accept`].
    ///
    /// [`Error::Expired`] when the body is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn accept(&self, max_bytes: usize) -> Result<Request, Error> {
        outside_a_reactor()?;
        drive(async {
            let mut request = self.endpoint.accept().await?;
            let meta = request.meta().clone();
            let payload = self
                .deadline
                .bounded(request.take_body().collect(max_bytes))
                .await?;
            Ok(Request {
                request,
                message: Message { payload, meta },
            })
        })
    }

    /// The asynchronous respondent underneath.
    pub fn endpoint(&self) -> &crate::Respondent {
        &self.endpoint
    }
}

/// A bus member: bound and dialling at once, every message to every other
/// member.
pub struct BusMember {
    endpoint: crate::BusMember,
    deadline: BodyDeadline,
}

dialling!(BusMember, crate::BusMember);

impl BusMember {
    /// The endpoint path this member answers on.
    pub fn path(&self) -> &str {
        self.endpoint.path()
    }

    /// Sends `body` to every **other** member, returning how many it was
    /// enqueued for.
    ///
    /// Never to the sender itself: structurally, because a send writes to the
    /// members this one dialled. Best effort per member with counted drops,
    /// exactly as a fan-out, so this does not wait for a receipt — there is no
    /// single peer to get one from.
    ///
    /// # Errors
    ///
    /// [`Error::LimitExceeded`] for a payload above
    /// `Limits::subscriber_buffer_bytes`, [`Error::Runtime`] from inside a
    /// reactor.
    pub fn send(&self, body: &[u8]) -> Result<usize, Error> {
        outside_a_reactor()?;
        drive(self.endpoint.send(body))
    }

    /// [`BusMember::send`] with explicit metadata.
    ///
    /// # Errors
    ///
    /// As [`BusMember::send`].
    pub fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<usize, Error> {
        outside_a_reactor()?;
        drive(self.endpoint.send_with(meta, body))
    }

    /// Waits for the next message from another member, at most `max_bytes`.
    ///
    /// # Errors
    ///
    /// As [`Puller::recv`].
    ///
    /// [`Error::Expired`] when the body is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn recv(&self, max_bytes: usize) -> Result<Message, Error> {
        outside_a_reactor()?;
        drive(async {
            let transfer = self.endpoint.recv().await?;
            let meta = transfer.meta().clone();
            let payload = self.deadline.bounded(transfer.collect(max_bytes)).await?;
            Ok(Message { payload, meta })
        })
    }

    /// Members this one has dialled.
    pub fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Copies that never reached a member.
    pub fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }
}

/// A radio: segments to every joined dish, dropped rather than waited for
/// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
///
/// Nothing here blocks: a radio never waits for a dish on the asynchronous
/// surface either, so these are direct calls rather than `block_on`s.
pub struct Radio {
    endpoint: crate::Radio,
}

impl Radio {
    /// Opens the next segment on `topic` under `terms`, superseding the
    /// previous one's copies there as [`crate::Radio::segment`] describes.
    ///
    /// # Errors
    ///
    /// As [`crate::Radio::segment`].
    pub fn segment(&self, topic: &str, terms: crate::SegmentTerms) -> Result<Segment, Error> {
        Ok(Segment {
            inner: self.endpoint.segment(topic, terms)?,
        })
    }

    /// Sends a one-packet segment on `topic`; returns how many dishes it was
    /// handed to.
    ///
    /// # Errors
    ///
    /// As [`crate::Radio::datagram`].
    pub fn datagram(&self, topic: &str, payload: &[u8]) -> Result<usize, Error> {
        self.endpoint.datagram(topic, payload)
    }

    /// Dishes currently joined.
    pub fn dish_count(&self) -> usize {
        self.endpoint.dish_count()
    }

    /// Copies dropped, over topics and causes.
    pub fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }

    /// Every topic that lost a copy, with its counts by cause.
    pub fn drops(&self) -> Vec<crate::TopicDrops> {
        self.endpoint.drops()
    }

    /// What each joined dish connection lost, summed over topics; see
    /// [`crate::Radio::dish_drops`].
    pub fn dish_drops(&self) -> Vec<crate::DishDrops> {
        self.endpoint.dish_drops()
    }

    /// Decides which dish may join which filter; see
    /// [`crate::Radio::with_admission`]. A refused join is silence.
    pub fn with_admission(
        self,
        admit: impl Fn(&crate::Join<'_>) -> bool + Send + Sync + 'static,
    ) -> Radio {
        Radio {
            endpoint: self.endpoint.with_admission(admit),
        }
    }

    /// Withdraws `filter` from every connection of `peer`; see
    /// [`crate::Radio::evict`]. Returns how many joins went.
    pub fn evict(&self, peer: &crate::PeerIdentity, filter: &str) -> usize {
        self.endpoint.evict(peer, filter)
    }

    /// The asynchronous radio underneath.
    pub fn endpoint(&self) -> &crate::Radio {
        &self.endpoint
    }
}

/// One segment, open on every dish joined when it opened.
pub struct Segment {
    inner: crate::Segment,
}

impl Segment {
    /// Hands `chunk` to layer 0 of every copy still open; returns how many
    /// copies took it.
    ///
    /// # Errors
    ///
    /// As [`crate::Segment::write`].
    pub fn write(&mut self, chunk: &[u8]) -> Result<usize, Error> {
        self.inner.write(bytes::Bytes::copy_from_slice(chunk))
    }

    /// Hands `chunk` to layer `layer` of every copy that wants it; returns
    /// how many copies took it.
    ///
    /// # Errors
    ///
    /// As [`crate::Segment::write_layer`]: a layer of 16 or above, a chunk
    /// above 4 GiB, or a layer already finished.
    pub fn write_layer(&mut self, layer: u8, chunk: &[u8]) -> Result<usize, Error> {
        self.inner
            .write_layer(layer, bytes::Bytes::copy_from_slice(chunk))
    }

    /// Ends layer `layer` on every copy that has it open; see
    /// [`crate::Segment::finish_layer`].
    pub fn finish_layer(&mut self, layer: u8) {
        self.inner.finish_layer(layer);
    }

    /// Ends every layer still open; returns how many copies still hold
    /// layer 0.
    pub fn finish(self) -> usize {
        self.inner.finish()
    }
}

/// What a dish receives, whole.
#[derive(Clone, Debug)]
pub enum Delivered {
    /// A stream segment; its topic and number are on the message's `meta`.
    /// Boxed: a message's metadata is several times a datagram's size.
    Segment(Box<Message>),
    /// A datagram segment.
    Datagram {
        /// The topic it was sent on.
        topic: String,
        /// Its segment number on that topic.
        segment: u64,
        /// The payload.
        payload: Vec<u8>,
    },
}

/// A dish: joins topics on a radio and receives the newest segment of each.
pub struct Dish {
    endpoint: crate::Dish,
    deadline: BodyDeadline,
}

dialling!(Dish, crate::Dish);

impl Dish {
    /// Joins every topic `filter` matches, under `terms` (a latency budget
    /// and a layer cap).
    ///
    /// # Errors
    ///
    /// As [`crate::Dish::join`]; plus [`Error::Runtime`] from inside a
    /// reactor.
    pub fn join(&self, filter: &str, terms: crate::JoinTerms) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.endpoint.join(filter, terms))
    }

    /// Leaves a filter.
    ///
    /// # Errors
    ///
    /// As [`Dish::join`].
    pub fn leave(&self, filter: &str) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.endpoint.leave(filter))
    }

    /// Waits for the next segment; a stream segment is read whole, at most
    /// `max_bytes`.
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`] when the dish is gone,
    /// [`Error::LimitExceeded`] past the ceiling, [`Error::Canceled`] for a
    /// segment the radio reset while it was being read, [`Error::Runtime`]
    /// from inside a reactor.
    ///
    /// [`Error::Expired`] when the body is not complete within
    /// `RuntimeConfig::receive_timeout`.
    pub fn recv(&self, max_bytes: usize) -> Result<Delivered, Error> {
        outside_a_reactor()?;
        drive(async {
            match self.endpoint.recv().await? {
                crate::Received::Segment(transfer) => {
                    let meta = transfer.meta().clone();
                    let payload = self.deadline.bounded(transfer.collect(max_bytes)).await?;
                    Ok(Delivered::Segment(Box::new(Message { payload, meta })))
                }
                crate::Received::Datagram {
                    topic,
                    segment,
                    payload,
                } => Ok(Delivered::Datagram {
                    topic: topic.to_string(),
                    segment,
                    payload: payload.to_vec(),
                }),
            }
        })
    }

    /// Connected radios.
    pub fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }
}

/// A self-signed identity, for a synchronous caller that binds.
///
/// Re-exported so that a program using this facade needs no other import to
/// serve: the identity is the whole of a weida server's configuration.
#[cfg(feature = "generate")]
pub fn identity() -> Result<Identity, Error> {
    Identity::generate()
}

/// Trust by what the address names, which is the common client case.
pub fn trust_by_address() -> Trust {
    Trust::by_address()
}
