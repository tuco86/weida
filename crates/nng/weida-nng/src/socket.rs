//! What every socket type shares: the engine, the two selection rules, and
//! the timeouts.
//!
//! A socket type is a **protocol over the engine**. The engine dials,
//! listens, reconnects and holds one [`Pipe`] per connection; what a socket
//! type adds is the two decisions each SP protocol makes — *which pipe* a
//! message goes to (round-robin over the ready, every pipe, one named pipe)
//! and *what header* it carries. Both live in the socket type; every bound
//! lives in the queue.
//!
//! **These sockets are `Send + Sync`, and that is NNG's rule rather than a
//! liberty.** libzmq's sockets may not be touched from two threads and
//! `weida-zmq` spells that as `!Sync`; NNG's may — `nng_socket` is a value
//! an application copies and uses from anywhere, and contexts exist
//! precisely so that concurrent work on one socket is ordinary
//! (`docs/research/nanomsg-nng.md` §2). So a socket here is `Clone` and
//! shares its engine, like the `nng_socket` value it is modelled on.
//!
//! **A timeout is `NNG_ETIMEDOUT`, not `NNG_EAGAIN`.** NNG distinguishes
//! them: `EAGAIN` is what a non-blocking call reports and `ETIMEDOUT` is
//! what an expired `NNG_OPT_SENDTIMEO` or `NNG_OPT_RECVTIMEO` reports (§5,
//! §8). [`within`] is the one place that mapping is made.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::futures::Notified;
use weida_runtime::Exec;
use weida_sp::EndpointType;

use crate::context::Context;
use crate::endpoint::Endpoint;
use crate::engine::{Dialer, Engine, Listener, PipeSet};
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::{Pipe, PipeId, Sent};
use crate::session::SpSession;

/// The half of a socket that is the same for every protocol.
///
/// **Dropping the last handle closes the socket**, after the messages
/// already queued for each peer have been written. `nng_close()` is
/// explicit in C because C has nothing else to say when a handle is
/// forgotten; here a socket nobody can reach any more has no reason to keep
/// its listener bound, its pipes open and its slot in the context's ceiling.
/// Everything this crate runs in the background holds the engine and not
/// this, so this is dropped when the application lets go of it. An explicit
/// [`SocketCore::close`] discards what is queued, as `nng_close()` does.
#[derive(Debug)]
pub struct SocketCore {
    engine: Engine,
    exec: Exec,
    /// Where the round-robin stands. Atomic because an NNG socket may be
    /// used from several threads at once, so two sends may be choosing a
    /// peer at the same moment; the worst a race costs is two messages to
    /// one peer, which is within "round-robin among available peers" (§4).
    cursor: AtomicUsize,
}

impl Drop for SocketCore {
    fn drop(&mut self) {
        self.engine.close_after_flush();
    }
}

impl SocketCore {
    /// Creates the engine and the SP session of a socket speaking
    /// `protocol`.
    ///
    /// There is no socket without a session: one that handed its
    /// connections to a no-op would be an SP implementation that speaks
    /// nothing.
    pub fn new(
        context: &Context,
        protocol: EndpointType,
        options: SocketOptions,
    ) -> Result<SocketCore> {
        let exec = context.exec().clone();
        let engine = Engine::new(context, protocol, options, SpSession::shared())?;
        Ok(SocketCore {
            engine,
            exec,
            cursor: AtomicUsize::new(0),
        })
    }

    /// What this socket speaks.
    pub fn protocol(&self) -> EndpointType {
        self.engine.protocol()
    }

    /// The engine underneath, for a protocol that needs more than these
    /// helpers.
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The reactor, for whatever clock a protocol keeps.
    pub const fn exec(&self) -> &Exec {
        &self.exec
    }

    /// The options this socket runs under.
    pub fn options(&self) -> &SocketOptions {
        self.engine.options()
    }

    /// `nng_dial()`: dials and returns once the peer's protocol header has
    /// arrived.
    pub async fn dial(&self, url: &str) -> Result<Dialer> {
        self.engine.dial(&Endpoint::parse(url)?).await
    }

    /// `nng_dial()` with `NNG_FLAG_NONBLOCK`: returns at once and retries.
    pub fn dial_nonblocking(&self, url: &str) -> Result<Dialer> {
        self.engine.dial_nonblocking(&Endpoint::parse(url)?)
    }

    /// `nng_listen()`: listens and accepts automatically.
    pub async fn listen(&self, url: &str) -> Result<Listener> {
        self.engine.listen(&Endpoint::parse(url)?).await
    }

    /// `nng_dial()` with this endpoint's own options.
    pub async fn dial_with(
        &self,
        url: &str,
        options: crate::options::EndpointOptions,
    ) -> Result<Dialer> {
        self.engine.dial_with(&Endpoint::parse(url)?, options).await
    }

    /// `nng_listen()` with this endpoint's own options.
    pub async fn listen_with(
        &self,
        url: &str,
        options: crate::options::EndpointOptions,
    ) -> Result<Listener> {
        self.engine
            .listen_with(&Endpoint::parse(url)?, options)
            .await
    }

    /// `nng_close()`.
    pub fn close(&self) {
        self.engine.close();
    }

    /// Every pipe this socket may use right now, as one immutable set that
    /// costs a reference count to take.
    pub fn pipes(&self) -> Arc<PipeSet> {
        self.engine.pipes()
    }

    /// One named pipe, if it still exists.
    pub fn pipe(&self, id: PipeId) -> Option<Pipe> {
        self.engine.pipes().get(id).cloned()
    }

    /// Whether a payload of `len` octets that arrived on `pipe` is within
    /// the payload limit of the endpoint that made it.
    ///
    /// For a protocol that takes a header off the message: the transport
    /// admits a message by its declared length with the header in it, and
    /// this is the judgement of what is left. A pipe that has since been
    /// retired judges nothing — what arrived on it was admitted by the
    /// transport, and its limit went with it.
    pub fn payload_fits(&self, pipe: PipeId, len: usize) -> bool {
        self.engine
            .pipes()
            .get(pipe)
            .is_none_or(|pipe| pipe.admits_payload(len))
    }

    /// A wait that ends when a message may be waiting on some pipe, or the
    /// pipes change.
    ///
    /// **Create it before looking** — before [`SocketCore::try_take_any`] —
    /// and await it only if the look found nothing: it sees every arrival
    /// since it was created, so a message that lands between the look and
    /// the await ends the wait instead of being missed by it.
    pub fn message_signal(&self) -> Notified<'_> {
        self.engine.signals().arrived()
    }

    /// A wait that ends when some pipe may have room, or the pipes change.
    /// Created before looking, for the reason [`SocketCore::message_signal`]
    /// gives.
    pub fn room_signal(&self) -> Notified<'_> {
        self.engine.signals().room()
    }

    /// Hands `message` to the next pipe that can accept it **now**,
    /// round-robin.
    ///
    /// "PUSH round-robins among connected pullers that can accept and does
    /// not select an unavailable puller merely to preserve turn order"
    /// (§4). The same rule spreads a REQ's requests over the available
    /// repliers (§4).
    ///
    /// The message comes back when no pipe can accept one at this instant,
    /// so nothing is lost to a race for the last slot.
    pub fn offer_round_robin(&self, message: Message) -> std::result::Result<PipeId, Message> {
        let pipes = self.pipes();
        if pipes.is_empty() {
            return Err(message);
        }
        let start = self.cursor.load(Ordering::Relaxed);
        let mut message = message;
        for offset in 0..pipes.len() {
            let index = (start.wrapping_add(offset)) % pipes.len();
            let pipe = &pipes[index];
            match pipe.outgoing().offer(message) {
                Ok(()) => {
                    self.cursor.store(index.wrapping_add(1), Ordering::Relaxed);
                    return Ok(pipe.id());
                }
                Err(returned) => message = returned,
            }
        }
        Err(message)
    }

    /// The non-blocking form: `NNG_ETIMEDOUT` when nothing can take it,
    /// which is what NNG reports for a send with no eligible peer (§4).
    pub fn try_send_round_robin(&self, message: Message) -> Result<PipeId> {
        self.offer_round_robin(message)
            .map_err(|_| nowhere_to_send())
    }

    /// `NNG_ECLOSED` once this socket is closed.
    ///
    /// Every waiting operation checks it on each turn of its loop, and
    /// [`Engine::close`](crate::Engine::close) wakes the waiters, so a
    /// close ends a parked send or receive **at the close** rather than at
    /// the far end of the operation's timeout. NNG's own rule is that an
    /// operation on a closed socket is `NNG_ECLOSED`, and a socket that
    /// answered `NNG_ETIMEDOUT` a minute later would be telling the caller
    /// the wrong thing about the wrong moment.
    pub fn ensure_open(&self) -> Result<()> {
        if self.engine.is_closed() {
            return Err(Error::ECLOSED("this socket is closed".into()));
        }
        Ok(())
    }

    /// Sends `message` to the next pipe that can accept it, waiting for one
    /// under `NNG_OPT_SENDTIMEO`.
    ///
    /// "With no eligible peer, the send waits or times out" (§4), and a
    /// close ends the wait with `NNG_ECLOSED`.
    pub async fn send_round_robin(&self, message: Message) -> Result<PipeId> {
        let limit = self.options().send_timeout;
        within(&self.exec, limit, async {
            let mut message = message;
            loop {
                // Created before the offer: room that appears after the
                // offer found none and before the await ends the wait.
                let room = self.room_signal();
                self.ensure_open()?;
                match self.offer_round_robin(message) {
                    Ok(id) => return Ok(id),
                    Err(returned) => message = returned,
                }
                room.await;
            }
        })
        .await
    }

    /// Offers `message` to **every** pipe except `exclude`, which is what a
    /// broadcast is: PUB "offers every subscriber connection a copy without
    /// testing subscription prefixes first" and BUS "considers every
    /// directly connected pipe" (§4), the latter minus the pipe a message
    /// arrived on when it is re-broadcast.
    ///
    /// Never waits and never fails: a peer that cannot take its copy loses
    /// it, "while the originating send succeeds without blocking" (§4). The
    /// count returned is how many copies were queued, which is the only
    /// trace a dropped copy leaves.
    ///
    /// A copy is a reference count: every pipe is handed the same body, and
    /// a pipe that refuses costs a lock and no allocation, so a subscriber
    /// that stopped reading costs the publisher nothing it would not spend
    /// on one that is keeping up.
    pub fn send_to_all_except(&self, message: &Message, exclude: Option<PipeId>) -> Broadcast {
        let mut broadcast = Broadcast::default();
        for pipe in self.pipes().iter() {
            if exclude == Some(pipe.id()) {
                continue;
            }
            match pipe.outgoing().offer(message.clone()) {
                Ok(()) => broadcast.queued += 1,
                Err(_) => broadcast.dropped += 1,
            }
        }
        broadcast
    }

    /// Sends to one named pipe: a REP's requester, a PAIR v1 peer.
    ///
    /// Reports `NNG_ECLOSED` when that pipe is gone, which is what a
    /// protocol with a reply to deliver turns into a discard. For a
    /// protocol that drops at a full queue — REP and RESPONDENT among them
    /// — the answer to a pipe that cannot take the message is
    /// [`Sent::Dropped`] and not a wait.
    pub async fn send_to(&self, id: PipeId, message: Message) -> Result<Sent> {
        let Some(pipe) = self.pipe(id) else {
            return Err(Error::ECLOSED(
                format!("{id} is gone; its queues went with it").into(),
            ));
        };
        within(&self.exec, self.options().send_timeout, async {
            pipe.outgoing().send(message).await
        })
        .await
    }

    /// Takes the next message from any pipe, fair-queued, waiting under
    /// `NNG_OPT_RECVTIMEO`.
    ///
    /// "PULL accepts incoming messages; simultaneously ready peers have no
    /// defined order" (§4). The cursor advances past whoever was served, so
    /// one busy peer cannot starve the others.
    pub async fn recv_any(&self) -> Result<(PipeId, Message)> {
        let limit = self.options().recv_timeout;
        within(&self.exec, limit, self.recv_any_untimed()).await
    }

    /// [`SocketCore::recv_any`] without the timeout, for a caller that puts
    /// one timeout around a loop of receives: a receive that is called again
    /// after each message it drops would otherwise restart its clock every
    /// time, and never expire while the peer keeps sending.
    pub async fn recv_any_untimed(&self) -> Result<(PipeId, Message)> {
        loop {
            // Created before the look, so that an arrival between the look
            // and the await ends the wait.
            let arrived = self.message_signal();
            self.ensure_open()?;
            if let Some(taken) = self.try_take_any() {
                return Ok(taken);
            }
            arrived.await;
        }
    }

    /// The non-blocking form: `NNG_EAGAIN` when nothing is queued.
    pub fn try_recv_any(&self) -> Result<(PipeId, Message)> {
        self.try_take_any()
            .ok_or_else(|| Error::EAGAIN("nothing is queued from any pipe".into()))
    }

    /// Takes one message from whichever pipe has one, advancing the
    /// rotation.
    ///
    /// What arrived on pipes which have since been retired is one more
    /// member of the rotation, taking its turn after the last pipe (see
    /// [`Engine::take_arrived`](crate::Engine::take_arrived)): it crossed
    /// the wire before the peer closed, and the close does not un-arrive it.
    /// It is **not served first**. A peer that connects, sends and closes as
    /// fast as the application receives would keep that buffer full for ever,
    /// and every pipe behind it unread; as a member it is one share of the
    /// rotation, as a persistent flooder is.
    ///
    /// Allocates nothing: the pipes are one shared set and a pipe with
    /// nothing queued is passed with a load.
    pub fn try_take_any(&self) -> Option<(PipeId, Message)> {
        let pipes = self.pipes();
        // The leftovers have a slot only while they hold something, so a
        // socket with none rotates over its pipes alone.
        let slots = pipes.len() + usize::from(self.engine.has_arrived());
        if slots == 0 {
            return None;
        }
        let start = self.cursor.load(Ordering::Relaxed);
        for offset in 0..slots {
            let index = (start.wrapping_add(offset)) % slots;
            if index == pipes.len() {
                if let Some(arrived) = self.engine.take_arrived() {
                    // The turn after the leftovers' is the first pipe's.
                    self.cursor.store(0, Ordering::Relaxed);
                    return Some(arrived);
                }
                continue;
            }
            let pipe = &pipes[index];
            if !pipe.incoming().has_messages() {
                continue;
            }
            if let Ok(message) = pipe.incoming().try_recv() {
                self.cursor.store(index.wrapping_add(1), Ordering::Relaxed);
                return Some((pipe.id(), message));
            }
        }
        None
    }
}

/// What one broadcast did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Broadcast {
    /// Copies queued for a peer.
    pub queued: usize,
    /// Copies discarded because a peer could not take one. "Delivery may
    /// reach some, all, or none" (§4).
    pub dropped: usize,
}

/// Gives a socket type the endpoint surface every socket type has.
///
/// `dial`, `listen` and `close` are identical for every protocol — "either
/// role may listen, dial, or do both" (§1) is a property of sockets, not of
/// REQ — so they are written once here rather than in each protocol module.
macro_rules! socket_endpoints {
    ($socket:ty) => {
        impl $socket {
            /// `nng_dial()`: dials `url` and returns once the peer's
            /// protocol header has arrived.
            pub async fn dial(&self, url: &str) -> $crate::error::Result<$crate::engine::Dialer> {
                self.core.dial(url).await
            }

            /// `nng_dial()` with `NNG_FLAG_NONBLOCK`: returns at once, and
            /// keeps retrying with the reconnect backoff.
            pub fn dial_nonblocking(
                &self,
                url: &str,
            ) -> $crate::error::Result<$crate::engine::Dialer> {
                self.core.dial_nonblocking(url)
            }

            /// `nng_listen()`: listens on `url` and accepts automatically.
            /// The returned listener carries the endpoint actually bound.
            pub async fn listen(
                &self,
                url: &str,
            ) -> $crate::error::Result<$crate::engine::Listener> {
                self.core.listen(url).await
            }

            /// `nng_listen()` with this endpoint's own options, which is
            /// where a per-listener `NNG_OPT_RECVMAXSZ` goes — "set
            /// before endpoint creation, ideally per listener/dialer"
            /// (§3), so that the address strangers reach can be bounded
            /// more tightly than one this socket dialled itself (§11).
            pub async fn listen_with(
                &self,
                url: &str,
                options: $crate::options::EndpointOptions,
            ) -> $crate::error::Result<$crate::engine::Listener> {
                self.core.listen_with(url, options).await
            }

            /// `nng_dial()` with this endpoint's own options.
            pub async fn dial_with(
                &self,
                url: &str,
                options: $crate::options::EndpointOptions,
            ) -> $crate::error::Result<$crate::engine::Dialer> {
                self.core.dial_with(url, options).await
            }

            /// `nng_pipe_notify()`: the callback that sees every pipe event.
            pub fn notify(&self, callback: $crate::engine::PipeCallback) {
                self.core.engine().notify(callback);
            }

            /// Pipes this socket holds right now.
            pub fn pipe_count(&self) -> usize {
                self.core.pipes().len()
            }

            /// What this socket knows about each of its pipes.
            pub fn pipe_infos(&self) -> Vec<$crate::engine::PipeInfo> {
                self.core.engine().pipe_infos()
            }

            /// The SP protocol this socket speaks, which is what goes in its
            /// protocol header.
            pub fn protocol(&self) -> ::weida_sp::EndpointType {
                self.core.protocol()
            }

            /// The options this socket runs under.
            pub fn options(&self) -> &$crate::options::SocketOptions {
                self.core.options()
            }

            /// `nng_close()`: stop dialling and accepting, and destroy every
            /// pipe.
            pub fn close(&self) {
                self.core.close();
            }
        }
    };
}

pub(crate) use socket_endpoints;

/// Runs `future` under a wall-clock bound — `NNG_OPT_SENDTIMEO` and
/// `NNG_OPT_RECVTIMEO` — reporting `NNG_ETIMEDOUT` when it expires, which
/// is the code NNG uses for both (§5, §8).
pub async fn within<T>(
    exec: &Exec,
    limit: Option<Duration>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    match limit {
        None => future.await,
        Some(limit) => match exec.within(limit, future).await {
            Some(result) => result,
            None => Err(Error::ETIMEDOUT(
                format!("the operation did not complete within {limit:?}").into(),
            )),
        },
    }
}

/// Why a send found nowhere to go. `NNG_ETIMEDOUT` rather than
/// `NNG_EUNREACHABLE`, because "with no eligible peer, the send waits or
/// times out" (§4) and a caller that asked not to wait has reached the end
/// of that wait immediately.
fn nowhere_to_send() -> Error {
    Error::ETIMEDOUT("no peer can accept a message right now".into())
}

/// A shared socket core, which is what every socket type holds: NNG's
/// sockets are values that may be copied and used from several threads, so
/// ours are clones of one `Arc`.
pub type SharedCore = Arc<SocketCore>;
