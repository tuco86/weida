//! What every socket type shares: the engine, the routing decisions and the
//! two thread rules.
//!
//! A socket type is a **pattern over the engine**. The engine binds,
//! connects, reconnects and holds one [`Pipe`] per peer; what a socket type
//! adds is the two decisions the pattern RFCs specify per type — *which pipe*
//! a message goes to (round-robin, fan-out, by routing id, the last peer) and
//! *what envelope* it carries. Both live in the socket type, and every bound
//! lives in the queue (`docs/research/zeromq.md` §4.1, §5).
//!
//! **`Send + !Sync`, and the type system says so.** libzmq: "Applications
//! MUST NOT use a *not* thread safe socket from multiple threads under any
//! circumstances. Doing so results in undefined behaviour" (§2). Every socket
//! type here holds a [`SocketCore`], which holds a `PhantomData<Cell<()>>`:
//! it may be moved between threads and it may not be shared between them, and
//! that is a compile error rather than a paragraph. The draft thread-safe
//! family, when it lands, is the family that *is* `Sync` and refuses
//! multipart — which is what the specification says it is.
//!
//! **No socket without a session.** [`SocketCore::new`] builds the
//! [`ZmtpSession`] for the socket type it is given, so a socket that speaks
//! nothing cannot be constructed
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4).

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::futures::Notified;
use weida_runtime::Exec;
use weida_zmtp::SocketType;

use crate::context::Context;
use crate::endpoint::Endpoint;
use crate::engine::{Discarded, Engine, Peer, PeerId, Session};
use crate::error::{Error, Result};
use crate::message::Multipart;
use crate::options::SocketOptions;
use crate::pipe::{MuteAction, Pipe, Sent, Weighed};
use crate::session::ZmtpSession;

/// What a round-robin send did, and to whom.
///
/// The peer matters to a pattern with a memory: REQ "SHALL accept an
/// incoming message only from the last peer that it sent a request to", and
/// that is the peer this reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivered {
    /// Which peer took it, or `None` when the socket type dropped it.
    pub peer: Option<PeerId>,
    /// Queued, or discarded at the bound.
    pub sent: Sent,
}

/// Every public socket type, once, in the order the patterns were built.
///
/// `$apply` is a macro invoked with the whole list, which is how a caller
/// says something about all of them without naming them again — the
/// compile-fail harness of `tests/thread_rule.rs` makes its two assertions
/// per socket type this way.
///
/// **The list is load-bearing, not documentation.** Every socket type takes
/// its endpoint surface from the crate's `socket_endpoints!`, and that macro
/// refuses to expand for a type this list does not name, so a new socket
/// type cannot arrive without the thread rule being asserted for it.
#[macro_export]
macro_rules! for_each_socket_type {
    ($apply:ident) => {
        $apply! {
            ReqSocket, RepSocket, DealerSocket, RouterSocket, PubSocket, SubSocket, XPubSocket,
            XSubSocket, PushSocket, PullSocket, PairSocket
        }
    };
}

/// Implemented for exactly the types [`for_each_socket_type`] names.
///
/// Nothing is ever called through it: it exists so that the list and the
/// socket types cannot drift apart silently.
pub trait Enumerated {}

macro_rules! mark_enumerated {
    ($($socket:ident),+ $(,)?) => {
        $(impl $crate::socket::Enumerated for $crate::$socket {})+
    };
}

crate::for_each_socket_type!(mark_enumerated);

/// Gives a socket type the endpoint surface every socket type has.
///
/// `bind`, `connect`, `unbind`, `disconnect`, `ZMQ_LAST_ENDPOINT` and
/// `zmq_close` are identical for every pattern — "a socket may connect to
/// many endpoints and bind many at once" is a property of sockets, not of
/// REQ. Written once here so that five socket-type modules do not write it
/// five times, and so that adding one cannot forget a method.
///
/// It also refuses to expand for a socket type [`for_each_socket_type`] does
/// not name: a socket without endpoints is not a socket anybody can use, so
/// this is where a new one is made to join the list the thread-rule harness
/// reads.
macro_rules! socket_endpoints {
    ($socket:ty) => {
        socket_endpoints!($socket, no_connect);

        impl $socket {
            /// Connects an endpoint. Returns as soon as the peer exists, like
            /// `zmq_connect`: the queue is there and the dialling happens
            /// behind it.
            pub fn connect(&self, endpoint: &str) -> $crate::error::Result<()> {
                self.core.connect(endpoint)
            }
        }
    };
    // For the one socket type whose `connect` is its own — PAIR takes at
    // most one peer, so its second call is `EINVAL` — while everything else
    // about its endpoints is every socket's.
    ($socket:ty, no_connect) => {
        const _: () = {
            const fn enumerated<T: $crate::socket::Enumerated>() {}
            enumerated::<$socket>()
        };

        impl $socket {
            /// Binds an endpoint and returns the one actually bound, which is
            /// what `ZMQ_LAST_ENDPOINT` reports and the only way to learn a
            /// wildcard port.
            ///
            /// The returned future is `Send` and borrows nothing — see
            /// [`Shared::bind`][$crate::socket::Shared::bind] — so
            /// `socket.bind(endpoint).await` works on a multi-thread
            /// executor as well as under `block_on`.
            pub fn bind(
                &self,
                endpoint: &str,
            ) -> impl ::std::future::Future<
                Output = $crate::error::Result<$crate::endpoint::Endpoint>,
            > + Send
            + 'static {
                self.core.bind(endpoint)
            }

            /// Stops accepting on an endpoint. Peers already accepted there
            /// keep their connections.
            pub fn unbind(&self, endpoint: &str) -> $crate::error::Result<()> {
                self.core.unbind(endpoint)
            }

            /// Disconnects an endpoint, destroying its queue and reporting
            /// what it held.
            pub fn disconnect(
                &self,
                endpoint: &str,
            ) -> $crate::error::Result<$crate::engine::Discarded> {
                self.core.disconnect(endpoint)
            }

            /// `ZMQ_LAST_ENDPOINT`: the last endpoint bound or connected.
            pub fn last_endpoint(&self) -> Option<$crate::endpoint::Endpoint> {
                self.core.last_endpoint()
            }

            /// Peers this socket may send to right now, honouring
            /// `ZMQ_IMMEDIATE`.
            pub fn peer_count(&self) -> usize {
                self.core.peers().len()
            }

            /// Every connection this socket has, connected or not, as a
            /// snapshot.
            ///
            /// Named for what it is rather than `peers`, which on a ROUTER is
            /// the routing table. A snapshot and not a handle: "application
            /// code cannot manipulate individual underlying connections"
            /// (`docs/research/zeromq.md` §2) — but it can read what the
            /// library knows about one, which for a local peer includes the
            /// credentials the kernel attributed to it
            /// ([`Peer::credentials`][$crate::engine::Peer::credentials]).
            /// A copy the caller owns, taken from the engine's shared
            /// snapshot; it is made on request and never on the message path.
            pub fn connections(&self) -> Vec<$crate::engine::Peer> {
                self.core.engine().peers().to_vec()
            }

            /// The socket type this socket announces in its `READY`.
            pub fn socket_type(&self) -> ::weida_zmtp::SocketType {
                self.core.socket_type()
            }

            /// `zmq_socket_monitor`: this socket's connection lifecycle as a
            /// typed stream, replacing any monitor installed before.
            ///
            /// [`crate::monitor::serve_pair`] renders the same stream onto a
            /// PAIR socket in libzmq's two-frame `inproc://` form, which is
            /// what the zguide's Espresso recipe reads.
            pub fn monitor(
                &self,
                events: $crate::monitor::MonitorEvents,
            ) -> $crate::monitor::Monitor {
                self.core.engine().monitor(events)
            }

            /// `zmq_close`: stops accepting and dialling, and destroys every
            /// queue.
            pub fn close(&self) {
                self.core.close();
            }
        }
    };
}

pub(crate) use socket_endpoints;

/// The half of a socket that is the same for every pattern.
///
/// Two layers, and the split is what lets a socket be shared **between its
/// own two halves** and nothing else. [`Shared`] underneath is `Sync`: the
/// engine, the reactor and two atomic cursors, and every send and receive
/// is a method on it taking `&self`, so their futures are `Send` and a
/// multi-thread executor may drive them. `SocketCore` is a `!Sync` handle
/// onto it — the `PhantomData<Cell<()>>` is libzmq's thread rule as a type,
/// a compile error rather than a paragraph — and it dereferences to
/// [`Shared`], so a pattern writes `self.core.recv_fair()` and gets a future
/// borrowing only the `Sync` inside.
///
/// `Clone` is what a socket's two halves are made of
/// ([`DealerSocket::split`][crate::DealerSocket::split]): each half holds a
/// handle, the engine stays alive while either does, and it closes when the
/// last one goes. The cursors are shared too, which is right: a rotation is
/// a property of the socket, not of the half that happens to advance it.
#[derive(Clone)]
pub struct SocketCore {
    shared: Shared,
    /// libzmq's thread rule, as a type: `Cell` is `Send` and not `Sync`, so
    /// this core — and every socket holding one — may cross threads and may
    /// not be shared across them.
    not_sync: PhantomData<Cell<()>>,
}

impl std::ops::Deref for SocketCore {
    type Target = Shared;

    fn deref(&self) -> &Shared {
        &self.shared
    }
}

/// The `Sync` inside of a [`SocketCore`]: what both halves of a socket use.
///
/// Not a socket — it has no thread rule — and not constructible outside
/// this module; it is reached through a `SocketCore`'s `Deref` and exists
/// so that the async methods below borrow something `Send`.
#[derive(Clone)]
pub struct Shared {
    engine: Engine,
    exec: Exec,
    socket_type: SocketType,
    /// Where the outbound round-robin stands. Relaxed atomics: a socket is
    /// used from one task per direction, so this is never contended, and an
    /// atomic is what lets the method take `&self`.
    send_cursor: Arc<AtomicUsize>,
    /// Where the fair-queued receive stands. Its own cursor, so that a
    /// receiving half and a sending half do not disturb each other's
    /// rotation.
    recv_cursor: Arc<AtomicUsize>,
}

impl SocketCore {
    /// Creates the engine for a socket of `socket_type`, with its ZMTP
    /// session.
    pub fn new(
        context: &Context,
        socket_type: SocketType,
        options: SocketOptions,
    ) -> Result<SocketCore> {
        // Type-blind validation happens in `Engine::new`; this is the half
        // only a socket type can judge — an option that means nothing for
        // this pattern is refused rather than ignored (0013 §4.4 item 4).
        options.validate_for(socket_type)?;
        let session: Arc<dyn Session> = Arc::new(ZmtpSession::new(socket_type));
        let engine = Engine::new(context, options, session)?;
        Ok(SocketCore::over(context, engine, socket_type))
    }

    /// Creates the engine for a socket of `socket_type` with a session the
    /// socket type built itself.
    ///
    /// A subscribing socket needs this: its session carries the
    /// subscription set, so that a reconnect re-sends it without the socket
    /// being told. The session is still a [`ZmtpSession`] — no socket can be
    /// built without one.
    pub fn with_session(
        context: &Context,
        socket_type: SocketType,
        options: SocketOptions,
        session: Arc<ZmtpSession>,
    ) -> Result<SocketCore> {
        options.validate_for(socket_type)?;
        let engine = Engine::new(context, options, session)?;
        Ok(SocketCore::over(context, engine, socket_type))
    }

    fn over(context: &Context, engine: Engine, socket_type: SocketType) -> SocketCore {
        SocketCore {
            shared: Shared {
                exec: context.exec().clone(),
                engine,
                socket_type,
                send_cursor: Arc::new(AtomicUsize::new(0)),
                recv_cursor: Arc::new(AtomicUsize::new(0)),
            },
            not_sync: PhantomData,
        }
    }
}

impl Shared {
    /// The socket type this socket announces in its `READY`.
    pub const fn socket_type(&self) -> SocketType {
        self.socket_type
    }

    /// The engine underneath, for a pattern that needs more than the helpers
    /// here.
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The reactor, for `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO` and any clock a pattern
    /// keeps.
    pub const fn exec(&self) -> &Exec {
        &self.exec
    }

    /// The options this socket runs under.
    pub fn options(&self) -> &SocketOptions {
        self.engine.options()
    }

    /// Binds an endpoint and returns the one actually bound
    /// (`ZMQ_LAST_ENDPOINT`).
    ///
    /// Not an `async fn`, and that is load-bearing: an `async fn(&self)` on a
    /// `!Sync` socket produces a future holding `&SocketCore`, which is not
    /// `Send` and therefore cannot be driven on a multi-thread executor at
    /// all. The future returned here owns an [`Engine`] handle — which *is*
    /// `Sync` — and the parsed endpoint, so it is `Send` like every other
    /// future this crate hands out, and `socket.bind(endpoint).await` reads
    /// exactly as before.
    pub fn bind(&self, endpoint: &str) -> impl Future<Output = Result<Endpoint>> + Send + 'static {
        let engine = self.engine.clone();
        let parsed = Endpoint::parse(endpoint);
        async move { engine.bind(&parsed?).await }
    }

    /// Connects an endpoint, asynchronously, like `zmq_connect`.
    pub fn connect(&self, endpoint: &str) -> Result<()> {
        self.engine.connect(&Endpoint::parse(endpoint)?).map(|_| ())
    }

    /// Stops accepting on an endpoint.
    pub fn unbind(&self, endpoint: &str) -> Result<()> {
        self.engine.unbind(&Endpoint::parse(endpoint)?)
    }

    /// Disconnects an endpoint, destroying its pipe and reporting what it
    /// held.
    pub fn disconnect(&self, endpoint: &str) -> Result<Discarded> {
        self.engine.disconnect(&Endpoint::parse(endpoint)?)
    }

    /// The last endpoint bound or connected.
    pub fn last_endpoint(&self) -> Option<Endpoint> {
        self.engine.last_endpoint()
    }

    /// Peers this socket may send to right now, honouring `ZMQ_IMMEDIATE`, as
    /// the engine's shared snapshot: taking it is a lock and a
    /// reference-count bump, however many peers there are.
    pub fn peers(&self) -> Arc<[Peer]> {
        self.engine.outgoing_peers()
    }

    /// Closes the socket: stops accepting and dialling, destroys every pipe.
    pub fn close(&self) {
        self.engine.close();
    }

    /// Sends to the next peer with room, round-robin, honouring the socket
    /// type's mute action when none has.
    ///
    /// "A peer is available only when it has an outgoing queue that is not
    /// full", and the rotation is over the available ones (28/REQREP's DEALER
    /// rule, which REQ and PUSH share). With [`MuteAction::Block`] this waits
    /// for room on *whichever* peer frees first, and for a peer at all when
    /// there is none — "SHALL block on sending… when it has no connected
    /// peers".
    ///
    /// "Not full" is judged for **this message**: a queue with room for a
    /// small one and not for this one is full for it. A socket that promises
    /// never to discard cannot afford a check that says yes to a message the
    /// queue then refuses, so the queue decides once, under its own lock, and
    /// hands the message back when it does not fit.
    pub async fn send_round_robin(&self, message: Multipart) -> Result<Delivered> {
        let mute = self.options().pipe.outgoing.mute;
        let mut message = Weighed::new(message);
        // A send that finds room never creates a bell: the look below it
        // repeats, under a bell, only for a send that is about to wait.
        let peers = self.peers();
        match self.offer_round_robin(&peers, message) {
            Ok(peer) => {
                return Ok(Delivered {
                    peer: Some(peer),
                    sent: Sent::Queued,
                });
            }
            Err(refused) => message = refused,
        }
        loop {
            // Created before the look, so that room which appears between the
            // look and the sleep ends the sleep ([`Engine::writable`]).
            let mut room = std::pin::pin!(self.engine.writable());
            room.as_mut().enable();
            let peers = self.peers();
            match self.offer_round_robin(&peers, message) {
                Ok(peer) => {
                    return Ok(Delivered {
                        peer: Some(peer),
                        sent: Sent::Queued,
                    });
                }
                Err(refused) => message = refused,
            }
            match mute {
                MuteAction::Drop => {
                    return Ok(Delivered {
                        peer: None,
                        sent: Sent::Dropped,
                    });
                }
                MuteAction::Fail => return Err(mute_error(peers.is_empty())),
                MuteAction::Block => {
                    #[cfg(test)]
                    in_window();
                    room.await;
                }
            }
        }
    }

    /// The `ZMQ_DONTWAIT` form: never waits, and reports `EAGAIN` when no
    /// peer has room — whatever the mute action, because a caller who asked
    /// not to block asked for the refusal.
    pub fn try_send_round_robin(&self, message: Multipart) -> Result<PeerId> {
        let peers = self.peers();
        self.offer_round_robin(&peers, Weighed::new(message))
            .map_err(|_| mute_error(peers.is_empty()))
    }

    /// Offers `message` to each peer in turn, starting at the cursor, and
    /// moves the cursor past the one that took it. A refusal hands the
    /// message back untouched.
    fn offer_round_robin(&self, peers: &[Peer], mut message: Weighed) -> Result<PeerId, Weighed> {
        let start = self.send_cursor.load(Ordering::Relaxed);
        for offset in 0..peers.len() {
            let index = start.wrapping_add(offset) % peers.len();
            match peers[index].pipe.outgoing_ref().offer(message) {
                Ok(()) => {
                    self.send_cursor
                        .store(index.wrapping_add(1), Ordering::Relaxed);
                    return Ok(peers[index].id);
                }
                Err(refused) => message = refused,
            }
        }
        Err(message)
    }

    /// Sends to one named peer: REP's originator, ROUTER's routing id.
    ///
    /// Reports `EHOSTUNREACH` when that peer is gone, which is what a socket
    /// type with `ZMQ_ROUTER_MANDATORY` surfaces and what one without turns
    /// into a drop.
    pub async fn send_to(&self, peer: PeerId, message: Multipart) -> Result<Sent> {
        let Some(pipe) = self.pipe_of(peer) else {
            return Err(Error::EHOSTUNREACH(
                format!("{peer} is gone; its queue was destroyed with it").into(),
            ));
        };
        pipe.outgoing_ref().send(message).await
    }

    /// This peer's pipe, if it still exists.
    pub fn pipe_of(&self, peer: PeerId) -> Option<Pipe> {
        self.engine.pipe_of(peer)
    }

    /// Takes the next message from any peer, fair-queued.
    ///
    /// "Fair-queued" is the rotation the manual names for every receiving
    /// socket type: the cursor advances past whoever was served, so one busy
    /// peer cannot starve the others (§4.1).
    pub async fn recv_fair(&self) -> Result<(PeerId, Multipart)> {
        // A receive that finds a message never creates a bell: the look below
        // repeats, under a bell, only for a receive that is about to wait.
        if let Some(taken) = self.take_fair() {
            return Ok(taken);
        }
        loop {
            // Created before the look, so that a message — or a peer — that
            // arrives between the look and the sleep ends the sleep
            // ([`Engine::readable`]).
            let mut arrived = std::pin::pin!(self.engine.readable());
            arrived.as_mut().enable();
            if let Some(taken) = self.take_fair() {
                return Ok(taken);
            }
            #[cfg(test)]
            in_window();
            arrived.await;
        }
    }

    /// The `ZMQ_DONTWAIT` form of [`Shared::recv_fair`].
    pub fn try_recv_fair(&self) -> Result<(PeerId, Multipart)> {
        match self.take_fair() {
            Some(taken) => Ok(taken),
            None => Err(Error::EAGAIN("nothing is queued from any peer".into())),
        }
    }

    /// Takes the next message from `peer` only, **discarding** whatever
    /// arrives from anybody else.
    ///
    /// REQ's rule, in the specification's words: "SHALL accept an incoming
    /// message only from the last peer that it sent a request to. SHALL
    /// discard silently any messages received from other peers" (§4.2).
    pub async fn recv_from(&self, peer: PeerId) -> Result<Multipart> {
        loop {
            let mut arrived = std::pin::pin!(self.engine.readable());
            arrived.as_mut().enable();
            let peers = self.engine.peers();
            for other in peers.iter().filter(|other| other.id != peer) {
                while let Ok(stray) = other.pipe.incoming_ref().try_recv() {
                    tracing::debug!(
                        peer = %other.id,
                        frames = stray.len(),
                        "discarded a message from a peer this socket did not ask"
                    );
                }
            }
            let Ok(index) = peers.binary_search_by_key(&peer, |candidate| candidate.id) else {
                return Err(Error::EHOSTUNREACH(
                    format!("{peer} is gone before it answered").into(),
                ));
            };
            if let Ok(message) = peers[index].pipe.incoming_ref().try_recv() {
                return Ok(message);
            }
            #[cfg(test)]
            in_window();
            arrived.await;
        }
    }

    /// A wait for the next thing a receive could act on, for a socket type
    /// whose receive has more to report than messages — an XPUB turns a
    /// departure into an unsubscribe — and so must wake on a peer going away
    /// as well as on a message, and decide for itself.
    ///
    /// Create it before looking, and look after creating it: see
    /// [`Engine::readable`].
    pub(crate) fn readable(&self) -> Notified<'_> {
        self.engine.readable()
    }

    /// One fair-queued take, advancing the cursor past whoever was served.
    fn take_fair(&self) -> Option<(PeerId, Multipart)> {
        let peers = self.engine.peers();
        if peers.is_empty() {
            return None;
        }
        let start = self.recv_cursor.load(Ordering::Relaxed);
        for offset in 0..peers.len() {
            let index = start.wrapping_add(offset) % peers.len();
            if let Ok(message) = peers[index].pipe.incoming_ref().try_recv() {
                self.recv_cursor
                    .store(index.wrapping_add(1), Ordering::Relaxed);
                return Some((peers[index].id, message));
            }
        }
        None
    }
}

/// Runs `future` under a wall-clock bound — `ZMQ_SNDTIMEO` and
/// `ZMQ_RCVTIMEO` — reporting `EAGAIN` when it expires, which is the errno
/// libzmq uses for both.
pub async fn within<T>(
    exec: &Exec,
    limit: Option<Duration>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    match limit {
        None => future.await,
        Some(limit) => match exec.within(limit, future).await {
            Some(result) => result,
            None => Err(Error::EAGAIN(
                format!("the operation did not complete within {limit:?}").into(),
            )),
        },
    }
}

impl std::fmt::Debug for SocketCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketCore")
            .field("socket_type", &self.socket_type.as_str())
            .field("engine", &self.engine)
            .finish_non_exhaustive()
    }
}

/// Why a send found nowhere to go.
fn mute_error(no_peers: bool) -> Error {
    if no_peers {
        Error::EAGAIN("this socket has no peer to send to".into())
    } else {
        Error::EAGAIN("every peer's queue is full".into())
    }
}

#[cfg(test)]
thread_local! {
    /// What a test makes happen in the instant between a wait's look and its
    /// sleep — see [`in_window`].
    static WINDOW: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Arms `action` to run once, in the next wait on this thread that reaches
/// the window between its look and its sleep.
///
/// That instant is **inside a single poll**: nothing else on this thread runs
/// there, so a peer arriving or a queue freeing at exactly that point is not
/// reachable through the public surface, and only a second thread landing on
/// it ever hits the hazard the waits are written against. This is how the
/// tests below reach it deterministically instead of racing for it, as
/// `pipe.rs` does for a queue's own waits.
#[cfg(test)]
fn arm_window(action: impl FnOnce() + 'static) {
    WINDOW.with(|window| *window.borrow_mut() = Some(Box::new(action)));
}

/// Runs the armed action, if there is one.
#[cfg(test)]
fn in_window() {
    if let Some(action) = WINDOW.with(|window| window.borrow_mut().take()) {
        action();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;
    use crate::pipe::{PipeConfig, Queue, QueueConfig};
    use std::task::{Context as TaskContext, Poll, Waker};

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    /// Options that dial once and give up: a peer that never connects stays
    /// as it is, with a queue nobody drains — the stalled reader the
    /// size-aware tests need, with no network behind it.
    fn dials_once() -> SocketOptions {
        SocketOptions {
            reconnect_ivl: None,
            ..SocketOptions::default()
        }
    }

    fn nowhere() -> Endpoint {
        Endpoint::parse("tcp://127.0.0.1:9").expect("endpoint")
    }

    /// Polls `future` by hand with a waker that does nothing, exactly as often
    /// as one wake is worth: once to let it register and look — the armed
    /// window fires inside that poll — and once more for the wake it is owed.
    /// `None` is the lost wake-up: the event happened and the waiter is still
    /// asleep, which no amount of further polling would change.
    fn poll_through_the_window<F: Future>(future: F) -> Option<F::Output> {
        let mut future = std::pin::pin!(future);
        let mut cx = TaskContext::from_waker(Waker::noop());
        for _ in 0..2 {
            if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
                return Some(out);
            }
        }
        None
    }

    /// Claim: a socket may be **moved** between threads, which is what
    /// `Send` means and what a caller handing a socket to a task needs.
    ///
    /// The other half of libzmq's rule — a socket may not be *shared* — is
    /// `!Sync`, carried by `PhantomData<Cell<()>>` in [`SocketCore`]. There
    /// is no negative bound to write here, so it is asserted where a
    /// negative can be: `tests/thread_rule.rs` compiles a file that asks for
    /// `Sync` on every socket type and requires it to fail, with the
    /// expected error committed beside it.
    #[tokio::test]
    async fn a_socket_core_can_be_moved_between_threads() {
        fn assert_send<T: Send>(_: &T) {}
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let core = SocketCore::new(&ctx, SocketType::Req, SocketOptions::default()).expect("core");
        assert_send(&core);
        assert_eq!(core.socket_type(), SocketType::Req);

        // Moved, not shared: a task takes ownership and uses it there.
        let moved = tokio::spawn(async move { core.socket_type() })
            .await
            .expect("the task");
        assert_eq!(moved, SocketType::Req);
    }

    // The waits of a socket, against the window between a wait's look and its
    // sleep. A peer arrives, or room appears, at exactly that instant — which
    // is inside one poll, so only a second thread landing there ever hits it
    // and `arm_window` is how a test lands there on purpose.
    //
    // What these tests catch is the shape that creates the bell **after** the
    // look: a snapshot taken, then a wait created, loses a ring that lands
    // between the two, and a PULL or a ROUTER with no peer yet sleeps through
    // the first message of a brand-new one. All three tests below report that
    // sleeper when the order is reversed.

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: `within` turns an expired wait into `EAGAIN`, which is the
        /// errno `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO` report.
        #[tokio::test]
        async fn an_expired_timeout_is_eagain() {
            let ctx = Context::new(ContextConfig::default()).expect("context");
            let core =
                SocketCore::new(&ctx, SocketType::Rep, SocketOptions::default()).expect("core");
            let err = within(
                core.exec(),
                Some(Duration::from_millis(5)),
                std::future::pending::<Result<()>>(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.errno(), "EAGAIN", "{err}");

            // And a bound that does not expire is transparent.
            within(core.exec(), Some(Duration::from_secs(30)), async { Ok(()) })
                .await
                .expect("inside the bound");
        }
        /// Claim: a peer that arrives, with a message already queued, in the
        /// instant between a receive's look and its sleep still wakes that
        /// receive. The peer was not in the snapshot the look took, so the
        /// per-queue waits of the snapshot could not have covered it.
        #[tokio::test]
        async fn a_peer_arriving_in_the_window_wakes_a_receive() {
            let core = SocketCore::new(&context(), SocketType::Pull, dials_once()).expect("core");
            let engine = core.engine().clone();
            arm_window(move || {
                let peer = engine.connect(&nowhere()).expect("connect");
                engine
                    .pipe_of(peer)
                    .expect("a pipe")
                    .incoming()
                    .try_send(Multipart::single("hello"))
                    .expect("queued");
            });
            let (_, message) = poll_through_the_window(core.recv_fair())
                .expect("a peer arriving in the window must wake the receive")
                .expect("the receive");
            assert_eq!(message, Multipart::single("hello"));
        }
        /// Claim: the same window for a blocked send with no peer to send to: a
        /// peer that arrives between the look and the sleep ends the wait and
        /// takes the message — "SHALL block on sending… when it has no connected
        /// peers" is a wait that must end when one appears.
        #[tokio::test]
        async fn a_peer_arriving_in_the_window_wakes_a_blocked_send() {
            let core = SocketCore::new(&context(), SocketType::Push, dials_once()).expect("core");
            let engine = core.engine().clone();
            arm_window(move || {
                engine.connect(&nowhere()).expect("connect");
            });
            let delivered =
                poll_through_the_window(core.send_round_robin(Multipart::single("work")))
                    .expect("a peer arriving in the window must wake the send")
                    .expect("the send");
            assert_eq!(delivered.sent, Sent::Queued);
            assert!(delivered.peer.is_some());
        }
        /// Claim: the same window for a send whose only peer has a full queue:
        /// room freed in the instant between the look and the sleep ends the
        /// wait. The queue rings the socket's bell when its session takes a
        /// message, which is what has to reach a sender that has not yet slept.
        #[tokio::test]
        async fn room_freed_in_the_window_wakes_a_blocked_send() {
            let options = SocketOptions {
                pipe: PipeConfig {
                    outgoing: QueueConfig {
                        hwm: 1,
                        ..PipeConfig::default().outgoing
                    },
                    ..PipeConfig::default()
                },
                ..dials_once()
            };
            let core = SocketCore::new(&context(), SocketType::Push, options).expect("core");
            let peer = core.engine().connect(&nowhere()).expect("connect");
            let pipe = core.pipe_of(peer).expect("a pipe");
            core.try_send_round_robin(Multipart::single("fills the queue"))
                .expect("room for one");

            arm_window(move || {
                pipe.outgoing()
                    .try_recv()
                    .expect("the session takes the message");
            });
            let delivered =
                poll_through_the_window(core.send_round_robin(Multipart::single("waits")))
                    .expect("room freed in the window must wake the send")
                    .expect("the send");
            assert_eq!(delivered.peer, Some(peer));
        }
        /// Claim: **a blocking send waits for room for its own size.** A message
        /// the queue cannot take yet is waited for, not refused with `EAGAIN` and
        /// lost — on a socket whose documentation says it blocks and never
        /// discards. Judging "room" by one byte and then sending the message's
        /// real size is the shape that gets this wrong. Here the queue holds three
        /// one-kilobyte messages under a ceiling that fits exactly three; the send
        /// of a large message stays blocked while the room freed is too small for
        /// it, ends when there is room for it, and the message arrives.
        #[tokio::test]
        async fn a_blocking_send_waits_for_room_for_its_own_size() {
            let small = Multipart::single(vec![b'x'; 1000]);
            let large = Multipart::single(vec![b'y'; 2500]);
            let each = Queue::charge(&small);
            let options = SocketOptions {
                pipe: PipeConfig {
                    outgoing: QueueConfig {
                        hwm: 0,
                        max_bytes: 3 * each,
                        mute: MuteAction::Block,
                    },
                    ..PipeConfig::default()
                },
                ..dials_once()
            };
            let core = SocketCore::new(&context(), SocketType::Push, options).expect("core");
            let peer = core.engine().connect(&nowhere()).expect("connect");
            for _ in 0..3 {
                core.try_send_round_robin(small.clone()).expect("it fits");
            }

            let shared = core.shared.clone();
            let message = large.clone();
            let sending = tokio::spawn(async move { shared.send_round_robin(message).await });
            // Margin: 100 ms for a send that must *not* complete. The phenomenon
            // is the absence of an event, so the interval only has to exceed the
            // time a send takes to refuse outright, which is microseconds.
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                !sending.is_finished(),
                "the send must wait for room, not fail with EAGAIN"
            );

            // The session takes two messages: room for a small one, not yet for
            // this one, so the send stays where it is.
            let pipe = core.pipe_of(peer).expect("a pipe");
            for _ in 0..2 {
                pipe.outgoing().try_recv().expect("queued");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                !sending.is_finished(),
                "room for a small message is not room for a large one"
            );

            pipe.outgoing().try_recv().expect("queued");
            let delivered = tokio::time::timeout(Duration::from_secs(10), sending)
                .await
                .expect("the send ended once there was room for it")
                .expect("the task")
                .expect("the send");
            assert_eq!(delivered.peer, Some(peer));
            assert_eq!(
                pipe.outgoing().try_recv().expect("the large one arrived"),
                large,
                "and it was not lost on the way"
            );
        }
    }
}
