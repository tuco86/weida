//! The connection engine: bind, connect, reconnect, and one pipe per peer.
//!
//! This is everything a ZeroMQ socket does *below* its pattern. It is
//! deliberately socket-type-agnostic: "a socket may connect to many endpoints
//! and bind many at once, thus allowing many-to-many relationships", "there
//! is no `zmq_accept()`: a bound endpoint accepts automatically", and
//! "application code cannot manipulate individual underlying connections"
//! (`docs/research/zeromq.md` §2). So an [`Engine`] owns the listeners, the
//! connecters and the [`Pipe`] of every peer, and the socket types built on
//! it choose *which* pipe a message goes to and in what shape —
//! round-robin, fan-out, or by routing id.
//!
//! **A queue exists before a connection does.** Every pattern RFC: the double
//! queue is created when an outgoing connection is *initiated* and is
//! maintained "whether or not the connection is established" (§2). So
//! [`Engine::connect`] returns immediately with a peer that already has a
//! pipe, and `ZMQ_IMMEDIATE` is what decides whether a sender may put
//! anything in it before the connection completes.
//!
//! **The queue belongs to the endpoint, not to the TCP connection.** A
//! connected endpoint keeps its pipe across reconnects — that is what makes
//! queueing to a peer that has not arrived meaningful, and what
//! `ZMQ_IMMEDIATE` exists to switch off. An *accepted* peer has no endpoint
//! of its own, so its pipe is destroyed with the connection, "discarding any
//! messages it contains" (§2), and only [`Engine::disconnect`] or a close
//! destroys a connected endpoint's.
//!
//! **Every timer goes through `weida-runtime`'s `Exec`**: the reconnect
//! backoff, the connect timeout and the handshake interval. Nothing here
//! calls `tokio::time`.
//!
//! What this module does **not** do is speak ZMTP. A connection is
//! established once its bytes flow, and it is handed to a [`Session`] — the
//! greeting, the security handshake, `READY` and the framing belong to
//! [`crate::session::ZmtpSession`]
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §5.3). The
//! engine has no default `Session`: a socket that handed its peers to a no-op
//! would be a ZeroMQ implementation that speaks nothing.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use weida_runtime::Exec;

use crate::context::{Context, SocketId, SocketSlot};
use crate::endpoint::{Endpoint, TcpHost};
use crate::error::{Error, Result};
use crate::identity::RoutingId;
use crate::inproc::InprocBinding;
use crate::monitor::{
    EVENT_ACCEPTED, EVENT_BIND_FAILED, EVENT_CLOSED, EVENT_CONNECT_DELAYED, EVENT_CONNECT_RETRIED,
    EVENT_CONNECTED, EVENT_DISCONNECTED, EVENT_HANDSHAKE_SUCCEEDED, EVENT_LISTENING, Monitor,
    MonitorEvent, MonitorEvents, MonitorSink,
};
use crate::options::SocketOptions;
use crate::pipe::Pipe;
use crate::subscriptions::Subscriptions;
use crate::transport::Stream;
use crate::zap::{AuthenticatedUser, ZapUserId};

/// What a [`Session`] returns: a future that ends when the connection does.
pub type SessionFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// Which side of the connection this socket is.
///
/// ZMTP's greeting carries it as the as-server bit and the security handshake
/// depends on it, so the engine records who dialled rather than letting the
/// session guess.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// This socket dialled: it connected to a bound endpoint.
    Connecter,
    /// This socket was dialled: the connection arrived on a bound endpoint.
    Binder,
}

/// One socket's identity for one peer, stable for that peer's lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(u64);

impl PeerId {
    /// The number, for logging and keying.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// A peer id belonging to no engine, for driving a session without one.
    #[cfg(test)]
    pub(crate) const fn detached() -> PeerId {
        PeerId(0)
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer {}", self.0)
    }
}

/// How a session tells the engine that its handshake finished.
///
/// `ZMQ_HANDSHAKE_IVL` bounds "the maximum handshake interval… If the
/// handshake does not complete within the interval the connection is closed"
/// (`docs/research/zeromq.md` §11), and only the session knows when it is
/// done. So the engine hands this over with the connection, races it against
/// the timer, and drops the connection — closing the socket — if the timer
/// wins. An unauthenticated peer holding a connection open is exactly what
/// the option defends against.
#[derive(Debug)]
pub struct HandshakeGate {
    done: Option<oneshot::Sender<()>>,
}

impl HandshakeGate {
    /// Reports that the handshake is complete. Idempotent; further calls do
    /// nothing.
    pub fn complete(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }

    /// A gate nobody watches, for driving a session without an engine.
    #[cfg(test)]
    pub(crate) const fn detached() -> HandshakeGate {
        HandshakeGate { done: None }
    }
}

/// One established connection, handed to a [`Session`].
#[derive(Debug)]
pub struct Connection {
    /// The byte stream. The session owns it, and dropping it closes the
    /// connection.
    pub stream: Stream,
    /// This peer's double queue: the session drains `outgoing` onto the wire
    /// and fills `incoming` from it.
    pub pipe: Pipe,
    /// Which peer of this socket the connection is.
    pub peer: PeerId,
    /// Who dialled.
    pub role: Role,
    /// The endpoint the connection belongs to: the one dialled, or the one it
    /// arrived on.
    pub endpoint: Endpoint,
    /// The socket's options, for `ZMQ_MAXMSGSIZE` and the heartbeat triple.
    pub options: SocketOptions,
    /// The reactor this connection's timers and tasks belong to.
    pub exec: Exec,
    /// Signal the handshake's completion here; see [`HandshakeGate`].
    pub handshake: HandshakeGate,
    /// Where to record the identity the peer announces; see
    /// [`AnnouncedIdentity`].
    pub identity: AnnouncedIdentity,
    /// This peer's subscription table, which a PUB or XPUB session fills
    /// from the peer's `SUBSCRIBE`/`CANCEL` in either wire form.
    pub subscriptions: Arc<Subscriptions>,
    /// The context this connection belongs to, for the `inproc://` namespace
    /// its ZAP handler lives in.
    pub context: Context,
    /// The peer's address for a ZAP request: an IP for `tcp://`, and empty
    /// for a local transport, which has no address to report.
    pub peer_address: String,
    /// The kernel's statement about a local peer, where the transport has
    /// one, offered to a ZAP handler as the fact it is.
    pub credentials: Option<weida_core::LocalPrincipal>,
    /// Where to record what a ZAP handler said about this connection; see
    /// [`AuthenticatedUser`].
    pub user: AuthenticatedUser,
}

/// What drives one connection once its bytes flow: in this library, ZMTP.
///
/// One implementation per protocol generation, shared by every socket type,
/// because the greeting and the framing do not depend on the pattern —
/// `Socket-Type` is a property in `READY`, not a different wire format
/// (`docs/research/zeromq.md` §3).
pub trait Session: Send + Sync + 'static {
    /// Drives `connection` until it ends. Returning `Ok(())` is a clean
    /// close; an error is logged and treated the same way — the connection is
    /// gone either way, and a connected endpoint then reconnects.
    fn run(&self, connection: Connection) -> SessionFuture;
}

/// A snapshot of one peer.
#[derive(Clone, Debug)]
pub struct Peer {
    /// The user id a ZAP handler returned for this connection, where this
    /// socket authorized it.
    ///
    /// A per-connection fact held by the server and never a per-message
    /// label: 27/ZAP's user id is a string a handler chose, with no bridge to
    /// weida's proved identities and none to the routing id either
    /// ([`ZapUserId`]).
    pub user_id: Option<ZapUserId>,
    /// The credentials the kernel attributes to this peer, for a local
    /// transport that has them.
    ///
    /// `Some` only for `ipc://`: TCP has an address and no process, and
    /// `inproc://` has no kernel between the halves. This is the input an
    /// authorization handler is given for a local peer — the kernel fact it
    /// is, and not an identity the peer asserted
    /// ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4
    /// item 6).
    pub credentials: Option<weida_core::LocalPrincipal>,
    /// Which peer of this socket.
    pub id: PeerId,
    /// The endpoint this peer belongs to: `Some` for one this socket dialled,
    /// `None` for one that arrived on a bound endpoint — an accepted peer has
    /// no address of its own that means anything to a dialler.
    pub endpoint: Option<Endpoint>,
    /// Whether a connection is up right now.
    pub connected: bool,
    /// Connect attempts made for this peer, which is how a reconnect loop is
    /// observable without a monitor socket; with one, the same loop is
    /// `ZMQ_EVENT_CONNECT_DELAYED` and `ZMQ_EVENT_CONNECT_RETRIED`.
    pub attempts: u64,
    /// This peer's double queue.
    pub pipe: Pipe,
    /// The identity this peer announced in its `READY`, if any.
    pub identity: Option<RoutingId>,
    /// Whether this peer has completed its handshake, which is when
    /// [`Peer::identity`] means anything. A socket type that keys a table by
    /// the identity must wait for this.
    pub announced: bool,
    /// This peer's subscriptions, which a publisher matches messages
    /// against; see [`Subscriptions`].
    pub subscriptions: Arc<Subscriptions>,
}

/// What destroying a pipe discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Discarded {
    /// Messages queued for the peer and never sent.
    pub outgoing: usize,
    /// Messages received from the peer and never read.
    pub incoming: usize,
}

struct PeerEntry {
    /// The kernel's statement about this peer, for an `ipc://` connection:
    /// captured when the connection was made and never re-read.
    credentials: Option<weida_core::LocalPrincipal>,
    /// What a ZAP handler said about this connection, where one was asked.
    user: AuthenticatedUser,
    pipe: Pipe,
    identity: AnnouncedIdentity,
    /// What this peer has subscribed to, as a publisher keeps it. Empty and
    /// unused for every socket type that is not PUB or XPUB.
    subscriptions: Arc<Subscriptions>,
    endpoint: Option<Endpoint>,
    connected: bool,
    attempts: u64,
}

struct Bound {
    requested: Endpoint,
    bound: Endpoint,
    task: JoinHandle<()>,
}

struct Connecter {
    endpoint: Endpoint,
    peer: PeerId,
    task: JoinHandle<()>,
}

struct EngineState {
    peers: HashMap<PeerId, PeerEntry>,
    /// Peers admitted from *accepted* connections, which is the count a
    /// stranger chooses and `max_peers` bounds. The connected ones are not
    /// counted here: their number is how many times the local application
    /// called `connect`.
    accepted: usize,
    binds: Vec<Bound>,
    connects: Vec<Connecter>,
    last_endpoint: Option<Endpoint>,
    next_peer: u64,
    closed: bool,
}

/// Where a session records what its peer announced in the handshake.
///
/// ROUTER addresses a peer by the `Identity` property of its `READY`
/// (`docs/research/zeromq.md` §4.2) — but only the session reads the
/// handshake, and only the socket type cares about the answer, so the slot
/// lives on the peer and both ends share it. Cloning shares the slot.
///
/// **The "announced" flag is not redundant.** A peer exists from the moment
/// its connection is accepted, which is before its `READY` has been read, so
/// a socket type that keyed a table at accept time would key it by a
/// generated id and never learn the peer's own. ROUTER therefore waits for
/// this — which is also libzmq's behaviour: it "learns an identity only
/// after that peer has sent something".
#[derive(Clone, Debug, Default)]
pub struct AnnouncedIdentity(Arc<Mutex<Announcement>>);

#[derive(Debug, Default)]
struct Announcement {
    announced: bool,
    identity: Option<RoutingId>,
}

impl AnnouncedIdentity {
    /// Records the handshake's outcome: the identity the peer chose, or
    /// `None` where it chose none. Called once, after the handshake.
    pub fn announce(&self, identity: Option<RoutingId>) {
        let mut slot = self.0.lock().expect("identity slot poisoned");
        slot.announced = true;
        slot.identity = identity;
    }

    /// What the peer announced, if anything. A peer that announced nothing
    /// gets an identity generated by whoever needs one.
    pub fn get(&self) -> Option<RoutingId> {
        self.0
            .lock()
            .expect("identity slot poisoned")
            .identity
            .clone()
    }

    /// Whether the handshake has happened at all, which is when the answer
    /// above becomes meaningful.
    pub fn is_announced(&self) -> bool {
        self.0.lock().expect("identity slot poisoned").announced
    }
}

/// What [`EngineInner::admit`] decided about an accepted connection.
enum Admitted {
    /// Admitted, as this peer.
    Peer(PeerId),
    /// Refused: this socket already holds `max_peers` accepted connections.
    AtCeiling,
    /// Refused: the socket is closed, so there is nothing to accept onto.
    SocketClosed,
}

struct EngineInner {
    /// The context this socket belongs to: its `inproc://` namespace, so
    /// that a bind can take a name and a dial can find one, and its ZAP
    /// handler, which a session asks about every connection it must
    /// authorize.
    context: Context,
    exec: Exec,
    session: Arc<dyn Session>,
    options: SocketOptions,
    /// The socket's slot under `ZMQ_MAX_SOCKETS`; dropping it is
    /// `zmq_close()`'s accounting.
    slot: SocketSlot,
    state: Mutex<EngineState>,
    /// Signalled whenever the peer set changes: one arrived, one connected,
    /// one went away. A socket type that must wait for *a* peer — every
    /// blocking mute action with nothing to send to — waits on this instead
    /// of polling.
    peers_changed: Notify,
    /// Where connection-lifecycle events are published, and the mask the
    /// one installed monitor asked for. `zmq_socket_monitor`'s events are
    /// one source rendered two ways, and this is the source
    /// ([`crate::monitor`]).
    monitor: MonitorSink,
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        // Closing a socket closes its connections. The tasks hold only weak
        // references, so this is what actually ends them: aborting drops the
        // streams they own.
        let mut state = self.state.lock().expect("engine state poisoned");
        for bound in state.binds.drain(..) {
            bound.task.abort();
        }
        for connecter in state.connects.drain(..) {
            connecter.task.abort();
        }
        for (_, peer) in state.peers.drain() {
            peer.pipe.close();
        }
    }
}

/// The transport-facing half of a socket: what binds, connects, reconnects
/// and holds the pipes.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

/// What a task needs to run a connection without keeping the engine alive.
#[derive(Clone)]
struct TaskCtx {
    context: Context,
    socket: SocketId,
    engine: Weak<EngineInner>,
    exec: Exec,
    session: Arc<dyn Session>,
    options: SocketOptions,
    monitor: MonitorSink,
}

impl Engine {
    /// Creates an engine on `context`, taking one socket slot under
    /// `ZMQ_MAX_SOCKETS`.
    ///
    /// Fails with `EINVAL` for an unusable option, `EMFILE` at the context's
    /// socket ceiling, and `ETERM` on a terminated context.
    pub fn new(
        context: &Context,
        options: SocketOptions,
        session: Arc<dyn Session>,
    ) -> Result<Engine> {
        options.validate()?;
        let slot = context.open_socket()?;
        Ok(Engine {
            inner: Arc::new(EngineInner {
                context: context.clone(),
                exec: context.exec().clone(),
                session,
                options,
                slot,
                state: Mutex::new(EngineState {
                    peers: HashMap::new(),
                    accepted: 0,
                    binds: Vec::new(),
                    connects: Vec::new(),
                    last_endpoint: None,
                    next_peer: 1,
                    closed: false,
                }),
                peers_changed: Notify::new(),
                monitor: MonitorSink::default(),
            }),
        })
    }

    /// This socket's id within its context.
    pub fn id(&self) -> SocketId {
        self.inner.slot.id()
    }

    /// The options this engine runs under.
    pub fn options(&self) -> &SocketOptions {
        &self.inner.options
    }

    /// Binds `endpoint` and starts accepting.
    ///
    /// Returns the endpoint actually bound, which is what `ZMQ_LAST_ENDPOINT`
    /// reports and the only way to learn the port of a wildcard bind:
    /// "wildcard binds require reading back `ZMQ_LAST_ENDPOINT` before
    /// `zmq_unbind()`" (`docs/research/zeromq.md` §2).
    ///
    /// `*` binds the IPv4 wildcard address. Whether the wildcard also takes
    /// IPv6 is `ZMQ_IPV6`'s business, which the option table refuses as
    /// absent ([`crate::optiontable`]); nothing here pretends to have decided
    /// it.
    ///
    /// `ZMQ_BACKLOG` is passed to `listen(2)`, and `SO_REUSEADDR` is set —
    /// what libzmq and Rust's own `TcpListener` both do, and what lets a
    /// restarted process bind the port it just had. Neither applies to
    /// `inproc://`, which takes a name in the context's namespace instead:
    /// there is no kernel queue to size and nobody to steal the name.
    pub async fn bind(&self, endpoint: &Endpoint) -> Result<Endpoint> {
        match self.bind_inner(endpoint).await {
            Ok(bound) => {
                self.inner
                    .monitor
                    .publish(EVENT_LISTENING, || MonitorEvent::Listening {
                        endpoint: bound.to_string(),
                    });
                Ok(bound)
            }
            Err(e) => {
                self.inner
                    .monitor
                    .publish(EVENT_BIND_FAILED, || MonitorEvent::BindFailed {
                        endpoint: endpoint.to_string(),
                        reason: e.to_string(),
                    });
                Err(e)
            }
        }
    }

    /// Installs a monitor for `events`, replacing any previous one:
    /// `zmq_socket_monitor`, with the event stream typed. The
    /// `inproc://` PAIR form is [`crate::monitor::serve_pair`] over the same
    /// stream.
    pub fn monitor(&self, events: MonitorEvents) -> Monitor {
        self.inner.monitor.install(events)
    }

    async fn bind_inner(&self, endpoint: &Endpoint) -> Result<Endpoint> {
        self.alive()?;
        check_transport(endpoint)?;
        let (bound, task) = match endpoint {
            Endpoint::Inproc(name) => {
                let binding = self.inner.context.inproc().bind(name)?;
                let ctx = self.task_ctx();
                let accepting = endpoint.clone();
                // The binding moves into the task, so aborting the task —
                // what `unbind` and `close` do — releases the name.
                let task = self
                    .inner
                    .exec
                    .spawn(async move { inproc_accept_loop(ctx, binding, accepting).await });
                (endpoint.clone(), task)
            }
            #[cfg(unix)]
            Endpoint::Ipc(path) => {
                let binding = crate::ipc::IpcBinding::bind(&self.inner.exec, path)?;
                let ctx = self.task_ctx();
                let accepting = endpoint.clone();
                // The binding moves into the task, so aborting the task —
                // what `unbind` and `close` do — unlinks the socket file.
                let task = self
                    .inner
                    .exec
                    .spawn(async move { ipc_accept_loop(ctx, binding, accepting).await });
                (endpoint.clone(), task)
            }
            _ => {
                let addr = self.resolve_one(endpoint).await?;
                let listener = {
                    // Inside the runtime context: tokio registers the socket
                    // with the reactor as it is constructed, and the calling
                    // thread may have no reactor of its own.
                    let _guard = self.inner.exec.enter();
                    let socket = if addr.is_ipv4() {
                        TcpSocket::new_v4()
                    } else {
                        TcpSocket::new_v6()
                    }?;
                    socket.set_reuseaddr(true)?;
                    socket.bind(addr)?;
                    socket.listen(self.inner.options.backlog)?
                };
                let local = listener.local_addr()?;
                let bound = Endpoint::Tcp {
                    host: TcpHost::Ip(local.ip()),
                    port: local.port(),
                };
                let ctx = self.task_ctx();
                let accepting = bound.clone();
                let task = self
                    .inner
                    .exec
                    .spawn(async move { accept_loop(ctx, listener, accepting).await });
                (bound, task)
            }
        };

        let mut state = self.lock();
        state.last_endpoint = Some(bound.clone());
        state.binds.push(Bound {
            requested: endpoint.clone(),
            bound: bound.clone(),
            task,
        });
        Ok(bound)
    }

    /// Connects to `endpoint`, asynchronously.
    ///
    /// Returns as soon as the peer exists, like `zmq_connect()`: the pipe is
    /// there immediately and the dialling happens behind it, with
    /// `ZMQ_RECONNECT_IVL` backoff on every failure and after every loss.
    pub fn connect(&self, endpoint: &Endpoint) -> Result<PeerId> {
        self.alive()?;
        check_transport(endpoint)?;
        if let Endpoint::Tcp {
            host: TcpHost::Any, ..
        } = endpoint
        {
            return Err(Error::EINVAL(
                "the wildcard address binds, it does not connect: tcp://*:port has no peer".into(),
            ));
        }

        let pipe = Pipe::new(self.inner.options.pipe);
        let peer = {
            let mut state = self.lock();
            let peer = PeerId(state.next_peer);
            state.next_peer += 1;
            state.peers.insert(
                peer,
                PeerEntry {
                    credentials: None,
                    user: AuthenticatedUser::default(),
                    pipe,
                    identity: AnnouncedIdentity::default(),
                    subscriptions: Arc::new(Subscriptions::new(
                        self.inner.options.max_subscriptions,
                        self.inner.options.max_subscription_bytes,
                    )),
                    endpoint: Some(endpoint.clone()),
                    connected: false,
                    attempts: 0,
                },
            );
            state.last_endpoint = Some(endpoint.clone());
            peer
        };

        let ctx = self.task_ctx();
        let dialling = endpoint.clone();
        let task = self
            .inner
            .exec
            .spawn(async move { connecter_loop(ctx, peer, dialling).await });
        self.lock().connects.push(Connecter {
            endpoint: endpoint.clone(),
            peer,
            task,
        });
        // `ZMQ_EVENT_CONNECT_DELAYED`: the peer exists with no connection
        // behind it, which is the state `ZMQ_IMMEDIATE` is about. Published
        // here because `zmq_connect` returning is exactly that moment.
        self.inner
            .monitor
            .publish(EVENT_CONNECT_DELAYED, || MonitorEvent::ConnectDelayed {
                endpoint: endpoint.to_string(),
            });
        self.inner.peers_changed.notify_waiters();
        Ok(peer)
    }

    /// Waits until the peer set changes: one arrived, one connected, one went
    /// away.
    ///
    /// What a socket type blocking with nowhere to send waits on. "SHALL
    /// block on sending… when it has no connected peers" is a wait for a
    /// peer, and this is that wait rather than a poll.
    pub async fn wait_for_peer_change(&self) {
        self.inner.peers_changed.notified().await;
    }

    /// Stops accepting on `endpoint`, which may be the endpoint as requested
    /// or as [`Engine::bind`] reported it.
    ///
    /// Peers already accepted there keep their connections and their pipes: a
    /// message the application has already been handed is not unmade by
    /// closing a door. Fails with `ENOENT` when nothing is bound there, which
    /// is libzmq's answer too.
    pub fn unbind(&self, endpoint: &Endpoint) -> Result<()> {
        let mut state = self.lock();
        let Some(index) = state
            .binds
            .iter()
            .position(|bound| bound.requested == *endpoint || bound.bound == *endpoint)
        else {
            return Err(Error::ENOENT(
                format!("this socket has nothing bound at {endpoint}").into(),
            ));
        };
        state.binds.remove(index).task.abort();
        Ok(())
    }

    /// Disconnects `endpoint`: stops dialling it and destroys its pipe,
    /// discarding what it held.
    ///
    /// The counts are the RFCs' "discarding any messages it contains" made
    /// visible: a `zmq_send` that returned is not a delivery, and this is
    /// where the difference becomes a number. Fails with `ENOENT` when this
    /// socket is not connected there.
    pub fn disconnect(&self, endpoint: &Endpoint) -> Result<Discarded> {
        let mut state = self.lock();
        let Some(index) = state
            .connects
            .iter()
            .position(|connecter| connecter.endpoint == *endpoint)
        else {
            return Err(Error::ENOENT(
                format!("this socket is not connected to {endpoint}").into(),
            ));
        };
        let connecter = state.connects.remove(index);
        connecter.task.abort();
        let discarded = match state.peers.remove(&connecter.peer) {
            Some(entry) => {
                let (outgoing, incoming) = entry.pipe.close();
                Discarded { outgoing, incoming }
            }
            None => Discarded {
                outgoing: 0,
                incoming: 0,
            },
        };
        drop(state);
        self.inner.peers_changed.notify_waiters();
        Ok(discarded)
    }

    /// The last endpoint bound or connected — `ZMQ_LAST_ENDPOINT`, with a
    /// wildcard bind resolved to the port the OS chose.
    pub fn last_endpoint(&self) -> Option<Endpoint> {
        self.lock().last_endpoint.clone()
    }

    /// Every endpoint this socket is accepting on, as bound.
    pub fn bound(&self) -> Vec<Endpoint> {
        self.lock()
            .binds
            .iter()
            .map(|bound| bound.bound.clone())
            .collect()
    }

    /// Every endpoint this socket is dialling or connected to.
    pub fn connected(&self) -> Vec<Endpoint> {
        self.lock()
            .connects
            .iter()
            .map(|connecter| connecter.endpoint.clone())
            .collect()
    }

    /// Every peer this socket has, connected or not.
    pub fn peers(&self) -> Vec<Peer> {
        let state = self.lock();
        let mut peers: Vec<Peer> = state
            .peers
            .iter()
            .map(|(id, entry)| snapshot(*id, entry))
            .collect();
        peers.sort_by_key(|peer| peer.id);
        peers
    }

    /// The peers a sender may put a message in right now.
    ///
    /// Every peer, unless `ZMQ_IMMEDIATE` is set — in which case only the
    /// connected ones, so that "messages shall be queued only to completed
    /// connections" and a round-robin socket does not fill the queue of an
    /// endpoint that never answers (`docs/research/zeromq.md` §5).
    pub fn outgoing_peers(&self) -> Vec<Peer> {
        let immediate = self.inner.options.immediate;
        self.peers()
            .into_iter()
            .filter(|peer| !immediate || peer.connected)
            .collect()
    }

    /// One peer, by id.
    pub fn peer(&self, id: PeerId) -> Option<Peer> {
        let state = self.lock();
        state.peers.get(&id).map(|entry| snapshot(id, entry))
    }

    /// Destroys one peer's pipe and forgets it, which ends its connection.
    ///
    /// What `ZMQ_ROUTER_HANDOVER` does to an incumbent whose identity a
    /// newcomer claimed: "hand-over the connection to the new client and
    /// disconnect the existing one" (`docs/research/zeromq.md` §4.2). The
    /// session notices its pipe is gone and drops the stream, so the peer
    /// observes a close.
    pub fn evict(&self, peer: PeerId) -> Option<Discarded> {
        let entry = {
            let mut state = self.lock();
            let entry = state.peers.remove(&peer);
            if entry.as_ref().is_some_and(|entry| entry.endpoint.is_none()) {
                state.accepted -= 1;
            }
            entry
        };
        self.inner.peers_changed.notify_waiters();
        entry.map(|entry| {
            let (outgoing, incoming) = entry.pipe.close();
            Discarded { outgoing, incoming }
        })
    }

    /// Closes the socket: stops accepting and dialling everywhere, and
    /// destroys every pipe.
    ///
    /// Idempotent. The engine's own drop does the same thing, so a socket
    /// that is simply let go closes its connections too.
    pub fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        for bound in state.binds.drain(..) {
            bound.task.abort();
        }
        for connecter in state.connects.drain(..) {
            connecter.task.abort();
        }
        for (_, entry) in state.peers.drain() {
            entry.pipe.close();
        }
        state.accepted = 0;
        drop(state);
        self.inner.peers_changed.notify_waiters();
        // One `ZMQ_EVENT_CLOSED` per connection is published by the sessions
        // as they end; what closing the socket adds is the last event a
        // monitor will ever see.
        self.inner.monitor.stop();
    }

    /// Refuses an operation on a closed socket or a terminated context.
    fn alive(&self) -> Result<()> {
        if self.inner.slot.is_terminated() {
            return Err(Error::ETERM(
                "the context is terminated; this socket accepts no further operation".into(),
            ));
        }
        if self.lock().closed {
            return Err(Error::ENOTSOCK("this socket is closed".into()));
        }
        Ok(())
    }

    async fn resolve_one(&self, endpoint: &Endpoint) -> Result<SocketAddr> {
        let Endpoint::Tcp { host, port } = endpoint else {
            unreachable!("only a tcp endpoint is resolved to an address");
        };
        match host {
            TcpHost::Any => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), *port)),
            TcpHost::Ip(ip) => Ok(SocketAddr::new(*ip, *port)),
            TcpHost::Name(name) => {
                let addrs = self
                    .inner
                    .exec
                    .resolve(name, *port, self.inner.options.max_resolved_addresses)
                    .await?;
                Ok(addrs[0])
            }
        }
    }

    fn task_ctx(&self) -> TaskCtx {
        TaskCtx {
            context: self.inner.context.clone(),
            socket: self.inner.slot.id(),
            engine: Arc::downgrade(&self.inner),
            exec: self.inner.exec.clone(),
            session: Arc::clone(&self.inner.session),
            options: self.inner.options.clone(),
            monitor: self.inner.monitor.clone(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, EngineState> {
        self.inner.state.lock().expect("engine state poisoned")
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("Engine")
            .field("socket", &self.inner.slot.id())
            .field("bound", &state.binds.len())
            .field("connected", &state.connects.len())
            .field("peers", &state.peers.len())
            .finish_non_exhaustive()
    }
}

fn snapshot(id: PeerId, entry: &PeerEntry) -> Peer {
    Peer {
        id,
        user_id: entry.user.get(),
        credentials: entry.credentials,
        endpoint: entry.endpoint.clone(),
        connected: entry.connected,
        attempts: entry.attempts,
        pipe: entry.pipe.clone(),
        identity: entry.identity.get(),
        announced: entry.identity.is_announced(),
        subscriptions: Arc::clone(&entry.subscriptions),
    }
}

/// The engine carries ZeroMQ's whole working set — `tcp`, `ipc` and
/// `inproc` — except that `ipc` needs `AF_UNIX`, which is a property of the
/// platform rather than of this library. Where there is none, the endpoint is
/// refused by name instead of accepted and ignored.
fn check_transport(endpoint: &Endpoint) -> Result<()> {
    match endpoint {
        Endpoint::Tcp { .. } | Endpoint::Inproc(_) => Ok(()),
        #[cfg(unix)]
        Endpoint::Ipc(_) => Ok(()),
        #[cfg(not(unix))]
        other => Err(Error::EPROTONOSUPPORT(
            format!(
                "the {} transport needs AF_UNIX, which this platform has not; tcp and inproc are \
                 carried everywhere",
                other.transport()
            )
            .into(),
        )),
    }
}

impl EngineInner {
    /// Admits an accepted connection, or says why it cannot be.
    ///
    /// This is where `max_peers` is enforced, because this is where the entry
    /// is inserted: `ZMQ_MAX_SOCKETS` bounds sockets per *context* and
    /// `ZMQ_BACKLOG` the kernel's accept queue, and neither bounds how many
    /// established connections one socket holds. Every admitted peer carries
    /// a `Pipe` of `ZMQ_SNDHWM`/`ZMQ_RCVHWM` messages per direction, so an
    /// unbounded count multiplies that product by a number a stranger picks.
    fn admit(&self, endpoint: Option<Endpoint>, pipe: Pipe) -> Admitted {
        let mut state = self.state.lock().expect("engine state poisoned");
        if state.closed {
            return Admitted::SocketClosed;
        }
        if state.accepted >= self.options.max_peers {
            return Admitted::AtCeiling;
        }
        let peer = PeerId(state.next_peer);
        state.next_peer += 1;
        state.accepted += 1;
        state.peers.insert(
            peer,
            PeerEntry {
                credentials: None,
                user: AuthenticatedUser::default(),
                pipe,
                identity: AnnouncedIdentity::default(),
                subscriptions: Arc::new(Subscriptions::new(
                    self.options.max_subscriptions,
                    self.options.max_subscription_bytes,
                )),
                endpoint,
                connected: true,
                attempts: 0,
            },
        );
        drop(state);
        self.peers_changed.notify_waiters();
        Admitted::Peer(peer)
    }

    fn forget(&self, peer: PeerId) {
        let entry = {
            let mut state = self.state.lock().expect("engine state poisoned");
            let entry = state.peers.remove(&peer);
            if entry.as_ref().is_some_and(|entry| entry.endpoint.is_none()) {
                state.accepted -= 1;
            }
            entry
        };
        if let Some(entry) = entry {
            // An accepted peer's queue is destroyed with its connection,
            // "discarding any messages it contains".
            entry.pipe.close();
        }
        self.peers_changed.notify_waiters();
    }

    fn set_connected(&self, peer: PeerId, connected: bool) {
        if let Some(entry) = self
            .state
            .lock()
            .expect("engine state poisoned")
            .peers
            .get_mut(&peer)
        {
            entry.connected = connected;
        }
        self.peers_changed.notify_waiters();
    }

    /// Records the kernel's statement about a peer, once, when its
    /// connection was made.
    fn set_credentials(&self, peer: PeerId, credentials: Option<weida_core::LocalPrincipal>) {
        if credentials.is_none() {
            return;
        }
        if let Some(entry) = self
            .state
            .lock()
            .expect("engine state poisoned")
            .peers
            .get_mut(&peer)
        {
            entry.credentials = credentials;
        }
    }

    fn count_attempt(&self, peer: PeerId) {
        if let Some(entry) = self
            .state
            .lock()
            .expect("engine state poisoned")
            .peers
            .get_mut(&peer)
        {
            entry.attempts += 1;
        }
    }

    fn subscriptions_of(&self, peer: PeerId) -> Option<Arc<Subscriptions>> {
        self.state
            .lock()
            .expect("engine state poisoned")
            .peers
            .get(&peer)
            .map(|entry| Arc::clone(&entry.subscriptions))
    }

    fn user_slot(&self, peer: PeerId) -> Option<AuthenticatedUser> {
        self.state
            .lock()
            .expect("engine state poisoned")
            .peers
            .get(&peer)
            .map(|entry| entry.user.clone())
    }

    fn identity_slot(&self, peer: PeerId) -> Option<AnnouncedIdentity> {
        self.state
            .lock()
            .expect("engine state poisoned")
            .peers
            .get(&peer)
            .map(|entry| entry.identity.clone())
    }

    fn pipe_of(&self, peer: PeerId) -> Option<Pipe> {
        self.state
            .lock()
            .expect("engine state poisoned")
            .peers
            .get(&peer)
            .map(|entry| entry.pipe.clone())
    }
}

/// Accepts connections on one bound endpoint. "A bound endpoint accepts
/// automatically" — there is no `zmq_accept()` (§2).
async fn accept_loop(ctx: TaskCtx, listener: TcpListener, endpoint: Endpoint) {
    loop {
        let Ok((stream, from)) = listener.accept().await else {
            return;
        };
        // 27/ZAP wants "the client IP address as an IPv4 dotted or IPv6
        // canonical string", so the address and not the port: authorization
        // is about who connected, and the source port is noise.
        let address = from.ip().to_string();
        // Upgraded per connection, never held across one: a task holding the
        // engine alive would make closing a socket impossible.
        let Some(engine) = ctx.engine.upgrade() else {
            return;
        };
        let pipe = Pipe::new(ctx.options.pipe);
        let peer = match engine.admit(None, pipe.clone()) {
            Admitted::Peer(peer) => peer,
            // Past the ceiling: the connection is closed by dropping the
            // stream, which a foreign peer observes as an immediate EOF, and
            // no entry is left behind. Accepting continues, because the
            // ceiling is a bound on live peers rather than on the listener.
            Admitted::AtCeiling => {
                tracing::warn!(
                    ceiling = ctx.options.max_peers,
                    "refused an accepted connection: this socket already holds its max_peers"
                );
                drop(stream);
                continue;
            }
            Admitted::SocketClosed => return,
        };
        let identity = engine.identity_slot(peer).unwrap_or_default();
        let user = engine.user_slot(peer).unwrap_or_default();
        let subscriptions = engine.subscriptions_of(peer).unwrap_or_else(|| {
            Arc::new(Subscriptions::new(
                ctx.options.max_subscriptions,
                ctx.options.max_subscription_bytes,
            ))
        });
        drop(engine);

        let ctx = ctx.clone();
        let endpoint = endpoint.clone();
        let exec = ctx.exec.clone();
        exec.spawn(async move {
            let stream = Stream::tcp(stream);
            let _ = stream.set_nodelay(true);
            let outcome = run_session(
                &ctx,
                stream,
                PeerSession {
                    peer,
                    pipe,
                    endpoint,
                    role: Role::Binder,
                    identity,
                    subscriptions,
                    address,
                    credentials: None,
                    user,
                },
            )
            .await;
            if let Err(e) = outcome {
                tracing::debug!(%peer, error = %e, "an accepted connection ended");
            }
            if let Some(engine) = ctx.engine.upgrade() {
                engine.forget(peer);
            }
        });
    }
}

/// Accepts dials to one bound `inproc://` name.
///
/// The same shape as [`accept_loop`], and deliberately so: an accepted
/// in-process connection is an accepted connection — no endpoint of its own,
/// its pipe destroyed with it, `max_peers` enforced at admission — and the
/// session that runs over it is the same ZMTP session that runs over TCP.
///
/// Holding the [`InprocBinding`] is what holds the name: when this task is
/// aborted by `unbind` or by closing the socket, the binding drops and the
/// name is free again.
async fn inproc_accept_loop(ctx: TaskCtx, mut binding: InprocBinding, endpoint: Endpoint) {
    loop {
        let Some(dial) = binding.accept().await else {
            return;
        };
        let Some(engine) = ctx.engine.upgrade() else {
            return;
        };
        let pipe = Pipe::new(ctx.options.pipe);
        let peer = match engine.admit(None, pipe.clone()) {
            Admitted::Peer(peer) => peer,
            Admitted::AtCeiling => {
                tracing::warn!(
                    ceiling = ctx.options.max_peers,
                    from = dial.from.to_string(),
                    "refused an inproc connection: this socket already holds its max_peers"
                );
                drop(dial);
                continue;
            }
            Admitted::SocketClosed => return,
        };
        let identity = engine.identity_slot(peer).unwrap_or_default();
        let user = engine.user_slot(peer).unwrap_or_default();
        let subscriptions = engine.subscriptions_of(peer).unwrap_or_else(|| {
            Arc::new(Subscriptions::new(
                ctx.options.max_subscriptions,
                ctx.options.max_subscription_bytes,
            ))
        });
        drop(engine);

        let ctx = ctx.clone();
        let endpoint = endpoint.clone();
        let exec = ctx.exec.clone();
        exec.spawn(async move {
            let outcome = run_session(
                &ctx,
                dial.stream,
                PeerSession {
                    peer,
                    pipe,
                    endpoint,
                    role: Role::Binder,
                    identity,
                    subscriptions,
                    // No address and no kernel: an in-process peer is this
                    // process, and 27/ZAP's address frame stays empty.
                    address: String::new(),
                    credentials: None,
                    user,
                },
            )
            .await;
            if let Err(e) = outcome {
                tracing::debug!(%peer, error = %e, "an inproc connection ended");
            }
            if let Some(engine) = ctx.engine.upgrade() {
                engine.forget(peer);
            }
        });
    }
}

/// Accepts connections on one bound `ipc://` endpoint.
///
/// The same shape as [`accept_loop`] and [`inproc_accept_loop`]. What is
/// specific to `AF_UNIX` happens twice: the socket file lives as long as the
/// [`IpcBinding`][crate::ipc::IpcBinding] this task holds, and every accepted
/// connection carries the kernel's statement about its peer, which is
/// recorded on the peer for an authorization handler to read.
#[cfg(unix)]
async fn ipc_accept_loop(ctx: TaskCtx, binding: crate::ipc::IpcBinding, endpoint: Endpoint) {
    loop {
        let stream = match binding.accept().await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::debug!(endpoint = %endpoint, error = %e, "an ipc accept failed");
                continue;
            }
        };
        let Some(engine) = ctx.engine.upgrade() else {
            return;
        };
        let pipe = Pipe::new(ctx.options.pipe);
        let peer = match engine.admit(None, pipe.clone()) {
            Admitted::Peer(peer) => peer,
            Admitted::AtCeiling => {
                tracing::warn!(
                    ceiling = ctx.options.max_peers,
                    "refused an ipc connection: this socket already holds its max_peers"
                );
                drop(stream);
                continue;
            }
            Admitted::SocketClosed => return,
        };
        let credentials = stream.peer_credentials();
        engine.set_credentials(peer, credentials);
        let identity = engine.identity_slot(peer).unwrap_or_default();
        let user = engine.user_slot(peer).unwrap_or_default();
        let subscriptions = engine.subscriptions_of(peer).unwrap_or_else(|| {
            Arc::new(Subscriptions::new(
                ctx.options.max_subscriptions,
                ctx.options.max_subscription_bytes,
            ))
        });
        drop(engine);

        let ctx = ctx.clone();
        let endpoint = endpoint.clone();
        let exec = ctx.exec.clone();
        exec.spawn(async move {
            let outcome = run_session(
                &ctx,
                stream,
                PeerSession {
                    peer,
                    pipe,
                    endpoint,
                    role: Role::Binder,
                    identity,
                    subscriptions,
                    // A filesystem endpoint has no IP to report, and the
                    // kernel's answer takes its place: 27/ZAP has no field
                    // for it, so it travels as the extension frame
                    // `crate::zap` documents.
                    address: String::new(),
                    credentials,
                    user,
                },
            )
            .await;
            if let Err(e) = outcome {
                tracing::debug!(%peer, error = %e, "an ipc connection ended");
            }
            if let Some(engine) = ctx.engine.upgrade() {
                engine.forget(peer);
            }
        });
    }
}

/// Dials one endpoint, forever, with `ZMQ_RECONNECT_IVL` backoff.
async fn connecter_loop(ctx: TaskCtx, peer: PeerId, endpoint: Endpoint) {
    let mut delay: Option<Duration> = None;
    let mut retried = false;
    loop {
        if let Some(delay) = delay {
            ctx.exec.sleep(delay).await;
        }

        let Some(engine) = ctx.engine.upgrade() else {
            return;
        };
        engine.count_attempt(peer);
        let Some(pipe) = engine.pipe_of(peer) else {
            // Disconnected while we waited.
            return;
        };
        let identity = engine.identity_slot(peer).unwrap_or_default();
        let user = engine.user_slot(peer).unwrap_or_default();
        let subscriptions = engine.subscriptions_of(peer).unwrap_or_else(|| {
            Arc::new(Subscriptions::new(
                ctx.options.max_subscriptions,
                ctx.options.max_subscription_bytes,
            ))
        });
        drop(engine);

        match dial(&ctx, &endpoint).await {
            Ok(stream) => {
                let credentials = stream.peer_credentials();
                if let Some(engine) = ctx.engine.upgrade() {
                    engine.set_credentials(peer, credentials);
                    engine.set_connected(peer, true);
                } else {
                    return;
                }
                let outcome = run_session(
                    &ctx,
                    stream,
                    PeerSession {
                        peer,
                        pipe,
                        endpoint: endpoint.clone(),
                        role: Role::Connecter,
                        identity: identity.clone(),
                        subscriptions: Arc::clone(&subscriptions),
                        // The address of a dialled peer is the one we
                        // dialled, which is the only one there is to report.
                        address: dialled_address(&endpoint),
                        credentials,
                        user: user.clone(),
                    },
                )
                .await;
                match ctx.engine.upgrade() {
                    Some(engine) => engine.set_connected(peer, false),
                    None => return,
                }
                if let Err(e) = outcome {
                    tracing::debug!(%peer, error = %e, "a dialled connection ended");
                }
                // A connection that was established resets the backoff: the
                // endpoint is reachable, so the next loss starts over at
                // ZMQ_RECONNECT_IVL.
                delay = None;
            }
            Err(e) => {
                tracing::debug!(%peer, endpoint = %endpoint, error = %e, "connect failed");
                // The next attempt is a retry, and the interval it waits is
                // what `ZMQ_EVENT_CONNECT_RETRIED` carries.
                retried = true;
            }
        }

        // The pipe survives, because it belongs to the endpoint rather than to
        // the connection; what ends here is only the attempt.
        match ctx.options.next_reconnect_ivl(delay) {
            Some(next) => {
                delay = Some(next);
                if retried {
                    ctx.monitor
                        .publish(EVENT_CONNECT_RETRIED, || MonitorEvent::ConnectRetried {
                            endpoint: endpoint.to_string(),
                            interval: next,
                        });
                }
            }
            // ZMQ_RECONNECT_IVL = -1: one attempt, and this endpoint is done.
            None => return,
        }
    }
}

/// One connect attempt, bounded by `ZMQ_CONNECT_TIMEOUT`.
///
/// **`inproc` has no attempt to bound.** There is no kernel, no address and
/// no handshake to time out: either the name is bound, in which case a buffer
/// pair is made at once, or nobody holds it, in which case the dial *waits*
/// for the bind — libzmq 4.0's "no longer requires bind before connect"
/// (`docs/research/zeromq.md` §12). So `ZMQ_CONNECT_TIMEOUT`, which
/// `zmq_setsockopt(3)` documents for TCP, is not applied to it; the caller
/// that wants to stop waiting drops the socket.
async fn dial(ctx: &TaskCtx, endpoint: &Endpoint) -> Result<Stream> {
    if let Endpoint::Inproc(name) = endpoint {
        loop {
            if let Some(stream) = ctx.context.inproc().dial(name, ctx.socket) {
                return Ok(stream);
            }
            ctx.context.inproc().wait_until_bound(name).await;
        }
    }
    #[cfg(unix)]
    if let Endpoint::Ipc(path) = endpoint {
        let attempt = crate::ipc::dial(path);
        return match ctx.options.connect_timeout {
            None => attempt.await,
            Some(limit) => match ctx.exec.within(limit, attempt).await {
                Some(result) => result,
                None => Err(Error::ETIMEDOUT(
                    format!(
                        "connect to {} gave up after {limit:?} (ZMQ_CONNECT_TIMEOUT)",
                        path.display()
                    )
                    .into(),
                )),
            },
        };
    }
    let Endpoint::Tcp { host, port } = endpoint else {
        return check_transport(endpoint).map(|()| unreachable!("every carried transport dialled"));
    };
    let addrs: Vec<SocketAddr> = match host {
        TcpHost::Ip(ip) => vec![SocketAddr::new(*ip, *port)],
        TcpHost::Name(name) => {
            ctx.exec
                .resolve(name, *port, ctx.options.max_resolved_addresses)
                .await?
        }
        TcpHost::Any => {
            return Err(Error::EINVAL(
                "the wildcard address binds, it does not connect".into(),
            ));
        }
    };

    let mut last = Error::EHOSTUNREACH("no address to dial".into());
    for addr in addrs {
        let attempt = TcpStream::connect(addr);
        let outcome = match ctx.options.connect_timeout {
            None => attempt.await.map_err(Error::from),
            Some(limit) => match ctx.exec.within(limit, attempt).await {
                Some(result) => result.map_err(Error::from),
                None => Err(Error::ETIMEDOUT(
                    format!("connect to {addr} gave up after {limit:?} (ZMQ_CONNECT_TIMEOUT)")
                        .into(),
                )),
            },
        };
        match outcome {
            Ok(stream) => {
                let stream = Stream::tcp(stream);
                let _ = stream.set_nodelay(true);
                return Ok(stream);
            }
            Err(e) => last = e,
        }
    }
    Err(last)
}

/// The address to report to a ZAP handler for an endpoint this socket
/// dialled.
///
/// A dialled `tcp://` endpoint's host, because that is the peer: 27/ZAP wants
/// an IP, and a name that was resolved is still the address the application
/// named. Empty for every local transport, which has no address at all.
fn dialled_address(endpoint: &Endpoint) -> String {
    match endpoint {
        Endpoint::Tcp { host, .. } => match host {
            TcpHost::Ip(ip) => ip.to_string(),
            TcpHost::Name(name) => name.clone(),
            TcpHost::Any => String::new(),
        },
        _ => String::new(),
    }
}

/// Everything one peer contributes to a connection: what the engine holds
/// for it, gathered so that starting a session is one argument rather than
/// six that must be kept in the same order at two call sites.
struct PeerSession {
    peer: PeerId,
    pipe: Pipe,
    endpoint: Endpoint,
    role: Role,
    identity: AnnouncedIdentity,
    subscriptions: Arc<Subscriptions>,
    /// The peer's address, for a ZAP request: an IP where the transport has
    /// one, and empty otherwise.
    address: String,
    /// The kernel's statement about a local peer.
    credentials: Option<weida_core::LocalPrincipal>,
    /// Where the session records what a ZAP handler said.
    user: AuthenticatedUser,
}

/// Hands a connection to the session, enforcing `ZMQ_HANDSHAKE_IVL` and
/// publishing the connection's lifecycle to the monitor.
///
/// Every connection of every transport passes through here, dialled or
/// accepted, which is why the monitor events live here rather than in each
/// accept loop: one place that knows a connection started, whether its
/// handshake completed, and how it ended.
async fn run_session(ctx: &TaskCtx, stream: Stream, peer: PeerSession) -> Result<()> {
    let endpoint = peer.endpoint.to_string();
    let role = peer.role;
    ctx.monitor.publish(
        match role {
            Role::Binder => EVENT_ACCEPTED,
            Role::Connecter => EVENT_CONNECTED,
        },
        || match role {
            Role::Binder => MonitorEvent::Accepted {
                endpoint: endpoint.clone(),
            },
            Role::Connecter => MonitorEvent::Connected {
                endpoint: endpoint.clone(),
            },
        },
    );

    let (done, handshaken) = oneshot::channel();
    let connection = Connection {
        stream,
        pipe: peer.pipe,
        peer: peer.peer,
        role: peer.role,
        endpoint: peer.endpoint,
        options: ctx.options.clone(),
        exec: ctx.exec.clone(),
        handshake: HandshakeGate { done: Some(done) },
        identity: peer.identity,
        subscriptions: peer.subscriptions,
        context: ctx.context.clone(),
        peer_address: peer.address,
        credentials: peer.credentials,
        user: peer.user,
    };
    let mut session = ctx.session.run(connection);
    let mut handshaken = handshaken;
    let mut succeeded = false;

    // `ZMQ_HANDSHAKE_IVL = 0` is no limit, which is also how a silent peer
    // holds a connection open; it is not the default. Either way the gate is
    // watched, because the handshake completing is itself an event.
    let limit = ctx.options.handshake_ivl;
    let deadline = async {
        match limit {
            Some(limit) => ctx.exec.sleep(limit).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(deadline);

    let early: Option<Result<()>> = tokio::select! {
        outcome = &mut session => Some(outcome),
        // The gate closed: `Ok` is the handshake completing, `Err` is the
        // sender dropped because the session is ending. Either way the
        // handshake bound is spent.
        gate = &mut handshaken => {
            if gate.is_ok() {
                succeeded = true;
                ctx.monitor.publish(EVENT_HANDSHAKE_SUCCEEDED, || {
                    MonitorEvent::HandshakeSucceeded { endpoint: endpoint.clone() }
                });
            }
            None
        }
        () = &mut deadline => Some(Err(Error::ETIMEDOUT(
            format!(
                "the handshake did not complete within {:?} (ZMQ_HANDSHAKE_IVL)",
                limit.unwrap_or_default()
            )
            .into(),
        ))),
    };
    // Dropping the session future drops the stream it owns, which is how
    // libzmq's "the connection is closed" happens here.
    let outcome = match early {
        Some(outcome) => outcome,
        None => session.await,
    };

    match &outcome {
        // The pipe was destroyed: this side closed the connection.
        Ok(()) => ctx
            .monitor
            .publish(EVENT_CLOSED, || MonitorEvent::Closed { endpoint }),
        // A connection that got past its handshake and then ended is a
        // disconnect; one that did not is a handshake failure, split the way
        // libzmq splits it.
        Err(_) if succeeded => {
            ctx.monitor
                .publish(EVENT_DISCONNECTED, || MonitorEvent::Disconnected {
                    endpoint,
                })
        }
        Err(e) => {
            let event = MonitorEvent::handshake_failure(endpoint, e);
            ctx.monitor.publish(event.id(), || event);
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;
    use crate::message::Multipart;
    use crate::pipe::Sent;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt;

    /// A test double for [`Session`], standing where `ZmtpSession` stands. It
    /// records what arrived, optionally completes the handshake, and then holds
    /// the connection until the peer closes it — enough to observe the engine,
    /// and deliberately not a protocol.
    struct Recorder {
        connections: Arc<Mutex<Vec<(PeerId, Role)>>>,
        completes_handshake: bool,
        dropped: Arc<AtomicUsize>,
    }

    impl Recorder {
        fn new() -> Arc<Recorder> {
            Arc::new(Recorder {
                connections: Arc::new(Mutex::new(Vec::new())),
                completes_handshake: true,
                dropped: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn silent() -> Arc<Recorder> {
            Arc::new(Recorder {
                connections: Arc::new(Mutex::new(Vec::new())),
                completes_handshake: false,
                dropped: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn count(&self) -> usize {
            self.connections.lock().expect("recorder").len()
        }

        fn roles(&self) -> Vec<Role> {
            self.connections
                .lock()
                .expect("recorder")
                .iter()
                .map(|(_, role)| *role)
                .collect()
        }
    }

    /// Counts a session future dropped before it finished, which is what
    /// ZMQ_HANDSHAKE_IVL does to a connection.
    struct DropCount(Arc<AtomicUsize>, bool);

    impl Drop for DropCount {
        fn drop(&mut self) {
            if !self.1 {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    impl Session for Recorder {
        fn run(&self, mut connection: Connection) -> SessionFuture {
            self.connections
                .lock()
                .expect("recorder")
                .push((connection.peer, connection.role));
            let completes = self.completes_handshake;
            let dropped = Arc::clone(&self.dropped);
            Box::pin(async move {
                let mut guard = DropCount(dropped, false);
                if completes {
                    connection.handshake.complete();
                }
                // Hold the connection until the peer goes away: a read that
                // returns zero is the peer's close.
                let mut byte = [0u8; 1];
                let _ = connection.stream.read(&mut byte).await;
                guard.1 = true;
                Ok(())
            })
        }
    }

    fn options() -> SocketOptions {
        SocketOptions {
            reconnect_ivl: Some(Duration::from_millis(20)),
            ..SocketOptions::default()
        }
    }

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    /// Coerces the test double to the trait object `Engine::new` takes;
    /// `Arc::clone` alone cannot, because the expected type propagates into
    /// its parameter.
    fn as_session(recorder: &Arc<Recorder>) -> Arc<dyn Session> {
        let same: Arc<Recorder> = Arc::clone(recorder);
        same
    }

    async fn wait_for(mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "condition never held");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Claim: a wildcard bind is only usable through the endpoint it reports —
    /// `ZMQ_LAST_ENDPOINT` — and that endpoint unbinds it. The requested form
    /// unbinds it too, and an endpoint nothing is bound at is `ENOENT`.
    #[tokio::test]
    async fn a_wildcard_bind_reports_the_endpoint_it_got() {
        let ctx = context();
        let engine = Engine::new(&ctx, options(), Recorder::new()).expect("engine");

        let requested = Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint");
        let bound = engine.bind(&requested).await.expect("bind");
        assert!(!bound.has_wildcard_port(), "the port must be resolved");
        assert_eq!(engine.last_endpoint().as_ref(), Some(&bound));
        assert_eq!(engine.bound(), vec![bound.clone()]);

        let err = engine
            .unbind(&Endpoint::parse("tcp://127.0.0.1:1").expect("endpoint"))
            .unwrap_err();
        assert_eq!(err.errno(), "ENOENT", "{err}");

        engine
            .unbind(&bound)
            .expect("unbind by the reported endpoint");
        assert!(engine.bound().is_empty());

        // And the requested form works too, for a caller that kept it.
        let bound = engine.bind(&requested).await.expect("rebind");
        engine
            .unbind(&requested)
            .expect("unbind by the requested form");
        assert!(engine.bound().is_empty());
        drop(bound);
    }

    /// Claim: an `inproc://` bind and connect meet inside one context, both
    /// sides get a session with the role they played, and the name is
    /// released when the bind is undone — the `ipc` transport's stealing is
    /// what `inproc` does not do.
    #[tokio::test]
    async fn an_inproc_bind_and_connect_meet_in_one_context() {
        let ctx = context();
        let binder = Recorder::new();
        let server = Engine::new(&ctx, options(), as_session(&binder)).expect("engine");
        let dialler = Recorder::new();
        let client = Engine::new(&ctx, options(), as_session(&dialler)).expect("engine");

        let endpoint = Endpoint::parse("inproc://orders").expect("endpoint");
        let bound = server.bind(&endpoint).await.expect("bind");
        assert_eq!(bound, endpoint, "an inproc bind has nothing to resolve");
        assert_eq!(server.last_endpoint().as_ref(), Some(&endpoint));
        assert!(ctx.inproc().is_bound("orders"));

        client.connect(&endpoint).expect("connect");
        wait_for(|| binder.count() == 1 && dialler.count() == 1).await;
        assert_eq!(binder.roles(), vec![Role::Binder]);
        assert_eq!(dialler.roles(), vec![Role::Connecter]);

        // A second socket cannot take a name somebody holds: no stealing.
        let other = Engine::new(&ctx, options(), Recorder::new()).expect("engine");
        let err = other.bind(&endpoint).await.unwrap_err();
        assert_eq!(err.errno(), "EADDRINUSE", "{err}");

        server.unbind(&endpoint).expect("unbind");
        wait_for(|| !ctx.inproc().is_bound("orders")).await;
        other.bind(&endpoint).await.expect("free after the unbind");
    }

    /// Claim: a connect that arrives before the bind **parks** and completes
    /// when the bind arrives, which is what libzmq 4.0 changed — and the
    /// pipe existed the whole time, because a queue belongs to the endpoint
    /// rather than to the connection.
    #[tokio::test]
    async fn an_inproc_connect_before_the_bind_completes_on_it() {
        let ctx = context();
        let dialler = Recorder::new();
        let client = Engine::new(&ctx, options(), as_session(&dialler)).expect("engine");
        let endpoint = Endpoint::parse("inproc://later").expect("endpoint");

        let peer = client.connect(&endpoint).expect("connect before any bind");
        assert!(client.peer(peer).is_some(), "the queue exists at once");
        assert_eq!(dialler.count(), 0, "and nothing is connected yet");

        let binder = Recorder::new();
        let server = Engine::new(&ctx, options(), as_session(&binder)).expect("engine");
        server.bind(&endpoint).await.expect("bind");
        wait_for(|| dialler.count() == 1 && binder.count() == 1).await;
        wait_for(|| client.peer(peer).is_some_and(|peer| peer.connected)).await;
    }

    /// Claim: one socket binds many endpoints and connects many, and accepts
    /// automatically on each — `zmq_socket(3)`'s "many-to-many".
    #[tokio::test]
    async fn one_socket_binds_and_connects_many_endpoints() {
        let ctx = context();
        let server_session = Recorder::new();
        let server = Engine::new(&ctx, options(), as_session(&server_session)).expect("server");
        let first = server
            .bind(&Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint"))
            .await
            .expect("first bind");
        let second = server
            .bind(&Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint"))
            .await
            .expect("second bind");
        assert_eq!(server.bound().len(), 2);

        let client_session = Recorder::new();
        let client = Engine::new(&ctx, options(), as_session(&client_session)).expect("client");
        client.connect(&first).expect("connect one");
        client.connect(&second).expect("connect two");
        assert_eq!(client.connected().len(), 2);

        wait_for(|| server_session.count() == 2 && client_session.count() == 2).await;
        assert_eq!(server_session.roles(), vec![Role::Binder, Role::Binder]);
        assert_eq!(
            client_session.roles(),
            vec![Role::Connecter, Role::Connecter]
        );
        wait_for(|| client.peers().iter().all(|peer| peer.connected)).await;
        assert_eq!(client.peers().len(), 2);
        assert_eq!(server.peers().len(), 2);
    }

    /// Claim: the queue exists before the connection does — the pattern
    /// RFCs' "whether or not the connection is established" — so a sender may
    /// fill it while the endpoint is still being dialled.
    #[tokio::test]
    async fn a_peer_that_never_connected_still_has_a_queue() {
        let ctx = context();
        let engine = Engine::new(&ctx, options(), Recorder::new()).expect("engine");
        // Port 9 is discard, and nothing listens on it here.
        let nowhere = Endpoint::parse("tcp://127.0.0.1:9").expect("endpoint");
        let peer = engine.connect(&nowhere).expect("connect");

        let peers = engine.outgoing_peers();
        assert_eq!(peers.len(), 1, "the peer is there immediately");
        assert!(!peers[0].connected);
        assert_eq!(
            peers[0]
                .pipe
                .outgoing()
                .send(Multipart::single("queued for nobody"))
                .await
                .expect("queued"),
            Sent::Queued
        );
        assert_eq!(engine.peer(peer).expect("peer").pipe.outgoing().len(), 1);

        // And it is dialling, not sitting still.
        wait_for(|| engine.peer(peer).map(|p| p.attempts) >= Some(2)).await;
    }

    /// Claim: `ZMQ_IMMEDIATE` hides a peer from the sender until its
    /// connection completes, which is what "messages shall be queued only to
    /// completed connections" means for a round-robin socket.
    #[tokio::test]
    async fn immediate_hides_a_peer_until_it_connects() {
        let ctx = context();
        let server = Engine::new(&ctx, options(), Recorder::new()).expect("server");
        let endpoint = server
            .bind(&Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint"))
            .await
            .expect("bind");

        let immediate = SocketOptions {
            immediate: true,
            ..options()
        };
        let client = Engine::new(&ctx, immediate, Recorder::new()).expect("client");
        let nowhere = Endpoint::parse("tcp://127.0.0.1:9").expect("endpoint");
        client.connect(&nowhere).expect("connect to nothing");
        assert_eq!(client.peers().len(), 1, "the peer and its queue exist");
        assert!(
            client.outgoing_peers().is_empty(),
            "but a sender may not use it yet"
        );

        client.connect(&endpoint).expect("connect to the server");
        wait_for(|| client.outgoing_peers().len() == 1).await;
        assert!(client.outgoing_peers()[0].connected);
        assert_eq!(client.peers().len(), 2);
    }

    /// Claim: a lost connection is redialled, so a peer whose listener goes
    /// away and comes back is reconnected without the application asking.
    #[tokio::test]
    async fn a_dropped_peer_is_reconnected_when_it_returns() {
        let ctx = context();
        let session = Recorder::new();
        let engine = Engine::new(&ctx, options(), as_session(&session)).expect("engine");

        // A peer that accepts one connection and immediately drops it.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let port = listener.local_addr().expect("addr").port();
        let endpoint = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).expect("endpoint");
        let accepted = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            drop(stream);
            drop(listener);
        });

        let peer = engine.connect(&endpoint).expect("connect");
        wait_for(|| session.count() == 1).await;
        accepted.await.expect("the peer served once");
        wait_for(|| engine.peer(peer).is_some_and(|p| !p.connected)).await;

        // The peer comes back on the same port, and nobody told the socket.
        let again = TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .expect("rebind");
        let served = tokio::spawn(async move {
            let (stream, _) = again.accept().await.expect("accept");
            // Hold it while the assertions below run. Margin: 200 ms
            // against `wait_for`'s 5 ms polling, and the assertions are
            // waits rather than samples, so this only has to outlast them.
            tokio::time::sleep(Duration::from_millis(200)).await;
            drop(stream);
        });
        wait_for(|| session.count() == 2).await;
        wait_for(|| engine.peer(peer).is_some_and(|p| p.connected)).await;
        assert!(
            engine.peer(peer).expect("peer").attempts >= 2,
            "the reconnect is a second attempt, not a retained connection"
        );
        served.await.expect("the peer served twice");
    }

    /// Claim: `ZMQ_CONNECT_TIMEOUT` bounds one attempt, so a black-hole
    /// address does not park the socket on the OS's SYN retries. RFC 8305's
    /// 250 ms scale, not the tens of seconds Linux would take.
    #[tokio::test]
    async fn connect_timeout_bounds_an_attempt_against_a_black_hole() {
        let ctx = context();
        let bounded = SocketOptions {
            connect_timeout: Some(Duration::from_millis(250)),
            reconnect_ivl: Some(Duration::from_millis(20)),
            ..SocketOptions::default()
        };
        let engine = Engine::new(&ctx, bounded, Recorder::new()).expect("engine");
        // TEST-NET-3 (RFC 5737): documentation space, routed nowhere.
        let black_hole = Endpoint::parse("tcp://203.0.113.1:5555").expect("endpoint");
        let peer = engine.connect(&black_hole).expect("connect");

        let started = std::time::Instant::now();
        wait_for(|| engine.peer(peer).map(|p| p.attempts) >= Some(2)).await;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "two attempts must fit in seconds, not in SYN retries: {:?}",
            started.elapsed()
        );
        assert!(!engine.peer(peer).expect("peer").connected);
    }

    /// Claim: `ZMQ_HANDSHAKE_IVL` closes a connection whose handshake never
    /// completes — the defence against a peer that connects and says nothing.
    #[tokio::test]
    async fn a_handshake_that_never_completes_is_closed() {
        let ctx = context();
        let silent = Recorder::silent();
        let dropped = Arc::clone(&silent.dropped);
        let engine = Engine::new(
            &ctx,
            SocketOptions {
                // Margin: a 30 ms handshake interval against a peer that is
                // held open for 5 s, so the interval is two orders of
                // magnitude inside the window in which it must fire; the
                // assertions wait for the drop rather than sampling for it.
                handshake_ivl: Some(Duration::from_millis(30)),
                // One attempt, so the assertion counts one connection.
                reconnect_ivl: None,
                ..SocketOptions::default()
            },
            as_session(&silent),
        )
        .expect("engine");

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let port = listener.local_addr().expect("addr").port();
        let held = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            // A peer that connects and never speaks: held open deliberately.
            tokio::time::sleep(Duration::from_secs(5)).await;
            drop(stream);
        });

        let endpoint = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).expect("endpoint");
        let peer = engine.connect(&endpoint).expect("connect");
        wait_for(|| silent.count() == 1).await;
        wait_for(|| dropped.load(Ordering::SeqCst) == 1).await;
        wait_for(|| engine.peer(peer).is_some_and(|p| !p.connected)).await;
        held.abort();
    }

    /// Claim: `disconnect` destroys the endpoint's pipe and the messages in
    /// it are counted rather than quietly lost — the RFCs' "discarding any
    /// messages it contains", made a number.
    #[tokio::test]
    async fn disconnect_destroys_the_pipe_and_counts_what_it_held() {
        let ctx = context();
        let engine = Engine::new(&ctx, options(), Recorder::new()).expect("engine");
        let nowhere = Endpoint::parse("tcp://127.0.0.1:9").expect("endpoint");
        let peer = engine.connect(&nowhere).expect("connect");

        let pipe = engine.peer(peer).expect("peer").pipe;
        for n in 0..3 {
            pipe.outgoing()
                .send(Multipart::single(format!("{n}")))
                .await
                .expect("queued");
        }

        let discarded = engine.disconnect(&nowhere).expect("disconnect");
        assert_eq!(
            discarded,
            Discarded {
                outgoing: 3,
                incoming: 0
            }
        );
        assert!(engine.peers().is_empty());
        assert!(engine.connected().is_empty());
        assert!(pipe.outgoing().is_closed());

        let err = engine.disconnect(&nowhere).unwrap_err();
        assert_eq!(err.errno(), "ENOENT", "{err}");
    }

    /// Claim: every transport this engine carries is accepted where it is
    /// asked for, and the wildcard address — which binds — is refused as a
    /// dial. A transport the library does not implement at all is refused by
    /// the endpoint parser, where it is named; there is no half-accepted
    /// endpoint anywhere in between.
    #[tokio::test]
    async fn every_carried_transport_is_accepted_and_the_wildcard_is_not_dialled() {
        let ctx = context();
        let engine = Engine::new(&ctx, options(), Recorder::new()).expect("engine");

        let inproc = Endpoint::parse("inproc://orders").expect("endpoint");
        engine.bind(&inproc).await.expect("inproc is carried");
        engine.connect(&inproc).expect("inproc is carried");

        #[cfg(unix)]
        {
            let path = std::env::temp_dir().join(format!("weida-zmq-{}-eng.sock", nonce()));
            let ipc = Endpoint::parse(&format!("ipc://{}", path.display())).expect("endpoint");
            engine.bind(&ipc).await.expect("ipc is carried");
            engine.connect(&ipc).expect("ipc is carried");
        }

        let err = Endpoint::parse("epgm://239.0.0.1:5555").unwrap_err();
        assert_eq!(err.errno(), "EPROTONOSUPPORT", "{err}");

        // The wildcard binds; it does not dial.
        let err = engine
            .connect(&Endpoint::parse("tcp://*:5555").expect("endpoint"))
            .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
    }

    /// A number no other test in this process uses, for a socket path.
    #[cfg(unix)]
    fn nonce() -> u64 {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, Ordering::SeqCst) as u64;
        u64::from(std::process::id()) * 1000 + n
    }

    /// Claim: an `ipc://` bind and connect meet over `AF_UNIX`, both sides
    /// are recorded with the kernel's statement about the other, and the
    /// socket file is gone once the bind is undone — the node belongs to the
    /// binding.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_ipc_bind_and_connect_meet_with_credentials() {
        let ctx = context();
        let binder = Recorder::new();
        let server = Engine::new(&ctx, options(), as_session(&binder)).expect("engine");
        let dialler = Recorder::new();
        let client = Engine::new(&ctx, options(), as_session(&dialler)).expect("engine");

        let path = std::env::temp_dir().join(format!("weida-zmq-{}-meet.sock", nonce()));
        let endpoint = Endpoint::parse(&format!("ipc://{}", path.display())).expect("endpoint");
        let bound = server.bind(&endpoint).await.expect("bind");
        assert_eq!(bound, endpoint, "an ipc bind has nothing to resolve");
        assert!(path.exists(), "the socket file is the endpoint");

        let peer = client.connect(&endpoint).expect("connect");
        wait_for(|| binder.count() == 1 && dialler.count() == 1).await;
        assert_eq!(binder.roles(), vec![Role::Binder]);
        assert_eq!(dialler.roles(), vec![Role::Connecter]);

        // Both ends hold the kernel's answer, which for two ends of one
        // connection in one process is this process.
        let dialled = client.peer(peer).expect("the dialled peer");
        let credentials = dialled.credentials.expect("credentials at connect");
        if let Some(pid) = credentials.pid {
            assert_eq!(pid, std::process::id());
        }
        wait_for(|| {
            server
                .peers()
                .first()
                .is_some_and(|peer| peer.credentials.is_some())
        })
        .await;

        server.unbind(&endpoint).expect("unbind");
        wait_for(|| !path.exists()).await;
    }

    /// Claim: a TCP peer has no credentials to report — there is no kernel
    /// fact about a process behind an address, and `None` says so rather
    /// than a zero that would read as root.
    #[tokio::test]
    async fn a_tcp_peer_has_no_credentials() {
        let ctx = context();
        let binder = Recorder::new();
        let server = Engine::new(&ctx, options(), as_session(&binder)).expect("engine");
        let bound = server
            .bind(&Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint"))
            .await
            .expect("bind");
        let client = Engine::new(&ctx, options(), Recorder::new()).expect("engine");
        let peer = client.connect(&bound).expect("connect");
        wait_for(|| client.peer(peer).is_some_and(|peer| peer.connected)).await;
        assert!(client.peer(peer).expect("peer").credentials.is_none());
    }

    /// Claim: closing the socket stops everything and destroys the pipes, and
    /// a terminated context refuses further operations with `ETERM`.
    #[tokio::test]
    async fn a_closed_socket_and_a_terminated_context_refuse_work() {
        let ctx = context();
        let engine = Engine::new(&ctx, options(), Recorder::new()).expect("engine");
        let bound = engine
            .bind(&Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint"))
            .await
            .expect("bind");
        let peer = engine.connect(&bound).expect("connect");
        wait_for(|| engine.peer(peer).is_some_and(|p| p.connected)).await;

        engine.close();
        assert!(engine.peers().is_empty());
        assert!(engine.bound().is_empty());
        let err = engine.connect(&bound).unwrap_err();
        assert_eq!(err.errno(), "ENOTSOCK", "{err}");

        let terminating = ctx.clone();
        let report = terminating.shutdown().await;
        assert_eq!(report.outstanding(), 1, "the socket slot is still held");
        let err = engine.bind(&bound).await.unwrap_err();
        assert_eq!(err.errno(), "ETERM", "{err}");
    }

    /// Claim: **a stranger cannot make one socket hold unboundedly many
    /// peers.** `max_peers` bounds the accepted connections; the excess is
    /// closed rather than admitted, leaves no entry behind, and the admitted
    /// one keeps working. Neither `ZMQ_MAX_SOCKETS` nor `ZMQ_BACKLOG` is
    /// this bound, which is why the option exists.
    #[tokio::test]
    async fn accepted_peers_stop_at_max_peers() {
        use tokio::io::AsyncReadExt;

        let ctx = context();
        let session = Recorder::new();
        let engine = Engine::new(
            &ctx,
            SocketOptions {
                max_peers: 1,
                ..options()
            },
            as_session(&session),
        )
        .expect("engine");
        let bound = engine
            .bind(&Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint"))
            .await
            .expect("bind");
        let addr: std::net::SocketAddr = bound.to_string()["tcp://".len()..].parse().expect("addr");

        let admitted = TcpStream::connect(addr).await.expect("first connection");
        wait_for(|| engine.peers().len() == 1).await;

        // Three more, all past the ceiling: each is accepted by the kernel
        // and then closed, which a foreign peer observes as an immediate EOF.
        for _ in 0..3 {
            let mut refused = TcpStream::connect(addr)
                .await
                .expect("a further connection");
            let mut byte = [0u8; 1];
            let read = tokio::time::timeout(Duration::from_secs(5), refused.read(&mut byte))
                .await
                .expect("the refusal was not left hanging")
                .expect("read");
            assert_eq!(read, 0, "a refused connection must be closed, not served");
        }

        // No entry is left behind, and the admitted peer is untouched.
        assert_eq!(engine.peers().len(), 1, "the ceiling held");
        assert_eq!(session.count(), 1, "only the admitted one got a session");
        assert!(engine.peers()[0].connected);

        // And a slot freed by a peer going away is usable again.
        drop(admitted);
        wait_for(|| engine.peers().is_empty()).await;
        let _next = TcpStream::connect(addr)
            .await
            .expect("after the slot freed");
        wait_for(|| engine.peers().len() == 1).await;
    }
}
