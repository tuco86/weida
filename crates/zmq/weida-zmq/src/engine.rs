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

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::futures::Notified;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use weida_runtime::Exec;

use crate::context::{Context, SocketId, SocketSlot};
use crate::endpoint::{Endpoint, TcpHost};
use crate::error::{Error, Result};
use crate::identity::RoutingId;
use crate::inproc::InprocBinding;
use crate::monitor::{
    EVENT_ACCEPT_FAILED, EVENT_ACCEPTED, EVENT_BIND_FAILED, EVENT_CLOSED, EVENT_CONNECT_DELAYED,
    EVENT_CONNECT_RETRIED, EVENT_CONNECTED, EVENT_DISCONNECTED, EVENT_HANDSHAKE_SUCCEEDED,
    EVENT_LISTENING, Monitor, MonitorEvent, MonitorEvents, MonitorSink,
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
    /// Where the engine learns that this slot changed, set by the engine when
    /// it first publishes the peer; see [`AnnouncedIdentity::watch`].
    changes: Option<Arc<PeerChanges>>,
}

impl AnnouncedIdentity {
    /// Records the handshake's outcome: the identity the peer chose, or
    /// `None` where it chose none. Called once, after the handshake.
    ///
    /// **The last thing a session records about a connection**, after the
    /// user id a ZAP handler returned: the engine rebuilds the snapshot it
    /// publishes after this call, so a socket that reads [`Peer::user_id`]
    /// beside [`Peer::identity`] sees both, and a ROUTER parked on the
    /// engine's bells is woken to learn the peer.
    pub fn announce(&self, identity: Option<RoutingId>) {
        let changes = {
            let mut slot = lock_unpoisoned(&self.0);
            slot.announced = true;
            slot.identity = identity;
            slot.changes.clone()
        };
        if let Some(changes) = changes {
            changes.notify_waiters();
        }
    }

    /// What the peer announced, if anything. A peer that announced nothing
    /// gets an identity generated by whoever needs one.
    pub fn get(&self) -> Option<RoutingId> {
        lock_unpoisoned(&self.0).identity.clone()
    }

    /// Whether the handshake has happened at all, which is when the answer
    /// above becomes meaningful.
    pub fn is_announced(&self) -> bool {
        lock_unpoisoned(&self.0).announced
    }

    /// Both answers, taken under one lock so that they agree.
    fn read(&self) -> (Option<RoutingId>, bool) {
        let slot = self.0.lock().expect("identity slot poisoned");
        (slot.identity.clone(), slot.announced)
    }

    /// Tells this slot where to report a change. Idempotent; called when the
    /// engine publishes the peer, which is before any session could have
    /// announced anything a published snapshot would miss: an announcement
    /// that landed earlier is read by the snapshot being built, and one that
    /// lands later reports itself.
    fn watch(&self, changes: &Arc<PeerChanges>) {
        self.0
            .lock()
            .expect("identity slot poisoned")
            .changes
            .get_or_insert_with(|| Arc::clone(changes));
    }
}

/// What a change to the peer set — or to anything a [`Peer`] reports — has
/// to do besides changing it: drop the snapshot the engine publishes, and
/// ring the two bells a socket's waits are parked on.
///
/// Shared by the engine and by every peer's [`AnnouncedIdentity`], which is
/// how an announcement — made by a session on its own task, with no access
/// to the engine's state — reaches the socket that is waiting for it.
#[derive(Debug, Default)]
struct PeerChanges {
    /// The published snapshot, built on demand and dropped on every change,
    /// so it is never stale and a departed peer's queues and subscription
    /// table are released at once rather than at the next rebuild.
    view: Mutex<Option<View>>,
    /// How many times the peer set has changed in a way a reader reconciles
    /// against — a peer arrived, connected, went or announced itself. A
    /// reader that remembers the number it last looked at knows, with one
    /// load and no lock, that nothing has changed since, and holds nothing
    /// of the peers it looked at while it waits to find out.
    generation: AtomicU64,
    /// Rung when a receive may now find something: a message arrived on any
    /// peer's incoming queue, or the peer set changed. Every pipe the engine
    /// publishes is attached to it ([`Pipe::attach`]).
    readable: Arc<Notify>,
    /// Rung when a send may now find room: a message left any peer's outgoing
    /// queue, or the peer set changed.
    writable: Arc<Notify>,
}

/// What the engine publishes about its peers: the same list twice when every
/// peer is usable, and the connected ones alone as the second when
/// `ZMQ_IMMEDIATE` hides the others from a sender.
#[derive(Debug)]
struct View {
    all: Arc<[Peer]>,
    outgoing: Arc<[Peer]>,
}

impl PeerChanges {
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Drops the published snapshot without waking anybody: for a change no
    /// wait can be parked on and no reader reconciles against, such as a
    /// connect attempt being counted.
    fn touch(&self) {
        *self.view.lock().expect("peer view poisoned") = None;
    }

    /// Drops the published snapshot, moves the generation on and rings both
    /// bells, so that every wait looks again at a peer set that has changed
    /// under it.
    ///
    /// In that order: a reader that sees the new generation must find the old
    /// snapshot already gone, or it would reconcile against the old peers and
    /// remember the new number.
    fn notify_waiters(&self) {
        self.touch();
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.readable.notify_waiters();
        self.writable.notify_waiters();
    }
}

/// What admission created for a peer it let in: the slots its session records
/// into, and the switch that evicts it.
struct PeerSlots {
    peer: PeerId,
    identity: AnnouncedIdentity,
    user: AuthenticatedUser,
    subscriptions: Arc<Subscriptions>,
    /// Fires when the engine evicts the peer; see [`Handshaking`].
    eviction: oneshot::Receiver<()>,
}

/// What [`EngineInner::admit`] decided about an accepted connection.
enum Admitted {
    /// Admitted, as this peer.
    Peer(PeerSlots),
    /// Refused: this socket holds `max_peers` accepted connections, and none of
    /// their handshakes has stalled long enough to give way.
    AtCeiling,
    /// Refused: this address holds its share of the table, and none of its own
    /// handshakes has stalled long enough to give way.
    PerAddress,
    /// Refused: the socket is closed, so there is nothing to accept onto.
    SocketClosed,
}

/// How long a handshake may stall before a newcomer may evict it from a full
/// socket. A handshake over a WAN takes a few round trips, so two seconds is
/// generous to a real peer and short for a silent one.
const HANDSHAKE_EVICT_AFTER: Duration = Duration::from_secs(2);

/// The first sleep after a failed accept.
const ACCEPT_BACKOFF_MIN: Duration = Duration::from_millis(10);

/// The longest sleep between accept attempts while they keep failing.
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// The divisor that sizes one host's share of `max_peers`.
const PER_ADDRESS_DIVISOR: usize = 8;

/// The fewest peers one host may hold, whatever `max_peers` is.
const PER_ADDRESS_FLOOR: usize = 8;

/// How often a refused connection or a failed accept is logged, per socket or
/// per listener.
const LOG_PERIOD: Duration = Duration::from_secs(1);

/// How many accepted peers one host may hold: its `1 / PER_ADDRESS_DIVISOR`
/// share of `max_peers`, never fewer than [`PER_ADDRESS_FLOOR`] and never more
/// than `max_peers`. A default socket admits 128 peers from one host, and a PAIR
/// with a `max_peers` of 1 admits one. A host is what [`weida_core::host_key`]
/// says it is, the key `weida-nng` counts its peers under too.
fn per_address_cap(max_peers: usize) -> usize {
    (max_peers / PER_ADDRESS_DIVISOR)
        .max(PER_ADDRESS_FLOOR)
        .min(max_peers)
}

/// One accepted peer whose handshake has not completed.
struct Handshaking {
    /// When the peer was admitted, which is when its handshake started.
    since: Instant,
    /// Sending on this evicts the peer: its session ends and its connection
    /// closes. Dropping it without sending evicts nobody, which is what a
    /// completed handshake and a released peer do.
    kill: oneshot::Sender<()>,
}

/// What the engine knows about accepted peers beyond [`EngineState`]: how many
/// of them each address key holds, and which handshakes are still running,
/// since those are the ones a newcomer may evict. Lock order: `state`, then
/// `admission`.
#[derive(Default)]
struct Admission {
    /// The accepted peers whose handshake has not completed, in id order, which
    /// is admission order.
    handshaking: BTreeMap<PeerId, Handshaking>,
    /// The key of each accepted peer that counts against the per-address cap,
    /// as [`weida_core::host_key`] makes it. Loopback peers have no entry.
    addresses: HashMap<PeerId, IpAddr>,
    /// How many accepted peers each key holds.
    per_address: HashMap<IpAddr, usize>,
}

impl Admission {
    /// Starts counting an admitted peer and its handshake.
    fn track(
        &mut self,
        peer: PeerId,
        address: Option<IpAddr>,
        since: Instant,
        kill: oneshot::Sender<()>,
    ) {
        self.handshaking.insert(peer, Handshaking { since, kill });
        if let Some(address) = address {
            self.addresses.insert(peer, address);
            *self.per_address.entry(address).or_insert(0) += 1;
        }
    }

    /// Stops counting `peer`, which went away or was evicted. Returns its kill
    /// switch when its handshake had not completed.
    fn release(&mut self, peer: PeerId) -> Option<oneshot::Sender<()>> {
        let kill = self
            .handshaking
            .remove(&peer)
            .map(|handshake| handshake.kill);
        if let Some(address) = self.addresses.remove(&peer)
            && let Some(held) = self.per_address.get_mut(&address)
        {
            *held -= 1;
            if *held == 0 {
                self.per_address.remove(&address);
            }
        }
        kill
    }

    /// The oldest handshake that has stalled for [`HANDSHAKE_EVICT_AFTER`] as of
    /// `now`, among the peers of `address` when one is given.
    fn stalled(&self, now: Instant, address: Option<IpAddr>) -> Option<PeerId> {
        self.handshaking
            .iter()
            .filter(|(_, handshake)| {
                now.saturating_duration_since(handshake.since) >= HANDSHAKE_EVICT_AFTER
            })
            .filter(|(peer, _)| {
                address.is_none_or(|wanted| self.addresses.get(*peer) == Some(&wanted))
            })
            .min_by_key(|(_, handshake)| handshake.since)
            .map(|(peer, _)| *peer)
    }
}

/// Lets one event through per period and counts the rest, so that an event a
/// peer can cause at network speed is logged at a rate a log can hold.
///
/// [`LogGate::permit`] reports how many events were refused since the last one
/// that got through, and only the first call of each period gets a `Some`. Safe
/// to share between threads: the refused path takes a short lock and allocates
/// nothing.
pub(crate) struct LogGate {
    period: Duration,
    state: Mutex<GateState>,
}

/// What a [`LogGate`] remembers between calls.
struct GateState {
    /// When the period of the last permit began, or `None` before the first.
    opened: Option<Instant>,
    /// Events refused since the last permit.
    suppressed: u64,
}

impl LogGate {
    /// A gate that lets one event through per `period`.
    pub(crate) fn new(period: Duration) -> LogGate {
        LogGate {
            period,
            state: Mutex::new(GateState {
                opened: None,
                suppressed: 0,
            }),
        }
    }

    /// Asks whether an event may be logged now. `Some(n)` says it may, and that
    /// `n` events were refused since the last one that was. `None` says this
    /// period has logged one already.
    pub(crate) fn permit(&self) -> Option<u64> {
        self.permit_at(Instant::now())
    }

    /// [`LogGate::permit`] at `now`, so that a test can move through periods
    /// without sleeping.
    fn permit_at(&self, now: Instant) -> Option<u64> {
        let mut state = lock_unpoisoned(&self.state);
        let due = state
            .opened
            .is_none_or(|opened| now.saturating_duration_since(opened) >= self.period);
        if due {
            state.opened = Some(now);
            Some(std::mem::take(&mut state.suppressed))
        } else {
            state.suppressed += 1;
            None
        }
    }
}

/// Locks `mutex`, even when a panic poisoned it. Every lock in this module
/// guards counters and maps that are consistent whenever the lock is released,
/// so a poisoned lock means that a holder panicked and not that the data is
/// torn. Refusing the lock would brick the socket over one panic.
fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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
    /// The per-address counts and the handshakes that eviction may end; see
    /// [`Admission`]. Locked after `state`, and never before it.
    admission: Mutex<Admission>,
    /// The peer set's change notification, and the snapshot it invalidates.
    /// Rung whenever the set changes — one arrived, one connected, one went
    /// away — and whenever a published pipe moves a message; see
    /// [`PeerChanges`].
    peers_changed: Arc<PeerChanges>,
    /// Where connection-lifecycle events are published, and the mask the
    /// one installed monitor asked for. `zmq_socket_monitor`'s events are
    /// one source rendered two ways, and this is the source
    /// ([`crate::monitor`]).
    monitor: MonitorSink,
    /// Rate limit for the lines and monitor events of refused connections.
    refusals: LogGate,
    /// Connections refused at admission, since the socket was created.
    refused: AtomicU64,
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        // Closing a socket closes its connections. The tasks hold only weak
        // references, so this is what actually ends them: aborting drops the
        // streams they own.
        let mut state = lock_unpoisoned(&self.state);
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
                admission: Mutex::new(Admission::default()),
                refusals: LogGate::new(LOG_PERIOD),
                refused: AtomicU64::new(0),
                peers_changed: Arc::default(),
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
                    .spawn(accept_forever(ctx, binding, accepting));
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
                    .spawn(accept_forever(ctx, binding, accepting));
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
                    .spawn(accept_forever(ctx, listener, accepting));
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

    /// A wait for the next thing a receive could act on: a message arriving
    /// from any peer, or the peer set changing.
    ///
    /// **Create it before looking, and look after creating it.** The returned
    /// future counts every ring from the moment it exists, so a message that
    /// lands between the look and the sleep ends the wait; one created after
    /// the look would sleep through it. [`Engine::peers`] is the look, and
    /// publishing a peer is what attaches its pipe to this bell, so a wait
    /// that creates this first and reads the peers second cannot miss a pipe.
    pub(crate) fn readable(&self) -> Notified<'_> {
        self.inner.peers_changed.readable.notified()
    }

    /// A wait for the next thing a send could act on: room appearing on any
    /// peer's outgoing queue, or the peer set changing — "SHALL block on
    /// sending… when it has no connected peers" is a wait for a peer, and
    /// this is that wait rather than a poll. Create it before looking; see
    /// [`Engine::readable`].
    pub(crate) fn writable(&self) -> Notified<'_> {
        self.inner.peers_changed.writable.notified()
    }

    /// How many times the peer set has changed in a way a reader reconciles
    /// against: a peer arrived, connected, went or announced itself.
    ///
    /// Read it **before** [`Engine::peers`] and remember it with what was
    /// read: a change between the two costs one pass more, never a missed
    /// one.
    pub(crate) fn generation(&self) -> u64 {
        self.inner.peers_changed.generation()
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

    /// Every peer this socket has, connected or not, sorted by id.
    ///
    /// One shared, immutable snapshot: the engine rebuilds it only after the
    /// peer set — or anything a [`Peer`] reports — has changed, so calling
    /// this once per message costs a lock and a reference-count bump. A fresh
    /// list per call costs a lock per peer and an allocation or three, at a
    /// peer count a stranger chooses. The caller keeps the snapshot it took
    /// however the set moves on, and two calls that return the same `Arc`
    /// saw the same peers.
    pub fn peers(&self) -> Arc<[Peer]> {
        self.view(|view| Arc::clone(&view.all))
    }

    /// The peers a sender may put a message in right now.
    ///
    /// Every peer, unless `ZMQ_IMMEDIATE` is set — in which case only the
    /// connected ones, so that "messages shall be queued only to completed
    /// connections" and a round-robin socket does not fill the queue of an
    /// endpoint that never answers (`docs/research/zeromq.md` §5). A shared
    /// snapshot like [`Engine::peers`].
    pub fn outgoing_peers(&self) -> Arc<[Peer]> {
        self.view(|view| Arc::clone(&view.outgoing))
    }

    /// One peer, by id.
    pub fn peer(&self, id: PeerId) -> Option<Peer> {
        let state = self.lock();
        state
            .peers
            .get(&id)
            .map(|entry| snapshot(id, entry, &self.inner.peers_changed))
    }

    /// One peer's pipe, by id: all a send to a named peer needs, without
    /// building the whole [`Peer`] it belongs to.
    pub fn pipe_of(&self, id: PeerId) -> Option<Pipe> {
        self.lock().peers.get(&id).map(|entry| entry.pipe.clone())
    }

    /// Records on a peer what a session records after its handshake — the user
    /// id a ZAP handler named, then the routing id the peer announced — for a
    /// test that needs a peer a ROUTER can learn with no connection behind it.
    #[cfg(test)]
    pub(crate) fn assume(
        &self,
        peer: PeerId,
        user: Option<ZapUserId>,
        identity: Option<RoutingId>,
    ) {
        let (user_slot, identity_slot) = {
            let state = self.lock();
            let entry = state.peers.get(&peer).expect("a peer this engine has");
            (entry.user.clone(), entry.identity.clone())
        };
        user_slot.set(user);
        identity_slot.announce(identity);
    }

    /// Reads the published snapshot, building it first if a change has
    /// dropped it.
    ///
    /// The state lock is taken before the snapshot's, here and nowhere in
    /// the other order, so a change that clears the snapshot while holding
    /// the state cannot deadlock against a rebuild. A change whose table
    /// update lands before a rebuild and whose notification lands after the
    /// rebuild's install clears a snapshot that was already current, which
    /// costs one more rebuild and never leaves a stale one behind.
    fn view<R>(&self, read: impl FnOnce(&View) -> R) -> R {
        let changes = &self.inner.peers_changed;
        if let Some(view) = changes.view.lock().expect("peer view poisoned").as_ref() {
            return read(view);
        }
        let state = self.lock();
        let mut published = changes.view.lock().expect("peer view poisoned");
        let view = published
            .get_or_insert_with(|| build_view(&state, changes, self.inner.options.immediate));
        read(view)
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
            let _ = lock_unpoisoned(&self.inner.admission).release(peer);
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
        // A closed socket admits nothing and evicts nothing, so its bookkeeping
        // goes with its peers.
        *lock_unpoisoned(&self.inner.admission) = Admission::default();
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
        lock_unpoisoned(&self.inner.state)
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

fn snapshot(id: PeerId, entry: &PeerEntry, changes: &Arc<PeerChanges>) -> Peer {
    // Publishing a peer is what wires it to the socket: its pipe starts
    // ringing the bells, and its identity slot starts reporting to the
    // engine. Every wait creates its bell before it reads a snapshot, so a
    // pipe that was not yet attached cannot have been one the wait failed to
    // look at.
    entry.pipe.attach(&changes.readable, &changes.writable);
    entry.identity.watch(changes);
    let (identity, announced) = entry.identity.read();
    Peer {
        id,
        user_id: entry.user.get(),
        credentials: entry.credentials,
        endpoint: entry.endpoint.clone(),
        connected: entry.connected,
        attempts: entry.attempts,
        pipe: entry.pipe.clone(),
        identity,
        announced,
        subscriptions: Arc::clone(&entry.subscriptions),
    }
}

/// The snapshot of every peer in `state`, sorted by id, and the part of it a
/// sender may use.
fn build_view(state: &EngineState, changes: &Arc<PeerChanges>, immediate: bool) -> View {
    let mut peers: Vec<Peer> = state
        .peers
        .iter()
        .map(|(id, entry)| snapshot(*id, entry, changes))
        .collect();
    peers.sort_unstable_by_key(|peer| peer.id);
    let all: Arc<[Peer]> = Arc::from(peers);
    let outgoing = if immediate {
        all.iter().filter(|peer| peer.connected).cloned().collect()
    } else {
        Arc::clone(&all)
    };
    View { all, outgoing }
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
    ///
    /// One host may hold only [`per_address_cap`] of the table, so it cannot
    /// take every slot; a host is what [`weida_core::host_key`] says it is. A full
    /// table or a full host makes room only by evicting a handshake that has
    /// stalled for [`HANDSHAKE_EVICT_AFTER`] as of `now`: a silent peer gives
    /// way to a newcomer, and a live one never does.
    fn admit(
        &self,
        address: Option<IpAddr>,
        credentials: Option<weida_core::LocalPrincipal>,
        pipe: Pipe,
        now: Instant,
    ) -> Admitted {
        let mut state = lock_unpoisoned(&self.state);
        if state.closed {
            return Admitted::SocketClosed;
        }
        let mut admission = lock_unpoisoned(&self.admission);
        // `ipc` and `inproc` peers have no address at all, and loopback is not
        // counted: it is this host's own processes, which a test or a sidecar
        // legitimately opens by the thousand.
        let counted = address
            .map(weida_core::host_key)
            .filter(|host| !host.loopback)
            .map(|host| host.key);
        let cap = per_address_cap(self.options.max_peers);
        let address_full = counted.is_some_and(|address| {
            admission
                .per_address
                .get(&address)
                .is_some_and(|held| *held >= cap)
        });
        let table_full = state.accepted >= self.options.max_peers;

        // An address at its cap makes room only with one of its own stalled
        // handshakes: evicting another host's peer would not free this host's
        // share of the table.
        let victim = if address_full || table_full {
            let scope = if address_full { counted } else { None };
            match admission.stalled(now, scope) {
                Some(victim) => Some(victim),
                None if address_full => return Admitted::PerAddress,
                None => return Admitted::AtCeiling,
            }
        } else {
            None
        };
        let mut evicted = None;
        if let Some(victim) = victim {
            if let Some(entry) = state.peers.remove(&victim) {
                if entry.endpoint.is_none() {
                    state.accepted -= 1;
                }
                evicted = Some(entry.pipe);
            }
            if let Some(kill) = admission.release(victim) {
                // A session that already ended dropped its receiver, and then
                // there is nothing left to evict.
                let _ = kill.send(());
            }
        }

        let peer = PeerId(state.next_peer);
        state.next_peer += 1;
        state.accepted += 1;
        let identity = AnnouncedIdentity::default();
        let user = AuthenticatedUser::default();
        let subscriptions = Arc::new(Subscriptions::new(
            self.options.max_subscriptions,
            self.options.max_subscription_bytes,
        ));
        let (kill, eviction) = oneshot::channel();
        state.peers.insert(
            peer,
            PeerEntry {
                credentials,
                user: user.clone(),
                pipe,
                identity: identity.clone(),
                subscriptions: Arc::clone(&subscriptions),
                endpoint: None,
                connected: true,
                attempts: 0,
            },
        );
        admission.track(peer, counted, now, kill);
        drop(admission);
        drop(state);
        // The evicted pipe is closed once the locks are released.
        if let Some(pipe) = evicted {
            pipe.close();
        }
        self.peers_changed.notify_waiters();
        Admitted::Peer(PeerSlots {
            peer,
            identity,
            user,
            subscriptions,
            eviction,
        })
    }

    /// Forgets an accepted peer, because its session ended by any route. A peer
    /// that was evicted has nothing left here, and that is not an error: its
    /// entry and its count are released exactly once.
    fn forget(&self, peer: PeerId) {
        let entry = {
            let mut state = lock_unpoisoned(&self.state);
            let entry = state.peers.remove(&peer);
            if entry.as_ref().is_some_and(|entry| entry.endpoint.is_none()) {
                state.accepted -= 1;
            }
            // Dropping the kill switch returned here evicts nobody.
            let _ = lock_unpoisoned(&self.admission).release(peer);
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
        if let Some(entry) = lock_unpoisoned(&self.state).peers.get_mut(&peer) {
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
        if let Some(entry) = lock_unpoisoned(&self.state).peers.get_mut(&peer) {
            entry.credentials = credentials;
        }
        self.peers_changed.touch();
    }

    fn count_attempt(&self, peer: PeerId) {
        if let Some(entry) = lock_unpoisoned(&self.state).peers.get_mut(&peer) {
            entry.attempts += 1;
        }
        self.peers_changed.touch();
    }

    fn subscriptions_of(&self, peer: PeerId) -> Option<Arc<Subscriptions>> {
        lock_unpoisoned(&self.state)
            .peers
            .get(&peer)
            .map(|entry| Arc::clone(&entry.subscriptions))
    }

    fn user_slot(&self, peer: PeerId) -> Option<AuthenticatedUser> {
        lock_unpoisoned(&self.state)
            .peers
            .get(&peer)
            .map(|entry| entry.user.clone())
    }

    fn identity_slot(&self, peer: PeerId) -> Option<AnnouncedIdentity> {
        lock_unpoisoned(&self.state)
            .peers
            .get(&peer)
            .map(|entry| entry.identity.clone())
    }

    fn pipe_of(&self, peer: PeerId) -> Option<Pipe> {
        lock_unpoisoned(&self.state)
            .peers
            .get(&peer)
            .map(|entry| entry.pipe.clone())
    }

    /// Takes `peer` out of the handshakes that eviction may end, because its
    /// handshake has completed.
    fn handshake_completed(&self, peer: PeerId) {
        lock_unpoisoned(&self.admission).handshaking.remove(&peer);
    }

    /// Counts one refused connection, and logs it at most once per
    /// [`LOG_PERIOD`]. A peer that can make the socket refuse at network speed
    /// must not be able to write at network speed as well.
    fn refuse(&self, endpoint: &Endpoint, reason: &'static str, who: &str) {
        let total = self.refused.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::debug!(%endpoint, who, reason, "refused an accepted connection");
        if let Some(suppressed) = self.refusals.permit() {
            tracing::warn!(
                %endpoint,
                reason,
                max_peers = self.options.max_peers,
                suppressed,
                refused = total,
                "refused an accepted connection; refusals in between are counted, not logged"
            );
            self.monitor
                .publish(EVENT_ACCEPT_FAILED, || MonitorEvent::AcceptFailed {
                    endpoint: endpoint.to_string(),
                    reason: reason.to_string(),
                });
        }
    }
}

/// One connection an [`Acceptor`] took off its binding.
struct Accepted {
    /// The byte stream. Dropping it closes the connection.
    stream: Stream,
    /// The peer's address for a ZAP request: the IP of a `tcp://` peer, and
    /// empty for a local transport, which has no address to report.
    address: String,
    /// The peer's IP as the kernel reported it, from which the per-address cap
    /// derives its key ([`weida_core::host_key`]). `None` for a local transport.
    ip: Option<IpAddr>,
    /// The kernel's statement about a local peer, where the transport has one.
    credentials: Option<weida_core::LocalPrincipal>,
    /// What a log line names for this peer: the socket address of a `tcp://`
    /// peer, the dialling socket of an `inproc://` one, and nothing for an
    /// `ipc://` peer, whose credentials name it.
    who: String,
}

/// Where one bound endpoint's connections come from: a TCP listener, an
/// `ipc://` binding, or an `inproc://` name.
///
/// `Ok(None)` ends the listener, which only `inproc://` reports, when its queue
/// ends. An `Err` is one accept that failed, and the listener survives it.
trait Acceptor: Send {
    fn accept(&mut self) -> impl Future<Output = Result<Option<Accepted>>> + Send;
}

impl Acceptor for TcpListener {
    async fn accept(&mut self) -> Result<Option<Accepted>> {
        let (stream, from) = TcpListener::accept(self).await?;
        let stream = Stream::tcp(stream);
        Ok(Some(Accepted {
            stream,
            // 27/ZAP wants the client address and not the port: authorization
            // is about who connected, and the source port is noise.
            address: from.ip().to_string(),
            ip: Some(from.ip()),
            credentials: None,
            who: from.to_string(),
        }))
    }
}

#[cfg(unix)]
impl Acceptor for crate::ipc::IpcBinding {
    async fn accept(&mut self) -> Result<Option<Accepted>> {
        let stream = crate::ipc::IpcBinding::accept(self).await?;
        // The kernel's answer about the peer, captured when the connection was
        // made.
        let credentials = stream.peer_credentials();
        Ok(Some(Accepted {
            stream,
            // A filesystem endpoint has no IP to report, and the kernel's answer
            // takes its place: 27/ZAP has no field for it, so it travels as the
            // extension frame `crate::zap` documents.
            address: String::new(),
            ip: None,
            credentials,
            who: String::new(),
        }))
    }
}

impl Acceptor for InprocBinding {
    async fn accept(&mut self) -> Result<Option<Accepted>> {
        let Some(dial) = InprocBinding::accept(self).await else {
            return Ok(None);
        };
        Ok(Some(Accepted {
            // No address and no kernel: an in-process peer is this process, and
            // 27/ZAP's address frame stays empty.
            stream: dial.stream,
            address: String::new(),
            ip: None,
            credentials: None,
            // The dialler names the socket that dialled, or says that the library
            // dialled itself.
            who: dial.from.to_string(),
        }))
    }
}

/// Accepts connections on one bound endpoint for as long as the socket is open.
/// "A bound endpoint accepts automatically", and there is no `zmq_accept()`
/// (`docs/research/zeromq.md` §2).
///
/// A failed accept does not end the listener. It is counted, logged and
/// published under the [`LOG_PERIOD`] limit, and the listener sleeps before its
/// next attempt. The sleep starts at [`ACCEPT_BACKOFF_MIN`], doubles for as long
/// as accepts keep failing, and stops at [`ACCEPT_BACKOFF_MAX`]. A listener that
/// cannot accept, because the process is out of descriptors, therefore retries
/// about once a second instead of spinning on a connection that stays in the
/// backlog. The task ends when the socket is gone or the binding is closed, and
/// `unbind` and `close` close a binding by aborting its task.
async fn accept_forever<A: Acceptor>(ctx: TaskCtx, mut acceptor: A, endpoint: Endpoint) {
    let accept_failures = LogGate::new(LOG_PERIOD);
    let mut backoff = ACCEPT_BACKOFF_MIN;
    loop {
        match acceptor.accept().await {
            Ok(Some(accepted)) => {
                backoff = ACCEPT_BACKOFF_MIN;
                if !serve_accepted(&ctx, accepted, &endpoint) {
                    return;
                }
            }
            Ok(None) => return,
            Err(error) => {
                // The engine is checked on the failure path as well as on
                // admission, so a socket dropped during a backoff ends its
                // listener rather than leaving it retrying. The check does not
                // hold the engine alive.
                if ctx.engine.strong_count() == 0 {
                    return;
                }
                if let Some(suppressed) = accept_failures.permit() {
                    tracing::warn!(
                        endpoint = %endpoint,
                        error = %error,
                        suppressed,
                        "an accept failed; the listener keeps accepting"
                    );
                    ctx.monitor
                        .publish(EVENT_ACCEPT_FAILED, || MonitorEvent::AcceptFailed {
                            endpoint: endpoint.to_string(),
                            reason: error.to_string(),
                        });
                }
                ctx.exec.sleep(backoff).await;
                backoff = (backoff * 2).min(ACCEPT_BACKOFF_MAX);
            }
        }
    }
}

/// Admits one accepted connection and starts its session. Returns `false` when
/// the socket is gone, which ends the listener.
fn serve_accepted(ctx: &TaskCtx, accepted: Accepted, endpoint: &Endpoint) -> bool {
    let Some(engine) = ctx.engine.upgrade() else {
        return false;
    };
    let Accepted {
        stream,
        address,
        ip,
        credentials,
        who,
    } = accepted;
    let pipe = Pipe::new(ctx.options.pipe);
    let slots = match engine.admit(ip, credentials, pipe.clone(), Instant::now()) {
        Admitted::Peer(slots) => slots,
        Admitted::SocketClosed => return false,
        // Dropping the stream closes the connection, which a foreign peer
        // observes as an immediate EOF. Accepting continues, because the refusal
        // bounds the live peers and not the listener.
        Admitted::AtCeiling => {
            engine.refuse(endpoint, "max_peers", &who);
            return true;
        }
        Admitted::PerAddress => {
            engine.refuse(endpoint, "per-address", &who);
            return true;
        }
    };
    drop(engine);
    // Tuned once the connection is admitted, so that one about to be refused
    // costs this side no more than the accept.
    tune(&stream, &ctx.options);

    let peer = slots.peer;
    let guard = AcceptedGuard {
        engine: ctx.engine.clone(),
        peer,
    };
    let session = PeerSession {
        peer,
        pipe,
        endpoint: endpoint.clone(),
        role: Role::Binder,
        identity: slots.identity,
        subscriptions: slots.subscriptions,
        address,
        credentials,
        user: slots.user,
        eviction: Some(slots.eviction),
    };
    let ctx = ctx.clone();
    let exec = ctx.exec.clone();
    exec.spawn(async move {
        // Held until the session has ended by any route, a panic included.
        let _guard = guard;
        if let Err(e) = run_session(&ctx, stream, session).await {
            tracing::debug!(%peer, error = %e, "an accepted connection ended");
        }
    });
    true
}

/// Releases an accepted peer's admission when its session task ends, by any
/// route: a clean end, an error, a panic or an abort. A guard that only ran on
/// the normal path would leave the slot taken for good after the first panic.
struct AcceptedGuard {
    engine: Weak<EngineInner>,
    peer: PeerId,
}

impl Drop for AcceptedGuard {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.upgrade() {
            engine.forget(self.peer);
        }
    }
}

/// A spawned task that is aborted when its handle is dropped, so that the work
/// ends with the owner that started it.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
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
                // The session runs in a task of its own: a panic in it ends
                // that task, and this loop sees a failed connection and goes on
                // reconnecting. Aborting this loop (`disconnect`, `close`) drops
                // the handle, which aborts the session and closes the connection.
                let session = PeerSession {
                    peer,
                    pipe,
                    endpoint: endpoint.clone(),
                    role: Role::Connecter,
                    identity: identity.clone(),
                    subscriptions: Arc::clone(&subscriptions),
                    // The address of a dialled peer is the one we dialled,
                    // which is the only one there is to report.
                    address: dialled_address(&endpoint),
                    credentials,
                    user: user.clone(),
                    eviction: None,
                };
                let session_ctx = ctx.clone();
                let mut task = AbortOnDrop(
                    ctx.exec
                        .spawn(async move { run_session(&session_ctx, stream, session).await }),
                );
                let outcome = (&mut task.0).await;
                match ctx.engine.upgrade() {
                    Some(engine) => engine.set_connected(peer, false),
                    None => return,
                }
                match outcome {
                    Ok(outcome) => {
                        if let Err(e) = outcome {
                            tracing::debug!(%peer, error = %e, "a dialled connection ended");
                        }
                        // A connection that was established resets the backoff:
                        // the endpoint is reachable, so the next loss starts over
                        // at ZMQ_RECONNECT_IVL.
                        delay = None;
                    }
                    // A panic is a failed connection, so the backoff keeps
                    // growing as it does for a refused dial.
                    Err(e) => {
                        tracing::error!(
                            %peer,
                            endpoint = %endpoint,
                            error = %e,
                            "a dialled session panicked; reconnecting"
                        );
                        retried = true;
                    }
                }
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
                tune(&stream, &ctx.options);
                return Ok(stream);
            }
            Err(e) => last = e,
        }
    }
    Err(last)
}

/// What every TCP connection of this socket is set up with, whether it dialled
/// the connection or accepted it: Nagle's algorithm off, so that a
/// request-reply exchange on loopback does not wait on it, and the keepalive
/// that `ZMQ_TCP_KEEPALIVE` and its three tuning options ask for. A failure to
/// set either is not fatal — the connection works, with less — so it is logged
/// and not raised. An `ipc` or `inproc` stream has neither to set.
fn tune(stream: &Stream, options: &SocketOptions) {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(%error, "could not disable Nagle on a TCP connection");
    }
    if let Err(error) = stream.set_keepalive(options) {
        tracing::debug!(%error, "could not set the TCP keepalive on a connection");
    }
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
    /// Where the engine evicts this connection, when it admitted the connection.
    /// A dialled connection is never evicted, so it has none.
    eviction: Option<oneshot::Receiver<()>>,
}

/// Resolves when the engine evicts the connection, and never otherwise. A
/// dropped sender is not an eviction: that is how a completed handshake or a
/// released peer lets go of its switch.
async fn evicted(signal: Option<oneshot::Receiver<()>>) {
    let fired = match signal {
        Some(signal) => signal.await.is_ok(),
        None => false,
    };
    if !fired {
        std::future::pending::<()>().await;
    }
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

    let peer_id = peer.peer;
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
    let mut handshaking = true;

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
    let eviction = evicted(peer.eviction);
    tokio::pin!(eviction);

    // The loop ends when the session ends, when the handshake runs out of time,
    // or when the engine evicts the peer. Eviction stays armed for the whole
    // session and not only for the handshake: a handshake can complete in the
    // same instant it is evicted, and the eviction must still close the
    // connection.
    let outcome = loop {
        tokio::select! {
            outcome = &mut session => break outcome,
            // The gate closed: `Ok` is the handshake completing, `Err` is the
            // sender dropped because the session is ending. Either way the
            // handshake bound is spent.
            gate = &mut handshaken, if handshaking => {
                handshaking = false;
                if gate.is_ok() {
                    succeeded = true;
                    ctx.monitor.publish(EVENT_HANDSHAKE_SUCCEEDED, || {
                        MonitorEvent::HandshakeSucceeded {
                            endpoint: endpoint.clone(),
                        }
                    });
                    // A completed handshake is no longer stalled, and eviction
                    // only ever takes a stalled one.
                    if let Some(engine) = ctx.engine.upgrade() {
                        engine.handshake_completed(peer_id);
                    }
                }
            }
            () = &mut deadline, if handshaking => {
                break Err(Error::ETIMEDOUT(
                    format!(
                        "the handshake did not complete within {:?} (ZMQ_HANDSHAKE_IVL)",
                        limit.unwrap_or_default()
                    )
                    .into(),
                ));
            }
            () = &mut eviction => {
                break Err(Error::ETIMEDOUT(
                    "evicted: the handshake stalled while this socket was full, \
                     and a newcomer took its slot"
                        .into(),
                ));
            }
        }
    };
    // The session future owns the stream, so dropping it is what closes the
    // connection when the session did not end by itself: an eviction or the
    // handshake bound ends it here, while it is still pending.
    drop(session);

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

    /// A session that panics inside its future, as a bug in a session would.
    struct Panicking {
        runs: Arc<AtomicUsize>,
    }

    impl Session for Panicking {
        fn run(&self, _connection: Connection) -> SessionFuture {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::poll_fn(|_| -> std::task::Poll<Result<()>> {
                panic!("a session bug")
            }))
        }
    }

    /// Panics in its first session, and holds every later connection the way
    /// [`Recorder`] does.
    struct PanicsOnce {
        runs: AtomicUsize,
        then: Arc<Recorder>,
    }

    impl Session for PanicsOnce {
        fn run(&self, connection: Connection) -> SessionFuture {
            if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
                return Box::pin(std::future::poll_fn(|_| -> std::task::Poll<Result<()>> {
                    panic!("a session bug")
                }));
            }
            self.then.run(connection)
        }
    }

    /// An [`Acceptor`] whose first `failures` accepts fail. After them it hands
    /// over one connection and then waits, as a listener does.
    struct Scripted {
        failures: usize,
        calls: Arc<AtomicUsize>,
        connection: Option<Stream>,
    }

    impl Acceptor for Scripted {
        async fn accept(&mut self) -> Result<Option<Accepted>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call < self.failures {
                return Err(Error::EMFILE("the process is out of descriptors".into()));
            }
            match self.connection.take() {
                Some(stream) => Ok(Some(Accepted {
                    stream,
                    address: String::new(),
                    ip: None,
                    credentials: None,
                    who: String::new(),
                })),
                None => std::future::pending().await,
            }
        }
    }

    /// Claim: a failed accept does not end the listener. The connection that
    /// follows the failures is admitted, and its session runs.
    #[tokio::test]
    async fn an_accept_error_does_not_end_the_listener() {
        let ctx = context();
        let recorder = Recorder::new();
        let engine = Engine::new(&ctx, options(), as_session(&recorder)).expect("engine");
        let (near, far) = tokio::io::duplex(64);
        let calls = Arc::new(AtomicUsize::new(0));
        let acceptor = Scripted {
            failures: 2,
            calls: Arc::clone(&calls),
            connection: Some(Stream::inproc(near)),
        };
        let endpoint = Endpoint::parse("inproc://scripted").expect("endpoint");
        let listener = tokio::spawn(accept_forever(engine.task_ctx(), acceptor, endpoint));

        wait_for(|| recorder.count() == 1).await;
        assert_eq!(
            engine.peers().len(),
            1,
            "the connection after the failures is admitted"
        );
        assert!(
            calls.load(Ordering::SeqCst) >= 3,
            "two failed accepts, then the connection"
        );

        // The session ends when the other end goes away, and its slot with it.
        drop(far);
        wait_for(|| engine.peers().is_empty()).await;
        listener.abort();
    }

    /// Claim: an accept that keeps failing is retried on a backoff rather than
    /// in a spin. The backoff of 10, 20, 40 and 80 ms allows about four attempts
    /// in 150 ms, where a spin would make thousands.
    #[tokio::test]
    async fn a_failing_acceptor_does_not_spin() {
        let ctx = context();
        let engine = Engine::new(&ctx, options(), Recorder::new()).expect("engine");
        let calls = Arc::new(AtomicUsize::new(0));
        let acceptor = Scripted {
            failures: usize::MAX,
            calls: Arc::clone(&calls),
            connection: None,
        };
        let endpoint = Endpoint::parse("inproc://broken").expect("endpoint");
        let listener = tokio::spawn(accept_forever(engine.task_ctx(), acceptor, endpoint));

        tokio::time::sleep(Duration::from_millis(150)).await;
        listener.abort();
        let attempts = calls.load(Ordering::SeqCst);
        assert!(
            attempts <= 6,
            "{attempts} accepts in 150 ms: the listener is spinning"
        );
    }

    /// Claim: a session that panics releases its slot. With one slot, the
    /// second connection is admitted only because the first one's panic
    /// released it.
    #[tokio::test]
    async fn a_panicking_session_releases_its_slot() {
        let ctx = context();
        let runs = Arc::new(AtomicUsize::new(0));
        let engine = Engine::new(
            &ctx,
            SocketOptions {
                max_peers: 1,
                ..options()
            },
            Arc::new(Panicking {
                runs: Arc::clone(&runs),
            }),
        )
        .expect("engine");
        let bound = engine
            .bind(&Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint"))
            .await
            .expect("bind");
        let addr: std::net::SocketAddr = bound.to_string()["tcp://".len()..].parse().expect("addr");

        let first = TcpStream::connect(addr).await.expect("first connection");
        wait_for(|| runs.load(Ordering::SeqCst) == 1).await;
        wait_for(|| engine.peers().is_empty()).await;

        let second = TcpStream::connect(addr).await.expect("second connection");
        wait_for(|| runs.load(Ordering::SeqCst) == 2).await;
        wait_for(|| engine.peers().is_empty()).await;
        drop((first, second));
    }

    /// Claim: a dialled session that panics ends its own task. The connecter
    /// survives it, sees a failed connection, and dials again.
    #[tokio::test]
    async fn a_panicking_dialled_session_is_retried() {
        let ctx = context();
        let recorder = Recorder::new();
        let session = Arc::new(PanicsOnce {
            runs: AtomicUsize::new(0),
            then: Arc::clone(&recorder),
        });
        let engine = Engine::new(&ctx, options(), session).expect("engine");

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let port = listener.local_addr().expect("addr").port();
        let endpoint = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).expect("endpoint");
        // The peer keeps what it accepts open, so each dial stays connected.
        let served = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });

        let peer = engine.connect(&endpoint).expect("connect");
        wait_for(|| recorder.count() == 1).await;
        wait_for(|| engine.peer(peer).is_some_and(|p| p.connected)).await;
        assert!(
            engine.peer(peer).expect("peer").attempts >= 2,
            "the panicked session was followed by a second dial"
        );
        served.abort();
    }

    /// Claim: a panic while the engine's state lock is held does not brick the
    /// socket. The guarded counters and maps are consistent whenever the lock is
    /// released, so every path that takes the lock keeps working.
    #[tokio::test]
    async fn a_poisoned_engine_lock_does_not_brick_the_socket() {
        fn poison(state: &Mutex<EngineState>) {
            let _held = state.lock().expect("the lock is not poisoned yet");
            panic!("a holder panics while the engine state is locked");
        }

        let ctx = context();
        let engine = Engine::new(&ctx, options(), Recorder::new()).expect("engine");
        let inner = Arc::clone(&engine.inner);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            poison(&inner.state);
        }));
        assert!(panicked.is_err());
        assert!(engine.inner.state.is_poisoned());

        assert!(engine.peers().is_empty());
        let admitted = engine
            .inner
            .admit(None, None, Pipe::new(options().pipe), Instant::now());
        let Admitted::Peer(slots) = admitted else {
            panic!("a poisoned lock refused an admission");
        };
        assert_eq!(engine.peers().len(), 1);
        engine.inner.forget(slots.peer);
        assert!(engine.peers().is_empty());
    }

    /// Claim: one address cannot hold every slot of a socket. The per-address
    /// cap refuses its ninth peer, another address is still admitted, loopback
    /// is exempt, and a share comes back when its peer goes away or a stalled
    /// handshake from the same address gives way.
    #[tokio::test]
    async fn one_address_cannot_hold_every_slot() {
        let ctx = context();
        // The cap for 32 is 32 / 8 = 4, raised to its floor of 8.
        let engine = Engine::new(
            &ctx,
            SocketOptions {
                max_peers: 32,
                ..options()
            },
            Recorder::new(),
        )
        .expect("engine");
        let host: IpAddr = "192.0.2.1".parse().expect("address");
        let other: IpAddr = "192.0.2.2".parse().expect("address");
        let loopback: IpAddr = "127.0.0.1".parse().expect("address");
        let now = Instant::now();
        let admit = |address: IpAddr, at: Instant| {
            engine
                .inner
                .admit(Some(address), None, Pipe::new(options().pipe), at)
        };

        let mut held = Vec::new();
        for _ in 0..8 {
            let Admitted::Peer(slots) = admit(host, now) else {
                panic!("an address under its cap was refused");
            };
            held.push(slots.peer);
        }
        assert!(
            matches!(admit(host, now), Admitted::PerAddress),
            "the ninth peer from one address is refused"
        );
        assert!(
            matches!(admit(other, now), Admitted::Peer(_)),
            "another address is still admitted"
        );
        for _ in 0..10 {
            assert!(
                matches!(admit(loopback, now), Admitted::Peer(_)),
                "loopback is exempt from the cap"
            );
        }

        // A peer that goes away gives its share back.
        engine.inner.forget(held[0]);
        assert!(
            matches!(admit(host, now), Admitted::Peer(_)),
            "a released share is usable again"
        );
        assert!(matches!(admit(host, now), Admitted::PerAddress));

        // A stalled handshake from the same address gives way to a newcomer from
        // it, and the address keeps exactly its cap.
        let later = now + HANDSHAKE_EVICT_AFTER;
        assert!(
            matches!(admit(host, later), Admitted::Peer(_)),
            "a stalled handshake of the same address gives way"
        );
        let held_by_host = lock_unpoisoned(&engine.inner.admission)
            .per_address
            .get(&host)
            .copied();
        assert_eq!(held_by_host, Some(8), "the eviction returned its count");
    }

    /// Claim: **the per-address cap counts a host, not an address it picks.**
    /// An IPv6 host is routed a whole `/64`, so two addresses of one `/64` are
    /// one host; an IPv4 client of a dual-stack listener arrives as an
    /// IPv4-mapped address and is the IPv4 address it carries; and loopback is
    /// exempt however it is written.
    #[tokio::test]
    async fn the_per_address_cap_counts_a_host_and_not_the_address_it_picks() {
        let ctx = context();
        // The cap for 64 is 64 / 8 = 8, which is also its floor.
        let engine = Engine::new(
            &ctx,
            SocketOptions {
                max_peers: 64,
                ..options()
            },
            Recorder::new(),
        )
        .expect("engine");
        let now = Instant::now();
        let admit = |address: &str| {
            engine.inner.admit(
                Some(address.parse().expect("address")),
                None,
                Pipe::new(options().pipe),
                now,
            )
        };

        // Eight addresses of one /64: every one is under any per-address cap.
        for suffix in 1..=8 {
            assert!(
                matches!(admit(&format!("2001:db8:0:1::{suffix}")), Admitted::Peer(_)),
                "address {suffix} of a /64 is under the cap"
            );
        }
        assert!(
            matches!(admit("2001:db8:0:1:ffff::9"), Admitted::PerAddress),
            "a ninth address of the same /64 is the same host"
        );
        assert!(
            matches!(admit("2001:db8:0:2::1"), Admitted::Peer(_)),
            "another /64 is another host"
        );

        // An IPv4 host reaching a dual-stack listener, and reaching it plain.
        for _ in 0..8 {
            assert!(matches!(admit("::ffff:192.0.2.1"), Admitted::Peer(_)));
        }
        assert!(
            matches!(admit("192.0.2.1"), Admitted::PerAddress),
            "the mapped form and the plain one are one host"
        );
        assert!(
            matches!(admit("::ffff:192.0.2.1"), Admitted::PerAddress),
            "and the mapped form is held to the same count"
        );

        for loopback in ["127.0.0.1", "::1", "::ffff:127.0.0.1"] {
            for _ in 0..10 {
                assert!(
                    matches!(admit(loopback), Admitted::Peer(_)),
                    "{loopback} is exempt from the cap"
                );
            }
        }
    }

    /// Claim: a newcomer takes the slot of a stalled handshake and never the
    /// slot of a live one. Until a handshake has stalled for
    /// `HANDSHAKE_EVICT_AFTER`, a newcomer is refused. Then the oldest stalled
    /// handshake is closed and the newcomer admitted, and a handshake that
    /// completed is never chosen.
    #[tokio::test]
    async fn a_stalled_handshake_is_evicted_for_a_newcomer() {
        let ctx = context();
        let silent = Recorder::silent();
        let engine = Engine::new(
            &ctx,
            SocketOptions {
                max_peers: 2,
                ..options()
            },
            as_session(&silent),
        )
        .expect("engine");
        let bound = engine
            .bind(&Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint"))
            .await
            .expect("bind");
        let addr: std::net::SocketAddr = bound.to_string()["tcp://".len()..].parse().expect("addr");

        // Two peers that never speak, connected one after the other, so the
        // first is the older of the two.
        let mut older_conn = TcpStream::connect(addr).await.expect("first connection");
        wait_for(|| engine.peers().len() == 1).await;
        let _younger_conn = TcpStream::connect(addr).await.expect("second connection");
        wait_for(|| engine.peers().len() == 2).await;
        let ids: Vec<PeerId> = engine.peers().iter().map(|peer| peer.id).collect();
        let (older, younger) = (ids[0], ids[1]);
        let handshake_started = |peer: PeerId| {
            lock_unpoisoned(&engine.inner.admission)
                .handshaking
                .get(&peer)
                .map(|handshake| handshake.since)
                .expect("a handshake that has not completed is tracked")
        };
        let older_since = handshake_started(older);
        let younger_since = handshake_started(younger);
        let newcomer = |at: Instant| {
            engine
                .inner
                .admit(None, None, Pipe::new(options().pipe), at)
        };

        // One second in, nothing has stalled for two seconds, so the newcomer is
        // refused and takes no slot.
        assert!(matches!(
            newcomer(older_since + Duration::from_secs(1)),
            Admitted::AtCeiling
        ));
        assert_eq!(engine.peers().len(), 2, "a refused newcomer takes no slot");

        // Two seconds in, the oldest stalled handshake is closed and the newcomer
        // takes its slot.
        let Admitted::Peer(_newcomer) = newcomer(older_since + HANDSHAKE_EVICT_AFTER) else {
            panic!("a stalled handshake was not evicted for a newcomer");
        };
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(10), older_conn.read(&mut byte))
            .await
            .expect("the evicted connection closed in time")
            .expect("read");
        assert_eq!(read, 0, "the socket closed the evicted connection");
        assert!(engine.peer(older).is_none(), "the evicted peer is gone");
        assert!(
            engine.peer(younger).is_some(),
            "the younger handshake stays"
        );

        // A completed handshake is no longer in the set that eviction draws from,
        // however old it is. The newcomer admitted above has not stalled yet at
        // this instant, so nothing is evictable.
        engine.inner.handshake_completed(younger);
        assert!(matches!(
            newcomer(younger_since + HANDSHAKE_EVICT_AFTER),
            Admitted::AtCeiling
        ));
        assert!(
            engine.peer(younger).is_some(),
            "a completed handshake is never chosen"
        );
    }

    /// Claim: a refusal storm is logged once per period, and the next period
    /// reports how many events it stood in for.
    #[test]
    fn log_gate_passes_one_event_per_period() {
        let gate = LogGate::new(Duration::from_secs(1));
        let passed: Vec<u64> = (0..10_000).filter_map(|_| gate.permit()).collect();
        assert_eq!(
            passed,
            vec![0],
            "one event passes per period, and none was suppressed before it"
        );

        // A period later the gate lets one more through and reports how many
        // events it stood in for.
        let next = Instant::now() + Duration::from_secs(1);
        assert_eq!(
            gate.permit_at(next),
            Some(9_999),
            "the next period reports the events it suppressed"
        );
        assert_eq!(gate.permit_at(next), None, "and it logs one of its own");
    }

    /// Claim: every connection the socket refuses at admission is counted on
    /// the engine, whether or not its log line gets through.
    #[tokio::test]
    async fn refused_connections_are_counted() {
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

        let _admitted = TcpStream::connect(addr).await.expect("first connection");
        wait_for(|| engine.peers().len() == 1).await;
        for _ in 0..3 {
            let _refused = TcpStream::connect(addr)
                .await
                .expect("a further connection");
        }
        wait_for(|| engine.inner.refused.load(Ordering::SeqCst) == 3).await;
        assert_eq!(
            engine.peers().len(),
            1,
            "the refused connections took no slot"
        );
    }

    /// An engine that dials once and gives up, so that a peer which never
    /// connects stays exactly as it is: no retry counts an attempt and
    /// publishes a new snapshot under a test that is comparing two.
    fn dials_once() -> SocketOptions {
        SocketOptions {
            reconnect_ivl: None,
            ..options()
        }
    }

    /// Claim: **the peers are one shared snapshot until something about them
    /// changes** — the same `Arc`, not an equal copy — so a send or a receive
    /// per message costs a lock and a reference-count bump instead of a lock
    /// per peer and an allocation or three, at a peer count a stranger
    /// chooses. A peer arriving, an announcement and a peer going each publish
    /// a new one, and a snapshot already taken is never altered under its
    /// holder.
    #[tokio::test]
    async fn the_peers_are_one_shared_snapshot_until_something_changes() {
        let ctx = context();
        let engine = Engine::new(&ctx, dials_once(), Recorder::new()).expect("engine");
        let nowhere = Endpoint::parse("tcp://127.0.0.1:9").expect("endpoint");
        let elsewhere = Endpoint::parse("tcp://127.0.0.1:10").expect("endpoint");
        let first = engine.connect(&nowhere).expect("connect");
        // The one attempt is counted, and that is the last change there is
        // until the test makes one.
        wait_for(|| engine.peers().first().map(|peer| peer.attempts) == Some(1)).await;

        let before = engine.peers();
        assert!(
            Arc::ptr_eq(&before, &engine.peers()),
            "nothing changed, so nothing was rebuilt"
        );
        assert!(
            Arc::ptr_eq(&before, &engine.outgoing_peers()),
            "without ZMQ_IMMEDIATE every peer is one a sender may use"
        );

        let before_connect = engine.generation();
        assert_eq!(before_connect, engine.generation(), "nothing changed");
        engine.connect(&elsewhere).expect("connect");
        let grown = engine.peers();
        assert!(!Arc::ptr_eq(&before, &grown), "a peer arrived");
        let grown_at = engine.generation();
        assert!(grown_at > before_connect, "a peer arrived");
        assert_eq!(grown.len(), 2);
        assert!(
            grown.windows(2).all(|pair| pair[0].id < pair[1].id),
            "sorted by id, which is what lets a reader search it"
        );
        assert_eq!(before.len(), 1, "the snapshot already taken is unchanged");

        engine.assume(
            first,
            Some(ZapUserId::new("alice")),
            Some(RoutingId::new(b"alpha".to_vec()).expect("id")),
        );
        let announced = engine.peers();
        assert!(!Arc::ptr_eq(&grown, &announced), "an announcement");
        let announced_at = engine.generation();
        assert!(
            announced_at > grown_at,
            "an announcement is a change of its own"
        );
        let peer = announced
            .iter()
            .find(|peer| peer.id == first)
            .expect("peer");
        assert!(peer.announced);
        assert_eq!(
            peer.identity.as_ref().map(RoutingId::as_bytes),
            Some(&b"alpha"[..])
        );
        assert_eq!(peer.user_id.as_ref().map(ZapUserId::as_str), Some("alice"));
        assert!(!grown[0].announced, "an older snapshot does not learn it");

        engine.disconnect(&nowhere).expect("disconnect");
        let shrunk = engine.peers();
        assert!(!Arc::ptr_eq(&announced, &shrunk), "a peer went");
        assert!(engine.generation() > announced_at, "a peer went");
        assert_eq!(shrunk.len(), 1);
    }

    /// Claim: **every change to the peer set rings both bells a socket's waits
    /// are parked on**, whichever way the set changes — a peer arriving, an
    /// announcement, a peer going — because a socket waiting for "a message or
    /// a peer" has nothing else to be woken by.
    #[tokio::test]
    async fn a_change_to_the_peer_set_rings_both_bells() {
        use std::task::{Context as TaskContext, Poll, Waker};

        let ctx = context();
        let engine = Engine::new(&ctx, dials_once(), Recorder::new()).expect("engine");
        let nowhere = Endpoint::parse("tcp://127.0.0.1:9").expect("endpoint");
        let mut cx = TaskContext::from_waker(Waker::noop());

        let mut rung = |change: &dyn Fn()| {
            let mut readable = std::pin::pin!(engine.readable());
            let mut writable = std::pin::pin!(engine.writable());
            assert!(readable.as_mut().poll(&mut cx).is_pending());
            assert!(writable.as_mut().poll(&mut cx).is_pending());
            change();
            (
                readable.as_mut().poll(&mut cx) == Poll::Ready(()),
                writable.as_mut().poll(&mut cx) == Poll::Ready(()),
            )
        };

        let first = std::cell::Cell::new(None);
        assert_eq!(
            rung(&|| first.set(Some(engine.connect(&nowhere).expect("connect")))),
            (true, true),
            "a peer arrived"
        );
        let first = first.get().expect("the peer");
        // A wait reads the peers after it creates its bell, which is what
        // publishes the peer and makes its announcement report to the engine.
        let _published = engine.peers();
        assert_eq!(
            rung(&|| engine.assume(first, None, None)),
            (true, true),
            "a peer announced"
        );
        assert_eq!(
            rung(&|| {
                engine.disconnect(&nowhere).expect("disconnect");
            }),
            (true, true),
            "a peer went"
        );
    }

    /// What the kernel has for one TCP connection: `SO_KEEPALIVE` and the three
    /// probe parameters, with the role the connection was made in.
    #[cfg(target_os = "linux")]
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct KeepaliveState {
        role: Role,
        enabled: bool,
        idle: Duration,
        interval: Duration,
        count: u32,
    }

    /// A session that records the [`KeepaliveState`] of each TCP connection it
    /// is handed and then holds the connection until the peer closes it.
    #[cfg(target_os = "linux")]
    #[derive(Default)]
    struct KeepaliveProbe {
        seen: Mutex<Vec<KeepaliveState>>,
    }

    #[cfg(target_os = "linux")]
    impl KeepaliveProbe {
        fn seen(&self) -> Vec<KeepaliveState> {
            self.seen.lock().expect("probe").clone()
        }
    }

    #[cfg(target_os = "linux")]
    impl Session for KeepaliveProbe {
        fn run(&self, mut connection: Connection) -> SessionFuture {
            let socket =
                socket2::SockRef::from(connection.stream.as_tcp().expect("a TCP connection"));
            self.seen.lock().expect("probe").push(KeepaliveState {
                role: connection.role,
                enabled: socket.keepalive().expect("SO_KEEPALIVE"),
                idle: socket.tcp_keepalive_time().expect("idle time"),
                interval: socket.tcp_keepalive_interval().expect("probe interval"),
                count: socket.tcp_keepalive_retries().expect("probe count"),
            });
            Box::pin(async move {
                connection.handshake.complete();
                let mut byte = [0u8; 1];
                let _ = connection.stream.read(&mut byte).await;
                Ok(())
            })
        }
    }

    /// Claim: **`ZMQ_TCP_KEEPALIVE` and its tuning options reach both kinds of
    /// TCP connection**, the one a listener accepts and the one a connecter
    /// dials, each from its own socket's options. A half-open connection is
    /// found by the kernel's probes or not at all while the heartbeat is off,
    /// and the probes are configured on the connection, not on the socket
    /// that listens or dials.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn keepalive_is_set_on_accepted_and_dialled_connections() {
        let keepalive = |idle: u64, intvl: u64, cnt: u32| SocketOptions {
            tcp_keepalive: Some(true),
            tcp_keepalive_idle: Some(Duration::from_secs(idle)),
            tcp_keepalive_intvl: Some(Duration::from_secs(intvl)),
            tcp_keepalive_cnt: Some(cnt),
            ..options()
        };
        let ctx = context();
        let accepting = Arc::new(KeepaliveProbe::default());
        let server = Engine::new(&ctx, keepalive(31, 7, 4), accepting.clone()).expect("server");
        let endpoint = server
            .bind(&Endpoint::parse("tcp://127.0.0.1:0").expect("endpoint"))
            .await
            .expect("bind");

        let dialling = Arc::new(KeepaliveProbe::default());
        let client = Engine::new(&ctx, keepalive(43, 9, 6), dialling.clone()).expect("client");
        client.connect(&endpoint).expect("connect");
        wait_for(|| !accepting.seen().is_empty() && !dialling.seen().is_empty()).await;

        let state = |role, idle, interval, count| KeepaliveState {
            role,
            enabled: true,
            idle: Duration::from_secs(idle),
            interval: Duration::from_secs(interval),
            count,
        };
        assert_eq!(accepting.seen(), [state(Role::Binder, 31, 7, 4)]);
        assert_eq!(dialling.seen(), [state(Role::Connecter, 43, 9, 6)]);
    }
}
