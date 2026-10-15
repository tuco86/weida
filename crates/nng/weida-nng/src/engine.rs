//! The endpoint engine: dialers, listeners, pipes and reconnect.
//!
//! This is everything an SP socket does *below* its protocol. "A socket owns
//! zero or more listener and dialer endpoints; endpoints create pipes, which
//! are message-oriented connected streams and commonly map 1:1 to TCP or IPC
//! connections", and "either role may listen, dial, or do both; endpoint
//! direction does not prescribe request/reply or other application role"
//! (`docs/research/nanomsg-nng.md` §1). So an [`Engine`] owns the
//! [`Dialer`]s, the [`Listener`]s and one [`Pipe`] per live connection, and
//! the socket types above choose which pipe a message goes to.
//!
//! **A pipe is a connection, not an endpoint.** Unlike ZMTP, where a queue
//! is created when a connection is *initiated* and survives reconnects, an
//! SP pipe exists only while its connection does and "communication over
//! that pipe is then impossible" once it is removed (§1). Nothing can be
//! queued here for a peer that has not arrived.
//!
//! **A synchronous dial waits for the peer's protocol header.** "The
//! synchronous `nng_dial()` returns only after the peer's 8-octet protocol
//! header has arrived, not when the TCP connection is established", and a
//! program that calls it from the thread that must answer a greeting
//! "deadlocks until the socket timeout fires" (§1). [`Engine::dial`] keeps
//! the first half and improves the second: it is `async`, so waiting for the
//! header occupies no thread and the reactor keeps serving every other
//! socket in the process — the deadlock the sheet describes cannot be
//! constructed. It is also bounded, by
//! [`SocketOptions::handshake_timeout`](crate::SocketOptions::handshake_timeout),
//! where NNG waits for its socket timeout.
//!
//! **Every timer goes through `weida-runtime`'s `Exec`**: the reconnect
//! backoff and the handshake deadline. Nothing here calls `tokio::time`.
//!
//! **A connection is set up under a budget, a slot and a handshake
//! ceiling.** What a peer has to do before it is a pipe — answer the TCP
//! connect, run the TLS handshake, send its 8-octet protocol header — is
//! bounded at every step it could stall: each TCP connect attempt is under
//! the handshake timeout, and the TLS handshake and the header share one.
//! The pipe slot is reserved **before** the TLS handshake, so a connection
//! over the pipe ceiling, or over one remote host's share of it
//! ([`SocketOptions::max_pipes_per_peer`](crate::SocketOptions::max_pipes_per_peer)),
//! is refused for the price of a lock; TLS handshakes run in the task of
//! their own connection and not in the loop that accepts, so a peer that
//! says nothing stops nobody else; and at most
//! [`SocketOptions::max_tls_handshakes`](crate::SocketOptions::max_tls_handshakes)
//! of them run at once.
//!
//! **A socket's pipes are one immutable set.** [`Engine::pipes`] is a
//! reference count: the set is rebuilt when a pipe is admitted or retired,
//! never edited, so every send and receive reads a consistent view without
//! collecting one. What the pipes tell the socket — a message arrived,
//! room appeared, the set changed — is [`Signals`], one
//! wake-up per socket and not one future per pipe.
//!
//! What this module does **not** do is speak SP. A connection is handed to a
//! [`Session`], and the 8-octet header, the pairing check and the 64-bit
//! framing are that session's
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §5.2).

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::futures::Notified;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::JoinHandle;
use weida_runtime::Exec;
use weida_sp::EndpointType;

use crate::context::{Context, SocketSlot};
use crate::endpoint::{Endpoint, TcpHost};
use crate::error::{Cause, Error, Result};
use crate::message::{Message, RECV_MAX_SIZE_UNLIMITED};
use crate::options::{EndpointOptions, SocketOptions};
use crate::pipe::{Discarded, Pipe, PipeId, Signals, crosses};
use crate::tls;
use crate::transport::{PeerKey, Stream, tune_tcp};

/// What a [`Session`] returns: a future that ends when the connection does.
pub type SessionFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// Which side of a connection this socket is.
///
/// SP itself does not care — "either role may listen, dial, or do both;
/// endpoint direction does not prescribe request/reply or other application
/// role" (§1) — but reconnect does: only a dialer redials, because only a
/// dialer has an address to redial.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// This side dialled the peer.
    Dial,
    /// This side accepted the peer.
    Listen,
}

/// How a session tells the engine that the peer's protocol header arrived.
///
/// The handshake is the only thing between a connection and a pipe, and only
/// the session can see it happen. So the engine hands this over with the
/// connection, races it against
/// [`SocketOptions::handshake_timeout`](crate::SocketOptions::handshake_timeout),
/// and closes the connection if the timer wins — which is what keeps a peer
/// that connects and says nothing from holding a pipe slot for as long as it
/// likes.
#[derive(Debug)]
pub struct HandshakeGate {
    done: Option<oneshot::Sender<EndpointType>>,
}

impl HandshakeGate {
    /// Reports that the peer's header arrived and named `peer`.
    ///
    /// Idempotent: a second call does nothing, because there is only one
    /// handshake per connection and a session that called twice would
    /// otherwise be a panic rather than a bug.
    pub fn complete(&mut self, peer: EndpointType) {
        if let Some(done) = self.done.take() {
            let _ = done.send(peer);
        }
    }
}

/// One established connection, handed to a [`Session`].
#[derive(Debug)]
pub struct Connection {
    /// The byte stream.
    pub stream: Stream,
    /// The queues this connection carries, already bounded.
    pub pipe: Pipe,
    /// Which side dialled.
    pub role: Role,
    /// The endpoint this connection belongs to, for a log line.
    pub endpoint: Endpoint,
    /// What this socket speaks, which is what goes in its protocol header.
    pub protocol: EndpointType,
    /// The most octets one message may declare on this connection, header
    /// and body together: `NNG_OPT_RECVMAXSZ` for this endpoint, with the
    /// tag stack added for the protocols that carry one (see
    /// [`SocketOptions::recv_max_size`]).
    pub recv_max_size: u64,
    /// Signalled once the peer's protocol header has been read and accepted.
    pub handshake: HandshakeGate,
    /// The reactor, for the clocks a session keeps.
    pub exec: Exec,
    /// [`SocketOptions::message_timeout`].
    pub message_timeout: Option<Duration>,
    /// [`SocketOptions::idle_timeout`].
    pub idle_timeout: Option<Duration>,
}

/// What drives one connection once its bytes flow: in this library, SP.
///
/// One implementation for every protocol, because the 8-octet header and the
/// 64-bit framing do not depend on the pattern — the protocol is a *field*
/// in the header, not a different wire format (§3).
pub trait Session: Send + Sync + 'static {
    /// Runs `connection` until it ends.
    fn start(&self, connection: Connection) -> SessionFuture;
}

/// A pipe lifecycle event, as `nng_pipe_notify(3)` defines them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PipeEvent {
    /// `NNG_PIPE_EV_ADD_PRE`: the connection handshook and the pipe is about
    /// to enter the socket. This is the only event whose answer matters: a
    /// callback may refuse the pipe here, "for example after local
    /// authorization logic", and that is "application policy, not SP
    /// authorization" (§10).
    AddPre,
    /// `NNG_PIPE_EV_ADD_POST`: the pipe is in the socket and traffic may
    /// flow.
    AddPost,
    /// `NNG_PIPE_EV_REM_POST`: the pipe is gone and "communication over that
    /// pipe is then impossible" (§1).
    RemPost,
}

/// What a callback knows about a pipe.
///
/// **A value, not a handle, and that is the rule NNG can only write down.**
/// NNG's callback "runs under the socket lock and must not access the
/// socket" (§2) — a sentence a C program obeys or deadlocks. Here the
/// callback is handed this and nothing else, so touching the socket is not
/// something it can do wrongly; it is something it cannot express.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipeInfo {
    /// The pipe's id within its socket.
    pub id: PipeId,
    /// Which side dialled.
    pub role: Role,
    /// The endpoint that created this pipe.
    pub endpoint: Endpoint,
    /// What the peer said it was, once its header has been read.
    pub peer: Option<EndpointType>,
    /// `NNG_OPT_LOCADDR`: this end's address, where the transport has one.
    pub local_addr: Option<String>,
    /// `NNG_OPT_REMADDR`: the peer's address, where the transport has one.
    pub remote_addr: Option<String>,
    /// What the kernel said about the peer, for the transport where that
    /// question has an answer.
    ///
    /// `Some` only for `ipc://`: "IPC can expose OS-derived peer UID, GID,
    /// PID" and the UID and GID "are described as non-forgeable at
    /// connection time" (§10). This is where "local authorization logic"
    /// reads them — a pipe-add-pre callback may refuse the pipe on them,
    /// "application policy, not SP authorization" (§10) — and the PID is
    /// an observation that must not be authorized on
    /// ([0010](../../../docs/decisions/0010-local-transport.md) §4.4).
    pub credentials: Option<weida_core::LocalPrincipal>,
    /// What TLS established about the peer, for the one transport where
    /// that question has an answer.
    ///
    /// `Some` only for `tls+tcp://`. It authenticates the transport peer
    /// of this one connection and terminates there, and no API turns it
    /// into a sender identity — see [`crate::tls`].
    pub tls: Option<crate::tls::TlsPeer>,
}

/// A callback's answer to [`PipeEvent::AddPre`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// Let the pipe into the socket.
    Accept,
    /// Refuse it. The connection is closed and nothing is sent to the peer,
    /// because SP has no way to say more than that (§6). The cause is for
    /// this side's log.
    Reject(Cause),
}

/// What the engine calls on a pipe event.
pub type PipeCallback = Arc<dyn Fn(PipeEvent, &PipeInfo) -> Admission + Send + Sync>;

/// A dialer's identity within its socket, as `nng_dialer_id()` reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DialerId(u32);

impl DialerId {
    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A listener's identity within its socket, as `nng_listener_id()` reports
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListenerId(u32);

impl ListenerId {
    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// One named dialling endpoint of a socket.
///
/// "A dialer initiates a connection to a URL … Both are associated with one
/// socket and create pipes" (§2). It is a value the application holds, so
/// that one of a socket's several dialers can be closed without closing the
/// others.
#[derive(Clone, Debug)]
pub struct Dialer {
    id: DialerId,
    url: Endpoint,
    engine: Engine,
}

impl Dialer {
    /// This dialer's id within its socket.
    pub const fn id(&self) -> DialerId {
        self.id
    }

    /// `NNG_OPT_URL`: the endpoint this dialer dials.
    pub const fn url(&self) -> &Endpoint {
        &self.url
    }

    /// `nng_dialer_close()`: stop dialling and redialling. A pipe this
    /// dialer already created is closed with it, because a pipe belongs to
    /// the endpoint that made it (§2).
    pub fn close(&self) {
        self.engine.close_dialer(self.id);
    }
}

/// One named listening endpoint of a socket.
#[derive(Clone, Debug)]
pub struct Listener {
    id: ListenerId,
    url: Endpoint,
    engine: Engine,
}

impl Listener {
    /// This listener's id within its socket.
    pub const fn id(&self) -> ListenerId {
        self.id
    }

    /// `NNG_OPT_URL`: the endpoint actually listened on, which is the only
    /// way to learn the port behind a wildcard.
    pub const fn url(&self) -> &Endpoint {
        &self.url
    }

    /// `nng_listener_close()`: stop accepting, and close the pipes this
    /// listener created.
    pub fn close(&self) {
        self.engine.close_listener(self.id);
    }
}

/// The pipes a socket may use, at one moment.
///
/// **Immutable.** A change of membership builds a new set instead of
/// editing this one, so a reader holds a consistent view for as long as it
/// likes and takes one for the price of a reference count — which is what
/// every send and receive does, where it used to collect, clone and drop a
/// vector of pipes. Ascending by pipe id, so a pipe is found by binary
/// search and a round-robin cursor means the same thing on every look at
/// one set.
#[derive(Debug, Default)]
pub struct PipeSet {
    pipes: Vec<Pipe>,
}

impl PipeSet {
    /// The pipe named `id`, if it is in this set.
    pub fn get(&self, id: PipeId) -> Option<&Pipe> {
        self.pipes
            .binary_search_by_key(&id, Pipe::id)
            .ok()
            .and_then(|at| self.pipes.get(at))
    }
}

impl Deref for PipeSet {
    type Target = [Pipe];

    fn deref(&self) -> &[Pipe] {
        &self.pipes
    }
}

struct PipeEntry {
    pipe: Pipe,
    info: PipeInfo,
    /// False while the connection is handshaking. Such a pipe is invisible
    /// to the socket and still counts against the ceiling, because a
    /// connection nobody has authenticated is exactly the one an attacker
    /// opens.
    admitted: bool,
    dialer: Option<DialerId>,
    listener: Option<ListenerId>,
    /// What this pipe counts against under
    /// [`SocketOptions::max_pipes_per_peer`]: set for a connection a
    /// listener accepted, from the moment its slot is reserved.
    source: Option<PeerKey>,
}

/// One live dialer or listener: the task that runs it, which is what
/// closing one aborts. The URL lives on the [`Dialer`] or [`Listener`] the
/// application holds, so it is not kept twice.
struct EndpointEntry {
    task: JoinHandle<()>,
}

/// What closing a socket does to the sessions of the connections its dialers
/// hold.
///
/// **A dialled connection's session is a local of its dialer's task**:
/// `dialer_loop` awaits `run_pipe`, which awaits the session, so aborting
/// the dialer drops the reader and the writer where they stand. An accepted
/// connection has a task of its own, which no abort of a listener reaches,
/// so the choice exists for dialled connections only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DialedSessions {
    /// Stopped with their dialers, with whatever the writer holds unwritten:
    /// what `nng_close()` asks for, which discards.
    Abort,
    /// Left running. Closing each pipe ends its session, because the writer
    /// finishes the message it has taken and then finds its queue closed, so
    /// a message handed to the writer is written however soon the socket is
    /// dropped behind it. The dialer returns when its session has, and a
    /// dialer with no connection up has nothing to finish and is aborted.
    Finish,
}

struct EngineState {
    pipes: HashMap<PipeId, PipeEntry>,
    /// Pipes per source, the numbers [`SocketOptions::max_pipes_per_peer`]
    /// is judged against. An entry exists only while it is nonzero.
    sources: HashMap<PeerKey, usize>,
    dialers: HashMap<DialerId, EndpointEntry>,
    listeners: HashMap<ListenerId, EndpointEntry>,
    next_pipe: u32,
    next_endpoint: u32,
    closed: bool,
}

impl EngineState {
    /// Takes one pipe off `source`'s count.
    fn release(&mut self, source: Option<PeerKey>) {
        let Some(source) = source else { return };
        if let Some(count) = self.sources.get_mut(&source) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.sources.remove(&source);
            }
        }
    }
}

/// The messages that arrived on pipes which have since been retired.
#[derive(Default)]
struct Leftovers {
    messages: VecDeque<(PipeId, Message)>,
    /// What the queued messages occupy, for the same ceiling a pipe's queue
    /// has.
    bytes: u64,
}

struct EngineInner {
    exec: Exec,
    protocol: EndpointType,
    options: SocketOptions,
    session: Arc<dyn Session>,
    notify: Mutex<Option<PipeCallback>>,
    state: Mutex<EngineState>,
    /// The admitted pipes as one immutable set, replaced whenever the
    /// membership changes. Readers take the lock for as long as it takes to
    /// clone an `Arc`, and never touch `state`.
    snapshot: RwLock<Arc<PipeSet>>,
    /// Woken whenever the admitted pipe set changes, so a task waiting for
    /// a peer does not poll.
    changed: Notify,
    /// What this socket's pipes tell its receivers and senders as a set;
    /// also woken when the pipe set changes.
    signals: Arc<Signals>,
    /// Messages that arrived on a pipe which has since been retired.
    ///
    /// The socket's own receive buffer, in miniature and only for the one
    /// case that needs it: a peer that answers and then closes. Bounded by
    /// [`Engine::store_arrived`], which is where the ceiling and the
    /// full-queue policy are, because the pipe it came from is gone by
    /// then and its bound went with it.
    leftovers: Mutex<Leftovers>,
    /// Whether `leftovers` holds anything, readable without its lock: every
    /// receive asks, and almost every answer is no.
    has_leftovers: AtomicBool,
    /// Holds this socket's slot under its context's ceiling for as long as
    /// the engine lives.
    slot: SocketSlot,
    /// This context's `inproc://` namespace, so that a socket can bind and
    /// dial a name in it.
    inproc: Arc<crate::inproc::Inproc>,
}

/// The transport-facing half of a socket: what dials, listens, reconnects
/// and holds the pipes.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

/// What a task needs to run a connection without keeping the socket alive.
#[derive(Clone)]
struct TaskCtx {
    engine: Engine,
    /// What the dialer or listener that owns this task configured for
    /// itself, which is where a per-endpoint `NNG_OPT_RECVMAXSZ` lives.
    endpoint_options: EndpointOptions,
}

impl Engine {
    /// Creates the engine of a socket speaking `protocol`.
    ///
    /// Fails with `NNG_EINVAL` or `NNG_ENOTSUP` for a configuration this
    /// protocol cannot have, and with `NNG_ENOFILES` when the context is
    /// already at its socket ceiling.
    pub fn new(
        context: &Context,
        protocol: EndpointType,
        options: SocketOptions,
        session: Arc<dyn Session>,
    ) -> Result<Engine> {
        options.validate_for(protocol)?;
        let slot = context.open_socket()?;
        Ok(Engine {
            inner: Arc::new(EngineInner {
                exec: context.exec().clone(),
                protocol,
                options,
                session,
                notify: Mutex::new(None),
                state: Mutex::new(EngineState {
                    pipes: HashMap::new(),
                    sources: HashMap::new(),
                    dialers: HashMap::new(),
                    listeners: HashMap::new(),
                    next_pipe: 1,
                    next_endpoint: 1,
                    closed: false,
                }),
                snapshot: RwLock::new(Arc::new(PipeSet::default())),
                changed: Notify::new(),
                signals: Arc::new(Signals::new()),
                leftovers: Mutex::new(Leftovers::default()),
                has_leftovers: AtomicBool::new(false),
                slot,
                inproc: Arc::clone(context.inproc()),
            }),
        })
    }

    /// What this socket speaks.
    pub fn protocol(&self) -> EndpointType {
        self.inner.protocol
    }

    /// The options this socket runs under.
    pub fn options(&self) -> &SocketOptions {
        &self.inner.options
    }

    /// The reactor this socket's tasks and timers run on.
    pub fn exec(&self) -> &Exec {
        &self.inner.exec
    }

    /// `nng_pipe_notify()`: install the callback that sees every pipe
    /// event, replacing any installed before.
    ///
    /// One callback for all three events rather than NNG's three
    /// registrations, because the three are one lifecycle and a program that
    /// registers for two of them has a gap it did not decide to have.
    pub fn notify(&self, callback: PipeCallback) {
        *self.inner.notify.lock().expect("notify poisoned") = Some(callback);
    }

    /// Every pipe the socket may use right now, as one immutable set.
    ///
    /// A reference count, not a collection: see [`PipeSet`].
    pub fn pipes(&self) -> Arc<PipeSet> {
        Arc::clone(&self.inner.snapshot.read().expect("pipe set poisoned"))
    }

    /// What this socket's pipes tell its receivers and senders as a set.
    ///
    /// Fed by every pipe's queues and by every change of membership. The
    /// rule for using it is the one in [`Signals`]: create the wait, then
    /// look, then await.
    pub fn signals(&self) -> &Signals {
        &self.inner.signals
    }

    /// What the socket knows about each of its pipes, in id order.
    pub fn pipe_infos(&self) -> Vec<PipeInfo> {
        let mut infos: Vec<PipeInfo> = self
            .lock()
            .pipes
            .values()
            .filter(|entry| entry.admitted)
            .map(|entry| entry.info.clone())
            .collect();
        infos.sort_by_key(|info| info.id);
        infos
    }

    /// A wait that ends when the admitted pipe set changes.
    ///
    /// A task with nothing to send to waits on this rather than polling:
    /// "with no eligible peer, the send waits or times out" (§4), and this
    /// is the wait.
    ///
    /// **Not an `async fn`, and that is the point.** The wait exists from
    /// the moment this returns, and sees every change from then on, so a
    /// caller that creates it *before* it looks at [`Engine::pipes`] cannot
    /// miss a pipe that arrives in between. An `async fn` would not exist
    /// until first polled, which is after the look.
    pub fn changed(&self) -> Notified<'_> {
        self.inner.changed.notified()
    }

    /// Whether the socket has been closed.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Dials `endpoint`, returning only once the peer's protocol header has
    /// arrived — `nng_dial()` without `NNG_FLAG_NONBLOCK` (§1).
    ///
    /// A refused first connection is reported and **no retry is started**,
    /// which is NNG's rule for this form; once a pipe has existed, the
    /// dialer redials it whenever it closes, which is NNG's rule for every
    /// form. A handshake that fails on that first connection is reported the
    /// same way as a refused connect, because the pipe never existed.
    ///
    /// Unlike NNG's, this wait costs no thread and is bounded by
    /// [`SocketOptions::handshake_timeout`](crate::SocketOptions::handshake_timeout)
    /// — for each address it tries to connect to, and then for the TLS
    /// handshake and the protocol header together.
    pub async fn dial(&self, endpoint: &Endpoint) -> Result<Dialer> {
        self.dial_with(endpoint, EndpointOptions::default()).await
    }

    /// Dials `endpoint` under this endpoint's own options — which is
    /// where a per-endpoint `NNG_OPT_RECVMAXSZ` goes, "set before endpoint
    /// creation, ideally per listener/dialer" (§3).
    pub async fn dial_with(
        &self,
        endpoint: &Endpoint,
        endpoint_options: EndpointOptions,
    ) -> Result<Dialer> {
        let (id, rx) = self.start_dialer(endpoint, false, endpoint_options)?;
        match rx.await {
            Ok(Ok(())) => Ok(self.dialer_handle(id, endpoint.clone())),
            Ok(Err(error)) => {
                self.close_dialer(id);
                Err(error)
            }
            Err(_) => {
                self.close_dialer(id);
                Err(Error::ECLOSED(
                    "the socket was closed while the dial was in progress".into(),
                ))
            }
        }
    }

    /// Dials `endpoint` without waiting — `NNG_FLAG_NONBLOCK` (§1).
    ///
    /// "Makes that first attempt asynchronous, after which failures retry
    /// periodically", with the backoff of `NNG_OPT_RECONNMINT` and
    /// `NNG_OPT_RECONNMAXT`. The dialer exists immediately and the pipe
    /// appears whenever the peer does.
    pub fn dial_nonblocking(&self, endpoint: &Endpoint) -> Result<Dialer> {
        self.dial_nonblocking_with(endpoint, EndpointOptions::default())
    }

    /// The non-blocking dial under this endpoint's own options.
    pub fn dial_nonblocking_with(
        &self,
        endpoint: &Endpoint,
        endpoint_options: EndpointOptions,
    ) -> Result<Dialer> {
        let (id, _rx) = self.start_dialer(endpoint, true, endpoint_options)?;
        Ok(self.dialer_handle(id, endpoint.clone()))
    }

    /// Listens on `endpoint` and accepts automatically.
    ///
    /// Returns the endpoint actually bound, which for a wildcard port is the
    /// only way to learn the port.
    pub async fn listen(&self, endpoint: &Endpoint) -> Result<Listener> {
        self.listen_with(endpoint, EndpointOptions::default()).await
    }

    /// Listens under this endpoint's own options — which is where a
    /// per-listener `NNG_OPT_RECVMAXSZ` goes, so that a socket can hold a
    /// tighter limit on the address strangers reach than on the one it
    /// dialled itself (§11).
    pub async fn listen_with(
        &self,
        endpoint: &Endpoint,
        endpoint_options: EndpointOptions,
    ) -> Result<Listener> {
        // Before the OS, not after: a closed socket refuses without taking
        // an address, so a close followed by a listen reports the close
        // rather than whatever the address happens to be doing.
        if self.is_closed() {
            return Err(closed());
        }
        let listener = bind(self, endpoint).await?;
        let bound = listener.bound_endpoint(endpoint)?;
        let id = {
            let mut state = self.lock();
            if state.closed {
                return Err(closed());
            }
            ListenerId(next(&mut state.next_endpoint))
        };
        let ctx = TaskCtx {
            engine: self.clone(),
            endpoint_options,
        };
        let url = bound.clone();
        let task = self
            .inner
            .exec
            .spawn(accept_loop(ctx, listener, id, url.clone()));
        let mut state = self.lock();
        if state.closed {
            task.abort();
            return Err(closed());
        }
        state.listeners.insert(id, EndpointEntry { task });
        drop(state);
        Ok(Listener {
            id,
            url,
            engine: self.clone(),
        })
    }

    /// `nng_dialer_close()`.
    pub fn close_dialer(&self, id: DialerId) {
        let entry = self.lock().dialers.remove(&id);
        if let Some(entry) = entry {
            entry.task.abort();
        }
        // Retired here rather than left to the task that was driving them:
        // aborting a dialer stops the very loop that would have reported
        // the removal, and a pipe nobody retires is a pipe that stays in
        // the socket's table for ever.
        for entry in self.remove_pipes(|entry| entry.dialer == Some(id)) {
            self.finish(entry);
        }
    }

    /// `nng_listener_close()`.
    pub fn close_listener(&self, id: ListenerId) {
        let entry = self.lock().listeners.remove(&id);
        if let Some(entry) = entry {
            entry.task.abort();
        }
        for entry in self.remove_pipes(|entry| entry.listener == Some(id)) {
            self.finish(entry);
        }
    }

    /// `nng_pipe_close()`: destroy one pipe and everything in its queues.
    ///
    /// The session sees its queues close and ends, which removes the entry
    /// and fires [`PipeEvent::RemPost`].
    pub fn close_pipe(&self, id: PipeId) -> Discarded {
        let pipe = self.lock().pipes.get(&id).map(|entry| entry.pipe.clone());
        match pipe {
            Some(pipe) => pipe.close(),
            None => Discarded::default(),
        }
    }

    /// The socket's last handle was dropped: stop dialling and accepting at
    /// once, and close each pipe after it has written what is already queued
    /// for its peer, for at most [`DEFAULT_CLOSE_BUDGET`].
    ///
    /// A close the application asked for discards what is queued — NNG has
    /// no linger, and [`Engine::close`] is that. A drop is not a decision
    /// about the data: `send` returns once a message is queued, and a reply
    /// sent just before its socket went out of scope would otherwise be lost
    /// in the moment between the queue and the connection's writer. A socket
    /// with nothing queued closes on the spot.
    ///
    /// **This holds for a connection the socket dialled as it does for one
    /// it accepted**, and for a message the writer has already taken as for
    /// one still queued: see [`DialedSessions::Finish`].
    pub(crate) fn close_after_flush(&self) {
        let waiting: Vec<Pipe> = self
            .pipes()
            .iter()
            .filter(|pipe| !pipe.outgoing().is_empty())
            .cloned()
            .collect();
        if waiting.is_empty() {
            self.shut(DialedSessions::Finish);
            return;
        }
        let stopped = {
            let mut state = self.lock();
            state.closed = true;
            Self::take_endpoints(&mut state, DialedSessions::Finish)
        };
        for task in stopped {
            task.abort();
        }
        // Receivers parked on this socket find it closed now.
        self.membership_changed();
        let engine = self.clone();
        self.inner.exec.spawn(async move {
            let deadline = Instant::now() + crate::context::DEFAULT_CLOSE_BUDGET;
            for pipe in &waiting {
                let left = deadline.saturating_duration_since(Instant::now());
                let _ = engine.exec().within(left, pipe.outgoing().drained()).await;
            }
            engine.shut(DialedSessions::Finish);
        });
    }

    /// `nng_close()`: stop dialling and accepting, and destroy every pipe.
    pub fn close(&self) {
        self.shut(DialedSessions::Abort);
    }

    /// Closes the socket: the endpoints stop, and every pipe is taken out of
    /// the table and destroyed.
    fn shut(&self, sessions: DialedSessions) {
        let (stopped, pipes) = {
            let mut state = self.lock();
            state.closed = true;
            // Before the pipes leave the table: which dialers hold a
            // connection is read from it.
            let stopped = Self::take_endpoints(&mut state, sessions);
            let pipes: Vec<PipeEntry> = state.pipes.drain().map(|(_, entry)| entry).collect();
            state.sources.clear();
            self.publish(&state);
            (stopped, pipes)
        };
        for task in stopped {
            task.abort();
        }
        for entry in pipes {
            self.finish(entry);
        }
        self.membership_changed();
    }

    /// Takes every listener and dialer out of the table and returns the tasks
    /// that are to be aborted: all of them for [`DialedSessions::Abort`], and
    /// for [`DialedSessions::Finish`] all but the dialers whose connection is
    /// up, which are left running.
    fn take_endpoints(state: &mut EngineState, sessions: DialedSessions) -> Vec<JoinHandle<()>> {
        let mut stopped: Vec<JoinHandle<()>> = state
            .listeners
            .drain()
            .map(|(_, entry)| entry.task)
            .collect();
        for (id, entry) in state.dialers.drain() {
            let connected = state
                .pipes
                .values()
                .any(|pipe| pipe.admitted && pipe.dialer == Some(id));
            if sessions == DialedSessions::Finish && connected {
                // Dropping a handle detaches its task, which is the point: the
                // task ends by itself, see the variant's documentation.
                continue;
            }
            stopped.push(entry.task);
        }
        stopped
    }

    fn dialer_handle(&self, id: DialerId, url: Endpoint) -> Dialer {
        Dialer {
            id,
            url,
            engine: self.clone(),
        }
    }

    fn start_dialer(
        &self,
        endpoint: &Endpoint,
        retry_first: bool,
        endpoint_options: EndpointOptions,
    ) -> Result<(DialerId, oneshot::Receiver<Result<()>>)> {
        let id = {
            let mut state = self.lock();
            if state.closed {
                return Err(closed());
            }
            DialerId(next(&mut state.next_endpoint))
        };
        let (tx, rx) = oneshot::channel();
        let ctx = TaskCtx {
            engine: self.clone(),
            endpoint_options,
        };
        let task = self.inner.exec.spawn(dialer_loop(
            ctx,
            id,
            endpoint.clone(),
            Some(tx),
            retry_first,
        ));
        let mut state = self.lock();
        if state.closed {
            task.abort();
            return Err(closed());
        }
        state.dialers.insert(id, EndpointEntry { task });
        Ok((id, rx))
    }

    /// Reserves a pipe slot against the socket's ceilings: the whole
    /// socket's, and for a connection a listener accepted, the one remote
    /// host's or local user's.
    ///
    /// Called before anything about the connection costs this side work —
    /// before a TLS handshake in particular — so that a connection that is
    /// going to be refused is refused for the price of a lock.
    fn reserve(
        &self,
        role: Role,
        endpoint: &Endpoint,
        dialer: Option<DialerId>,
        listener: Option<ListenerId>,
        facts: Facts,
        recv_limit: u64,
    ) -> Result<(PipeId, Pipe)> {
        let options = &self.inner.options;
        let mut state = self.lock();
        if state.closed {
            return Err(closed());
        }
        let ceiling = options.max_pipes;
        if state.pipes.len() >= ceiling {
            return Err(Error::ENOFILES(
                format!("this socket already holds its ceiling of {ceiling} pipes").into(),
            ));
        }
        let source = if role == Role::Listen {
            facts.source
        } else {
            None
        };
        if let Some(key) = source {
            let ceiling = options.max_pipes_per_peer;
            if state.sources.get(&key).copied().unwrap_or(0) >= ceiling {
                return Err(Error::ENOFILES(
                    format!(
                        "this socket already holds its ceiling of {ceiling} pipes from one peer"
                    )
                    .into(),
                ));
            }
        }
        let id = free_pipe_id(&mut state)?;
        let pipe = Pipe::linked(
            id,
            options.pipe_config(self.inner.protocol),
            &self.inner.signals,
        )
        .with_recv_limit(recv_limit);
        if let Some(key) = source {
            *state.sources.entry(key).or_insert(0) += 1;
        }
        state.pipes.insert(
            id,
            PipeEntry {
                pipe: pipe.clone(),
                info: PipeInfo {
                    id,
                    role,
                    endpoint: endpoint.clone(),
                    peer: None,
                    local_addr: facts.local_addr,
                    remote_addr: facts.remote_addr,
                    credentials: facts.credentials,
                    tls: facts.tls,
                },
                admitted: false,
                dialer,
                listener,
                source,
            },
        );
        Ok((id, pipe))
    }

    /// Records what TLS established about the peer of a pipe that is still
    /// handshaking, which is what the pipe-add-pre callback is then shown.
    fn set_tls_peer(&self, id: PipeId, peer: &tls::TlsPeer) {
        if let Some(entry) = self.lock().pipes.get_mut(&id) {
            entry.info.tls = Some(peer.clone());
        }
    }

    /// Runs the callback, if one is installed. Never under the state lock:
    /// the callback is the application's code and the socket's lock is not
    /// the application's to hold.
    fn fire(&self, event: PipeEvent, info: &PipeInfo) -> Admission {
        let callback = self.inner.notify.lock().expect("notify poisoned").clone();
        match callback {
            Some(callback) => callback(event, info),
            None => Admission::Accept,
        }
    }

    /// Moves a handshaken pipe into the socket, or refuses it.
    fn admit(&self, id: PipeId, peer: EndpointType) -> Result<()> {
        let info = {
            let mut state = self.lock();
            let Some(entry) = state.pipes.get_mut(&id) else {
                return Err(closed());
            };
            entry.info.peer = Some(peer);
            entry.info.clone()
        };
        if let Admission::Reject(why) = self.fire(PipeEvent::AddPre, &info) {
            return Err(Error::EPEERAUTH(why));
        }
        {
            let mut state = self.lock();
            if state.closed {
                return Err(closed());
            }
            match state.pipes.get_mut(&id) {
                Some(entry) => entry.admitted = true,
                None => return Err(closed()),
            }
            self.publish(&state);
        }
        self.fire(PipeEvent::AddPost, &info);
        self.membership_changed();
        Ok(())
    }

    /// Removes a pipe and fires `REM_POST`, whether it was ever admitted or
    /// not.
    fn retire(&self, id: PipeId) {
        let entry = {
            let mut state = self.lock();
            let Some(entry) = state.pipes.remove(&id) else {
                return;
            };
            state.release(entry.source);
            if entry.admitted {
                self.publish(&state);
            }
            entry
        };
        self.finish(entry);
    }

    /// Takes every pipe `wanted` picks out of the table, with the
    /// membership published once for all of them.
    fn remove_pipes(&self, mut wanted: impl FnMut(&PipeEntry) -> bool) -> Vec<PipeEntry> {
        let mut state = self.lock();
        let ids: Vec<PipeId> = state
            .pipes
            .iter()
            .filter(|(_, entry)| wanted(entry))
            .map(|(id, _)| *id)
            .collect();
        let mut removed = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(entry) = state.pipes.remove(&id) {
                state.release(entry.source);
                removed.push(entry);
            }
        }
        if removed.iter().any(|entry| entry.admitted) {
            self.publish(&state);
        }
        removed
    }

    /// What happens to a pipe after it has left the table: what it had
    /// already received is kept, its queues are destroyed, and the socket's
    /// waiters are told.
    fn finish(&self, entry: PipeEntry) {
        // What already arrived is not lost with the pipe. NNG keeps
        // received messages in the socket's own receive buffer rather
        // than in the pipe, so a peer that replies and then closes —
        // which is what a REP socket answering its last request does —
        // has its answer delivered. Only what was still queued *for*
        // the peer goes, because that never left (§1).
        //
        // Only for a pipe that was admitted. One that never was — refused
        // by the pipe-add-pre callback, or too slow to finish its
        // handshake — has nothing to deliver: the peer was refused, and a
        // message it managed to queue in the moment between its header and
        // the refusal is not to reach the application on its behalf.
        let mut arrived = Vec::new();
        if entry.admitted {
            while let Ok(message) = entry.pipe.incoming().try_recv() {
                arrived.push(message);
            }
        }
        entry.pipe.close();
        self.store_arrived(entry.info.id, arrived);
        if entry.admitted {
            self.fire(PipeEvent::RemPost, &entry.info);
            self.membership_changed();
        }
    }

    /// Replaces the published set of pipes with the admitted ones in
    /// `state`. Called with the state lock held, so that the set and the
    /// table cannot disagree.
    fn publish(&self, state: &EngineState) {
        let mut pipes: Vec<Pipe> = state
            .pipes
            .values()
            .filter(|entry| entry.admitted)
            .map(|entry| entry.pipe.clone())
            .collect();
        pipes.sort_by_key(Pipe::id);
        *self.inner.snapshot.write().expect("pipe set poisoned") = Arc::new(PipeSet { pipes });
    }

    /// Wakes everything that waits on the set of pipes.
    fn membership_changed(&self) {
        self.inner.changed.notify_waiters();
        self.inner.signals.pipes_changed();
    }

    /// Puts what a retired pipe had already received into the socket's
    /// buffer, under that buffer's own ceiling.
    ///
    /// **The ceiling is the point.** Retiring the pipe frees its slot
    /// before this runs, so a stranger can connect, fill a queue,
    /// disconnect and do it again: the pipe set is bounded and the number
    /// of pipes over time is not. Nothing drains this buffer unless the
    /// application receives, so without a bound of its own a peer that
    /// reconnects in a loop would grow it without limit
    /// (`docs/INVARIANTS.md`).
    ///
    /// The bound is [`PipeConfig::incoming`](crate::PipeConfig::incoming)'s
    /// — one pipe's worth, in messages **and in octets**, not one per pipe
    /// — because this is the socket's receive buffer and not a per-pipe
    /// queue, and at least one message, because holding the last answer is
    /// the whole reason the buffer exists. The octet ceiling is the one
    /// that matters: a depth counts messages and a message may be as large
    /// as `NNG_OPT_RECVMAXSZ`, so a count alone would let reconnecting
    /// peers leave the socket holding depth × `RECVMAXSZ`.
    ///
    /// Full, and the policy is the socket's rather than the pipe's:
    /// [`FullAction::receiving`](crate::FullAction::receiving) is `Block`
    /// for every protocol and blocking is not available here — the pipe is
    /// gone and no reader can push back on a peer that has left. So the
    /// choice is the one `SUB_PREFNEW` makes at this same layer: the oldest
    /// goes for a SUB that prefers new publications, and for everyone else
    /// the **newest is refused**, because the oldest is the answer somebody
    /// is already waiting for.
    fn store_arrived(&self, id: PipeId, arrived: Vec<Message>) {
        if arrived.is_empty() {
            return;
        }
        let options = &self.inner.options;
        let mut bound = options.pipe_config(self.inner.protocol).incoming;
        bound.depth = bound.depth.max(1);
        let prefer_new = self.inner.protocol == EndpointType::Sub && options.sub_prefer_new;
        let mut refused = 0usize;
        let mut leftovers = self.inner.leftovers.lock().expect("leftovers poisoned");
        for message in arrived {
            let size = message.wire_len() as u64;
            while prefer_new
                && crosses(bound, 0, leftovers.messages.len(), leftovers.bytes, size).is_some()
            {
                let Some((_, oldest)) = leftovers.messages.pop_front() else {
                    break;
                };
                leftovers.bytes = leftovers.bytes.saturating_sub(oldest.wire_len() as u64);
            }
            if crosses(bound, 0, leftovers.messages.len(), leftovers.bytes, size).is_some() {
                refused += 1;
                continue;
            }
            leftovers.bytes = leftovers.bytes.saturating_add(size);
            leftovers.messages.push_back((id, message));
        }
        self.inner
            .has_leftovers
            .store(!leftovers.messages.is_empty(), Ordering::Release);
        drop(leftovers);
        if refused > 0 {
            tracing::debug!(
                refused,
                depth = bound.depth,
                max_bytes = bound.max_bytes,
                "the socket's buffer is full of messages from pipes that have gone"
            );
        }
    }

    /// The next message that arrived on a pipe which has since been
    /// retired, if any.
    ///
    /// A socket takes from here as one more member of the rotation it
    /// receives in, with a turn after its last pipe and not ahead of them: a
    /// message that crossed the wire has arrived, and the peer closing
    /// afterwards does not un-arrive it, but a peer that comes and goes
    /// quickly must not keep the pipes that stay waiting behind it.
    pub fn take_arrived(&self) -> Option<(PipeId, Message)> {
        if !self.inner.has_leftovers.load(Ordering::Acquire) {
            return None;
        }
        let mut leftovers = self.inner.leftovers.lock().expect("leftovers poisoned");
        let (id, message) = leftovers.messages.pop_front()?;
        leftovers.bytes = leftovers.bytes.saturating_sub(message.wire_len() as u64);
        self.inner
            .has_leftovers
            .store(!leftovers.messages.is_empty(), Ordering::Release);
        Some((id, message))
    }

    /// Whether any message is waiting from a pipe that has been retired.
    pub fn has_arrived(&self) -> bool {
        self.inner.has_leftovers.load(Ordering::Acquire)
    }

    /// Octets held for messages from pipes that have been retired: what
    /// `Engine::store_arrived` bounds.
    pub fn leftover_bytes(&self) -> u64 {
        self.inner
            .leftovers
            .lock()
            .expect("leftovers poisoned")
            .bytes
    }

    fn lock(&self) -> MutexGuard<'_, EngineState> {
        self.inner.state.lock().expect("engine state poisoned")
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("Engine")
            .field("protocol", &self.inner.protocol)
            .field("dialers", &state.dialers.len())
            .field("listeners", &state.listeners.len())
            .field("pipes", &state.pipes.len())
            .field("closed", &state.closed)
            .finish_non_exhaustive()
    }
}

fn next(counter: &mut u32) -> u32 {
    let id = *counter;
    *counter = counter.wrapping_add(1).max(1);
    id
}

/// The next pipe id no live pipe holds.
///
/// The counter is 31 bits wide because the id travels in a tag stack, so it
/// wraps, and after enough connections it revisits the ids of pipes that
/// are still open. An id taken twice would make the table forget the first
/// pipe while its session kept running. A free id is found within one more
/// try than there are live pipes, which is why the search is bounded.
fn free_pipe_id(state: &mut EngineState) -> Result<PipeId> {
    for _ in 0..=state.pipes.len() {
        let id = PipeId::new(next(&mut state.next_pipe));
        if !state.pipes.contains_key(&id) {
            return Ok(id);
        }
    }
    Err(Error::ENOFILES("every pipe id is in use".into()))
}

fn closed() -> Error {
    Error::ECLOSED("this socket is closed".into())
}

/// What a connection reveals about itself before it is a pipe.
struct Facts {
    local_addr: Option<String>,
    remote_addr: Option<String>,
    credentials: Option<weida_core::LocalPrincipal>,
    tls: Option<tls::TlsPeer>,
    source: Option<PeerKey>,
}

/// A connection the transport has established and the engine has not yet
/// turned into a pipe.
enum Raw {
    /// Everything the session needs is in hand: `tcp`, `ipc` and `inproc`.
    Ready(Stream),
    /// A TCP connection whose TLS handshake is still to run.
    ///
    /// Left to the engine because the handshake must run **after** the pipe
    /// slot is reserved and **under** the handshake deadline, and off the
    /// loop that accepts: a peer that connects and says nothing must hold
    /// nothing but its slot, for no longer than the deadline, and must not
    /// stop the next connection from being accepted.
    Tls {
        tcp: TcpStream,
        handshake: tls::Handshake,
        /// One of the listener's concurrent handshakes, released when this
        /// one is over.
        permit: Option<OwnedSemaphorePermit>,
    },
}

impl Raw {
    fn facts(&self) -> Facts {
        match self {
            Raw::Ready(stream) => Facts {
                local_addr: stream.local_addr(),
                remote_addr: stream.remote_addr(),
                credentials: stream.peer_credentials(),
                tls: stream.tls_peer(),
                source: stream.peer_key(),
            },
            Raw::Tls { tcp, .. } => {
                let peer = tcp.peer_addr().ok();
                Facts {
                    local_addr: tcp.local_addr().ok().map(|addr| addr.to_string()),
                    remote_addr: peer.map(|addr| addr.to_string()),
                    credentials: None,
                    tls: None,
                    source: peer.map(|addr| PeerKey::Host(weida_core::host_key(addr.ip()).key)),
                }
            }
        }
    }
}

/// Dials one endpoint, and keeps redialling it after each pipe closes.
async fn dialer_loop(
    ctx: TaskCtx,
    id: DialerId,
    endpoint: Endpoint,
    mut first: Option<oneshot::Sender<Result<()>>>,
    retry_first: bool,
) {
    let mut attempt = 0u32;
    // Built on the first TLS dial and kept for every redial: the
    // configuration does not change, and parsing it is not free.
    let mut tls: Option<tls::Connector> = None;
    loop {
        if ctx.engine.is_closed() {
            return;
        }
        let outcome = match connect(&ctx.engine, &endpoint, &mut tls).await {
            Ok(raw) => run_pipe(&ctx, raw, Role::Dial, &endpoint, Some(id), None, &mut first).await,
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => {
                // The pipe lived and then closed: "after a dialer pipe
                // closes, its dialer attempts reconnection" (§1), from the
                // shortest delay again.
                attempt = 0;
            }
            Err(error) => {
                if let Some(tx) = first.take() {
                    let _ = tx.send(Err(error));
                    if !retry_first {
                        // "A refused first connection is returned
                        // immediately and no retry is started" (§1).
                        return;
                    }
                } else {
                    tracing::debug!(%endpoint, %error, "SP dial failed; retrying");
                }
            }
        }
        // A closed socket redials nothing, and the task ends here, as soon as
        // the connection it was running has, rather than after one more
        // pause: the engine it holds is released with it.
        if ctx.engine.is_closed() {
            return;
        }
        let delay = ctx.engine.inner.options.reconnect_delay(attempt);
        attempt = attempt.saturating_add(1);
        ctx.engine.exec().sleep(delay).await;
    }
}

/// The first pause after an accept error that is not about one connection.
const ACCEPT_PAUSE_MIN: Duration = Duration::from_millis(10);

/// The longest an accept loop rests between tries.
const ACCEPT_PAUSE_MAX: Duration = Duration::from_secs(1);

/// How long an accept loop rests after an error, so that a failure that
/// does not clear — a process out of file descriptors — costs a sleep per
/// try and not a spin on a core.
///
/// An error about **one connection** — it was aborted or reset between the
/// kernel's accept and ours — is not a reason to rest: the next one is
/// already waiting. Anything else is: `EMFILE` and `ENFILE` leave the
/// pending connection queued, so an immediate retry fails the same way, as
/// do the memory and buffer errors, and an error nobody classified is
/// treated as the worst case rather than the best.
struct AcceptPause {
    delay: Duration,
}

impl AcceptPause {
    fn new() -> AcceptPause {
        AcceptPause {
            delay: ACCEPT_PAUSE_MIN,
        }
    }

    /// An accept succeeded: start from the shortest rest again.
    fn reset(&mut self) {
        self.delay = ACCEPT_PAUSE_MIN;
    }

    /// How long to rest after `error`, doubling up to [`ACCEPT_PAUSE_MAX`],
    /// or `None` when the next connection is the answer.
    fn after(&mut self, error: &Error) -> Option<Duration> {
        if matches!(
            error,
            Error::ECONNABORTED(_) | Error::ECONNRESET(_) | Error::EINTR(_)
        ) {
            return None;
        }
        let rest = self.delay;
        self.delay = self.delay.saturating_mul(2).min(ACCEPT_PAUSE_MAX);
        Some(rest)
    }
}

/// What an accept loop needs of a listening endpoint.
///
/// A trait only so that the loop's behaviour at a failing listener can be
/// driven without exhausting a process's file descriptors; the one
/// implementation that is not a test's is [`Bound`].
trait Listen: Send + 'static {
    /// The next connection.
    fn accept(&mut self) -> Pin<Box<dyn Future<Output = Result<Raw>> + Send + '_>>;

    /// Whether accepting will never succeed again, so the loop should stop
    /// rather than rest and retry. Only the in-process namespace can say
    /// so: a socket listener's accept error is about the moment, not about
    /// the listener.
    fn is_finished(&self) -> bool;
}

/// Accepts connections on one listening endpoint. There is no accept call in
/// SP's API: a listener accepts automatically (§1).
async fn accept_loop<L: Listen>(ctx: TaskCtx, mut listener: L, id: ListenerId, endpoint: Endpoint) {
    // The TLS handshakes this listener has in flight: each is a connection
    // that has proved nothing and already costs asymmetric cryptography.
    let handshakes = Arc::new(Semaphore::new(ctx.engine.inner.options.max_tls_handshakes));
    let mut pause = AcceptPause::new();
    loop {
        let mut raw = match listener.accept().await {
            Ok(raw) => {
                pause.reset();
                raw
            }
            Err(error) => {
                tracing::debug!(%endpoint, %error, "SP listener could not accept");
                if listener.is_finished() {
                    return;
                }
                if let Some(rest) = pause.after(&error) {
                    ctx.engine.exec().sleep(rest).await;
                }
                continue;
            }
        };
        if let Raw::Tls { permit, .. } = &mut raw {
            match Arc::clone(&handshakes).try_acquire_owned() {
                Ok(held) => *permit = Some(held),
                Err(_) => {
                    // Shed, unhandshaken: the connection is dropped, which
                    // is the only refusal SP has (§6), and the accept loop
                    // is back at `accept` at once.
                    tracing::debug!(%endpoint, "SP listener is at its TLS handshake ceiling");
                    continue;
                }
            }
        }
        let ctx = ctx.clone();
        let endpoint = endpoint.clone();
        ctx.engine.exec().clone().spawn(async move {
            let mut first = None;
            if let Err(error) = run_pipe(
                &ctx,
                raw,
                Role::Listen,
                &endpoint,
                None,
                Some(id),
                &mut first,
            )
            .await
            {
                tracing::debug!(%endpoint, %error, "SP connection ended before it was a pipe");
            }
        });
    }
}

/// The most octets one message may declare on a connection, given the
/// endpoint's `NNG_OPT_RECVMAXSZ`: the figure the transport closes the pipe
/// at.
///
/// **The tag stack rides on top of the limit.** A REQ, REP, SURVEYOR or
/// RESPONDENT message carries a tag stack in front of its payload, one tag
/// per forwarding hop and one for the request id, and `NNG_OPT_MAXTTL`
/// already bounds how long that stack may be. A device pushes one more tag
/// onto every request it forwards, so a limit that counted the stack would
/// admit a request at a device and then close the pipe that device shares
/// with every one of its clients, when the same request arrived at the
/// backend four octets longer than it left the client.
///
/// So the transport's figure leaves room for the longest stack — `4 ×
/// (MAXTTL + 1)` octets — and the limit itself is applied to the payload
/// once the stack is known ([`Pipe::admits_payload`]), where a message that
/// is over it is dropped with its pipe kept: the transport can only say
/// "this declaration is absurd", and the pipe belongs to somebody else when
/// a device forwarded the message.
fn wire_limit(recv_max_size: u64, protocol: EndpointType, max_ttl: usize) -> u64 {
    let carries_stack = matches!(
        protocol,
        EndpointType::Req | EndpointType::Rep | EndpointType::Surveyor | EndpointType::Respondent
    );
    if recv_max_size == RECV_MAX_SIZE_UNLIMITED || !carries_stack {
        return recv_max_size;
    }
    recv_max_size.saturating_add((weida_sp::backtrace::TAG_LEN as u64) * (max_ttl as u64 + 1))
}

/// Runs one connection from its first octet to its last.
///
/// Returns `Ok(())` once a pipe that was admitted has closed, and an error
/// when the connection never became a pipe — which is what a synchronous
/// dial reports and what a listener logs.
async fn run_pipe(
    ctx: &TaskCtx,
    raw: Raw,
    role: Role,
    endpoint: &Endpoint,
    dialer: Option<DialerId>,
    listener: Option<ListenerId>,
    first: &mut Option<oneshot::Sender<Result<()>>>,
) -> Result<()> {
    let engine = &ctx.engine;
    let options = &engine.inner.options;
    // One budget for what the peer has to do before it is a pipe: the TLS
    // handshake, if there is one, and then the protocol header.
    let started = Instant::now();
    let recv_limit = if endpoint.enforces_recv_max_size() {
        ctx.endpoint_options
            .recv_max_size
            .unwrap_or(options.recv_max_size)
    } else {
        // "`inproc` accepts but deliberately ignores `RECVMAXSZ`, because
        // peers share an address space" (§3). The option is taken and
        // honoured on every other transport rather than refused here,
        // which is NNG's own behaviour.
        RECV_MAX_SIZE_UNLIMITED
    };
    // The slot first, before anything that costs this side work.
    let (id, pipe) = engine.reserve(role, endpoint, dialer, listener, raw.facts(), recv_limit)?;
    let stream = match raw {
        Raw::Ready(stream) => stream,
        Raw::Tls {
            tcp,
            handshake,
            permit: _permit,
        } => {
            let handshake = engine
                .exec()
                .within(options.handshake_timeout, handshake.run(tcp))
                .await;
            match handshake {
                Some(Ok((stream, peer))) => {
                    engine.set_tls_peer(id, &peer);
                    Stream::tls(stream, peer)
                }
                Some(Err(error)) => {
                    engine.retire(id);
                    return Err(error);
                }
                None => {
                    engine.retire(id);
                    return Err(Error::ETIMEDOUT(
                        "the peer did not finish the TLS handshake in time".into(),
                    ));
                }
            }
        }
    };

    let (done, signal) = oneshot::channel();
    let connection = Connection {
        stream,
        pipe,
        role,
        endpoint: endpoint.clone(),
        protocol: engine.protocol(),
        recv_max_size: wire_limit(recv_limit, engine.protocol(), options.max_ttl),
        handshake: HandshakeGate { done: Some(done) },
        exec: engine.exec().clone(),
        message_timeout: options.message_timeout,
        idle_timeout: options.idle_timeout,
    };
    let mut session = engine.inner.session.start(connection);

    let deadline = engine
        .exec()
        .sleep(options.handshake_timeout.saturating_sub(started.elapsed()));
    let peer = {
        tokio::pin!(deadline);
        tokio::select! {
            ended = &mut session => {
                engine.retire(id);
                return match ended {
                    Ok(()) => Err(Error::ECONNRESET(
                        "the peer closed before its protocol header arrived".into(),
                    )),
                    Err(error) => Err(error),
                };
            }
            handshook = signal => match handshook {
                Ok(peer) => peer,
                Err(_) => {
                    engine.retire(id);
                    return Err(Error::EPROTO(
                        "the session ended the handshake without naming the peer".into(),
                    ));
                }
            },
            () = &mut deadline => {
                engine.retire(id);
                return Err(Error::ETIMEDOUT(
                    "the peer did not send its protocol header in time".into(),
                ));
            }
        }
    };

    if let Err(error) = engine.admit(id, peer) {
        engine.retire(id);
        return Err(error);
    }
    if let Some(tx) = first.take() {
        let _ = tx.send(Ok(()));
    }

    let outcome = session.await;
    engine.retire(id);
    match outcome {
        Ok(()) => Ok(()),
        Err(error) => {
            tracing::debug!(%endpoint, %error, "SP pipe ended");
            Ok(())
        }
    }
}

/// One TCP connection to `host:port`: the addresses it names, in order, each
/// attempt under the handshake deadline.
///
/// The deadline is per attempt, and the resolver's answer is one more wait
/// under it, because a peer that never answers a `SYN` is as much a stall
/// as one that never answers a `ClientHello`, and the kernel's own bound on
/// it is minutes.
async fn tcp_connect(engine: &Engine, host: &TcpHost, port: u16) -> Result<TcpStream> {
    let options = &engine.inner.options;
    let exec = &engine.inner.exec;
    let limit = options.handshake_timeout;
    let resolving = addresses(exec, host, port, options.max_addresses);
    let addresses = exec.within(limit, resolving).await.ok_or_else(|| {
        Error::ETIMEDOUT("resolving the host took longer than the handshake timeout".into())
    })??;
    let mut last = Error::EADDRINVAL("no address to dial".into());
    for addr in addresses {
        match exec.within(limit, TcpStream::connect(addr)).await {
            Some(Ok(stream)) => return Ok(stream),
            Some(Err(error)) => last = Error::from(error),
            None => {
                last = Error::ETIMEDOUT(
                    format!("connecting to {addr} took longer than the handshake timeout").into(),
                );
            }
        }
    }
    Err(last)
}

/// One connect attempt against one endpoint.
///
/// For `tls+tcp` this is the TCP connection only: the TLS handshake is run
/// by [`run_pipe`], under the same deadline as the protocol header.
async fn connect(
    engine: &Engine,
    endpoint: &Endpoint,
    connector: &mut Option<tls::Connector>,
) -> Result<Raw> {
    let options = &engine.inner.options;
    match endpoint {
        Endpoint::Tcp { host, port } => {
            let stream = tcp_connect(engine, host, *port).await?;
            Ok(Raw::Ready(Stream::tcp(stream)))
        }
        Endpoint::TlsTcp { host, port } => {
            let Some(config) = options.tls.as_ref() else {
                return Err(Error::EINVAL(
                    "a tls+tcp endpoint needs SocketOptions::tls; a TLS transport with no \
                     configuration is a TCP transport with a longer name"
                        .into(),
                ));
            };
            // The name the peer's certificate is checked against: the
            // option where one is set, otherwise the URL's host, which is
            // what NNG validates (§1). An address carries no name, so an
            // endpoint that has neither is refused rather than dialled
            // with nothing to verify.
            let name = match (config.server_name.as_deref(), host) {
                (Some(name), _) => name.to_owned(),
                (None, TcpHost::Name(name)) => name.clone(),
                (None, other) => {
                    return Err(Error::EADDRINVAL(
                        format!(
                            "tls+tcp://{other}:{port} names no host, so there is nothing for a \
                             certificate to be checked against; set \
                             NNG_OPT_TLS_SERVER_NAME (SocketOptions::tls.server_name)"
                        )
                        .into(),
                    ));
                }
            };
            if connector.is_none() {
                *connector = Some(tls::Connector::new(config)?);
            }
            let Some(connector) = connector.as_ref() else {
                return Err(Error::EINVAL("the TLS client side is not built".into()));
            };
            // Judged before dialling: a name no certificate can carry is
            // not worth a connection.
            let handshake = tls::Handshake::connect(connector, &name)?;
            let tcp = tcp_connect(engine, host, *port).await?;
            tune_tcp(&tcp);
            Ok(Raw::Tls {
                tcp,
                handshake,
                permit: None,
            })
        }
        Endpoint::Inproc(name) => {
            // NNG's inproc dialer retries rather than failing outright, so
            // a dial to a name nobody holds waits for the bind. The wait is
            // on a `Notify`, so a dial nobody ever answers costs nothing.
            engine.inner.inproc.wait_until_bound(name).await;
            engine
                .inner
                .inproc
                .dial(name, engine.inner.slot.id())
                .map(Raw::Ready)
                .ok_or_else(|| {
                    Error::ECONNREFUSED(
                        format!("inproc://{name} was unbound again before the dial landed").into(),
                    )
                })
        }
        #[cfg(unix)]
        Endpoint::Ipc(path) => {
            let stream = crate::ipc::dial(path).await?;
            // The kernel's answer is taken now, because now is when it is
            // about the process that connected.
            let principal = crate::ipc::credentials(&stream)?;
            Ok(Raw::Ready(Stream::unix(stream, principal)))
        }
        #[cfg(not(unix))]
        Endpoint::Ipc(_) => Err(unsupported(endpoint)),
    }
}

/// One listening endpoint, whichever transport it is on.
enum Bound {
    Tcp(TcpListener),
    Tls {
        listener: TcpListener,
        /// Built once, when the listener was bound: every connection that
        /// reaches it shares the parsed certificate chain and loaded key
        /// instead of building its own before it has proved anything.
        acceptor: tls::Acceptor,
    },
    Inproc(crate::inproc::InprocBinding),
    #[cfg(unix)]
    Ipc(crate::ipc::IpcBinding),
}

impl Bound {
    /// The next connection, with its credentials where the transport has
    /// any.
    async fn accept_one(&mut self) -> Result<Raw> {
        match self {
            Bound::Tcp(listener) => {
                let (stream, _) = listener.accept().await.map_err(Error::from)?;
                Ok(Raw::Ready(Stream::tcp(stream)))
            }
            Bound::Tls { listener, acceptor } => {
                let (tcp, _) = listener.accept().await.map_err(Error::from)?;
                tune_tcp(&tcp);
                Ok(Raw::Tls {
                    tcp,
                    handshake: tls::Handshake::accept(acceptor),
                    permit: None,
                })
            }
            Bound::Inproc(binding) => match binding.accept().await {
                Some(dial) => Ok(Raw::Ready(dial.stream)),
                None => Err(Error::ECLOSED("the inproc name was released".into())),
            },
            #[cfg(unix)]
            Bound::Ipc(binding) => {
                let (stream, principal) = binding.accept().await?;
                Ok(Raw::Ready(Stream::unix(stream, principal)))
            }
        }
    }

    /// The endpoint actually bound, which for a wildcard port is the only
    /// way to learn the port.
    fn bound_endpoint(&self, requested: &Endpoint) -> Result<Endpoint> {
        match (self, requested) {
            (Bound::Tcp(listener), Endpoint::Tcp { host, .. }) => {
                let local = listener.local_addr().map_err(Error::from)?;
                Ok(Endpoint::Tcp {
                    host: host.clone(),
                    port: local.port(),
                })
            }
            (Bound::Tls { listener, .. }, Endpoint::TlsTcp { host, .. }) => {
                let local = listener.local_addr().map_err(Error::from)?;
                Ok(Endpoint::TlsTcp {
                    host: host.clone(),
                    port: local.port(),
                })
            }
            _ => Ok(requested.clone()),
        }
    }
}

impl Listen for Bound {
    fn accept(&mut self) -> Pin<Box<dyn Future<Output = Result<Raw>> + Send + '_>> {
        Box::pin(self.accept_one())
    }

    fn is_finished(&self) -> bool {
        matches!(self, Bound::Inproc(_))
    }
}

/// Binds one endpoint.
async fn bind(engine: &Engine, endpoint: &Endpoint) -> Result<Bound> {
    match endpoint {
        Endpoint::Tcp { host, port } => {
            let addr = listen_addr(host, *port)?;
            TcpListener::bind(addr)
                .await
                .map(Bound::Tcp)
                .map_err(Error::from)
        }
        Endpoint::TlsTcp { host, port } => {
            let Some(config) = engine.inner.options.tls.as_ref() else {
                return Err(Error::EINVAL(
                    "a tls+tcp listener needs SocketOptions::tls with a certificate and key".into(),
                ));
            };
            // Refused here rather than at the first connection: a listener
            // that cannot present a certificate would accept connections
            // and then fail every one of them. Built here too, once, for
            // the same reason and so that no connection pays for it.
            let acceptor = tls::Acceptor::new(config)?;
            let addr = listen_addr(host, *port)?;
            let listener = TcpListener::bind(addr).await.map_err(Error::from)?;
            Ok(Bound::Tls { listener, acceptor })
        }
        Endpoint::Inproc(name) => engine.inner.inproc.bind(name).map(Bound::Inproc),
        #[cfg(unix)]
        Endpoint::Ipc(path) => crate::ipc::IpcBinding::bind(path).map(Bound::Ipc),
        #[cfg(not(unix))]
        Endpoint::Ipc(_) => Err(unsupported(endpoint)),
    }
}

/// The socket address a listener binds for `host:port`.
fn listen_addr(host: &TcpHost, port: u16) -> Result<SocketAddr> {
    Ok(match host {
        // A wildcard listens on every IPv4 interface, which is what
        // `tcp://*:port` means to NNG. An IPv6 wildcard is written
        // `tcp://[::]:port`.
        TcpHost::Any => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
        TcpHost::Ip(ip) => SocketAddr::new(*ip, port),
        TcpHost::Name(name) => {
            // A listener's host is an interface, and resolving one is the
            // resolver's job only for a literal; anything else would bind
            // an address this machine may not have.
            let ip: IpAddr = name.parse().map_err(|_| {
                Error::EADDRINVAL(
                    format!(
                        "cannot listen on {name:?}: a listening address must be an IP literal \
                         or *"
                    )
                    .into(),
                )
            })?;
            SocketAddr::new(ip, port)
        }
    })
}

async fn addresses(
    exec: &Exec,
    host: &TcpHost,
    port: u16,
    max_addresses: usize,
) -> Result<Vec<SocketAddr>> {
    Ok(match host {
        // Dialling the wildcard is dialling this host, which is what NNG's
        // own URL parser makes of it.
        TcpHost::Any => vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)],
        TcpHost::Ip(ip) => vec![SocketAddr::new(*ip, port)],
        TcpHost::Name(name) => exec.resolve(name, port, max_addresses).await?,
    })
}

/// A transport this build cannot carry, refused by name rather than
/// accepted and dropped somewhere quieter.
///
/// Only reachable where the platform lacks `AF_UNIX`: every transport
/// `Endpoint` parses is carried otherwise.
#[cfg(not(unix))]
fn unsupported(endpoint: &Endpoint) -> Error {
    Error::ENOTSUP(
        format!(
            "the {} transport is not carried by this engine",
            endpoint.transport()
        )
        .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::context::{Context, ContextConfig};

    /// A session that does the least a session can do: send the 8-octet
    /// protocol header, read the peer's, report it, then sit on the
    /// connection until its queues are closed.
    ///
    /// Enough to exercise every engine rule — the handshake gate, the
    /// ceiling, the pipe events, reconnect — and no more, because the SP
    /// session proper is `session.rs`'s and these tests must not anticipate it.
    struct HeaderOnly {
        handshakes: Arc<AtomicUsize>,
        answer: bool,
    }

    impl Session for HeaderOnly {
        fn start(&self, connection: Connection) -> SessionFuture {
            let handshakes = Arc::clone(&self.handshakes);
            let answer = self.answer;
            Box::pin(async move {
                let Connection {
                    mut stream,
                    pipe,
                    protocol,
                    mut handshake,
                    ..
                } = connection;
                stream
                    .write_all(&weida_sp::ProtocolHeader::new(protocol).encode())
                    .await?;
                stream.flush().await?;
                if !answer {
                    // A peer that connects and never speaks: the engine's
                    // handshake deadline is the only thing that ends it.
                    std::future::pending::<()>().await;
                }
                let mut theirs = [0u8; weida_sp::HEADER_LEN];
                stream.read_exact(&mut theirs).await?;
                let peer = weida_sp::ProtocolHeader::decode(&theirs)?;
                handshakes.fetch_add(1, Ordering::SeqCst);
                handshake.complete(peer.endpoint);
                // Stay until the pipe is destroyed under us.
                let _ = pipe.incoming().recv().await;
                Ok(())
            })
        }
    }

    fn engine(
        context: &Context,
        protocol: EndpointType,
        options: SocketOptions,
        handshakes: &Arc<AtomicUsize>,
    ) -> Engine {
        Engine::new(
            context,
            protocol,
            options,
            Arc::new(HeaderOnly {
                handshakes: Arc::clone(handshakes),
                answer: true,
            }),
        )
        .expect("engine")
    }

    fn counter() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(0))
    }

    /// Claim: a listener accepts automatically, a dial creates one pipe per
    /// connection on both sides, and the endpoint objects carry the URL
    /// actually used — including the port behind a wildcard.
    #[tokio::test]
    async fn a_dial_and_a_listen_make_one_pipe_each() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let client = engine(&ctx, EndpointType::Req, SocketOptions::default(), &seen);

        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");
        assert!(!listener.url().has_wildcard_port(), "the port is knowable");

        let dialer = client.dial(listener.url()).await.expect("dial");
        assert_eq!(dialer.url(), listener.url());

        assert_eq!(client.pipes().len(), 1);
        let infos = client.pipe_infos();
        assert_eq!(infos[0].role, Role::Dial);
        assert_eq!(infos[0].peer, Some(EndpointType::Rep));
        assert!(infos[0].remote_addr.is_some());

        // The server side is admitted too, once its own handshake finishes.
        for _ in 0..100 {
            if server.pipes().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(server.pipes().len(), 1);
        assert_eq!(server.pipe_infos()[0].role, Role::Listen);
    }

    /// Claim: the synchronous dial returns only once the peer's protocol
    /// header has arrived — not when the TCP connection is established.
    /// A listener that accepts and says nothing is the difference.
    #[tokio::test]
    async fn a_synchronous_dial_waits_for_the_peers_header() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a raw listener");
        let port = silent.local_addr().unwrap().port();
        let held = tokio::spawn(async move {
            // Accept and never answer, which is what a peer stuck in its own
            // dial looks like.
            let (stream, _) = silent.accept().await.expect("accept");
            std::future::pending::<()>().await;
            drop(stream);
        });

        let seen = counter();
        let client = engine(
            &ctx,
            EndpointType::Req,
            SocketOptions {
                handshake_timeout: Duration::from_millis(200),
                ..SocketOptions::default()
            },
            &seen,
        );
        let url = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).unwrap();
        let error = client.dial(&url).await.unwrap_err();
        assert!(matches!(error, Error::ETIMEDOUT(_)), "{error:?}");
        assert_eq!(seen.load(Ordering::SeqCst), 0, "no handshake completed");
        assert!(client.pipes().is_empty(), "no pipe without a handshake");
        held.abort();
    }

    /// Claim: a refused first synchronous dial is reported and starts no
    /// retry — NNG's own rule — while the non-blocking form returns at once
    /// and keeps trying until the peer appears.
    #[tokio::test]
    async fn the_two_dial_forms_differ_in_exactly_one_thing() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let client = engine(
            &ctx,
            EndpointType::Req,
            SocketOptions {
                reconnect_min: Duration::from_millis(5),
                ..SocketOptions::default()
            },
            &seen,
        );

        // A port nobody is on. Bound and dropped, so it is free and refusing.
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let port = dead.local_addr().unwrap().port();
        drop(dead);
        let url = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).unwrap();

        let error = client.dial(&url).await.unwrap_err();
        assert!(
            matches!(error, Error::ECONNREFUSED(_) | Error::EUNREACHABLE(_)),
            "{error:?}"
        );
        assert!(
            client.lock().dialers.is_empty(),
            "a refused synchronous dial leaves no dialer behind"
        );

        // The non-blocking form returns before anything has connected, and
        // the pipe appears once somebody listens on that address.
        let dialer = client.dial_nonblocking(&url).expect("nonblocking dial");
        assert!(client.pipes().is_empty());

        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        server
            .listen(&Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).unwrap())
            .await
            .expect("listen");
        for _ in 0..200 {
            if client.pipes().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(client.pipes().len(), 1, "the retry found the peer");
        dialer.close();
    }

    /// Claim: a dialer redials after its pipe closes, which is the rule that
    /// applies to both dial forms once a pipe has existed.
    #[tokio::test]
    async fn a_dialer_redials_after_its_pipe_closes() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let client = engine(
            &ctx,
            EndpointType::Req,
            SocketOptions {
                reconnect_min: Duration::from_millis(5),
                ..SocketOptions::default()
            },
            &seen,
        );
        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");
        client.dial(listener.url()).await.expect("dial");
        let first = client.pipe_infos()[0].id;

        client.close_pipe(first);
        for _ in 0..200 {
            let pipes = client.pipe_infos();
            if pipes.len() == 1 && pipes[0].id != first {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the dialer did not redial");
    }

    /// Claim: all three pipe events reach the callback in order, and the
    /// callback's refusal at `ADD_PRE` keeps the pipe out of the socket —
    /// which is the hook local authorization runs in (§10).
    #[tokio::test]
    async fn pipe_events_reach_the_callback_and_add_pre_can_refuse() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let client = engine(&ctx, EndpointType::Req, SocketOptions::default(), &seen);

        let events: Arc<Mutex<Vec<PipeEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&events);
        client.notify(Arc::new(move |event, info: &PipeInfo| {
            recorded.lock().unwrap().push(event);
            assert_eq!(info.role, Role::Dial);
            Admission::Accept
        }));

        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");
        client.dial(listener.url()).await.expect("dial");
        assert_eq!(
            *events.lock().unwrap(),
            vec![PipeEvent::AddPre, PipeEvent::AddPost]
        );

        let id = client.pipe_infos()[0].id;
        client.close_pipe(id);
        for _ in 0..200 {
            if events.lock().unwrap().contains(&PipeEvent::RemPost) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            events.lock().unwrap().first(),
            Some(&PipeEvent::AddPre),
            "the order is add-pre, add-post, remove-post"
        );
        assert!(events.lock().unwrap().contains(&PipeEvent::RemPost));

        // A refusal keeps the pipe out, and the dial reports it.
        let refusing = engine(&ctx, EndpointType::Req, SocketOptions::default(), &seen);
        refusing.notify(Arc::new(|event, _: &PipeInfo| match event {
            PipeEvent::AddPre => Admission::Reject("not on the allow-list".into()),
            _ => Admission::Accept,
        }));
        let error = refusing.dial(listener.url()).await.unwrap_err();
        assert!(matches!(error, Error::EPEERAUTH(_)), "{error:?}");
        assert!(refusing.pipes().is_empty());
    }

    /// Claim: the pipe ceiling is real and is reported as `NNG_ENOFILES`,
    /// and it counts a connection that has not handshaken — which is the
    /// only kind an attacker opens.
    #[tokio::test]
    async fn the_pipe_ceiling_bounds_what_a_stranger_can_open() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");

        let client = engine(
            &ctx,
            EndpointType::Req,
            SocketOptions {
                max_pipes: 1,
                ..SocketOptions::default()
            },
            &seen,
        );
        client.dial(listener.url()).await.expect("the first pipe");
        let error = client.dial(listener.url()).await.unwrap_err();
        assert!(matches!(error, Error::ENOFILES(_)), "{error:?}");
        assert!(error.cause().contains('1'));
    }

    /// Claim: closing a dialer closes the pipe it made, and closing the
    /// socket closes everything — a dialer that kept redialling after
    /// `nng_close()` would be a socket that cannot be shut down.
    #[tokio::test]
    async fn closing_an_endpoint_closes_what_it_created() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let client = engine(&ctx, EndpointType::Req, SocketOptions::default(), &seen);
        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");

        let dialer = client.dial(listener.url()).await.expect("dial");
        dialer.close();
        for _ in 0..200 {
            if client.pipes().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            client.pipes().is_empty(),
            "the dialer took its pipe with it"
        );

        server.close();
        assert!(server.is_closed());
        assert!(server.pipes().is_empty());
        let error = server.listen(listener.url()).await.unwrap_err();
        assert!(matches!(error, Error::ECLOSED(_)), "{error:?}");
    }

    /// Claim: every transport an [`Endpoint`] parses is carried, and a
    /// `tls+tcp` endpoint with no TLS configuration is refused **where it
    /// is configured** rather than accepted and failed at the first
    /// connection — a TLS transport with no configuration is a TCP
    /// transport with a longer name.
    #[tokio::test]
    async fn a_tls_endpoint_without_a_configuration_is_refused() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let socket = engine(&ctx, EndpointType::Bus, SocketOptions::default(), &seen);
        let error = socket
            .listen(&Endpoint::parse("tls+tcp://127.0.0.1:0").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::EINVAL(_)), "{error:?}");
        assert!(error.cause().contains("certificate"));

        // And the transports that need no configuration are not refused.
        assert!(
            socket
                .listen(&Endpoint::parse("inproc://orders").unwrap())
                .await
                .is_ok()
        );
        assert!(
            socket
                .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
                .await
                .is_ok()
        );
    }

    /// A connection's facts when there is nothing to say about it.
    fn no_facts() -> Facts {
        Facts {
            local_addr: None,
            remote_addr: None,
            credentials: None,
            tls: None,
            source: None,
        }
    }

    /// Claim: a pipe id that is in use is never handed out again, however
    /// far the counter has run. The id is 31 bits wide, so after enough
    /// connections the counter revisits the ids of pipes that are still
    /// open; an id taken twice would make the table forget a live pipe
    /// while its session kept running.
    #[tokio::test]
    async fn a_pipe_id_that_is_in_use_is_never_handed_out_again() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let engine = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let endpoint = Endpoint::parse("inproc://ids").unwrap();

        let (first, _first_pipe) = engine
            .reserve(Role::Listen, &endpoint, None, None, no_facts(), 0)
            .expect("the first pipe");
        // The counter is about to wrap past 2^31 and come round to the id
        // of the pipe that is still open.
        engine.lock().next_pipe = PipeId::MAX;
        let mut held = Vec::new();
        let mut ids = vec![first];
        for _ in 0..4 {
            let (id, pipe) = engine
                .reserve(Role::Listen, &endpoint, None, None, no_facts(), 0)
                .expect("a free id");
            ids.push(id);
            held.push(pipe);
        }
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 5, "an id was handed out twice: {ids:?}");
        assert_eq!(
            engine.lock().pipes.len(),
            5,
            "a live pipe was replaced in the table"
        );
    }

    /// A listener whose accept fails with `error` `failures` times, then
    /// waits for ever, counting every call.
    struct Failing {
        calls: Arc<AtomicUsize>,
        error: fn() -> Error,
        failures: usize,
    }

    impl Listen for Failing {
        fn accept(&mut self) -> Pin<Box<dyn Future<Output = Result<Raw>> + Send + '_>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.failures == 0 {
                return Box::pin(std::future::pending());
            }
            self.failures -= 1;
            let error = (self.error)();
            Box::pin(async move { Err(error) })
        }

        fn is_finished(&self) -> bool {
            false
        }
    }

    /// Runs an accept loop over a [`Failing`] listener for `window` and
    /// reports how many times it called `accept`.
    async fn accept_calls(error: fn() -> Error, failures: usize, window: Duration) -> usize {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let engine = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let calls = Arc::new(AtomicUsize::new(0));
        let task = ctx.exec().spawn(accept_loop(
            TaskCtx {
                engine,
                endpoint_options: EndpointOptions::default(),
            },
            Failing {
                calls: Arc::clone(&calls),
                error,
                failures,
            },
            ListenerId(1),
            Endpoint::parse("tcp://127.0.0.1:1").unwrap(),
        ));
        tokio::time::sleep(window).await;
        task.abort();
        calls.load(Ordering::SeqCst)
    }

    /// Claim: a listener whose accept keeps failing for a reason that does
    /// not clear — a process out of file descriptors leaves the pending
    /// connection queued, so an immediate retry fails the same way — rests
    /// between tries instead of spinning on a core.
    #[tokio::test]
    async fn a_listener_that_cannot_accept_rests_between_tries() {
        let calls = accept_calls(
            || Error::ESYSERR("too many open files".into()),
            usize::MAX,
            Duration::from_millis(300),
        )
        .await;
        assert!(
            calls <= 20,
            "{calls} accepts in 300 ms: the loop is not resting"
        );
        assert!(calls >= 2, "the loop retries after resting: {calls}");
    }

    /// Claim: an error about one connection — aborted or reset between the
    /// kernel's accept and ours — is not a reason to rest: the next
    /// connection is already waiting.
    #[tokio::test]
    async fn an_error_about_one_connection_does_not_slow_the_next() {
        let calls = accept_calls(
            || Error::ECONNABORTED("aborted".into()),
            50,
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(calls, 51, "fifty failures and then the call that waits");
    }

    /// Claim: one peer address cannot hold more pipes than its ceiling,
    /// handshaking connections included, and another address is not
    /// affected by it. Without the ceiling one host opens `max_pipes`
    /// connections, completes the 8-octet handshake on each and holds them,
    /// and every later peer is refused until it stops.
    #[tokio::test]
    async fn one_peer_address_cannot_hold_more_pipes_than_its_ceiling() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(
            &ctx,
            EndpointType::Rep,
            SocketOptions {
                max_pipes_per_peer: 2,
                handshake_timeout: Duration::from_secs(5),
                ..SocketOptions::default()
            },
            &seen,
        );
        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");
        let addr = listener
            .url()
            .to_string()
            .trim_start_matches("tcp://")
            .to_owned();

        let mut held = Vec::new();
        for _ in 0..2 {
            held.push(
                tokio::net::TcpStream::connect(&addr)
                    .await
                    .expect("connect"),
            );
        }
        for _ in 0..200 {
            if server.lock().pipes.len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            server.lock().pipes.len(),
            2,
            "two silent peers hold two slots"
        );

        let mut third = tokio::net::TcpStream::connect(&addr)
            .await
            .expect("connect");
        let mut buffer = [0u8; 16];
        let read = tokio::time::timeout(Duration::from_secs(2), third.read(&mut buffer))
            .await
            .expect("the third connection from this host is closed rather than held")
            .expect("read");
        assert_eq!(read, 0, "nothing is sent to a refused connection");
        assert_eq!(server.lock().pipes.len(), 2);

        // The whole of 127.0.0.0/8 is this host on Linux, which makes a
        // second source address available without any setup.
        #[cfg(target_os = "linux")]
        {
            let socket = tokio::net::TcpSocket::new_v4().expect("socket");
            socket
                .bind("127.0.0.2:0".parse().unwrap())
                .expect("a second source address");
            let _other = tokio::time::timeout(
                Duration::from_secs(2),
                socket.connect(addr.parse().unwrap()),
            )
            .await
            .expect("connect")
            .expect("connected");
            for _ in 0..200 {
                if server.lock().pipes.len() == 3 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(
                server.lock().pipes.len(),
                3,
                "another address is not held to the first one's ceiling"
            );
        }
    }

    /// Claim: a wait for the pipe set to change sees a change that happens
    /// before the wait is first polled, because it exists from the moment
    /// it is created. A caller that creates it before looking at the pipes
    /// cannot miss the pipe that arrives in between, which is what an
    /// `async fn` — created at first poll — could.
    #[tokio::test]
    async fn a_wait_made_before_the_pipe_set_changes_ends_with_the_change() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let client = engine(&ctx, EndpointType::Req, SocketOptions::default(), &seen);
        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");

        let wait = client.changed();
        client.dial(listener.url()).await.expect("dial");
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("the wait saw the pipe arrive although it was first polled afterwards");
    }

    /// Claim: what a retired pipe left in the socket's buffer takes its turn
    /// among the live pipes and is not served ahead of them. A peer that
    /// connects, sends and goes as fast as the application receives would
    /// otherwise keep that buffer full for ever, and a pipe that stays would
    /// never be read.
    #[tokio::test]
    async fn what_a_retired_pipe_left_behind_takes_its_turn_among_the_live_pipes() {
        async fn bounded<T>(step: impl Future<Output = T>) -> T {
            tokio::time::timeout(Duration::from_secs(10), step)
                .await
                .expect("the step finished within 10 s")
        }

        let ctx = Context::new(ContextConfig::default()).expect("context");
        let core =
            crate::socket::SocketCore::new(&ctx, EndpointType::Pull, SocketOptions::default())
                .expect("pull");
        let listener = bounded(core.listen("tcp://127.0.0.1:0"))
            .await
            .expect("listen");
        let addr = listener.url().to_string();

        // One peer that stays, with three messages waiting in its pipe.
        let mut peer = bounded(tokio::net::TcpStream::connect(
            addr.trim_start_matches("tcp://"),
        ))
        .await
        .expect("connect");
        bounded(peer.write_all(&weida_sp::ProtocolHeader::new(EndpointType::Push).encode()))
            .await
            .expect("our header");
        let mut theirs = [0u8; weida_sp::HEADER_LEN];
        bounded(peer.read_exact(&mut theirs))
            .await
            .expect("their header");
        for n in 0..3 {
            let message = weida_sp::message::encode(format!("live {n}").as_bytes());
            bounded(peer.write_all(&message)).await.expect("a message");
        }
        bounded(async {
            while !core
                .pipes()
                .first()
                .is_some_and(|pipe| pipe.incoming().len() == 3)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;

        // And two messages from a pipe that has gone.
        core.engine().store_arrived(
            PipeId::new(500),
            vec![
                Message::from_body(b"gone 0".to_vec()),
                Message::from_body(b"gone 1".to_vec()),
            ],
        );

        let mut order = Vec::new();
        while let Some((_, message)) = core.try_take_any() {
            order.push(String::from_utf8(message.body().to_vec()).expect("utf8"));
        }
        assert_eq!(order, ["live 0", "gone 0", "live 1", "gone 1", "live 2"]);
    }
}
