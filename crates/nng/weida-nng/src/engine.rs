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
//! What this module does **not** do is speak SP. A connection is handed to a
//! [`Session`], and the 8-octet header, the pairing check and the 64-bit
//! framing are that session's
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §5.2).

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use weida_runtime::Exec;
use weida_sp::EndpointType;

use crate::context::{Context, SocketSlot};
use crate::endpoint::{Endpoint, TcpHost};
use crate::error::{Cause, Error, Result};
use crate::message::Message;
use crate::options::{EndpointOptions, SocketOptions};
use crate::pipe::{Discarded, Pipe, PipeId};
use crate::transport::Stream;

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
    /// `NNG_OPT_RECVMAXSZ` for this connection.
    pub recv_max_size: u64,
    /// Signalled once the peer's protocol header has been read and accepted.
    pub handshake: HandshakeGate,
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
}

/// One live dialer or listener: the task that runs it, which is what
/// closing one aborts. The URL lives on the [`Dialer`] or [`Listener`] the
/// application holds, so it is not kept twice.
struct EndpointEntry {
    task: JoinHandle<()>,
}

struct EngineState {
    pipes: HashMap<PipeId, PipeEntry>,
    dialers: HashMap<DialerId, EndpointEntry>,
    listeners: HashMap<ListenerId, EndpointEntry>,
    next_pipe: u32,
    next_endpoint: u32,
    closed: bool,
}

struct EngineInner {
    exec: Exec,
    protocol: EndpointType,
    options: SocketOptions,
    session: Arc<dyn Session>,
    notify: Mutex<Option<PipeCallback>>,
    state: Mutex<EngineState>,
    /// Woken whenever the admitted pipe set changes, so a socket waiting
    /// for a peer does not poll.
    changed: Notify,
    /// Messages that arrived on a pipe which has since been retired.
    ///
    /// The socket's own receive buffer, in miniature and only for the one
    /// case that needs it: a peer that answers and then closes. Bounded by
    /// [`Engine::store_arrived`], which is where the ceiling and the
    /// full-queue policy are, because the pipe it came from is gone by
    /// then and its bound went with it.
    leftovers: Mutex<std::collections::VecDeque<(PipeId, Message)>>,
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
                    dialers: HashMap::new(),
                    listeners: HashMap::new(),
                    next_pipe: 1,
                    next_endpoint: 1,
                    closed: false,
                }),
                changed: Notify::new(),
                leftovers: Mutex::new(std::collections::VecDeque::new()),
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

    /// Every pipe the socket may use right now.
    pub fn pipes(&self) -> Vec<Pipe> {
        self.lock()
            .pipes
            .values()
            .filter(|entry| entry.admitted)
            .map(|entry| entry.pipe.clone())
            .collect()
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

    /// Waits until the admitted pipe set changes.
    ///
    /// A socket with nothing to send to waits on this rather than polling:
    /// "with no eligible peer, the send waits or times out" (§4), and this
    /// is the wait.
    pub async fn changed(&self) {
        let notified = self.inner.changed.notified();
        notified.await;
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
    /// [`SocketOptions::handshake_timeout`](crate::SocketOptions::handshake_timeout).
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
        let owned: Vec<PipeId> = self
            .lock()
            .pipes
            .values()
            .filter(|entry| entry.dialer == Some(id))
            .map(|entry| entry.info.id)
            .collect();
        // Retired here rather than left to the task that was driving them:
        // aborting a dialer stops the very loop that would have reported
        // the removal, and a pipe nobody retires is a pipe that stays in
        // the socket's table for ever.
        for pipe in owned {
            self.retire(pipe);
        }
    }

    /// `nng_listener_close()`.
    pub fn close_listener(&self, id: ListenerId) {
        let entry = self.lock().listeners.remove(&id);
        if let Some(entry) = entry {
            entry.task.abort();
        }
        let owned: Vec<PipeId> = self
            .lock()
            .pipes
            .values()
            .filter(|entry| entry.listener == Some(id))
            .map(|entry| entry.info.id)
            .collect();
        for pipe in owned {
            self.retire(pipe);
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

    /// `nng_close()`: stop dialling and accepting, and destroy every pipe.
    pub fn close(&self) {
        let (dialers, listeners, pipes) = {
            let mut state = self.lock();
            state.closed = true;
            (
                state.dialers.drain().collect::<Vec<_>>(),
                state.listeners.drain().collect::<Vec<_>>(),
                state.pipes.keys().copied().collect::<Vec<PipeId>>(),
            )
        };
        for (_, entry) in dialers {
            entry.task.abort();
        }
        for (_, entry) in listeners {
            entry.task.abort();
        }
        for pipe in pipes {
            self.retire(pipe);
        }
        self.inner.changed.notify_waiters();
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

    /// Reserves a pipe slot against the socket's ceiling.
    fn reserve(
        &self,
        role: Role,
        endpoint: &Endpoint,
        dialer: Option<DialerId>,
        listener: Option<ListenerId>,
        stream: &Stream,
    ) -> Result<(PipeId, Pipe)> {
        let mut state = self.lock();
        if state.closed {
            return Err(closed());
        }
        let ceiling = self.inner.options.max_pipes;
        if state.pipes.len() >= ceiling {
            return Err(Error::ENOFILES(
                format!("this socket already holds its ceiling of {ceiling} pipes").into(),
            ));
        }
        let id = PipeId::new(next(&mut state.next_pipe));
        let pipe = Pipe::new(id, self.inner.options.pipe_config(self.inner.protocol));
        state.pipes.insert(
            id,
            PipeEntry {
                pipe: pipe.clone(),
                info: PipeInfo {
                    id,
                    role,
                    endpoint: endpoint.clone(),
                    peer: None,
                    local_addr: stream.local_addr(),
                    remote_addr: stream.remote_addr(),
                    credentials: stream.peer_credentials(),
                    tls: stream.tls_peer(),
                },
                admitted: false,
                dialer,
                listener,
            },
        );
        Ok((id, pipe))
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
        }
        self.fire(PipeEvent::AddPost, &info);
        self.inner.changed.notify_waiters();
        Ok(())
    }

    /// Removes a pipe and fires `REM_POST`, whether it was ever admitted or
    /// not.
    fn retire(&self, id: PipeId) {
        let entry = self.lock().pipes.remove(&id);
        if let Some(entry) = entry {
            // What already arrived is not lost with the pipe. NNG keeps
            // received messages in the socket's own receive buffer rather
            // than in the pipe, so a peer that replies and then closes —
            // which is what a REP socket answering its last request does —
            // has its answer delivered. Only what was still queued *for*
            // the peer goes, because that never left (§1).
            let mut arrived = Vec::new();
            while let Ok(message) = entry.pipe.incoming().try_recv() {
                arrived.push(message);
            }
            entry.pipe.close();
            self.store_arrived(id, arrived);
            if entry.admitted {
                self.fire(PipeEvent::RemPost, &entry.info);
                self.inner.changed.notify_waiters();
            }
        }
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
    /// The bound is [`PipeConfig::incoming`]'s depth — one pipe's worth of
    /// messages, not one per pipe — because this is the socket's receive
    /// buffer and not a per-pipe queue, and at least one, because holding
    /// the last answer is the whole reason the buffer exists.
    ///
    /// Full, and the policy is the socket's rather than the pipe's:
    /// [`FullAction::receiving`] is `Block` for every protocol and blocking
    /// is not available here — the pipe is gone and no reader can push back
    /// on a peer that has left. So the choice is the one `SUB_PREFNEW`
    /// makes at this same layer: the oldest goes for a SUB that prefers new
    /// publications, and for everyone else the **newest is refused**,
    /// because the oldest is the answer somebody is already waiting for.
    fn store_arrived(&self, id: PipeId, arrived: Vec<Message>) {
        if arrived.is_empty() {
            return;
        }
        let options = &self.inner.options;
        let ceiling = options
            .pipe_config(self.inner.protocol)
            .incoming
            .depth
            .max(1);
        let prefer_new = self.inner.protocol == EndpointType::Sub && options.sub_prefer_new;
        let mut refused = 0usize;
        let mut leftovers = self.inner.leftovers.lock().expect("leftovers poisoned");
        for message in arrived {
            if leftovers.len() >= ceiling {
                if prefer_new {
                    leftovers.pop_front();
                } else {
                    refused += 1;
                    continue;
                }
            }
            leftovers.push_back((id, message));
        }
        if refused > 0 {
            tracing::debug!(
                refused,
                ceiling,
                "the socket's buffer is full of messages from pipes that have gone"
            );
        }
    }

    /// The next message that arrived on a pipe which has since been
    /// retired, if any.
    ///
    /// A socket takes from here before it looks at its pipes: a message
    /// that crossed the wire has arrived, and the peer closing afterwards
    /// does not un-arrive it.
    pub fn take_arrived(&self) -> Option<(PipeId, Message)> {
        self.inner
            .leftovers
            .lock()
            .expect("leftovers poisoned")
            .pop_front()
    }

    /// Whether any message is waiting from a pipe that has been retired.
    pub fn has_arrived(&self) -> bool {
        !self
            .inner
            .leftovers
            .lock()
            .expect("leftovers poisoned")
            .is_empty()
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

fn closed() -> Error {
    Error::ECLOSED("this socket is closed".into())
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
    loop {
        if ctx.engine.is_closed() {
            return;
        }
        let outcome = match connect(&ctx.engine, &endpoint).await {
            Ok(stream) => {
                run_pipe(
                    &ctx,
                    stream,
                    Role::Dial,
                    &endpoint,
                    Some(id),
                    None,
                    &mut first,
                )
                .await
            }
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
        let delay = ctx.engine.inner.options.reconnect_delay(attempt);
        attempt = attempt.saturating_add(1);
        ctx.engine.exec().sleep(delay).await;
    }
}

/// Accepts connections on one listening endpoint. There is no accept call in
/// SP's API: a listener accepts automatically (§1).
async fn accept_loop(ctx: TaskCtx, listener: Bound, id: ListenerId, endpoint: Endpoint) {
    let mut listener = listener;
    loop {
        let accepted = match listener.accept().await {
            Ok(stream) => stream,
            Err(error) => {
                tracing::debug!(%endpoint, %error, "SP listener could not accept");
                if listener.is_finished() {
                    return;
                }
                continue;
            }
        };
        let ctx = ctx.clone();
        let endpoint = endpoint.clone();
        ctx.engine.exec().clone().spawn(async move {
            let mut first = None;
            if let Err(error) = run_pipe(
                &ctx,
                accepted,
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

/// Runs one connection from its first octet to its last.
///
/// Returns `Ok(())` once a pipe that was admitted has closed, and an error
/// when the connection never became a pipe — which is what a synchronous
/// dial reports and what a listener logs.
async fn run_pipe(
    ctx: &TaskCtx,
    stream: Stream,
    role: Role,
    endpoint: &Endpoint,
    dialer: Option<DialerId>,
    listener: Option<ListenerId>,
    first: &mut Option<oneshot::Sender<Result<()>>>,
) -> Result<()> {
    let (id, pipe) = ctx
        .engine
        .reserve(role, endpoint, dialer, listener, &stream)?;
    let (done, signal) = oneshot::channel();
    let connection = Connection {
        stream,
        pipe,
        role,
        endpoint: endpoint.clone(),
        protocol: ctx.engine.protocol(),
        recv_max_size: if endpoint.enforces_recv_max_size() {
            ctx.endpoint_options
                .recv_max_size
                .unwrap_or(ctx.engine.inner.options.recv_max_size)
        } else {
            // "`inproc` accepts but deliberately ignores `RECVMAXSZ`,
            // because peers share an address space" (§3). The option is
            // taken and honoured on every other transport rather than
            // refused here, which is NNG's own behaviour.
            crate::message::RECV_MAX_SIZE_UNLIMITED
        },
        handshake: HandshakeGate { done: Some(done) },
    };
    let mut session = ctx.engine.inner.session.start(connection);

    let deadline = ctx
        .engine
        .exec()
        .sleep(ctx.engine.inner.options.handshake_timeout);
    let peer = {
        tokio::pin!(deadline);
        tokio::select! {
            ended = &mut session => {
                ctx.engine.retire(id);
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
                    ctx.engine.retire(id);
                    return Err(Error::EPROTO(
                        "the session ended the handshake without naming the peer".into(),
                    ));
                }
            },
            () = &mut deadline => {
                ctx.engine.retire(id);
                return Err(Error::ETIMEDOUT(
                    "the peer did not send its protocol header in time".into(),
                ));
            }
        }
    };

    if let Err(error) = ctx.engine.admit(id, peer) {
        ctx.engine.retire(id);
        return Err(error);
    }
    if let Some(tx) = first.take() {
        let _ = tx.send(Ok(()));
    }

    let outcome = session.await;
    ctx.engine.retire(id);
    match outcome {
        Ok(()) => Ok(()),
        Err(error) => {
            tracing::debug!(%endpoint, %error, "SP pipe ended");
            Ok(())
        }
    }
}

/// One connect attempt against one endpoint.
async fn connect(engine: &Engine, endpoint: &Endpoint) -> Result<Stream> {
    let options = &engine.inner.options;
    match endpoint {
        Endpoint::Tcp { host, port } => {
            let mut last = Error::EADDRINVAL("no address to dial".into());
            for addr in addresses(&engine.inner.exec, host, *port, options.max_addresses).await? {
                match TcpStream::connect(addr).await {
                    Ok(stream) => return Ok(Stream::tcp(stream)),
                    Err(error) => last = Error::from(error),
                }
            }
            Err(last)
        }
        Endpoint::TlsTcp { host, port } => {
            let Some(tls) = options.tls.as_ref() else {
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
            let name = match (tls.server_name.as_deref(), host) {
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
            let mut last = Error::EADDRINVAL("no address to dial".into());
            for addr in addresses(&engine.inner.exec, host, *port, options.max_addresses).await? {
                match TcpStream::connect(addr).await {
                    Ok(stream) => {
                        let (tls_stream, peer) = crate::tls::connect(tls, &name, stream).await?;
                        return Ok(Stream::tls(tls_stream, peer));
                    }
                    Err(error) => last = Error::from(error),
                }
            }
            Err(last)
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
            Ok(Stream::unix(stream, principal))
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
        config: Box<crate::tls::TlsConfig>,
    },
    Inproc(crate::inproc::InprocBinding),
    #[cfg(unix)]
    Ipc(crate::ipc::IpcBinding),
}

impl Bound {
    /// The next connection, with its credentials where the transport has
    /// any.
    async fn accept(&mut self) -> Result<Stream> {
        match self {
            Bound::Tcp(listener) => {
                let (stream, _) = listener.accept().await.map_err(Error::from)?;
                Ok(Stream::tcp(stream))
            }
            Bound::Tls { listener, config } => {
                let (stream, _) = listener.accept().await.map_err(Error::from)?;
                let (tls, peer) = crate::tls::accept(config, stream).await?;
                Ok(Stream::tls(tls, peer))
            }
            Bound::Inproc(binding) => match binding.accept().await {
                Some(dial) => Ok(dial.stream),
                None => Err(Error::ECLOSED("the inproc name was released".into())),
            },
            #[cfg(unix)]
            Bound::Ipc(binding) => {
                let (stream, principal) = binding.accept().await?;
                Ok(Stream::unix(stream, principal))
            }
        }
    }

    /// Whether accepting will never succeed again, so the loop should stop
    /// rather than spin. Only the in-process namespace can say so: a
    /// socket listener's accept error is per-connection.
    const fn is_finished(&self) -> bool {
        matches!(self, Bound::Inproc(_))
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

/// Binds one endpoint.
async fn bind(engine: &Engine, endpoint: &Endpoint) -> Result<Bound> {
    match endpoint {
        Endpoint::Tcp { host, port } => {
            let addr = match host {
                // A wildcard listens on every IPv4 interface, which is what
                // `tcp://*:port` means to NNG. An IPv6 wildcard is written
                // `tcp://[::]:port`.
                TcpHost::Any => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), *port),
                TcpHost::Ip(ip) => SocketAddr::new(*ip, *port),
                TcpHost::Name(name) => {
                    // A listener's host is an interface, and resolving one
                    // is the resolver's job only for a literal; anything
                    // else would bind an address this machine may not have.
                    let ip: IpAddr = name.parse().map_err(|_| {
                        Error::EADDRINVAL(
                            format!(
                                "cannot listen on {name:?}: a listening address must be an IP \
                                 literal or *"
                            )
                            .into(),
                        )
                    })?;
                    SocketAddr::new(ip, *port)
                }
            };
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
            // and then fail every one of them.
            config.validate(true)?;
            let addr = match host {
                TcpHost::Any => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), *port),
                TcpHost::Ip(ip) => SocketAddr::new(*ip, *port),
                TcpHost::Name(name) => {
                    let ip: IpAddr = name.parse().map_err(|_| {
                        Error::EADDRINVAL(
                            format!(
                                "cannot listen on {name:?}: a listening address must be an IP \
                                 literal or *"
                            )
                            .into(),
                        )
                    })?;
                    SocketAddr::new(ip, *port)
                }
            };
            let listener = TcpListener::bind(addr).await.map_err(Error::from)?;
            Ok(Bound::Tls {
                listener,
                config: Box::new(config.clone()),
            })
        }
        Endpoint::Inproc(name) => engine.inner.inproc.bind(name).map(Bound::Inproc),
        #[cfg(unix)]
        Endpoint::Ipc(path) => crate::ipc::IpcBinding::bind(path).map(Bound::Ipc),
        #[cfg(not(unix))]
        Endpoint::Ipc(_) => Err(unsupported(endpoint)),
    }
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
}
