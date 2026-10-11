//! Listener, bindings and the endpoint namespace.
//!
//! A Listener is one externally reachable messaging namespace, not one socket
//! (master doc §3): several bindings may serve the same set of endpoints.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use quinn::VarInt;
use tokio::sync::{mpsc, watch};
use weida_core::{Error, Fingerprint, Limits, validate_endpoint_path};
use weida_protocol::codes;
use weida_protocol::header::GuaranteeSet;

use crate::config::{ClientTls, ServerTls};
use crate::conn::ConnCtx;
use crate::endpoint::{
    BusMember, BusState, Endpoint, PairState, Paired, PubState, Publisher, PullState, Puller,
    RepState, Replier, RespondState, Respondent,
};
use crate::inproc;
use crate::pubsub::SubRegistry;
use crate::runtime::{Exec, RuntimeInner, Shared};
use crate::stream::{Acceptor, Incoming};
use crate::tls;
use crate::transfer::{IncomingRequest, IncomingTransfer};
use crate::transport::Link;

/// What a registered endpoint path does with an inbound transfer.
pub(crate) enum Route {
    /// A replier: inbound exchanges become [`IncomingRequest`]s.
    Request(mpsc::Sender<IncomingRequest>),
    /// A puller or subscriber: inbound one-way transfers are queued as-is.
    Transfer(mpsc::Sender<IncomingTransfer>),
    /// A raw L0 acceptor: both stream kinds are queued, tagged.
    Raw(mpsc::Sender<Incoming>),
    /// A publisher. Nothing inbound is accepted on this path; the entry exists
    /// so the path is claimed exclusively and so SUBSCRIBE can be answered for
    /// a path that really is served here.
    Pub,
    /// A paired endpoint: one-way transfers in both directions, from exactly
    /// **one** peer ([`PairOwner`]).
    Pair {
        /// Where an accepted transfer goes.
        queue: mpsc::Sender<IncomingTransfer>,
        /// The one connection this endpoint talks to.
        owner: Arc<PairOwner>,
    },
    /// A radio. Like a publisher it accepts nothing inbound; its SUBSCRIBE
    /// frames are dish joins, kept in the hub
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
    Radio(Arc<crate::radio::RadioHub>),
    /// A dish: inbound segments are checked against the newest delivered on
    /// their topic and queued without ever blocking the connection.
    Dish(crate::radio::DishRoute),
}

/// The single peer of a paired endpoint, and the claim on it.
///
/// `id` is `0` until a connection takes the endpoint, then that connection's
/// `stable_id` **plus one** — `0` is the sentinel, so a stable id of zero
/// cannot masquerade as "unclaimed". A stream from any other connection is
/// refused with `LIMIT_EXCEEDED` and the connection survives: ZeroMQ's PAIR
/// drops the newcomer silently, and refusing while saying so is this
/// repository's rule for a capacity decision
/// ([decisions/0005](../../../docs/decisions/0005-refusal-race.md)).
///
/// The connection is held **weakly**. A bound endpoint lives in the
/// listener's namespace and every connection holds that namespace, so a
/// strong handle here would be a cycle: the connection would keep itself
/// alive through the route that names it. A peer that went away upgrades to
/// `None`, which is exactly what the sending side needs to know.
///
/// **The claim is released when its holder dies.** A pair whose peer
/// restarts, or whose connection went past the idle timeout, is claimable
/// again: holding the endpoint against a connection that no longer exists
/// would make one peer's shutdown permanent, which is neither ZeroMQ's
/// behaviour nor nanomsg's and is not what "one peer at a time" means. The
/// release happens inside [`PairOwner::claim`] rather than in a destructor
/// because the holder is exactly what a newcomer's claim has to compare
/// itself against.
pub(crate) struct PairOwner {
    id: AtomicU64,
    conn: watch::Sender<Option<Weak<ConnCtx>>>,
}

impl PairOwner {
    pub(crate) fn new() -> PairOwner {
        PairOwner {
            id: AtomicU64::new(0),
            conn: watch::channel(None).0,
        }
    }

    /// Claims this endpoint for `conn`, or reports that another peer holds it.
    ///
    /// The loop runs at most twice: either the claim is free, or a dead
    /// holder is released and the retry takes it, or a live holder refuses
    /// it. Two newcomers racing a dead holder both see it gone, exactly one
    /// wins the `0 -> id` exchange, and the loser refuses against the
    /// winner.
    pub(crate) fn claim(&self, conn: &Arc<ConnCtx>) -> bool {
        let id = conn.conn.stable_id() as u64 + 1;
        loop {
            match self
                .id
                .compare_exchange(0, id, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.conn.send_replace(Some(Arc::downgrade(conn)));
                    return true;
                }
                // The same connection again: every stream after the first.
                Err(held) if held == id => return true,
                Err(held) => {
                    if self.holder_is_live() {
                        return false;
                    }
                    // Dropping the id back to the sentinel is what makes the
                    // endpoint claimable; the watch value goes with it so a
                    // sender waiting in `peer` waits for the next peer
                    // instead of reading a dead one.
                    if self
                        .id
                        .compare_exchange(held, 0, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.conn.send_replace(None);
                    }
                }
            }
        }
    }

    /// Is the connection that holds this endpoint still usable?
    ///
    /// A dropped `ConnCtx` fails to upgrade; a *closed* one may still be
    /// upgradable, because a pool or a drain can outlive the connection it
    /// holds, and a closed connection can carry nothing for this pair.
    fn holder_is_live(&self) -> bool {
        let held = self.conn.borrow().clone();
        held.and_then(|weak| weak.upgrade())
            .is_some_and(|ctx| ctx.conn.close_reason().is_none())
    }

    /// The peer, once one has claimed the endpoint and while it is alive.
    pub(crate) async fn peer(&self) -> Result<Arc<ConnCtx>, Error> {
        let mut rx = self.conn.subscribe();
        loop {
            if let Some(weak) = rx.borrow_and_update().clone() {
                return weak.upgrade().ok_or(Error::NotConnected);
            }
            rx.changed().await.map_err(|_| Error::NotConnected)?;
        }
    }
}

/// Endpoint path to accept-queue map.
///
/// A plain `RwLock<HashMap>`, not an async lock: lookup is a hash and a channel
/// clone, and the guard is never held across an `await`.
pub(crate) struct Namespace {
    routes: RwLock<HashMap<Arc<str>, Route>>,
    /// Which raw paths each connection holds a subscription on.
    ///
    /// Only L2 consumers appear here: a Pub/Sub subscription is the
    /// [`SubRegistry`]'s business, and a queue's is this table's, because the
    /// thing that must happen when the connection closes is different — a
    /// publisher forgets a subscriber, a queue is *told*, so it can requeue
    /// what that consumer owed (0018 §4.7). Bounded by the same
    /// `max_subscriptions` count the registry enforces.
    consumers: RwLock<HashMap<usize, HashSet<Arc<str>>>>,
}

impl Namespace {
    pub(crate) fn new() -> Namespace {
        Namespace {
            routes: RwLock::new(HashMap::new()),
            consumers: RwLock::new(HashMap::new()),
        }
    }

    /// Looks a path up. The path is compared byte for byte: it is an opaque
    /// identifier, so there is no splitting, prefix matching or wildcarding
    /// (master doc §4, §81 rule 5).
    ///
    /// Returns a clone of the route's sender rather than a guard, so the lock
    /// is never held across an `await`.
    pub(crate) fn lookup(&self, path: &str) -> Option<Route> {
        self.routes
            .read()
            .expect("namespace lock poisoned")
            .get(path)
            .map(Route::clone_sender)
    }

    pub(crate) fn register(&self, path: &str, route: Route) -> Result<(), Error> {
        let mut routes = self.routes.write().expect("namespace lock poisoned");
        match routes.entry(Arc::from(path)) {
            Entry::Occupied(_) => Err(Error::AlreadyRegistered),
            Entry::Vacant(slot) => {
                slot.insert(route);
                Ok(())
            }
        }
    }

    /// Releases a path so the same endpoint can be registered again.
    ///
    /// Used by subscriber teardown: a `Sub` claims its path on the pooled
    /// client connection and must give it back, or a later `Sub` on the same
    /// connection would fail with [`Error::AlreadyRegistered`] forever.
    pub(crate) fn unregister(&self, path: &str) {
        self.routes
            .write()
            .expect("namespace lock poisoned")
            .remove(path);
    }

    /// Records that `conn_id` consumes on `path`.
    pub(crate) fn note_consumer(&self, conn_id: usize, path: &str) {
        self.consumers
            .write()
            .expect("namespace lock poisoned")
            .entry(conn_id)
            .or_default()
            .insert(Arc::from(path));
    }

    /// Forgets one (connection, path) pair; the last one removes the entry.
    pub(crate) fn forget_consumer(&self, conn_id: usize, path: &str) {
        let mut consumers = self.consumers.write().expect("namespace lock poisoned");
        if let Some(paths) = consumers.get_mut(&conn_id) {
            paths.remove(path);
            if paths.is_empty() {
                consumers.remove(&conn_id);
            }
        }
    }

    /// Takes every path `conn_id` consumed on, with the route serving it.
    ///
    /// Called once, when the connection closes: the entry is removed, so a
    /// queue is told exactly once that a consumer is gone.
    pub(crate) fn take_consumer_routes(&self, conn_id: usize) -> Vec<(Arc<str>, Route)> {
        let taken = self
            .consumers
            .write()
            .expect("namespace lock poisoned")
            .remove(&conn_id);
        let Some(paths) = taken else {
            return Vec::new();
        };
        let routes = self.routes.read().expect("namespace lock poisoned");
        paths
            .into_iter()
            .filter_map(|path| {
                let route = routes.get(&path)?.clone_sender();
                Some((path, route))
            })
            .collect()
    }
}

impl Route {
    fn clone_sender(&self) -> Route {
        match self {
            Route::Request(tx) => Route::Request(tx.clone()),
            Route::Transfer(tx) => Route::Transfer(tx.clone()),
            Route::Raw(tx) => Route::Raw(tx.clone()),
            Route::Pair { queue, owner } => Route::Pair {
                queue: queue.clone(),
                owner: Arc::clone(owner),
            },
            Route::Pub => Route::Pub,
            Route::Radio(hub) => Route::Radio(Arc::clone(hub)),
            Route::Dish(route) => Route::Dish(route.clone()),
        }
    }
}

pub(crate) struct ListenerInner {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) namespace: Arc<Namespace>,
    pub(crate) subs: Arc<SubRegistry>,
}

/// One externally reachable messaging namespace.
///
/// A Listener is a namespace, not a socket and not a transport: it owns the
/// endpoint routing table and nothing else (master doc §3). Credentials belong
/// to the individual bindings, because they are transport-specific — a QUIC
/// binding needs a certificate and key, a future in-process or adapter binding
/// needs neither — and because two interfaces of one service may legitimately
/// present different certificates.
#[derive(Clone)]
pub struct Listener {
    inner: Arc<ListenerInner>,
}

impl Listener {
    pub(crate) fn new(runtime: Arc<RuntimeInner>) -> Listener {
        // A binding cannot tell the tiers apart — nothing on the wire
        // distinguishes them (`docs/PROTOCOL.md` §2.5) — so it serves every
        // accepted connection on the bulk profile, which is the one that must
        // tolerate payload.
        let limits = runtime.config.limits;
        let ordering = runtime.config.guarantees.ordering;
        Listener {
            inner: Arc::new(ListenerInner {
                runtime,
                namespace: Arc::new(Namespace::new()),
                subs: Arc::new(SubRegistry::new(limits, ordering)),
            }),
        }
    }

    /// Adds a native QUIC binding with its own server identity.
    ///
    /// `tls` may be a bare [`crate::Identity`] — the binding then accepts
    /// anonymous peers — or a [`ServerTls`] that also requires clients to
    /// present an identity it trusts. The material is loaded and validated
    /// here, so a misconfigured certificate fails before the socket serves
    /// anything. Pass port `0` to let the OS choose; read it back with
    /// [`Binding::local_addr`].
    ///
    /// Several bindings may serve the same Listener, each with its own
    /// identity: the endpoints they expose are the same, the identities they
    /// present need not be.
    pub async fn bind_quic(
        &self,
        addr: SocketAddr,
        tls: impl Into<ServerTls>,
    ) -> Result<Binding, Error> {
        let server_config = tls::server_config(&tls.into(), &self.inner.runtime.config.limits)?;
        let socket = std::net::UdpSocket::bind(addr).map_err(Error::Io)?;
        self.serve_quic(socket, server_config)
    }

    /// Adds a native QUIC binding on a socket that is already bound.
    ///
    /// For a process that received its socket rather than binding it: from
    /// its predecessor over `SCM_RIGHTS` during a restart
    /// ([`Binding::restart`]), or through socket activation. Otherwise it is
    /// exactly [`Listener::bind_quic`].
    ///
    /// The socket is taken as it is: weida neither binds it nor sets options
    /// on it, and [`Binding::local_addr`] reports the address it is bound to.
    ///
    /// While another process still reads the same socket, each datagram
    /// reaches only one of the two, so a successor should start serving only
    /// once its predecessor's `restart` has returned.
    pub async fn bind_quic_socket(
        &self,
        socket: std::net::UdpSocket,
        tls: impl Into<ServerTls>,
    ) -> Result<Binding, Error> {
        let server_config = tls::server_config(&tls.into(), &self.inner.runtime.config.limits)?;
        self.serve_quic(socket, server_config)
    }

    /// Serves this Listener's endpoints over QUIC on `socket`.
    fn serve_quic(
        &self,
        socket: std::net::UdpSocket,
        server_config: quinn::ServerConfig,
    ) -> Result<Binding, Error> {
        let exec = self.inner.runtime.exec.clone();

        // Inside the runtime context: `quinn` registers the socket with the
        // reactor as it is constructed, and the calling thread may have none.
        let endpoint = {
            let _guard = exec.enter();
            let runtime = quinn::default_runtime()
                .ok_or_else(|| Error::Runtime("no async runtime for the QUIC endpoint".into()))?;
            quinn::Endpoint::new(
                quinn::EndpointConfig::default(),
                Some(server_config),
                socket,
                runtime,
            )
            .map_err(Error::Io)?
        };
        let local_addr = endpoint.local_addr().map_err(Error::Io)?;
        self.inner.runtime.track_endpoint(endpoint.clone());

        let peers = Arc::new(PeerConns::default());
        exec.spawn(accept_connections(
            endpoint.clone(),
            Arc::clone(&self.inner),
            Arc::clone(&peers),
        ));

        tracing::info!(%local_addr, "quic binding listening");
        Ok(Binding {
            endpoint,
            local_addr,
            peers,
            close_timeout: self.inner.runtime.config.shutdown_timeout,
            exec,
        })
    }

    /// Binds this Listener's endpoints on an in-process bus.
    ///
    /// The counterpart of [`Listener::bind_quic`] for the transport that has
    /// no socket: `weida+inproc://<bus>/<path>` reaches the same endpoints
    /// from the same process, with the same frames, the same HELLO and the
    /// same negotiation, and without TLS or credentials
    /// (`docs/PROTOCOL.md` §2.1,
    /// [decisions/0010](../../../docs/decisions/0010-local-transport.md)
    /// §4.1). The bus name is at most 256 bytes and unique within the
    /// process; binding a name twice fails with
    /// [`Error::AlreadyRegistered`].
    pub fn bind_inproc(&self, bus: &str) -> Result<LocalBinding, Error> {
        let incoming = inproc::bind(bus)?;
        let exec = self.inner.runtime.exec.clone();
        exec.spawn(accept_local(
            incoming,
            Arc::clone(&self.inner.namespace),
            Arc::clone(&self.inner.subs),
            self.inner.runtime.config.limits,
            exec.clone(),
            self.inner.runtime.config.guarantees,
            self.inner.runtime.shared(),
        ));
        tracing::info!(bus, "inproc binding listening");
        Ok(LocalBinding {
            bus: bus.to_owned(),
        })
    }

    /// Binds this Listener's endpoints on an `AF_UNIX` socket.
    ///
    /// `SOCK_STREAM` on a filesystem path, no TLS and no credentials of our
    /// own: the peer is proved by the kernel instead
    /// ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
    /// §4.4, §4.5). The socket file is replaced if a crash left one, and its
    /// mode is set to `0600` explicitly rather than inherited from `umask` —
    /// but **the directory is what makes that safe**: unlink-then-bind has a
    /// substitution race that only directory ownership and permissions close
    /// (`docs/research/ipc.md` §1.2, §7), so `path` MUST live in a directory
    /// this process owns and no other user may write.
    ///
    /// Connections are grouped into peers by
    /// [decision 0012](../../../docs/decisions/0012-local-connection-grouping.md):
    /// the first connection is the peer, further ones carry its group token
    /// and are admitted only if the kernel credentials match.
    #[cfg(unix)]
    pub fn bind_unix(&self, path: impl AsRef<std::path::Path>) -> Result<UnixBinding, Error> {
        let path = path.as_ref();
        let exec = self.inner.runtime.exec.clone();
        let (binding, listener) = {
            // Inside the runtime context: tokio registers the socket with the
            // reactor as it is constructed, and the calling thread may have
            // no reactor of its own.
            let _guard = exec.enter();
            // The bind hygiene — the `sun_path` budget, the socket-type
            // check, unlink-then-bind and the explicit mode — is
            // `weida-runtime`'s, because `ipc://` on any protocol needs the
            // same four answers [0010 §4.5].
            weida_runtime::BoundUnixSocket::bind(path)?
        };
        exec.spawn(accept_unix(
            listener,
            self.local_accept(|link| Link::Unix(Box::new(link))),
        ));
        tracing::info!(path = %path.display(), "unix binding listening");
        Ok(UnixBinding { inner: binding })
    }

    /// Binds a Windows named pipe, `\\.\pipe\<name>`, and serves it.
    ///
    /// The pipe is created with an owner-only descriptor, for local clients
    /// only, and only if the name does not exist yet — the three answers a
    /// pipe needs where a socket file needs a private directory
    /// ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
    /// §4.5). Connections are grouped into peers exactly as on `AF_UNIX`
    /// ([decision 0012](../../../docs/decisions/0012-local-connection-grouping.md)).
    #[cfg(windows)]
    pub fn bind_pipe(&self, name: &str) -> Result<PipeBinding, Error> {
        let addr = weida_core::PipeAddr::parse(&format!("{}://{name}/", weida_core::SCHEME_PIPE))?;
        let exec = self.inner.runtime.exec.clone();
        let (binding, first) = {
            // Inside the runtime context, for the same reason as on unix.
            let _guard = exec.enter();
            weida_runtime::BoundPipe::bind(addr.os_path())?
        };
        let name = addr.name.clone();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        exec.spawn(accept_pipe(
            binding,
            first,
            stop_rx,
            self.local_accept(|link| Link::Pipe(Box::new(link))),
        ));
        tracing::info!(pipe = %name, "pipe binding listening");
        Ok(PipeBinding {
            name,
            _stop: stop_tx,
        })
    }

    /// What every accepted connection of a grouped local transport is
    /// served with.
    #[cfg(any(unix, windows))]
    fn local_accept<S: crate::grouped::Stream>(
        &self,
        link: fn(crate::grouped::Grouped<S>) -> Link,
    ) -> LocalAccept<S> {
        LocalAccept {
            groups: Arc::new(crate::grouped::Groups::default()),
            namespace: Arc::clone(&self.inner.namespace),
            subs: Arc::clone(&self.inner.subs),
            limits: self.inner.runtime.config.limits,
            exec: self.inner.runtime.exec.clone(),
            guarantees: self.inner.runtime.config.guarantees,
            shared: self.inner.runtime.shared(),
            link,
        }
    }

    /// Registers a replier for `path`.
    ///
    /// The path must be a valid endpoint path and must not already be
    /// registered on this listener.
    pub fn replier(&self, path: &str) -> Result<Replier, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.endpoint_queue);
        self.inner.namespace.register(path, Route::Request(tx))?;
        Ok(Endpoint::from_state(RepState::new(path, rx)))
    }

    /// Registers a puller for `path`.
    ///
    /// Pull binds and Push connects, mirroring Rep/Req. The path must be a
    /// valid endpoint path and must not already be registered on this
    /// listener, whatever pattern claimed it.
    pub fn puller(&self, path: &str) -> Result<Puller, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.endpoint_queue);
        self.inner.namespace.register(path, Route::Transfer(tx))?;
        Ok(Endpoint::from_state(PullState::new(path, rx)))
    }

    /// Registers a publisher for `path`.
    ///
    /// Pub binds and Sub connects. Nothing inbound is accepted on the path —
    /// the registration claims it exclusively — and subscriptions that arrived
    /// before this call are already recorded, so a publisher created after its
    /// subscribers still reaches them.
    pub fn publisher(&self, path: &str) -> Result<Publisher, Error> {
        validate_endpoint_path(path)?;
        self.inner.namespace.register(path, Route::Pub)?;
        Ok(Endpoint::from_state(PubState::new(
            path,
            Arc::clone(&self.inner.subs),
            self.inner.runtime.config.limits.subscriber_buffer_bytes,
        )))
    }

    /// Registers a radio for `path`
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
    ///
    /// RADIO binds and DISH connects. Nothing inbound is accepted on the
    /// path; a dish's join is a SUBSCRIBE and lands in the radio. Every
    /// segment goes to the dishes joined when it opens, and each copy is
    /// dropped rather than waited for: by its successor, by the dish's
    /// `max_age`, or by the dish's byte budget.
    pub fn radio(&self, path: &str) -> Result<crate::Radio, Error> {
        validate_endpoint_path(path)?;
        let hub = Arc::new(crate::radio::RadioHub::new(
            path,
            self.inner.runtime.config.limits,
        ));
        self.inner
            .namespace
            .register(path, Route::Radio(Arc::clone(&hub)))?;
        Ok(Endpoint::from_state(crate::radio::RadioState::new(
            hub,
            Arc::clone(&self.inner.namespace),
        )))
    }

    /// Registers a raw L0 acceptor for `path`.
    ///
    /// Below the patterns: an acceptor takes both stream kinds on one path and
    /// hands them over tagged ([`crate::Incoming`]), leaving the shape of the
    /// conversation to the application. The path is claimed exactly like a
    /// replier's or a puller's.
    pub fn acceptor(&self, path: &str) -> Result<Acceptor, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.endpoint_queue);
        self.inner.namespace.register(path, Route::Raw(tx))?;
        Ok(Acceptor::new(path, rx))
    }

    /// Registers a bound paired endpoint for `path`.
    ///
    /// PAIR is one connection and one peer, carrying one-way transfers in
    /// both directions ([ARCHITECTURE.md](../../../docs/ARCHITECTURE.md)
    /// §6b). It adds **no wire vocabulary**: a paired endpoint talks to a
    /// bare [`crate::Peer`] or [`Acceptor`] on the same path, which is the
    /// test that proves the pattern layer is API and nothing else.
    ///
    /// The one-peer rule is enforced at dispatch: the first connection to
    /// send here claims the endpoint, and a stream from any other is refused
    /// with `LIMIT_EXCEEDED` while **the first keeps working**.
    pub fn pair(&self, path: &str) -> Result<Paired, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.endpoint_queue);
        let owner = Arc::new(PairOwner::new());
        self.inner.namespace.register(
            path,
            Route::Pair {
                queue: tx,
                owner: Arc::clone(&owner),
            },
        )?;
        Ok(Endpoint::from_state(PairState::bound(path, owner, rx)))
    }

    /// Registers a respondent for `path`.
    ///
    /// A survey question is an exchange, so a respondent's route is
    /// **byte-for-byte a replier's**: the same request route, the same accept
    /// queue, the same backpressure. What makes it a survey is entirely on
    /// the asking side — the fan-out and the deadline — so this pattern adds
    /// no routing and no wire vocabulary.
    pub fn respondent(&self, path: &str) -> Result<Respondent, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.endpoint_queue);
        self.inner.namespace.register(path, Route::Request(tx))?;
        Ok(Endpoint::from_state(RespondState::new(path, rx)))
    }

    /// Registers a bus member on `path` that dials the others on `tls`'s
    /// terms.
    ///
    /// The **only** factory that takes both a path and dialling terms,
    /// because a bus member is the one role that is bound and dialling at
    /// once ([ARCHITECTURE.md](../../../docs/ARCHITECTURE.md) §6b): it
    /// accepts on its own path with a puller's route and reaches the others
    /// with [`crate::BusMember::connect`].
    pub fn bus(&self, path: &str, tls: impl Into<ClientTls>) -> Result<BusMember, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.endpoint_queue);
        self.inner.namespace.register(path, Route::Transfer(tx))?;
        Ok(Endpoint::from_state(BusState::new(
            path,
            Arc::clone(&self.inner.runtime),
            Arc::new(tls.into()),
            rx,
            self.inner.runtime.config.endpoint_queue,
            self.inner.runtime.config.limits.subscriber_buffer_bytes,
        )))
    }
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listener").finish_non_exhaustive()
    }
}

/// One concrete transport binding of a [`Listener`].
pub struct Binding {
    endpoint: quinn::Endpoint,
    local_addr: SocketAddr,
    peers: Arc<PeerConns>,
    exec: Exec,
    close_timeout: Duration,
}

impl std::fmt::Debug for Binding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Binding")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl Binding {
    /// The address actually bound, with the port resolved.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting and closes this binding's connections with
    /// `SHUTDOWN`, then waits for the socket to go idle for at most
    /// [`RuntimeConfig::shutdown_timeout`](crate::RuntimeConfig::shutdown_timeout).
    pub async fn close(&self) {
        self.close_with(codes::SHUTDOWN, b"binding closed").await;
    }

    /// Stops accepting and closes every connection of this binding with
    /// `RESTARTING`, for a server that hands its socket over to a successor
    /// ([decisions/0038](../../../docs/decisions/0038-restart-handoff.md)).
    ///
    /// Diallers see [`LossCause::PeerRestarting`](crate::LossCause::PeerRestarting)
    /// and redial at once, whatever their
    /// [`stop_on_peer_closed`](crate::ReconnectPolicy::stop_on_peer_closed)
    /// says. The wait for the socket to go idle is bounded like
    /// [`Binding::close`]'s. The successor takes the socket over through
    /// [`Listener::bind_quic_socket`].
    ///
    /// A closed endpoint keeps reading its socket and refuses new connections
    /// until this binding and its runtime are gone, so drop the runtime (or
    /// exit) as soon as `restart` returns. A client whose immediate redial
    /// was refused in that window retries on its normal schedule.
    pub async fn restart(&self) {
        self.close_with(codes::RESTARTING, b"server restarting")
            .await;
    }

    /// Closes the endpoint with `code` and waits, for at most the close
    /// timeout, for its socket to go idle.
    async fn close_with(&self, code: u64, reason: &[u8]) {
        self.endpoint.close(VarInt::from_u32(code as u32), reason);
        if self
            .exec
            .within(self.close_timeout, self.endpoint.wait_idle())
            .await
            .is_none()
        {
            tracing::debug!(
                timeout_ms = self.close_timeout.as_millis(),
                "timeout reached before the binding's socket went idle"
            );
        }
    }

    /// Closes every live connection this binding holds from `peer`, with
    /// `REJECTED` and the reason `disconnected by the application`, and
    /// returns how many it closed
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.4).
    ///
    /// **Not a ban.** The dialler sees
    /// [`LossCause::PeerClosed`](crate::LossCause::PeerClosed) and redials
    /// under its own [`ReconnectPolicy`](crate::ReconnectPolicy); keeping a
    /// peer out is refusing its arrivals and joins. An anonymous client
    /// cannot be named, so it cannot be disconnected here.
    pub fn disconnect(&self, peer: Fingerprint) -> usize {
        self.peers.disconnect(&peer)
    }
}

/// One in-process binding: a bus name and nothing else.
///
/// No socket, no TLS and no credentials — there is nothing to configure and
/// nobody to prove ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
/// §4.1, §4.4). Dropping it unregisters the bus, after which a dial to that
/// name fails as a dial to a closed port does.
#[derive(Debug)]
pub struct LocalBinding {
    bus: String,
}

impl LocalBinding {
    /// The bus this binding answers on.
    pub fn bus(&self) -> &str {
        &self.bus
    }
}

impl Drop for LocalBinding {
    fn drop(&mut self) {
        inproc::unbind(&self.bus);
    }
}

/// One `AF_UNIX` binding: the socket file, removed when this drops.
#[cfg(unix)]
#[derive(Debug)]
pub struct UnixBinding {
    inner: weida_runtime::BoundUnixSocket,
}

#[cfg(unix)]
impl UnixBinding {
    /// The socket path actually bound.
    pub fn path(&self) -> &std::path::Path {
        self.inner.path()
    }
}

/// One named-pipe binding: the pipe name, released when this drops.
#[cfg(windows)]
#[derive(Debug)]
pub struct PipeBinding {
    name: String,
    /// Dropping it ends the accept loop, and with it the last listening
    /// instance, which is what releases the name.
    _stop: tokio::sync::oneshot::Sender<()>,
}

#[cfg(windows)]
impl PipeBinding {
    /// The pipe name bound, without the `\\.\pipe\` prefix.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// What one accepted local connection is served with.
#[cfg(any(unix, windows))]
struct LocalAccept<S: crate::grouped::Stream> {
    groups: Arc<crate::grouped::Groups<S>>,
    namespace: Arc<Namespace>,
    subs: Arc<SubRegistry>,
    limits: Limits,
    exec: Exec,
    guarantees: GuaranteeSet,
    shared: Arc<Shared>,
    /// Which `Link` variant a peer over this stream is.
    link: fn(crate::grouped::Grouped<S>) -> Link,
}

#[cfg(any(unix, windows))]
impl<S: crate::grouped::Stream> LocalAccept<S> {
    fn clone_for(&self) -> LocalAccept<S> {
        LocalAccept {
            groups: Arc::clone(&self.groups),
            namespace: Arc::clone(&self.namespace),
            subs: Arc::clone(&self.subs),
            limits: self.limits,
            exec: self.exec.clone(),
            guarantees: self.guarantees,
            shared: Arc::clone(&self.shared),
            link: self.link,
        }
    }

    /// Serves one accepted connection: a control connection becomes a
    /// peer, a transfer or reverse connection joins the peer its token
    /// names ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.1, §4.2, §4.4).
    async fn serve(self, stream: S) {
        use crate::grouped::{
            Accepted, accept_control, admit_reverse, admit_transfer, read_accepted,
        };
        let accepted = match read_accepted(stream).await {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::debug!(error = %e, "local connection preamble rejected");
                return;
            }
        };
        match accepted {
            Accepted::Control(stream, principal) => {
                let link = match accept_control(
                    stream,
                    principal,
                    Arc::clone(&self.groups),
                    self.limits.max_local_streams,
                    self.limits.max_parked_reverse,
                )
                .await
                {
                    Ok(link) => link,
                    Err(e) => {
                        tracing::debug!(error = %e, "local control connection failed");
                        return;
                    }
                };
                let ctx = ConnCtx::spawn(
                    (self.link)(link),
                    self.limits,
                    self.namespace,
                    Some(Arc::clone(&self.subs)),
                    self.exec,
                    self.guarantees,
                    self.shared,
                );
                let conn_id = ctx.conn.stable_id();
                let reason = ctx.conn.closed().await;
                self.subs.remove_connection(conn_id);
                crate::conn::drop_consumers(&ctx).await;
                tracing::debug!(%reason, "local connection closed");
            }
            Accepted::Transfer(token, stream, principal) => {
                // The token names the group and the kernel says who is
                // asking; an unbound connection is dispatched nowhere
                // [0012 §4.2].
                if !admit_transfer(&self.groups, &token, &principal, stream) {
                    tracing::warn!(
                        "local transfer connection refused: unknown or mismatched group"
                    );
                }
            }
            Accepted::Reverse(token, stream, principal) => {
                // A connection parked for fan-out, under the same admission
                // rule and the two bounds of [0012 §4.4].
                if !admit_reverse(&self.groups, &token, &principal, stream) {
                    tracing::debug!(
                        "local reverse connection not parked: unknown group or pool full"
                    );
                }
            }
        }
    }
}

/// Accepts `AF_UNIX` connections and serves each one.
#[cfg(unix)]
async fn accept_unix(
    listener: tokio::net::UnixListener,
    serve: LocalAccept<crate::unix::UnixLocal>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            tracing::debug!("unix binding closed; accept loop ending");
            return;
        };
        if serve.shared.drain.is_draining() {
            // Admission stopped: the same rule as on every other binding
            // (`docs/decisions/0009-drain.md` §4.5).
            continue;
        }
        // The accepted socket carries the runtime its halves write their
        // end-of-payload marker on (B-245), exactly as a pipe instance does.
        let stream = crate::unix::UnixLocal::accepted(stream, serve.exec.clone());
        serve.exec.spawn(serve.clone_for().serve(stream));
    }
}

/// Accepts named-pipe connections and serves each one.
///
/// A pipe instance is one connection: after each accept the next listening
/// instance is created before the accepted one is served, so that a client
/// arriving in between finds the pipe busy rather than absent
/// (`docs/research/ipc.md` §3.1).
#[cfg(windows)]
async fn accept_pipe(
    binding: weida_runtime::BoundPipe,
    first: tokio::net::windows::named_pipe::NamedPipeServer,
    mut stop: tokio::sync::oneshot::Receiver<()>,
    serve: LocalAccept<crate::pipe::PipeStream>,
) {
    let mut listening = first;
    loop {
        let connected = tokio::select! {
            connected = listening.connect() => connected,
            _ = &mut stop => {
                tracing::debug!("pipe binding dropped; accept loop ending");
                return;
            }
        };
        if let Err(e) = connected {
            tracing::debug!(error = %e, "pipe accept failed; accept loop ending");
            return;
        }
        let accepted = std::mem::replace(
            &mut listening,
            match binding.next_instance() {
                Ok(next) => next,
                Err(e) => {
                    tracing::warn!(error = %e, "pipe instance not created; accept loop ending");
                    return;
                }
            },
        );
        if serve.shared.drain.is_draining() {
            // Admission stopped: the accepted instance is dropped, which
            // disconnects the client.
            continue;
        }
        let stream = crate::pipe::PipeStream::accepted(accepted, serve.exec.clone());
        serve.exec.spawn(serve.clone_for().serve(stream));
    }
}

/// Accepts in-process connections until the bus is unbound.
async fn accept_local(
    mut incoming: mpsc::UnboundedReceiver<inproc::LocalConn>,
    namespace: Arc<Namespace>,
    subs: Arc<SubRegistry>,
    limits: Limits,
    exec: Exec,
    guarantees: GuaranteeSet,
    shared: Arc<Shared>,
) {
    while let Some(conn) = incoming.recv().await {
        if shared.drain.is_draining() {
            // Admission stopped: the same rule as on a QUIC binding
            // (`docs/decisions/0009-drain.md` §4.5).
            conn.close(codes::SHUTDOWN, "runtime draining");
            continue;
        }
        let namespace = Arc::clone(&namespace);
        let subs = Arc::clone(&subs);
        let exec_for_conn = exec.clone();
        let shared = Arc::clone(&shared);
        exec.spawn(async move {
            let ctx = ConnCtx::spawn(
                Link::Local(conn),
                limits,
                namespace,
                Some(Arc::clone(&subs)),
                exec_for_conn,
                guarantees,
                shared,
            );
            let conn_id = ctx.conn.stable_id();
            let reason = ctx.conn.closed().await;
            subs.remove_connection(conn_id);
            crate::conn::drop_consumers(&ctx).await;
            tracing::debug!(%reason, "local connection closed");
        });
    }
}

/// Live connections per proved peer identity, for `max_connections_per_peer`
/// and [`Binding::disconnect`].
///
/// Keyed by the fingerprint a peer proved in the handshake, because that is
/// the only thing that binds two connections into one peer
/// (`docs/decisions/0008-session-identity.md` §4.2). A connection that proved
/// nothing is not counted here at all: two anonymous connections cannot be
/// shown to be one peer, so counting them together would refuse strangers for
/// each other's traffic. They remain bounded by `max_connections`.
///
/// The table holds the connection handles rather than a count, so a binding
/// can close one peer's connections; it is bounded by `max_connections`, as
/// the count was.
#[derive(Default)]
struct PeerConns(std::sync::Mutex<HashMap<Fingerprint, Vec<quinn::Connection>>>);

impl PeerConns {
    /// Records one more connection for `peer` and reports whether it fits.
    ///
    /// The entry is taken before the connection is served and released when
    /// it closes, so what is bounded is *live* connections rather than dials
    /// over time.
    fn admit(&self, peer: Option<Fingerprint>, conn: &quinn::Connection, max: usize) -> bool {
        let Some(peer) = peer else {
            return true;
        };
        let mut conns = self.0.lock().expect("peer table poisoned");
        let held = conns.entry(peer).or_default();
        if held.len() >= max {
            return false;
        }
        held.push(conn.clone());
        true
    }

    fn release(&self, peer: Option<Fingerprint>, conn_id: usize) {
        let Some(peer) = peer else {
            return;
        };
        let mut conns = self.0.lock().expect("peer table poisoned");
        if let Some(held) = conns.get_mut(&peer) {
            held.retain(|c| c.stable_id() != conn_id);
            // The table is keyed by remote input, so an entry that holds
            // nothing is removed rather than left behind.
            if held.is_empty() {
                conns.remove(&peer);
            }
        }
    }

    /// Closes every live connection of `peer`; entries leave through
    /// [`PeerConns::release`] as each accept task sees its connection close.
    fn disconnect(&self, peer: &Fingerprint) -> usize {
        let held = self
            .0
            .lock()
            .expect("peer table poisoned")
            .get(peer)
            .cloned()
            .unwrap_or_default();
        for conn in &held {
            conn.close(
                VarInt::from_u32(codes::REJECTED as u32),
                b"disconnected by the application",
            );
        }
        held.len()
    }
}

impl std::fmt::Debug for PeerConns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let peers = self.0.lock().map_or(0, |conns| conns.len());
        f.debug_struct("PeerConns").field("peers", &peers).finish()
    }
}

/// Accepts connections until the endpoint is closed.
///
/// Everything it needs is already on the listener it serves: the namespace to
/// route into, the subscription registry, and the runtime whose configuration
/// decides the limits and whose executor runs the connections. Holding the
/// listener for as long as a binding accepts is the honest lifetime — a
/// binding without its namespace serves nothing.
async fn accept_connections(
    endpoint: quinn::Endpoint,
    listener: Arc<ListenerInner>,
    peers: Arc<PeerConns>,
) {
    let config = &listener.runtime.config;
    let limits = config.limits;
    let max_connections = config.max_connections;
    let max_connections_per_peer = config.max_connections_per_peer;
    let guarantees = config.guarantees;
    let exec = listener.runtime.exec.clone();
    let shared = listener.runtime.shared();
    let namespace = Arc::clone(&listener.namespace);
    let subs = Arc::clone(&listener.subs);
    let live = Arc::new(AtomicUsize::new(0));
    while let Some(incoming) = endpoint.accept().await {
        // A draining runtime admits nothing new: the handshake is refused
        // outright rather than accepted and then closed
        // (`docs/decisions/0009-drain.md` §4.5).
        if shared.drain.is_draining() {
            incoming.refuse();
            continue;
        }
        if live.load(Ordering::Relaxed) >= max_connections {
            // Complete the handshake, then say why: a bare refusal leaves the
            // peer unable to distinguish overload from a routing mistake.
            tracing::warn!(max = max_connections, "connection limit reached; refusing");
            exec.spawn(async move {
                if let Ok(conn) = incoming.await {
                    conn.close(
                        VarInt::from_u32(codes::LIMIT_EXCEEDED as u32),
                        b"connection limit reached",
                    );
                }
            });
            continue;
        }

        let namespace = Arc::clone(&namespace);
        let subs = Arc::clone(&subs);
        let live = Arc::clone(&live);
        live.fetch_add(1, Ordering::Relaxed);
        let exec_for_conn = exec.clone();
        let shared = Arc::clone(&shared);
        let peers = Arc::clone(&peers);
        exec.spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    let remote = conn.remote_address();
                    let conn_id = conn.stable_id();
                    // The per-peer ceiling can only be applied here: the
                    // identity exists once the handshake is complete, and one
                    // peer now holds one connection per endpoint path it
                    // dials, so the path count it chooses would otherwise be
                    // the only bound
                    // (`docs/decisions/0002-control-and-bulk-separation.md` §7).
                    let peer = crate::tls::peer_fingerprint(&conn);
                    if !peers.admit(peer, &conn, max_connections_per_peer) {
                        tracing::warn!(
                            %remote,
                            max = max_connections_per_peer,
                            "per-peer connection limit reached; refusing"
                        );
                        conn.close(
                            VarInt::from_u32(codes::LIMIT_EXCEEDED as u32),
                            b"per-peer connection limit reached",
                        );
                    } else {
                        tracing::debug!(%remote, "connection accepted");
                        // The handle must outlive the connection: it owns the
                        // actor's control channel.
                        let ctx = ConnCtx::spawn(
                            Link::Quic(conn.clone()),
                            limits,
                            namespace,
                            Some(Arc::clone(&subs)),
                            exec_for_conn,
                            guarantees,
                            shared,
                        );
                        let reason = conn.closed().await;
                        // A peer that goes away takes its subscriptions with
                        // it; otherwise connection churn would grow the
                        // registry.
                        subs.remove_connection(conn_id);
                        crate::conn::drop_consumers(&ctx).await;
                        peers.release(peer, conn_id);
                        tracing::debug!(%remote, %reason, "connection closed");
                    }
                }
                Err(e) => tracing::debug!(error = %e, "handshake failed"),
            }
            live.fetch_sub(1, Ordering::Relaxed);
        });
    }
    tracing::debug!("binding stopped accepting");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn namespace_rejects_duplicate_paths() {
        let ns = Namespace::new();
        let (tx, _rx) = mpsc::channel(1);
        ns.register("/a", Route::Request(tx.clone())).unwrap();
        assert!(matches!(
            ns.register("/a", Route::Request(tx.clone())),
            Err(Error::AlreadyRegistered)
        ));
        // The pattern behind the path is irrelevant: a path is claimed once.
        assert!(matches!(
            ns.register("/a", Route::Pub),
            Err(Error::AlreadyRegistered)
        ));
        ns.register("/b", Route::Request(tx)).unwrap();
    }

    #[tokio::test]
    async fn namespace_lookup_is_exact() {
        let ns = Namespace::new();
        let (tx, _rx) = mpsc::channel(1);
        ns.register("/jobs/a", Route::Request(tx)).unwrap();
        assert!(ns.lookup("/jobs/a").is_some());
        // No prefix matching, no wildcards, no trailing-slash equivalence.
        assert!(ns.lookup("/jobs").is_none());
        assert!(ns.lookup("/jobs/a/").is_none());
        assert!(ns.lookup("/jobs/*").is_none());
        assert!(ns.lookup("/JOBS/A").is_none());
    }

    #[tokio::test]
    async fn unregistering_releases_the_path() {
        let ns = Namespace::new();
        let (tx, _rx) = mpsc::channel(1);
        ns.register("/s", Route::Transfer(tx)).unwrap();
        ns.unregister("/s");
        assert!(ns.lookup("/s").is_none());
        // Unregistering an unknown path is not an error.
        ns.unregister("/s");
        ns.register("/s", Route::Pub).unwrap();
    }
}
