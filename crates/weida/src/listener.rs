//! Listener, bindings and the endpoint namespace.
//!
//! A Listener is one externally reachable messaging namespace, not one socket
//! (master doc §3): several bindings may serve the same set of endpoints.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant};

use quinn::VarInt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use weida_core::{Error, Fingerprint, Limits, validate_endpoint_path};
use weida_protocol::codes;
use weida_protocol::header::GuaranteeSet;

use crate::config::{ClientTls, ServerTls};
use crate::conn::ConnCtx;
use crate::endpoint::{
    BusMember, BusState, Endpoint, PairState, Paired, PubState, Publisher, PullState, Puller,
    RepState, Replier, RespondState, Respondent,
};
use crate::handoff::{self, Handoff, HandoverSocket};
use crate::inproc;
use crate::pubsub::SubRegistry;
use crate::runtime::{Exec, RuntimeInner, Shared};
use crate::stream::{Acceptor, Incoming};
use crate::tls;
use crate::transfer::{IncomingRequest, IncomingTransfer};
use crate::transport::{Link, QuicLink};

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

/// Connection, then path, then the filters held: [`Namespace`]'s record of
/// raw consumers.
type ConsumerFilters = HashMap<usize, HashMap<Arc<str>, HashSet<String>>>;

/// Endpoint path to accept-queue map.
///
/// A plain `RwLock<HashMap>`, not an async lock: lookup is a hash and a channel
/// clone, and the guard is never held across an `await`.
pub(crate) struct Namespace {
    routes: RwLock<HashMap<Arc<str>, Route>>,
    /// Which filters each connection holds on which raw paths: connection,
    /// then path, then the set of filters.
    ///
    /// Only L2 consumers appear here: a Pub/Sub subscription is the
    /// [`SubRegistry`]'s business, and a queue's is this table's, because the
    /// thing that must happen when the connection closes is different — a
    /// publisher forgets a subscriber, a queue is *told*, so it can requeue
    /// what that consumer owed (0018 §4.7). The set holds **filters**, not
    /// paths, because a connection may hold several on one path and the path
    /// is only forgotten with the last of them; it is also what makes a
    /// reservation against `max_subscriptions` exist exactly once per filter
    /// held. Bounded by the same `max_subscriptions` count the registry
    /// enforces.
    consumers: RwLock<ConsumerFilters>,
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

    /// Records that `conn_id` consumes on `path` with `filter`.
    ///
    /// `false` when it already did: the caller then has nothing to count and
    /// nothing to announce.
    pub(crate) fn note_consumer(&self, conn_id: usize, path: &str, filter: &str) -> bool {
        let mut consumers = self.consumers.write().expect("namespace lock poisoned");
        let paths = consumers.entry(conn_id).or_default();
        match paths.get_mut(path) {
            Some(filters) if filters.contains(filter) => false,
            Some(filters) => filters.insert(filter.to_owned()),
            None => {
                paths.insert(Arc::from(path), HashSet::from([filter.to_owned()]));
                true
            }
        }
    }

    /// Forgets one filter `conn_id` held on `path`; `false` when it held no
    /// such filter. The last filter of a path takes the path with it and the
    /// last path takes the connection, so nothing is left behind.
    pub(crate) fn forget_consumer(&self, conn_id: usize, path: &str, filter: &str) -> bool {
        let mut consumers = self.consumers.write().expect("namespace lock poisoned");
        let Some(paths) = consumers.get_mut(&conn_id) else {
            return false;
        };
        let Some(filters) = paths.get_mut(path) else {
            return false;
        };
        let held = filters.remove(filter);
        if filters.is_empty() {
            paths.remove(path);
        }
        if paths.is_empty() {
            consumers.remove(&conn_id);
        }
        held
    }

    /// Takes every path `conn_id` still consumes on, with the route serving
    /// it.
    ///
    /// Called once, when the connection closes: the entry is removed, so a
    /// queue is told exactly once that a consumer is gone, and told so for
    /// **every** path with a filter still held, however many filters the
    /// connection withdrew along the way.
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
            .into_keys()
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
    /// its predecessor over `SCM_RIGHTS` after [`Binding::hand_over`], or
    /// through socket activation. Otherwise it is exactly
    /// [`Listener::bind_quic`].
    ///
    /// The socket is taken as it is: weida neither binds it nor sets options
    /// on it, and [`Binding::local_addr`] reports the address it is bound to.
    ///
    /// A predecessor that called `hand_over` no longer reads the socket, so
    /// the successor may serve it at once; it calls [`Binding::take_over`]
    /// once its endpoints are registered.
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
        // The duplicate a handover passes on; the endpoint gets the original.
        let handed = socket.try_clone().map_err(Error::Io)?;

        // Inside the runtime context: `quinn` registers the socket with the
        // reactor as it is constructed, and the calling thread may have none.
        let (endpoint, socket) = {
            let _guard = exec.enter();
            let runtime = quinn::default_runtime()
                .ok_or_else(|| Error::Runtime("no async runtime for the QUIC endpoint".into()))?;
            let socket = Arc::new(HandoverSocket::new(
                runtime.wrap_udp_socket(socket).map_err(Error::Io)?,
            ));
            let endpoint = quinn::Endpoint::new_with_abstract_socket(
                quinn::EndpointConfig::default(),
                Some(server_config),
                Arc::clone(&socket) as Arc<dyn quinn::AsyncUdpSocket>,
                runtime,
            )
            .map_err(Error::Io)?;
            (endpoint, socket)
        };
        let local_addr = endpoint.local_addr().map_err(Error::Io)?;
        self.inner.runtime.track_endpoint(endpoint.clone());

        let peers = Arc::new(PeerConns::default());
        let open = Arc::new(OpenConns::default());
        exec.spawn(accept_connections(
            endpoint.clone(),
            Arc::clone(&self.inner),
            Arc::clone(&peers),
            Arc::clone(&open),
        ));

        tracing::info!(%local_addr, "quic binding listening");
        Ok(Binding {
            endpoint,
            local_addr,
            peers,
            socket,
            handed,
            open,
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
    /// §4.4, §4.5). A live listener on the path is refused
    /// ([`ErrorKind::AddrInUse`](std::io::ErrorKind::AddrInUse)); a socket
    /// file a crash left behind is replaced. The socket is bound in a private
    /// `0700` directory next to `path`, given mode `0600` there rather than
    /// inheriting `umask`, and renamed into place, so it is never visible with
    /// another mode. **The directory is still what makes that safe**:
    /// whoever can write it can rename over the node afterwards
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
            // check, the live-listener probe, the private-directory bind
            // and rename, and the explicit mode — is `weida-runtime`'s,
            // because `ipc://` on any protocol needs the same answers
            // [0010 §4.5].
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
        let max_connections = self.inner.runtime.config.max_connections;
        LocalAccept {
            groups: Arc::new(crate::grouped::Groups::default()),
            namespace: Arc::clone(&self.inner.namespace),
            subs: Arc::clone(&self.inner.subs),
            limits: self.inner.runtime.config.limits,
            exec: self.inner.runtime.exec.clone(),
            guarantees: self.inner.runtime.config.guarantees,
            shared: self.inner.runtime.shared(),
            link,
            admitted: Arc::new(Semaphore::new(max_connections)),
            departing: Arc::new(Semaphore::new(
                max_connections.clamp(1, Semaphore::MAX_PERMITS),
            )),
            refusals: Arc::new(LogThrottle::new(REFUSAL_LOG_INTERVAL)),
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
    /// The decorator under the endpoint, which `hand_over` makes hold.
    socket: Arc<HandoverSocket>,
    /// A duplicate of the bound socket, for the successor.
    handed: std::net::UdpSocket,
    open: Arc<OpenConns>,
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
        self.endpoint
            .close(VarInt::from_u32(codes::SHUTDOWN as u32), b"binding closed");
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

    /// Hands this binding's socket over to a successor process, which tells
    /// the clients to redial once it serves
    /// ([decisions/0039](../../../docs/decisions/0039-handover-says-when.md)).
    ///
    /// From the call on, this process neither reads nor sends on the socket.
    /// The successor may serve it, through [`Listener::bind_quic_socket`], as
    /// soon as `hand_over` returns, and this process may keep its runtime as
    /// long as it likes.
    ///
    /// Every connection is closed with `RESTARTING`, but the close packets
    /// are held in the [`Handoff`] and not sent. Clients learn of the restart
    /// only when the successor calls [`Binding::take_over`]: they see
    /// [`LossCause::PeerRestarting`](crate::LossCause::PeerRestarting) and
    /// redial at once, whatever their
    /// [`stop_on_peer_closed`](crate::ReconnectPolicy::stop_on_peer_closed)
    /// says, into a binding that already reads.
    ///
    /// The wait for the closes is bounded by
    /// [`RuntimeConfig::shutdown_timeout`](crate::RuntimeConfig::shutdown_timeout),
    /// and a connection still in its handshake is not waited for. If the
    /// successor never calls `take_over`, or a datagram is lost, a client
    /// notices through [`Limits::idle_timeout`] and redials on its normal
    /// schedule.
    pub async fn hand_over(self) -> Handoff {
        self.socket.hold();
        let pending: Vec<quinn::Connection> = self
            .open
            .snapshot()
            .into_iter()
            .filter(|conn| conn.close_reason().is_none())
            .collect();
        self.endpoint.close(
            VarInt::from_u32(codes::RESTARTING as u32),
            b"server restarting",
        );
        // The endpoint's close reaches each connection as an event its driver
        // applies, and the same driver poll then passes the close packet to
        // the socket, all under the lock `closed()` takes to read the reason.
        // A connection that was open above and is now closed has therefore
        // handed its close to the held socket.
        let all_held = self
            .exec
            .within(self.close_timeout, async {
                for conn in &pending {
                    conn.closed().await;
                }
            })
            .await
            .is_some();
        if !all_held {
            tracing::debug!(
                timeout_ms = self.close_timeout.as_millis(),
                "timeout reached before every close was held"
            );
        }
        let held = self.socket.release();
        tracing::info!(
            local_addr = %self.local_addr,
            connections = pending.len(),
            datagrams = held.len(),
            "binding handed over"
        );
        Handoff {
            socket: self.handed,
            local_addr: self.local_addr,
            held,
        }
    }

    /// Sends the close packets a predecessor held in its [`Handoff`], so its
    /// clients redial into this binding
    /// ([decisions/0039](../../../docs/decisions/0039-handover-says-when.md)).
    ///
    /// `held` is the second half of [`Handoff::into_parts`]. Call this after
    /// the Listener's endpoints the clients use are registered: a client
    /// redials within one round trip, and a path not yet registered refuses
    /// it. Each datagram is sent once; the return value is how many were.
    ///
    /// **The bytes must reach this process over a channel as trusted as the
    /// one that carries the socket descriptor.** Whoever can write them makes
    /// this binding send up to the decoder's cap of datagrams from its port,
    /// to destinations of their choosing within what the decoder accepts.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] of kind `InvalidData` for bytes that are not a handoff,
    /// and of kind `InvalidInput` for a handoff of a socket bound to another
    /// address than this binding's.
    pub async fn take_over(&self, held: &[u8]) -> Result<usize, Error> {
        let (from, datagrams) = handoff::decode(held)?;
        if from != self.local_addr {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "handoff for {from} offered to a binding on {}",
                    self.local_addr
                ),
            )));
        }
        let mut sent = 0;
        for datagram in &datagrams {
            match self.socket.send_raw(datagram).await {
                Ok(()) => sent += 1,
                Err(e) => tracing::debug!(
                    destination = %datagram.destination,
                    error = %e,
                    "handed-over datagram not sent"
                ),
            }
        }
        tracing::info!(local_addr = %self.local_addr, sent, "binding took over");
        Ok(sent)
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

/// How long a local accept loop waits after a failed `accept` before it asks
/// again. Descriptor or memory exhaustion passes, and a loop that spun on it
/// would only make it worse.
#[cfg(any(unix, windows))]
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

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
    /// The places `RuntimeConfig::max_connections` hands out: one per live
    /// local connection, taken before it is accepted and kept for as long as
    /// it lives (a control connection for the life of its peer, a transfer or
    /// parked connection until its last half is gone). What a hostile or buggy
    /// local process can make a binding hold is this count, not the
    /// descriptor limit of the whole process.
    admitted: Arc<Semaphore>,
    /// How many closed connections may wait, as a task of their own, for a
    /// queue to take the notice that their consumer is gone ([`depart`]).
    departing: Arc<Semaphore>,
    /// One record a second for everything an accept loop turns away or trips
    /// over, so a flood is not a flood of log lines.
    refusals: Arc<LogThrottle>,
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
            admitted: Arc::clone(&self.admitted),
            departing: Arc::clone(&self.departing),
            refusals: Arc::clone(&self.refusals),
        }
    }

    /// Serves one accepted connection: a control connection becomes a
    /// peer, a transfer or reverse connection joins the peer its token
    /// names ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.1, §4.2, §4.4).
    async fn serve(self, stream: S, place: OwnedSemaphorePermit) {
        use crate::grouped::{
            Accepted, accept_control, admit_reverse, admit_transfer, read_accepted,
        };
        // The preamble is the peer's to send, and a socket that never sends
        // one would hold a place and a descriptor for ever.
        let deadline = Duration::from_millis(self.limits.hello_timeout_ms);
        let accepted = match self.exec.within(deadline, read_accepted(stream)).await {
            Some(Ok(accepted)) => accepted,
            Some(Err(e)) => {
                tracing::debug!(error = %e, "local connection preamble rejected");
                return;
            }
            None => {
                tracing::debug!("local connection sent no preamble in time");
                return;
            }
        };
        match accepted {
            Accepted::Control(stream, principal) => {
                // A control connection is a peer, and keeps its place until
                // it closes.
                let link = match accept_control(
                    stream,
                    principal,
                    Arc::clone(&self.groups),
                    self.limits.max_local_streams,
                    self.limits.max_parked_reverse,
                    self.exec.clone(),
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
                let departure = crate::conn::take_departure(&ctx);
                tracing::debug!(%reason, "local connection closed");
                depart(departure, &self.departing, move || {
                    drop(ctx);
                    drop(place);
                })
                .await;
            }
            Accepted::Transfer(token, stream, principal) => {
                // The connection keeps its place until both of its halves are
                // gone. The token names the group and the kernel says who is
                // asking; an unbound connection is dispatched nowhere
                // [0012 §4.2].
                if !admit_transfer(&self.groups, &token, &principal, stream, place)
                    && let Some(held_back) = self.refusals.pass()
                {
                    tracing::warn!(
                        held_back,
                        "local transfer connection refused: unknown or mismatched group"
                    );
                }
            }
            Accepted::Reverse(token, stream, principal) => {
                // A connection parked for fan-out, under the same admission
                // rule and the two bounds of [0012 §4.4]; it keeps its place
                // while it is parked.
                if !admit_reverse(&self.groups, &token, &principal, stream, place) {
                    tracing::debug!(
                        "local reverse connection not parked: unknown group or pool full"
                    );
                }
            }
        }
    }
}

/// Accepts `AF_UNIX` connections and serves each one.
///
/// A place is taken before the next connection is accepted. Past
/// `max_connections` live connections the surplus waits in the kernel's queue,
/// and a dialler's `connect` fails visibly once that is full, instead of the
/// connection being accepted and closed with its payload already written: a
/// sender that outruns a slow consumer loses nothing.
#[cfg(unix)]
async fn accept_unix(
    listener: tokio::net::UnixListener,
    serve: LocalAccept<crate::unix::UnixLocal>,
) {
    loop {
        // The semaphore is never closed, so this only fails if it were.
        let Ok(place) = Arc::clone(&serve.admitted).acquire_owned().await else {
            return;
        };
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                // Descriptor or memory exhaustion passes, and a socket that
                // died before it was accepted concerns nobody but its peer:
                // neither ends the binding, which would stay dead after the
                // cause was gone.
                if let Some(held_back) = serve.refusals.pass() {
                    tracing::warn!(error = %e, held_back, "accepting a unix connection failed; retrying");
                }
                serve.exec.sleep(ACCEPT_RETRY).await;
                continue;
            }
        };
        if serve.shared.drain.is_draining() {
            // Admission stopped: the same rule as on every other binding
            // (`docs/decisions/0009-drain.md` §4.5).
            continue;
        }
        // The accepted socket carries the runtime its halves write their
        // end-of-payload marker on, exactly as a pipe instance does.
        let stream = crate::unix::UnixLocal::accepted(stream, serve.exec.clone());
        serve.exec.spawn(serve.clone_for().serve(stream, place));
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
        // A place is taken before a client is accepted, for the reason
        // `accept_unix` gives: the surplus waits on the pipe, which a
        // client sees as busy, instead of being accepted and dropped.
        let place = tokio::select! {
            place = Arc::clone(&serve.admitted).acquire_owned() => match place {
                Ok(place) => place,
                Err(_) => return,
            },
            _ = &mut stop => {
                tracing::debug!("pipe binding dropped; accept loop ending");
                return;
            }
        };
        let connected = tokio::select! {
            connected = listening.connect() => connected,
            _ = &mut stop => {
                tracing::debug!("pipe binding dropped; accept loop ending");
                return;
            }
        };
        if let Err(e) = connected {
            // A failed connect leaves this instance in no state to be trusted:
            // wait, take a fresh one if one can be made, and ask again.
            if let Some(held_back) = serve.refusals.pass() {
                tracing::warn!(error = %e, held_back, "pipe accept failed; retrying");
            }
            tokio::select! {
                () = serve.exec.sleep(ACCEPT_RETRY) => {}
                _ = &mut stop => return,
            }
            if let Ok(fresh) = binding.next_instance() {
                listening = fresh;
            }
            continue;
        }
        // The next listening instance is made before the accepted one is
        // served; failing to make it is waited out, not the end of the
        // binding.
        let next = loop {
            match binding.next_instance() {
                Ok(next) => break next,
                Err(e) => {
                    if let Some(held_back) = serve.refusals.pass() {
                        tracing::warn!(error = %e, held_back, "pipe instance not created; retrying");
                    }
                    tokio::select! {
                        () = serve.exec.sleep(ACCEPT_RETRY) => {}
                        _ = &mut stop => return,
                    }
                }
            }
        };
        let accepted = std::mem::replace(&mut listening, next);
        if serve.shared.drain.is_draining() {
            // Admission stopped: the accepted instance is dropped, which
            // disconnects the client.
            continue;
        }
        let stream = crate::pipe::PipeStream::accepted(accepted, serve.exec.clone());
        serve.exec.spawn(serve.clone_for().serve(stream, place));
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
            let departure = crate::conn::take_departure(&ctx);
            tracing::debug!(%reason, "local connection closed");
            drop(ctx);
            departure.announce().await;
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

/// Every handshaken connection of a binding, anonymous ones included, so
/// [`Binding::hand_over`] can wait for each one's close.
///
/// [`PeerConns`] cannot serve here: it holds proved peers only. Keyed by
/// `stable_id()` and bounded by `max_connections`.
#[derive(Default)]
struct OpenConns(std::sync::Mutex<HashMap<usize, quinn::Connection>>);

impl OpenConns {
    fn insert(&self, conn: &quinn::Connection) {
        self.0
            .lock()
            .expect("open connections poisoned")
            .insert(conn.stable_id(), conn.clone());
    }

    fn remove(&self, conn_id: usize) {
        self.0
            .lock()
            .expect("open connections poisoned")
            .remove(&conn_id);
    }

    fn snapshot(&self) -> Vec<quinn::Connection> {
        self.0
            .lock()
            .expect("open connections poisoned")
            .values()
            .cloned()
            .collect()
    }
}

/// How often a refusal is let through to the log.
const REFUSAL_LOG_INTERVAL: Duration = Duration::from_secs(1);

/// Lets one log record through per interval and counts the ones it held back,
/// so a flood of refusals is one line a second and not one per packet.
struct LogThrottle {
    every: Duration,
    state: Mutex<ThrottleState>,
}

struct ThrottleState {
    next: Option<Instant>,
    held_back: u64,
}

impl LogThrottle {
    fn new(every: Duration) -> LogThrottle {
        LogThrottle {
            every,
            state: Mutex::new(ThrottleState {
                next: None,
                held_back: 0,
            }),
        }
    }

    /// `Some(n)` when the caller should log now, `n` being how many records
    /// were held back since the last one that passed.
    fn pass(&self) -> Option<u64> {
        self.pass_at(Instant::now())
    }

    fn pass_at(&self, now: Instant) -> Option<u64> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match state.next {
            Some(next) if now < next => {
                state.held_back += 1;
                None
            }
            _ => {
                state.next = Some(now + self.every);
                Some(std::mem::take(&mut state.held_back))
            }
        }
    }
}

/// One unit of a shared count, given back when this drops, however the task
/// that holds it ends.
struct Counted(Arc<AtomicUsize>);

impl Counted {
    fn take(counter: &Arc<AtomicUsize>) -> Counted {
        counter.fetch_add(1, Ordering::Relaxed);
        Counted(Arc::clone(counter))
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Tells the queues a closed connection consumed on that it is gone, and runs
/// `release`, which gives back everything that counted the connection.
///
/// A queue that is full and unread makes the telling wait for as long as its
/// application likes. A connection that waited holding everything it is
/// counted against would stay in every table that limits live ones: its share
/// of `max_connections`, its peer's share of `max_connections_per_peer`, a
/// local place. So `release` runs first, and what waits is a [`Departure`],
/// which holds the queues and an id and nothing of the connection.
///
/// `departing` bounds how many of those waits there are, to `max_connections`.
/// Past it the connection keeps what counts it until its queues have taken
/// the notice, which bounds the tasks at twice what the binding admits and
/// still loses no notice: a queue owes something for what it handed that
/// consumer, and has to be told.
async fn depart(
    departure: crate::conn::Departure,
    departing: &Arc<Semaphore>,
    release: impl FnOnce() + Send,
) {
    if departure.is_empty() {
        release();
        return;
    }
    match Arc::clone(departing).try_acquire_owned() {
        Ok(_wait) => {
            release();
            departure.announce().await;
        }
        Err(_) => {
            departure.announce().await;
            release();
        }
    }
}

/// Accepts connections until the endpoint is closed.
///
/// Everything it needs is already on the listener it serves: the namespace to
/// route into, the subscription registry, and the runtime whose configuration
/// decides the limits and whose executor runs the connections. Holding the
/// listener for as long as a binding accepts is the honest lifetime — a
/// binding without its namespace serves nothing.
///
/// **What an Initial costs is decided before it is accepted.** Accepting one
/// builds the connection's state and runs the server half of the TLS
/// handshake for a source that has not proved the address it wrote, so every
/// refusal below is made on the `Incoming` itself and builds nothing: a
/// binding at `max_connections` or `max_handshakes` refuses
/// (`CONNECTION_REFUSED`), and above `retry_above_handshakes` an Initial from
/// an unvalidated address is answered with a stateless Retry, so a flood from
/// spoofed sources allocates nothing and a real dialler pays one round trip.
async fn accept_connections(
    endpoint: quinn::Endpoint,
    listener: Arc<ListenerInner>,
    peers: Arc<PeerConns>,
    open: Arc<OpenConns>,
) {
    let config = &listener.runtime.config;
    let limits = config.limits;
    let max_connections = config.max_connections;
    let max_connections_per_peer = config.max_connections_per_peer;
    let max_handshakes = config.max_handshakes;
    let retry_above = config.retry_above_handshakes;
    let guarantees = config.guarantees;
    let exec = listener.runtime.exec.clone();
    let shared = listener.runtime.shared();
    let namespace = Arc::clone(&listener.namespace);
    let subs = Arc::clone(&listener.subs);
    let live = Arc::new(AtomicUsize::new(0));
    let handshaking = Arc::new(AtomicUsize::new(0));
    let refusals = Arc::new(LogThrottle::new(REFUSAL_LOG_INTERVAL));
    let departing = Arc::new(Semaphore::new(
        max_connections.clamp(1, Semaphore::MAX_PERMITS),
    ));
    while let Some(incoming) = endpoint.accept().await {
        // A draining runtime admits nothing new: the handshake is refused
        // outright rather than accepted and then closed
        // (`docs/decisions/0009-drain.md` §4.5).
        if shared.drain.is_draining() {
            incoming.refuse();
            continue;
        }
        if live.load(Ordering::Relaxed) >= max_connections {
            if let Some(held_back) = refusals.pass() {
                tracing::warn!(
                    max = max_connections,
                    held_back,
                    "connection limit reached; refusing"
                );
            }
            incoming.refuse();
            continue;
        }
        let in_progress = handshaking.load(Ordering::Relaxed);
        if in_progress >= max_handshakes {
            if let Some(held_back) = refusals.pass() {
                tracing::warn!(
                    max = max_handshakes,
                    held_back,
                    "handshake limit reached; refusing"
                );
            }
            incoming.refuse();
            continue;
        }
        if in_progress >= retry_above && !incoming.remote_address_validated() {
            // An address that has not been validated may always be asked to
            // prove itself, so the Retry cannot be refused; an Initial handed
            // back anyway is turned away rather than accepted unvalidated.
            if let Err(unretried) = incoming.retry() {
                unretried.into_incoming().refuse();
            }
            continue;
        }

        let namespace = Arc::clone(&namespace);
        let subs = Arc::clone(&subs);
        let live = Counted::take(&live);
        let handshake = Counted::take(&handshaking);
        let exec_for_conn = exec.clone();
        let shared = Arc::clone(&shared);
        let peers = Arc::clone(&peers);
        let open = Arc::clone(&open);
        let refusals = Arc::clone(&refusals);
        let departing = Arc::clone(&departing);
        exec.spawn(async move {
            let connected = incoming.await;
            drop(handshake);
            match connected {
                Ok(conn) => {
                    open.insert(&conn);
                    let remote = conn.remote_address();
                    // `quinn`'s own id: it keys the two tables below, which
                    // hold a handle on the connection and so keep the id from
                    // being handed out again. Everything that outlives the
                    // connection keys on the link's serial instead.
                    let quic_id = conn.stable_id();
                    // The per-peer ceiling can only be applied here: the
                    // identity exists once the handshake is complete, and one
                    // peer now holds one connection per endpoint path it
                    // dials, so the path count it chooses would otherwise be
                    // the only bound
                    // (`docs/decisions/0002-control-and-bulk-separation.md` §7).
                    let peer = crate::tls::peer_fingerprint(&conn);
                    if !peers.admit(peer, &conn, max_connections_per_peer) {
                        if let Some(held_back) = refusals.pass() {
                            tracing::warn!(
                                %remote,
                                max = max_connections_per_peer,
                                held_back,
                                "per-peer connection limit reached; refusing"
                            );
                        }
                        conn.close(
                            VarInt::from_u32(codes::LIMIT_EXCEEDED as u32),
                            b"per-peer connection limit reached",
                        );
                        open.remove(quic_id);
                    } else {
                        tracing::debug!(%remote, "connection accepted");
                        // The handle must outlive the connection: it owns the
                        // actor's control channel.
                        let ctx = ConnCtx::spawn(
                            Link::Quic(QuicLink::new(conn.clone())),
                            limits,
                            namespace,
                            Some(Arc::clone(&subs)),
                            exec_for_conn,
                            guarantees,
                            shared,
                        );
                        let serial = ctx.conn.stable_id();
                        let reason = conn.closed().await;
                        // A peer that goes away takes its subscriptions with
                        // it; otherwise connection churn would grow the
                        // registry.
                        subs.remove_connection(serial);
                        let departure = crate::conn::take_departure(&ctx);
                        tracing::debug!(%remote, %reason, "connection closed");
                        depart(departure, &departing, move || {
                            peers.release(peer, quic_id);
                            open.remove(quic_id);
                            drop((ctx, conn, live));
                        })
                        .await;
                    }
                }
                Err(e) => tracing::debug!(error = %e, "handshake failed"),
            }
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

#[cfg(test)]
mod accept_tests {
    use super::*;

    /// A flood of refusals is one log record per interval, and the record
    /// says how many were held back.
    #[test]
    fn a_log_throttle_passes_one_record_per_interval_and_counts_the_rest() {
        let throttle = LogThrottle::new(Duration::from_secs(1));
        let start = Instant::now();
        assert_eq!(throttle.pass_at(start), Some(0));
        for _ in 0..5 {
            assert_eq!(throttle.pass_at(start + Duration::from_millis(500)), None);
        }
        assert_eq!(
            throttle.pass_at(start + Duration::from_millis(1500)),
            Some(5)
        );
        assert_eq!(throttle.pass_at(start + Duration::from_millis(1600)), None);
    }

    /// A raw consumer is recorded per filter: a repeat is not a new
    /// subscription, a filter the connection never held frees nothing, and the
    /// path stays reportable until its last filter goes.
    #[test]
    fn a_consumer_is_recorded_per_filter_and_reported_with_its_last_path() {
        let ns = Namespace::new();
        assert!(ns.note_consumer(7, "/q", "a"));
        assert!(!ns.note_consumer(7, "/q", "a"));
        assert!(ns.note_consumer(7, "/q", "b"));
        assert!(!ns.forget_consumer(7, "/q", "never"));
        assert!(ns.forget_consumer(7, "/q", "a"));
        assert!(!ns.forget_consumer(7, "/q", "a"));

        let (tx, _rx) = mpsc::channel(1);
        ns.register("/q", Route::Raw(tx)).expect("register");
        // "b" is still held, so closing the connection reports the path.
        assert_eq!(ns.take_consumer_routes(7).len(), 1);
        assert!(ns.take_consumer_routes(7).is_empty());
    }
}
