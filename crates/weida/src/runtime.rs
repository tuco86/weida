//! The process-level runtime: configuration, the client connection pool,
//! shutdown, and the one place this crate touches the async runtime.
//!
//! Every task, timer and name lookup in `weida` goes through [`Exec`], which
//! lives in `weida-runtime` and is re-exported here. That is what lets a
//! caller drive weida from an executor that is not Tokio: `quinn` needs a
//! Tokio reactor for its sockets and timers, nothing else here does, so the
//! reactor is an implementation detail the runtime owns rather than an
//! ambient requirement on every caller.
//!
//! The reactor, the resolver and the close budget are not weida's protocol
//! wearing a runtime's clothes, so they live in `weida-runtime`: a
//! standalone implementation of a foreign protocol needs exactly them and
//! none of the rest
//! ([decisions/0013](../../../docs/decisions/0013-competitor-libraries.md)
//! §4.2).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use quinn::VarInt;
use weida_core::{EndpointAddr, Error, Limits};
use weida_protocol::codes;

use crate::config::{ClientTls, RuntimeConfig};
use crate::conn::{ConnCtx, ConnHandle};
use crate::drain::{self, DrainState, Drained};
use crate::endpoint::{
    Endpoint, PairState, Paired, PushState, Pusher, ReqState, Requester, SubState, Subscriber,
    SurveyState, Surveyor,
};
use crate::listener::Listener;
use crate::network::AddrLease;
use crate::pool::ClientPool;
use crate::stream::Peer;
use crate::transport::Link;

/// The crate's whole surface onto the async runtime: tasks, timers and DNS.
///
/// Re-exported from `weida-runtime`, where it is public and documented for a
/// consumer with no weida in the picture. `pub(crate)` here, so it is not
/// part of `weida`'s surface: every module of this crate takes its `Exec`
/// from the `ConnCtx`, `RuntimeInner` or `Namespace` it already holds.
pub(crate) use weida_runtime::{CloseBudget, Exec, OwnedReactor};

/// What every connection of one runtime shares: the counters and flags that
/// outlive any single connection.
pub(crate) struct Shared {
    /// Duplicates suppressed by any connection this runtime owns.
    pub(crate) duplicates: AtomicU64,
    /// Outgoing transfers this runtime reset because their deadline passed
    /// ([`crate::OutgoingTransfer::expire_at`]).
    pub(crate) expired: AtomicU64,
    /// Admission flag and parked receipts of [`Runtime::drain`].
    pub(crate) drain: DrainState,
}

pub(crate) struct RuntimeInner {
    pub(crate) config: RuntimeConfig,
    pub(crate) pool: ClientPool,
    pub(crate) exec: Exec,
    /// Present only for a runtime created by [`Runtime::owned`]; dropped with
    /// the last handle.
    _owned: Option<OwnedReactor>,
    /// Every QUIC endpoint this runtime owns, for shutdown, with the lease on
    /// its address.
    endpoints: Mutex<Vec<(quinn::Endpoint, AddrLease)>>,
    pub(crate) shared: Arc<Shared>,
}

impl RuntimeInner {
    pub(crate) fn track_endpoint(&self, endpoint: quinn::Endpoint, lease: AddrLease) {
        self.endpoints
            .lock()
            .expect("endpoint list poisoned")
            .push((endpoint, lease));
    }

    /// Dials, or reuses a pooled connection to, `path` on `host:port` on
    /// `tls`'s terms, accepting only `expected` when the address named a peer.
    ///
    /// The path is part of the pool key: one connection per dialled endpoint
    /// path, so two paths on one peer cannot stall each other
    /// ([decisions/0002](../../../docs/decisions/0002-control-and-bulk-separation.md) §6.2).
    /// A new connection takes `limits`, which are the runtime's except for a
    /// swarm's larger receive windows; see [`ClientPool::connect`].
    pub(crate) async fn connect(
        &self,
        addr: &EndpointAddr,
        tls: &Arc<ClientTls>,
        limits: &Limits,
    ) -> Result<ConnHandle, Error> {
        self.pool
            .connect(&self.config, limits, &self.exec, addr, tls)
            .await
    }

    /// Dials an in-process bus, with no pool and no TLS.
    ///
    /// Nothing is pooled because nothing is expensive: a local connection is
    /// a pair of channels, and each caller keeps the handle it dialled
    /// ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
    /// §4.2).
    pub(crate) async fn connect_local(&self, bus: &str) -> Result<ConnHandle, Error> {
        let conn = crate::inproc::dial(
            bus,
            self.config.limits.max_local_streams,
            self.config.limits.stream_receive_window as usize,
        )?;
        let handle = ConnCtx::spawn(
            Link::Local(conn),
            self.config.limits,
            Arc::new(crate::listener::Namespace::new()),
            None,
            self.exec.clone(),
            self.config.guarantees,
            self.shared(),
        );
        // The same rule as on QUIC: negotiation completes before the caller
        // can send anything (`docs/PROTOCOL.md` §2.3).
        handle.negotiated().await?;
        Ok(handle)
    }

    /// Dials an `AF_UNIX` socket, with no pool and no TLS.
    ///
    /// The first connection is the peer's control connection and carries the
    /// HELLO exchange; transfer connections are opened per transfer and
    /// bound to it by the group token
    /// ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.1, §4.2).
    #[cfg(unix)]
    pub(crate) async fn connect_unix(&self, socket: &str) -> Result<ConnHandle, Error> {
        let link = crate::grouped::dial::<crate::unix::UnixLocal>(
            crate::unix::UnixEndpoint {
                path: std::path::PathBuf::from(socket),
                exec: self.exec.clone(),
            },
            self.config.limits.max_local_streams,
            self.config.limits.max_parked_reverse,
        )
        .await?;
        let handle = ConnCtx::spawn(
            Link::Unix(Box::new(link)),
            self.config.limits,
            Arc::new(crate::listener::Namespace::new()),
            None,
            self.exec.clone(),
            self.config.guarantees,
            self.shared(),
        );
        handle.negotiated().await?;
        Ok(handle)
    }

    /// Dials a named pipe, with no pool and no TLS: the same grouping as on
    /// `AF_UNIX`, over `\\.\pipe\<name>` [0012 §4.1, §4.2].
    #[cfg(windows)]
    pub(crate) async fn connect_pipe(
        &self,
        addr: &weida_core::PipeAddr,
    ) -> Result<ConnHandle, Error> {
        let link = crate::grouped::dial::<crate::pipe::PipeStream>(
            crate::pipe::PipeEndpoint {
                path: addr.os_path().into(),
                exec: self.exec.clone(),
            },
            self.config.limits.max_local_streams,
            self.config.limits.max_parked_reverse,
        )
        .await?;
        let handle = ConnCtx::spawn(
            Link::Pipe(Box::new(link)),
            self.config.limits,
            Arc::new(crate::listener::Namespace::new()),
            None,
            self.exec.clone(),
            self.config.guarantees,
            self.shared(),
        );
        handle.negotiated().await?;
        Ok(handle)
    }

    /// The state handed to every connection this runtime owns.
    pub(crate) fn shared(&self) -> Arc<Shared> {
        Arc::clone(&self.shared)
    }
}

/// A process-level execution and resource container.
///
/// Cloning shares the same pool, limits and bindings. Every task, timer and
/// name lookup the runtime needs goes through the Tokio handle it holds, so
/// only the runtime needs a reactor: a caller may drive weida futures on any
/// executor, including `futures::executor::block_on`.
///
/// Three ways to get one, differing only in where the reactor comes from:
/// [`Runtime::new`] borrows the ambient one, [`Runtime::with_handle`] takes a
/// handle to somebody else's, and [`Runtime::owned`] creates and owns one.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<RuntimeInner>,
}

impl Runtime {
    /// Creates a runtime on the current Tokio reactor.
    ///
    /// Fails when there is none: `quinn` needs one, and failing here beats
    /// failing later at an unrelated call site.
    pub fn new(config: RuntimeConfig) -> Result<Runtime, Error> {
        Ok(Runtime::from_parts(config, Exec::current()?, None))
    }

    /// Creates a runtime on the Tokio runtime `handle` names.
    ///
    /// For a process that already runs a reactor somewhere other than the
    /// calling thread: nothing has to be ambient, and the caller's own
    /// executor is never consulted.
    pub fn with_handle(handle: tokio::runtime::Handle, config: RuntimeConfig) -> Runtime {
        Runtime::from_parts(config, Exec::from_handle(handle), None)
    }

    /// Creates a runtime that **owns** a multi-thread Tokio runtime with
    /// [`RuntimeConfig::worker_threads`] workers.
    ///
    /// The caller needs no reactor of its own, now or later: weida's tasks,
    /// timers and name lookups run on the owned runtime while the caller
    /// drives the futures it awaits on whatever executor it likes. The owned
    /// runtime is shut down when the last clone of this `Runtime` — and every
    /// endpoint made from it — is dropped.
    ///
    /// Fails when `worker_threads` is `0`, or when the OS refuses the threads.
    pub fn owned(config: RuntimeConfig) -> Result<Runtime, Error> {
        let (exec, reactor) = Exec::owned(config.worker_threads, "weida")?;
        Ok(Runtime::from_parts(config, exec, Some(reactor)))
    }

    fn from_parts(config: RuntimeConfig, exec: Exec, owned: Option<OwnedReactor>) -> Runtime {
        // One set of counters and flags per runtime: the pool hands it to the
        // connections it dials, the listener to the connections it accepts.
        let shared = Arc::new(Shared {
            duplicates: AtomicU64::new(0),
            expired: AtomicU64::new(0),
            drain: DrainState::new(),
        });
        Runtime {
            inner: Arc::new(RuntimeInner {
                pool: ClientPool::new(Arc::clone(&shared)),
                config,
                exec,
                _owned: owned,
                endpoints: Mutex::new(Vec::new()),
                shared,
            }),
        }
    }

    /// Duplicates suppressed by every connection this runtime owns.
    ///
    /// The counterpart of `Publisher::dropped` on the receiving side: a
    /// message the peer sent twice inside the negotiated
    /// `Deduplication::Bounded` window was read, thrown away and counted
    /// here, and the application never saw it. Zero unless deduplication is
    /// negotiated, because nothing is suppressed without it.
    pub fn suppressed_duplicates(&self) -> u64 {
        self.inner.shared.duplicates.load(Ordering::Relaxed)
    }

    /// Outgoing transfers reset because their deadline passed before the peer
    /// acknowledged every byte
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.3). Counted apart from a cancel, which the wire cannot tell apart.
    pub fn expired_transfers(&self) -> u64 {
        self.inner.shared.expired.load(Ordering::Relaxed)
    }

    /// Creates an empty messaging namespace.
    ///
    /// Nothing is bound and no credentials are needed yet: a Listener is a
    /// namespace, and server identity belongs to the individual bindings
    /// ([`Listener::bind_quic`]).
    pub fn listener(&self) -> Listener {
        Listener::new(Arc::clone(&self.inner))
    }

    /// Creates a raw L0 peer that dials on `tls`'s terms.
    ///
    /// Below the patterns: a [`Peer`] opens unidirectional and bidirectional
    /// streams directly, with exactly QUIC's guarantees and no pattern
    /// vocabulary layered on top.
    ///
    /// `tls` may be a bare [`crate::Trust`] when the endpoint dials
    /// anonymously, or a [`ClientTls`] when it also presents an identity.
    pub fn peer(&self, tls: impl Into<ClientTls>) -> Peer {
        Peer::new(Arc::clone(&self.inner), Arc::new(tls.into()))
    }

    /// Creates a requester that dials on `tls`'s terms.
    ///
    /// Trust belongs to the dialling endpoint, not to the runtime: one process
    /// may legitimately talk to an internal service behind an internal CA and
    /// to a public one, and it should not need two runtimes to do so. An
    /// endpoint may still dial many peers — they simply share these terms.
    pub fn requester(&self, tls: impl Into<ClientTls>) -> Requester {
        Endpoint::from_state(ReqState::new(Arc::clone(&self.inner), Arc::new(tls.into())))
    }

    /// Creates a pusher that dials on `tls`'s terms.
    pub fn pusher(&self, tls: impl Into<ClientTls>) -> Pusher {
        Endpoint::from_state(PushState::new(
            Arc::clone(&self.inner),
            Arc::new(tls.into()),
        ))
    }

    /// Creates a subscriber that dials on `tls`'s terms.
    ///
    /// Inbound published messages queue up to `Limits::endpoint_queue`; a
    /// subscriber that stops reading therefore stalls its own delivery and,
    /// once the publisher's byte budget for it is exhausted, starts losing
    /// messages rather than slowing the publisher down.
    pub fn subscriber(&self, tls: impl Into<ClientTls>) -> Subscriber {
        Endpoint::from_state(SubState::new(
            Arc::clone(&self.inner),
            Arc::new(tls.into()),
            self.inner.config.endpoint_queue,
        ))
    }

    /// Creates a dish that dials on `tls`'s terms
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
    ///
    /// Arrived segments queue up to `endpoint_queue`; a dish that stops
    /// reading loses segments at the radio — superseded, expired or over its
    /// budget — and never stalls it, and one whose queue is full discards on
    /// arrival.
    pub fn dish(&self, tls: impl Into<ClientTls>) -> crate::Dish {
        Endpoint::from_state(crate::radio::DishState::new(
            Arc::clone(&self.inner),
            Arc::new(tls.into()),
            self.inner.config.endpoint_queue,
        ))
    }

    /// Creates a dialling paired endpoint on `tls`'s terms.
    ///
    /// The other half is [`crate::Listener::pair`]. PAIR is symmetric above
    /// the connection — both sides send and receive one-way transfers — so
    /// the split here is only about who dials, exactly as it is for Sub
    /// against Pub.
    pub fn pair(&self, tls: impl Into<ClientTls>) -> Paired {
        Endpoint::from_state(PairState::dialling(
            Arc::clone(&self.inner),
            Arc::new(tls.into()),
            self.inner.config.endpoint_queue,
        ))
    }

    /// Creates a surveyor that dials on `tls`'s terms.
    ///
    /// Unlike a requester, a surveyor uses **every** peer it connected to:
    /// one exchange each, per survey.
    pub fn surveyor(&self, tls: impl Into<ClientTls>) -> Surveyor {
        Endpoint::from_state(SurveyState::new(
            Arc::clone(&self.inner),
            Arc::new(tls.into()),
        ))
    }

    /// Creates a SWARM fetcher that dials seeders on `tls`'s terms
    /// ([decisions/0040](../../../docs/decisions/0040-swarm-pattern.md),
    /// [decisions/0041](../../../docs/decisions/0041-swarm-sessions.md)).
    ///
    /// Like a surveyor, a swarm uses **every** seeder it connected to: one
    /// [`crate::Fetch`] takes pieces from all of them at once. Its QUIC
    /// connections are dialled with this runtime's limits, the receive
    /// windows raised to at least 32 MiB per stream and 64 MiB per
    /// connection, so one seeder's pipelined answers can fill a fast path.
    #[cfg(feature = "swarm")]
    pub fn swarm(&self, tls: impl Into<ClientTls>) -> crate::Swarm {
        Endpoint::from_state(crate::swarm::GatherState::new(
            Arc::clone(&self.inner),
            Arc::new(tls.into()),
        ))
    }

    /// Closes every binding and pooled connection, then waits — for at most
    /// [`RuntimeConfig::shutdown_timeout`] — for the sockets to go idle, so
    /// peers see a clean `SHUTDOWN` rather than a timeout.
    ///
    /// The close is **abortive**: a transfer still in flight is reset, and one
    /// whose FIN is queued but unacknowledged may never arrive. That is what
    /// `Runtime::shutdown` has always meant
    /// (`docs/decisions/0009-drain.md` §4.1); giving a finished transfer its
    /// chance is a drain, which is a different operation.
    ///
    /// The wait is bounded on purpose. Without a bound its length is decided
    /// by the path — QUIC's closing and draining periods last about three
    /// times the current probe timeout, which grows with round-trip time and
    /// loss — so a process that must exit within a budget of its own could not
    /// use it [0009 §4.4].
    pub async fn shutdown(self) {
        let budget = CloseBudget::start(self.inner.config.shutdown_timeout);
        self.close(budget).await;
    }

    /// Stops admitting work, gives the transfers that were already
    /// [`crate::OutgoingTransfer::finish`]ed until `deadline` to reach the
    /// peer's transport, and then performs the same close as
    /// [`Runtime::shutdown`].
    ///
    /// This is the counterpart of that abortive close
    /// (`docs/decisions/0009-drain.md` §4.1). In order:
    ///
    /// 1. Admission stops. Bindings accept no new connection, and a new
    ///    inbound stream on an existing connection is refused with
    ///    `SHUTDOWN` [0009 §4.5]. Nothing is sent to announce it: a peer that
    ///    had to be told would have to answer, and that would be an
    ///    application acknowledgement [0009 §4.8].
    /// 2. Finished transfers are awaited to their **transport** receipt —
    ///    the condition [`crate::Delivery::delivered`] reports, and no more. A
    ///    drained transfer may still be discarded by the peer's application
    ///    (`docs/decisions/0005-refusal-race.md` §4.2); there is no
    ///    `Processed` at L0 [0009 §4.2]. A transfer that was never finished
    ///    is not part of the drain: the application still owns it.
    /// 3. The close runs, bounded by whatever is left of `deadline`.
    ///
    /// The deadline is mandatory and finite on purpose: an infinite drain is
    /// a hang with a rationale, which is the one thing every protocol in the
    /// catalogue warns about [0009 §4.3].
    ///
    /// Returns what this drain achieved, as counts. An expired drain with a
    /// non-zero [`Drained::outstanding`] is **not an error** — it is a number
    /// to log, retry against or ignore [0009 §4.6].
    pub async fn drain(self, deadline: Duration) -> Drained {
        // One budget for the drain and the close that follows it: nothing
        // about process exit may depend on a peer's behaviour [0009 §4.4].
        let budget = CloseBudget::start(deadline);
        self.inner.shared.drain.begin();

        let (receipts, evicted) = self.inner.shared.drain.take();
        let wait = self.inner.exec.sleep(budget.remaining());
        let drained = drain::wait_for(receipts, evicted, wait).await;

        self.close(budget).await;
        drained
    }

    /// Closes every binding and pooled connection and waits, for whatever is
    /// left of `budget`, for the sockets to go idle.
    async fn close(self, budget: CloseBudget) {
        let (endpoints, leases): (Vec<quinn::Endpoint>, Vec<AddrLease>) = self
            .inner
            .endpoints
            .lock()
            .expect("endpoint list poisoned")
            .drain(..)
            .collect();
        let client = self.inner.pool.take_endpoint().await;

        for endpoint in endpoints.iter().chain(client.iter()) {
            endpoint.close(
                VarInt::from_u32(codes::SHUTDOWN as u32),
                b"runtime shutting down",
            );
        }
        // A local connection has no endpoint to close, so the connections
        // themselves are the only handle: closing the registered links is
        // what makes a local peer see the shutdown
        // (`docs/decisions/0010-local-transport.md` §4.2).
        self.inner
            .shared
            .drain
            .close_all(codes::SHUTDOWN, "runtime shutting down");

        // One budget for the whole close, not one per endpoint: what a
        // caller cares about is when the call returns.
        let idle = async {
            for endpoint in endpoints.iter().chain(client.iter()) {
                endpoint.wait_idle().await;
            }
        };
        let remaining = budget.remaining();
        let deadline = self.inner.exec.sleep(remaining);
        tokio::select! {
            () = idle => {}
            () = deadline => tracing::debug!(
                timeout_ms = remaining.as_millis(),
                "timeout reached before the sockets went idle"
            ),
        }
        // The simulated addresses are free once their sockets went idle or
        // the budget ran out, as an OS frees a closed socket's port.
        for lease in &leases {
            lease.release();
        }
    }

    /// The configuration this runtime was created with.
    pub fn config(&self) -> &RuntimeConfig {
        &self.inner.config
    }

    /// The executor this runtime drives its work on.
    ///
    /// Exposed for one caller and stated so that it is not mistaken for an
    /// invitation: a **binding** needs it. `weida-py`'s asyncio bridge drives
    /// this crate's futures on the runtime that owns the connections, and a
    /// bridge that built an executor of its own would put two Tokio runtimes
    /// in one process — the cost
    /// [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.2
    /// names for not sharing one. Application code has no use for it: every
    /// future this crate returns may be driven on any executor, which is the
    /// contract `Exec` exists to keep.
    pub fn exec(&self) -> &Exec {
        &self.inner.exec
    }
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("limits", &self.inner.config.limits)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClientTls, Identity, Trust};
    use tokio::runtime::Handle;
    use weida_core::Limits;

    /// An endpoint that trusts only what an address names; a plain address
    /// under it must fail before any packet is sent.
    fn no_trust() -> ClientTls {
        ClientTls::new(Trust::by_address())
    }

    #[test]
    fn creating_a_runtime_without_a_reactor_fails() {
        let err = Runtime::new(RuntimeConfig::default()).unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    #[test]
    fn zero_worker_threads_is_rejected() {
        let err = Runtime::owned(RuntimeConfig {
            worker_threads: 0,
            ..RuntimeConfig::default()
        })
        .unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    /// The whole point of `owned`: no ambient reactor, on this thread or any
    /// other, and weida still binds a socket and shuts it down.
    #[test]
    fn an_owned_runtime_needs_no_ambient_reactor() {
        assert!(Handle::try_current().is_err());
        let rt = Runtime::owned(RuntimeConfig::default()).expect("owned runtime");
        let listener = rt.listener();
        futures::executor::block_on(async {
            let binding = listener
                .bind_quic(
                    "127.0.0.1:0".parse().unwrap(),
                    Identity::generate().expect("identity"),
                )
                .await
                .expect("bind");
            assert_ne!(binding.local_addr().port(), 0);
            rt.shutdown().await;
        });
    }

    /// `with_handle` takes somebody else's reactor; the calling thread still
    /// has none.
    #[test]
    fn with_handle_uses_the_handed_runtime() {
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let rt = Runtime::with_handle(tokio_rt.handle().clone(), RuntimeConfig::default());
        assert!(Handle::try_current().is_err());
        assert_eq!(rt.config().worker_threads, 1);
        assert_eq!(rt.requester(no_trust()).peer_count(), 0);
    }

    #[tokio::test]
    async fn a_runtime_clone_shares_its_configuration() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let clone = rt.clone();
        assert_eq!(clone.config().limits, rt.config().limits);
        assert_eq!(clone.requester(no_trust()).peer_count(), 0);
        rt.shutdown().await;
    }

    /// Claim: a hostname whose first address is unreachable still connects,
    /// because the dialling path tries the rest.
    ///
    /// This is the debt entry that said "use the IP literal until address
    /// selection learns to try more than one". On a host where `localhost`
    /// resolves to `::1` before `127.0.0.1` — the common Linux ordering, and
    /// this machine's — a server bound to `127.0.0.1` was simply unreachable
    /// by name. The address is pinned so trust does not depend on the name: a
    /// pin consults no hostname.
    #[tokio::test]
    async fn a_hostname_whose_first_address_is_unreachable_still_connects() {
        let identity = Identity::generate().expect("identity");
        let fingerprint = identity.fingerprint().expect("fingerprint");

        let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
        let listener = server.listener();
        let binding = listener
            .bind_quic("127.0.0.1:0".parse().unwrap(), identity)
            .await
            .expect("bind");
        let _puller = listener.puller("/sink").expect("puller");
        let url = format!(
            "weida://{fingerprint}@localhost:{}/sink",
            binding.local_addr().port()
        );

        let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
        let pusher = client.pusher(Trust::by_address());
        pusher.connect(&url).await.expect("connect by name");
        assert_eq!(pusher.peer_count(), 1);

        client.shutdown().await;
        server.shutdown().await;
    }

    /// Claim: `shutdown`'s wait for idle sockets is bounded by
    /// `shutdown_timeout`, so process exit never waits on somebody else's
    /// network.
    ///
    /// The peer is made unreachable the hard way: the server runtime is
    /// dropped without being shut down, which closes its socket and sends
    /// nothing, so the client's `CONNECTION_CLOSE` is answered by silence and
    /// the close runs out its draining period — about three times the path's
    /// probe timeout, ~96 ms on loopback and longer as round-trip time and
    /// loss grow.
    ///
    /// The assertion is **relative**, and measured in the same run: the same
    /// shutdown with a 1 ms cap must be at least twice as fast as one with a
    /// cap far beyond the draining period. An absolute millisecond bound would
    /// pin this machine; an unbounded wait — the defect this defends against —
    /// makes the two times equal and fails it.
    #[tokio::test]
    async fn the_wait_for_idle_sockets_is_bounded() {
        async fn shutdown_with(cap: Duration) -> Duration {
            let identity = Identity::generate().expect("identity");
            let trust = Trust::pin(identity.fingerprint().expect("fingerprint"));

            let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
            let listener = server.listener();
            let binding = listener
                .bind_quic("127.0.0.1:0".parse().unwrap(), identity)
                .await
                .expect("bind");
            let url = format!("weida://127.0.0.1:{}/sink", binding.local_addr().port());
            let puller = listener.puller("/sink").expect("puller");

            let client = Runtime::new(RuntimeConfig {
                shutdown_timeout: cap,
                ..RuntimeConfig::default()
            })
            .expect("client runtime");
            let pusher = client.pusher(trust);
            pusher.connect(&url).await.expect("connect");

            // The peer stops existing without saying so.
            drop(pusher);
            drop(puller);
            drop(binding);
            drop(listener);
            drop(server);

            let start = std::time::Instant::now();
            client.shutdown().await;
            start.elapsed()
        }

        let capped = shutdown_with(Duration::from_millis(1)).await;
        let uncapped = shutdown_with(Duration::from_secs(10)).await;
        assert!(
            capped * 2 < uncapped,
            "the cap must bite: capped {capped:?} against uncapped {uncapped:?}"
        );
    }

    /// Claim: `Binding::close` waits for its socket to go idle for at most
    /// `shutdown_timeout`, so closing one binding is not held up by its
    /// connections' closing periods.
    ///
    /// The client stays connected and answers the close, but the binding's
    /// side of the connection still runs out QUIC's closing period, about
    /// three times the path's probe timeout. The assertion is relative for
    /// the reason `the_wait_for_idle_sockets_is_bounded` gives.
    #[tokio::test]
    async fn a_binding_close_is_bounded_by_the_shutdown_timeout() {
        async fn close_with(cap: Duration) -> Duration {
            let identity = Identity::generate().expect("identity");
            let trust = Trust::pin(identity.fingerprint().expect("fingerprint"));

            let server = Runtime::new(RuntimeConfig {
                shutdown_timeout: cap,
                ..RuntimeConfig::default()
            })
            .expect("server runtime");
            let listener = server.listener();
            let binding = listener
                .bind_quic("127.0.0.1:0".parse().unwrap(), identity)
                .await
                .expect("bind");
            let url = format!("weida://127.0.0.1:{}/sink", binding.local_addr().port());
            let puller = listener.puller("/sink").expect("puller");

            let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
            let pusher = client.pusher(trust);
            pusher.connect(&url).await.expect("connect");

            let start = std::time::Instant::now();
            binding.close().await;
            let elapsed = start.elapsed();

            drop(pusher);
            drop(puller);
            drop(binding);
            drop(listener);
            client.shutdown().await;
            server.shutdown().await;
            elapsed
        }

        let capped = close_with(Duration::from_millis(1)).await;
        let uncapped = close_with(Duration::from_secs(10)).await;
        assert!(
            capped * 2 < uncapped,
            "the cap must bite: capped {capped:?} against uncapped {uncapped:?}"
        );
    }

    #[tokio::test]
    async fn a_binding_with_unreadable_tls_material_fails_before_serving() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        // Creating the namespace needs no credentials at all.
        let listener = rt.listener();
        let err = listener
            .bind_quic(
                "127.0.0.1:0".parse().unwrap(),
                Identity::from_pem_files("/nonexistent/c.pem", "/nonexistent/k.pem"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[tokio::test]
    async fn opening_without_a_peer_fails_with_not_connected() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let requester = rt.requester(no_trust());
        let err = requester
            .open(crate::TransferMeta::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotConnected), "{err:?}");
    }

    #[tokio::test]
    async fn connecting_without_trust_fails_unless_the_address_names_the_peer() {
        // Short idle timeout: the second dial goes to a port nobody answers
        // on, and QUIC gives up only when the handshake idles out.
        let rt = Runtime::new(RuntimeConfig {
            limits: Limits {
                idle_timeout: std::time::Duration::from_millis(200),
                ..Limits::default()
            },
            ..RuntimeConfig::default()
        })
        .unwrap();
        // An endpoint with nothing to trust cannot authenticate anyone, so
        // dialling a plain address fails rather than trusting whatever answers.
        let requester = rt.requester(no_trust());
        let err = requester
            .connect("weida://127.0.0.1:1/x")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");

        // With a fingerprint in the address there is something to check, so
        // the dial proceeds — and fails on the socket, since nothing listens.
        let err = requester
            .connect("weida://sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08@127.0.0.1:1/x")
            .await
            .unwrap_err();
        assert!(!matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_malformed_url_is_rejected_before_dialling() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let err = rt
            .requester(no_trust())
            .connect("http://127.0.0.1:7443/x")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    }
}
