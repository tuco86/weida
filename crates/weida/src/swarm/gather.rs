//! The dialling side of SWARM: one blob from every connected seeder at once,
//! each piece verified against the root before it is handed out
//! ([decisions/0040](../../../../docs/decisions/0040-swarm-pattern.md) §4.4,
//! [decisions/0041](../../../../docs/decisions/0041-swarm-sessions.md) §4.3).
//!
//! What to order of whom is the [`Scheduler`]'s; this module is the I/O
//! around it: one session task per seeder, which writes the orders it is
//! given and reads the answers back in order, or one task per ask toward a
//! seeder that only speaks version 1.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use weida_core::{Error, Limits};

use super::schedule::Scheduler;
use super::seed::{MIN_RATE, SEED_STALL, deadline_for};
use super::wire::{self, FRAME_HEAD_BYTES, Head};
use super::{Ask, Bits, Blob, PieceList};
use crate::config::ClientTls;
use crate::conn::ConnHandle;
use crate::reconnect::PeerEvents;
use crate::runtime::{Exec, RuntimeInner};
use crate::stream::{Peer, PeerShared, open_exchange_on};
use crate::transfer::{IncomingTransfer, OutgoingTransfer, ReplyStream, TransferMeta};

/// The least QUIC stream receive window a swarm dials with: room for a
/// seeder's pipelined answers, which quinn cannot grow after the dial.
pub(crate) const SWARM_STREAM_WINDOW: u64 = 32 << 20;

/// The least QUIC connection receive window a swarm dials with.
pub(crate) const SWARM_CONNECTION_WINDOW: u64 = 64 << 20;

/// State of a swarm.
pub struct GatherState {
    peer: Peer,
}

impl GatherState {
    /// A swarm dials QUIC with the runtime's limits, its receive windows
    /// raised to at least [`SWARM_STREAM_WINDOW`] and
    /// [`SWARM_CONNECTION_WINDOW`], so one seeder can fill a fast path
    /// without the application tuning anything. Local transports keep the
    /// runtime's limits.
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> GatherState {
        let base = runtime.config.limits;
        let limits = Limits {
            stream_receive_window: base.stream_receive_window.max(SWARM_STREAM_WINDOW),
            connection_receive_window: base.connection_receive_window.max(SWARM_CONNECTION_WINDOW),
            ..base
        };
        GatherState {
            peer: Peer::with_limits(runtime, tls, limits),
        }
    }
}

impl crate::Swarm {
    /// Connects to a seeder at `weida://host:port/path`.
    ///
    /// A swarm takes pieces from **every** seeder it is connected to, so like
    /// a surveyor it accumulates peers on purpose. Connecting during a
    /// [`Fetch`] adds the seeder to it at its next [`Fetch::next`].
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state().peer.connect(url).await
    }

    /// Forgets a dialled address; see [`Peer::disconnect`].
    pub fn disconnect(&self, url: &str) -> bool {
        self.state().peer.disconnect(url)
    }

    /// Number of connected seeders.
    pub fn peer_count(&self) -> usize {
        self.state().peer.peer_count()
    }

    /// One record per live connection, labelled by the URL as dialled; see
    /// [`Peer::connection_stats`].
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state().peer.connection_stats()
    }

    /// The event stream of this swarm's addresses; see [`Peer::events`].
    pub fn events(&self) -> PeerEvents {
        self.state().peer.events()
    }

    /// Starts fetching the pieces of `blob` that `have` lacks.
    ///
    /// `have` is what the caller already holds, so a fetch resumes where an
    /// earlier one stopped; [`FetchOptions::list`] spares it the LIST order
    /// when the caller kept the list ([`Fetch::list`]). Nothing is ordered
    /// until the first [`Fetch::next`].
    ///
    /// # Panics
    ///
    /// When `have` does not have one bit per piece of `blob`, or
    /// [`FetchOptions::list`] is the list of another blob.
    pub fn fetch(&self, blob: Blob, have: Bits, mut options: FetchOptions) -> Fetch {
        assert_eq!(
            have.len(),
            blob.count(),
            "`have` needs one bit per piece of the blob"
        );
        let list = match options.list.take() {
            Some(list) => {
                assert_eq!(list.blob(), blob, "the piece list is of another blob");
                Some(list)
            }
            // A blob of at most one piece has an empty list, checked against
            // the root.
            None if blob.count() <= 1 => PieceList::parse(blob, &[]),
            None => None,
        };
        options.per_peer = options.per_peer.max(1);
        options.max_inflight = options.max_inflight.max(1);
        options.min_rate = options.min_rate.max(1);
        let scheduler = Scheduler::new(blob, have, list.is_some(), &options);
        // Every answer event is one order's, and orders are at most
        // `max_inflight`; the one more is room for a session's end.
        let (tx, rx) = mpsc::channel(options.max_inflight + 1);
        let peer = &self.state().peer;
        Fetch {
            shared: peer.downgrade(),
            exec: peer.exec().clone(),
            events: peer.events(),
            blob,
            options,
            list: list.map(Arc::new),
            scheduler,
            views: HashMap::new(),
            dropped: HashSet::new(),
            next_session: 0,
            closing: Vec::new(),
            tx,
            rx,
        }
    }
}

/// How a [`Fetch`] orders.
#[derive(Clone, Debug)]
pub struct FetchOptions {
    /// The piece list, if the caller already holds it (from an earlier
    /// [`Fetch::list`]); otherwise one seeder is asked for it and the answer
    /// is checked against the root.
    pub list: Option<PieceList>,
    /// The fixed part of every answer's deadline. Default 30 s.
    pub stall: Duration,
    /// The rate, in bytes per second, below which a seeder's answer is too
    /// slow: the answer to an order of `n` bytes is due `stall + (9 + n) /
    /// min_rate` after it was written, or after the answer before it, which
    /// ever is later. Default 64 KiB/s; zero counts as one.
    pub min_rate: u64,
    /// The credit of one seeder: the most orders it holds at once, each from
    /// the moment it is written until the caller takes its piece from
    /// [`Fetch::next`]. A caller that stops taking pieces therefore stops
    /// the seeder after this many. How many of them are used follows the
    /// seeder's rate: two at first, one more per piece while the rate grows,
    /// then three seconds of its rate in pieces (BitTorrent's pipelining as
    /// libtorrent sizes it, 0041 §4.3). Default 8; zero counts as one.
    pub per_peer: usize,
    /// Orders over all seeders at once, which bounds the piece bytes this
    /// fetch holds at `max_inflight` × piece size. Default 32; zero counts
    /// as one.
    pub max_inflight: usize,
    /// Seeders this fetch takes pieces from; the others stay connected and
    /// unused. Default unbounded.
    pub max_peers: usize,
    /// How long a seeder with nothing this fetch can take waits before it is
    /// asked HAVE again, so a seeder that is still filling serves what it
    /// gained. Default 5 s.
    pub have_refresh: Duration,
}

impl Default for FetchOptions {
    fn default() -> FetchOptions {
        FetchOptions {
            list: None,
            stall: SEED_STALL,
            min_rate: MIN_RATE,
            per_peer: 8,
            max_inflight: 32,
            max_peers: usize::MAX,
            have_refresh: Duration::from_secs(5),
        }
    }
}

/// One verified piece of a blob.
#[derive(Clone, Debug)]
pub struct Piece {
    /// Which piece: its bytes are [`Blob::span`] of it.
    pub index: u32,
    /// The piece's bytes, checked against the blob's root.
    pub bytes: Bytes,
}

/// Which wire a seeder speaks to this fetch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wire {
    /// One session of orders (0041).
    Session,
    /// One exchange per ask (0040 §4.2): the seeder refused a session.
    Asks,
}

/// What one seeder looks like to one fetch, besides its scheduling.
struct PeerView {
    conn: ConnHandle,
    path: Arc<str>,
    wire: Wire,
    session: Option<SessionLink>,
    /// The version-1 asks in flight, by order id.
    asks: HashMap<u64, JoinHandle<()>>,
}

impl PeerView {
    fn abort(&self) {
        if let Some(session) = &self.session {
            session.handle.abort();
        }
        for handle in self.asks.values() {
            handle.abort();
        }
    }
}

/// The fetch's end of a running session task.
struct SessionLink {
    id: u64,
    orders: mpsc::Sender<SessionOrder>,
    handle: JoinHandle<()>,
}

/// One order for a session task to write.
struct SessionOrder {
    id: u64,
    ask: Ask,
    /// The list a PIECE answer is checked against.
    list: Option<Arc<PieceList>>,
}

/// A verified answer.
enum Answer {
    List(PieceList),
    Have(Bits),
    Piece(Bytes),
}

/// Why an order or a session failed, which decides what happens to its
/// seeder.
#[derive(Debug)]
enum Fault {
    /// The connection went: the seeder keeps its place, and a redial brings
    /// it back.
    Lost,
    /// A refusal, a timeout, a wrong length or bytes that do not verify: the
    /// seeder is dropped for this fetch.
    Bad,
    /// The seeder refused the session before any answer: it speaks only the
    /// version-1 ask, and is asked that way.
    Version1,
}

/// What the tasks of a fetch report.
enum Event {
    /// One order's answer, verified, or a version-1 ask's failure; `at` is
    /// when it arrived.
    Answer {
        order: u64,
        result: Result<Answer, Fault>,
        at: Instant,
    },
    /// A session task ended.
    Ended {
        url: Arc<str>,
        session: u64,
        result: Result<(), Fault>,
    },
}

/// One blob being gathered from every connected seeder.
///
/// [`Fetch::next`] yields each verified piece once, in any order. Dropping
/// the fetch aborts every order it has in flight; the pieces it yielded are
/// the caller's.
pub struct Fetch {
    shared: Weak<PeerShared>,
    exec: Exec,
    events: PeerEvents,
    blob: Blob,
    options: FetchOptions,
    list: Option<Arc<PieceList>>,
    scheduler: Scheduler,
    /// Keyed by the URL as dialled.
    views: HashMap<Arc<str>, PeerView>,
    /// Seeders that failed this fetch.
    dropped: HashSet<Arc<str>>,
    next_session: u64,
    /// Sessions told to finish once every piece was delivered, still reading
    /// the answers to their last orders.
    closing: Vec<JoinHandle<()>>,
    tx: mpsc::Sender<Event>,
    rx: mpsc::Receiver<Event>,
}

impl Fetch {
    /// The blob being fetched.
    pub fn blob(&self) -> Blob {
        self.blob
    }

    /// The piece list, once it is known and verified: keep it to resume
    /// without LIST ([`FetchOptions::list`]).
    pub fn list(&self) -> Option<&PieceList> {
        self.list.as_deref()
    }

    /// The pieces the caller holds: those it started with and those this
    /// fetch yielded.
    pub fn delivered(&self) -> &Bits {
        self.scheduler.delivered()
    }

    /// The pieces ordered of some seeder and not yet taken: on their way,
    /// or received and verified and waiting for [`Fetch::next`]. A progress
    /// display draws them between missing and held.
    pub fn in_flight(&self) -> Bits {
        self.scheduler.in_flight()
    }

    /// The next verified piece, or `None` once the caller holds every piece.
    ///
    /// Taking a piece frees its order's credit, so this is where the next
    /// order to that seeder is written: a caller that takes pieces only as
    /// fast as it can store them is all the backpressure a fetch needs.
    ///
    /// `Some(Err(Error::NotConnected))` when no order is in flight and no
    /// seeder is left that has not failed this fetch, or when the swarm is
    /// gone; connecting a seeder and calling `next` again resumes.
    ///
    /// Cancel-safe: a call dropped before it returns loses nothing, and the
    /// orders it wrote keep running.
    pub async fn next(&mut self) -> Option<Result<Piece, Error>> {
        loop {
            if self.scheduler.delivered().all() {
                self.close_sessions();
                return None;
            }
            let refresh = {
                let Some(shared) = self.shared.upgrade() else {
                    return Some(Err(Error::NotConnected));
                };
                self.sync_views(&shared);
                self.dispatch_planned();
                if self.scheduler.idle()
                    && !shared.has_slot_besides(|url| self.dropped.contains(url))
                {
                    return Some(Err(Error::NotConnected));
                }
                self.scheduler.next_refresh()
            };
            let sleep = refresh.map(|at| {
                self.exec
                    .sleep(at.saturating_duration_since(Instant::now()))
            });
            let sleep = async move {
                match sleep {
                    Some(sleep) => sleep.await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                event = self.rx.recv() => {
                    // `self` holds a sender, so the channel never closes.
                    if let Some(event) = event
                        && let Some(piece) = self.handle(event)
                    {
                        if self.scheduler.delivered().all() {
                            self.close_sessions();
                        } else {
                            self.dispatch_planned();
                        }
                        return Some(Ok(piece));
                    }
                }
                event = self.events.recv() => {
                    if event.is_none() {
                        return Some(Err(Error::NotConnected));
                    }
                }
                () = sleep => {}
            }
        }
    }

    /// Brings the views in line with the live connections: a seeder whose
    /// address is gone or whose connection was replaced loses its view and
    /// its orders, and a new one gains a view while there is room.
    fn sync_views(&mut self, shared: &PeerShared) {
        let live = shared.live_slots();
        let stale: Vec<Arc<str>> = self
            .views
            .iter()
            .filter(|(url, view)| {
                !live
                    .iter()
                    .any(|(u, conn, _)| u == *url && Arc::ptr_eq(conn, &view.conn))
            })
            .map(|(url, _)| Arc::clone(url))
            .collect();
        for url in stale {
            if let Some(view) = self.views.remove(&url) {
                view.abort();
            }
            self.scheduler.remove(&url);
        }
        for (url, conn, path) in live {
            if self.views.len() >= self.options.max_peers {
                break;
            }
            if self.dropped.contains(&url) || self.views.contains_key(&url) {
                continue;
            }
            self.scheduler.add(Arc::clone(&url));
            self.views.insert(
                url,
                PeerView {
                    conn,
                    path,
                    wire: Wire::Session,
                    session: None,
                    asks: HashMap::new(),
                },
            );
        }
    }

    /// Writes every order there is credit for: into its seeder's session,
    /// opened at its first order, or as a version-1 ask.
    fn dispatch_planned(&mut self) {
        let planned = self.scheduler.plan(Instant::now(), &mut rand::rng());
        for (id, url, ask) in planned {
            let Some(view) = self.views.get_mut(&url) else {
                self.scheduler.finish(id, None);
                continue;
            };
            match view.wire {
                Wire::Asks => {
                    let handle = spawn_ask(
                        &self.exec,
                        self.tx.clone(),
                        view,
                        self.blob,
                        &self.options,
                        id,
                        ask,
                        self.list.clone(),
                    );
                    view.asks.insert(id, handle);
                }
                Wire::Session => {
                    let link = view.session.get_or_insert_with(|| {
                        let session = self.next_session;
                        self.next_session += 1;
                        spawn_session(
                            &self.exec,
                            self.tx.clone(),
                            Arc::clone(&url),
                            session,
                            ConnHandle::clone(&view.conn),
                            Arc::clone(&view.path),
                            self.blob,
                            &self.options,
                        )
                    });
                    // Never full: the orders in it, in flight and untaken
                    // are at most this seeder's credit, which is the
                    // channel's capacity. A closed channel is a session that
                    // ended, and its end releases this order.
                    let _ = link.orders.try_send(SessionOrder {
                        id,
                        ask,
                        list: self.list.clone(),
                    });
                }
            }
        }
    }

    /// Takes one task's report in, and yields a piece if it is the first
    /// copy.
    fn handle(&mut self, event: Event) -> Option<Piece> {
        match event {
            Event::Answer { order, result, at } => {
                // An order aborted or released before its answer was taken:
                // already forgotten.
                let (url, ask) = self.scheduler.order(order)?;
                if let Some(view) = self.views.get_mut(&url) {
                    view.asks.remove(&order);
                }
                match result {
                    Ok(Answer::List(list)) => {
                        self.scheduler.finish(order, None);
                        self.list.get_or_insert_with(|| Arc::new(list));
                        self.scheduler.set_list_known();
                        None
                    }
                    Ok(Answer::Have(have)) => {
                        self.scheduler.finish(order, None);
                        self.scheduler.set_have(&url, have, Instant::now());
                        None
                    }
                    Ok(Answer::Piece(bytes)) => {
                        self.scheduler.finish(order, Some((bytes.len() as u64, at)));
                        let Ask::Piece(index) = ask else {
                            unreachable!("only a PIECE order answers with a piece");
                        };
                        let others = self.scheduler.deliver(index)?;
                        // The other copies are not needed any more. A
                        // version-1 ask is aborted; a session order cannot
                        // be taken back, so its answer arrives and is
                        // dropped as a duplicate.
                        for other in others {
                            let Some((owner, _)) = self.scheduler.order(other) else {
                                continue;
                            };
                            if let Some(handle) = self
                                .views
                                .get_mut(&owner)
                                .and_then(|view| view.asks.remove(&other))
                            {
                                handle.abort();
                                self.scheduler.finish(other, None);
                            }
                        }
                        Some(Piece { index, bytes })
                    }
                    Err(Fault::Lost) => {
                        self.scheduler.finish(order, None);
                        None
                    }
                    Err(Fault::Bad | Fault::Version1) => {
                        self.drop_seeder(&url);
                        None
                    }
                }
            }
            Event::Ended {
                url,
                session,
                result,
            } => {
                let view = self.views.get_mut(&url)?;
                if view.session.as_ref().map(|link| link.id) != Some(session) {
                    return None;
                }
                view.session = None;
                match result {
                    Ok(()) | Err(Fault::Lost) => {
                        self.scheduler.release(&url);
                    }
                    Err(Fault::Version1) => {
                        tracing::debug!(url = %url, "a seeder speaks only version-1 asks");
                        view.wire = Wire::Asks;
                        self.scheduler.release(&url);
                    }
                    Err(Fault::Bad) => self.drop_seeder(&url),
                }
                None
            }
        }
    }

    /// Drops a seeder for this fetch: its session and asks are aborted and
    /// its pieces are ordered elsewhere.
    fn drop_seeder(&mut self, url: &Arc<str>) {
        tracing::debug!(url = %url, blob = %self.blob.root(), "a seeder failed this fetch and is dropped");
        if let Some(view) = self.views.remove(url) {
            view.abort();
        }
        self.scheduler.remove(url);
        self.dropped.insert(Arc::clone(url));
    }

    /// Tells every session that no order follows: each finishes its request
    /// and reads the answers still due, which are dropped as duplicates.
    fn close_sessions(&mut self) {
        for view in self.views.values_mut() {
            if let Some(link) = view.session.take() {
                self.closing.push(link.handle);
            }
        }
    }
}

impl Drop for Fetch {
    fn drop(&mut self) {
        for view in self.views.values() {
            view.abort();
        }
        for handle in &self.closing {
            handle.abort();
        }
    }
}

impl std::fmt::Debug for Fetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fetch")
            .field("blob", &self.blob)
            .field("delivered", &self.scheduler.delivered().ones())
            .field("seeders", &self.views.len())
            .field("in_flight", &self.scheduler.in_flight().ones())
            .finish_non_exhaustive()
    }
}

/// Starts one version-1 ask of `view`'s seeder, under its deadline.
#[expect(
    clippy::too_many_arguments,
    reason = "one ask's whole context, spelled out"
)]
fn spawn_ask(
    exec: &Exec,
    tx: mpsc::Sender<Event>,
    view: &PeerView,
    blob: Blob,
    options: &FetchOptions,
    id: u64,
    ask: Ask,
    list: Option<Arc<PieceList>>,
) -> JoinHandle<()> {
    let conn = ConnHandle::clone(&view.conn);
    let path = Arc::clone(&view.path);
    let task_exec = exec.clone();
    let limit = deadline_for(options.stall, wire::reply_len(&blob, ask), options.min_rate);
    exec.spawn(async move {
        let result = task_exec
            .within(limit, ask_one(&conn, &path, blob, ask, list.as_deref()))
            .await
            .unwrap_or(Err(Fault::Bad));
        let _ = tx
            .send(Event::Answer {
                order: id,
                result,
                at: Instant::now(),
            })
            .await;
    })
}

/// Asks one seeder once and checks the answer: its exact length, and its
/// content against the root.
async fn ask_one(
    conn: &ConnHandle,
    path: &str,
    blob: Blob,
    ask: Ask,
    list: Option<&PieceList>,
) -> Result<Answer, Fault> {
    let len = wire::reply_len(&blob, ask);
    let (mut request, reply) = open_exchange_on(conn, path, TransferMeta::default())
        .await
        .map_err(fault)?;
    request
        .write_all(&wire::encode(&blob, ask))
        .await
        .map_err(fault)?;
    request.finish().map_err(fault)?;
    let mut body = reply.recv().await.map_err(fault)?;
    let bytes = body
        .read_capped(usize::try_from(len).unwrap_or(usize::MAX))
        .await
        .map_err(fault)?;
    if bytes.len() as u64 != len {
        return Err(Fault::Bad);
    }
    verify(blob, ask, bytes, list)
}

/// Checks an answer of the right length against what it claims to be: a
/// list against the root, a bit map for its padding, a piece against the
/// list.
fn verify(blob: Blob, ask: Ask, bytes: Vec<u8>, list: Option<&PieceList>) -> Result<Answer, Fault> {
    match ask {
        Ask::List => PieceList::parse(blob, &bytes)
            .map(Answer::List)
            .ok_or(Fault::Bad),
        Ask::Have => Bits::from_bytes(blob.count(), &bytes)
            .map(Answer::Have)
            .ok_or(Fault::Bad),
        Ask::Piece(index) => match list {
            Some(list) if list.check(index, &bytes) => Ok(Answer::Piece(Bytes::from(bytes))),
            _ => Err(Fault::Bad),
        },
    }
}

/// A transport failure keeps the seeder; anything else drops it.
fn fault(e: Error) -> Fault {
    match e {
        Error::ConnectionLost(_) | Error::NotConnected | Error::Indeterminate => Fault::Lost,
        _ => Fault::Bad,
    }
}

/// What a session's writer tells its reader.
enum Written {
    /// An order is on its way, written at `at`: its answer is the next one
    /// due after those before it.
    Order { order: SessionOrder, at: Instant },
    /// The writer failed.
    Failed(Error),
}

/// Starts the task of one session with one seeder.
#[expect(
    clippy::too_many_arguments,
    reason = "one session's whole context, spelled out"
)]
fn spawn_session(
    exec: &Exec,
    tx: mpsc::Sender<Event>,
    url: Arc<str>,
    session: u64,
    conn: ConnHandle,
    path: Arc<str>,
    blob: Blob,
    options: &FetchOptions,
) -> SessionLink {
    let (orders, orders_rx) = mpsc::channel(options.per_peer);
    let context = SessionContext {
        exec: exec.clone(),
        blob,
        stall: options.stall,
        min_rate: options.min_rate,
        per_peer: options.per_peer,
        tx: tx.clone(),
    };
    let handle = exec.spawn(async move {
        let result = run_session(&context, &conn, &path, orders_rx).await;
        let _ = tx
            .send(Event::Ended {
                url,
                session,
                result,
            })
            .await;
    });
    SessionLink {
        id: session,
        orders,
        handle,
    }
}

/// What a session task needs besides its connection.
struct SessionContext {
    exec: Exec,
    blob: Blob,
    stall: Duration,
    min_rate: u64,
    per_peer: usize,
    tx: mpsc::Sender<Event>,
}

/// One session: the writer writes the open and each order it is given, the
/// reader reads each answer in order and reports it. The reader decides how
/// the session ends.
async fn run_session(
    context: &SessionContext,
    conn: &ConnHandle,
    path: &str,
    orders: mpsc::Receiver<SessionOrder>,
) -> Result<(), Fault> {
    let (request, reply) = open_exchange_on(conn, path, TransferMeta::default())
        .await
        .map_err(fault)?;
    // The writer reports each order before it writes it, so this holds at
    // most the seeder's credit plus one failure.
    let (written, written_rx) = mpsc::channel(context.per_peer + 1);
    let writer = async {
        write_orders(request, context.blob, orders, written).await;
        std::future::pending::<()>().await;
    };
    tokio::select! {
        () = writer => unreachable!("the writer never finishes on its own"),
        result = read_answers(context, reply, written_rx) => result,
    }
}

/// Writes the open and every order, then finishes the request once the
/// fetch sends no more.
async fn write_orders(
    mut request: OutgoingTransfer,
    blob: Blob,
    mut orders: mpsc::Receiver<SessionOrder>,
    written: mpsc::Sender<Written>,
) {
    if let Err(e) = request.write_all(&wire::encode_open(&blob)).await {
        let _ = written.send(Written::Failed(e)).await;
        return;
    }
    while let Some(order) = orders.recv().await {
        let bytes = wire::encode_order(order.ask);
        let at = Instant::now();
        if written.send(Written::Order { order, at }).await.is_err() {
            return;
        }
        if let Err(e) = request.write_all(&bytes).await {
            let _ = written.send(Written::Failed(e)).await;
            return;
        }
    }
    if let Err(e) = request.finish() {
        let _ = written.send(Written::Failed(e)).await;
    }
}

/// Reads the answer to every written order, in order, and reports each.
/// `Ok(())` once the writer finished and every answer was read.
async fn read_answers(
    context: &SessionContext,
    reply: ReplyStream,
    mut written: mpsc::Receiver<Written>,
) -> Result<(), Fault> {
    let mut reply = Some(reply);
    let mut body: Option<IncomingTransfer> = None;
    // When the answer before was read: the next one is due a deadline after
    // that or after its order was written, whichever is later.
    let mut previous: Option<Instant> = None;
    while let Some(next) = written.recv().await {
        let (order, at) = match next {
            Written::Order { order, at } => (order, at),
            // A refusal before any answer is a seeder that does not speak
            // sessions.
            Written::Failed(Error::Rejected) if previous.is_none() => {
                return Err(Fault::Version1);
            }
            Written::Failed(e) => return Err(fault(e)),
        };
        let len = wire::reply_len(&context.blob, order.ask);
        let start = previous.map_or(at, |previous| previous.max(at));
        let due = start
            + deadline_for(
                context.stall,
                FRAME_HEAD_BYTES as u64 + len,
                context.min_rate,
            );
        let answer = context
            .exec
            .within(
                due.saturating_duration_since(Instant::now()),
                read_answer(context.blob, &mut reply, &mut body, &order, len),
            )
            .await
            .unwrap_or(Err(Fault::Bad))?;
        let _ = context
            .tx
            .send(Event::Answer {
                order: order.id,
                result: Ok(answer),
                at: Instant::now(),
            })
            .await;
        previous = Some(Instant::now());
    }
    Ok(())
}

/// Reads one answer: its head must answer `order` with `len` bytes, and the
/// bytes must verify.
async fn read_answer(
    blob: Blob,
    reply: &mut Option<ReplyStream>,
    body: &mut Option<IncomingTransfer>,
    order: &SessionOrder,
    len: u64,
) -> Result<Answer, Fault> {
    let body = match body {
        Some(body) => body,
        None => {
            let stream = reply.take().expect("the reply is awaited once");
            let opened = stream.recv().await.map_err(|e| match e {
                // Refused before any answer: a version-1 seeder.
                Error::Rejected => Fault::Version1,
                e => fault(e),
            })?;
            body.insert(opened)
        }
    };
    let mut head = Vec::with_capacity(FRAME_HEAD_BYTES);
    // A clean end with an order outstanding is a seeder that stopped
    // answering.
    if !body
        .read_exact_into(&mut head, FRAME_HEAD_BYTES)
        .await
        .map_err(fault)?
    {
        return Err(Fault::Bad);
    }
    let head: &[u8; FRAME_HEAD_BYTES] = head
        .as_slice()
        .try_into()
        .expect("read_exact_into read a whole head");
    match wire::decode_head(head) {
        Some(Head::Answer { ask, len: got }) if ask == order.ask && u64::from(got) == len => {}
        _ => return Err(Fault::Bad),
    }
    let size = usize::try_from(len).map_err(|_| Fault::Bad)?;
    let mut bytes = Vec::new();
    if !body
        .read_exact_into(&mut bytes, size)
        .await
        .map_err(fault)?
    {
        return Err(Fault::Bad);
    }
    verify(blob, order.ask, bytes, order.list.as_deref())
}
