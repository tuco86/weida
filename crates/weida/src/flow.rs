//! Datagram flows: the third carrier
//! ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.2,
//! §4.4).
//!
//! A flow is registered once, on a unidirectional stream of kind `7` FLOW,
//! and that stream is the flow's lifetime: FIN closes it, `RESET_STREAM`
//! abandons it, `STOP_SENDING` refuses it. Its units travel as QUIC DATAGRAM
//! frames prefixed with the flow id (`docs/PROTOCOL.md` §6.9); on a local
//! transport, which has no datagrams, they travel on the FLOW stream itself as
//! `varint length || bytes` records (§2.1).
//!
//! Three rules shape everything below, and each is a bound:
//!
//! * **Sending never waits.** [`Flow::send`] is synchronous. On QUIC it hands
//!   the datagram to `quinn`, which discards the oldest queued ones beyond
//!   `datagram_send_bytes`; on a local transport it pushes into a drop-oldest
//!   ring of the same size that one writer task drains.
//! * **Receiving never blocks the connection.** One reader per connection
//!   demultiplexes into a per-flow ring of `flow_queue_bytes`; a consumer that
//!   falls behind loses its oldest datagrams, counted per flow.
//! * **An unknown id costs one bounded ring.** A datagram can overtake its
//!   FLOW header, so datagrams naming no live flow wait in one
//!   per-connection ring of `flow_early_bytes` for at most `flow_early_hold`,
//!   and are dropped and counted after that — never a protocol error.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::{Notify, oneshot};
use weida_core::{Error, Limits, LossCause, PeerIdentity, TraceContext};
use weida_protocol::{
    FlowHeader, FrameKind, LOCAL_MAX_DATAGRAM, codes, decode_varint, encode_frame, encode_varint,
    flow_prefix, split_flow_datagram, varint,
};

use crate::PathStats;
use crate::conn::{ConnHandle, Wanted, refusal_for, violation};
use crate::listener::Route;
use crate::stream::{Incoming, PeerShared};
use crate::transport::{RecvHalf, SendHalf};

/// What a sender attaches to a flow, once, when it registers it.
///
/// The FLOW header's optional keys (`docs/PROTOCOL.md` §6.8); the receiver
/// reads them back as [`FlowInfo`].
#[derive(Clone, Debug, Default)]
pub struct FlowMeta {
    content_type: Option<String>,
    trace: Option<TraceContext>,
    topic: Option<String>,
}

impl FlowMeta {
    /// An opaque media type label, at most 256 bytes.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> FlowMeta {
        self.content_type = Some(content_type.into());
        self
    }

    /// The trace context the flow belongs to.
    pub fn with_trace(mut self, trace: TraceContext) -> FlowMeta {
        self.trace = Some(trace);
        self
    }

    /// The topic the flow carries, at most 256 bytes.
    pub fn with_topic(mut self, topic: impl Into<String>) -> FlowMeta {
        self.topic = Some(topic.into());
        self
    }
}

/// What the receiver learns about a flow when it arrives.
#[derive(Clone, Debug)]
pub struct FlowInfo {
    /// The path the flow was addressed to.
    pub endpoint: String,
    /// The topic the sender named, if any.
    pub topic: Option<String>,
    /// The media type label the sender named, if any.
    pub content_type: Option<String>,
    /// The trace context the sender named, if any.
    pub trace: Option<TraceContext>,
    /// Who sent it, as proved in the handshake; `None` in process or for an
    /// anonymous client.
    pub peer: Option<PeerIdentity>,
    /// The certificate chain the peer presented behind `peer`, leaf first
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.2). `None` when `peer` is not a key, and when the chain exceeded
    /// the bound weida keeps.
    pub peer_chain: Option<crate::PeerChain>,
}

/// Counters of one flow, on the side that holds it.
///
/// A sender fills `sent`, `too_large`, `discarded` and `not_live`; a receiver
/// fills `received` and `overflow`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlowStats {
    /// Datagrams handed to the transport.
    pub sent: u64,
    /// Datagrams refused by `send` because they exceeded the largest datagram
    /// the connection carries.
    pub too_large: u64,
    /// Queued datagrams discarded oldest-first before they were written.
    /// Counted on the local transports only: on QUIC the discard happens
    /// inside `quinn`, which reports none per flow.
    pub discarded: u64,
    /// Datagrams dropped because no connection was live
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.10).
    pub not_live: u64,
    /// Datagrams that arrived for this flow.
    pub received: u64,
    /// Arrived datagrams dropped, oldest first, because the consumer had
    /// `flow_queue_bytes` unread.
    pub overflow: u64,
}

#[derive(Default)]
struct Counters {
    sent: AtomicU64,
    too_large: AtomicU64,
    discarded: AtomicU64,
    not_live: AtomicU64,
    received: AtomicU64,
    overflow: AtomicU64,
}

impl Counters {
    fn add(counter: &AtomicU64, n: u64) {
        if n > 0 {
            counter.fetch_add(n, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> FlowStats {
        FlowStats {
            sent: self.sent.load(Ordering::Relaxed),
            too_large: self.too_large.load(Ordering::Relaxed),
            discarded: self.discarded.load(Ordering::Relaxed),
            not_live: self.not_live.load(Ordering::Relaxed),
            received: self.received.load(Ordering::Relaxed),
            overflow: self.overflow.load(Ordering::Relaxed),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What each queued datagram costs beyond its payload: the `Bytes` handle
/// the queue stores. Charging it is what keeps a flood of empty datagrams
/// bounded by the same byte cap as a flood of full ones.
const ENTRY_OVERHEAD: usize = std::mem::size_of::<Bytes>();

/// A drop-oldest queue of datagrams bounded in bytes.
struct Ring {
    items: VecDeque<Bytes>,
    bytes: usize,
    cap: usize,
}

impl Ring {
    fn new(cap: usize) -> Ring {
        Ring {
            items: VecDeque::new(),
            bytes: 0,
            cap,
        }
    }

    /// Queues `datagram`, dropping the oldest until it fits; returns how many
    /// were dropped, counting `datagram` itself when it alone exceeds the cap.
    fn push(&mut self, datagram: Bytes) -> u64 {
        let cost = datagram.len() + ENTRY_OVERHEAD;
        if cost > self.cap {
            return 1;
        }
        let mut dropped = 0;
        while self.bytes + cost > self.cap {
            let Some(old) = self.items.pop_front() else {
                break;
            };
            self.bytes -= old.len() + ENTRY_OVERHEAD;
            dropped += 1;
        }
        self.bytes += cost;
        self.items.push_back(datagram);
        dropped
    }

    fn pop(&mut self) -> Option<Bytes> {
        let datagram = self.items.pop_front()?;
        self.bytes -= datagram.len() + ENTRY_OVERHEAD;
        Some(datagram)
    }
}

/// What one held early datagram costs beyond its payload.
const EARLY_OVERHEAD: usize = std::mem::size_of::<(Instant, u64, Bytes)>();

/// Datagrams that named no live flow, waiting for their FLOW header.
struct EarlyRing {
    items: VecDeque<(Instant, u64, Bytes)>,
    bytes: usize,
    cap: usize,
    hold: Duration,
}

impl EarlyRing {
    fn new(cap: usize, hold: Duration) -> EarlyRing {
        EarlyRing {
            items: VecDeque::new(),
            bytes: 0,
            cap,
            hold,
        }
    }

    fn pop_front(&mut self) {
        if let Some((_, _, old)) = self.items.pop_front() {
            self.bytes -= old.len() + EARLY_OVERHEAD;
        }
    }

    /// Drops every entry older than the hold; returns how many.
    fn evict(&mut self, now: Instant) -> u64 {
        let mut evicted = 0;
        while let Some((at, _, _)) = self.items.front() {
            if now.saturating_duration_since(*at) <= self.hold {
                break;
            }
            self.pop_front();
            evicted += 1;
        }
        evicted
    }

    /// Holds `datagram` for flow `id`; returns how many held datagrams were
    /// dropped, by age or by the cap, including `datagram` itself when it
    /// alone exceeds the cap.
    fn push(&mut self, now: Instant, id: u64, datagram: Bytes) -> u64 {
        let mut dropped = self.evict(now);
        let cost = datagram.len() + EARLY_OVERHEAD;
        if cost > self.cap {
            return dropped + 1;
        }
        while self.bytes + cost > self.cap && !self.items.is_empty() {
            self.pop_front();
            dropped += 1;
        }
        self.bytes += cost;
        self.items.push_back((now, id, datagram));
        dropped
    }

    /// Takes the held datagrams of flow `id`, oldest first; the second value
    /// is how many aged out on the way.
    fn adopt(&mut self, now: Instant, id: u64) -> (Vec<Bytes>, u64) {
        let evicted = self.evict(now);
        let mut adopted = Vec::new();
        let mut kept = VecDeque::with_capacity(self.items.len());
        for (at, owner, datagram) in self.items.drain(..) {
            if owner == id {
                self.bytes -= datagram.len() + EARLY_OVERHEAD;
                adopted.push(datagram);
            } else {
                kept.push_back((at, owner, datagram));
            }
        }
        self.items = kept;
        (adopted, evicted)
    }
}

/// The receiving side of one flow, shared between the connection's readers
/// and the application's [`IncomingFlow`].
struct Inbound {
    ring: Mutex<Ring>,
    ready: Notify,
    ended: AtomicBool,
    stop: Mutex<Option<u64>>,
    stop_signal: Notify,
    counters: Counters,
}

impl Inbound {
    fn new(cap: usize) -> Inbound {
        Inbound {
            ring: Mutex::new(Ring::new(cap)),
            ready: Notify::new(),
            ended: AtomicBool::new(false),
            stop: Mutex::new(None),
            stop_signal: Notify::new(),
            counters: Counters::default(),
        }
    }

    fn push(&self, datagram: Bytes) {
        let dropped = lock(&self.ring).push(datagram);
        Counters::add(&self.counters.received, 1);
        Counters::add(&self.counters.overflow, dropped);
        self.ready.notify_waiters();
    }

    fn end(&self) {
        self.ended.store(true, Ordering::Release);
        self.ready.notify_waiters();
    }

    /// Asks the stream task to stop the FLOW stream with `code`. The first
    /// request wins, so a `refuse` is not overwritten by the drop after it.
    fn request_stop(&self, code: u64) {
        let mut stop = lock(&self.stop);
        if stop.is_none() {
            *stop = Some(code);
            drop(stop);
            self.stop_signal.notify_waiters();
        }
    }

    async fn stop_requested(&self) -> u64 {
        loop {
            let mut notified = pin!(self.stop_signal.notified());
            notified.as_mut().enable();
            if let Some(code) = *lock(&self.stop) {
                return code;
            }
            notified.await;
        }
    }
}

/// Why a flow no longer carries anything.
#[derive(Clone, Copy, Debug)]
enum End {
    /// The receiver stopped the FLOW stream with this code.
    Stopped(u64),
    /// The connection went away.
    Lost(LossCause),
    /// The dialled address is gone for good, so a redial will not come.
    NotConnected,
}

impl End {
    fn from_error(error: &Error) -> End {
        match error {
            Error::Rejected => End::Stopped(codes::REJECTED),
            Error::UnknownEndpoint => End::Stopped(codes::UNKNOWN_ENDPOINT),
            Error::Unsupported => End::Stopped(codes::UNSUPPORTED),
            Error::LimitExceeded => End::Stopped(codes::LIMIT_EXCEEDED),
            Error::Canceled => End::Stopped(codes::CANCELED),
            Error::ConnectionLost(cause) => End::Lost(*cause),
            Error::NotConnected => End::NotConnected,
            _ => End::Lost(LossCause::TransportError),
        }
    }

    fn to_error(self) -> Error {
        match self {
            End::Stopped(code) => codes::stop_reason(code).into(),
            End::Lost(cause) => Error::ConnectionLost(cause),
            End::NotConnected => Error::NotConnected,
        }
    }
}

/// Per-connection flow state: the inbound table and the early ring.
pub(crate) struct FlowTable {
    next_id: AtomicU64,
    inbound: Mutex<HashMap<u64, Arc<Inbound>>>,
    early: Mutex<EarlyRing>,
    unknown: AtomicU64,
    limits: Limits,
}

/// Why an inbound flow could not be registered.
enum Refused {
    /// `max_flows` inbound flows are live.
    Full,
    /// The id names a flow that is still live: a protocol violation.
    Duplicate,
}

impl FlowTable {
    pub(crate) fn new(limits: &Limits) -> FlowTable {
        FlowTable {
            next_id: AtomicU64::new(0),
            inbound: Mutex::new(HashMap::new()),
            early: Mutex::new(EarlyRing::new(
                limits.flow_early_bytes,
                limits.flow_early_hold,
            )),
            unknown: AtomicU64::new(0),
            limits: *limits,
        }
    }

    fn count_unknown(&self, n: u64) {
        if n > 0 {
            let total = self.unknown.fetch_add(n, Ordering::Relaxed) + n;
            tracing::trace!(total, "datagrams for no live flow dropped");
        }
    }

    /// Routes one received QUIC datagram to its flow, or into the early ring.
    fn deliver(&self, datagram: Bytes, now: Instant) {
        let Some((id, offset)) = split_flow_datagram(&datagram) else {
            self.count_unknown(1);
            return;
        };
        // A copy, deliberately: a slice of `datagram` would pin the whole
        // receive buffer it came from — one UDP read, up to a GRO batch —
        // for as long as the ring keeps it, while the ring charges only the
        // payload, so a flood of one-byte datagrams would hold far more than
        // `flow_queue_bytes` says.
        let payload = Bytes::copy_from_slice(&datagram[offset..]);
        let inbound = lock(&self.inbound);
        if let Some(flow) = inbound.get(&id) {
            let flow = Arc::clone(flow);
            drop(inbound);
            flow.push(payload);
            return;
        }
        // Still holding the table: a FLOW header registering `id` now takes
        // the same two locks in the same order, so a datagram lands either
        // in the flow or in the ring it adopts from, never between them.
        let dropped = lock(&self.early).push(now, id, payload);
        drop(inbound);
        self.count_unknown(dropped);
    }

    /// Registers inbound flow `id` and hands it the datagrams that arrived
    /// before its header, oldest first.
    fn register(&self, id: u64, now: Instant) -> Result<Arc<Inbound>, Refused> {
        let mut inbound = lock(&self.inbound);
        if inbound.contains_key(&id) {
            return Err(Refused::Duplicate);
        }
        if inbound.len() >= self.limits.max_flows {
            return Err(Refused::Full);
        }
        let flow = Arc::new(Inbound::new(self.limits.flow_queue_bytes));
        let (early, evicted) = lock(&self.early).adopt(now, id);
        for datagram in early {
            flow.push(datagram);
        }
        inbound.insert(id, Arc::clone(&flow));
        drop(inbound);
        self.count_unknown(evicted);
        Ok(flow)
    }

    fn remove(&self, id: u64, flow: &Arc<Inbound>) {
        let mut inbound = lock(&self.inbound);
        if inbound.get(&id).is_some_and(|live| Arc::ptr_eq(live, flow)) {
            inbound.remove(&id);
        }
    }
}

/// The sending half of one datagram flow.
///
/// [`Flow::send`] never waits: it hands the datagram to the connection and
/// returns. Dropping the flow closes it with FIN, which the receiver sees as
/// the end of the flow; [`Flow::abort`] resets it instead.
///
/// A flow opened through a dialling [`crate::Peer`] belongs to the address,
/// not to the connection: when the connection is lost it is registered again
/// on the redialled one, under a new id, and every datagram sent while no
/// connection is live is dropped and counted as `not_live` — never queued,
/// because a queued unit is a late one
/// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.10).
pub struct Flow {
    binding: Arc<Mutex<Option<Binding>>>,
    counters: Arc<Counters>,
    /// Present for a flow opened through a dialling peer.
    redial: Option<Arc<Redial>>,
}

/// What re-registering a flow on a redialled connection needs.
struct Redial {
    peer: Arc<PeerShared>,
    meta: FlowMeta,
    /// A re-registration is under way; at most one at a time.
    running: AtomicBool,
    /// Why re-registration gave up, which ends the flow.
    failed: OnceLock<End>,
}

/// Finds a live connection for `redial`'s peer and registers the flow on it
/// again. A failure ends the flow with its error.
async fn reregister(slot: Weak<Mutex<Option<Binding>>>, redial: Arc<Redial>) {
    let bound = async {
        let (conn, path) = redial.peer.pick().await?;
        bind(&conn, &path, &redial.meta).await
    }
    .await;
    match bound {
        // A flow dropped meanwhile is gone; the new binding then closes as
        // it drops.
        Ok(binding) => {
            if let Some(slot) = slot.upgrade() {
                *lock(&slot) = Some(binding);
            }
        }
        Err(e) => {
            let _ = redial.failed.set(End::from_error(&e));
        }
    }
    redial.running.store(false, Ordering::Release);
}

/// A flow's registration on one connection.
struct Binding {
    conn: ConnHandle,
    prefix: ([u8; varint::MAX_ENCODED_LEN], usize),
    end: Arc<OnceLock<End>>,
    carrier: Carrier,
    /// Dropped with the binding, which ends the watcher task.
    _done: oneshot::Sender<()>,
}

enum Carrier {
    /// The FLOW stream stays open until FIN; datagrams go to `quinn`.
    Quic(SendHalf),
    /// A writer task owns the FLOW stream and drains this ring onto it.
    Local(Arc<SendRing>),
}

/// How a local writer ends.
#[derive(Clone, Copy)]
enum Close {
    Finish,
    Abort,
}

enum Next {
    Datagram(Bytes),
    Close(Close),
}

/// The drop-oldest queue a local flow's writer drains.
struct SendRing {
    state: Mutex<(Ring, Option<Close>)>,
    ready: Notify,
}

impl SendRing {
    fn push(&self, datagram: Bytes) -> u64 {
        let dropped = lock(&self.state).0.push(datagram);
        self.ready.notify_one();
        dropped
    }

    fn close(&self, close: Close) {
        let mut state = lock(&self.state);
        if !matches!(state.1, Some(Close::Abort)) {
            state.1 = Some(close);
        }
        drop(state);
        self.ready.notify_one();
    }

    async fn next(&self) -> Next {
        loop {
            let notified = self.ready.notified();
            {
                let mut state = lock(&self.state);
                if let Some(Close::Abort) = state.1 {
                    return Next::Close(Close::Abort);
                }
                if let Some(datagram) = state.0.pop() {
                    return Next::Datagram(datagram);
                }
                if let Some(close) = state.1 {
                    return Next::Close(close);
                }
            }
            // `notify_one` stores a permit when nobody waits, so a push
            // between the check and this await is not lost.
            notified.await;
        }
    }
}

/// Drains a local flow's ring onto its FLOW stream as `varint len || bytes`
/// records (`docs/PROTOCOL.md` §2.1).
async fn local_writer(mut send: SendHalf, ring: Arc<SendRing>, end: Arc<OnceLock<End>>) {
    let mut record = Vec::with_capacity(varint::MAX_ENCODED_LEN + LOCAL_MAX_DATAGRAM);
    loop {
        match ring.next().await {
            Next::Datagram(datagram) => {
                record.clear();
                encode_varint(datagram.len() as u64, &mut record)
                    .expect("a local datagram is at most 1200 bytes");
                record.extend_from_slice(&datagram);
                if let Err(e) = send.write_all(&record).await {
                    let _ = end.set(End::from_error(&e));
                    return;
                }
            }
            Next::Close(Close::Finish) => {
                let _ = send.finish();
                return;
            }
            Next::Close(Close::Abort) => {
                send.reset(codes::CANCELED);
                return;
            }
        }
    }
}

/// Records how a flow ended: the receiver's stop code, or the connection's
/// loss. Ends early when the binding is dropped, so a closed flow holds no
/// task.
type Stopped = Pin<Box<dyn Future<Output = Result<Option<u64>, Error>> + Send + Sync>>;

async fn watch(
    conn: ConnHandle,
    stopped: Stopped,
    end: Arc<OnceLock<End>>,
    mut done: oneshot::Receiver<()>,
) {
    let first = tokio::select! {
        _ = &mut done => return,
        receipt = stopped => match receipt {
            Ok(Some(code)) => Some(End::Stopped(code)),
            // No code: a socket transport never reports one, and a QUIC
            // receipt without one means the stream finished. Either way
            // only the connection's end is left to learn.
            _ => None,
        },
        reason = conn.conn.closed() => Some(End::from_error(&reason)),
    };
    let ended = match first {
        Some(ended) => ended,
        None => tokio::select! {
            _ = &mut done => return,
            reason = conn.conn.closed() => End::from_error(&reason),
        },
    };
    let _ = end.set(ended);
}

/// Registers a flow to `path` on `conn`: writes the FLOW header and returns
/// without waiting for an answer, which arrives as the stream's stop code.
pub(crate) async fn open_flow_on(
    conn: &ConnHandle,
    path: &str,
    meta: &FlowMeta,
) -> Result<Flow, Error> {
    let binding = bind(conn, path, meta).await?;
    Ok(Flow {
        binding: Arc::new(Mutex::new(Some(binding))),
        counters: Arc::new(Counters::default()),
        redial: None,
    })
}

/// Registers a flow through a dialling peer: on its next live connection
/// now, and on the redialled one after a loss.
pub(crate) async fn open_flow_via(peer: &Arc<PeerShared>, meta: FlowMeta) -> Result<Flow, Error> {
    let (conn, path) = peer.pick().await?;
    let binding = bind(&conn, &path, &meta).await?;
    Ok(Flow {
        binding: Arc::new(Mutex::new(Some(binding))),
        counters: Arc::new(Counters::default()),
        redial: Some(Arc::new(Redial {
            peer: Arc::clone(peer),
            meta,
            running: AtomicBool::new(false),
            failed: OnceLock::new(),
        })),
    })
}

async fn bind(conn: &ConnHandle, path: &str, meta: &FlowMeta) -> Result<Binding, Error> {
    if !conn.negotiated().await?.datagrams {
        return Err(Error::DatagramsUnavailable);
    }
    let id = conn.flows.next_id.fetch_add(1, Ordering::Relaxed);
    let header = FlowHeader {
        content_type: meta.content_type.clone(),
        traceparent: meta.trace.map(|t| t.to_traceparent()),
        tracestate: None,
        topic: meta.topic.clone(),
        ..FlowHeader::new(path, id)
    };
    // Refused before anything is written: a label no peer would read, or a
    // header longer than the peer said it reads, would make it close the
    // connection every other transfer shares.
    header.check().map_err(|_| Error::LimitExceeded)?;
    let encoded = header.encode();
    crate::conn::check_header_len(conn, encoded.len())?;
    let mut send = conn.open_uni().await?;
    send.write_all(&encode_frame(FrameKind::Flow, &encoded))
        .await?;

    let end = Arc::new(OnceLock::new());
    let (done_tx, done_rx) = oneshot::channel();
    conn.exec.spawn(watch(
        ConnHandle::clone(conn),
        send.stopped(),
        Arc::clone(&end),
        done_rx,
    ));
    let carrier = if conn.conn.is_quic() {
        Carrier::Quic(send)
    } else {
        let ring = Arc::new(SendRing {
            state: Mutex::new((Ring::new(conn.limits.datagram_send_bytes), None)),
            ready: Notify::new(),
        });
        conn.exec
            .spawn(local_writer(send, Arc::clone(&ring), Arc::clone(&end)));
        Carrier::Local(ring)
    };
    Ok(Binding {
        conn: ConnHandle::clone(conn),
        prefix: flow_prefix(id),
        end,
        carrier,
        _done: done_tx,
    })
}

impl Binding {
    fn max_payload(&self) -> Option<usize> {
        match self.carrier {
            Carrier::Quic(_) => self
                .conn
                .conn
                .max_datagram_size()
                .map(|max| max.saturating_sub(self.prefix.1)),
            Carrier::Local(_) => Some(LOCAL_MAX_DATAGRAM),
        }
    }

    /// Ends this registration; a carrier not aborted is finished as it
    /// drops.
    fn close(mut self, how: Close) {
        if let Close::Abort = how {
            match &mut self.carrier {
                Carrier::Quic(send) => send.reset(codes::CANCELED),
                Carrier::Local(ring) => ring.close(Close::Abort),
            }
        }
    }
}

impl Drop for Carrier {
    /// FIN, whichever way the registration went: closed, replaced by a
    /// redial, or orphaned by a flow dropped while it was being made.
    fn drop(&mut self) {
        match self {
            Carrier::Quic(send) => {
                let _ = send.finish();
            }
            Carrier::Local(ring) => ring.close(Close::Finish),
        }
    }
}

impl Flow {
    /// Sends one datagram. Never waits.
    ///
    /// `Ok` means the datagram was handed to the connection, not that it
    /// arrived: a flow is `BestEffort` per datagram. Errors:
    ///
    /// * [`Error::TooLarge`] when `payload` exceeds [`Flow::max_payload`];
    /// * [`Error::DatagramsUnavailable`] when the connection carries no
    ///   datagrams at all;
    /// * on a flow that has ended, the error that ended it — the receiver's
    ///   refusal ([`Error::Rejected`], [`Error::UnknownEndpoint`],
    ///   [`Error::Unsupported`], [`Error::LimitExceeded`]), its release
    ///   ([`Error::Canceled`]), or [`Error::ConnectionLost`].
    pub fn send(&self, payload: impl Into<Bytes>) -> Result<(), Error> {
        let payload = payload.into();
        let mut binding = lock(&self.binding);
        if let Some(redial) = &self.redial {
            if let Some(end) = redial.failed.get() {
                return Err(end.to_error());
            }
            let live = binding
                .as_ref()
                .is_some_and(|b| b.conn.conn.close_reason().is_none());
            if !live {
                binding.take();
                drop(binding);
                Counters::add(&self.counters.not_live, 1);
                if !redial.running.swap(true, Ordering::AcqRel) {
                    redial.peer.exec().spawn(reregister(
                        Arc::downgrade(&self.binding),
                        Arc::clone(redial),
                    ));
                }
                return Ok(());
            }
        }
        let Some(binding) = binding.as_ref() else {
            return Err(Error::Canceled);
        };
        if let Some(end) = binding.end.get() {
            return Err(end.to_error());
        }
        let Some(max) = binding.max_payload() else {
            return Err(Error::DatagramsUnavailable);
        };
        if payload.len() > max {
            Counters::add(&self.counters.too_large, 1);
            return Err(Error::TooLarge { max });
        }
        match &binding.carrier {
            Carrier::Quic(_) => {
                let (prefix, len) = binding.prefix;
                let mut datagram = BytesMut::with_capacity(len + payload.len());
                datagram.extend_from_slice(&prefix[..len]);
                datagram.extend_from_slice(&payload);
                binding.conn.conn.send_datagram(datagram.freeze())?;
            }
            Carrier::Local(ring) => {
                Counters::add(&self.counters.discarded, ring.push(payload));
            }
        }
        Counters::add(&self.counters.sent, 1);
        Ok(())
    }

    /// The largest payload [`Flow::send`] accepts right now: the
    /// connection's current datagram size minus the flow id's prefix on QUIC,
    /// [`LOCAL_MAX_DATAGRAM`] on a local transport. `None` when the
    /// connection carries no datagrams.
    pub fn max_payload(&self) -> Option<usize> {
        lock(&self.binding).as_ref()?.max_payload()
    }

    /// This flow's counters.
    pub fn stats(&self) -> FlowStats {
        self.counters.snapshot()
    }

    /// The path under this flow, or `None` on a local transport, which has
    /// no path to measure.
    pub fn path_stats(&self) -> Option<PathStats> {
        let path = lock(&self.binding)
            .as_ref()?
            .conn
            .conn
            .transport_stats()?
            .path;
        Some(path)
    }

    /// Closes the flow with FIN; the receiver sees its end after the
    /// datagrams already delivered. Dropping the flow does the same.
    pub fn close(self) {
        drop(self);
    }

    /// Abandons the flow: `RESET_STREAM(CANCELED)`.
    pub fn abort(self) {
        if let Some(binding) = lock(&self.binding).take() {
            binding.close(Close::Abort);
        }
    }
}

impl Drop for Flow {
    fn drop(&mut self) {
        if let Some(binding) = lock(&self.binding).take() {
            binding.close(Close::Finish);
        }
    }
}

impl fmt::Debug for Flow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Flow")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

/// The receiving half of one datagram flow.
///
/// Dropping it stops the FLOW stream with `CANCELED`, which the sender learns
/// as [`Error::Canceled`] on its next `send`; [`IncomingFlow::refuse`] stops
/// it with `REJECTED` instead.
pub struct IncomingFlow {
    info: FlowInfo,
    inbound: Arc<Inbound>,
    conn: ConnHandle,
}

impl IncomingFlow {
    /// What the sender attached to the flow.
    pub fn info(&self) -> &FlowInfo {
        &self.info
    }

    /// The next datagram, oldest first; `None` once the flow has ended and
    /// every datagram that arrived before its end has been taken.
    pub async fn recv(&self) -> Option<Bytes> {
        loop {
            let mut notified = pin!(self.inbound.ready.notified());
            notified.as_mut().enable();
            if let Some(datagram) = self.try_recv() {
                return Some(datagram);
            }
            if self.inbound.ended.load(Ordering::Acquire) {
                return self.try_recv();
            }
            notified.await;
        }
    }

    /// The next datagram if one is queued.
    pub fn try_recv(&self) -> Option<Bytes> {
        lock(&self.inbound.ring).pop()
    }

    /// This flow's counters.
    pub fn stats(&self) -> FlowStats {
        self.inbound.counters.snapshot()
    }

    /// The path under this flow, or `None` on a local transport, which has
    /// no path to measure.
    pub fn path_stats(&self) -> Option<PathStats> {
        self.conn.conn.transport_stats().map(|t| t.path)
    }

    /// Refuses the flow: `STOP_SENDING(REJECTED)`.
    pub fn refuse(self) {
        self.inbound.request_stop(codes::REJECTED);
    }
}

impl Drop for IncomingFlow {
    fn drop(&mut self) {
        self.inbound.request_stop(codes::CANCELED);
    }
}

impl fmt::Debug for IncomingFlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncomingFlow")
            .field("info", &self.info)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

/// Reads this connection's QUIC datagrams into their flows until it closes.
///
/// Waits on the link and holds the connection only weakly: reading is not
/// a use of the connection, so it must not keep an unused pooled one alive
/// (B-383). A datagram that arrives after the last handle went has no flow
/// left to go to.
pub(crate) async fn datagram_reader(
    ctx: std::sync::Weak<crate::conn::ConnCtx>,
    link: Arc<crate::transport::Link>,
) {
    while let Ok(datagram) = link.read_datagram().await {
        let Some(ctx) = ctx.upgrade() else { return };
        ctx.flows.deliver(datagram, ctx.exec.now());
    }
}

/// Serves one inbound FLOW stream: refusal, registration, delivery, and the
/// stream's life until FIN, reset or a stop.
pub(crate) async fn handle_flow(
    ctx: &ConnHandle,
    mut stream: RecvHalf,
    header: FlowHeader,
) -> Result<(), Error> {
    if !ctx.negotiated().await?.datagrams {
        return violation(ctx, "FLOW without the datagram capability");
    }
    let route = ctx.namespace.lookup(&header.endpoint);
    if let Some(refusal) = refusal_for(route.as_ref(), Wanted::Flow) {
        tracing::debug!(
            path = header.endpoint,
            ?refusal,
            "the path does not serve a flow"
        );
        stream.stop(refusal.stop);
        return Ok(());
    }
    let inbound = match ctx.flows.register(header.flow, ctx.exec.now()) {
        Ok(inbound) => inbound,
        Err(Refused::Full) => {
            stream.stop(codes::LIMIT_EXCEEDED);
            return Ok(());
        }
        Err(Refused::Duplicate) => return violation(ctx, "FLOW names a live flow id"),
    };
    let flow = IncomingFlow {
        info: FlowInfo {
            endpoint: header.endpoint.clone(),
            topic: header.topic,
            content_type: header.content_type,
            trace: header
                .traceparent
                .as_deref()
                .and_then(|v| TraceContext::parse_traceparent(v).ok()),
            peer: ctx.peer.clone(),
            peer_chain: ctx.peer_chain.clone(),
        },
        inbound: Arc::clone(&inbound),
        conn: ConnHandle::clone(ctx),
    };
    match route {
        Some(Route::Raw(queue)) => {
            // A failed hand-over drops the flow, which stops the stream with
            // `CANCELED` below: the acceptor went away.
            if queue.send(Incoming::Flow(flow)).await.is_err() {
                tracing::debug!(
                    path = header.endpoint,
                    "acceptor went away while dispatching"
                );
            }
        }
        // A radio's datagram segments: a pump moves them into the dish's
        // queue, so this task is free to watch the stream.
        Some(Route::Dish(route)) => {
            ctx.exec
                .spawn(crate::radio::pump_flow(ConnHandle::clone(ctx), route, flow));
        }
        _ => unreachable!("refusal_for serves a flow on a raw acceptor or a dish only"),
    }
    let outcome = run_stream(ctx, &mut stream, &inbound).await;
    ctx.flows.remove(header.flow, &inbound);
    inbound.end();
    outcome
}

/// The FLOW stream's life: on QUIC nothing may follow the header, and on a
/// local transport the datagrams do.
async fn run_stream(
    ctx: &ConnHandle,
    stream: &mut RecvHalf,
    inbound: &Inbound,
) -> Result<(), Error> {
    if ctx.conn.is_quic() {
        let mut byte = [0u8; 1];
        return tokio::select! {
            read = stream.read(&mut byte) => match read {
                Ok(Some(_)) => violation(ctx, "bytes after a FLOW header"),
                // FIN, a reset, or the connection's end: the flow is over.
                Ok(None) | Err(_) => Ok(()),
            },
            code = inbound.stop_requested() => {
                stream.stop(code);
                Ok(())
            }
        };
    }
    let mut pending: Vec<u8> = Vec::new();
    let mut scratch = vec![0u8; 4096];
    loop {
        // Every complete record is taken before the buffer is trimmed, and it
        // is trimmed once: trimming per record moves the whole remainder each
        // time, so a peer writing empty records would make every read a
        // quadratic amount of copying.
        let mut at = 0;
        while let Ok((len, used)) = decode_varint(&pending[at..]) {
            if len > LOCAL_MAX_DATAGRAM as u64 {
                return violation(ctx, "FLOW record longer than 1200 bytes");
            }
            let end = at + used + len as usize;
            if pending.len() < end {
                break;
            }
            // A copy, deliberately: a slice of `pending` would pin the whole
            // read buffer for as long as the consumer leaves it queued, and
            // the ring's byte cap would stop meaning what it says.
            inbound.push(Bytes::copy_from_slice(&pending[at + used..end]));
            at = end;
        }
        pending.drain(..at);
        tokio::select! {
            read = stream.read(&mut scratch) => match read {
                Ok(Some(n)) => pending.extend_from_slice(&scratch[..n]),
                Ok(None) if pending.is_empty() => return Ok(()),
                Ok(None) => return violation(ctx, "FLOW record truncated at FIN"),
                Err(_) => return Ok(()),
            },
            code = inbound.stop_requested() => {
                stream.stop(code);
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_id_allocates_nothing_beyond_the_early_ring() {
        let limits = Limits {
            flow_early_bytes: 4096,
            flow_early_hold: Duration::from_millis(50),
            ..Limits::default()
        };
        let mut ring = EarlyRing::new(limits.flow_early_bytes, limits.flow_early_hold);
        let start = Instant::now();
        let mut dropped = 0;
        for i in 0..1000u64 {
            dropped += ring.push(start, i, Bytes::from(vec![0u8; 100]));
            assert!(
                ring.bytes <= limits.flow_early_bytes,
                "{} bytes",
                ring.bytes
            );
        }
        assert_eq!(dropped + ring.items.len() as u64, 1000);
        // Past the hold, the next push evicts everything that was waiting.
        let later = start + Duration::from_millis(51);
        let held = ring.items.len() as u64;
        assert_eq!(ring.push(later, 0, Bytes::from_static(b"x")), held);
        assert_eq!(ring.items.len(), 1);
    }

    #[test]
    fn a_full_ring_drops_its_oldest() {
        let mut ring = Ring::new(3 * (10 + ENTRY_OVERHEAD));
        for i in 0..5u8 {
            ring.push(Bytes::from(vec![i; 10]));
        }
        assert_eq!(ring.pop().unwrap()[0], 2);
        assert_eq!(ring.pop().unwrap()[0], 3);
        assert_eq!(ring.pop().unwrap()[0], 4);
        assert!(ring.pop().is_none());
    }
}
