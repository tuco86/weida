//! Reaching a dial-only peer through a relay (`docs/PROTOCOL.md` §9.6).
//!
//! Three roles: the **hidden** peer H, which can only dial out, the
//! **relay** R, and a **dialler** D. H keeps one connection to R and serves
//! its Listener over it ([`crate::Listener::bind_relay`]); D carries the QUIC
//! packets of its own connection to H through R in a **tunnel**. R forwards
//! opaque QUIC datagrams and terminates nothing, so QUIC and TLS run end to
//! end between D and H, and each proves its own key to the other.
//!
//! Everything uses the existing vocabulary on one path, [`RELAY_PATH`]: one
//! DATA exchange per registration and per tunnel, whose payloads carry
//! `varint tunnel || varint len || bytes` records, and at most one FLOW per
//! direction and connection, whose datagrams carry `varint tunnel ||
//! datagram`. Nothing here adds a frame kind or a header key.
//!
//! **Lifetimes.** A relay connection's outgoing flow is opened on its first
//! relay use (a tunnel, a registration, a served registration) and closed on
//! its last. The flow, and the FLOW stream its peer reads, each hold their
//! connection, so a flow kept for a connection nobody uses would keep that
//! connection, and the one at the other end, alive forever: the pool's
//! reaper could never close it.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{self, IoSliceMut};
use std::net::{Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};

use bytes::{Bytes, BytesMut};
use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;
use weida_core::{Error, ErrorCode, Fingerprint, PeerIdentity, RELAY_PATH};
use weida_protocol::{codes, decode_varint, encode_varint, flow_prefix, split_flow_datagram};

use crate::conn::{ConnCtx, ConnHandle};
use crate::flow::{Flow, FlowMeta, IncomingFlow, open_flow_on};
use crate::listener::{ListenerInner, Route};
use crate::reconnect::PeerEvents;
use crate::stream::{Attach, Incoming, Peer, open_exchange_on};
use crate::transfer::{IncomingRequest, IncomingTransfer, OutgoingTransfer, TransferMeta};

/// The largest inner datagram a record carries: RFC 9000 §14's minimum, so
/// a QUIC Initial always passes before outer MTU discovery has run.
pub(crate) const TUNNEL_RECORD_MAX: usize = 1200;

/// Records queued per exchange writer; a record past it is dropped, as a
/// full socket buffer drops a datagram.
const RECORD_QUEUE: usize = 256;

/// Datagrams a tunnel socket holds unread; past it they are dropped.
const TUNNEL_INBOX: usize = 4096;

/// Flows queued on a dialling connection's relay route.
const ROUTE_QUEUE: usize = 16;

/// How many queued records one write of an exchange writer takes at most.
const WRITE_BATCH: usize = 64;

/// Request preamble of a registration.
const REGISTER: u8 = 0;

/// Request preamble of a tunnel, followed by the target's key digest.
const REACH: u8 = 1;

/// The /64 the inner `quinn` endpoints see tunnels in: `fd77:6569:6461::/64`
/// (a ULA prefix spelling "weida"). Nothing outside those endpoints ever
/// sees one of these addresses.
const VIRTUAL_PREFIX: u128 = 0xfd77_6569_6461_0000;

/// One record: a tunnel and one inner datagram.
pub(crate) type Record = (u64, Bytes);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The virtual address of tunnel `id`. Id `0` is the hidden peer's own
/// endpoint, and the address a dialler's inner endpoint dials.
pub(crate) fn tunnel_addr(id: u64) -> SocketAddr {
    SocketAddr::new(
        Ipv6Addr::from((VIRTUAL_PREFIX << 64) | u128::from(id)).into(),
        1,
    )
}

/// The tunnel a virtual address names; `None` for any other address.
pub(crate) fn tunnel_of(addr: SocketAddr) -> Option<u64> {
    let SocketAddr::V6(addr) = addr else {
        return None;
    };
    let bits = u128::from(*addr.ip());
    (addr.port() == 1 && bits >> 64 == VIRTUAL_PREFIX).then_some(bits as u64)
}

/// `varint tunnel || datagram`: one relay DATAGRAM payload.
fn encode_datagram(tunnel: u64, datagram: &[u8]) -> Bytes {
    let (prefix, len) = flow_prefix(tunnel);
    let mut out = BytesMut::with_capacity(len + datagram.len());
    out.extend_from_slice(&prefix[..len]);
    out.extend_from_slice(datagram);
    out.freeze()
}

/// The inverse of [`encode_datagram`].
fn split_datagram(payload: &[u8]) -> Option<(u64, &[u8])> {
    let (tunnel, offset) = split_flow_datagram(payload)?;
    Some((tunnel, &payload[offset..]))
}

/// Appends `varint tunnel || varint len || datagram` to `out`.
fn encode_record(tunnel: u64, datagram: &[u8], out: &mut Vec<u8>) {
    encode_varint(tunnel, out).expect("tunnel ids stay below 2^62");
    encode_varint(datagram.len() as u64, out).expect("a record is at most 1200 bytes");
    out.extend_from_slice(datagram);
}

/// Reads one QUIC varint; `None` on EOF before its first byte.
async fn read_varint<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Option<u64>> {
    let mut buf = [0u8; 8];
    if r.read(&mut buf[..1]).await? == 0 {
        return Ok(None);
    }
    let len = 1usize << (buf[0] >> 6);
    r.read_exact(&mut buf[1..len]).await?;
    let (value, _) = decode_varint(&buf[..len]).expect("the length came from the first byte");
    Ok(Some(value))
}

fn truncated(e: io::Error) -> Error {
    if e.kind() == io::ErrorKind::UnexpectedEof {
        Error::Protocol("relay record truncated".into())
    } else {
        Error::Io(e)
    }
}

/// Reads one record. EOF on a record boundary is `Ok(None)`; EOF inside one,
/// and a length of 0 or above [`TUNNEL_RECORD_MAX`], are errors.
async fn read_record<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Record>, Error> {
    let Some(tunnel) = read_varint(r).await.map_err(truncated)? else {
        return Ok(None);
    };
    let len = read_varint(r)
        .await
        .map_err(truncated)?
        .ok_or_else(|| Error::Protocol("relay record truncated".into()))?;
    if len == 0 || len > TUNNEL_RECORD_MAX as u64 {
        return Err(Error::Protocol("relay record length out of range".into()));
    }
    let mut datagram = vec![0u8; len as usize];
    r.read_exact(&mut datagram).await.map_err(truncated)?;
    Ok(Some((tunnel, Bytes::from(datagram))))
}

/// How one inner datagram travels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Carrier {
    Datagram,
    Record,
    Drop,
}

/// The carrier rule: a datagram when the prefixed datagram fits the out
/// flow, a record when the inner datagram fits one, and otherwise nothing,
/// as a path with a smaller MTU drops it.
fn carrier(prefixed_len: usize, datagram_len: usize, max_payload: Option<usize>) -> Carrier {
    if datagram_len == 0 {
        Carrier::Drop
    } else if max_payload.is_some_and(|max| prefixed_len <= max) {
        Carrier::Datagram
    } else if datagram_len <= TUNNEL_RECORD_MAX {
        Carrier::Record
    } else {
        Carrier::Drop
    }
}

/// Writes the records queued on `rx` onto an exchange half, until every
/// sender is gone, which cancels the half, or a write fails.
async fn write_records(mut out: OutgoingTransfer, mut rx: mpsc::Receiver<Record>) {
    let mut buf = Vec::with_capacity(WRITE_BATCH * (16 + TUNNEL_RECORD_MAX));
    while let Some((tunnel, datagram)) = rx.recv().await {
        buf.clear();
        encode_record(tunnel, &datagram, &mut buf);
        for _ in 1..WRITE_BATCH {
            let Ok((tunnel, datagram)) = rx.try_recv() else {
                break;
            };
            encode_record(tunnel, &datagram, &mut buf);
        }
        if out.write_all(&buf).await.is_err() {
            return;
        }
    }
    out.cancel();
}

/// Reads the records of an exchange half into `conn`'s tunnels until it
/// ends. With `only`, the half belongs to that one tunnel: a record of any
/// other, or one arriving after the tunnel ended, ends the read, and
/// dropping the half stops it.
async fn read_records(mut body: IncomingTransfer, conn: Weak<ConnCtx>, only: Option<u64>) {
    while let Ok(Some((tunnel, datagram))) = read_record(&mut body).await {
        if only.is_some_and(|id| id != tunnel) {
            return;
        }
        let Some(conn) = conn.upgrade() else { return };
        if !conn.relay.dispatch(tunnel, datagram) && only.is_some() {
            return;
        }
    }
}

/// Where a tunnel's datagrams go on one connection.
pub(crate) trait TunnelSink: Send + Sync {
    fn deliver(&self, tunnel: u64, datagram: Bytes);
}

/// One connection's relay state: its tunnels and its outgoing flow.
pub(crate) struct RelayLink {
    /// Whether the dialling side registered its relay route.
    route: Mutex<bool>,
    state: Mutex<LinkState>,
    /// Serializes opening the outgoing flow.
    opening: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct LinkState {
    sinks: HashMap<u64, Arc<dyn TunnelSink>>,
    /// Every tunnel no sink claims: a hidden peer's relay socket.
    catch_all: Option<Arc<dyn TunnelSink>>,
    /// Registrations a relay serves on this connection.
    holds: usize,
    out: Option<Flow>,
}

impl LinkState {
    fn in_use(&self) -> bool {
        !self.sinks.is_empty() || self.catch_all.is_some() || self.holds > 0
    }

    /// The outgoing flow, taken once nothing uses the connection for relay
    /// any more; the caller drops it outside the lock.
    fn take_unused(&mut self) -> Option<Flow> {
        if self.in_use() { None } else { self.out.take() }
    }
}

impl RelayLink {
    pub(crate) fn new() -> RelayLink {
        RelayLink {
            route: Mutex::new(false),
            state: Mutex::new(LinkState::default()),
            opening: tokio::sync::Mutex::new(()),
        }
    }

    /// Hands a tunnel's datagram to its sink, else to the catch-all; false
    /// when neither exists and the datagram was dropped.
    fn dispatch(&self, tunnel: u64, datagram: Bytes) -> bool {
        let sink = {
            let state = lock(&self.state);
            state
                .sinks
                .get(&tunnel)
                .or(state.catch_all.as_ref())
                .cloned()
        };
        match sink {
            Some(sink) => {
                sink.deliver(tunnel, datagram);
                true
            }
            None => false,
        }
    }

    fn insert(&self, tunnel: u64, sink: Arc<dyn TunnelSink>) {
        lock(&self.state).sinks.insert(tunnel, sink);
    }

    pub(crate) fn remove(&self, tunnel: u64) {
        let (sink, out) = {
            let mut state = lock(&self.state);
            (state.sinks.remove(&tunnel), state.take_unused())
        };
        drop((sink, out));
    }

    fn set_catch_all(&self, sink: Option<Arc<dyn TunnelSink>>) {
        let (old, out) = {
            let mut state = lock(&self.state);
            let old = std::mem::replace(&mut state.catch_all, sink);
            (old, state.take_unused())
        };
        drop((old, out));
    }

    fn hold(&self) {
        lock(&self.state).holds += 1;
    }

    fn unhold(&self) {
        let out = {
            let mut state = lock(&self.state);
            state.holds = state.holds.saturating_sub(1);
            state.take_unused()
        };
        drop(out);
    }

    /// Opens the outgoing flow if the connection is in relay use, carries
    /// datagrams, and has none. Without one everything goes as records.
    async fn open_out(&self, conn: &ConnHandle) {
        let _opening = self.opening.lock().await;
        {
            let state = lock(&self.state);
            if state.out.is_some() || !state.in_use() {
                return;
            }
        }
        let Ok(flow) = open_flow_on(conn, RELAY_PATH, &FlowMeta::default()).await else {
            return;
        };
        let unused = {
            let mut state = lock(&self.state);
            if state.in_use() {
                state.out = Some(flow);
                None
            } else {
                Some(flow)
            }
        };
        drop(unused);
    }
}

/// Sends one inner datagram of `tunnel` toward the other end of `conn`, by
/// the carrier rule. Never waits: a full record queue drops the datagram.
pub(crate) fn send(conn: &ConnCtx, records: &mpsc::Sender<Record>, tunnel: u64, datagram: &[u8]) {
    let prefixed = weida_protocol::varint::varint_len(tunnel) + datagram.len();
    let chosen = {
        let state = lock(&conn.relay.state);
        let out = state.out.as_ref();
        match carrier(prefixed, datagram.len(), out.and_then(Flow::max_payload)) {
            Carrier::Datagram => {
                let flow = out.expect("a datagram carrier has a flow");
                if flow.send(encode_datagram(tunnel, datagram)).is_ok() {
                    return;
                }
                if datagram.len() <= TUNNEL_RECORD_MAX {
                    Carrier::Record
                } else {
                    Carrier::Drop
                }
            }
            other => other,
        }
    };
    if chosen == Carrier::Record {
        let _ = records.try_send((tunnel, Bytes::copy_from_slice(datagram)));
    }
}

/// Reads a relay flow's datagrams into the tunnels of its connection.
async fn pump_flow(flow: IncomingFlow) {
    while let Some(payload) = flow.recv().await {
        let Some((tunnel, datagram)) = split_datagram(&payload) else {
            continue;
        };
        if datagram.is_empty() {
            continue;
        }
        let datagram = payload.slice(payload.len() - datagram.len()..);
        flow.conn().relay.dispatch(tunnel, datagram);
    }
}

/// Registers the relay route on a dialling connection, once: where the
/// relay's flow toward this side arrives. It lives as long as the
/// connection, so a second call is a no-op.
fn ensure_route(conn: &ConnHandle) -> Result<(), Error> {
    let mut routed = lock(&conn.relay.route);
    if *routed {
        return Ok(());
    }
    let (tx, mut rx) = mpsc::channel(ROUTE_QUEUE);
    conn.namespace.register(RELAY_PATH, Route::Raw(tx))?;
    *routed = true;
    let exec = conn.exec.clone();
    conn.exec.spawn(async move {
        while let Some(incoming) = rx.recv().await {
            if let Incoming::Flow(flow) = incoming {
                exec.spawn(pump_flow(flow));
            }
        }
    });
    Ok(())
}

/// Datagrams a tunnel socket has received and `quinn` has not read.
#[derive(Default)]
struct Inbox {
    queue: VecDeque<(SocketAddr, Bytes)>,
    waker: Option<Waker>,
}

impl Inbox {
    fn push(inbox: &Mutex<Inbox>, from: SocketAddr, datagram: Bytes) {
        let mut inbox = lock(inbox);
        if inbox.queue.len() >= TUNNEL_INBOX {
            return;
        }
        inbox.queue.push_back((from, datagram));
        let waker = inbox.waker.take();
        drop(inbox);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn poll_recv(
        inbox: &Mutex<Inbox>,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let mut inbox = lock(inbox);
        let mut filled = 0;
        while filled < bufs.len().min(meta.len()) {
            let Some((from, datagram)) = inbox.queue.pop_front() else {
                break;
            };
            let len = datagram.len().min(bufs[filled].len());
            bufs[filled][..len].copy_from_slice(&datagram[..len]);
            meta[filled] = RecvMeta {
                addr: from,
                len,
                stride: len,
                ecn: None,
                dst_ip: None,
            };
            filled += 1;
        }
        if filled > 0 {
            return Poll::Ready(Ok(filled));
        }
        inbox.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// A tunnel socket is always writable: a send never blocks.
#[derive(Debug)]
struct Writable;

impl UdpPoller for Writable {
    fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Splits a transmit into its GSO segments.
fn segments<'a>(transmit: &Transmit<'a>) -> std::slice::Chunks<'a, u8> {
    let size = transmit.segment_size.unwrap_or(transmit.contents.len());
    transmit.contents.chunks(size.max(1))
}

/// The registration a hidden peer's relay socket sends on.
struct Uplink {
    conn: Weak<ConnCtx>,
    records: mpsc::Sender<Record>,
}

/// The socket a hidden peer's Listener serves on: every tunnel of its
/// registration at the relay, each under its virtual address.
pub(crate) struct RelaySocket {
    inbox: Mutex<Inbox>,
    uplink: Mutex<Option<Uplink>>,
    /// The dialling peer that keeps the registration, redials included.
    peer: Mutex<Option<Peer>>,
    released: AtomicBool,
}

impl RelaySocket {
    pub(crate) fn new() -> RelaySocket {
        RelaySocket {
            inbox: Mutex::new(Inbox::default()),
            uplink: Mutex::new(None),
            peer: Mutex::new(None),
            released: AtomicBool::new(false),
        }
    }

    pub(crate) fn set_peer(&self, peer: Peer) {
        *lock(&self.peer) = Some(peer);
    }

    /// The events of the registration's connection to the relay.
    pub(crate) fn events(&self) -> Option<PeerEvents> {
        lock(&self.peer).as_ref().map(Peer::events)
    }

    /// Ends the registration and sends and receives nothing from here on.
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
        {
            let mut inbox = lock(&self.inbox);
            inbox.queue.clear();
            inbox.waker = None;
        }
        let peer = lock(&self.peer).take();
        if let Some(peer) = peer {
            peer.detach_live();
        }
        drop(lock(&self.uplink).take());
    }
}

impl TunnelSink for RelaySocket {
    fn deliver(&self, tunnel: u64, datagram: Bytes) {
        if !self.released.load(Ordering::Acquire) {
            Inbox::push(&self.inbox, tunnel_addr(tunnel), datagram);
        }
    }
}

impl AsyncUdpSocket for RelaySocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Writable)
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        if self.released.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(tunnel) = tunnel_of(transmit.destination) else {
            return Ok(());
        };
        let uplink = lock(&self.uplink);
        let Some(uplink) = uplink.as_ref() else {
            return Ok(());
        };
        let Some(conn) = uplink.conn.upgrade() else {
            return Ok(());
        };
        for datagram in segments(transmit) {
            send(&conn, &uplink.records, tunnel, datagram);
        }
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if self.released.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        Inbox::poll_recv(&self.inbox, cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(tunnel_addr(0))
    }
}

impl fmt::Debug for RelaySocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RelaySocket")
            .field("released", &self.released.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// What a hidden peer does on every connection to its relay, first dial and
/// redial alike: register, and serve the tunnels the relay hands it.
pub(crate) struct RegisterAttach {
    socket: Weak<RelaySocket>,
}

impl RegisterAttach {
    pub(crate) fn new(socket: &Arc<RelaySocket>) -> RegisterAttach {
        RegisterAttach {
            socket: Arc::downgrade(socket),
        }
    }
}

impl Attach for RegisterAttach {
    fn attach<'a>(
        &'a self,
        conn: &'a ConnHandle,
        path: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            ensure_route(conn)?;
            let (mut up, down) = open_exchange_on(conn, path, TransferMeta::default()).await?;
            up.write_all(&[REGISTER]).await?;
            // A refusal is the ERROR frame: `Rejected`, which the first
            // `connect` returns and a redial tries again.
            let reply = down.recv().await?;
            let socket = match self.socket.upgrade() {
                Some(socket) if !socket.released.load(Ordering::Acquire) => socket,
                _ => {
                    up.cancel();
                    return Err(Error::NotConnected);
                }
            };
            let (tx, rx) = mpsc::channel(RECORD_QUEUE);
            conn.relay
                .set_catch_all(Some(Arc::clone(&socket) as Arc<dyn TunnelSink>));
            *lock(&socket.uplink) = Some(Uplink {
                conn: Arc::downgrade(conn),
                records: tx,
            });
            conn.exec.spawn(write_records(up, rx));
            let weak = Arc::downgrade(conn);
            conn.exec.spawn(async move {
                read_records(reply, weak.clone(), None).await;
                // The registration ended with its connection, or the relay
                // ended it: this connection serves no tunnel any more.
                if let Some(conn) = weak.upgrade() {
                    conn.relay.set_catch_all(None);
                }
            });
            conn.relay.open_out(conn).await;
            Ok(())
        })
    }

    fn detach(&self, conn: &ConnHandle, _path: &Arc<str>) {
        conn.relay.set_catch_all(None);
        // Dropping the sender cancels the registration's request half, and
        // the relay ends the registration.
        if let Some(socket) = self.socket.upgrade() {
            let mut uplink = lock(&socket.uplink);
            if uplink
                .as_ref()
                .is_some_and(|u| std::ptr::eq(u.conn.as_ptr(), Arc::as_ptr(conn)))
            {
                *uplink = None;
            }
        }
    }
}

/// The socket a dialler's inner endpoint dials its tunnel on.
pub(crate) struct TunnelSocket {
    id: u64,
    conn: Weak<ConnCtx>,
    /// `None` once the tunnel ended. `quinn` keeps a connection's socket as
    /// long as any handle on that connection lives, so the sender, which
    /// keeps the tunnel's request half and with it the hop connection, must
    /// not live as long as the socket does.
    records: Mutex<Option<mpsc::Sender<Record>>>,
    inbox: Mutex<Inbox>,
}

impl TunnelSocket {
    /// Ends the tunnel: the hop forgets it, the relay's half is canceled,
    /// and the socket sends and receives nothing from here on.
    pub(crate) fn end(&self) {
        if let Some(conn) = self.conn.upgrade() {
            conn.relay.remove(self.id);
        }
        drop(lock(&self.records).take());
        let mut inbox = lock(&self.inbox);
        inbox.queue.clear();
        inbox.waker = None;
    }
}

impl TunnelSink for TunnelSocket {
    fn deliver(&self, _tunnel: u64, datagram: Bytes) {
        Inbox::push(&self.inbox, tunnel_addr(0), datagram);
    }
}

impl AsyncUdpSocket for TunnelSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Writable)
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        let records = lock(&self.records);
        if let Some(records) = records.as_ref()
            && let Some(conn) = self.conn.upgrade()
        {
            for datagram in segments(transmit) {
                send(&conn, records, self.id, datagram);
            }
        }
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        Inbox::poll_recv(&self.inbox, cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(tunnel_addr(self.id))
    }
}

impl fmt::Debug for TunnelSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunnelSocket")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Asks the relay at the other end of `conn` for a tunnel to `target`, and
/// returns the socket an inner endpoint dials it on.
pub(crate) async fn open_tunnel(
    conn: &ConnHandle,
    target: Fingerprint,
) -> Result<Arc<TunnelSocket>, Error> {
    ensure_route(conn)?;
    let (mut up, down) = open_exchange_on(conn, RELAY_PATH, TransferMeta::default()).await?;
    let mut preamble = [0u8; 33];
    preamble[0] = REACH;
    preamble[1..].copy_from_slice(target.as_bytes());
    up.write_all(&preamble).await?;
    let mut reply = down.recv().await?;
    let id = read_varint(&mut reply)
        .await
        .map_err(truncated)?
        .ok_or_else(|| Error::Protocol("relay reply ended before its tunnel id".into()))?;
    let (tx, rx) = mpsc::channel(RECORD_QUEUE);
    let socket = Arc::new(TunnelSocket {
        id,
        conn: Arc::downgrade(conn),
        records: Mutex::new(Some(tx)),
        inbox: Mutex::new(Inbox::default()),
    });
    conn.relay
        .insert(id, Arc::clone(&socket) as Arc<dyn TunnelSink>);
    conn.exec.spawn(write_records(up, rx));
    conn.exec
        .spawn(read_records(reply, Arc::downgrade(conn), Some(id)));
    conn.relay.open_out(conn).await;
    Ok(socket)
}

/// One hidden peer's registration at the relay.
struct Registration {
    conn: ConnHandle,
    records: mpsc::Sender<Record>,
    /// Its tunnels, each with the dialler's connection.
    tunnels: Mutex<HashMap<u64, Weak<ConnCtx>>>,
}

impl Registration {
    fn end_tunnel(&self, id: u64) {
        let dialler = lock(&self.tunnels).remove(&id);
        self.conn.relay.remove(id);
        if let Some(dialler) = dialler.and_then(|d| d.upgrade()) {
            dialler.relay.remove(id);
        }
    }

    fn end_all(&self) {
        let ids: Vec<u64> = lock(&self.tunnels).keys().copied().collect();
        for id in ids {
            self.end_tunnel(id);
        }
    }
}

/// A relay's registrations and its tunnel numbering.
pub(crate) struct RelayHub {
    /// Never reused while the hub lives; `0` is the hidden peer's own
    /// address.
    next_tunnel: AtomicU64,
    registrations: Mutex<HashMap<Fingerprint, Arc<Registration>>>,
}

impl RelayHub {
    /// Ends every registration and closes its connection, so the hidden
    /// peer redials.
    fn close(&self) {
        let all: Vec<Arc<Registration>> = lock(&self.registrations)
            .drain()
            .map(|(_, reg)| reg)
            .collect();
        for reg in all {
            reg.conn.conn.close(codes::SHUTDOWN, "relay closed");
            reg.end_all();
        }
    }

    fn is_current(&self, key: &Fingerprint, reg: &Arc<Registration>) -> bool {
        lock(&self.registrations)
            .get(key)
            .is_some_and(|live| Arc::ptr_eq(live, reg))
    }
}

/// Forwards a tunnel's datagrams to the connection at its other end.
struct Forward {
    to: Weak<ConnCtx>,
    records: mpsc::Sender<Record>,
}

impl TunnelSink for Forward {
    fn deliver(&self, tunnel: u64, datagram: Bytes) {
        if let Some(to) = self.to.upgrade() {
            send(&to, &self.records, tunnel, &datagram);
        }
    }
}

/// The relay a [`crate::Listener`] serves on [`RELAY_PATH`].
///
/// A hidden peer registers under its key ([`crate::Listener::bind_relay`]),
/// and a dialler reaches it as `weida+relay://<peer>@<relay>/path`. The relay
/// forwards their QUIC datagrams and sees nothing inside them. Which
/// registration and which tunnel it serves is the application's decision:
/// each arrives at [`Relay::accept`] as a [`RelayRequest`], with the key its
/// sender proved.
///
/// Dropping the relay releases the path and closes every registration's
/// connection, so the hidden peers redial.
pub struct Relay {
    hub: Arc<RelayHub>,
    requests: tokio::sync::Mutex<mpsc::Receiver<RelayRequest>>,
    listener: Arc<ListenerInner>,
}

impl Relay {
    /// Serves [`RELAY_PATH`] on `listener`.
    pub(crate) fn serve_on(listener: &Arc<ListenerInner>) -> Result<Relay, Error> {
        let runtime = &listener.runtime;
        let (tx, rx) = mpsc::channel(runtime.config.endpoint_queue);
        listener.namespace.register(RELAY_PATH, Route::Raw(tx))?;
        let (requests_tx, requests_rx) = mpsc::channel(runtime.config.endpoint_queue);
        let hub = Arc::new(RelayHub {
            next_tunnel: AtomicU64::new(1),
            registrations: Mutex::new(HashMap::new()),
        });
        runtime.exec.spawn(serve(Arc::clone(&hub), rx, requests_tx));
        Ok(Relay {
            hub,
            requests: tokio::sync::Mutex::new(requests_rx),
            listener: Arc::clone(listener),
        })
    }

    /// The next registration or tunnel asking to be served.
    ///
    /// A registration from an anonymous peer, a tunnel to a key nobody
    /// registered, and an unknown preamble are refused before they get here.
    /// Fails with [`Error::NotConnected`] once the relay stopped serving.
    pub async fn accept(&self) -> Result<RelayRequest, Error> {
        let mut requests = self.requests.lock().await;
        requests.recv().await.ok_or(Error::NotConnected)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.listener.namespace.unregister(RELAY_PATH);
        self.hub.close();
    }
}

impl fmt::Debug for Relay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registrations = lock(&self.hub.registrations).len();
        f.debug_struct("Relay")
            .field("registrations", &registrations)
            .finish_non_exhaustive()
    }
}

/// Classifies what arrives on [`RELAY_PATH`]: exchanges by their preamble,
/// flows into the tunnels of their connection.
async fn serve(
    hub: Arc<RelayHub>,
    mut rx: mpsc::Receiver<Incoming>,
    requests: mpsc::Sender<RelayRequest>,
) {
    while let Some(incoming) = rx.recv().await {
        match incoming {
            Incoming::Exchange(request) => {
                let exec = request.conn().exec.clone();
                exec.spawn(classify(Arc::clone(&hub), request, requests.clone()));
            }
            Incoming::Flow(flow) => {
                let exec = flow.conn().exec.clone();
                exec.spawn(pump_flow(flow));
            }
            _ => {}
        }
    }
}

async fn classify(
    hub: Arc<RelayHub>,
    mut request: IncomingRequest,
    requests: mpsc::Sender<RelayRequest>,
) {
    let mut body = request.take_body();
    let mut kind = [0u8; 1];
    let target = match body.read_exact(&mut kind).await {
        Ok(_) if kind[0] == REGISTER => None,
        Ok(_) if kind[0] == REACH => {
            let mut key = [0u8; 32];
            if body.read_exact(&mut key).await.is_err() {
                drop(body);
                return request.refuse(ErrorCode::Unsupported).await;
            }
            Some(Fingerprint::from_bytes(key))
        }
        _ => {
            drop(body);
            return request.refuse(ErrorCode::Unsupported).await;
        }
    };
    let refusal = match target {
        // A registration is addressed by its key: an anonymous one could
        // never be reached.
        None if !matches!(request.meta().peer, Some(PeerIdentity::Key(_))) => {
            Some(ErrorCode::Rejected)
        }
        Some(target) if !lock(&hub.registrations).contains_key(&target) => {
            Some(ErrorCode::UnknownEndpoint)
        }
        _ => None,
    };
    if let Some(code) = refusal {
        drop(body);
        return request.refuse(code).await;
    }
    let ask = RelayRequest {
        request,
        body,
        target,
        hub,
    };
    if let Err(full) = requests.try_send(ask) {
        let ask = match full {
            mpsc::error::TrySendError::Full(ask) | mpsc::error::TrySendError::Closed(ask) => ask,
        };
        ask.refuse().await;
    }
}

/// A registration or a tunnel asking a [`Relay`] to be served.
///
/// Dropping it without an answer refuses it as the dropped request it is
/// ([`ErrorCode::NoReply`]).
pub struct RelayRequest {
    request: IncomingRequest,
    body: IncomingTransfer,
    target: Option<Fingerprint>,
    hub: Arc<RelayHub>,
}

impl RelayRequest {
    /// The key the asking peer proved, `None` for an anonymous one. Always a
    /// key for a registration.
    pub fn peer(&self) -> Option<&PeerIdentity> {
        self.request.meta().peer.as_ref()
    }

    /// `None` for a registration; for a tunnel, the key of the peer it
    /// reaches.
    pub fn target(&self) -> Option<Fingerprint> {
        self.target
    }

    /// Refuses it: the asking peer fails with [`Error::Rejected`].
    pub async fn refuse(self) {
        drop(self.body);
        self.request.refuse(ErrorCode::Rejected).await;
    }

    /// Serves it.
    ///
    /// A registration replaces one of the same key on another connection,
    /// whose connection is closed: that is the hidden peer's redial, which
    /// overtook the relay's idle timeout. A second registration on the same
    /// connection is refused with [`Error::AlreadyRegistered`]. A tunnel
    /// whose target went away meanwhile is refused with
    /// [`Error::UnknownEndpoint`].
    pub async fn admit(self) -> Result<(), Error> {
        match self.target {
            None => self.register().await,
            Some(target) => self.reach(target).await,
        }
    }

    async fn register(self) -> Result<(), Error> {
        let RelayRequest {
            request, body, hub, ..
        } = self;
        let conn = ConnHandle::clone(request.conn());
        let Some(key) = conn.peer.as_ref().and_then(PeerIdentity::key) else {
            drop(body);
            request.refuse(ErrorCode::Rejected).await;
            return Err(Error::Rejected);
        };
        let serial = conn.conn.stable_id();
        let replaced = {
            let mut registrations = lock(&hub.registrations);
            match registrations.get(&key) {
                Some(live) if live.conn.conn.stable_id() == serial => None,
                Some(_) => Some(registrations.remove(&key)),
                None => Some(None),
            }
        };
        let Some(replaced) = replaced else {
            drop(body);
            request.refuse(ErrorCode::Rejected).await;
            return Err(Error::AlreadyRegistered);
        };
        if let Some(old) = replaced {
            old.conn
                .conn
                .close(codes::SHUTDOWN, "relay registration replaced");
            old.end_all();
        }

        let out = request.reply(TransferMeta::default()).await?;
        let (tx, rx) = mpsc::channel(RECORD_QUEUE);
        let reg = Arc::new(Registration {
            conn: ConnHandle::clone(&conn),
            records: tx,
            tunnels: Mutex::new(HashMap::new()),
        });
        conn.relay.hold();
        let raced = lock(&hub.registrations).insert(key, Arc::clone(&reg));
        if let Some(raced) = raced {
            raced
                .conn
                .conn
                .close(codes::SHUTDOWN, "relay registration replaced");
            raced.end_all();
        }
        conn.exec.spawn(write_records(out, rx));
        let exec = conn.exec.clone();
        exec.spawn(async move {
            read_records(body, Arc::downgrade(&reg.conn), None).await;
            if hub.is_current(&key, &reg) {
                lock(&hub.registrations).remove(&key);
            }
            reg.end_all();
            reg.conn.relay.unhold();
        });
        conn.relay.open_out(&conn).await;
        Ok(())
    }

    async fn reach(self, target: Fingerprint) -> Result<(), Error> {
        let RelayRequest {
            request, body, hub, ..
        } = self;
        let dialler = ConnHandle::clone(request.conn());
        let reg = lock(&hub.registrations).get(&target).cloned();
        let Some(reg) = reg else {
            drop(body);
            request.refuse(ErrorCode::UnknownEndpoint).await;
            return Err(Error::UnknownEndpoint);
        };
        let id = hub.next_tunnel.fetch_add(1, Ordering::Relaxed);
        let mut out = request.reply(TransferMeta::default()).await?;
        let mut head = Vec::with_capacity(8);
        encode_varint(id, &mut head).expect("tunnel ids stay below 2^62");
        out.write_all(&head).await?;

        let (tx, rx) = mpsc::channel(RECORD_QUEUE);
        dialler.relay.insert(
            id,
            Arc::new(Forward {
                to: Arc::downgrade(&reg.conn),
                records: reg.records.clone(),
            }),
        );
        reg.conn.relay.insert(
            id,
            Arc::new(Forward {
                to: Arc::downgrade(&dialler),
                records: tx,
            }),
        );
        lock(&reg.tunnels).insert(id, Arc::downgrade(&dialler));
        // The registration may have ended between the lookup and here, and
        // then nothing else would end this tunnel's half on its side.
        if !hub.is_current(&target, &reg) {
            reg.end_tunnel(id);
        }

        let exec = dialler.exec.clone();
        exec.spawn(write_records(out, rx));
        let registration = Arc::downgrade(&reg);
        let hidden = Arc::downgrade(&reg.conn);
        let weak_dialler = Arc::downgrade(&dialler);
        exec.spawn(async move {
            read_records(body, weak_dialler.clone(), Some(id)).await;
            match registration.upgrade() {
                Some(reg) => reg.end_tunnel(id),
                None => {
                    if let Some(hidden) = hidden.upgrade() {
                        hidden.relay.remove(id);
                    }
                    if let Some(dialler) = weak_dialler.upgrade() {
                        dialler.relay.remove(id);
                    }
                }
            }
        });
        dialler.relay.open_out(&dialler).await;
        Ok(())
    }
}

impl fmt::Debug for RelayRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RelayRequest")
            .field("peer", &self.peer())
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tunnel_address_round_trips_and_no_other_address_is_one() {
        for id in [0, 1, u64::MAX] {
            assert_eq!(tunnel_of(tunnel_addr(id)), Some(id));
        }
        for other in [
            "127.0.0.1:1",
            "[fd77:6569:6461::5]:2",
            "[fd77:6569:6462::5]:1",
        ] {
            assert_eq!(tunnel_of(other.parse().expect("address")), None, "{other}");
        }
    }

    #[test]
    fn a_relay_datagram_splits_back_into_its_tunnel_and_datagram() {
        let encoded = encode_datagram(300, b"inner");
        assert_eq!(split_datagram(&encoded), Some((300, &b"inner"[..])));
    }

    #[tokio::test]
    async fn concatenated_records_read_back_one_by_one() {
        let mut stream = Vec::new();
        encode_record(1, b"a", &mut stream);
        encode_record(70, &[7u8; 1200], &mut stream);
        encode_record(u64::from(u32::MAX), b"c", &mut stream);
        let mut r = &stream[..];
        assert_eq!(
            read_record(&mut r).await.expect("first"),
            Some((1, Bytes::from_static(b"a")))
        );
        assert_eq!(
            read_record(&mut r).await.expect("second"),
            Some((70, Bytes::from(vec![7u8; 1200])))
        );
        assert_eq!(
            read_record(&mut r).await.expect("third"),
            Some((u64::from(u32::MAX), Bytes::from_static(b"c")))
        );
        assert_eq!(read_record(&mut r).await.expect("end"), None);
    }

    #[tokio::test]
    async fn a_record_longer_than_1200_bytes_is_refused() {
        let mut stream = Vec::new();
        encode_varint(1, &mut stream).expect("tunnel");
        encode_varint(1201, &mut stream).expect("length");
        stream.extend_from_slice(&[0u8; 1201]);
        assert!(matches!(
            read_record(&mut &stream[..]).await,
            Err(Error::Protocol(m)) if m.contains("out of range")
        ));

        let mut empty = Vec::new();
        encode_varint(1, &mut empty).expect("tunnel");
        encode_varint(0, &mut empty).expect("length");
        assert!(matches!(
            read_record(&mut &empty[..]).await,
            Err(Error::Protocol(m)) if m.contains("out of range")
        ));
    }

    #[tokio::test]
    async fn a_record_cut_short_is_refused() {
        let mut stream = Vec::new();
        encode_record(1, b"abcdef", &mut stream);
        for cut in 1..stream.len() {
            assert!(
                matches!(
                    read_record(&mut &stream[..cut]).await,
                    Err(Error::Protocol(m)) if m.contains("truncated")
                ),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn the_carrier_rule_prefers_a_datagram_then_a_record_then_drops() {
        assert_eq!(carrier(1203, 1200, Some(1100)), Carrier::Record);
        assert_eq!(carrier(1203, 1200, Some(1300)), Carrier::Datagram);
        assert_eq!(carrier(1403, 1400, Some(1300)), Carrier::Drop);
        assert_eq!(carrier(1403, 1400, None), Carrier::Drop);
        assert_eq!(carrier(503, 500, None), Carrier::Record);
    }
}
