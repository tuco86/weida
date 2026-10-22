//! A simulated UDP network for tests: real QUIC and TLS over memory
//! ([decisions/0042](../../../docs/decisions/0042-simulated-network.md)).
//!
//! Give every runtime of one test the same [`SimNet`] through
//! `RuntimeConfig { network: Network::sim(&net), .. }`. Everything above the socket is the
//! production code: `bind_quic` binds a simulated address, `connect("weida://fp@127.0.0.1:port/..")`
//! dials it, the TLS handshake proves the same keys, mTLS refuses the same clients, and a redial
//! reports the same `PeerEvent`s. No socket is opened, so a test needs no network.
//!
//! **Addresses.** A port identifies one socket in a `SimNet`, whatever its IP. Port `0` takes
//! the next free port from 49152 upward. A port in use is `Error::Io` of kind `AddrInUse` until
//! its binding closes: `Binding::close`, or the runtime's `shutdown` or `drain`. A socket bound to
//! an unspecified IP is seen from `127.0.0.1` or `::1`, whichever family the receiver was
//! addressed in; a v4 source reaches a socket bound to a v6 address v4-mapped, as the OS presents
//! it. There is no name table: names go through `RuntimeConfig::resolver`, so a test dials IP
//! literals.
//!
//! **Faults** are keyed by port and apply to every datagram from or to it:
//! [`SimNet::set_delay`] holds datagrams for a one-way delay, [`SimNet::drop_next`] loses the next
//! few (QUIC retransmits them), and [`SimNet::partition`] loses everything until
//! [`SimNet::heal`]: an unannounced disconnect, which QUIC notices at `Limits::idle_timeout`.
//!
//! **Time.** Nothing here sleeps on its own: a delayed datagram is released by a Tokio timer, and
//! QUIC's own timers are Tokio's, so a test under `#[tokio::test(start_paused = true)]` spends
//! paused time, not wall time, on delays, idle timeouts and redial backoff. Reading the clock
//! and waiting on a paused clock cost no wall time and are fine in a unit test: a test under
//! `#[tokio::test(start_paused = true)]` that waits for a delay, an idle timeout or a redial
//! backoff is a unit test. A test that waits on a running clock is an integration test.
//!
//! **Bound.** A socket's receive buffer holds [`INBOX_DATAGRAMS`] datagrams; one arriving at a
//! full buffer is dropped and counted, as UDP drops past its receive buffer.
//!
//! `Binding::hand_over` needs an OS socket and panics on a simulated binding.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use weida_core::Error;

/// A simulated socket's receive buffer, in datagrams. A datagram that finds it full is dropped
/// and counted in [`SimStats::dropped`].
pub(crate) const INBOX_DATAGRAMS: usize = 4096;

/// The first port that port `0` hands out, and where the search wraps to.
const FIRST_EPHEMERAL: u16 = 49152;
/// Ports from [`FIRST_EPHEMERAL`] to 65535.
const EPHEMERAL_PORTS: u32 = 65536 - FIRST_EPHEMERAL as u32;

/// An in-memory UDP network: QUIC and TLS run over it unchanged, so identities, pinned
/// addresses, mTLS refusals, redials and their `PeerEvent`s are the real ones. Clone to share.
#[derive(Clone, Debug, Default)]
pub struct SimNet {
    inner: Arc<Net>,
}

/// What a [`SimNet`] did with the datagrams sent on it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimStats {
    /// Datagrams put into a receiving socket's buffer.
    pub delivered: u64,
    /// Datagrams lost to a fault, to no socket at the destination, or to a full buffer.
    pub dropped: u64,
}

#[derive(Debug)]
struct Net {
    state: Mutex<NetState>,
    delivered: AtomicU64,
    dropped: AtomicU64,
}

impl Default for Net {
    fn default() -> Self {
        Net {
            state: Mutex::new(NetState {
                sockets: HashMap::new(),
                faults: HashMap::new(),
                next_port: FIRST_EPHEMERAL,
            }),
            delivered: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
struct NetState {
    sockets: HashMap<u16, Weak<SimSocket>>,
    faults: HashMap<u16, Fault>,
    next_port: u16,
}

/// The faults on one port.
#[derive(Clone, Copy, Debug, Default)]
struct Fault {
    delay: Duration,
    drop_next: u64,
    partitioned: bool,
}

impl Net {
    fn lock(&self) -> MutexGuard<'_, NetState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn count_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

impl NetState {
    fn live(&self, port: u16) -> Option<Arc<SimSocket>> {
        self.sockets.get(&port).and_then(Weak::upgrade)
    }

    fn fault(&mut self, port: u16) -> &mut Fault {
        self.faults.entry(port).or_default()
    }
}

impl SimNet {
    /// A network with no sockets and no faults.
    pub fn new() -> SimNet {
        SimNet::default()
    }

    /// Holds every datagram to or from `addr` for `one_way`; `Duration::ZERO` clears it.
    ///
    /// A datagram between two delayed ports waits the larger delay, once. A delay applies to
    /// datagrams sent after the call.
    pub fn set_delay(&self, addr: SocketAddr, one_way: Duration) {
        self.inner.lock().fault(addr.port()).delay = one_way;
    }

    /// Drops the next `datagrams` datagrams to or from `addr` (added to any still pending).
    pub fn drop_next(&self, addr: SocketAddr, datagrams: u64) {
        let mut state = self.inner.lock();
        let fault = state.fault(addr.port());
        fault.drop_next = fault.drop_next.saturating_add(datagrams);
    }

    /// Drops every datagram to or from `addr` until [`SimNet::heal`]: an unannounced
    /// disconnect, which QUIC notices at `Limits::idle_timeout` (`LossCause::IdleTimeout`). For
    /// an announced one use `Binding::close` or `Binding::disconnect`.
    pub fn partition(&self, addr: SocketAddr) {
        self.inner.lock().fault(addr.port()).partitioned = true;
    }

    /// Ends a [`SimNet::partition`] of `addr`; its delay and drop count stay.
    pub fn heal(&self, addr: SocketAddr) {
        self.inner.lock().fault(addr.port()).partitioned = false;
    }

    /// The datagrams delivered and dropped so far.
    pub fn stats(&self) -> SimStats {
        SimStats {
            delivered: self.inner.delivered.load(Ordering::Relaxed),
            dropped: self.inner.dropped.load(Ordering::Relaxed),
        }
    }

    /// A bare `quinn` endpoint at `addr` on this network, accepting connections when `server`
    /// is given: a peer that speaks the wire protocol by hand, for a test that sends what weida
    /// never would. Port `0` takes the next free port, as [`Network::sim`] bindings do. Call it
    /// inside a Tokio runtime.
    ///
    /// [`Network::sim`]: crate::Network::sim
    pub fn endpoint(
        &self,
        addr: SocketAddr,
        server: Option<quinn::ServerConfig>,
    ) -> Result<quinn::Endpoint, Error> {
        let socket = self.bind(addr)?;
        quinn::Endpoint::new_with_abstract_socket(
            quinn::EndpointConfig::default(),
            server,
            socket,
            Arc::new(quinn::TokioRuntime),
        )
        .map_err(Error::Io)
    }

    /// Binds a socket at `addr`; port `0` takes the next free port.
    pub(crate) fn bind(&self, addr: SocketAddr) -> Result<Arc<SimSocket>, Error> {
        let mut state = self.inner.lock();
        let port = if addr.port() == 0 {
            let mut candidate = state.next_port;
            let mut found = None;
            for _ in 0..EPHEMERAL_PORTS {
                if state.live(candidate).is_none() {
                    found = Some(candidate);
                    break;
                }
                candidate = next_ephemeral(candidate);
            }
            let Some(port) = found else {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "no free port on this simulated network",
                )));
            };
            state.next_port = next_ephemeral(port);
            port
        } else {
            if state.live(addr.port()).is_some() {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("{addr} is in use on this simulated network"),
                )));
            }
            addr.port()
        };
        let socket = Arc::new(SimSocket {
            net: Arc::clone(&self.inner),
            addr: SocketAddr::new(addr.ip(), port),
            inbox: Mutex::new(Inbox::default()),
            released: AtomicBool::new(false),
        });
        state.sockets.insert(port, Arc::downgrade(&socket));
        Ok(socket)
    }
}

fn next_ephemeral(port: u16) -> u16 {
    if port == u16::MAX {
        FIRST_EPHEMERAL
    } else {
        port + 1
    }
}

/// A v4-mapped v6 address as the v4 address it names; anything else as it is.
fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 => v4,
    }
}

fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(canonical_ip(addr.ip()), addr.port())
}

/// Whether a socket bound to `bound` receives a datagram addressed to `dst` (canonical).
fn accepts(bound: IpAddr, dst: IpAddr) -> bool {
    match bound {
        IpAddr::V6(v6) if v6.is_unspecified() => true,
        IpAddr::V4(v4) if v4.is_unspecified() => dst.is_ipv4(),
        specific => canonical_ip(specific) == dst,
    }
}

/// The source a receiver bound to `receiver` sees for a datagram from a socket bound to
/// `sender`, addressed to `dst` (canonical).
fn source_seen(sender: SocketAddr, dst: SocketAddr, receiver: IpAddr) -> SocketAddr {
    let ip = if sender.ip().is_unspecified() {
        match dst.ip() {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        }
    } else {
        canonical_ip(sender.ip())
    };
    let ip = match (receiver, ip) {
        (IpAddr::V6(_), IpAddr::V4(v4)) => IpAddr::V6(v4.to_ipv6_mapped()),
        (_, ip) => ip,
    };
    SocketAddr::new(ip, sender.port())
}

/// One socket on a [`SimNet`]: what a simulated binding or client endpoint reads and sends on.
pub(crate) struct SimSocket {
    net: Arc<Net>,
    addr: SocketAddr,
    inbox: Mutex<Inbox>,
    released: AtomicBool,
}

#[derive(Default)]
struct Inbox {
    queue: VecDeque<Datagram>,
    waker: Option<Waker>,
    /// Armed while the oldest datagram is still delayed.
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

struct Datagram {
    from: SocketAddr,
    release: tokio::time::Instant,
    contents: Vec<u8>,
}

impl SimSocket {
    /// Frees this socket's port and stops it reading and sending: the end of its binding.
    /// Idempotent; a port already taken by a newer socket stays that socket's.
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.unregister();
        let mut inbox = self.inbox();
        inbox.queue.clear();
        inbox.timer = None;
        inbox.waker = None;
    }

    fn unregister(&self) {
        let mut state = self.net.lock();
        let ours = state
            .sockets
            .get(&self.addr.port())
            .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self));
        if ours {
            state.sockets.remove(&self.addr.port());
        }
    }

    fn inbox(&self) -> MutexGuard<'_, Inbox> {
        self.inbox.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sends one datagram to `destination`, or counts why it was lost.
    fn send_one(&self, destination: SocketAddr, contents: &[u8]) {
        let dst = canonical(destination);
        let (receiver, delay) = {
            let mut state = self.net.lock();
            let source = state
                .faults
                .get(&self.addr.port())
                .copied()
                .unwrap_or_default();
            let target = state.faults.get(&dst.port()).copied().unwrap_or_default();
            if source.partitioned || target.partitioned {
                drop(state);
                self.net.count_dropped();
                return;
            }
            if source.drop_next > 0 {
                state.fault(self.addr.port()).drop_next -= 1;
                drop(state);
                self.net.count_dropped();
                return;
            }
            if target.drop_next > 0 {
                state.fault(dst.port()).drop_next -= 1;
                drop(state);
                self.net.count_dropped();
                return;
            }
            let receiver = state
                .live(dst.port())
                .filter(|socket| accepts(socket.addr.ip(), dst.ip()));
            (receiver, source.delay.max(target.delay))
        };
        let Some(receiver) = receiver else {
            self.net.count_dropped();
            return;
        };
        let from = source_seen(self.addr, dst, receiver.addr.ip());
        receiver.deliver(Datagram {
            from,
            release: tokio::time::Instant::now() + delay,
            contents: contents.to_vec(),
        });
    }

    fn deliver(&self, datagram: Datagram) {
        let mut inbox = self.inbox();
        if self.released.load(Ordering::Acquire) || inbox.queue.len() >= INBOX_DATAGRAMS {
            drop(inbox);
            self.net.count_dropped();
            return;
        }
        inbox.queue.push_back(datagram);
        let waker = inbox.waker.take();
        drop(inbox);
        self.net.delivered.fetch_add(1, Ordering::Relaxed);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Drop for SimSocket {
    fn drop(&mut self) {
        self.unregister();
    }
}

impl AsyncUdpSocket for SimSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(SimPoller)
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        // A closed binding's socket is gone: what its endpoint still sends goes nowhere.
        if self.released.load(Ordering::Acquire) {
            self.net.count_dropped();
            return Ok(());
        }
        // A GSO batch is several datagrams of `segment_size` bytes, the last one possibly
        // shorter; each travels on its own.
        let size = transmit.segment_size.unwrap_or(transmit.contents.len());
        for contents in transmit.contents.chunks(size.max(1)) {
            self.send_one(transmit.destination, contents);
        }
        // Memory is never full the way a socket buffer is: a send never blocks.
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
        let mut inbox = self.inbox();
        loop {
            let now = tokio::time::Instant::now();
            let mut filled = 0;
            while filled < bufs.len().min(meta.len()) {
                let Some(head) = inbox.queue.front() else {
                    break;
                };
                if head.release > now {
                    break;
                }
                let datagram = inbox.queue.pop_front().expect("the head was just read");
                let len = datagram.contents.len().min(bufs[filled].len());
                bufs[filled][..len].copy_from_slice(&datagram.contents[..len]);
                meta[filled] = RecvMeta {
                    addr: datagram.from,
                    len,
                    stride: len,
                    ecn: None,
                    dst_ip: None,
                };
                filled += 1;
            }
            if filled > 0 {
                inbox.timer = None;
                return Poll::Ready(Ok(filled));
            }
            let Some(release) = inbox.queue.front().map(|head| head.release) else {
                break;
            };
            let timer = match &mut inbox.timer {
                Some(timer) => {
                    if timer.deadline() != release {
                        timer.as_mut().reset(release);
                    }
                    timer
                }
                none => none.insert(Box::pin(tokio::time::sleep_until(release))),
            };
            if timer.as_mut().poll(cx).is_pending() {
                break;
            }
        }
        inbox.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }
}

impl fmt::Debug for SimSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimSocket")
            .field("addr", &self.addr)
            .field("released", &self.released.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// A simulated socket is always writable.
#[derive(Debug)]
struct SimPoller;

impl UdpPoller for SimPoller {
    fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("address")
    }

    fn send(socket: &SimSocket, to: SocketAddr, contents: &[u8]) {
        socket
            .try_send(&Transmit {
                destination: to,
                ecn: None,
                contents,
                segment_size: None,
                src_ip: None,
            })
            .expect("a simulated send never fails");
    }

    /// What one poll finds: the sender and the bytes of the first datagram, or nothing.
    fn recv(socket: &SimSocket) -> Option<(SocketAddr, Vec<u8>)> {
        let mut buf = [0u8; 64];
        let mut bufs = [IoSliceMut::new(&mut buf)];
        let mut meta = [RecvMeta::default()];
        let mut cx = Context::from_waker(Waker::noop());
        match socket.poll_recv(&mut cx, &mut bufs, &mut meta) {
            Poll::Ready(Ok(1)) => Some((meta[0].addr, buf[..meta[0].len].to_vec())),
            Poll::Pending => None,
            other => panic!("unexpected poll result {other:?}"),
        }
    }

    #[test]
    fn a_datagram_reaches_the_socket_of_its_port_in_the_receivers_family() {
        let net = SimNet::new();
        let server = net.bind(addr("127.0.0.1:0")).expect("server");
        let client = net.bind(addr("[::]:0")).expect("client");
        let (server_addr, client_port) = (server.addr, client.addr.port());
        assert_ne!(server_addr.port(), client_port);

        // A v6 socket addresses a v4 peer v4-mapped, as quinn does.
        send(
            &client,
            SocketAddr::new(
                IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
                server_addr.port(),
            ),
            b"hello",
        );
        let (from, bytes) = recv(&server).expect("the server received it");
        assert_eq!(
            from,
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), client_port)
        );
        assert_eq!(bytes, b"hello");

        send(&server, from, b"back");
        let (from, bytes) = recv(&client).expect("the client received the reply");
        assert_eq!(
            from,
            SocketAddr::new(
                IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
                server_addr.port()
            )
        );
        assert_eq!(bytes, b"back");
        assert_eq!(
            net.stats(),
            SimStats {
                delivered: 2,
                dropped: 0
            }
        );
    }

    #[test]
    fn a_partition_and_a_drop_count_lose_datagrams_and_heal_restores_them() {
        let net = SimNet::new();
        let server = net.bind(addr("127.0.0.1:0")).expect("server");
        let client = net.bind(addr("127.0.0.1:0")).expect("client");
        let to = server.addr;

        net.drop_next(to, 1);
        send(&client, to, b"one");
        assert_eq!(recv(&server), None, "the first send is dropped");
        send(&client, to, b"two");
        assert_eq!(recv(&server).expect("delivered").1, b"two");

        net.partition(to);
        send(&client, to, b"three");
        assert_eq!(recv(&server), None, "a partition loses everything");
        net.heal(to);
        send(&client, to, b"four");
        assert_eq!(recv(&server).expect("delivered after heal").1, b"four");

        assert_eq!(net.stats().dropped, 2);
    }

    #[test]
    fn a_port_is_taken_until_its_socket_is_released() {
        let net = SimNet::new();
        let first = net.bind(addr("127.0.0.1:0")).expect("first");
        let taken = first.addr;
        match net.bind(taken) {
            Err(Error::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::AddrInUse),
            other => panic!("expected AddrInUse, got {other:?}"),
        }
        first.release();
        let second = net.bind(taken).expect("the port is free after release");
        assert_eq!(second.addr, taken);
        // The old socket's release does not free the new one's port.
        first.release();
        assert!(net.bind(taken).is_err(), "the new socket still holds it");
    }
}
