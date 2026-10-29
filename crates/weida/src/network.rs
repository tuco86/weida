//! Where a runtime's QUIC sockets come from
//! ([decisions/0042](../../../docs/decisions/0042-simulated-network.md)).

use std::io::{self, IoSliceMut};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::task::{Context, Poll, ready};

use quinn::udp::{RecvMeta, Transmit};
use tokio::io::Interest;

use weida_core::Error;

use crate::runtime::Exec;

/// Where a runtime's QUIC sockets live: the operating system's UDP (the default) or, with the
/// cargo feature `test-util`, a `SimNet` shared by the runtimes of one test. On a `SimNet` a
/// `weida+unix` binding and dial are simulated too (by path, with no socket file);
/// `weida+inproc` and `weida+pipe` are not affected.
#[derive(Clone, Debug, Default)]
pub struct Network {
    kind: Kind,
}

#[derive(Clone, Debug, Default)]
enum Kind {
    #[default]
    Os,
    #[cfg(feature = "test-util")]
    Sim(crate::sim::SimNet),
}

impl Network {
    /// The simulated network `net`: give every runtime of one test the same one.
    #[cfg(feature = "test-util")]
    pub fn sim(net: &crate::SimNet) -> Network {
        Network {
            kind: Kind::Sim(net.clone()),
        }
    }

    /// The simulated network, where this is one: what a `weida+unix` binding and dial use.
    #[cfg(all(unix, feature = "test-util"))]
    pub(crate) fn sim_net(&self) -> Option<&crate::sim::SimNet> {
        match &self.kind {
            Kind::Os => None,
            Kind::Sim(net) => Some(net),
        }
    }

    /// The socket a QUIC binding on `addr` serves on.
    pub(crate) fn bind_server(&self, addr: SocketAddr, exec: &Exec) -> Result<BoundSocket, Error> {
        match &self.kind {
            Kind::Os => BoundSocket::os(std::net::UdpSocket::bind(addr).map_err(Error::Io)?, exec),
            #[cfg(feature = "test-util")]
            Kind::Sim(net) => {
                let socket = net.bind(addr)?;
                let _guard = exec.enter();
                Ok(BoundSocket {
                    inner: Arc::clone(&socket) as Arc<dyn quinn::AsyncUdpSocket>,
                    runtime: quic_runtime()?,
                    lease: AddrLease::Sim(socket),
                })
            }
        }
    }

    /// The runtime's one client endpoint.
    pub(crate) fn bind_client(&self, exec: &Exec) -> Result<quinn::Endpoint, Error> {
        match &self.kind {
            Kind::Os => bind_os_client(exec),
            #[cfg(feature = "test-util")]
            Kind::Sim(net) => {
                let socket = net.bind(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))?;
                let _guard = exec.enter();
                quinn::Endpoint::new_with_abstract_socket(
                    quinn::EndpointConfig::default(),
                    None,
                    socket,
                    quic_runtime()?,
                )
                .map_err(Error::Io)
            }
        }
    }
}

/// `quinn`'s runtime for the reactor the caller entered.
fn quic_runtime() -> Result<Arc<dyn quinn::Runtime>, Error> {
    quinn::default_runtime()
        .ok_or_else(|| Error::Runtime("no async runtime for the QUIC endpoint".into()))
}

/// Binds the shared client socket, preferring dual-stack IPv6.
///
/// Inside the runtime context: `quinn` registers the socket with the reactor
/// as it is constructed, and the caller's thread may have none.
fn bind_os_client(exec: &Exec) -> Result<quinn::Endpoint, Error> {
    let _guard = exec.enter();
    let v6 = SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0));
    match quinn::Endpoint::client(v6) {
        Ok(endpoint) => Ok(endpoint),
        Err(e) => {
            tracing::debug!(error = %e, "IPv6 client socket unavailable; falling back to IPv4");
            quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).map_err(Error::Io)
        }
    }
}

/// A socket a QUIC binding serves on.
pub(crate) struct BoundSocket {
    pub(crate) inner: Arc<dyn quinn::AsyncUdpSocket>,
    pub(crate) runtime: Arc<dyn quinn::Runtime>,
    pub(crate) lease: AddrLease,
}

impl BoundSocket {
    /// An OS socket wrapped for `quinn`, with the duplicate a handover passes on.
    pub(crate) fn os(socket: std::net::UdpSocket, exec: &Exec) -> Result<BoundSocket, Error> {
        // Inside the runtime context: the socket is registered with the
        // reactor as it is wrapped, and the calling thread may have none.
        let _guard = exec.enter();
        let runtime = quic_runtime()?;
        let socket = OsSocket::new(socket).map_err(Error::Io)?;
        Ok(BoundSocket {
            inner: Arc::clone(&socket) as Arc<dyn quinn::AsyncUdpSocket>,
            runtime,
            lease: AddrLease::Os(socket),
        })
    }
}

/// Frees a binding's address when it closes: closes the OS socket and its
/// handover duplicate, or frees the simulated address.
#[derive(Clone, Debug)]
pub(crate) enum AddrLease {
    Os(Arc<OsSocket>),
    #[cfg(feature = "test-util")]
    Sim(Arc<crate::sim::SimSocket>),
}

impl AddrLease {
    pub(crate) fn release(&self) {
        match self {
            AddrLease::Os(socket) => socket.release(),
            #[cfg(feature = "test-util")]
            AddrLease::Sim(socket) => socket.release(),
        }
    }

    /// The duplicate a handover passes on; `None` on a simulated network, once
    /// taken, and once released.
    pub(crate) fn take_handed(&self) -> Option<std::net::UdpSocket> {
        match self {
            AddrLease::Os(socket) => socket.take_handed(),
            #[cfg(feature = "test-util")]
            AddrLease::Sim(_) => None,
        }
    }
}

/// A future that resolves once a socket is writable.
type Writable = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;

/// A binding's OS UDP socket as `quinn` sees it: what `quinn`'s Tokio runtime
/// wraps a socket into, plus [`OsSocket::release`], which closes the socket
/// while `quinn` still holds this wrapper. `quinn` keeps a connection's socket
/// as long as any handle on that connection lives; without the release a
/// closed binding's port stays taken by whatever still holds one.
pub(crate) struct OsSocket {
    /// `None` once released. Shared only with the writability waits of the
    /// pollers, which `release` clears.
    io: RwLock<Option<Arc<tokio::net::UdpSocket>>>,
    state: quinn::udp::UdpSocketState,
    /// Read at bind time, so it is still answered after the release.
    local_addr: SocketAddr,
    /// The duplicate a handover passes on.
    handed: Mutex<Option<std::net::UdpSocket>>,
    /// The wait slot of every poller made for this socket.
    pollers: Mutex<Vec<Weak<Mutex<Option<Writable>>>>>,
}

impl OsSocket {
    /// Wraps `socket`, inside the runtime context: it is registered with the
    /// reactor here.
    pub(crate) fn new(socket: std::net::UdpSocket) -> io::Result<Arc<OsSocket>> {
        let handed = socket.try_clone()?;
        // Also makes the socket non-blocking.
        let state = quinn::udp::UdpSocketState::new((&socket).into())?;
        let local_addr = socket.local_addr()?;
        let io = tokio::net::UdpSocket::from_std(socket)?;
        Ok(Arc::new(OsSocket {
            io: RwLock::new(Some(Arc::new(io))),
            state,
            local_addr,
            handed: Mutex::new(Some(handed)),
            pollers: Mutex::new(Vec::new()),
        }))
    }

    pub(crate) fn take_handed(&self) -> Option<std::net::UdpSocket> {
        self.handed.lock().expect("handed socket poisoned").take()
    }

    /// Closes the socket and its duplicate; from then on sends are dropped
    /// and nothing is received. Idempotent.
    ///
    /// A poll that cloned the socket before the first step stored its wait
    /// under its slot's lock, which the third step takes afterwards and
    /// clears; a poll after the first step finds `None`. So once the last
    /// step drops it, nothing holds the socket.
    pub(crate) fn release(&self) {
        let io = self.io.write().expect("socket poisoned").take();
        drop(self.handed.lock().expect("handed socket poisoned").take());
        let pollers = std::mem::take(&mut *self.pollers.lock().expect("poller list poisoned"));
        for slot in pollers.iter().filter_map(Weak::upgrade) {
            slot.lock().expect("poller slot poisoned").take();
        }
        drop(io);
    }
}

impl std::fmt::Debug for OsSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OsSocket")
            .field("local_addr", &self.local_addr)
            .field(
                "released",
                &self.io.read().expect("socket poisoned").is_none(),
            )
            .finish_non_exhaustive()
    }
}

impl quinn::AsyncUdpSocket for OsSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn quinn::UdpPoller>> {
        let waiting = Arc::new(Mutex::new(None));
        {
            let mut pollers = self.pollers.lock().expect("poller list poisoned");
            pollers.retain(|w| w.strong_count() > 0);
            pollers.push(Arc::downgrade(&waiting));
        }
        Box::pin(OsPoller {
            socket: self,
            waiting,
        })
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        match &*self.io.read().expect("socket poisoned") {
            Some(io) => io.try_io(Interest::WRITABLE, || {
                self.state.send((&**io).into(), transmit)
            }),
            // A released socket drops what is sent, as a released `SimSocket` does.
            None => Ok(()),
        }
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let guard = self.io.read().expect("socket poisoned");
        // A released socket receives nothing. `Pending` rather than an error,
        // which `quinn` would log on every close; its driver ends once its
        // handles are gone.
        let Some(io) = &*guard else {
            return Poll::Pending;
        };
        loop {
            ready!(io.poll_recv_ready(cx))?;
            if let Ok(n) = io.try_io(Interest::READABLE, || {
                self.state.recv((&**io).into(), bufs, meta)
            }) {
                return Poll::Ready(Ok(n));
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    fn may_fragment(&self) -> bool {
        self.state.may_fragment()
    }

    fn max_transmit_segments(&self) -> usize {
        self.state.max_gso_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.state.gro_segments()
    }
}

/// One writability wait on an [`OsSocket`]. A future per poller rather than
/// `tokio::net::UdpSocket::poll_send_ready`, which keeps only the last
/// waker: every connection has a poller of its own.
struct OsPoller {
    socket: Arc<OsSocket>,
    waiting: Arc<Mutex<Option<Writable>>>,
}

impl std::fmt::Debug for OsPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OsPoller").finish_non_exhaustive()
    }
}

impl quinn::UdpPoller for OsPoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let mut waiting = self.waiting.lock().expect("poller slot poisoned");
        let wait = match &mut *waiting {
            Some(wait) => wait,
            None => {
                let Some(io) = self.socket.io.read().expect("socket poisoned").clone() else {
                    // Released: sends are dropped, so the socket never blocks.
                    return Poll::Ready(Ok(()));
                };
                waiting.insert(Box::pin(async move { io.writable().await }))
            }
        };
        let result = ready!(wait.as_mut().poll(cx));
        *waiting = None;
        Poll::Ready(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        #[tokio::test]
        async fn the_client_socket_binds() {
            let exec = Exec::current().expect("ambient runtime");
            let endpoint = Network::default().bind_client(&exec).unwrap();
            assert_ne!(endpoint.local_addr().unwrap().port(), 0);
        }
    }
}
