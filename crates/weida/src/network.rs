//! Where a runtime's QUIC sockets come from
//! ([decisions/0042](../../../docs/decisions/0042-simulated-network.md)).

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

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
                    handed: None,
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
    /// The duplicate a handover passes on; `None` on a simulated network.
    pub(crate) handed: Option<std::net::UdpSocket>,
    pub(crate) lease: AddrLease,
}

impl BoundSocket {
    /// An OS socket: the duplicate a handover passes on, and the original wrapped for `quinn`.
    pub(crate) fn os(socket: std::net::UdpSocket, exec: &Exec) -> Result<BoundSocket, Error> {
        let handed = socket.try_clone().map_err(Error::Io)?;
        // Inside the runtime context: `quinn` registers the socket with the
        // reactor as it is constructed, and the calling thread may have none.
        let _guard = exec.enter();
        let runtime = quic_runtime()?;
        let inner = runtime.wrap_udp_socket(socket).map_err(Error::Io)?;
        Ok(BoundSocket {
            inner,
            runtime,
            handed: Some(handed),
            lease: AddrLease::Os,
        })
    }
}

/// Frees a simulated address when its binding closes; the OS frees its own.
#[derive(Clone, Debug)]
pub(crate) enum AddrLease {
    Os,
    #[cfg(feature = "test-util")]
    Sim(Arc<crate::sim::SimSocket>),
}

impl AddrLease {
    pub(crate) fn release(&self) {
        match self {
            AddrLease::Os => {}
            #[cfg(feature = "test-util")]
            AddrLease::Sim(socket) => socket.release(),
        }
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
