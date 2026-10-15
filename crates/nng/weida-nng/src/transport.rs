//! The one place this crate knows what a connection is made of.
//!
//! An enum rather than a trait object, for the reason weida's own transport
//! boundary gives (`docs/ARCHITECTURE.md` §5): the set of transports is
//! closed, small and decided in this crate, and the payload path must stay a
//! direct call. Above it the SP session is written once and cannot tell
//! which transport carried its octets, which is why the protocol is tested
//! once and runs over all of them.
//!
//! Three of the four this library implements are here — `tcp`, `ipc` and
//! `inproc`; `tls+tcp` is carried by `tls.rs`. A transport this library does
//! not carry is refused by name rather than accepted and dropped somewhere
//! quieter, because a refusal SP can express is a close and a refusal it
//! cannot is a hang (§6).

use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use weida_core::LocalPrincipal;

/// How long a TCP connection may carry nothing in either direction before
/// the kernel starts probing it.
///
/// **NNG's `NNG_OPT_TCP_KEEPALIVE` is off by default and this is not**, for
/// the reason SP gives a dead peer no other way to be found: SP has no
/// heartbeat, and a PULL or REP socket may never write to a peer at all, so
/// a peer that vanished without a FIN — a crash, a dropped NAT mapping —
/// would hold its pipe slot for ever. The probes are TCP's own, invisible
/// to a peer that is alive, and sixty seconds is far enough out that a
/// quiet but healthy connection sends nothing.
pub const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(60);

/// The gap between keepalive probes, on the platforms that let it be set.
/// Elsewhere the platform's own is used.
pub const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// What a listener counts a connection against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PeerKey {
    /// A remote host, under the key [`weida_core::host_key`] gives it: its IPv4
    /// address, or the `/64` an IPv6 address belongs to, because an IPv6 host
    /// is given a whole `/64` and a count per address would be a count per
    /// attacker-chosen suffix. `weida-zmq` counts a host the same way.
    ///
    /// Loopback is counted like any other host, as an `ipc://` peer is
    /// counted under its uid: a local process holds pipes against
    /// `max_pipes_per_peer` too. `weida-zmq` exempts it.
    Host(IpAddr),
    /// The user id the kernel attributed to an `ipc://` peer.
    User(u32),
}

/// What every TCP connection of this crate is set up with, however it was
/// made.
///
/// Nagle is disabled: SP's request-reply and survey patterns are
/// round-trip shaped, and a 40-millisecond delay on a four-octet tag stack
/// is the difference between a working benchmark and a mystery. Keepalive
/// is enabled ([`TCP_KEEPALIVE_IDLE`]). A failure to set either is not
/// fatal — the connection works, with less — so it is logged rather than
/// thrown.
pub(crate) fn tune_tcp(stream: &TcpStream) {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(%error, "could not disable Nagle on an SP connection");
    }
    let probes = socket2::TcpKeepalive::new().with_time(TCP_KEEPALIVE_IDLE);
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
        target_os = "windows",
    ))]
    let probes = probes.with_interval(TCP_KEEPALIVE_INTERVAL);
    if let Err(error) = socket2::SockRef::from(stream).set_tcp_keepalive(&probes) {
        tracing::debug!(%error, "could not enable keepalive on an SP connection");
    }
}

/// One established connection's byte stream.
#[derive(Debug)]
pub struct Stream(Inner);

#[derive(Debug)]
enum Inner {
    Tcp(TcpStream),
    /// Boxed because a TLS session's buffers make this variant an order of
    /// magnitude larger than the others, and every `Stream` would
    /// otherwise carry that weight.
    Tls {
        stream: Box<tokio_rustls::TlsStream<TcpStream>>,
        peer: crate::tls::TlsPeer,
    },
    Inproc(DuplexStream),
    #[cfg(unix)]
    Unix {
        stream: UnixStream,
        /// Captured once, when the connection was made, because that is
        /// when the kernel's answer is about the process that connected.
        principal: LocalPrincipal,
    },
}

impl Stream {
    /// Wraps an established TCP connection, set up as `tune_tcp` does.
    pub fn tcp(stream: TcpStream) -> Stream {
        tune_tcp(&stream);
        Stream(Inner::Tcp(stream))
    }

    /// Wraps one half of an in-process connection.
    pub fn inproc(stream: DuplexStream) -> Stream {
        Stream(Inner::Inproc(stream))
    }

    /// Wraps an established `AF_UNIX` connection together with the
    /// credentials the kernel attributed to its peer.
    ///
    /// The credentials are taken by the caller at connection time rather
    /// than read here on demand: the kernel's answer is about the process
    /// that connected, and asking later would ask about whatever holds
    /// that pid now
    /// ([0010](../../../docs/decisions/0010-local-transport.md) §4.4).
    #[cfg(unix)]
    pub fn unix(stream: UnixStream, principal: LocalPrincipal) -> Stream {
        Stream(Inner::Unix { stream, principal })
    }

    /// Wraps an established TLS connection together with what the
    /// handshake established about its peer.
    pub fn tls(stream: tokio_rustls::TlsStream<TcpStream>, peer: crate::tls::TlsPeer) -> Stream {
        Stream(Inner::Tls {
            stream: Box::new(stream),
            peer,
        })
    }

    /// What TLS established about this connection's peer, for the one
    /// transport where that question has an answer.
    ///
    /// It authenticates the transport peer of this one connection and
    /// terminates there; see [`crate::tls`].
    pub fn tls_peer(&self) -> Option<crate::tls::TlsPeer> {
        match &self.0 {
            Inner::Tls { peer, .. } => Some(peer.clone()),
            _ => None,
        }
    }

    /// The credentials the kernel attributes to this connection's peer.
    ///
    /// `None` for every transport where there is no such fact: TCP has an
    /// address and no process, and `inproc` has no kernel at all. `None`
    /// rather than a zeroed principal, which would read as root.
    pub fn peer_credentials(&self) -> Option<weida_core::LocalPrincipal> {
        match &self.0 {
            #[cfg(unix)]
            Inner::Unix { principal, .. } => Some(*principal),
            _ => None,
        }
    }

    /// The transport this stream came over, for a log line and for
    /// `NNG_OPT_URL`'s scheme.
    pub const fn transport(&self) -> &'static str {
        match &self.0 {
            Inner::Tcp(_) => "tcp",
            Inner::Tls { .. } => "tls+tcp",
            Inner::Inproc(_) => "inproc",
            #[cfg(unix)]
            Inner::Unix { .. } => "ipc",
        }
    }

    /// This end's address, as `NNG_OPT_LOCADDR` reports it, or `None` for a
    /// transport with no address.
    pub fn local_addr(&self) -> Option<String> {
        match &self.0 {
            Inner::Tcp(stream) => stream.local_addr().ok().map(|a| a.to_string()),
            Inner::Tls { stream, .. } => {
                stream.get_ref().0.local_addr().ok().map(|a| a.to_string())
            }
            Inner::Inproc(_) => None,
            #[cfg(unix)]
            Inner::Unix { stream, .. } => stream
                .local_addr()
                .ok()
                .and_then(|a| a.as_pathname().map(|p| p.display().to_string())),
        }
    }

    /// The peer's address, as `NNG_OPT_REMADDR` reports it.
    ///
    /// An `AF_UNIX` peer that did not bind a path of its own has none,
    /// which is the usual case for a dialler, and `None` says so rather
    /// than inventing one.
    pub fn remote_addr(&self) -> Option<String> {
        match &self.0 {
            Inner::Tcp(stream) => stream.peer_addr().ok().map(|a| a.to_string()),
            Inner::Tls { stream, .. } => stream.get_ref().0.peer_addr().ok().map(|a| a.to_string()),
            Inner::Inproc(_) => None,
            #[cfg(unix)]
            Inner::Unix { stream, .. } => stream
                .peer_addr()
                .ok()
                .and_then(|a| a.as_pathname().map(|p| p.display().to_string())),
        }
    }

    /// What a listener counts this connection against: the host it came
    /// from for a network transport, the user the kernel attributed it to
    /// for `ipc`, and nothing for `inproc`, whose peers are this process's
    /// own.
    pub fn peer_key(&self) -> Option<PeerKey> {
        match &self.0 {
            Inner::Tcp(stream) => stream
                .peer_addr()
                .ok()
                .map(|a| PeerKey::Host(weida_core::host_key(a.ip()).key)),
            Inner::Tls { stream, .. } => stream
                .get_ref()
                .0
                .peer_addr()
                .ok()
                .map(|a| PeerKey::Host(weida_core::host_key(a.ip()).key)),
            Inner::Inproc(_) => None,
            #[cfg(unix)]
            Inner::Unix { principal, .. } => Some(PeerKey::User(principal.uid)),
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            Inner::Tls { stream, .. } => Pin::new(stream.as_mut()).poll_read(cx, buf),
            Inner::Inproc(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            Inner::Tls { stream, .. } => Pin::new(stream.as_mut()).poll_write(cx, buf),
            Inner::Inproc(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            Inner::Tls { stream, .. } => Pin::new(stream.as_mut()).poll_flush(cx),
            Inner::Inproc(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            Inner::Tls { stream, .. } => Pin::new(stream.as_mut()).poll_shutdown(cx),
            Inner::Inproc(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
