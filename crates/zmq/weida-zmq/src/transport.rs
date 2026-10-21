//! The one place this crate knows what a connection is made of.
//!
//! ZeroMQ's working set is `tcp`, `ipc` and `inproc`
//! (`docs/research/zeromq.md` §13), and all three are carried here. `ipc` is
//! `#[cfg(unix)]`: `AF_UNIX` is not a transport a platform can be given, so
//! on a platform without it the engine **refuses** the endpoint with
//! `EPROTONOSUPPORT` rather than holding a variant that cannot carry bytes.
//!
//! `inproc` is a [`tokio::io::duplex`] pair rather than a socket, which is
//! the whole of "passes messages via memory directly between threads sharing
//! a single 0MQ context, involving no I/O threads" (§12): above this enum the
//! session code cannot tell the difference, and that is why the protocol is
//! tested once and runs over both.
//!
//! An enum rather than a trait object, for the reason weida's own transport
//! boundary gives (`docs/ARCHITECTURE.md` §5): the set of transports is
//! closed, small and decided in this crate, and the payload path must stay a
//! direct call.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use weida_core::LocalPrincipal;

use crate::options::SocketOptions;

/// One established connection's byte stream.
///
/// `AsyncRead + AsyncWrite`, so the session layer above it is written once
/// and does not know which transport carried it — which is what makes the
/// ZMTP session testable against a plain `TcpStream` peer.
#[derive(Debug)]
pub struct Stream(Inner);

#[derive(Debug)]
enum Inner {
    Tcp(TcpStream),
    Inproc(DuplexStream),
    #[cfg(unix)]
    Unix {
        stream: UnixStream,
        /// Captured once, when the connection was made, because that is when
        /// the kernel takes the snapshot.
        principal: LocalPrincipal,
    },
}

impl Stream {
    /// Wraps an established TCP connection.
    pub fn tcp(stream: TcpStream) -> Stream {
        Stream(Inner::Tcp(stream))
    }

    /// Wraps one half of an in-process connection.
    pub fn inproc(stream: DuplexStream) -> Stream {
        Stream(Inner::Inproc(stream))
    }

    /// Wraps an established `AF_UNIX` connection, capturing the peer's
    /// credentials as the kernel reports them now.
    ///
    /// Fails when the kernel refuses to answer, which is a connection that
    /// has already gone: a local peer whose credentials cannot be read is not
    /// a peer this library carries, because for `ipc` the kernel's answer is
    /// the only thing there is to know about who is on the other end
    /// ([0010](../../../docs/decisions/0010-local-transport.md) §4.4).
    #[cfg(unix)]
    pub fn unix(stream: UnixStream) -> crate::error::Result<Stream> {
        let principal = crate::ipc::credentials(&stream)?;
        Ok(Stream(Inner::Unix { stream, principal }))
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

    /// Disables Nagle's algorithm, which a request-reply pattern on loopback
    /// notices and a bulk one does not.
    ///
    /// Nothing to do for `inproc`: there is no kernel between the halves, so
    /// there is no algorithm to disable. Reported as success rather than as
    /// an error, because the caller asked for a property that already holds.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        match &self.0 {
            Inner::Tcp(stream) => stream.set_nodelay(nodelay),
            #[cfg(unix)]
            Inner::Unix { .. } => Ok(()),
            Inner::Inproc(_) => Ok(()),
        }
    }

    /// Applies `ZMQ_TCP_KEEPALIVE` and its three tuning options to a TCP
    /// connection, which is what libzmq does to every one it accepts or dials.
    ///
    /// `None` is libzmq's `-1`: `SO_KEEPALIVE` stays as the operating system
    /// has it, whatever the tuning options say (they are refused with it, see
    /// [`SocketOptions::validate`]). `Some(false)` switches it off.
    /// `Some(true)` switches it on, with the idle time, the probe interval and
    /// the probe count the options name, and the operating system's own for
    /// each one they leave `None`.
    ///
    /// Nothing to do for `ipc` and `inproc`, which have no kernel probe; the
    /// caller asked for a property of a TCP connection, so it is reported as
    /// success as [`Stream::set_nodelay`] reports it.
    pub fn set_keepalive(&self, options: &SocketOptions) -> io::Result<()> {
        let Inner::Tcp(stream) = &self.0 else {
            return Ok(());
        };
        let socket = SockRef::from(stream);
        match options.tcp_keepalive {
            None => Ok(()),
            Some(false) => socket.set_keepalive(false),
            Some(true) => socket.set_tcp_keepalive(&probes(options)),
        }
    }

    /// The TCP connection underneath, for a test that reads back what was set
    /// on it.
    #[cfg(test)]
    pub(crate) fn as_tcp(&self) -> Option<&TcpStream> {
        match &self.0 {
            Inner::Tcp(stream) => Some(stream),
            _ => None,
        }
    }

    /// The transport this stream came over, for a log line.
    pub const fn transport(&self) -> &'static str {
        match &self.0 {
            Inner::Tcp(_) => "tcp",
            #[cfg(unix)]
            Inner::Unix { .. } => "ipc",
            Inner::Inproc(_) => "inproc",
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
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_read(cx, buf),
            Inner::Inproc(stream) => Pin::new(stream).poll_read(cx, buf),
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
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_write(cx, buf),
            Inner::Inproc(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_flush(cx),
            Inner::Inproc(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_shutdown(cx),
            Inner::Inproc(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// The probe parameters `options` ask for. What they leave `None` stays the
/// operating system's own.
fn probes(options: &SocketOptions) -> TcpKeepalive {
    let mut probes = TcpKeepalive::new();
    if let Some(idle) = options.tcp_keepalive_idle {
        probes = probes.with_time(idle);
    }
    tune_probes(
        probes,
        options.tcp_keepalive_intvl,
        options.tcp_keepalive_cnt,
    )
}

/// Adds the probe interval and count, on the platforms where the kernel has
/// them. Elsewhere the operating system's own stay, as libzmq's manual says:
/// "where supported by OS".
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
fn tune_probes(
    mut probes: TcpKeepalive,
    interval: Option<Duration>,
    count: Option<u32>,
) -> TcpKeepalive {
    if let Some(interval) = interval {
        probes = probes.with_interval(interval);
    }
    if let Some(count) = count {
        probes = probes.with_retries(count);
    }
    probes
}

/// See the other definition: this platform has neither knob.
#[cfg(not(any(
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
)))]
fn tune_probes(
    probes: TcpKeepalive,
    _interval: Option<Duration>,
    _count: Option<u32>,
) -> TcpKeepalive {
    probes
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// A connected pair over loopback: the dialling end and the accepted one.
    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("address");
        let (dialled, accepted) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(TcpStream::connect(addr), listener.accept())
        })
        .await
        .expect("the pair connected in time");
        (dialled.expect("connect"), accepted.expect("accept").0)
    }

    /// Claim: a transport with no kernel probe takes the options and does
    /// nothing, as it does for `TCP_NODELAY`.
    #[test]
    fn a_stream_that_is_not_tcp_has_no_probe_to_set() {
        let (memory, _other) = tokio::io::duplex(64);
        Stream::inproc(memory)
            .set_keepalive(&SocketOptions {
                tcp_keepalive: Some(true),
                ..SocketOptions::default()
            })
            .expect("a property that cannot apply is not a failure");
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: **`ZMQ_TCP_KEEPALIVE` is applied as libzmq applies it.** `-1`
        /// leaves `SO_KEEPALIVE` as the operating system has it, whichever way
        /// that is; `0` switches it off and `1` switches it on.
        #[tokio::test]
        async fn the_keepalive_switch_is_applied_as_libzmq_applies_it() {
            let (tcp, _peer) = pair().await;
            let stream = Stream::tcp(tcp);
            let socket = || SockRef::from(stream.as_tcp().expect("a TCP stream"));
            let with = |tcp_keepalive| SocketOptions {
                tcp_keepalive,
                ..SocketOptions::default()
            };

            for before in [false, true] {
                socket().set_keepalive(before).expect("set");
                stream.set_keepalive(&with(None)).expect("apply -1");
                assert_eq!(
                    socket().keepalive().expect("get"),
                    before,
                    "-1 leaves SO_KEEPALIVE as it was"
                );
            }

            socket().set_keepalive(true).expect("set");
            stream.set_keepalive(&with(Some(false))).expect("apply 0");
            assert!(!socket().keepalive().expect("get"), "0 switches it off");

            stream.set_keepalive(&with(Some(true))).expect("apply 1");
            assert!(socket().keepalive().expect("get"), "1 switches it on");
        }
        /// Claim: the three tuning options become the kernel's idle time, probe
        /// interval and probe count, and one left at `-1` keeps the kernel's own.
        #[cfg(target_os = "linux")]
        #[tokio::test]
        async fn the_keepalive_tuning_reaches_the_kernel() {
            let (tcp, _peer) = pair().await;
            let stream = Stream::tcp(tcp);
            let socket = SockRef::from(stream.as_tcp().expect("a TCP stream"));
            stream
                .set_keepalive(&SocketOptions {
                    tcp_keepalive: Some(true),
                    tcp_keepalive_idle: Some(Duration::from_secs(41)),
                    tcp_keepalive_intvl: Some(Duration::from_secs(7)),
                    tcp_keepalive_cnt: Some(5),
                    ..SocketOptions::default()
                })
                .expect("apply");
            assert!(socket.keepalive().expect("get"));
            assert_eq!(
                socket.tcp_keepalive_time().expect("idle"),
                Duration::from_secs(41)
            );
            assert_eq!(
                socket.tcp_keepalive_interval().expect("interval"),
                Duration::from_secs(7)
            );
            assert_eq!(socket.tcp_keepalive_retries().expect("count"), 5);

            let (tcp, _peer) = pair().await;
            let stream = Stream::tcp(tcp);
            let socket = SockRef::from(stream.as_tcp().expect("a TCP stream"));
            let (idle, interval) = (
                socket.tcp_keepalive_time().expect("idle"),
                socket.tcp_keepalive_interval().expect("interval"),
            );
            stream
                .set_keepalive(&SocketOptions {
                    tcp_keepalive: Some(true),
                    tcp_keepalive_cnt: Some(3),
                    ..SocketOptions::default()
                })
                .expect("apply");
            assert_eq!(socket.tcp_keepalive_retries().expect("count"), 3);
            assert_eq!(socket.tcp_keepalive_time().expect("idle"), idle);
            assert_eq!(socket.tcp_keepalive_interval().expect("interval"), interval);
        }
    }
}
