//! The byte stream an HTTP/1.1 or HTTP/2 connection runs on.
//!
//! An enum rather than a boxed trait object, as in `weida-mqtt` and
//! `weida-nats`: the cases are few, the dispatch is one branch per read, and a
//! `Box<dyn AsyncRead + AsyncWrite>` would add an allocation and a vtable call
//! for nothing. The unit tests run connections over `tokio::io::duplex`
//! through a test-only variant.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// The byte stream a connection runs on.
#[derive(Debug)]
pub(crate) enum Stream {
    /// Plain TCP.
    Plain(TcpStream),
    /// A client's TLS over TCP.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    /// A server's TLS over TCP.
    #[cfg(feature = "tls")]
    TlsServer(Box<tokio_rustls::server::TlsStream<TcpStream>>),
    /// An in-memory pipe, for the unit tests.
    #[cfg(test)]
    Test(tokio::io::DuplexStream),
}

macro_rules! each {
    ($self:expr, $s:ident => $e:expr) => {
        match $self.get_mut() {
            Stream::Plain($s) => {
                let $s = Pin::new($s);
                $e
            }
            #[cfg(feature = "tls")]
            Stream::Tls($s) => {
                let $s = Pin::new($s.as_mut());
                $e
            }
            #[cfg(feature = "tls")]
            Stream::TlsServer($s) => {
                let $s = Pin::new($s.as_mut());
                $e
            }
            #[cfg(test)]
            Stream::Test($s) => {
                let $s = Pin::new($s);
                $e
            }
        }
    };
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        each!(self, s => s.poll_read(cx, buf))
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        each!(self, s => s.poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        each!(self, s => s.poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        each!(self, s => s.poll_shutdown(cx))
    }
}
