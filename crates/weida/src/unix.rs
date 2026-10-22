//! The `AF_UNIX` transport: `weida+unix://<percent-encoded-path>/<path>`.
//!
//! `SOCK_STREAM` on a filesystem path, and the peer proved by `SO_PEERCRED`
//! / `LOCAL_PEERCRED`
//! ([decision 0010](../../../docs/decisions/0010-local-transport.md) §4.4,
//! §4.5). The grouping of connections into peers is [`crate::grouped`]; this
//! module is only what a unix socket contributes to it: how it is dialled,
//! how the kernel names its peer, and how its payload ends.
//!
//! **A half-close is not an abort.** The kernel ends a socket's write
//! direction one way: a shutdown the reader sees as EOF. There is no abort to
//! pair with it — `SO_LINGER` with a zero timeout plus `close` is
//! byte-for-byte a plain close at the reader — so without framing a cancelled
//! transfer, a dropped `OutgoingTransfer` and a producer that died
//! mid-payload would all arrive as a *complete* payload, and an application
//! would read half a message for a whole one with nothing on the wire to warn
//! it.
//!
//! So the payload is framed, in [`crate::chunked`]'s chunks — the framing the
//! named-pipe transport shares, so the two socket transports have one
//! convention and one reader. The cost is five bytes per write and the zero-copy read, which
//! [0012 §4.7(e)](../../../docs/decisions/0012-local-connection-grouping.md)
//! priced before choosing it.

use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use weida_core::{Error, LocalPrincipal, LossCause, PeerIdentity};
use weida_runtime::{Exec, peer_credentials};

use crate::chunked::{ChunkReader, ChunkWriter, Marker};
use crate::grouped::{Claims, Stream};

/// What a dial needs: the socket path, the runtime whose tasks write the
/// end-of-payload marker, and, under `Network::sim`, the simulated network
/// whose listener answers on that path.
pub(crate) struct UnixEndpoint {
    pub(crate) path: PathBuf,
    pub(crate) exec: Exec,
    #[cfg(feature = "test-util")]
    pub(crate) sim: Option<crate::sim::SimNet>,
}

/// One end of a unix-socket connection, and the runtime its halves report on.
///
/// The `Exec` is here for the same reason it is on the pipe's stream: ending a
/// payload is synchronous at the call site, so the marker is written in place
/// when the socket takes it at once and on a task only when it does not
/// ([`ChunkWriter::end`]).
pub(crate) struct UnixLocal {
    io: Io,
    exec: Exec,
}

/// The connection under a [`UnixLocal`]: the kernel's, or one end of a
/// simulated network's in-memory pair, which carries the same preamble,
/// chunks and grouping and reports [`crate::sim::SIM_PRINCIPAL`] as its peer.
enum Io {
    Os(UnixStream),
    #[cfg(feature = "test-util")]
    Sim(tokio::io::DuplexStream),
}

impl UnixLocal {
    /// An accepted connection, with the runtime its halves will use.
    pub(crate) fn accepted(io: UnixStream, exec: Exec) -> UnixLocal {
        UnixLocal {
            io: Io::Os(io),
            exec,
        }
    }

    /// An accepted simulated connection, with the runtime its halves will use.
    #[cfg(feature = "test-util")]
    pub(crate) fn accepted_sim(io: tokio::io::DuplexStream, exec: Exec) -> UnixLocal {
        UnixLocal {
            io: Io::Sim(io),
            exec,
        }
    }
}

impl AsyncRead for UnixLocal {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Os(io) => Pin::new(io).poll_read(cx, buf),
            #[cfg(feature = "test-util")]
            Io::Sim(io) => Pin::new(io).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for UnixLocal {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut self.get_mut().io {
            Io::Os(io) => Pin::new(io).poll_write(cx, buf),
            #[cfg(feature = "test-util")]
            Io::Sim(io) => Pin::new(io).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Os(io) => Pin::new(io).poll_flush(cx),
            #[cfg(feature = "test-util")]
            Io::Sim(io) => Pin::new(io).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Os(io) => Pin::new(io).poll_shutdown(cx),
            #[cfg(feature = "test-util")]
            Io::Sim(io) => Pin::new(io).poll_shutdown(cx),
        }
    }
}

/// The reading half of a [`UnixLocal`].
pub(crate) enum ReadHalf {
    Os(OwnedReadHalf),
    #[cfg(feature = "test-util")]
    Sim(tokio::io::ReadHalf<tokio::io::DuplexStream>),
}

/// The writing half of a [`UnixLocal`].
pub(crate) enum WriteHalf {
    Os(OwnedWriteHalf),
    #[cfg(feature = "test-util")]
    Sim(tokio::io::WriteHalf<tokio::io::DuplexStream>),
}

impl AsyncRead for ReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ReadHalf::Os(io) => Pin::new(io).poll_read(cx, buf),
            #[cfg(feature = "test-util")]
            ReadHalf::Sim(io) => Pin::new(io).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for WriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            WriteHalf::Os(io) => Pin::new(io).poll_write(cx, buf),
            #[cfg(feature = "test-util")]
            WriteHalf::Sim(io) => Pin::new(io).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            WriteHalf::Os(io) => Pin::new(io).poll_flush(cx),
            #[cfg(feature = "test-util")]
            WriteHalf::Sim(io) => Pin::new(io).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            WriteHalf::Os(io) => Pin::new(io).poll_shutdown(cx),
            #[cfg(feature = "test-util")]
            WriteHalf::Sim(io) => Pin::new(io).poll_shutdown(cx),
        }
    }
}

impl Stream for UnixLocal {
    type Endpoint = UnixEndpoint;
    type Principal = LocalPrincipal;
    type Writer = ChunkWriter<WriteHalf>;
    type Reader = ChunkReader<ReadHalf>;

    async fn connect(endpoint: &UnixEndpoint) -> Result<UnixLocal, Error> {
        #[cfg(feature = "test-util")]
        if let Some(net) = &endpoint.sim {
            return Ok(UnixLocal {
                io: Io::Sim(net.connect_unix(&endpoint.path)?),
                exec: endpoint.exec.clone(),
            });
        }
        let io = UnixStream::connect(&endpoint.path)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused => {
                    Error::ConnectionLost(LossCause::PeerClosed)
                }
                _ => Error::Io(e),
            })?;
        Ok(UnixLocal {
            io: Io::Os(io),
            exec: endpoint.exec.clone(),
        })
    }

    fn principal(&self) -> Result<LocalPrincipal, Error> {
        match &self.io {
            Io::Os(io) => peer_credentials(io),
            #[cfg(feature = "test-util")]
            Io::Sim(_) => Ok(crate::sim::SIM_PRINCIPAL),
        }
    }

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (recv, send) = match self.io {
            Io::Os(io) => {
                let (recv, send) = io.into_split();
                (ReadHalf::Os(recv), WriteHalf::Os(send))
            }
            #[cfg(feature = "test-util")]
            Io::Sim(io) => {
                let (recv, send) = tokio::io::split(io);
                (ReadHalf::Sim(recv), WriteHalf::Sim(send))
            }
        };
        (
            ChunkReader::new(recv, self.exec.clone()),
            ChunkWriter::new(send, self.exec),
        )
    }

    /// The uid always; the pid where the platform reports one, and never
    /// the pid alone [0012 §4.2].
    fn same_peer(group: &LocalPrincipal, asking: &LocalPrincipal) -> bool {
        if group.uid != asking.uid {
            return false;
        }
        match (group.pid, asking.pid) {
            (Some(expected), Some(actual)) => expected == actual,
            _ => true,
        }
    }

    fn identity(principal: &LocalPrincipal) -> PeerIdentity {
        PeerIdentity::Local(*principal)
    }

    /// A FIN chunk, which is what tells the reader the payload is complete —
    /// and the *only* thing that does.
    fn finish(writer: Self::Writer, claims: Claims) {
        writer.end(Marker::Fin, claims);
    }

    /// A RESET chunk carrying the code, so an abandoned transfer is reported
    /// by name instead of arriving as a payload that looks whole.
    fn reset(writer: Self::Writer, code: u64, claims: Claims) {
        writer.end(Marker::Reset(code), claims);
    }

    /// A reader that stops **drains**: closing the read half would give the
    /// writer an `EPIPE` with no code, so the rest of the payload is read and
    /// discarded and the refusal travels in the reply [0012 §4.7]. The drain
    /// keeps the connection's claims, so its place of `max_connections` is
    /// not given back while it still holds the descriptor.
    fn stop(reader: Self::Reader, _code: u64, claims: Claims) {
        reader.drain(claims);
    }

    fn read_error(error: std::io::Error) -> Error {
        crate::chunked::read_error(error)
    }
}
