//! The named-pipe transport: `weida+pipe://<name>/<path>`.
//!
//! The third local transport of
//! [decision 0010](../../../docs/decisions/0010-local-transport.md) §4.1: a
//! pipe under `\\.\pipe\`, one connection per transfer, and the peer proved
//! by the client's token SID on the accepting side and by the pipe's owner
//! SID on the dialling side [0010 §4.4, §4.5]. The grouping of connections
//! into peers is [`crate::grouped`]; this module is what a pipe contributes
//! to it, and one thing a pipe lacks.
//!
//! **A pipe has no half-close.** A socket ends a stream by shutting down one
//! direction and the reader sees EOF; a pipe handle closes whole, and closing
//! it would take the reply direction with it. So a pipe connection carries its
//! payload in [`crate::chunked`]'s chunks, and the end of the payload is a
//! chunk rather than a socket state.
//!
//! That framing is [`crate::chunked`]'s, shared with the unix socket
//! transport: a unix socket has the opposite problem — a half-close and no
//! abort, so a cancelled transfer would arrive as a complete one — and the
//! same framing fixes it. One convention, one reader, two transports.
//!
//! What the framing cannot carry is the reader's refusal: there is no
//! `STOP_SENDING`, because the writer's direction is the only one the reader
//! could signal on and the reply already owns it. A reader that stops
//! therefore **drains**, so the writer finishes normally and learns of the
//! refusal from the reply, as
//! [0012 §4.7](../../../docs/decisions/0012-local-connection-grouping.md)
//! names it for the socket. The named loss that remains on this transport: a
//! refused writer is not `Canceled` at the write.

use std::ffi::OsString;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, ReadHalf, WriteHalf};
use tokio::net::windows::named_pipe::{NamedPipeClient, NamedPipeServer};
use weida_core::{Error, PeerIdentity, WindowsPrincipal};
use weida_runtime::Exec;

use crate::chunked::{ChunkReader, ChunkWriter, Marker};
use crate::grouped::{Claims, Stream};

/// What a dial needs: the pipe's OS path and the runtime whose timer waits
/// out a busy pipe.
pub(crate) struct PipeEndpoint {
    pub(crate) path: OsString,
    pub(crate) exec: Exec,
}

/// One end of a pipe connection.
pub(crate) struct PipeStream {
    io: PipeIo,
    exec: Exec,
}

/// Crate-visible because it appears in this transport's `Stream::Reader` and
/// `Stream::Writer`, which the shared framing is generic over.
pub(crate) enum PipeIo {
    /// The instance that accepted, on the serving side.
    Server(NamedPipeServer),
    /// The handle that dialled.
    Client(NamedPipeClient),
}

impl PipeStream {
    /// An accepted server instance, once `connect` returned on it.
    pub(crate) fn accepted(server: NamedPipeServer, exec: Exec) -> PipeStream {
        PipeStream {
            io: PipeIo::Server(server),
            exec,
        }
    }
}

impl AsyncRead for PipeIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PipeIo::Server(io) => Pin::new(io).poll_read(cx, buf),
            PipeIo::Client(io) => Pin::new(io).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PipeIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            PipeIo::Server(io) => Pin::new(io).poll_write(cx, buf),
            PipeIo::Client(io) => Pin::new(io).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PipeIo::Server(io) => Pin::new(io).poll_flush(cx),
            PipeIo::Client(io) => Pin::new(io).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PipeIo::Server(io) => Pin::new(io).poll_shutdown(cx),
            PipeIo::Client(io) => Pin::new(io).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for PipeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl AsyncWrite for PipeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

impl Stream for PipeStream {
    type Endpoint = PipeEndpoint;
    type Principal = WindowsPrincipal;
    type Writer = ChunkWriter<WriteHalf<PipeIo>>;
    type Reader = ChunkReader<ReadHalf<PipeIo>>;

    async fn connect(endpoint: &PipeEndpoint) -> Result<PipeStream, Error> {
        let client = weida_runtime::connect_pipe(&endpoint.exec, &endpoint.path).await?;
        Ok(PipeStream {
            io: PipeIo::Client(client),
            exec: endpoint.exec.clone(),
        })
    }

    fn principal(&self) -> Result<WindowsPrincipal, Error> {
        match &self.io {
            PipeIo::Server(io) => weida_runtime::client_principal(io),
            PipeIo::Client(io) => weida_runtime::server_principal(io),
        }
    }

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (recv, send) = tokio::io::split(self.io);
        (
            ChunkReader::new(recv, self.exec.clone()),
            ChunkWriter::new(send, self.exec),
        )
    }

    /// The account always; the pid too, since the pipe reports one for
    /// both ends, and never the pid alone [0012 §4.2].
    fn same_peer(group: &WindowsPrincipal, asking: &WindowsPrincipal) -> bool {
        if group.sid != asking.sid {
            return false;
        }
        match (group.pid, asking.pid) {
            (Some(expected), Some(actual)) => expected == actual,
            _ => true,
        }
    }

    fn identity(principal: &WindowsPrincipal) -> PeerIdentity {
        PeerIdentity::Windows(principal.clone())
    }

    fn finish(writer: Self::Writer, claims: Claims) {
        writer.end(Marker::Fin, claims);
    }

    fn reset(writer: Self::Writer, code: u64, claims: Claims) {
        writer.end(Marker::Reset(code), claims);
    }

    fn stop(reader: Self::Reader, _code: u64, claims: Claims) {
        reader.drain(claims);
    }

    fn read_error(error: std::io::Error) -> Error {
        crate::chunked::read_error(error)
    }
}
