//! Per-write chunk framing for the local stream transports.
//!
//! A QUIC stream ends two ways and says which: a FIN the reader sees as the
//! end of the payload, and a `RESET_STREAM` carrying a code the reader sees as
//! a failure. Neither local transport gets both from the kernel:
//!
//! * a **named pipe** has no half-close at all — closing the handle would take
//!   the reply direction with it;
//! * an **`AF_UNIX` socket** has a half-close, and that is the whole problem:
//!   a shutdown and an abort are byte-for-byte the same thing at the reader,
//!   because Linux has no kernel-level abort for a stream socket (`SO_LINGER`
//!   with a zero timeout plus `close` is a plain close at the reader).
//!
//! So the distinction lives in the payload, for both of them, in one framing:
//!
//! ```text
//! 0x00 + u32 length + bytes    payload, as much as one write produced
//! 0x01                          FIN: the payload is complete
//! 0x02 + u64 code               RESET: the payload was abandoned, with why
//! ```
//!
//! Five bytes per write and nothing per byte, and it carries what the socket
//! could not: **a cancelled transfer is `Canceled` at the reader rather than a
//! short payload that looks complete**. A transport that cannot signal an
//! abort would otherwise let an application read half a message as a whole
//! one, with nothing anywhere naming it.
//! [0012 §4.7(e)](../../../docs/decisions/0012-local-connection-grouping.md)
//! priced the alternative and this is the shape it chose; both socket
//! transports share it.
//!
//! What the framing still cannot carry is the **reader's** refusal: there is
//! no `STOP_SENDING`, because on a pipe the writer's direction is the only one
//! the reader could signal on and the reply already owns it, and on a socket
//! closing the read half gives the writer an `EPIPE` with no code. A reader
//! that stops therefore **drains** — the rest of the payload is read and
//! discarded in the background, so the writer finishes normally and learns of
//! the refusal from the reply.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use weida_runtime::Exec;

/// A payload chunk: `u32` little-endian length, then the bytes.
const CHUNK_DATA: u8 = 0x00;
/// The end of the payload.
const CHUNK_FIN: u8 = 0x01;
/// The payload abandoned, with a `u64` little-endian code.
const CHUNK_RESET: u8 = 0x02;
/// The longest chunk header: kind plus a reset code.
const HEADER_MAX: usize = 9;
/// The header of a payload chunk: kind plus a `u32` length.
const HEADER_DATA: usize = 5;
/// Scratch for a draining reader.
const DRAIN_BUF: usize = 8 * 1024;

/// The peer abandoned its payload, with the code it gave.
///
/// Carried inside an `io::Error` so that a transport's `read_error` can
/// recover the code and report `Canceled` rather than a generic failure.
#[derive(Debug)]
pub(crate) struct PeerReset(pub(crate) u64);

impl std::fmt::Display for PeerReset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream reset by the peer with code {}", self.0)
    }
}

impl std::error::Error for PeerReset {}

/// How a stream ended.
pub(crate) enum Marker {
    Fin,
    Reset(u64),
}

impl Marker {
    fn encode(&self) -> ([u8; HEADER_MAX], usize) {
        let mut bytes = [0u8; HEADER_MAX];
        match self {
            Marker::Fin => {
                bytes[0] = CHUNK_FIN;
                (bytes, 1)
            }
            Marker::Reset(code) => {
                bytes[0] = CHUNK_RESET;
                bytes[1..].copy_from_slice(&code.to_le_bytes());
                (bytes, HEADER_MAX)
            }
        }
    }
}

fn closed() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "stream already closed")
}

/// The writing half: frames every write as a chunk.
pub(crate) struct ChunkWriter<W> {
    io: Option<W>,
    exec: Exec,
    /// The chunk header being written, and how much of it is still owed.
    header: [u8; HEADER_MAX],
    header_len: usize,
    header_written: usize,
    /// Payload bytes the current chunk's header promised and that have not
    /// been written yet.
    body_left: usize,
    /// A FIN went out through `poll_shutdown`; `end` has nothing to add.
    finished: bool,
}

impl<W: AsyncWrite + Unpin + Send + 'static> ChunkWriter<W> {
    pub(crate) fn new(io: W, exec: Exec) -> ChunkWriter<W> {
        ChunkWriter {
            io: Some(io),
            exec,
            header: [0; HEADER_MAX],
            header_len: 0,
            header_written: 0,
            body_left: 0,
            finished: false,
        }
    }

    /// Writes the end-of-stream marker on the runtime, since the callers of
    /// `finish` and `reset` are synchronous.
    ///
    /// A chunk left half-written cannot be ended cleanly — the reader is owed
    /// bytes this side no longer has — so nothing is written and the reader
    /// sees the connection close when the last half goes.
    pub(crate) fn end(mut self, marker: Marker) {
        let Some(mut io) = self.io.take() else {
            return;
        };
        if self.finished || self.body_left > 0 || self.header_written != self.header_len {
            return;
        }
        let (bytes, len) = marker.encode();
        self.exec.spawn(async move {
            if let Err(e) = io.write_all(&bytes[..len]).await {
                tracing::debug!(error = %e, "local stream end marker not written");
                return;
            }
            let _ = io.flush().await;
        });
    }

    /// Writes what is owed of the current header; `Ready(Ok(()))` when none
    /// is.
    fn poll_header(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let Some(io) = self.io.as_mut() else {
            return Poll::Ready(Err(closed()));
        };
        while self.header_written < self.header_len {
            let n = ready!(
                Pin::new(&mut *io)
                    .poll_write(cx, &self.header[self.header_written..self.header_len])
            )?;
            if n == 0 {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "the local stream accepted no bytes",
                )));
            }
            self.header_written += n;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.io.as_mut() {
            Some(io) => Pin::new(io).poll_flush(cx),
            None => Poll::Ready(Ok(())),
        }
    }
}

impl<W: AsyncWrite + Unpin + Send + 'static> AsyncWrite for ChunkWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.io.is_none() || this.finished {
            return Poll::Ready(Err(closed()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.body_left == 0 && this.header_written == this.header_len {
            // A new chunk: as long as this write, capped by the length field.
            let len = buf.len().min(u32::MAX as usize);
            this.header[0] = CHUNK_DATA;
            this.header[1..HEADER_DATA].copy_from_slice(&(len as u32).to_le_bytes());
            this.header_len = HEADER_DATA;
            this.header_written = 0;
            this.body_left = len;
        }
        ready!(this.poll_header(cx))?;
        let want = buf.len().min(this.body_left);
        let io = this.io.as_mut().expect("checked above");
        let n = ready!(Pin::new(io).poll_write(cx, &buf[..want]))?;
        this.body_left -= n;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.get_mut().poll_flush_inner(cx)
    }

    /// A shutdown is a FIN, as it is on every other transport, written in
    /// place through the polls rather than on the runtime.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.io.is_none() {
            return Poll::Ready(Ok(()));
        }
        if this.finished {
            return this.poll_flush_inner(cx);
        }
        if this.body_left > 0 {
            return Poll::Ready(Err(std::io::Error::other(
                "shutdown in the middle of a write",
            )));
        }
        if this.header_written == this.header_len {
            let (bytes, len) = Marker::Fin.encode();
            this.header = bytes;
            this.header_len = len;
            this.header_written = 0;
        }
        ready!(this.poll_header(cx))?;
        this.finished = true;
        this.poll_flush_inner(cx)
    }
}

enum ReadState {
    /// Reading a chunk header; the first byte says how long it is.
    Header {
        buf: [u8; HEADER_MAX],
        filled: usize,
    },
    /// Inside a payload chunk, with this many bytes still to come.
    Body { left: usize },
    /// FIN seen, RESET reported, or the connection failed: nothing more to
    /// read.
    Ended,
}

/// What a completed header means.
enum Chunk {
    Data(usize),
    Fin,
    Reset(u64),
}

/// The reading half: unframes chunks, ends at FIN, fails at RESET.
pub(crate) struct ChunkReader<R> {
    io: Option<R>,
    exec: Exec,
    state: ReadState,
}

impl<R: AsyncRead + Unpin + Send + 'static> ChunkReader<R> {
    pub(crate) fn new(io: R, exec: Exec) -> ChunkReader<R> {
        ChunkReader {
            io: Some(io),
            exec,
            state: ReadState::Header {
                buf: [0; HEADER_MAX],
                filled: 0,
            },
        }
    }

    /// Reads and discards the rest of the payload on the runtime, so the
    /// peer's writes complete instead of blocking on a stream nobody reads.
    pub(crate) fn drain(mut self) {
        if matches!(self.state, ReadState::Ended) || self.io.is_none() {
            return;
        }
        let exec = self.exec.clone();
        exec.spawn(async move {
            let mut scratch = vec![0u8; DRAIN_BUF];
            loop {
                match self.read(&mut scratch).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        });
    }

    /// How many header bytes a chunk of `kind` has, or `None` for a kind this
    /// side does not know.
    fn header_len(kind: u8) -> Option<usize> {
        match kind {
            CHUNK_DATA => Some(HEADER_DATA),
            CHUNK_FIN => Some(1),
            CHUNK_RESET => Some(HEADER_MAX),
            _ => None,
        }
    }

    fn poll_read_inner(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let Some(io) = self.io.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        loop {
            match &mut self.state {
                ReadState::Ended => return Poll::Ready(Ok(())),
                ReadState::Header {
                    buf: header,
                    filled,
                } => {
                    let need = if *filled == 0 {
                        1
                    } else {
                        match ChunkReader::<R>::header_len(header[0]) {
                            Some(need) => need,
                            None => {
                                return Poll::Ready(Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("unknown local chunk kind {:#04x}", header[0]),
                                )));
                            }
                        }
                    };
                    if *filled < need {
                        let mut slice = ReadBuf::new(&mut header[*filled..need]);
                        ready!(Pin::new(&mut *io).poll_read(cx, &mut slice))?;
                        let n = slice.filled().len();
                        if n == 0 {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "the local stream closed before the end of the payload",
                            )));
                        }
                        *filled += n;
                        continue;
                    }
                    let chunk = match header[0] {
                        CHUNK_DATA => {
                            let len =
                                u32::from_le_bytes([header[1], header[2], header[3], header[4]]);
                            Chunk::Data(len as usize)
                        }
                        CHUNK_FIN => Chunk::Fin,
                        _ => {
                            let mut code = [0u8; 8];
                            code.copy_from_slice(&header[1..HEADER_MAX]);
                            Chunk::Reset(u64::from_le_bytes(code))
                        }
                    };
                    match chunk {
                        Chunk::Data(0) => {
                            self.state = ReadState::Header {
                                buf: [0; HEADER_MAX],
                                filled: 0,
                            };
                        }
                        Chunk::Data(len) => self.state = ReadState::Body { left: len },
                        Chunk::Fin => {
                            self.state = ReadState::Ended;
                            return Poll::Ready(Ok(()));
                        }
                        Chunk::Reset(code) => {
                            self.state = ReadState::Ended;
                            return Poll::Ready(Err(std::io::Error::other(PeerReset(code))));
                        }
                    }
                }
                ReadState::Body { left } => {
                    let want = buf.remaining().min(*left);
                    if want == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    // Initialised up front so that `advance` below is sound
                    // without an `unsafe` `assume_init`.
                    buf.initialize_unfilled_to(want);
                    let mut slice = buf.take(want);
                    ready!(Pin::new(&mut *io).poll_read(cx, &mut slice))?;
                    let n = slice.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "the local stream closed in the middle of a chunk",
                        )));
                    }
                    buf.advance(n);
                    *left -= n;
                    if *left == 0 {
                        self.state = ReadState::Header {
                            buf: [0; HEADER_MAX],
                            filled: 0,
                        };
                    }
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> AsyncRead for ChunkReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let polled = this.poll_read_inner(cx, buf);
        if let Poll::Ready(Err(_)) = &polled {
            // Any failure ends the stream: a drain must not spin on it.
            this.state = ReadState::Ended;
        }
        polled
    }
}

/// A read error as both socket transports report it: a peer's RESET by its
/// code, anything else as a transport failure.
pub(crate) fn read_error(error: std::io::Error) -> weida_core::Error {
    match error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<PeerReset>())
    {
        Some(reset) => weida_protocol::codes::stop_reason(reset.0).into(),
        None => weida_core::Error::Transport(format!("local stream read failed: {error}")),
    }
}
