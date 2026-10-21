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
//! the refusal from the reply. A drain gives up on a peer that goes quiet for
//! `DRAIN_IDLE`, which releases the stream; a payload that keeps flowing is
//! drained to its FIN however long it runs.
//!
//! A drain, and an end marker the stream did not take at once, run on a task
//! that owns the OS handle, outliving the half that started it. Each is given
//! what the caller counts the connection against (`hold`) and drops it after
//! the handle, so a binding's `max_connections` bounds the descriptors it
//! holds and not only the halves still in hand.

use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};
use std::time::Duration;

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
/// Read-ahead of a chunk reader. One `read` serves a short message's header,
/// body and FIN, and a bounded buffer keeps a flood from pinning memory.
const READ_AHEAD: usize = 4 * 1024;
/// Scratch for a draining reader.
const DRAIN_BUF: usize = 8 * 1024;
/// How long a draining reader waits for the peer's next bytes before it
/// gives the stream up. The bound is per read, so a flowing payload drains
/// to its FIN however long it is.
const DRAIN_IDLE: Duration = Duration::from_secs(30);
/// Errors for a stream that ends inside a header and inside a chunk.
const HEADER_EOF: &str = "the local stream closed before the end of the payload";
const BODY_EOF: &str = "the local stream closed in the middle of a chunk";

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

    /// Writes the end-of-stream marker: in place when the stream takes all of
    /// it at once, and on the runtime for whatever it did not take.
    ///
    /// The callers of `finish` and `reset` are synchronous, so the task is the
    /// fallback, not the path. It owns the stream until the peer has read the
    /// rest of the marker, which a peer that never reads makes for ever, and
    /// it is why `hold` is here: it is dropped after the stream, so what it
    /// counts is not given back while the descriptor is still open.
    ///
    /// A chunk left half-written cannot be ended cleanly — the reader is owed
    /// bytes this side no longer has — so nothing is written and the reader
    /// sees the connection close when the last half goes.
    pub(crate) fn end<H: Send + 'static>(mut self, marker: Marker, hold: H) {
        let Some(mut io) = self.io.take() else {
            return;
        };
        if self.finished || self.body_left > 0 || self.header_written != self.header_len {
            return;
        }
        let (bytes, len) = marker.encode();
        // A waker that is never woken: a stream that answers `Pending` here is
        // finished by the task below, which polls with a real waker.
        let mut cx = Context::from_waker(Waker::noop());
        let mut written = 0;
        while written < len {
            match Pin::new(&mut io).poll_write(&mut cx, &bytes[written..len]) {
                Poll::Ready(Ok(n)) if n > 0 => written += n,
                Poll::Ready(Err(e)) => {
                    tracing::debug!(error = %e, "local stream end marker not written");
                    return;
                }
                // Pending, or a write that took nothing: the task reports it.
                Poll::Ready(Ok(_)) | Poll::Pending => break,
            }
        }
        if written == len {
            match Pin::new(&mut io).poll_flush(&mut cx) {
                Poll::Ready(Ok(())) => return,
                Poll::Ready(Err(e)) => {
                    tracing::debug!(error = %e, "local stream end marker not flushed");
                    return;
                }
                Poll::Pending => {}
            }
        }
        self.exec.spawn(async move {
            if let Err(e) = io.write_all(&bytes[written..len]).await {
                tracing::debug!(error = %e, "local stream end marker not written");
            } else if let Err(e) = io.flush().await {
                tracing::debug!(error = %e, "local stream end marker not flushed");
            }
            drop(io);
            drop(hold);
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

/// Where the reader is in the framing.
enum ReadState {
    /// At a chunk header; its first byte says how long the header is.
    Header,
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
///
/// Reads from the stream go through a read-ahead buffer, so one `read` serves
/// a short chunk's header, body and FIN together. Bytes read ahead are part of
/// the payload: every read, and `drain`, consumes them before the stream.
pub(crate) struct ChunkReader<R> {
    io: Option<R>,
    exec: Exec,
    state: ReadState,
    /// Bytes read from the stream and not consumed yet: `rbuf[rpos..rfill]`.
    rbuf: Box<[u8]>,
    rpos: usize,
    rfill: usize,
}

impl<R: AsyncRead + Unpin + Send + 'static> ChunkReader<R> {
    pub(crate) fn new(io: R, exec: Exec) -> ChunkReader<R> {
        ChunkReader {
            io: Some(io),
            exec,
            state: ReadState::Header,
            rbuf: vec![0; READ_AHEAD].into_boxed_slice(),
            rpos: 0,
            rfill: 0,
        }
    }

    /// Reads and discards the rest of the payload on the runtime, so the
    /// peer's writes complete instead of blocking on a stream nobody reads.
    ///
    /// Each read waits at most `DRAIN_IDLE` for the peer's next bytes: a peer
    /// that stalls releases the descriptor, and a payload that keeps flowing
    /// drains to its FIN however long it is. `hold` is dropped after the
    /// descriptor, whenever the drain ends.
    pub(crate) fn drain<H: Send + 'static>(self, hold: H) {
        self.drain_within(DRAIN_IDLE, hold);
    }

    fn drain_within<H: Send + 'static>(mut self, idle: Duration, hold: H) {
        if matches!(self.state, ReadState::Ended) || self.io.is_none() {
            // Nothing left to read: the handle goes first, and `hold` as this
            // returns.
            drop(self);
            return;
        }
        let exec = self.exec.clone();
        let timer = self.exec.clone();
        exec.spawn(async move {
            let mut scratch = vec![0u8; DRAIN_BUF];
            loop {
                match timer.within(idle, self.read(&mut scratch)).await {
                    Some(Ok(n)) if n > 0 => {}
                    // The FIN or the end of the stream, a failure, or a peer
                    // that went quiet: in every case the reader is let go.
                    _ => break,
                }
            }
            drop(self);
            drop(hold);
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

    fn buffered(&self) -> usize {
        self.rfill - self.rpos
    }

    /// Reads more of the stream into the read-ahead buffer. Unconsumed bytes
    /// move to the front first, so the free space is at the back.
    fn poll_fill(&mut self, cx: &mut Context<'_>, eof: &'static str) -> Poll<std::io::Result<()>> {
        let Some(io) = self.io.as_mut() else {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                eof,
            )));
        };
        if self.rpos > 0 {
            self.rbuf.copy_within(self.rpos..self.rfill, 0);
            self.rfill -= self.rpos;
            self.rpos = 0;
        }
        debug_assert!(self.rfill < self.rbuf.len(), "a fill needs free space");
        let mut room = ReadBuf::new(&mut self.rbuf[self.rfill..]);
        ready!(Pin::new(io).poll_read(cx, &mut room))?;
        let n = room.filled().len();
        if n == 0 {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                eof,
            )));
        }
        self.rfill += n;
        Poll::Ready(Ok(()))
    }

    fn set_body_left(&mut self, left: usize) {
        self.state = if left == 0 {
            ReadState::Header
        } else {
            ReadState::Body { left }
        };
    }

    fn poll_read_inner(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.io.is_none() {
            return Poll::Ready(Ok(()));
        }
        loop {
            match self.state {
                ReadState::Ended => return Poll::Ready(Ok(())),
                ReadState::Header => {
                    if self.buffered() == 0 {
                        ready!(self.poll_fill(cx, HEADER_EOF))?;
                        continue;
                    }
                    let kind = self.rbuf[self.rpos];
                    let Some(need) = ChunkReader::<R>::header_len(kind) else {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unknown local chunk kind {kind:#04x}"),
                        )));
                    };
                    if self.buffered() < need {
                        ready!(self.poll_fill(cx, HEADER_EOF))?;
                        continue;
                    }
                    let header = &self.rbuf[self.rpos..self.rpos + need];
                    let chunk = match kind {
                        CHUNK_DATA => Chunk::Data(u32::from_le_bytes([
                            header[1], header[2], header[3], header[4],
                        ]) as usize),
                        CHUNK_FIN => Chunk::Fin,
                        _ => {
                            let mut code = [0u8; 8];
                            code.copy_from_slice(&header[1..HEADER_MAX]);
                            Chunk::Reset(u64::from_le_bytes(code))
                        }
                    };
                    self.rpos += need;
                    match chunk {
                        // A zero-length chunk carries no payload: the next
                        // header follows it directly.
                        Chunk::Data(0) => {}
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
                    if buf.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    if self.buffered() > 0 {
                        let n = buf.remaining().min(left).min(self.buffered());
                        buf.put_slice(&self.rbuf[self.rpos..self.rpos + n]);
                        self.rpos += n;
                        self.set_body_left(left - n);
                        return Poll::Ready(Ok(()));
                    }
                    if buf.remaining() >= left.min(READ_AHEAD) {
                        // The caller's buffer takes the payload as it comes,
                        // with no copy through the read-ahead buffer. A small
                        // caller buffer would only mean more reads, so it is
                        // served from the read-ahead instead.
                        let want = buf.remaining().min(left);
                        let Some(io) = self.io.as_mut() else {
                            return Poll::Ready(Ok(()));
                        };
                        buf.initialize_unfilled_to(want);
                        let mut slice = buf.take(want);
                        ready!(Pin::new(io).poll_read(cx, &mut slice))?;
                        let n = slice.filled().len();
                        if n == 0 {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                BODY_EOF,
                            )));
                        }
                        buf.advance(n);
                        self.set_body_left(left - n);
                        return Poll::Ready(Ok(()));
                    }
                    ready!(self.poll_fill(cx, BODY_EOF))?;
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    /// The bound on every await in these tests.
    const WAIT: Duration = Duration::from_secs(10);
    const WIRE_CAP: usize = 64 * 1024;

    fn exec() -> Exec {
        Exec::current().expect("a tokio runtime")
    }

    fn data_chunk(body: &[u8]) -> Vec<u8> {
        let mut wire = vec![CHUNK_DATA];
        wire.extend_from_slice(&(body.len() as u32).to_le_bytes());
        wire.extend_from_slice(body);
        wire
    }

    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 251) as u8).collect()
    }

    /// Reads to the end of the reader with `size`-byte caller buffers.
    async fn read_all<R>(reader: &mut ChunkReader<R>, size: usize) -> std::io::Result<Vec<u8>>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let mut out = Vec::new();
        let mut buf = vec![0u8; size];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                return Ok(out);
            }
            out.extend_from_slice(&buf[..n]);
        }
    }

    /// Feeds `wire` to a reader and then closes the stream.
    async fn read_wire(wire: &[u8]) -> std::io::Result<Vec<u8>> {
        let exec = exec();
        let (mut tx, rx) = tokio::io::duplex(WIRE_CAP);
        tx.write_all(wire).await.expect("write");
        drop(tx);
        let mut reader = ChunkReader::new(rx, exec.clone());
        exec.within(WAIT, read_all(&mut reader, 64 * 1024))
            .await
            .expect("bounded")
    }

    /// Writes `payload` through a `ChunkWriter` in `piece`-byte writes and
    /// reads it back with `size`-byte caller buffers.
    async fn round_trip(payload: &[u8], piece: usize, size: usize) -> Vec<u8> {
        let exec = exec();
        let (tx, rx) = tokio::io::duplex(WIRE_CAP);
        let owned = payload.to_vec();
        let writer_exec = exec.clone();
        let writer = tokio::spawn(async move {
            let mut writer = ChunkWriter::new(tx, writer_exec);
            for part in owned.chunks(piece) {
                writer.write_all(part).await?;
            }
            writer.shutdown().await
        });
        let mut reader = ChunkReader::new(rx, exec.clone());
        let out = exec
            .within(WAIT, read_all(&mut reader, size))
            .await
            .expect("bounded")
            .expect("read");
        exec.within(WAIT, writer)
            .await
            .expect("bounded")
            .expect("join")
            .expect("write");
        out
    }

    /// Counts the reads that reach the wrapped stream.
    struct Counting<T> {
        inner: T,
        reads: Arc<AtomicUsize>,
    }

    impl<T: AsyncRead + Unpin> AsyncRead for Counting<T> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            this.reads.fetch_add(1, Ordering::SeqCst);
            Pin::new(&mut this.inner).poll_read(cx, buf)
        }
    }

    #[tokio::test]
    async fn a_one_chunk_message_costs_at_most_two_reads() {
        let exec = exec();
        let mut wire = data_chunk(b"a short message");
        wire.push(CHUNK_FIN);
        let (mut tx, rx) = tokio::io::duplex(WIRE_CAP);
        tx.write_all(&wire).await.expect("write");
        let reads = Arc::new(AtomicUsize::new(0));
        let counted = Counting {
            inner: rx,
            reads: Arc::clone(&reads),
        };
        let mut reader = ChunkReader::new(counted, exec.clone());
        let out = exec
            .within(WAIT, read_all(&mut reader, 64 * 1024))
            .await
            .expect("bounded")
            .expect("read");
        assert_eq!(out, b"a short message");
        let reads = reads.load(Ordering::SeqCst);
        assert!(reads <= 2, "a one-chunk message took {reads} reads");
    }

    #[tokio::test]
    async fn payloads_decode_byte_identically() {
        for len in [0, 1, 4095, 4096, 4097, 1 << 20] {
            let sent = payload(len);
            let got = round_trip(&sent, 1000, 64 * 1024).await;
            assert!(got == sent, "a payload of {len} bytes changed in transit");
        }
    }

    #[tokio::test]
    async fn short_caller_reads_decode_byte_identically() {
        for (len, size) in [(4097, 3), (4096, 1), (1, 1)] {
            let sent = payload(len);
            let got = round_trip(&sent, 1000, size).await;
            assert!(
                got == sent,
                "a {len}-byte payload changed with {size}-byte reads"
            );
        }
    }

    #[tokio::test]
    async fn zero_length_chunks_are_skipped() {
        let mut wire = data_chunk(b"");
        wire.extend(data_chunk(b"abc"));
        wire.push(CHUNK_FIN);
        assert_eq!(read_wire(&wire).await.expect("read"), b"abc");
    }

    #[tokio::test]
    async fn an_unknown_chunk_kind_is_invalid_data() {
        let error = read_wire(&[0x7f]).await.expect_err("unknown kind");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn a_stream_closed_before_any_chunk_is_unexpected_eof() {
        let error = read_wire(&[]).await.expect_err("no chunk");
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn a_stream_closed_inside_a_header_is_unexpected_eof() {
        let error = read_wire(&[CHUNK_DATA, 5]).await.expect_err("short header");
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn a_stream_closed_inside_a_chunk_is_unexpected_eof() {
        let wire = data_chunk(b"0123456789");
        let error = read_wire(&wire[..7]).await.expect_err("short body");
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn a_reset_surfaces_the_peer_code() {
        let code = 0x0123_4567_89ab_cdef_u64;
        let mut wire = vec![CHUNK_RESET];
        wire.extend_from_slice(&code.to_le_bytes());
        let error = read_wire(&wire).await.expect_err("reset");
        let reset = error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<PeerReset>())
            .expect("a peer reset");
        assert_eq!(reset.0, code);
    }

    /// A sink that keeps what it accepts. It takes at most `limit` bytes per
    /// call and answers `Pending` once, when `stall_at` bytes are in.
    struct Sink {
        taken: Arc<Mutex<Vec<u8>>>,
        limit: usize,
        stall_at: Option<usize>,
    }

    impl AsyncWrite for Sink {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            let mut taken = this.taken.lock().expect("sink");
            let len = taken.len();
            if this.stall_at == Some(len) {
                this.stall_at = None;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let room = this.stall_at.map_or(usize::MAX, |at| at - len);
            let n = buf.len().min(this.limit).min(room);
            taken.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn end_writes_the_marker_without_a_task() {
        let taken = Arc::new(Mutex::new(Vec::new()));
        let sink = Sink {
            taken: Arc::clone(&taken),
            limit: usize::MAX,
            stall_at: None,
        };
        let mut writer = ChunkWriter::new(sink, exec());
        writer.write_all(b"abc").await.expect("write");
        writer.end(Marker::Fin, ());
        // Nothing awaits between `end` and this check, so a task spawned by
        // `end` cannot have run yet.
        let mut expected = data_chunk(b"abc");
        expected.push(CHUNK_FIN);
        assert_eq!(*taken.lock().expect("sink"), expected);
    }

    #[tokio::test]
    async fn end_writes_what_the_stream_did_not_take_on_a_task() {
        let taken = Arc::new(Mutex::new(Vec::new()));
        let sink = Sink {
            taken: Arc::clone(&taken),
            limit: 4,
            stall_at: Some(4),
        };
        let exec = exec();
        let writer = ChunkWriter::new(sink, exec.clone());
        let code = 0x0102_0304_0506_0708_u64;
        writer.end(Marker::Reset(code), ());
        assert_eq!(taken.lock().expect("sink").len(), 4);
        exec.within(WAIT, async {
            while taken.lock().expect("sink").len() < 9 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the remainder of the marker was written");
        let mut expected = vec![CHUNK_RESET];
        expected.extend_from_slice(&code.to_le_bytes());
        assert_eq!(*taken.lock().expect("sink"), expected);
    }

    /// Reports the bytes it hands out and, on drop, how many that was.
    struct Watched<T> {
        inner: T,
        delivered: usize,
        released: Option<oneshot::Sender<usize>>,
    }

    impl<T: AsyncRead + Unpin> AsyncRead for Watched<T> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let before = buf.filled().len();
            let polled = Pin::new(&mut this.inner).poll_read(cx, buf);
            this.delivered += buf.filled().len() - before;
            polled
        }
    }

    impl<T> Drop for Watched<T> {
        fn drop(&mut self) {
            if let Some(released) = self.released.take() {
                let _ = released.send(self.delivered);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_drain_releases_its_reader() {
        let exec = exec();
        let (mut tx, rx) = tokio::io::duplex(WIRE_CAP);
        // A payload that promises ten bytes and delivers three.
        tx.write_all(&[CHUNK_DATA, 10, 0, 0, 0, 1, 2, 3])
            .await
            .expect("write");
        let (released, dropped) = oneshot::channel();
        let reader = ChunkReader::new(
            Watched {
                inner: rx,
                delivered: 0,
                released: Some(released),
            },
            exec.clone(),
        );
        reader.drain_within(Duration::from_millis(50), ());
        let delivered = exec
            .within(WAIT, dropped)
            .await
            .expect("the drain gave up on the stalled peer")
            .expect("the reader was dropped");
        assert_eq!(delivered, 8);
        drop(tx);
    }

    #[tokio::test(start_paused = true)]
    async fn a_payload_still_flowing_drains_to_its_fin() {
        const PIECES: usize = 20;
        const PIECE: usize = 100;
        let exec = exec();
        let (mut tx, rx) = tokio::io::duplex(WIRE_CAP);
        let pacing = exec.clone();
        // Each gap is well under the idle bound, and the whole flow is well
        // over it: only a per-read bound may release the reader.
        let writer = tokio::spawn(async move {
            for _ in 0..PIECES {
                tx.write_all(&data_chunk(&[7u8; PIECE])).await?;
                pacing.sleep(Duration::from_millis(25)).await;
            }
            tx.write_all(&[CHUNK_FIN]).await
        });
        let (released, dropped) = oneshot::channel();
        let reader = ChunkReader::new(
            Watched {
                inner: rx,
                delivered: 0,
                released: Some(released),
            },
            exec.clone(),
        );
        reader.drain_within(Duration::from_millis(200), ());
        let delivered = exec
            .within(WAIT, dropped)
            .await
            .expect("the drain reached the FIN")
            .expect("the reader was dropped");
        assert_eq!(delivered, PIECES * (HEADER_DATA + PIECE) + 1);
        exec.within(WAIT, writer)
            .await
            .expect("bounded")
            .expect("join")
            .expect("write");
    }

    /// Appends its name to a shared log when it is dropped: the order in which
    /// a stream and what counts its connection are let go.
    struct Dropped {
        name: &'static str,
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Dropped {
        fn new(name: &'static str, log: &Arc<Mutex<Vec<&'static str>>>) -> Dropped {
            Dropped {
                name,
                log: Arc::clone(log),
            }
        }
    }

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.log.lock().expect("log").push(self.name);
        }
    }

    /// A reader that logs when it is dropped.
    struct Traced<T> {
        inner: T,
        _dropped: Dropped,
    }

    impl<T: AsyncRead + Unpin> AsyncRead for Traced<T> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
        }
    }

    async fn logged(log: &Arc<Mutex<Vec<&'static str>>>, entries: usize) {
        while log.lock().expect("log").len() < entries {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn an_end_marker_finished_on_a_task_holds_what_counts_the_connection() {
        let exec = exec();
        let log = Arc::new(Mutex::new(Vec::new()));
        let taken = Arc::new(Mutex::new(Vec::new()));
        let sink = Sink {
            taken: Arc::clone(&taken),
            limit: 4,
            stall_at: Some(4),
        };
        let writer = ChunkWriter::new(sink, exec.clone());
        writer.end(Marker::Reset(7), Dropped::new("hold", &log));
        assert_eq!(taken.lock().expect("sink").len(), 4);
        assert!(
            log.lock().expect("log").is_empty(),
            "the task still owns the stream, so it still holds"
        );
        exec.within(WAIT, logged(&log, 1))
            .await
            .expect("the task finished the marker and let go");
        assert_eq!(taken.lock().expect("sink").len(), 9);
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        #[tokio::test]
        async fn a_drain_lets_go_of_what_counts_the_connection_after_the_handle() {
            let exec = exec();
            let log = Arc::new(Mutex::new(Vec::new()));
            let (mut tx, rx) = tokio::io::duplex(WIRE_CAP);
            // A payload that promises ten bytes, delivers three and goes quiet.
            tx.write_all(&[CHUNK_DATA, 10, 0, 0, 0, 1, 2, 3])
                .await
                .expect("write");
            let reader = ChunkReader::new(
                Traced {
                    inner: rx,
                    _dropped: Dropped::new("handle", &log),
                },
                exec.clone(),
            );
            reader.drain_within(Duration::from_millis(50), Dropped::new("hold", &log));
            assert!(
                log.lock().expect("log").is_empty(),
                "the drain owns both until it ends"
            );
            exec.within(WAIT, logged(&log, 2))
                .await
                .expect("the drain gave up on the stalled peer");
            assert_eq!(*log.lock().expect("log"), ["handle", "hold"]);
            drop(tx);
        }
    }
}
