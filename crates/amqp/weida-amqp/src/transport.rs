//! The byte stream under the frames, and the two ways TLS gets under it.
//!
//! AMQP defines TCP and nothing else: `PORT` 5672 and `SECURE-PORT` 5671, with
//! UDP and SCTP assignments "reserved" and no mapping defined
//! (Part 2 §2.8.19). TLS is a *layer*, reached one of two ways
//! (Part 5 §5.2):
//!
//! ```text
//! Layered      client -> AMQP %d2 1.0.0     server -> AMQP %d2 1.0.0
//!              ...TLS handshake...
//!              client -> AMQP %d0 1.0.0     server -> AMQP %d0 1.0.0   (inside TLS)
//!
//! Direct       ...TLS handshake...                                      (no header)
//!              client -> AMQP %d0 1.0.0     server -> AMQP %d0 1.0.0   (inside TLS)
//! ```
//!
//! The difference is observable and neither substitutes for the other: the
//! layered form announces itself in the clear on 5672, the direct form is a
//! pure TLS listener on 5671 that would see `AMQP %d2 1.0.0` as a failed
//! handshake. [`TlsMode`](crate::TlsMode) is the caller's choice between them.
//!
//! # The trust anchors are not here
//!
//! `upgrade_tls` takes the caller's `rustls::ClientConfig`. Part 5 §5.2
//! requires the TLS client to validate the server certificate and this crate
//! has no opinion about which certificates are valid: an application talking
//! to a broker on its own network trusts a private CA, one talking to Service
//! Bus trusts the public roots, and a messaging library that shipped a
//! default would be making that decision for both. What this crate does
//! supply is the SNI name, because Part 5 §5.2.1 notes it "can select both
//! the back end and the domain against which to validate client
//! credentials" — so getting it from the dialled host rather than from
//! nowhere is a protocol behaviour, not a convenience.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use weida_amqp_codec::frame::{self, Frame, FrameKind, MIN_FRAME_SIZE};
use weida_amqp_codec::protocol_header::{self, LEN as HEADER_LEN, ProtocolHeader};
use weida_amqp_codec::{DecodeError, EncodeError};
use weida_runtime::Exec;

use crate::error::{Error, Result};
use crate::options::ConnectionOptions;

/// The byte stream a connection runs on.
///
/// An enum rather than a boxed trait object: there are exactly two cases, the
/// dispatch is one branch per read, and a `Box<dyn AsyncRead + AsyncWrite>`
/// would add an allocation and a vtable call to every frame for no gain.
#[derive(Debug)]
pub enum Stream {
    /// Plain TCP.
    Plain(TcpStream),
    /// TCP under TLS.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Stream {
    /// The peer's address, for a log line.
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        match self {
            Self::Plain(stream) => Ok(stream.peer_addr()?),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Ok(stream.get_ref().0.peer_addr()?),
        }
    }

    /// Whether the stream is encrypted.
    ///
    /// Not cosmetic: `PLAIN` carries a password in the clear, so a caller
    /// deciding whether to offer it wants to be able to ask.
    #[must_use]
    pub const fn is_encrypted(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            #[cfg(feature = "tls")]
            Self::Tls(_) => true,
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Opens a plain TCP connection to `host:port`, trying every address the
/// resolver offered in its order.
///
/// Every address, not the first: `localhost` commonly resolves to both `::1`
/// and `127.0.0.1`, and a broker bound to one of them is unreachable through
/// the other. The count is capped by
/// [`ConnectionOptions::max_resolved_addresses`], because a resolver answer is
/// remote input.
///
/// The name lookup and every attempt share **one** budget,
/// [`ConnectionOptions::handshake_timeout`]. A budget per address would let a
/// handful of addresses that swallow packets hold the caller for that long
/// each, and a lookup outside any budget would hold it for as long as the
/// resolver took.
pub async fn connect(
    exec: &Exec,
    host: &str,
    port: u16,
    options: &ConnectionOptions,
) -> Result<Stream> {
    let budget = Budget::start(options.handshake_timeout);
    let lookup = exec.resolve(host, port, options.max_resolved_addresses);
    connect_to(exec, &budget, lookup).await
}

/// [`connect`] with the lookup handed in: what the resolver answers, inside
/// what is left of `budget`.
async fn connect_to<L>(exec: &Exec, budget: &Budget, lookup: L) -> Result<Stream>
where
    L: Future<Output = std::result::Result<Vec<SocketAddr>, weida_core::Error>>,
{
    let addrs = match exec.within(budget.left(), lookup).await {
        Some(answer) => answer?,
        None => {
            return Err(Error::HandshakeTimeout {
                step: "the name lookup",
            });
        }
    };
    dial(exec, &addrs, budget).await
}

/// What is left of one time budget, spent a step at a time.
struct Budget {
    /// Where the budget runs out. `None` only where the total is so large
    /// that the instant cannot be represented, in which case no step is given
    /// less than the whole of it.
    ends: Option<Instant>,
    whole: Duration,
}

impl Budget {
    fn start(whole: Duration) -> Self {
        Self {
            ends: Instant::now().checked_add(whole),
            whole,
        }
    }

    fn left(&self) -> Duration {
        self.ends.map_or(self.whole, |ends| {
            ends.saturating_duration_since(Instant::now())
        })
    }
}

/// Tries `addrs` in order inside what is left of `budget`.
///
/// Every attempt but the last gets half of what is left, so an address that
/// swallows packets — the unreachable IPv6 half of a dual-stack name — leaves
/// the next one time to answer. An attempt that runs out is aborted rather
/// than left to finish a connect nobody is waiting for.
async fn dial(exec: &Exec, addrs: &[SocketAddr], budget: &Budget) -> Result<Stream> {
    if addrs.is_empty() {
        return Err(Error::Runtime(weida_core::Error::InvalidAddress(
            "the resolver offered no addresses".into(),
        )));
    }
    let mut last = None;
    for (index, addr) in addrs.iter().enumerate() {
        let left = budget.left();
        let share = if index + 1 < addrs.len() {
            left / 2
        } else {
            left
        };
        if share.is_zero() {
            last = Some(Error::HandshakeTimeout {
                step: "the TCP connect",
            });
            break;
        }
        // `TcpStream::connect` registers the socket with the reactor as it is
        // built, and a caller driving this future on its own executor is not
        // inside one. Spawning puts the constructor where the reactor is.
        let mut attempt = exec.spawn(TcpStream::connect(*addr));
        match exec.within(share, &mut attempt).await {
            Some(Ok(Ok(stream))) => {
                // Nagle off: an AMQP handshake is a sequence of small frames
                // that each need an answer, and coalescing them adds a round
                // trip's worth of delay to every step.
                let _ = stream.set_nodelay(true);
                return Ok(Stream::Plain(stream));
            }
            Some(Ok(Err(error))) => last = Some(Error::Io(error)),
            Some(Err(error)) => {
                last = Some(Error::Runtime(weida_core::Error::Runtime(format!(
                    "connect task failed: {error}"
                ))));
            }
            None => {
                attempt.abort();
                last = Some(Error::HandshakeTimeout {
                    step: "the TCP connect",
                });
            }
        }
    }
    Err(last.unwrap_or(Error::HandshakeTimeout {
        step: "the TCP connect",
    }))
}

/// Puts TLS under an already-connected stream.
///
/// `server_name` is what the certificate is validated against and what goes
/// in SNI. The caller's `config` decides what "valid" means.
#[cfg(feature = "tls")]
pub async fn upgrade_tls(
    stream: Stream,
    config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    server_name: &str,
) -> Result<Stream> {
    let Stream::Plain(tcp) = stream else {
        return Err(Error::Tls("the stream is already under TLS".into()));
    };
    let name = tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_owned())
        .map_err(|_| Error::Tls(format!("{server_name} is not a valid server name")))?;
    let connector = tokio_rustls::TlsConnector::from(config);
    let tls = connector
        .connect(name, tcp)
        .await
        .map_err(|error| Error::Tls(error.to_string()))?;
    Ok(Stream::Tls(Box::new(tls)))
}

/// A framed AMQP byte stream: protocol headers and frames, with one read
/// buffer.
///
/// The buffer exists because a frame arrives in however many TCP segments the
/// network chose, and the codec needs it contiguous. It grows to at most one
/// frame plus whatever of the next frame arrived with it, which is why
/// `max_frame_size` is held here: the bound on the buffer is the bound on a
/// frame, and before `open` has been read that bound is
/// [`MIN_MAX_FRAME_SIZE`](weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE).
#[derive(Debug)]
pub struct Wire {
    stream: Stream,
    read: Vec<u8>,
    /// Where the unconsumed octets start inside `read`.
    from: usize,
    max_frame_size: u32,
    write: Vec<u8>,
}

impl Wire {
    /// Wraps a stream, with the pre-negotiation frame bound in force.
    #[must_use]
    pub fn new(stream: Stream) -> Self {
        Self {
            stream,
            read: Vec::new(),
            from: 0,
            max_frame_size: weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE,
            write: Vec::new(),
        }
    }

    /// Raises the frame bound to what the peer's `open` advertised.
    ///
    /// One-way on purpose: the bound starts at 512 and only ever grows, so a
    /// forgotten call leaves the strictest check in force rather than the
    /// loosest.
    pub fn negotiated(&mut self, max_frame_size: u32) {
        self.max_frame_size = self.max_frame_size.max(max_frame_size);
    }

    /// The frame bound currently in force.
    #[must_use]
    pub const fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// The stream underneath, for the TLS upgrade and for the peer address.
    #[must_use]
    pub const fn stream(&self) -> &Stream {
        &self.stream
    }

    /// Consumes the wire, handing back the stream.
    ///
    /// Used for exactly one thing: the `%d2` layering, where the header
    /// exchange happens on the plain stream and everything after it happens
    /// inside TLS. Any buffered octets are discarded, which is correct —
    /// a server that pipelined anything after its `%d2` header sent it before
    /// the handshake and it cannot be part of the TLS session.
    #[must_use]
    pub fn into_stream(self) -> Stream {
        self.stream
    }

    /// Sends `header` and flushes it.
    pub async fn write_protocol_header(&mut self, header: ProtocolHeader) -> Result<()> {
        self.stream.write_all(&header.encode()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Reads the peer's eight-octet protocol header.
    pub async fn read_protocol_header(&mut self) -> Result<ProtocolHeader> {
        self.fill_to(HEADER_LEN).await?;
        let header =
            protocol_header::decode(&self.read[self.from..]).map_err(Error::BadProtocolHeader)?;
        self.from += HEADER_LEN;
        Ok(header)
    }

    /// Appends to the write buffer. Nothing leaves until [`Wire::flush`].
    pub fn queue(&mut self) -> &mut Vec<u8> {
        &mut self.write
    }

    /// Writes and clears the write buffer.
    pub async fn flush(&mut self) -> Result<()> {
        if self.write.is_empty() {
            return Ok(());
        }
        self.stream.write_all(&self.write).await?;
        self.write.clear();
        self.stream.flush().await?;
        Ok(())
    }

    /// Reads one frame, borrowing its body from the read buffer.
    ///
    /// The bound in force is applied to the declared `SIZE` from the
    /// eight-octet header alone, so a peer that declares four gigabytes
    /// before `open` has been read is refused on those eight octets and
    /// nothing is reserved.
    pub async fn read_frame(&mut self) -> Result<Frame<'_>> {
        self.fill_to(MIN_FRAME_SIZE as usize).await?;
        // The header is present; it decides how much more is needed, and the
        // ceiling is checked here rather than after the body arrives.
        let header = frame::decode_header(&self.read[self.from..], self.max_frame_size)?;
        let size = header.size as usize;
        self.fill_to(size).await?;
        let frame = frame::decode(&self.read[self.from..], self.max_frame_size)?;
        self.from += size;
        Ok(frame)
    }

    /// Splits the wire for the connection driver.
    ///
    /// The handshake is a strict request/response sequence and reads and
    /// writes on one object; everything after it is not — a `flow` may have
    /// to go out while a `transfer` is half-read — so the driver needs the
    /// two directions separately. Any octets already buffered travel with
    /// the reader, because a server that pipelined frames behind its `open`
    /// is doing something the specification explicitly permits
    /// (Part 2 §2.4.2) and dropping them would lose them.
    #[must_use]
    pub fn split(self) -> (FrameReader, FrameWriter) {
        let (read, write) = tokio::io::split(self.stream);
        let mut buf = BytesMut::with_capacity(READ_CHUNK);
        buf.extend_from_slice(&self.read[self.from..]);
        (
            FrameReader {
                read,
                buf,
                max_frame_size: self.max_frame_size,
            },
            FrameWriter {
                write,
                max_frame_size: self.max_frame_size,
            },
        )
    }

    /// Ensures at least `want` unconsumed octets are buffered.
    async fn fill_to(&mut self, want: usize) -> Result<()> {
        fill_to(&mut self.stream, &mut self.read, &mut self.from, want).await
    }
}

/// How much one read asks the kernel for, and so how large a read buffer is.
///
/// A frame is read into a buffer this size and a delivery is a slice of the
/// frame it arrived in, so a delivery the application holds keeps the whole
/// buffer alive: this is the most one held delivery can pin beyond its own
/// octets, and `credit granted x READ_CHUNK` is the most a peer can make an
/// application pin by sending small messages one buffer apart. A frame larger
/// than this gets a buffer of its own size.
pub const READ_CHUNK: usize = 64 * 1024;

/// Reads whole frames from one half of a split wire.
///
/// Frames come out of one shared read buffer as [`Bytes`], not copies: a frame
/// is the octets the kernel handed over, and a one-frame delivery reaches the
/// application as a slice of it. The buffer belongs to the reader alone, so
/// [`FrameReader::next_frame`] can be dropped half-way — as the driver's
/// `select!` does whenever another branch is ready first — without losing an
/// octet: everything read so far is in the buffer, and the next call goes on
/// from there.
#[derive(Debug)]
pub struct FrameReader {
    read: tokio::io::ReadHalf<Stream>,
    buf: BytesMut,
    max_frame_size: u32,
}

impl FrameReader {
    /// Raises the frame bound to what the peer's `open` advertised.
    pub fn negotiated(&mut self, max_frame_size: u32) {
        self.max_frame_size = self.max_frame_size.max(max_frame_size);
    }

    /// The next whole frame, header included.
    ///
    /// The ceiling is applied to the declared `SIZE` from the eight-octet
    /// header alone, before the body is waited for, and the buffer is sized
    /// from the same header: nothing the peer declares is reserved beyond what
    /// that ceiling admits.
    ///
    /// Cancel-safe.
    pub async fn next_frame(&mut self) -> Result<Bytes> {
        loop {
            let want = match frame::decode_header(&self.buf, self.max_frame_size) {
                Ok(header) => {
                    let size = header.size as usize;
                    if self.buf.len() >= size {
                        return Ok(self.buf.split_to(size).freeze());
                    }
                    size
                }
                Err(DecodeError::Incomplete { .. }) => MIN_FRAME_SIZE as usize,
                Err(error) => return Err(error.into()),
            };
            let missing = want - self.buf.len();
            if self.buf.capacity() - self.buf.len() < missing.max(MIN_READ) {
                self.buf.reserve(missing.max(READ_CHUNK));
            }
            if self.read.read_buf(&mut self.buf).await? == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "the peer closed the transport",
                )));
            }
        }
    }
}

/// The least spare room worth a read: below it the buffer is topped up first,
/// so a read is never a handful of octets.
const MIN_READ: usize = 4096;

/// Writes to one half of a split wire.
///
/// Two calls and no more, both cancel-safe: [`FrameWriter::write`] hands the
/// stream as much as it will take in one go, and [`FrameWriter::flush`] pushes
/// out whatever a TLS session still holds. The caller owns the octets and the
/// clock — the driver's outbox — because a write that waits on a peer is exactly
/// what must not be able to hold up the task that also reads.
#[derive(Debug)]
pub struct FrameWriter {
    write: tokio::io::WriteHalf<Stream>,
    max_frame_size: u32,
}

impl FrameWriter {
    /// Raises the frame bound to what the peer's `open` advertised.
    pub fn negotiated(&mut self, max_frame_size: u32) {
        self.max_frame_size = self.max_frame_size.max(max_frame_size);
    }

    /// The bound a frame this client sends must stay inside.
    #[must_use]
    pub const fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// Hands the stream as much of `bytes` as it will take now, and says how
    /// much that was.
    ///
    /// One write. If the future is dropped before it completes nothing was
    /// written, which is what makes it usable as a `select!` branch.
    pub async fn write(&mut self, bytes: &[u8]) -> Result<usize> {
        let taken = self.write.write(bytes).await?;
        if taken == 0 && !bytes.is_empty() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "the transport accepted no octets",
            )));
        }
        Ok(taken)
    }

    /// Pushes out what the stream is still holding: a TLS session buffers a
    /// record until told to send it. Nothing for a plain socket.
    pub async fn flush(&mut self) -> Result<()> {
        self.write.flush().await?;
        Ok(())
    }

    /// One turn of getting queued octets to the peer: a write if `pending` has
    /// anything in it, and the flush that follows the last of them otherwise.
    ///
    /// One future, because the driver's `select!` builds every branch's
    /// future at once and a writer can be borrowed by one of them only. Both
    /// halves are cancel-safe, so the whole is.
    pub(crate) async fn step(&mut self, pending: &[u8]) -> Result<Step> {
        if pending.is_empty() {
            self.flush().await?;
            Ok(Step::Flushed)
        } else {
            Ok(Step::Wrote(self.write(pending).await?))
        }
    }

    /// Closes the write half, which is what "`close` is the last thing ever
    /// written" comes to in practice. A TLS session writes its closing alert
    /// first, which is a write like any other and can wait on a peer, so the
    /// caller bounds this one too.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.write.shutdown().await?;
        Ok(())
    }
}

/// What one [`FrameWriter::step`] did.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Step {
    /// The stream took this many octets.
    Wrote(usize),
    /// The stream was flushed.
    Flushed,
}

/// How many octets may be queued for the peer before the driver stops taking
/// more from anywhere.
///
/// **A bound of ours.** Everything the connection writes is queued in an
/// outbox and goes out as the socket takes it; a peer that has stopped
/// reading leaves it queued. Past this the driver stops taking frames from
/// local senders, and it stops reading the peer once the *answers* to the
/// peer's own frames are what is past it — not what the application queued,
/// which a peer streaming in both directions would otherwise turn into a
/// reason for neither side ever to read. So what a stalled peer can make the
/// driver hold is this much of the application's frames plus this much of
/// answers, and the stall clock ([`ConnectionOptions::write_timeout`]) ends
/// the connection if it lasts.
pub const OUTBOX_HIGH_WATER: usize = 256 * 1024;

/// Octets queued for the peer, and how far the socket has taken them.
///
/// Frames are built straight into the buffer — [`frame::write`] appends and
/// backfills the size, with no scratch buffer — and leave in whatever
/// batches the socket accepts, so frames queued in one turn of the driver go
/// out in one `write` rather than one per frame.
#[derive(Debug)]
pub(crate) struct Outbox {
    buf: Vec<u8>,
    /// How much of `buf` the socket has taken.
    sent: usize,
    /// Whether octets have gone to the stream since it was last flushed.
    dirty: bool,
    /// The largest frame the peer will take.
    max_frame_size: u32,
    /// Where `buf[0]` sits among every octet ever queued, counted from the
    /// first: what lets a stretch of the queue be named after the fact.
    base: u64,
    /// The stretches of the queue that are answers to what the peer sent,
    /// `(from, to)` in the counting above and oldest first, as far as the
    /// socket has not taken them.
    answers: VecDeque<(u64, u64)>,
    /// How many octets of `answers` are still waiting for the socket.
    answering: usize,
}

impl Outbox {
    pub(crate) const fn new(max_frame_size: u32) -> Self {
        Self {
            buf: Vec::new(),
            sent: 0,
            dirty: false,
            max_frame_size,
            base: 0,
            answers: VecDeque::new(),
            answering: 0,
        }
    }

    /// The bound a frame this client sends must stay inside.
    pub(crate) const fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// Queues an AMQP frame on `channel`, its body written by `build`.
    ///
    /// Nothing is queued where `build` fails or the frame is above the
    /// peer's `max-frame-size`.
    pub(crate) fn frame<F>(
        &mut self,
        channel: u16,
        build: F,
    ) -> std::result::Result<(), EncodeError>
    where
        F: FnOnce(&mut Vec<u8>) -> std::result::Result<(), EncodeError>,
    {
        frame::write(
            &mut self.buf,
            FrameKind::Amqp,
            channel,
            self.max_frame_size,
            build,
        )
    }

    /// Queues a frame built elsewhere.
    pub(crate) fn push(&mut self, frame: &[u8]) {
        self.buf.extend_from_slice(frame);
    }

    /// What the socket has not taken yet.
    pub(crate) fn pending(&self) -> &[u8] {
        &self.buf[self.sent..]
    }

    /// Whether anything is waiting for the socket.
    pub(crate) fn has_pending(&self) -> bool {
        self.sent < self.buf.len()
    }

    /// How many octets are waiting for the socket.
    pub(crate) fn len(&self) -> usize {
        self.buf.len() - self.sent
    }

    /// Whether so much is waiting that nothing more should be taken from
    /// local senders.
    pub(crate) fn congested(&self) -> bool {
        self.len() >= OUTBOX_HIGH_WATER
    }

    /// Where the next octet queued will sit, in the counting
    /// [`Outbox::answered`] takes.
    pub(crate) fn end(&self) -> u64 {
        self.base + self.buf.len() as u64
    }

    /// Records that everything queued since `from`, a position taken from
    /// [`Outbox::end`], is an answer to something the peer sent: a `flow` it
    /// asked for, a `disposition` it owes a reply to, a `close` it provoked.
    pub(crate) fn answered(&mut self, from: u64) {
        let to = self.end();
        if to > from {
            self.answers.push_back((from, to));
            self.answering += usize::try_from(to - from).unwrap_or(usize::MAX);
        }
    }

    /// Whether the answers to the peer's frames, alone, are past the
    /// high-water mark: what a peer that never reads can make the driver
    /// hold of what it provoked, and the reason to stop reading it.
    ///
    /// Not [`Outbox::congested`]: that counts the application's frames too,
    /// and an application streaming to a peer that is streaming back would
    /// keep it true for as long as the socket is slower than the
    /// application, so neither end would ever read.
    pub(crate) fn answers_congested(&self) -> bool {
        self.answering >= OUTBOX_HIGH_WATER
    }

    /// Records that the socket took `taken` octets.
    pub(crate) fn took(&mut self, taken: usize) {
        // Where the socket has got to, in the counting of `base`.
        let through = self.base + (self.sent + taken) as u64;
        self.sent += taken;
        self.dirty = true;
        if self.sent == self.buf.len() {
            self.base += self.buf.len() as u64;
            self.buf.clear();
            self.sent = 0;
            // A burst should not leave a large buffer behind for good.
            if self.buf.capacity() > 4 * OUTBOX_HIGH_WATER {
                self.buf.shrink_to(OUTBOX_HIGH_WATER);
            }
        } else if self.sent >= self.buf.len() / 2 && self.sent >= MIN_READ {
            // Never let the dead prefix outgrow what is still waiting.
            self.base += self.sent as u64;
            self.buf.drain(..self.sent);
            self.sent = 0;
        }
        while let Some((from, to)) = self.answers.front_mut() {
            if *from >= through {
                break;
            }
            let upto = (*to).min(through);
            self.answering -= usize::try_from(upto - *from).unwrap_or(usize::MAX);
            if *to <= through {
                self.answers.pop_front();
            } else {
                *from = through;
                break;
            }
        }
    }

    /// Whether the stream was written to and has not been flushed since.
    pub(crate) fn needs_flush(&self) -> bool {
        self.dirty && !self.has_pending()
    }

    /// Records that the stream was flushed.
    pub(crate) fn flushed(&mut self) {
        self.dirty = false;
    }

    /// Whether the socket still owes something: octets it has not taken, or
    /// a flush. What the stall clock runs while it is true.
    pub(crate) fn owes(&self) -> bool {
        self.has_pending() || self.dirty
    }
}

/// Gives the socket everything queued, and flushes it.
///
/// For ending a connection, where the octets still queued — `close` above all
/// — should leave if the peer will take them; the caller bounds how long that
/// may take.
pub(crate) async fn drain(writer: &mut FrameWriter, out: &mut Outbox) -> Result<()> {
    while out.has_pending() {
        let taken = writer.write(out.pending()).await?;
        out.took(taken);
    }
    if out.needs_flush() {
        writer.flush().await?;
        out.flushed();
    }
    Ok(())
}

/// Ensures at least `want` unconsumed octets sit in `buf` at or after
/// `from`.
///
/// For [`Wire`], the handshake's reader. The driver's reader keeps its own
/// buffer, because a frame there is handed out as a slice of it.
async fn fill_to<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    from: &mut usize,
    want: usize,
) -> Result<()> {
    loop {
        if buf.len() - *from >= want {
            return Ok(());
        }
        // Reclaim the consumed prefix before growing. Done here rather than
        // after every frame so that a run of small frames inside one segment
        // is not memmoved once per frame.
        if *from > 0 {
            buf.drain(..*from);
            *from = 0;
            if buf.len() >= want {
                return Ok(());
            }
        }
        buf.reserve(want.saturating_sub(buf.len()));
        let read = reader.read_buf(buf).await?;
        if read == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the peer closed the transport",
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_amqp_codec::frame::FrameKind;

    /// A `Wire` over a pair of in-process sockets, for tests that need to put
    /// exact octets in front of the reader.
    async fn wired() -> (Wire, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        let client = client.await.unwrap();
        (Wire::new(Stream::Plain(client)), server)
    }

    #[tokio::test]
    async fn a_frame_split_across_reads_is_reassembled() {
        let (mut wire, mut server) = wired().await;
        let mut whole = Vec::new();
        frame::write(&mut whole, FrameKind::Amqp, 7, 512, |body| {
            body.extend_from_slice(b"performative");
            Ok(())
        })
        .unwrap();
        let (first, second) = whole.split_at(5);
        server.write_all(first).await.unwrap();
        server.flush().await.unwrap();
        let first = first.to_vec();
        let second = second.to_vec();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            server.write_all(&second).await.unwrap();
            server.flush().await.unwrap();
            // Hold the socket open so the reader does not see EOF.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });
        let frame = wire.read_frame().await.expect("a frame");
        assert_eq!(frame.header.channel, 7);
        assert_eq!(frame.body, b"performative");
        assert_eq!(first.len(), 5);
    }

    #[tokio::test]
    async fn two_frames_in_one_segment_are_read_one_at_a_time() {
        let (mut wire, mut server) = wired().await;
        let mut both = Vec::new();
        for channel in [1u16, 2] {
            frame::write(&mut both, FrameKind::Amqp, channel, 512, |body| {
                body.push(channel as u8);
                Ok(())
            })
            .unwrap();
        }
        server.write_all(&both).await.unwrap();
        server.flush().await.unwrap();
        let keep = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            drop(server);
        });
        for channel in [1u16, 2] {
            let frame = wire.read_frame().await.expect("a frame");
            assert_eq!(frame.header.channel, channel);
            assert_eq!(frame.body, &[channel as u8]);
        }
        keep.abort();
    }

    #[tokio::test]
    async fn the_pre_negotiation_ceiling_is_in_force_before_open() {
        let (mut wire, mut server) = wired().await;
        assert_eq!(
            wire.max_frame_size(),
            weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE
        );
        // Eight octets claiming four gigabytes. The refusal comes from the
        // header alone: no body is sent at all, and the test would hang
        // rather than fail if the reader waited for one.
        server
            .write_all(&[0xff, 0xff, 0xff, 0xff, 0x02, 0x00, 0x00, 0x00])
            .await
            .unwrap();
        server.flush().await.unwrap();
        let error = wire.read_frame().await.expect_err("refused");
        assert!(
            matches!(
                error,
                Error::Decode(weida_amqp_codec::DecodeError::FrameTooLarge { max: 512, .. })
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn the_ceiling_only_ever_grows() {
        let (mut wire, _server) = wired().await;
        wire.negotiated(128 * 1024);
        assert_eq!(wire.max_frame_size(), 128 * 1024);
        wire.negotiated(16);
        assert_eq!(
            wire.max_frame_size(),
            128 * 1024,
            "a forgotten or a smaller negotiation leaves the stricter bound"
        );
    }

    #[tokio::test]
    async fn a_closed_transport_is_reported_as_end_of_file() {
        let (mut wire, server) = wired().await;
        drop(server);
        let error = wire.read_frame().await.expect_err("the peer is gone");
        assert!(matches!(error, Error::Io(_)), "{error}");
    }

    /// What the application queued does not count as answers to the peer:
    /// 300 KiB of its frames congest the outbox and leave reading alone, which
    /// is what lets two peers that both stream bulk go on reading each other.
    #[test]
    fn the_applications_frames_are_not_answers_to_the_peer() {
        let mut out = Outbox::new(512);
        out.push(&vec![0u8; 300 * 1024]);
        assert!(out.congested(), "the application's frames fill the outbox");
        assert!(
            !out.answers_congested(),
            "and say nothing about what the peer is owed"
        );
    }

    /// What the peer provoked is counted from where it was queued, wherever
    /// the application's frames sit around it, and stops counting as the
    /// socket takes it — the application's octets taken first, the answers
    /// after, and a partial take of an answer counted down to the octet.
    #[test]
    fn answers_are_counted_where_they_sit_and_leave_as_the_socket_takes_them() {
        let mut out = Outbox::new(512);
        out.push(&[1u8; 1000]);
        let from = out.end();
        out.push(&[2u8; OUTBOX_HIGH_WATER]);
        out.answered(from);
        out.push(&[3u8; 500]);
        assert!(out.answers_congested(), "the answers alone fill the mark");

        out.took(1000);
        assert!(out.answers_congested(), "the application's octets went");
        out.took(1);
        assert!(!out.answers_congested(), "one octet of the answers went");
        out.took(OUTBOX_HIGH_WATER - 1);
        assert!(!out.answers_congested());
        assert_eq!(out.len(), 500, "only the application's octets are left");
        out.took(500);
        assert!(!out.owes() || out.needs_flush());
        assert!(!out.has_pending());

        // And a position taken again after everything was taken still means
        // the same octets.
        let from = out.end();
        out.push(&[4u8; OUTBOX_HIGH_WATER]);
        out.answered(from);
        assert!(out.answers_congested());
    }

    /// A listener whose accept queue is full, which makes a connect to it
    /// neither complete nor fail: the kernel drops the `SYN`, as a link that
    /// swallows packets does. Linux only, because it is the Linux queue that
    /// is being filled.
    #[cfg(target_os = "linux")]
    async fn blackhole() -> (tokio::net::TcpListener, Vec<TcpStream>, SocketAddr) {
        let socket = tokio::net::TcpSocket::new_v4().expect("a socket");
        socket
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("bound");
        let listener = socket.listen(1).expect("listening");
        let addr = listener.local_addr().expect("an address");
        let mut held = Vec::new();
        // Connections nobody accepts: they fill the queue, and the first
        // connect that finds it full is the one that hangs.
        while let Ok(stream) =
            tokio::time::timeout(Duration::from_millis(150), TcpStream::connect(addr)).await
        {
            held.push(stream.expect("the queue still had room"));
            assert!(held.len() < 32, "the accept queue never filled");
        }
        (listener, held, addr)
    }

    /// Several addresses that swallow packets share the one budget, instead
    /// of each being given the whole of it: three of them under a 600 ms
    /// budget end in about 600 ms, not in 1.8 s.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn addresses_that_swallow_packets_share_one_budget() {
        let exec = Exec::current().expect("a runtime");
        let (_l1, _h1, first) = blackhole().await;
        let (_l2, _h2, second) = blackhole().await;
        let (_l3, _h3, third) = blackhole().await;

        let budget = Budget::start(Duration::from_millis(600));
        let started = Instant::now();
        let error = dial(&exec, &[first, second, third], &budget)
            .await
            .expect_err("nothing answers");
        let took = started.elapsed();

        assert!(matches!(error, Error::HandshakeTimeout { .. }), "{error}");
        assert!(
            took < Duration::from_millis(1200),
            "three addresses took {took:?} out of a 600 ms budget"
        );
    }

    /// An address that swallows packets leaves the next one time to answer:
    /// it is given half of what is left and not all of it.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_black_hole_leaves_the_next_address_time_to_answer() {
        let exec = Exec::current().expect("a runtime");
        let (_hole, _held, dead) = blackhole().await;
        let live = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bound");
        let live_addr = live.local_addr().expect("an address");

        let budget = Budget::start(Duration::from_secs(2));
        let stream = dial(&exec, &[dead, live_addr], &budget)
            .await
            .expect("the second address answers");
        assert!(matches!(stream, Stream::Plain(_)));
    }

    /// A resolver that never answers does not hold the caller past the
    /// budget, and the failure says what was being waited for.
    #[tokio::test]
    async fn a_lookup_that_never_answers_ends_inside_the_budget() {
        let exec = Exec::current().expect("a runtime");
        let silent =
            std::future::pending::<std::result::Result<Vec<SocketAddr>, weida_core::Error>>();
        let budget = Budget::start(Duration::from_millis(200));
        let started = Instant::now();
        let error = connect_to(&exec, &budget, silent)
            .await
            .expect_err("it never answers");
        assert!(
            matches!(
                error,
                Error::HandshakeTimeout {
                    step: "the name lookup"
                }
            ),
            "{error}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
