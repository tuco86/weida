//! The byte stream under the control lines, and how TLS gets under it.
//!
//! The NATS client protocol is TCP and text: "the client protocol's default
//! transport is TCP/IP; it also supports TLS over the connection"
//! (`docs/research/nats.md` §12 P18). There is **one** way to reach TLS and
//! it is not AMQP's two:
//!
//! ```text
//! TCP connect
//!       <---------------------- INFO {"tls_required":true,...}   (in the clear)
//! ...TLS handshake...
//! CONNECT {...} ------------->                                   (inside TLS)
//! PING          ------------->
//!       <---------------------- PONG
//! ```
//!
//! The `INFO` that *demands* TLS arrives before TLS, unencrypted, and is not
//! resent afterwards — it is the only protocol traffic that ever travels in
//! the clear on such a connection. Everything the client says is inside the
//! session, `CONNECT` and its credentials first of all. There is no port
//! split: TLS and plain share 4222, because `INFO` is what distinguishes
//! them.
//!
//! # The trust anchors are not here
//!
//! `upgrade_tls` takes the caller's `rustls::ClientConfig` — not a link,
//! because that function exists only under the `tls` feature and this
//! module's documentation is built without it too. Which
//! certificates are valid is the application's decision — a service on a
//! private network trusts a private CA, a hosted one trusts the public
//! roots — and a messaging library that shipped a default would be making
//! that choice for both. What this module does supply is the SNI name, taken
//! from the host that was dialled, because that is a protocol behaviour
//! rather than a convenience.
//!
//! # Reading is cancel-safe
//!
//! [`OpReader::next_op`] is used directly inside the driver's `select!`, with
//! no forwarding task in between, and that is only sound because every octet
//! it has read lives in `self` rather than in the future: a `next_op` dropped
//! half-way through a `MSG` leaves the partial `MSG` in the buffer and the
//! next call continues it. The alternative — a task that reads ahead and
//! forwards — would have to guess how far ahead to read, and the bound on
//! that guess would be one more bound nobody asked for.
//!
//! # A message is cut out of the buffer, not copied out of it
//!
//! The reader fills one [`BytesMut`]. A decoded `MSG` or `HMSG` is split off
//! its front as a reference-counted [`Bytes`] frame and the message's subject,
//! reply subject and payload are slices of that frame: no allocation per
//! message and no copy of the payload. The buffer's own allocation is renewed
//! once per 16 KiB chunk of traffic, or once per message that is larger. The
//! price is that a slice keeps its chunk alive; `crate::queue` charges for it
//! while a message waits, and [`crate::message`] says what it means for an
//! application that holds one.
//!
//! A message declared larger than what has been read is never grown towards by
//! doubling: the codec checks the declared count against the decode bound
//! before it reports how much is missing, so the reader asks for what the
//! message needs and never more than the decode bound allows. (The buffer
//! grows by at least doubling, so the allocation can be up to twice the
//! message.)
//!
//! # Writing is queued, and never awaited by whoever queues
//!
//! [`OpWriter`] holds what is waiting for the socket in one buffer. Queueing
//! never blocks; the driver offers the socket the buffer from its own
//! `select!`, so that **a peer that stops reading cannot stop the driver
//! reading, answering or timing out**. Everything queued between two writes
//! goes out in one write and one flush, which is also what keeps a TLS session
//! from sealing a record per operation.

use std::fmt;
use std::net::SocketAddr;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use weida_nats_codec::error::DecodeError;
use weida_nats_codec::{Limits, Op};
use weida_runtime::Exec;

use crate::error::{Error, Result};
use crate::message::{Message, OwnedHeaders};
use crate::options::ConnectionOptions;

/// `PING`, as it is written.
pub(crate) const PING: &[u8] = b"PING\r\n";

/// `PONG`, as it is written.
pub(crate) const PONG: &[u8] = b"PONG\r\n";

/// The room a read is offered, and the size one buffer allocation is renewed
/// in.
///
/// Every message queued for an application reader keeps the chunk it was read
/// into alive, so this is also the most that one small message can pin;
/// `crate::queue::PER_MESSAGE_OVERHEAD` is a quarter of it for that reason.
pub(crate) const READ_CHUNK: usize = 16 * 1024;

/// Spare room below which a read is not offered what is left: a read into a
/// few hundred octets is a system call per few hundred octets.
const MIN_READ: usize = 2 * 1024;

/// The most an idle buffer is allowed to keep. After a large message the
/// buffer is empty and still pointing at an allocation as big as the message;
/// it is let go rather than held for the life of the connection.
const RETAIN_MAX: usize = 4 * READ_CHUNK;

/// The most one `reserve` asks for. The declared count is already under the
/// caller's `max_inbound_payload`; this keeps a caller who configured that
/// absurdly high from turning one control line into an allocation failure.
const RESERVE_MAX: usize = 1 << 30;

/// One operation from the server.
///
/// Owned because it has to outlive the read it was decoded from: a `MSG`
/// travels down a channel to whatever task holds the subscription, and a
/// borrow would tie that task's lifetime to the driver's buffer. Owned does not
/// mean copied: the octets of a `MSG` are slices of the read buffer.
///
/// The server-to-client verbs that carry no state — `PING`, `PONG`, `+OK` —
/// cost nothing to own. `INFO` keeps its JSON octets unparsed, because the
/// fields a client acts on are
/// [`ServerInfo`](weida_nats_codec::ServerInfo)'s to read and the connection
/// is the layer that acts on them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Incoming {
    /// `INFO {json}`: the object octets, `{` through `}`.
    Info(Bytes),
    /// `MSG` or `HMSG`, dispatched by `sid`.
    Msg(Message),
    /// `PING`, which must be answered with `PONG`.
    Ping,
    /// `PONG`, which answers one of ours.
    Pong,
    /// `+OK`, sent per operation while `verbose` is on.
    Ok,
    /// `-ERR '<reason>'`. The reason is lossily decoded, because an error
    /// message a client could not report because it was not UTF-8 would be
    /// the one explanation it loses.
    Err(String),
}

/// The byte stream a connection runs on.
///
/// An enum rather than a boxed trait object: there are exactly two cases, the
/// dispatch is one branch per read, and a `Box<dyn AsyncRead + AsyncWrite>`
/// would add an allocation and a vtable call for no gain.
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
    /// Not cosmetic: `CONNECT` carries a password or a JWT, and a caller
    /// deciding whether to offer one wants to be able to ask.
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
/// and `127.0.0.1`, and a server bound to one of them is unreachable through
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
    /// that the instant cannot be represented, in which case no step is
    /// given less than the whole of it.
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
                // Nagle off: the handshake is a run of short control lines
                // that each need an answer, and a request-reply round trip is
                // two more. Coalescing them adds a delay to every one.
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
/// in SNI. The caller's `config` decides what "valid" means. Unbounded in
/// time by itself — a server that goes quiet after the `ClientHello` holds
/// it — so the caller wraps it in the handshake's deadline.
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

/// A NATS byte stream with one read buffer, for the handshake.
///
/// The buffer exists because a control line arrives in however many TCP
/// segments the network chose and the codec needs it contiguous. Its growth
/// is bounded by the bounds the codec already applies: a line with no `CRLF`
/// inside its bound is refused, and a declared payload above the decode bound
/// is refused from the control line alone — so nothing is ever reserved on the
/// server's word.
pub struct Wire {
    stream: Stream,
    buf: BytesMut,
}

impl fmt::Debug for Wire {
    /// How much is buffered, not what: the octets are the server's traffic.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wire")
            .field("stream", &self.stream)
            .field("buffered", &self.buf.len())
            .finish()
    }
}

impl Wire {
    /// Wraps a stream.
    #[must_use]
    pub fn new(stream: Stream) -> Self {
        Self {
            stream,
            buf: BytesMut::new(),
        }
    }

    /// The stream underneath, for the peer address and the encryption flag.
    #[must_use]
    pub const fn stream(&self) -> &Stream {
        &self.stream
    }

    /// Writes `bytes` and flushes them.
    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Reads one operation.
    pub async fn read_op(&mut self, limits: Limits) -> Result<Incoming> {
        next_op(&mut self.stream, &mut self.buf, limits).await
    }

    /// Consumes the wire, handing back the stream for the TLS upgrade.
    ///
    /// Buffered octets are an error rather than something to discard. On a
    /// `tls_required` connection the server sends `INFO` and then waits for a
    /// TLS `ClientHello`; anything it sent after that `INFO` it sent in the
    /// clear, and carrying it into the session or dropping it would both mean
    /// treating unencrypted protocol as though it had been protected.
    pub fn into_stream(self) -> Result<Stream> {
        if !self.buf.is_empty() {
            return Err(Error::Protocol(format!(
                "{} octet(s) arrived after INFO and before the TLS handshake, \
                 in the clear",
                self.buf.len()
            )));
        }
        Ok(self.stream)
    }

    /// Splits the wire for the connection driver.
    ///
    /// The handshake is a strict sequence on one object; everything after it
    /// is not — a `PONG` has to go out while a `MSG` is half-read — so the
    /// driver needs the two directions separately. Octets already buffered
    /// travel with the reader.
    #[must_use]
    pub fn split(self) -> (OpReader, OpWriter) {
        let (read, write) = tokio::io::split(self.stream);
        (
            OpReader {
                read,
                buf: self.buf,
            },
            OpWriter::over(write),
        )
    }
}

/// Reads whole operations from one half of a split wire.
pub struct OpReader {
    read: tokio::io::ReadHalf<Stream>,
    buf: BytesMut,
}

impl fmt::Debug for OpReader {
    /// How much is buffered, not what: the octets are the server's traffic.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpReader")
            .field("buffered", &self.buf.len())
            .finish_non_exhaustive()
    }
}

impl OpReader {
    /// The next whole operation.
    ///
    /// **Cancel-safe**: every octet read lives in `self`, so a call dropped
    /// by a `select!` loses nothing and the next call continues where it
    /// stopped. `limits` is an argument rather than a field because
    /// `max_payload` changes when an asynchronous `INFO` arrives, and the
    /// value in force is the driver's to know.
    pub async fn next_op(&mut self, limits: Limits) -> Result<Incoming> {
        next_op(&mut self.read, &mut self.buf, limits).await
    }
}

/// Writes to one half of a split wire, from a buffer that whoever has
/// something to say can add to without waiting.
///
/// [`queue`](Self::queue) never blocks. [`step`](Self::step) is what offers
/// the socket the buffer, one write at a time, so that the caller can stand it
/// in a `select!` beside everything else it has to do and keep a clock on
/// whether it makes progress.
pub struct OpWriter<W = tokio::io::WriteHalf<Stream>> {
    write: W,
    /// Everything queued and not yet accepted by the socket.
    out: BytesMut,
    /// The octets the socket has accepted, so that "has this `PONG` gone yet"
    /// is a comparison of two numbers rather than a search of the buffer.
    written: u64,
    /// Where the `PONG` queued last ends, in the same count.
    pong_end: u64,
    /// The socket has accepted octets since the last flush. A TLS session
    /// holds sealed records until it is flushed; a plain socket has nothing to
    /// flush and says so at once.
    flush_due: bool,
}

impl<W> fmt::Debug for OpWriter<W> {
    /// How much is waiting, not what: the octets are the application's
    /// publications.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpWriter")
            .field("queued", &self.out.len())
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

impl<W: AsyncWrite + Unpin> OpWriter<W> {
    /// A writer over `write`, with nothing queued.
    #[must_use]
    pub fn over(write: W) -> Self {
        Self {
            write,
            out: BytesMut::new(),
            written: 0,
            pong_end: 0,
            flush_due: false,
        }
    }

    /// Queues `bytes` behind whatever is waiting. Never blocks and never
    /// writes: the octets leave in [`step`](Self::step).
    pub fn queue(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }

    /// Queues a `PONG` unless the one queued last has not gone yet.
    ///
    /// A `PONG` answers every `PING` the server has sent before it: the
    /// server counts unanswered pings and a `PONG` resets the count. What this
    /// buys is that a peer that sends `PING`s and does not read the answers
    /// costs one queued `PONG` while the socket is stuck, and not one per
    /// `PING` at line rate.
    pub fn queue_pong(&mut self) {
        if self.pong_end > self.written {
            return;
        }
        self.out.extend_from_slice(PONG);
        self.pong_end = self.written + self.out.len() as u64;
    }

    /// How many octets are waiting for the socket.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.out.len()
    }

    /// Whether [`step`](Self::step) has anything left to do: octets waiting,
    /// or a flush owed for the ones already written.
    #[must_use]
    pub fn has_output(&self) -> bool {
        !self.out.is_empty() || self.flush_due
    }

    /// One step towards an empty buffer: a write if octets are waiting, and
    /// once none are, the flush the writes before it owe.
    ///
    /// Returns after **one** write, not after the buffer is empty, so that a
    /// caller can tell progress from a stall by whether this keeps returning.
    /// **Cancel-safe**: the state is in `self`, a write that is dropped wrote
    /// nothing, and a flush that is dropped is owed again.
    pub async fn step(&mut self) -> Result<()> {
        if !self.out.is_empty() {
            let wrote = self.write.write_buf(&mut self.out).await?;
            if wrote == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "the socket accepted no octets",
                )));
            }
            self.written += wrote as u64;
            self.flush_due = true;
        }
        if self.out.is_empty() {
            if self.flush_due {
                self.write.flush().await?;
                self.flush_due = false;
            }
            if self.out.capacity() > RETAIN_MAX {
                self.out = BytesMut::new();
            }
        }
        Ok(())
    }

    /// Closes the write half.
    ///
    /// This is the whole of a NATS close: "a normal client protocol close has
    /// no dedicated `CLOSE` verb; ending the transport ends the connection
    /// and its subscriptions" (`docs/research/nats.md` §1).
    pub async fn shutdown(&mut self) -> Result<()> {
        self.write.shutdown().await?;
        Ok(())
    }
}

/// One operation out of `buf`, filling from `reader` until there is one.
async fn next_op<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
    limits: Limits,
) -> Result<Incoming> {
    // How much has to be buffered before decoding is worth another try. The
    // codec says how much of a declared body is missing, so a large message is
    // not decoded again after every read of it.
    let mut ready_at = 0;
    loop {
        if !buf.is_empty() && buf.len() >= ready_at {
            match take(buf, limits)? {
                Taken::Op(incoming) => {
                    // After a large message the buffer is empty and still
                    // points at an allocation as large as it: let that go.
                    if buf.is_empty() && buf.capacity() > RETAIN_MAX {
                        *buf = BytesMut::new();
                    }
                    return Ok(incoming);
                }
                Taken::More(missing) => {
                    ready_at = buf.len().saturating_add(missing);
                    // `missing` is what is left of a count the codec has
                    // already checked against the decode bound, so this
                    // reserves what the message needs and nothing a peer
                    // merely announced beyond it.
                    buf.reserve(missing.min(RESERVE_MAX));
                }
            }
        }
        if buf.capacity() - buf.len() < MIN_READ {
            buf.reserve(READ_CHUNK);
        }
        let read = reader.read_buf(buf).await?;
        if read == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the server closed the transport",
            )));
        }
    }
}

/// What one attempt to take an operation off the front of the buffer found.
enum Taken {
    /// A whole operation, which has left the buffer.
    Op(Incoming),
    /// Part of one: this many more octets are needed.
    More(usize),
}

/// What a decoded operation is, with its octets as offsets into the buffer it
/// was decoded from rather than borrows of it, so that the buffer can be
/// split once the decode is over.
enum Parsed {
    Info(Range<usize>),
    Msg {
        subject: Range<usize>,
        sid: u64,
        reply_to: Option<Range<usize>>,
        headers: Option<OwnedHeaders>,
        payload: Range<usize>,
    },
    Ping,
    Pong,
    Ok,
    Err(String),
}

/// Takes one operation off the front of `buf`, if a whole one is there.
fn take(buf: &mut BytesMut, limits: Limits) -> Result<Taken> {
    // Only the address: the offsets below are measured from it, and holding
    // it does not hold a borrow of `buf`.
    let base = buf.as_ptr() as usize;
    let (parsed, used) = match Op::decode(&buf[..], limits) {
        Ok((op, used)) => (parse(&op, base)?, used),
        Err(DecodeError::Incomplete { needed }) => return Ok(Taken::More(needed)),
        Err(error) => return Err(Error::Decode(error)),
    };
    Ok(Taken::Op(match parsed {
        Parsed::Ping => {
            buf.advance(used);
            Incoming::Ping
        }
        Parsed::Pong => {
            buf.advance(used);
            Incoming::Pong
        }
        Parsed::Ok => {
            buf.advance(used);
            Incoming::Ok
        }
        Parsed::Err(reason) => {
            buf.advance(used);
            Incoming::Err(reason)
        }
        Parsed::Info(json) => {
            let frame = buf.split_to(used).freeze();
            Incoming::Info(cut(&frame, json))
        }
        Parsed::Msg {
            subject,
            sid,
            reply_to,
            headers,
            payload,
        } => {
            let frame = buf.split_to(used).freeze();
            Incoming::Msg(Message {
                subject: cut(&frame, subject),
                sid,
                reply_to: reply_to.map(|range| cut(&frame, range)),
                headers,
                payload: cut(&frame, payload),
            })
        }
    }))
}

/// Reads a decoded operation into the form that outlives the decode.
fn parse(op: &Op<'_>, base: usize) -> Result<Parsed> {
    Ok(match op {
        Op::Info { json } => Parsed::Info(offsets(base, json)),
        Op::Msg {
            subject,
            sid,
            reply_to,
            payload,
        } => Parsed::Msg {
            subject: offsets(base, subject),
            sid: parse_sid(sid)?,
            reply_to: reply_to.map(|reply_to| offsets(base, reply_to)),
            headers: None,
            payload: offsets(base, payload),
        },
        Op::Hmsg {
            subject,
            sid,
            reply_to,
            headers,
            payload,
        } => Parsed::Msg {
            subject: offsets(base, subject),
            sid: parse_sid(sid)?,
            reply_to: reply_to.map(|reply_to| offsets(base, reply_to)),
            headers: Some(OwnedHeaders::from_borrowed(headers)),
            payload: offsets(base, payload),
        },
        Op::Ping => Parsed::Ping,
        Op::Pong => Parsed::Pong,
        Op::Ok => Parsed::Ok,
        Op::Err { reason } => Parsed::Err(String::from_utf8_lossy(reason).into_owned()),
        // A client is never sent these. `CONNECT`, `PUB`, `HPUB`, `SUB` and
        // `UNSUB` are client-to-server verbs, so a server writing one is a
        // server this client cannot reason about.
        other => {
            return Err(Error::Protocol(format!(
                "the server sent {}, which is a client-to-server operation",
                other.verb()
            )));
        }
    })
}

/// Where `part`, a slice of the buffer that starts at address `base`, lies in
/// it.
fn offsets(base: usize, part: &[u8]) -> Range<usize> {
    let start = (part.as_ptr() as usize).saturating_sub(base);
    start..start.saturating_add(part.len())
}

/// The part of `frame` at `range`, as a slice of it and not a copy.
///
/// `range` comes from [`offsets`] over slices the decoder cut out of the same
/// octets, so it is always inside; an empty answer rather than a panic is what
/// a range that was not would get, because the octets are a peer's.
fn cut(frame: &Bytes, range: Range<usize>) -> Bytes {
    match frame.get(range) {
        Some(part) => frame.slice_ref(part),
        None => {
            debug_assert!(false, "an offset outside the frame it was measured in");
            Bytes::new()
        }
    }
}

/// The `sid` a `MSG` came back on.
///
/// "A unique alphanumeric subscription ID, generated by the client" — so
/// every `sid` on this connection is one this client wrote as decimal, and
/// one that is not is a `sid` the server invented.
fn parse_sid(sid: &[u8]) -> Result<u64> {
    std::str::from_utf8(sid)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or_else(|| {
            Error::Protocol(format!(
                "the server delivered a message with sid {:?}, and every sid on \
                 this connection is one this client generated",
                String::from_utf8_lossy(sid)
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reader that hands out at most `step` octets per read, the way a
    /// network hands out a message in segments.
    struct Segments {
        data: Vec<u8>,
        at: usize,
        step: usize,
    }

    impl Segments {
        fn new(data: &[u8], step: usize) -> Self {
            Self {
                data: data.to_vec(),
                at: 0,
                step,
            }
        }
    }

    impl AsyncRead for Segments {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = &mut *self;
            let rest = &this.data[this.at..];
            let take = rest.len().min(this.step).min(buf.remaining());
            buf.put_slice(&rest[..take]);
            this.at += take;
            Poll::Ready(Ok(()))
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime")
    }

    /// Reads every operation out of one buffer of octets, so the framing is
    /// tested without a socket.
    fn read_all(wire: &[u8]) -> Vec<Incoming> {
        let mut buf = BytesMut::new();
        let mut out = Vec::new();
        // A finite reader: once the octets are gone, `next_op` sees EOF.
        let mut reader = std::io::Cursor::new(wire.to_vec());
        let runtime = runtime();
        loop {
            let next = runtime.block_on(next_op(&mut reader, &mut buf, Limits::DEFAULT));
            match next {
                Ok(op) => out.push(op),
                Err(_) => return out,
            }
        }
    }

    /// Several operations inside one buffer are read as several operations,
    /// including a payload that itself holds a `CRLF` — the case a reader
    /// that searched for a terminator instead of trusting the declared count
    /// would split in the wrong place.
    #[test]
    fn a_pipelined_buffer_is_read_operation_by_operation() {
        let wire = b"INFO {\"max_payload\":1024}\r\n\
                     PING\r\n\
                     MSG a.b 7 5\r\nhi\r\nx\r\n\
                     PONG\r\n\
                     -ERR 'Stale Connection'\r\n";
        let ops = read_all(wire);
        assert_eq!(ops.len(), 5, "{ops:?}");
        assert_eq!(
            ops[0],
            Incoming::Info(Bytes::from_static(b"{\"max_payload\":1024}"))
        );
        assert_eq!(ops[1], Incoming::Ping);
        let Incoming::Msg(message) = &ops[2] else {
            panic!("expected a MSG, got {:?}", ops[2]);
        };
        assert_eq!(message.sid, 7);
        assert_eq!(message.subject, &b"a.b"[..]);
        assert_eq!(
            message.payload,
            &b"hi\r\nx"[..],
            "the declared count delimits the payload, not a search for CRLF"
        );
        assert_eq!(ops[3], Incoming::Pong);
        assert_eq!(ops[4], Incoming::Err("Stale Connection".to_owned()));
    }

    /// `HMSG` arrives with its block copied out, status and all.
    #[test]
    fn a_headered_message_keeps_its_block() {
        let wire = b"HMSG a.b 3 _INBOX.9 16 16\r\nNATS/1.0 503\r\n\r\n\r\n";
        let ops = read_all(wire);
        let Some(Incoming::Msg(message)) = ops.first() else {
            panic!("expected an HMSG, got {ops:?}");
        };
        assert_eq!(message.sid, 3);
        assert_eq!(message.reply_to.as_deref(), Some(&b"_INBOX.9"[..]));
        assert!(message.payload.is_empty());
        assert!(message.is_no_responders());
    }

    /// The subject, reply subject and payload of one message are slices of the
    /// same frame, and each is exactly its own octets: a slice that was off by
    /// one would hand a payload the `CRLF` that follows it.
    #[test]
    fn the_parts_of_a_message_are_exactly_their_own_octets() {
        let wire = b"MSG orders.created 12 _INBOX.r 6\r\nabc\r\nd\r\nMSG empty 1 0\r\n\r\n";
        let ops = read_all(wire);
        let [Incoming::Msg(first), Incoming::Msg(second)] = &ops[..] else {
            panic!("two messages expected, got {ops:?}");
        };
        assert_eq!(first.subject, &b"orders.created"[..]);
        assert_eq!(first.reply_to.as_deref(), Some(&b"_INBOX.r"[..]));
        assert_eq!(first.payload, &b"abc\r\nd"[..]);
        assert_eq!(second.subject, &b"empty"[..]);
        assert!(second.reply_to.is_none());
        assert!(second.payload.is_empty());
    }

    /// A message far larger than one read arrives whole when the network hands
    /// it over in small segments, and the message after it is read from what
    /// the same buffer holds next.
    #[test]
    fn a_large_message_arrives_whole_across_small_segments() {
        let body: Vec<u8> = (0..300_000u32).map(|n| (n % 251) as u8).collect();
        let mut wire = format!("MSG big 1 {}\r\n", body.len()).into_bytes();
        wire.extend_from_slice(&body);
        wire.extend_from_slice(b"\r\nPING\r\n");

        let limits = Limits {
            max_payload: 1 << 20,
            ..Limits::DEFAULT
        };
        let mut reader = Segments::new(&wire, 1000);
        let mut buf = BytesMut::new();
        let runtime = runtime();
        let first = runtime
            .block_on(next_op(&mut reader, &mut buf, limits))
            .expect("the message");
        let Incoming::Msg(message) = first else {
            panic!("a MSG expected, got {first:?}");
        };
        assert_eq!(message.payload.len(), body.len());
        assert_eq!(&message.payload[..], &body[..]);
        assert_eq!(
            runtime
                .block_on(next_op(&mut reader, &mut buf, limits))
                .expect("what followed"),
            Incoming::Ping
        );
    }

    /// A `sid` this client could not have written is the server inventing
    /// one, and inventing a number to match it would route a message to a
    /// subscription that never asked for it.
    #[test]
    fn a_sid_the_client_did_not_generate_is_a_protocol_error() {
        let mut buf = BytesMut::new();
        let mut reader = std::io::Cursor::new(b"MSG a.b abc 0\r\n\r\n".to_vec());
        let error = runtime()
            .block_on(next_op(&mut reader, &mut buf, Limits::DEFAULT))
            .expect_err("refused");
        assert!(matches!(error, Error::Protocol(_)), "{error}");
    }

    /// A server-to-client reader that accepted `PUB` would be a reader with
    /// no idea which side of the connection it is on.
    #[test]
    fn a_client_to_server_verb_from_the_server_is_refused() {
        let mut buf = BytesMut::new();
        let mut reader = std::io::Cursor::new(b"PUB a.b 0\r\n\r\n".to_vec());
        let error = runtime()
            .block_on(next_op(&mut reader, &mut buf, Limits::DEFAULT))
            .expect_err("refused");
        assert!(
            matches!(&error, Error::Protocol(why) if why.contains("PUB")),
            "{error}"
        );
    }

    /// A declared count above the decode bound is refused from the control
    /// line, before a single octet of the body is read or reserved.
    #[test]
    fn a_declared_count_above_the_bound_is_refused_before_the_body() {
        let limits = Limits {
            max_payload: 1024,
            ..Limits::DEFAULT
        };
        let mut buf = BytesMut::new();
        // The body is never sent: the line alone is enough to be refused.
        let mut reader = std::io::Cursor::new(b"MSG a 1 3221225472\r\n".to_vec());
        let error = runtime()
            .block_on(next_op(&mut reader, &mut buf, limits))
            .expect_err("refused");
        assert!(
            matches!(
                error,
                Error::Decode(DecodeError::PayloadTooLarge {
                    declared: 3_221_225_472,
                    cap: 1024
                })
            ),
            "{error}"
        );
        assert!(
            buf.capacity() <= READ_CHUNK,
            "nothing was reserved on the strength of the declared count"
        );
    }

    /// A buffer that has held a large message does not keep its allocation
    /// for the life of the connection.
    #[test]
    fn an_idle_buffer_lets_a_large_allocation_go() {
        let body = vec![b'x'; 200_000];
        let mut wire = format!("MSG big 1 {}\r\n", body.len()).into_bytes();
        wire.extend_from_slice(&body);
        wire.extend_from_slice(b"\r\n");
        let mut buf = BytesMut::new();
        let mut reader = std::io::Cursor::new(wire);
        let limits = Limits {
            max_payload: 1 << 20,
            ..Limits::DEFAULT
        };
        let message = runtime()
            .block_on(next_op(&mut reader, &mut buf, limits))
            .expect("the message");
        assert!(matches!(message, Incoming::Msg(_)));
        assert!(buf.is_empty());
        assert!(
            buf.capacity() <= RETAIN_MAX,
            "the idle buffer kept {} octets",
            buf.capacity()
        );
    }

    /// The writer queues without waiting and writes what it queued, in order,
    /// in as few writes as the socket allows.
    #[tokio::test]
    async fn queued_octets_are_written_in_order() {
        let (near, mut far) = tokio::io::duplex(1024);
        let mut writer = OpWriter::over(near);
        writer.queue(b"SUB a 1\r\n");
        writer.queue(b"PUB a 1\r\nx\r\n");
        assert_eq!(writer.queued(), 9 + 12);
        while writer.has_output() {
            writer.step().await.expect("written");
        }
        assert_eq!(writer.queued(), 0);

        let mut got = vec![0u8; 21];
        far.read_exact(&mut got).await.expect("read");
        assert_eq!(got, b"SUB a 1\r\nPUB a 1\r\nx\r\n");
    }

    /// A `PONG` that is already waiting answers every `PING` that arrives
    /// before it is written; one that has gone is answered afresh.
    #[tokio::test]
    async fn pongs_are_coalesced_only_while_one_is_waiting() {
        let (near, mut far) = tokio::io::duplex(1024);
        let mut writer = OpWriter::over(near);
        for _ in 0..1000 {
            writer.queue_pong();
        }
        assert_eq!(
            writer.queued(),
            PONG.len(),
            "a thousand PINGs while the socket is stuck are one PONG"
        );
        while writer.has_output() {
            writer.step().await.expect("written");
        }
        writer.queue_pong();
        assert_eq!(writer.queued(), PONG.len(), "the first one has gone");
        while writer.has_output() {
            writer.step().await.expect("written");
        }
        let mut got = vec![0u8; 2 * PONG.len()];
        far.read_exact(&mut got).await.expect("read");
        assert_eq!(got, b"PONG\r\nPONG\r\n");
    }

    /// A socket that takes nothing leaves the step pending rather than
    /// failing or spinning, which is what lets the driver put a clock on it.
    #[tokio::test]
    async fn a_stuck_socket_leaves_the_step_pending() {
        let (near, _far) = tokio::io::duplex(8);
        let mut writer = OpWriter::over(near);
        writer.queue(&[b'x'; 4096]);
        // The first step takes what the pipe holds; the second can take
        // nothing, because nobody reads the far end.
        writer.step().await.expect("the pipe had room for a little");
        let stuck = tokio::time::timeout(std::time::Duration::from_millis(100), writer.step());
        assert!(stuck.await.is_err(), "no progress, and no error either");
        assert!(writer.has_output());
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
