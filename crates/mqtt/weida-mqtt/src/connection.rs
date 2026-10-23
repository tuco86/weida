//! The wire: one packet at a time in each direction, and the handshake.
//!
//! MQTT's transport requirement is a single "ordered, lossless, stream of
//! bytes" in both directions ([MQTT-4.2-1]) [mqtt5 §12/P18], so every packet
//! of a session interleaves on one connection. The read and write halves are
//! **split** rather than shared, and the reason is not style: the event loop
//! must answer a PUBLISH while it is still reading the next one, and must keep
//! reading while a write is waiting for the peer, so neither half may hold the
//! other up.
//!
//! **A packet is decoded where it was read.** The reader reads into a buffer
//! it owns and lends the codec the packet as it lies in it: the topic, the
//! properties and the payload are slices of that buffer, and a PUBACK costs no
//! allocation at all. The payload is copied **once**, into the
//! [`crate::Delivery`] the application keeps — the copy that cannot be avoided
//! while the application owns what it is handed and the buffer is reused. A
//! delivery that shared the receive buffer instead would pin it for as long as
//! the application held the message, and a peer that interleaves large
//! packets with small ones could make every queued small message hold a
//! multi-megabyte buffer alive; so what the queue is charged for is what it
//! holds.
//!
//! **Nothing is read into a buffer sized from a declared length.**
//! The reader grows its buffer by what the socket actually delivered and
//! asks the codec whether a whole packet is present yet; the codec applies
//! `Maximum Packet Size` from the fixed header alone, so a peer that declares
//! 268 MB over a four-byte read is refused before anything is reserved
//! (3.1.2.11.4) [mqtt5 §5].
//!
//! **A write is a queue, and the event loop drains it alongside everything
//! else.** `Writer::queue` encodes into a buffer and returns at once;
//! `Writer::step` moves some of it to the socket and is raced against reads,
//! commands and timers by the loop that owns the connection. A peer that stops
//! reading therefore stops one `step` and nothing else, and the loop's
//! deadline on it — a socket that took no byte for the write timeout — ends
//! the connection with a named error instead of holding it for good.
//!
//! **A partial packet at end of stream is a closed connection, not a
//! malformed one.** The peer is always free to drop the socket — that is what
//! 3.1.1 did for every error [mqtt5 §1] — and reporting it as a protocol fault
//! would put the blame in the wrong place.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use weida_mqtt_codec::{
    Auth, AuthReasonCode, Connack, Connect, ConnectReasonCode, DecodeError, Disconnect,
    DisconnectReasonCode, EncodeError, FixedHeader, Packet, PacketType, Properties, Will,
};
use weida_runtime::Exec;

use crate::error::{Error, Result};
use crate::limits::{Limits, ServerLimits};
use crate::options::{ConnectOptions, interval_seconds};
use crate::transport::Stream;

/// Bytes to ask the socket for at a time. Not a bound on anything: the bound
/// is `Maximum Packet Size`, applied by the codec from the fixed header.
const READ_CHUNK: usize = 8 * 1024;

/// A receive or encode buffer larger than this is replaced rather than kept
/// once it is idle, so that one large packet does not pin its allocation — and
/// the doubling `Vec` growth that came with it — for the life of the
/// connection.
const KEEP_BUFFER: usize = 4 * READ_CHUNK;

/// The most AUTH packets one enhanced-authentication exchange may take.
///
/// The protocol has no round count: a mechanism may challenge as often as it
/// likes. A real one takes two or three, and each round costs this client an
/// [`Authenticator::challenge`] — for SCRAM a key derivation, run on the event
/// loop — and a write, so a server that never ends the exchange is a way to
/// make a client do work for as long as it likes.
pub(crate) const MAX_AUTH_ROUNDS: u32 = 8;

/// What a failed or overlong AUTH exchange reports.
pub(crate) const AUTH_EXCHANGE: &str = "the AUTH exchange to finish";

/// Answers the server's `AUTH` challenges during enhanced authentication
/// (4.12) [mqtt5 §10], and says whether the server proved itself.
///
/// A client that sets `Authentication Method` "MUST send nothing but AUTH or
/// DISCONNECT until CONNACK" ([MQTT-3.1.2-30]), and the exchange runs with
/// reason code 0x18 until the server sends CONNACK 0x00 [mqtt5 §10]. Every
/// AUTH and any successful CONNACK MUST repeat the same method
/// ([MQTT-4.12.0-5]), which this module checks rather than trusting.
///
/// **The exchange ends in the server's own data, and that is where the server
/// is authenticated.** In SCRAM and in every other mutual mechanism the proof
/// that the server knows the secret travels in the *final* message, which MQTT
/// carries as the `Authentication Data` of the CONNACK — or, on a
/// re-authentication, of the AUTH 0x00. A client that does not look at it has
/// authenticated *itself* and learned nothing about who answered.
/// [`Authenticator::finish`] is where it looks, and it has no default: an
/// implementation that forgot it would accept an impostor without a signal.
pub trait Authenticator: Send + Sync {
    /// Answers a `Continue authentication` challenge.
    ///
    /// `data` is the server's `Authentication Data`, absent where it sent
    /// none. The returned bytes become the client's `Authentication Data`.
    ///
    /// # Errors
    ///
    /// Any error ends the exchange; the client sends DISCONNECT and closes,
    /// which is what both sides SHOULD do on failure ([MQTT-4.12.1-2]).
    fn challenge(&self, data: Option<&[u8]>) -> Result<Vec<u8>>;

    /// Verifies what the server sent to **end** the exchange.
    ///
    /// Called once per exchange: with the CONNACK's `Authentication Data` when
    /// the handshake ends, and with the AUTH 0x00's when a re-authentication
    /// does. `data` is `None` where the server sent none.
    ///
    /// **It is called when no challenge was ever sent, too.** A server that
    /// answers a CONNECT that named a method with an immediate CONNACK 0x00
    /// has skipped the exchange, and for a mechanism with a server proof that
    /// is not authentication: `finish(None)` is where that mechanism refuses.
    /// A mechanism with no server proof — a bearer token the server checks and
    /// does not answer — returns `Ok(())` here, and says by writing it that
    /// the server is authenticated by something else, TLS.
    ///
    /// # Errors
    ///
    /// Any error means the server is not who it says it is: the connect, or
    /// the re-authentication, fails with it, and the client sends DISCONNECT
    /// and closes ([MQTT-4.12.1-2]). [`Error::AuthenticationFailed`] is the
    /// variant for that.
    fn finish(&self, data: Option<&[u8]>) -> Result<()>;
}

/// The authenticator a client that gave none gets.
///
/// "Absent a client-named method the server MUST NOT send AUTH"
/// ([MQTT-4.12.0-6]) [mqtt5 §10], so reaching `challenge` is the server
/// breaking the protocol rather than a missing feature.
///
/// It verifies nothing, and it says so rather than pretending: a CONNACK that
/// carries final `Authentication Data` is a server offering a proof, and this
/// client has no way to check one, so it is refused and not ignored. A
/// CONNACK with none is what a one-way method answers with, and is accepted.
pub(crate) struct NoAuthenticator;

impl Authenticator for NoAuthenticator {
    fn challenge(&self, _data: Option<&[u8]>) -> Result<Vec<u8>> {
        Err(Error::UnexpectedPacket {
            packet_type: PacketType::Auth,
        })
    }

    fn finish(&self, data: Option<&[u8]>) -> Result<()> {
        match data {
            None => Ok(()),
            Some(_) => Err(Error::AuthenticationFailed(
                "the server sent final authentication data and this client was given no \
                 Authenticator to verify it; use Client::connect_with"
                    .into(),
            )),
        }
    }
}

/// The read half, with the receive buffer.
pub(crate) struct Reader {
    half: ReadHalf<Stream>,
    buf: BytesMut,
    /// Bytes of `buf` the last [`Reader::next`] handed out, dropped at the
    /// start of the next call. Deferring it is what lets the returned slice
    /// live as long as the caller needs it, and advancing the buffer rather
    /// than shifting its contents is what makes the drop O(1) however many
    /// small packets one read brought in.
    pending: usize,
    max_packet_size: u32,
}

impl Reader {
    pub(crate) fn new(half: ReadHalf<Stream>, max_packet_size: u32) -> Reader {
        Reader {
            half,
            buf: BytesMut::with_capacity(READ_CHUNK),
            pending: 0,
            max_packet_size,
        }
    }

    /// The next whole packet's bytes, where they were read to.
    ///
    /// Nothing is copied, not even to give the caller a packet it owns: the
    /// codec decodes the slice in place, so a PUBACK costs no allocation and a
    /// PUBLISH costs the one copy the application's own `Delivery` needs. The
    /// slice is valid until the next call, which is why the reader is a
    /// value of its own and not a field of the loop that handles what it
    /// returns.
    ///
    /// **Cancel-safe, and that is load-bearing.** The event loop races this
    /// future against the write, the command channel and the timers, so it is
    /// dropped mid-read routinely. `read_buf` appends only what the socket
    /// actually delivered and nothing is consumed until a packet is returned,
    /// so a cancelled read is a no-op.
    ///
    /// # Errors
    ///
    /// [`Error::ConnectionClosed`] at end of stream, including a partial
    /// packet; [`Error::Protocol`] for a fixed header the codec refuses,
    /// which includes a declaration above `max_packet_size`; [`Error::Io`] for
    /// a transport failure.
    pub(crate) async fn next(&mut self) -> Result<&[u8]> {
        if self.pending > 0 {
            self.buf.advance(self.pending);
            self.pending = 0;
            self.release_idle_capacity();
        }

        loop {
            match FixedHeader::decode(&self.buf, self.max_packet_size) {
                Ok((header, header_len)) => {
                    let total = header_len + header.remaining_length as usize;
                    if self.buf.len() >= total {
                        self.pending = total;
                        return Ok(&self.buf[..total]);
                    }
                }
                Err(DecodeError::Incomplete) => {}
                Err(error) => return Err(Error::Protocol(error)),
            }

            // A fixed chunk, never the declared length: the buffer grows by
            // what the socket delivers.
            self.buf.reserve(READ_CHUNK);
            if self.half.read_buf(&mut self.buf).await? == 0 {
                return Err(Error::ConnectionClosed);
            }
        }
    }

    /// Gives back what one large packet made the buffer grow to.
    ///
    /// A packet near `Maximum Packet Size` leaves the buffer holding up to
    /// twice that for as long as it lives, and a hostile peer can send one at
    /// will. What is behind the packet just consumed is copied into a fresh
    /// small buffer when it is no more than a chunk — a partial packet larger
    /// than that is the next large one arriving, and its space is in use.
    fn release_idle_capacity(&mut self) {
        if self.buf.capacity() <= KEEP_BUFFER || self.buf.len() > READ_CHUNK {
            return;
        }
        let mut fresh = BytesMut::with_capacity(READ_CHUNK);
        fresh.extend_from_slice(&self.buf);
        self.buf = fresh;
    }
}

/// The write half, with the outbox and the server's size ceiling.
///
/// Three counters, all in bytes since the connection began, say where the
/// outbox stands: `queued` is what has been encoded into it, `written` what
/// the transport has taken, and `confirmed` what the transport has taken *and
/// flushed* — the same on a plain socket, and not under TLS, where `write`
/// fills the TLS layer's buffer and only `flush` puts the records on the wire.
pub(crate) struct Writer {
    half: WriteHalf<Stream>,
    /// Encoded bytes not yet confirmed, of which the first `sent` have been
    /// written.
    out: Vec<u8>,
    sent: usize,
    queued: u64,
    written: u64,
    confirmed: u64,
    /// When the transport last took a byte, or — where it was idle — when
    /// something was last queued for it. The write deadline runs from here.
    progress_at: Instant,
    /// When the last byte confirmed was confirmed, which is what Keep Alive
    /// measures from ([MQTT-3.1.2-20]): the end of one packet to the start of
    /// the next.
    confirmed_at: Instant,
    /// The server's `Maximum Packet Size`, or the encoding's ceiling where it
    /// declared none. "The Client MUST NOT send packets exceeding Maximum
    /// Packet Size to the Server" ([MQTT-3.2.2-15]) [mqtt5 §5], so this is
    /// checked here and never discovered from a DISCONNECT 0x95.
    ceiling: u32,
}

impl Writer {
    pub(crate) fn new(half: WriteHalf<Stream>) -> Writer {
        let now = Instant::now();
        Writer {
            half,
            out: Vec::with_capacity(256),
            sent: 0,
            queued: 0,
            written: 0,
            confirmed: 0,
            progress_at: now,
            confirmed_at: now,
            // Until CONNACK the server's ceiling is unknown, so the
            // encoding's own is the only one that applies.
            ceiling: Limits::MAX_PACKET_SIZE,
        }
    }

    /// Encodes `packet` into the outbox and returns the offset its last byte
    /// ends at, which [`Writer::confirmed`] reaches once it has been written.
    ///
    /// Nothing is written here and nothing waits: this is the half of a send
    /// that cannot block, and the loop that owns the connection decides when
    /// the rest of it happens.
    ///
    /// # Errors
    ///
    /// [`Error::Encode`] where the packet cannot be built or exceeds the
    /// server's ceiling. Nothing is queued then, and the connection is
    /// untouched: the packet is refused, the stream is intact.
    pub(crate) fn queue(&mut self, packet: &Packet<'_>) -> Result<u64> {
        let size = packet.encoded_len()?;
        if size > self.ceiling {
            return Err(Error::Encode(EncodeError::PacketTooLarge {
                size,
                max: self.ceiling,
            }));
        }
        self.make_room();
        if !self.has_work() {
            // The deadline is for a write that is *waiting*, so it starts
            // when there is something to wait for.
            self.progress_at = Instant::now();
        }
        // The size is known, so the buffer grows once and not by doubling.
        self.out.reserve(size as usize);
        let before = self.out.len();
        if let Err(error) = packet.encode(&mut self.out) {
            self.out.truncate(before);
            return Err(error.into());
        }
        self.queued += (self.out.len() - before) as u64;
        Ok(self.queued)
    }

    /// Whether anything queued has not been confirmed yet.
    pub(crate) fn has_work(&self) -> bool {
        self.sent < self.out.len() || self.confirmed < self.written
    }

    /// Moves some of the outbox to the transport: one `write`, or the flush
    /// that follows the last of them.
    ///
    /// **Cancel-safe**: every await is a single `write` or `flush`, the
    /// counters move before the next one starts, and a call after a drop picks
    /// up where the state says it is.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] for a transport failure, including a transport that took
    /// zero bytes of a non-empty write.
    pub(crate) async fn step(&mut self) -> Result<()> {
        if self.sent < self.out.len() {
            let taken = self.half.write(&self.out[self.sent..]).await?;
            if taken == 0 {
                return Err(Error::Io(io::ErrorKind::WriteZero.into()));
            }
            self.sent += taken;
            self.written += taken as u64;
            self.progress_at = Instant::now();
        }
        if self.sent == self.out.len() && self.confirmed < self.written {
            self.half.flush().await?;
            self.confirmed = self.written;
            self.progress_at = Instant::now();
            self.confirmed_at = self.progress_at;
            self.recycle();
        }
        Ok(())
    }

    /// Runs the outbox to empty. For the handshake, which has nothing else to
    /// do and is under a deadline of its own.
    pub(crate) async fn drain(&mut self) -> Result<()> {
        while self.has_work() {
            self.step().await?;
        }
        Ok(())
    }

    /// Queues `packet` and writes it out: the handshake's one-at-a-time send.
    ///
    /// # Errors
    ///
    /// As [`Writer::queue`], and [`Error::Io`] for a transport failure.
    pub(crate) async fn send(&mut self, packet: &Packet<'_>) -> Result<()> {
        self.queue(packet)?;
        self.drain().await
    }

    /// Bytes encoded so far: the offset a packet queued now would end at
    /// less its own length.
    pub(crate) fn queued(&self) -> u64 {
        self.queued
    }

    /// Bytes the transport has taken and flushed.
    pub(crate) fn confirmed(&self) -> u64 {
        self.confirmed
    }

    /// Bytes the transport has taken, flushed or not.
    pub(crate) fn written(&self) -> u64 {
        self.written
    }

    /// When the transport last made progress on what is queued.
    pub(crate) fn progress_at(&self) -> Instant {
        self.progress_at
    }

    /// When the last packet finished being written, the instant Keep Alive
    /// runs from.
    pub(crate) fn confirmed_at(&self) -> Instant {
        self.confirmed_at
    }

    /// Raises the ceiling to what the server declared, once CONNACK has said
    /// so.
    pub(crate) fn set_ceiling(&mut self, ceiling: u32) {
        self.ceiling = ceiling;
    }

    /// Drops what the transport has already taken, once that is most of the
    /// buffer, so a slow peer does not make the outbox a growing log.
    fn make_room(&mut self) {
        if self.sent >= READ_CHUNK && self.sent >= self.out.len() / 2 {
            self.out.drain(..self.sent);
            self.sent = 0;
        }
    }

    /// Empties the outbox, and gives back what one large packet grew it to.
    fn recycle(&mut self) {
        if self.sent == self.out.len() {
            self.out.clear();
            self.sent = 0;
            if self.out.capacity() > KEEP_BUFFER {
                self.out.shrink_to(READ_CHUNK);
            }
        }
    }
}

/// How long a connection that sent its DISCONNECT keeps reading after its
/// own FIN, for the server's close.
pub(crate) const LINGER: Duration = Duration::from_secs(1);

/// Closes a connection whose DISCONNECT is flushed so that the DISCONNECT
/// reaches the server: the write side is shut down, which puts a FIN (and on
/// TLS a `close_notify`) behind it, and then whatever the server still sends
/// is read and discarded until it closes too, for at most `linger`.
///
/// Dropping the socket at once is what loses it. A socket closed with
/// unread bytes in its receive buffer is reset rather than closed (RFC 1122
/// §4.2.2.13), and Windows then discards what it had not sent yet, the
/// DISCONNECT included: the server never reads it, takes the end for a
/// network failure and publishes the Will (MQTT 5.0 §3.1.2.5), which a
/// normal DISCONNECT exists to prevent (§3.14.4).
pub(crate) async fn linger_close(
    exec: &Exec,
    reader: Reader,
    mut writer: Writer,
    linger: Duration,
) {
    let mut half = reader.half;
    let ended = async {
        if writer.half.shutdown().await.is_err() {
            return;
        }
        let mut sink = vec![0u8; READ_CHUNK];
        while let Ok(read) = half.read(&mut sink).await {
            if read == 0 {
                return;
            }
        }
    };
    tokio::select! {
        () = ended => {}
        () = exec.sleep(linger) => {}
    }
}

/// What a completed handshake produced.
pub(crate) struct Handshake {
    pub(crate) reader: Reader,
    pub(crate) writer: Writer,
    pub(crate) limits: ServerLimits,
    pub(crate) session_present: bool,
    pub(crate) keep_alive: Option<Duration>,
    pub(crate) client_id: String,
    /// Whether the transport is encrypted, so a caller can ask what its
    /// credentials travelled over.
    pub(crate) encrypted: bool,
}

/// The steps of a connect, for saying which one ran out of time.
const PHASES: [&str; 4] = [
    "the transport connection",
    "the TLS handshake",
    "write",
    "CONNACK",
];
const DIALLING: usize = 0;
#[cfg(feature = "tls")]
const NEGOTIATING_TLS: usize = 1;
const SENDING_CONNECT: usize = 2;
const AWAITING_CONNACK: usize = 3;

/// Dials `address`, sends CONNECT, and awaits CONNACK.
///
/// The order is the protocol's: "the client's first packet MUST be CONNECT"
/// ([MQTT-3.1.0-1]) and the server "MUST send CONNACK 0x00 before any packet
/// other than AUTH" ([MQTT-3.2.0-2]) [mqtt5 §1]. So this function sends one
/// packet and then accepts exactly two kinds of answer — CONNACK, or an AUTH
/// that continues enhanced authentication — and anything else is
/// [`Error::UnexpectedPacket`].
///
/// **Everything that waits on the server is under one deadline**,
/// `options.connect_timeout`: the name lookup, every dial, the TLS handshake,
/// the CONNECT write, and the CONNACK with whatever AUTH exchange comes first.
/// A server that stalls any of them is the same server, and a deadline on the
/// CONNACK alone left the others to the operating system.
///
/// # Errors
///
/// [`Error::Configuration`] for options that cannot be sent,
/// [`Error::ConnectionRefused`] for a CONNACK with a failure code,
/// [`Error::ServerDisconnected`] where the server sent DISCONNECT instead,
/// [`Error::Timeout`] naming the step that did not finish within
/// `options.connect_timeout`, and the transport and protocol errors of
/// [`Reader::next`].
pub(crate) async fn handshake(
    exec: &Exec,
    address: &str,
    options: &ConnectOptions,
    authenticator: &dyn Authenticator,
) -> Result<Handshake> {
    options.validate()?;

    let deadline = Instant::now() + options.connect_timeout;
    let phase = AtomicUsize::new(DIALLING);
    let connecting = establish(exec, address, options, authenticator, deadline, &phase);
    match exec.within(options.connect_timeout, connecting).await {
        Some(result) => result,
        None => Err(Error::Timeout(PHASES[phase.load(Ordering::Relaxed)])),
    }
}

/// The body of [`handshake`], which has no deadline of its own.
async fn establish(
    exec: &Exec,
    address: &str,
    options: &ConnectOptions,
    authenticator: &dyn Authenticator,
    deadline: Instant,
    phase: &AtomicUsize,
) -> Result<Handshake> {
    // Only TLS needs the host: it is what the certificate is validated
    // against where the caller named no other name.
    #[cfg(feature = "tls")]
    let (host, _) = split_host_port(address)?;
    let stream = Stream::Plain(dial(exec, address, options, deadline).await?);
    // TLS **before** the CONNECT, which is the whole ordering requirement:
    // the User Name, the Password and the Authentication Data all travel in
    // that packet, and a handshake completed afterwards would have protected
    // none of them (5.4.3) [mqtt5 §10].
    #[cfg(feature = "tls")]
    let stream = match &options.tls {
        Some(tls) => {
            phase.store(NEGOTIATING_TLS, Ordering::Relaxed);
            crate::transport::upgrade(stream, tls, host).await?
        }
        None => stream,
    };
    stream.set_nodelay()?;
    let encrypted = stream.is_encrypted();
    let (read_half, write_half) = tokio::io::split(stream);

    let mut reader = Reader::new(read_half, options.limits.maximum_packet_size);
    let mut writer = Writer::new(write_half);

    let will = build_will(options)?;
    let user_properties = borrowed_pairs(&options.user_properties);
    let connect = build_connect(options, will.as_deref(), &user_properties)?;
    phase.store(SENDING_CONNECT, Ordering::Relaxed);
    writer.send(&Packet::Connect(connect)).await?;

    phase.store(AWAITING_CONNACK, Ordering::Relaxed);
    let (limits, session_present, assigned) =
        await_connack(&mut reader, &mut writer, options, authenticator).await?;

    writer.set_ceiling(limits.send_packet_ceiling());

    // "Server Keep Alive present, the client MUST use it ([MQTT-3.2.2-21]);
    // absent, the server MUST use the client's ([MQTT-3.2.2-22])" [mqtt5 §1].
    // The only property in the protocol that overrides the client.
    let keep_alive = match limits.server_keep_alive {
        Some(server) => server,
        None => options.keep_alive,
    };
    let keep_alive = if keep_alive.is_zero() {
        None
    } else {
        Some(keep_alive)
    };

    let client_id = assigned.unwrap_or_else(|| options.client_id.clone());

    Ok(Handshake {
        reader,
        writer,
        limits,
        session_present,
        encrypted,
        keep_alive,
        client_id,
    })
}

/// Reads until CONNACK, answering AUTH challenges on the way.
async fn await_connack(
    reader: &mut Reader,
    writer: &mut Writer,
    options: &ConnectOptions,
    authenticator: &dyn Authenticator,
) -> Result<(ServerLimits, bool, Option<String>)> {
    let mut rounds = 0u32;
    loop {
        // The answer is decoded under *our* declared ceiling, which is the
        // number the server was told to respect (3.1.2.11.4).
        let answer = reader.next().await?;
        let (packet, _) = Packet::decode(answer, options.limits.maximum_packet_size)?;
        match packet {
            Packet::Connack(connack) => {
                check_connack(&connack, options)?;
                // The end of the exchange is the server's last word, and it is
                // asked for whether or not an exchange happened: a CONNACK
                // that skips one is for the authenticator to refuse.
                if options.authentication_method.is_some()
                    && let Err(error) = authenticator.finish(connack.properties.authentication_data)
                {
                    return Err(abort_authentication(writer, error).await);
                }
                return Ok(accept_connack(&connack));
            }
            Packet::Auth(auth) => {
                rounds += 1;
                if rounds > MAX_AUTH_ROUNDS {
                    return Err(Error::Timeout(AUTH_EXCHANGE));
                }
                let reply = match continue_authentication(&auth, options, authenticator) {
                    Ok(reply) => reply,
                    Err(error) => {
                        return Err(abort_authentication(writer, error).await);
                    }
                };
                let properties = Properties {
                    authentication_method: options.authentication_method.as_deref(),
                    authentication_data: Some(&reply),
                    ..Properties::new()
                };
                writer
                    .send(&Packet::Auth(Auth {
                        reason_code: AuthReasonCode::ContinueAuthentication,
                        properties,
                    }))
                    .await?;
            }
            Packet::Disconnect(disconnect) => {
                return Err(Error::ServerDisconnected(disconnect.reason_code));
            }
            other => {
                return Err(Error::UnexpectedPacket {
                    packet_type: other.packet_type(),
                });
            }
        }
    }
}

/// Ends a connect whose authentication failed, the way the protocol asks:
/// "both sides SHOULD send DISCONNECT with an appropriate Reason Code and MUST
/// close the Network Connection" ([MQTT-4.12.1-2]).
///
/// The code is 0x80, because 0x87 is the server's: a client cannot say "not
/// authorized" about a server. Best effort — the connection is closing either
/// way, and the caller's error is what the connect reports.
async fn abort_authentication(writer: &mut Writer, error: Error) -> Error {
    let disconnect = Packet::Disconnect(Disconnect {
        reason_code: DisconnectReasonCode::UnspecifiedError,
        properties: Properties::new(),
    });
    let _ = writer.send(&disconnect).await;
    error
}

/// Checks an AUTH and produces the client's answer.
fn continue_authentication(
    auth: &Auth<'_>,
    options: &ConnectOptions,
    authenticator: &dyn Authenticator,
) -> Result<Vec<u8>> {
    // [MQTT-4.12.0-6]: "absent a client-named method the Server MUST NOT send
    // AUTH". So a client that named none is looking at a protocol violation,
    // and that is a different fault from a method mismatch — checked first,
    // because otherwise every unrequested AUTH would be misreported as a
    // mismatch with a method that was never offered.
    let Some(ours) = options.authentication_method.as_deref() else {
        return Err(Error::UnexpectedPacket {
            packet_type: PacketType::Auth,
        });
    };
    if auth.reason_code != AuthReasonCode::ContinueAuthentication {
        return Err(Error::UnexpectedPacket {
            packet_type: PacketType::Auth,
        });
    }
    // [MQTT-4.12.0-5]: every AUTH repeats the CONNECT's method. A server that
    // switches method mid-exchange is not continuing this authentication.
    if auth.properties.authentication_method != Some(ours) {
        return Err(Error::AuthenticationMethodMismatch);
    }
    authenticator.challenge(auth.properties.authentication_data)
}

/// Refuses a CONNACK that is a refusal, or that names another method.
fn check_connack(connack: &Connack<'_>, options: &ConnectOptions) -> Result<()> {
    if connack.reason_code.is_error() {
        return Err(Error::ConnectionRefused(connack.reason_code));
    }
    // [MQTT-4.12.0-5] again, on the packet that ends the exchange. A server
    // that omits the method on a successful CONNACK is tolerated: the
    // requirement is that it not name a *different* one, and whether what it
    // sent is *proof* is for `Authenticator::finish`.
    if let (Some(ours), Some(theirs)) = (
        options.authentication_method.as_deref(),
        connack.properties.authentication_method,
    ) && ours != theirs
    {
        return Err(Error::AuthenticationMethodMismatch);
    }
    debug_assert_eq!(connack.reason_code, ConnectReasonCode::Success);
    Ok(())
}

/// What a CONNACK that passed [`check_connack`] tells the connection.
fn accept_connack(connack: &Connack<'_>) -> (ServerLimits, bool, Option<String>) {
    let limits = ServerLimits::from_connack(connack);
    let assigned = limits.assigned_client_identifier.clone();
    (limits, connack.session_present, assigned)
}

/// Resolves and dials, trying every address the resolver offered in order.
///
/// Every attempt but the last gets half of what is left of the connect's
/// budget, so that an address that swallows packets — the unreachable IPv6
/// half of a dual-stack name — leaves the next one time to answer. An attempt
/// that runs out is aborted, and not left to finish a connect nobody is
/// waiting for.
async fn dial(
    exec: &Exec,
    address: &str,
    options: &ConnectOptions,
    deadline: Instant,
) -> Result<TcpStream> {
    let (host, port) = split_host_port(address)?;
    let addrs = exec
        .resolve(host, port, options.limits.max_addresses)
        .await?;

    let mut last: Option<Error> = None;
    let count = addrs.len();
    for (index, addr) in addrs.into_iter().enumerate() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let budget = if index + 1 < count {
            remaining / 2
        } else {
            remaining
        };
        // `TcpStream::connect` registers the socket with the reactor as it is
        // built, and a caller driving this future on its own executor — which
        // `Context::owned` exists for — is not inside one. Spawning puts the
        // constructor where the reactor is; awaiting the handle does not need
        // to be.
        let mut connect = exec.spawn(TcpStream::connect(addr));
        match exec.within(budget, &mut connect).await {
            Some(Ok(Ok(stream))) => return Ok(stream),
            Some(Ok(Err(error))) => last = Some(Error::Io(error)),
            Some(Err(error)) => {
                last = Some(Error::Runtime(format!("connect task failed: {error}")));
            }
            None => {
                connect.abort();
                last = Some(Error::Timeout(PHASES[DIALLING]));
            }
        }
    }
    Err(last.unwrap_or(Error::Timeout(PHASES[DIALLING])))
}

/// `host:port`, with the bracket form for an IPv6 literal.
fn split_host_port(address: &str) -> Result<(&str, u16)> {
    let (host, port) = if let Some(rest) = address.strip_prefix('[') {
        let (host, rest) = rest
            .split_once(']')
            .ok_or_else(|| Error::Configuration(format!("unclosed IPv6 literal in {address}")))?;
        let port = rest.strip_prefix(':').ok_or_else(|| {
            Error::Configuration(format!("{address} needs a port; MQTT's are 1883 and 8883"))
        })?;
        (host, port)
    } else {
        address.rsplit_once(':').ok_or_else(|| {
            Error::Configuration(format!("{address} needs a port; MQTT's are 1883 and 8883"))
        })?
    };
    let port = port
        .parse::<u16>()
        .map_err(|_| Error::Configuration(format!("{port} is not a port")))?;
    if port == 0 {
        return Err(Error::Configuration("port 0 is not a port".into()));
    }
    Ok((host, port))
}

/// The Will, encoded from the owned options into the codec's borrowed shape.
fn build_will(options: &ConnectOptions) -> Result<Option<Box<Will<'_>>>> {
    let Some(will) = &options.will else {
        return Ok(None);
    };
    Ok(Some(Box::new(Will {
        topic: &will.topic,
        payload: &will.payload,
        qos: will.qos,
        retain: will.retain,
        properties: Properties {
            will_delay_interval: interval_seconds("will.delay", will.delay)?,
            message_expiry_interval: interval_seconds("will.message_expiry", will.message_expiry)?,
            content_type: will.content_type.as_deref(),
            response_topic: will.response_topic.as_deref(),
            correlation_data: will.correlation_data.as_deref(),
            ..Properties::new()
        },
    })))
}

/// The owned `(String, String)` pairs as the borrowed pairs the codec takes.
fn borrowed_pairs(pairs: &[(String, String)]) -> Vec<(&str, &str)> {
    pairs
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

/// The CONNECT this client's options describe.
fn build_connect<'a>(
    options: &'a ConnectOptions,
    will: Option<&'a Will<'a>>,
    user_properties: &'a [(&'a str, &'a str)],
) -> Result<Connect<'a>> {
    Ok(Connect {
        client_id: &options.client_id,
        clean_start: options.clean_start,
        keep_alive: options.keep_alive_seconds()?,
        properties: Properties {
            session_expiry_interval: interval_seconds("session_expiry", options.session_expiry)?,
            receive_maximum: Some(options.limits.receive_maximum),
            maximum_packet_size: Some(options.limits.maximum_packet_size),
            // 0 is the protocol's default and means "no aliases", so it is
            // sent only when it is not the default — a property whose value is
            // the default is noise on the wire.
            topic_alias_maximum: (options.limits.topic_alias_maximum != 0)
                .then_some(options.limits.topic_alias_maximum),
            request_response_information: options.request_response_information.then_some(true),
            // Default 1, so only the non-default is worth a property.
            request_problem_information: (!options.request_problem_information).then_some(false),
            authentication_method: options.authentication_method.as_deref(),
            authentication_data: options.authentication_data.as_deref(),
            ..Properties::new()
        }
        .with_user_properties(user_properties),
        will: will.cloned().map(Box::new),
        user_name: options.user_name.as_deref(),
        password: options.password.as_deref(),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpSocket};
    use weida_mqtt_codec::Publish;

    use super::*;

    /// Every await in these tests is bounded.
    async fn bounded<F: Future>(future: F) -> F::Output {
        weida_testkit::within("a step of the test", Duration::from_secs(10), future).await
    }

    /// A reader and a writer over one end of a loopback connection, and the
    /// other end.
    async fn connected() -> (Reader, Writer, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let address = listener.local_addr().expect("an address");
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let (read_half, write_half) = tokio::io::split(Stream::Plain(client.expect("connects")));
        let peer = accepted.expect("accepts").0;
        (
            Reader::new(read_half, Limits::MAX_PACKET_SIZE),
            Writer::new(write_half),
            peer,
        )
    }

    fn encoded(packet: &Packet<'_>) -> Vec<u8> {
        let mut out = Vec::new();
        packet.encode(&mut out).expect("encodes");
        out
    }

    #[test]
    fn host_and_port_parse_in_both_literal_forms() {
        assert_eq!(split_host_port("broker:1883").unwrap(), ("broker", 1883));
        assert_eq!(
            split_host_port("127.0.0.1:8883").unwrap(),
            ("127.0.0.1", 8883)
        );
        assert_eq!(split_host_port("[::1]:1883").unwrap(), ("::1", 1883));
        // An unbracketed IPv6 literal is split on its **last** colon, so
        // `::1:1883` parses as the host `::1` - which is what `rsplit_once`
        // does and what a caller writing the bracketless form gets. Asserted
        // rather than left open: a test that accepted either answer would
        // assert nothing.
        assert_eq!(split_host_port("::1:1883").unwrap(), ("::1", 1883));
        assert!(split_host_port("broker").is_err());
        assert!(split_host_port("broker:0").is_err());
        assert!(split_host_port("broker:no").is_err());
        assert!(split_host_port("[::1").is_err());
    }

    /// A property whose value is the protocol's default is left off the wire:
    /// `Topic Alias Maximum` 0 and `Request Problem Information` 1 both mean
    /// exactly what absence means [mqtt5 §11].
    #[test]
    fn a_default_valued_property_is_not_sent() {
        let options = ConnectOptions::new("a");
        let pairs = Vec::new();
        let connect = build_connect(&options, None, &pairs).expect("builds");
        assert_eq!(connect.properties.topic_alias_maximum, None);
        assert_eq!(connect.properties.request_problem_information, None);
        assert_eq!(connect.properties.request_response_information, None);
        // The two this client always declares, because its defaults are not
        // the protocol's.
        assert_eq!(
            connect.properties.receive_maximum,
            Some(options.limits.receive_maximum)
        );
        assert_eq!(
            connect.properties.maximum_packet_size,
            Some(options.limits.maximum_packet_size)
        );
    }

    #[test]
    fn a_non_default_value_is_sent() {
        let mut options = ConnectOptions::new("a");
        options.limits.topic_alias_maximum = 10;
        options.request_problem_information = false;
        options.request_response_information = true;
        let pairs = Vec::new();
        let connect = build_connect(&options, None, &pairs).expect("builds");
        assert_eq!(connect.properties.topic_alias_maximum, Some(10));
        assert_eq!(connect.properties.request_problem_information, Some(false));
        assert_eq!(connect.properties.request_response_information, Some(true));
    }

    /// A refusal carries its code out of the handshake rather than becoming a
    /// closed socket.
    #[test]
    fn a_failed_connack_becomes_a_named_refusal() {
        let options = ConnectOptions::new("a");
        let connack = Connack {
            session_present: false,
            reason_code: ConnectReasonCode::BadUserNameOrPassword,
            properties: Properties::new(),
        };
        let error = check_connack(&connack, &options).expect_err("refused");
        assert_eq!(error.reason_code(), Some(0x86));
    }

    /// [MQTT-3.2.2-21]: Server Keep Alive overrides, which is what the
    /// handshake's own arithmetic does with it.
    #[test]
    fn server_keep_alive_overrides_the_clients() {
        let ours = Duration::from_secs(60);
        let theirs = ServerLimits {
            server_keep_alive: Some(Duration::from_secs(30)),
            ..ServerLimits::default()
        };
        assert_eq!(
            theirs.server_keep_alive.unwrap_or(ours),
            theirs.server_keep_alive.unwrap()
        );
        let silent = ServerLimits::default();
        assert_eq!(silent.server_keep_alive.unwrap_or(ours), ours);
    }

    /// [MQTT-4.12.0-6]: a server that sends AUTH to a client that named no
    /// method is breaking the protocol, so the default authenticator refuses
    /// rather than reporting a missing feature.
    #[test]
    fn an_unrequested_auth_is_refused() {
        let error = NoAuthenticator.challenge(None).expect_err("refused");
        assert!(matches!(
            error,
            Error::UnexpectedPacket {
                packet_type: PacketType::Auth
            }
        ));
    }

    /// The authenticator a client gets by giving none verifies nothing, and so
    /// does not accept a proof it cannot check: final data from the server is
    /// refused, and no data — a one-way method's answer — is not.
    #[test]
    fn the_default_authenticator_refuses_a_proof_it_cannot_check() {
        assert!(NoAuthenticator.finish(None).is_ok());
        let error = NoAuthenticator
            .finish(Some(b"v=ServerSignature"))
            .expect_err("unverifiable");
        assert!(matches!(error, Error::AuthenticationFailed(_)), "{error}");
    }

    /// [MQTT-4.12.0-5]: the method may not change mid-exchange.
    #[test]
    fn an_auth_with_a_different_method_is_refused() {
        struct Echo;
        impl Authenticator for Echo {
            fn challenge(&self, _data: Option<&[u8]>) -> Result<Vec<u8>> {
                Ok(vec![1])
            }
            fn finish(&self, _data: Option<&[u8]>) -> Result<()> {
                Ok(())
            }
        }
        let mut options = ConnectOptions::new("a");
        options.authentication_method = Some("SCRAM-SHA-1".into());

        let matching = Auth {
            reason_code: AuthReasonCode::ContinueAuthentication,
            properties: Properties {
                authentication_method: Some("SCRAM-SHA-1"),
                ..Properties::new()
            },
        };
        assert_eq!(
            continue_authentication(&matching, &options, &Echo).unwrap(),
            vec![1]
        );

        let switched = Auth {
            reason_code: AuthReasonCode::ContinueAuthentication,
            properties: Properties {
                authentication_method: Some("GS2-KRB5"),
                ..Properties::new()
            },
        };
        assert!(matches!(
            continue_authentication(&switched, &options, &Echo),
            Err(Error::AuthenticationMethodMismatch)
        ));
    }

    /// A connection whose socket buffers are a few kilobytes, so that a peer
    /// that does not read stalls a write of any real size at once and on any
    /// machine, whatever its TCP autotuning does.
    async fn stalling() -> (Writer, TcpStream) {
        let listener_socket = TcpSocket::new_v4().expect("a socket");
        listener_socket
            .set_recv_buffer_size(4096)
            .expect("a buffer");
        listener_socket
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("binds");
        let listener = listener_socket.listen(1).expect("listens");
        let address = listener.local_addr().expect("an address");
        let client_socket = TcpSocket::new_v4().expect("a socket");
        client_socket.set_send_buffer_size(4096).expect("a buffer");
        let (client, accepted) = tokio::join!(client_socket.connect(address), listener.accept());
        let (_read_half, write_half) = tokio::io::split(Stream::Plain(client.expect("connects")));
        (Writer::new(write_half), accepted.expect("accepts").0)
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Several packets in one read come out one at a time, in order, and each
        /// is its own bytes: cutting one off the buffer leaves the rest where it
        /// was.
        #[tokio::test]
        async fn packets_in_one_read_come_out_whole_and_in_order() {
            let (mut reader, _writer, mut peer) = connected().await;
            let mut wire = Vec::new();
            for topic in ["a", "bb", "ccc"] {
                wire.extend(encoded(&Packet::Publish(Publish {
                    topic,
                    payload: topic.as_bytes(),
                    ..Publish::default()
                })));
            }
            wire.extend(encoded(&Packet::Pingresp));
            peer.write_all(&wire).await.expect("writes");

            for topic in ["a", "bb", "ccc"] {
                let bytes = bounded(reader.next()).await.expect("a packet");
                let (Packet::Publish(publish), used) =
                    Packet::decode(bytes, Limits::MAX_PACKET_SIZE).expect("decodes")
                else {
                    panic!("a publish");
                };
                assert_eq!((publish.topic, publish.payload), (topic, topic.as_bytes()));
                assert_eq!(used, bytes.len(), "the packet is exactly its own bytes");
            }
            let bytes = bounded(reader.next()).await.expect("the ping");
            assert_eq!(bytes, [0xD0, 0x00]);
        }
        /// A declaration above the cap is refused from the fixed header, with
        /// nothing reserved for it.
        #[tokio::test]
        async fn a_declaration_above_the_cap_is_refused_before_a_body_arrives() {
            let (mut reader, _writer, mut peer) = connected().await;
            reader.max_packet_size = 1024;
            // PUBLISH declaring 268,435,455 bytes, and not one of them sent.
            peer.write_all(&[0x30, 0xFF, 0xFF, 0xFF, 0x7F])
                .await
                .expect("writes");
            let error = bounded(reader.next()).await.expect_err("refused");
            assert!(matches!(error, Error::Protocol(_)), "{error}");
            assert!(reader.buf.capacity() < 64 * 1024, "nothing was reserved");
        }
        /// One packet near the cap must not leave its allocation behind for the
        /// life of the connection: a hostile peer can send one at will, and the
        /// doubling that grew the buffer for it would otherwise be kept.
        #[tokio::test]
        async fn a_large_packet_does_not_pin_the_receive_buffer() {
            let (mut reader, _writer, mut peer) = connected().await;
            let big = vec![7u8; 1024 * 1024];
            let mut wire = encoded(&Packet::Publish(Publish {
                topic: "t",
                payload: &big,
                ..Publish::default()
            }));
            for _ in 0..10 {
                wire.extend(encoded(&Packet::Pingresp));
            }
            let sender = tokio::spawn(async move { peer.write_all(&wire).await.map(|()| peer) });

            {
                let packet = bounded(reader.next()).await.expect("the large packet");
                assert!(packet.len() > 1024 * 1024);
            }
            for _ in 0..10 {
                bounded(reader.next()).await.expect("a ping");
            }
            assert!(
                reader.buf.capacity() <= 64 * 1024,
                "the buffer holds {} bytes after the large packet is gone",
                reader.buf.capacity()
            );
            let _peer = bounded(sender).await.expect("task").expect("written");
        }
        /// The outbox is the same on a plain socket and on TLS: what `queue`
        /// encodes, `step` delivers, in order, and the counters say so.
        #[tokio::test]
        async fn queued_packets_reach_the_peer_in_order_and_are_confirmed() {
            let (_reader, mut writer, mut peer) = connected().await;
            assert!(!writer.has_work());
            let first = writer.queue(&Packet::Pingreq).expect("queues");
            let second = writer.queue(&Packet::Pingreq).expect("queues");
            assert_eq!((first, second), (2, 4));
            assert!(writer.has_work());
            assert_eq!(writer.confirmed(), 0, "nothing is written by queueing");

            bounded(writer.drain()).await.expect("drains");
            assert!(!writer.has_work());
            assert_eq!(
                (writer.queued(), writer.written(), writer.confirmed()),
                (4, 4, 4)
            );

            let mut received = [0u8; 4];
            bounded(peer.read_exact(&mut received))
                .await
                .expect("reads");
            assert_eq!(received, [0xC0, 0x00, 0xC0, 0x00]);
        }
        /// A packet over the server's ceiling is refused without touching the
        /// outbox, so the stream stays intact and the connection stays up: the
        /// failure is the packet's and not the transport's.
        #[tokio::test]
        async fn a_packet_over_the_ceiling_leaves_the_outbox_alone() {
            let (_reader, mut writer, mut peer) = connected().await;
            writer.set_ceiling(64);
            writer.queue(&Packet::Pingreq).expect("fits");

            let big = vec![0u8; 200];
            let error = writer
                .queue(&Packet::Publish(Publish {
                    topic: "t",
                    payload: &big,
                    ..Publish::default()
                }))
                .expect_err("over the ceiling");
            assert!(
                matches!(
                    error,
                    Error::Encode(EncodeError::PacketTooLarge { max: 64, .. })
                ),
                "{error}"
            );
            assert_eq!(writer.queued(), 2, "only the ping is queued");

            writer
                .queue(&Packet::Pingreq)
                .expect("and the next one fits");
            bounded(writer.drain()).await.expect("drains");
            let mut received = [0u8; 4];
            bounded(peer.read_exact(&mut received))
                .await
                .expect("reads");
            assert_eq!(received, [0xC0, 0x00, 0xC0, 0x00], "no half packet between");
        }
        /// A peer that stops reading stops one `step`, and `step` is cancel-safe:
        /// dropped mid-wait it loses nothing, and a later one carries on.
        #[tokio::test]
        async fn a_step_that_waits_for_the_peer_can_be_dropped_and_resumed() {
            let (mut writer, peer) = stalling().await;
            // Far more than the two socket buffers hold, so the write must wait.
            let big = vec![1u8; 4 * 1024 * 1024];
            let publish = Packet::Publish(Publish {
                topic: "t",
                payload: &big,
                ..Publish::default()
            });
            let mut queued = writer.queue(&publish).expect("queues");

            // Steps until one has to wait; each that completes took some bytes.
            // Winsock takes a whole non-blocking send while its buffer has any
            // room, however large the send, so there the first step takes the
            // packet and only the next one waits: a step that left nothing to
            // do queues another.
            let mut waited = false;
            for _ in 0..1000 {
                if !writer.has_work() {
                    queued = writer.queue(&publish).expect("queues");
                }
                if tokio::time::timeout(Duration::from_millis(100), writer.step())
                    .await
                    .is_err()
                {
                    waited = true;
                    break;
                }
            }
            assert!(
                waited,
                "the peer is not reading, so a step must come to wait"
            );
            assert!(writer.has_work());
            assert!(writer.written() < queued, "not all of it went");
            assert!(
                writer.confirmed() < queued,
                "and not all of it is confirmed"
            );

            // The peer starts reading, and the same writer finishes the packet.
            let reader = tokio::spawn(async move {
                let mut peer = peer;
                let mut sink = vec![0u8; 1 << 20];
                let mut total = 0u64;
                while total < queued {
                    match peer.read(&mut sink).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => total += n as u64,
                    }
                }
                total
            });
            bounded(writer.drain()).await.expect("drains");
            assert_eq!(writer.confirmed(), queued);
            assert_eq!(bounded(reader).await.expect("task"), queued);
        }
    }
}
