//! SP over one connection: the handshake, the framing, and nothing else.
//!
//! `weida-sp` is sans-I/O — it turns bytes into headers, messages and tag
//! stacks and back — so this is where a socket, a buffer and the sequence
//! live. Everything byte-exact is the codec's and is checked against the SP
//! RFCs in a crate that cannot see weida
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.3);
//! everything here is plumbing that can be read against the codec's types.
//!
//! **The handshake is two writes and two reads with no round trip in
//! between.** "As soon as the underlying TCP connection is established, both
//! parties MUST send the protocol header immediately. Both endpoints MUST
//! then wait for the protocol header from the peer before proceeding on"
//! [rfc-tcp §2]. So this session writes its eight octets before it reads
//! anything, and reads the peer's before it sends or accepts one message
//! (`docs/research/nanomsg-nng.md` §1, §3).
//!
//! **A refusal is a close, and only a close.** A header whose magic,
//! version or reserved field differs, or a peer whose endpoint type may not
//! pair with ours, ends the connection with nothing sent back: "the
//! disconnection is the whole error report: there is no reply, no reason
//! code and no round trip in the mapping at all" (§3). The peer learns what
//! a real NNG socket would have told it, which is nothing (§6).
//!
//! **The pairing check happens before any traffic.** Not because a
//! mismatched peer would be hard to handle later, but because there is no
//! later: a PUSH that connects to a PUB has no legal message to send, and
//! admitting the pipe would mean the socket's first observation of the
//! mistake is a message it cannot interpret.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::BufMut;
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};
use weida_runtime::Exec;
use weida_sp::{HEADER_LEN, ProtocolHeader};

use crate::engine::{Connection, Session, SessionFuture};
use crate::error::{Error, Result};
use crate::message::{self, Message};
use crate::pipe::{Pipe, PipeConfig};

/// How much of a connection is read ahead of the message being parsed, so
/// that many small messages cost one read between them.
///
/// This is the only buffer a connection owns for reading: a message body is
/// read straight into its own allocation and nothing is retained between
/// messages, so a connection that once carried a megabyte holds the same
/// eight kilobytes as one that never did.
const READ_BUFFER_BYTES: usize = 8 * 1024;

/// How much outgoing data is gathered before it is written: the size field
/// and header of a message, a small message whole, and a burst of them
/// between two flushes. A larger body bypasses it.
const WRITE_BUFFER_BYTES: usize = 8 * 1024;

/// The least a message body's buffer grows by while it is being read.
const MIN_BODY_GROWTH: usize = 4 * 1024;

/// The SP session: one per socket, shared by every connection it makes.
///
/// Stateless, because SP's connection setup is: the protocol this socket
/// speaks is the only thing a connection needs to know, and it is the same
/// for every connection. There is no negotiation to remember, no capability
/// exchange and no security handshake — the mapping has none (§1).
#[derive(Clone, Copy, Debug)]
pub struct SpSession;

impl SpSession {
    /// The session every socket of this library uses.
    pub const fn new() -> SpSession {
        SpSession
    }

    /// As the [`Session`] the engine takes.
    pub fn shared() -> Arc<dyn Session> {
        Arc::new(SpSession::new())
    }
}

impl Default for SpSession {
    fn default() -> SpSession {
        SpSession::new()
    }
}

impl Session for SpSession {
    fn start(&self, connection: Connection) -> SessionFuture {
        Box::pin(run(connection))
    }
}

/// The clocks one connection runs under.
struct Clocks {
    exec: Exec,
    message_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
}

async fn run(connection: Connection) -> Result<()> {
    let Connection {
        mut stream,
        pipe,
        protocol,
        recv_max_size,
        mut handshake,
        exec,
        message_timeout,
        idle_timeout,
        ..
    } = connection;

    // Ours first and immediately: the mapping requires it, and a peer that
    // waits for ours before sending its own would otherwise deadlock with us
    // (§3).
    stream
        .write_all(&ProtocolHeader::new(protocol).encode())
        .await?;
    stream.flush().await?;

    // Read exactly eight octets and no more: whatever the peer sends after
    // its header is the first message, and belongs to the read loop.
    let mut theirs = [0u8; HEADER_LEN];
    stream.read_exact(&mut theirs).await?;
    let peer = ProtocolHeader::decode(&theirs)?;
    if !peer.accepts(protocol) {
        // Before any traffic, and answered by the close this returning
        // causes. Nothing is written: SP has no frame that could carry a
        // reason (§6).
        return Err(Error::EPROTO(
            format!(
                "a {:?} peer may not pair with a {:?} socket",
                peer.endpoint, protocol
            )
            .into(),
        ));
    }
    handshake.complete(peer.endpoint);

    let clocks = Clocks {
        exec,
        message_timeout,
        idle_timeout,
    };
    // How many messages have crossed in either direction. The reader asks
    // whether this moved while it waited, so that a pipe that is only being
    // written to is not idle.
    let activity = AtomicU64::new(0);
    let discard = !PipeConfig::receives(protocol);
    let (read, write) = tokio::io::split(stream);
    tokio::select! {
        reading = read_loop(read, &pipe, recv_max_size, discard, &clocks, &activity) => reading,
        writing = write_loop(write, &pipe, &clocks, &activity) => writing,
    }
}

/// Runs `work` under [`SocketOptions::message_timeout`](crate::SocketOptions::message_timeout),
/// if one is set.
///
/// `what` names the thing that ran long, for the error: the log line that
/// says so is how an operator learns that a peer, and which direction of
/// it, is the slow one.
async fn bounded<T>(
    clocks: &Clocks,
    what: &'static str,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    let Some(limit) = clocks.message_timeout else {
        return work.await;
    };
    match clocks.exec.within(limit, work).await {
        Some(outcome) => outcome,
        None => Err(Error::ETIMEDOUT(
            format!("{what} took longer than message_timeout ({limit:?}) to cross the connection")
                .into(),
        )),
    }
}

/// The error for a connection that ended inside a message.
fn mid_message(error: std::io::Error) -> Error {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        return Error::ECONNRESET(
            "the peer closed the connection in the middle of a message".into(),
        );
    }
    Error::from(error)
}

/// Waits for the first octet of the next message.
///
/// Nothing is owed while this waits — a quiet connection is not a slow
/// message — so it is bounded only by `idle_timeout`, and only if no octet
/// moved the other way either.
async fn first_octet<R: AsyncRead + Unpin>(
    read: &mut BufReader<R>,
    clocks: &Clocks,
    activity: &AtomicU64,
) -> Result<()> {
    loop {
        let seen = activity.load(Ordering::Relaxed);
        let waited = match clocks.idle_timeout {
            None => Some(read.fill_buf().await),
            Some(limit) => clocks.exec.within(limit, read.fill_buf()).await,
        };
        match waited {
            Some(Ok([])) => {
                return Err(Error::ECONNRESET("the peer closed the connection".into()));
            }
            Some(Ok(_)) => return Ok(()),
            Some(Err(error)) => return Err(error.into()),
            None if activity.load(Ordering::Relaxed) != seen => {}
            None => {
                return Err(Error::ETIMEDOUT(
                    "the pipe moved no octet in either direction within idle_timeout".into(),
                ));
            }
        }
    }
}

/// Reads one message: its size, judged against `cap` before anything is
/// allocated, then its octets.
///
/// `None` for a message that was read and thrown away, which is what a
/// protocol with no receive operation does with everything it is sent.
async fn read_message<R: AsyncRead + Unpin>(
    read: &mut BufReader<R>,
    cap: u64,
    discard: bool,
) -> Result<Option<Message>> {
    let mut size = [0u8; weida_sp::message::SIZE_LEN];
    read.read_exact(&mut size).await.map_err(mid_message)?;
    let len = message::declared_len(size, cap)?;
    if discard {
        let copied = tokio::io::copy(&mut (&mut *read).take(len as u64), &mut tokio::io::sink())
            .await
            .map_err(mid_message)?;
        if copied < len as u64 {
            return Err(mid_message(std::io::ErrorKind::UnexpectedEof.into()));
        }
        return Ok(None);
    }
    let body = read_body(read, len).await?;
    // The wire carries one run of octets; which of them in front belong to
    // the protocol is the socket type's question, not this loop's (§3).
    Ok(Some(Message::from_body(body)))
}

/// Reads exactly `len` octets into an allocation of their own.
///
/// **Memory follows what has arrived**, never the declared length: the
/// buffer starts empty and grows by at most its own size at a time, capped
/// at what is still owed, so a peer that declares a megabyte and sends a
/// kilobyte holds a few kilobytes. And because the last growth is capped at
/// what remains, the buffer ends exactly as large as the message, which is
/// what lets it become the message's body without a copy.
async fn read_body<R: AsyncRead + Unpin>(read: &mut BufReader<R>, len: usize) -> Result<Vec<u8>> {
    let mut body: Vec<u8> = Vec::new();
    while body.len() < len {
        let remaining = len - body.len();
        if body.len() == body.capacity() {
            let growth = remaining.min(body.len().max(MIN_BODY_GROWTH));
            body.try_reserve_exact(growth).map_err(|_| {
                Error::ESYSERR(
                    format!("could not allocate {growth} more octets for a message").into(),
                )
            })?;
        }
        let taken = read
            .read_buf(&mut (&mut body).limit(remaining))
            .await
            .map_err(mid_message)?;
        if taken == 0 {
            return Err(mid_message(std::io::ErrorKind::UnexpectedEof.into()));
        }
    }
    Ok(body)
}

/// Reads whole messages and hands them to the pipe's incoming queue.
///
/// The queue applies the protocol's own action at its bound: SUB drops, a
/// full queue on anything else stops this loop reading, which is the only
/// backpressure SP has — it grants no credit on the wire (§12/P12). A
/// message is under `message_timeout` from its first octet to its last and
/// not while it waits for room: a slow reader is this side's doing, and the
/// peer is not made to pay for it.
async fn read_loop<R>(
    read: R,
    pipe: &Pipe,
    recv_max_size: u64,
    discard: bool,
    clocks: &Clocks,
    activity: &AtomicU64,
) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut read = BufReader::with_capacity(READ_BUFFER_BYTES, read);
    loop {
        first_octet(&mut read, clocks, activity).await?;
        let message = bounded(
            clocks,
            "an incoming message",
            read_message(&mut read, recv_max_size, discard),
        )
        .await?;
        activity.fetch_add(1, Ordering::Relaxed);
        if let Some(message) = message {
            pipe.incoming().send(message).await?;
        }
    }
}

/// Writes one message: the size field, the header, the body.
///
/// Nothing is concatenated: the three go to the buffered writer in turn,
/// which gathers a small message into one write and lets a large body go
/// straight through.
async fn write_message<W: AsyncWrite + Unpin>(
    write: &mut BufWriter<W>,
    message: &Message,
) -> Result<()> {
    write
        .write_all(&(message.wire_len() as u64).to_be_bytes())
        .await?;
    write.write_all(message.header()).await?;
    write.write_all(message.body()).await?;
    Ok(())
}

/// Takes messages from the pipe's outgoing queue and frames them.
///
/// Whatever is already queued behind a message goes out with it, in one
/// flush: a burst of small messages is one write to the connection and not
/// one each.
async fn write_loop<W>(write: W, pipe: &Pipe, clocks: &Clocks, activity: &AtomicU64) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut write = BufWriter::with_capacity(WRITE_BUFFER_BYTES, write);
    loop {
        let message: Message = pipe.outgoing().recv().await?;
        bounded(
            clocks,
            "an outgoing message",
            write_message(&mut write, &message),
        )
        .await?;
        while let Ok(next) = pipe.outgoing().try_recv() {
            bounded(
                clocks,
                "an outgoing message",
                write_message(&mut write, &next),
            )
            .await?;
        }
        bounded(clocks, "an outgoing message", async {
            write.flush().await.map_err(Error::from)
        })
        .await?;
        activity.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use weida_sp::EndpointType;

    use super::*;
    use crate::context::{Context, ContextConfig};
    use crate::endpoint::Endpoint;
    use crate::engine::Engine;
    use crate::options::SocketOptions;

    fn socket(context: &Context, protocol: EndpointType) -> Engine {
        Engine::new(
            context,
            protocol,
            SocketOptions {
                handshake_timeout: Duration::from_millis(500),
                ..SocketOptions::default()
            },
            SpSession::shared(),
        )
        .expect("engine")
    }

    /// A raw peer: a plain TCP connection with no SP code behind it, so that
    /// what this library puts on the wire is read octet by octet rather than
    /// compared against itself.
    async fn raw_peer(engine: &Engine) -> (TcpStream, Endpoint) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
        let url = Endpoint::parse(&format!(
            "tcp://127.0.0.1:{}",
            listener.local_addr().unwrap().port()
        ))
        .unwrap();
        let dialling = {
            let engine = engine.clone();
            let url = url.clone();
            tokio::spawn(async move { engine.dial_nonblocking(&url) })
        };
        let (stream, _) = listener.accept().await.expect("accept");
        dialling.await.expect("task").expect("dialer");
        (stream, url)
    }

    /// Claim: this side sends its eight octets **immediately** — before the
    /// peer has said anything at all — and they are exactly the header the
    /// codec's golden vectors pin for that protocol.
    #[tokio::test]
    async fn the_protocol_header_goes_out_before_anything_arrives() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        for (protocol, expected) in [
            (EndpointType::Req, [0x00, 0x53, 0x50, 0, 0x00, 0x30, 0, 0]),
            (EndpointType::Pub, [0x00, 0x53, 0x50, 0, 0x00, 0x20, 0, 0]),
            (EndpointType::Bus, [0x00, 0x53, 0x50, 0, 0x00, 0x70, 0, 0]),
        ] {
            let engine = socket(&ctx, protocol);
            let (mut peer, _) = raw_peer(&engine).await;

            let mut theirs = [0u8; HEADER_LEN];
            peer.read_exact(&mut theirs)
                .await
                .expect("the header arrives without us sending one");
            assert_eq!(theirs, expected, "{protocol:?} put the wrong octets out");
            engine.close();
        }
    }

    /// Claim: a peer whose endpoint type may not pair with ours is
    /// disconnected **before any traffic**, and the disconnection is the
    /// whole error report — no reply, no reason code, nothing after the
    /// eight octets we had already sent.
    #[tokio::test]
    async fn a_mispaired_peer_is_disconnected_and_told_nothing() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let engine = socket(&ctx, EndpointType::Req);
        let (mut peer, _) = raw_peer(&engine).await;

        let mut ours = [0u8; HEADER_LEN];
        peer.read_exact(&mut ours).await.expect("their header");

        // A PUSH where a REP belongs.
        peer.write_all(&ProtocolHeader::new(EndpointType::Push).encode())
            .await
            .expect("write");

        let mut after = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(2), peer.read_to_end(&mut after))
            .await
            .expect("the connection is closed rather than left hanging")
            .expect("read to end");
        assert_eq!(read, 0, "a refusal sent {after:?} instead of nothing");
        assert!(engine.pipes().is_empty(), "no pipe from a mispaired peer");
    }

    /// Claim: every rule the mapping states about the header closes the
    /// connection, and closing is all that happens — bad magic, bad version
    /// and a nonzero reserved field alike.
    #[tokio::test]
    async fn a_malformed_header_closes_the_connection_and_nothing_else() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let good = ProtocolHeader::new(EndpointType::Rep).encode();
        let mut magic = good;
        magic[2] = 0x51;
        let mut version = good;
        version[3] = 1;
        let mut reserved = good;
        reserved[7] = 1;

        for bad in [magic, version, reserved] {
            let engine = socket(&ctx, EndpointType::Req);
            let (mut peer, _) = raw_peer(&engine).await;
            let mut ours = [0u8; HEADER_LEN];
            peer.read_exact(&mut ours).await.expect("their header");
            peer.write_all(&bad).await.expect("write");

            let mut after = Vec::new();
            let read = tokio::time::timeout(Duration::from_secs(2), peer.read_to_end(&mut after))
                .await
                .expect("closed rather than hanging")
                .expect("read to end");
            assert_eq!(read, 0, "{bad:02x?} was answered with {after:02x?}");
            assert!(engine.pipes().is_empty());
            engine.close();
        }
    }

    /// Claim: what a message looks like on the wire is the codec's 64-bit
    /// framing and nothing else — a big-endian length and exactly that many
    /// octets, with no tag, flag or continuation bit anywhere.
    #[tokio::test]
    async fn a_message_on_the_wire_is_a_length_and_a_body() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let engine = socket(&ctx, EndpointType::Bus);
        let (mut peer, _) = raw_peer(&engine).await;

        let mut ours = [0u8; HEADER_LEN];
        peer.read_exact(&mut ours).await.expect("their header");
        peer.write_all(&ProtocolHeader::new(EndpointType::Bus).encode())
            .await
            .expect("write");

        // Wait for the pipe to be admitted, then send through it.
        let pipe = loop {
            if let Some(pipe) = engine.pipes().first().cloned() {
                break pipe;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        pipe.outgoing()
            .send(Message::from_body(b"hello".to_vec()))
            .await
            .expect("queued");

        let mut wire = [0u8; 13];
        peer.read_exact(&mut wire)
            .await
            .expect("the framed message");
        assert_eq!(wire, *b"\x00\x00\x00\x00\x00\x00\x00\x05hello");

        // And the other direction, including the empty message, which is
        // eight zero octets and nothing after them.
        peer.write_all(b"\x00\x00\x00\x00\x00\x00\x00\x03abc")
            .await
            .expect("write");
        peer.write_all(&[0u8; 8]).await.expect("write empty");
        let first = pipe.incoming().recv().await.expect("delivered");
        assert_eq!(first.body(), b"abc");
        assert!(first.header().is_empty(), "the session claims no header");
        let second = pipe.incoming().recv().await.expect("delivered");
        assert!(second.body().is_empty());
    }

    /// Claim: two sockets of this library exchange messages over the
    /// engine's pipes, in both directions, with the framing above.
    #[tokio::test]
    async fn two_sockets_of_this_library_talk_to_each_other() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let server = socket(&ctx, EndpointType::Rep);
        let client = socket(&ctx, EndpointType::Req);
        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");
        client.dial(listener.url()).await.expect("dial");

        let out = client.pipes().first().cloned().expect("a pipe");
        out.outgoing()
            .send(Message::from_body(b"ping".to_vec()))
            .await
            .expect("queued");

        let inbound = loop {
            if let Some(pipe) = server.pipes().first().cloned() {
                break pipe;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let request = inbound.incoming().recv().await.expect("arrived");
        assert_eq!(request.body(), b"ping");

        inbound
            .outgoing()
            .send(Message::from_body(b"pong".to_vec()))
            .await
            .expect("queued");
        let reply = out.incoming().recv().await.expect("arrived");
        assert_eq!(reply.body(), b"pong");
    }

    /// Claim: `NNG_OPT_RECVMAXSZ` is enforced on the connection from the
    /// declared length alone — a peer that declares 2^64-1 octets and sends
    /// none of them loses its pipe rather than our memory.
    #[tokio::test]
    async fn an_oversized_declaration_ends_the_pipe_before_it_is_believed() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let engine = Engine::new(
            &ctx,
            EndpointType::Bus,
            SocketOptions {
                recv_max_size: 64,
                handshake_timeout: Duration::from_millis(500),
                ..SocketOptions::default()
            },
            SpSession::shared(),
        )
        .expect("engine");
        let (mut peer, _) = raw_peer(&engine).await;
        let mut ours = [0u8; HEADER_LEN];
        peer.read_exact(&mut ours).await.expect("their header");
        peer.write_all(&ProtocolHeader::new(EndpointType::Bus).encode())
            .await
            .expect("write");
        while engine.pipes().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        peer.write_all(&[0xFFu8; 8]).await.expect("a huge claim");
        let mut after = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(2), peer.read_to_end(&mut after))
            .await
            .expect("the pipe is closed rather than left waiting for 2^64 octets")
            .expect("read to end");
        assert_eq!(read, 0);
        for _ in 0..200 {
            if engine.pipes().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the pipe survived a declaration it could never satisfy");
    }

    /// A socket of `protocol` under `options`, with a handshake deadline
    /// short enough for a test.
    fn socket_under(context: &Context, protocol: EndpointType, options: SocketOptions) -> Engine {
        Engine::new(
            context,
            protocol,
            SocketOptions {
                handshake_timeout: Duration::from_millis(500),
                ..options
            },
            SpSession::shared(),
        )
        .expect("engine")
    }

    /// A raw peer that has completed the handshake as `as_protocol` and
    /// whose pipe the engine has admitted.
    async fn handshaken_peer(engine: &Engine, as_protocol: EndpointType) -> TcpStream {
        let (mut peer, _) = raw_peer(engine).await;
        let mut ours = [0u8; HEADER_LEN];
        peer.read_exact(&mut ours).await.expect("their header");
        peer.write_all(&ProtocolHeader::new(as_protocol).encode())
            .await
            .expect("write");
        for _ in 0..400 {
            if !engine.pipes().is_empty() {
                return peer;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the pipe was not admitted");
    }

    /// Claim: whole messages come out of the read loop however the stream
    /// is cut — inside a size field, between a size field and its body,
    /// inside a long body — and the empty message is a message. The cuts are
    /// where a connection's reads end, and they are the peer's to choose.
    #[tokio::test]
    async fn the_read_loop_delivers_whole_messages_however_the_stream_is_cut() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let bodies: Vec<Vec<u8>> = vec![
            b"first".to_vec(),
            Vec::new(),
            vec![9u8; 20_000],
            b"last".to_vec(),
        ];
        let mut wire = Vec::new();
        for body in &bodies {
            wire.extend_from_slice(&Message::from_body(body.clone()).encode());
        }
        let clocks = Clocks {
            exec: ctx.exec().clone(),
            message_timeout: None,
            idle_timeout: None,
        };
        let cuts: Vec<usize> = (1..60).chain((60..wire.len()).step_by(499)).collect();
        for cut in cuts {
            let (mut peer, ours) = tokio::io::duplex(64 * 1024);
            let pipe = Pipe::new(
                crate::pipe::PipeId::new(1),
                PipeConfig::of(EndpointType::Bus),
            );
            let activity = AtomicU64::new(0);
            let reading = read_loop(ours, &pipe, 1024 * 1024, false, &clocks, &activity);
            let feeding = async {
                peer.write_all(&wire[..cut]).await.expect("first part");
                tokio::task::yield_now().await;
                peer.write_all(&wire[cut..]).await.expect("second part");
                drop(peer);
            };
            let (ended, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(reading, feeding)
            })
            .await
            .expect("the loop ends when the stream does");
            assert!(
                matches!(ended, Err(Error::ECONNRESET(_))),
                "cut {cut}: {ended:?}"
            );
            for (n, body) in bodies.iter().enumerate() {
                let message = pipe
                    .incoming()
                    .try_recv()
                    .unwrap_or_else(|e| panic!("cut {cut}: message {n} missing: {e}"));
                assert_eq!(message.body(), &body[..], "cut {cut}: message {n}");
                assert!(message.header().is_empty());
            }
            assert!(pipe.incoming().is_empty(), "cut {cut}: more than was sent");
        }
    }

    /// Claim: a message that is declared and then trickled closes the pipe
    /// after `message_timeout`, and a connection that is merely quiet does
    /// not. The first holds a pipe slot and every octet it sent for as long
    /// as its peer likes; the second is what every PUB/SUB pipe between two
    /// rare publications is.
    #[tokio::test]
    async fn a_message_that_trickles_closes_the_pipe_and_a_quiet_connection_does_not() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let timeout = Duration::from_millis(300);
        let engine = socket_under(
            &ctx,
            EndpointType::Bus,
            SocketOptions {
                message_timeout: Some(timeout),
                ..SocketOptions::default()
            },
        );

        let mut quiet = handshaken_peer(&engine, EndpointType::Bus).await;
        tokio::time::sleep(timeout * 3).await;
        assert_eq!(
            engine.pipes().len(),
            1,
            "silence between messages is not slowness"
        );
        quiet
            .write_all(&Message::from_body(b"still here".to_vec()).encode())
            .await
            .expect("write");
        let pipe = engine.pipes().first().cloned().expect("pipe");
        let message = tokio::time::timeout(Duration::from_secs(2), pipe.incoming().recv())
            .await
            .expect("delivered")
            .expect("a message");
        assert_eq!(message.body(), b"still here");

        // A kilobyte declared, ten octets sent, then nothing.
        let started = std::time::Instant::now();
        quiet.write_all(&1024u64.to_be_bytes()).await.expect("size");
        quiet.write_all(&[1u8; 10]).await.expect("a few octets");
        let mut after = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(3), quiet.read_to_end(&mut after))
            .await
            .expect("the pipe is closed rather than held for a message that never finishes")
            .expect("read to end");
        assert_eq!(read, 0);
        assert!(
            started.elapsed() >= timeout / 2,
            "closed after {:?}, before the deadline",
            started.elapsed()
        );
    }

    /// Claim: a pipe with no octet moving in either direction is closed
    /// after `idle_timeout`, and one that is only being written to is not —
    /// a PUB's pipe carries nothing inbound for its whole life.
    #[tokio::test]
    async fn an_idle_pipe_is_closed_and_one_being_written_to_is_not() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let idle = Duration::from_millis(300);
        let options = SocketOptions {
            idle_timeout: Some(idle),
            ..SocketOptions::default()
        };

        let engine = socket_under(&ctx, EndpointType::Bus, options.clone());
        let mut silent = handshaken_peer(&engine, EndpointType::Bus).await;
        let mut after = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(3), silent.read_to_end(&mut after))
            .await
            .expect("an idle pipe is closed")
            .expect("read to end");
        assert_eq!(read, 0);

        let engine = socket_under(&ctx, EndpointType::Bus, options);
        let mut listening = handshaken_peer(&engine, EndpointType::Bus).await;
        let pipe = engine.pipes().first().cloned().expect("pipe");
        let drain = tokio::spawn(async move {
            let mut scratch = [0u8; 256];
            while matches!(listening.read(&mut scratch).await, Ok(n) if n > 0) {}
        });
        for _ in 0..12 {
            pipe.outgoing()
                .send(Message::from_body(b"tick".to_vec()))
                .await
                .expect("queued");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            engine.pipes().len(),
            1,
            "a pipe that is being written to is not idle"
        );
        drain.abort();
    }

    /// Claim: a protocol with no receive operation holds nothing a peer
    /// writes to it. A SUB-typed peer that streams data at a PUB fills no
    /// queue, and the socket keeps nothing of it once the peer has gone: the
    /// octets are read and thrown away as they arrive.
    #[tokio::test]
    async fn a_protocol_with_no_receive_operation_discards_what_a_peer_writes_to_it() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let engine = socket(&ctx, EndpointType::Pub);
        let mut peer = handshaken_peer(&engine, EndpointType::Sub).await;
        let pipe = engine.pipes().first().cloned().expect("pipe");

        let wire = Message::from_body(vec![1u8; 1024 * 1024]).encode();
        for _ in 0..20 {
            tokio::time::timeout(Duration::from_secs(5), peer.write_all(&wire))
                .await
                .expect("the PUB keeps reading")
                .expect("write");
        }
        assert_eq!(
            pipe.incoming().queued_bytes(),
            0,
            "nothing is queued for nobody"
        );
        assert!(pipe.incoming().is_empty());

        drop(peer);
        for _ in 0..400 {
            if engine.pipes().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(engine.pipes().is_empty(), "the pipe went with its peer");
        assert!(
            !engine.has_arrived(),
            "nothing the peer wrote is left in the socket"
        );
    }
}
