//! REQ and REP: the synchronous half of request-reply, state machine and all.
//!
//! 28/REQREP, through `docs/research/zeromq.md` §4.2:
//!
//! - **REQ** "SHALL send and then receive exactly one message at a time".
//!   Outgoing: prepend an empty delimiter, round-robin, "SHALL block on
//!   sending, or return a suitable error, when it has no connected peers",
//!   and "SHALL NOT discard messages that it cannot send". Incoming: "SHALL
//!   accept an incoming message only from the last peer that it sent a
//!   request to. SHALL discard silently any messages received from other
//!   peers." Any other order of operations is `EFSM`.
//! - **REP** "SHALL receive and then send exactly one message at a time".
//!   Incoming: fair-queue, "SHALL remove and store the address envelope,
//!   including the delimiter", pass the rest up. Outgoing: prepend the stored
//!   envelope, deliver to the originator, "SHALL silently discard the reply,
//!   or return an error, if the originating peer is no longer connected", and
//!   "SHALL not block on sending".
//!
//! **The envelope is the pattern.** "The ZeroMQ reply envelope formally
//! consists of zero or more reply addresses, followed by an empty frame (the
//! envelope delimiter), followed by the message body." REQ sending `Hello`
//! puts `[empty][Hello]` on the wire; REP strips the envelope up to and
//! including the delimiter, keeps it, and prepends it again on the way back.
//! That is the whole of why a REP reply reaches the requester that asked and
//! not some other one.
//!
//! **`EFSM` is a feature, not an accident.** Lazy Pirate — the zguide's first
//! reliability recipe — "closes and reopens a REQ socket after `EFSM`"
//! (§12/P1), so the error has to be recoverable by discarding the socket.
//! Nothing here keeps process-wide state, so a dropped socket takes its state
//! machine with it and a fresh one starts clean; a test asserts exactly that.

use std::time::Duration;

use weida_zmtp::SocketType;

use crate::context::Context;
use crate::engine::PeerId;
use crate::error::{Error, Result};
use crate::message::{Message, Multipart};
use crate::options::SocketOptions;
use crate::pipe::{MuteAction, Sent};
use crate::socket::{SocketCore, socket_endpoints};

/// Length of `ZMQ_REQ_CORRELATE`'s request-id frame: a `u32`, big-endian.
const REQUEST_ID_LEN: usize = 4;

/// A REQ socket: one request out, one reply in, in that order.
#[derive(Debug)]
pub struct ReqSocket {
    core: SocketCore,
    state: ReqState,
    next_request_id: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReqState {
    /// A request may be sent.
    Ready,
    /// A reply is owed, by this peer, for this request id.
    Awaiting {
        peer: PeerId,
        request_id: Option<u32>,
    },
}

impl ReqSocket {
    /// A REQ socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<ReqSocket> {
        ReqSocket::with_options(context, SocketOptions::default())
    }

    /// A REQ socket with `options`.
    ///
    /// The mute action is not among them: `zmq_socket(3)`'s table says REQ
    /// blocks, and that is a property of the socket type rather than a
    /// setting (§4.1).
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<ReqSocket> {
        options.pipe.outgoing.mute = MuteAction::Block;
        options.pipe.incoming.mute = MuteAction::Block;
        Ok(ReqSocket {
            core: SocketCore::new(context, SocketType::Req, options)?,
            state: ReqState::Ready,
            next_request_id: 1,
        })
    }

    /// Sends a request, prepending the empty delimiter — and, under
    /// `ZMQ_REQ_CORRELATE`, a request-id frame before it.
    ///
    /// Round-robin over the peers whose queue has room, blocking when none
    /// has and when there is no peer at all. Bounded by `ZMQ_SNDTIMEO`.
    ///
    /// Fails with `EFSM` when a reply is still owed, unless
    /// `ZMQ_REQ_RELAXED` is set — in which case the pending exchange is
    /// abandoned and its late reply will be discarded by its request id.
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        let request_id = self.begin_request()?;
        let framed = wrap_request(message.into(), request_id);
        let limit = self.core.options().send_timeout;
        let exec = self.core.exec().clone();
        let delivered = match limit {
            None => self.core.send_round_robin(framed).await?,
            Some(limit) => match exec.within(limit, self.core.send_round_robin(framed)).await {
                Some(result) => result?,
                None => {
                    // The exchange never started, so the socket is still
                    // ready: a timed-out send is not half a request.
                    self.state = ReqState::Ready;
                    return Err(Error::EAGAIN(
                        format!("no peer took the request within {limit:?} (ZMQ_SNDTIMEO)").into(),
                    ));
                }
            },
        };
        let Some(peer) = delivered.peer else {
            self.state = ReqState::Ready;
            return Err(Error::EAGAIN(
                "no peer took the request; a REQ socket never discards one".into(),
            ));
        };
        self.state = ReqState::Awaiting { peer, request_id };
        Ok(())
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` rather than a wait.
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        let request_id = self.begin_request()?;
        let framed = wrap_request(message.into(), request_id);
        match self.core.try_send_round_robin(framed) {
            Ok(peer) => {
                self.state = ReqState::Awaiting { peer, request_id };
                Ok(())
            }
            Err(e) => {
                self.state = ReqState::Ready;
                Err(e)
            }
        }
    }

    /// Sends under an explicit wall-clock bound, whatever `ZMQ_SNDTIMEO`
    /// says.
    pub async fn send_timeout(
        &mut self,
        message: impl Into<Multipart>,
        limit: Duration,
    ) -> Result<()> {
        let exec = self.core.exec().clone();
        match exec.within(limit, self.send(message)).await {
            Some(result) => result,
            None => {
                self.state = ReqState::Ready;
                Err(Error::EAGAIN(
                    format!("no peer took the request within {limit:?}").into(),
                ))
            }
        }
    }

    /// Receives the reply to the request just sent.
    ///
    /// Only from the peer that took the request; anything from another peer
    /// is discarded silently, as is a reply whose envelope is not the
    /// pattern's (no delimiter) or whose request id is not the one
    /// outstanding. Bounded by `ZMQ_RCVTIMEO`.
    ///
    /// Fails with `EFSM` when no request is outstanding.
    pub async fn recv(&mut self) -> Result<Multipart> {
        let ReqState::Awaiting { peer, request_id } = self.state else {
            return Err(Error::EFSM(
                "a REQ socket must send a request before it receives a reply".into(),
            ));
        };
        let limit = self.core.options().recv_timeout;
        let exec = self.core.exec().clone();
        let reply = match limit {
            None => self.await_reply(peer, request_id).await?,
            Some(limit) => match exec.within(limit, self.await_reply(peer, request_id)).await {
                Some(result) => result?,
                None => {
                    return Err(Error::EAGAIN(
                        format!("no reply within {limit:?} (ZMQ_RCVTIMEO)").into(),
                    ));
                }
            },
        };
        self.state = ReqState::Ready;
        Ok(reply)
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` when no reply is queued yet.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        let ReqState::Awaiting { peer, request_id } = self.state else {
            return Err(Error::EFSM(
                "a REQ socket must send a request before it receives a reply".into(),
            ));
        };
        let Some(pipe) = self.core.pipe_of(peer) else {
            return Err(Error::EHOSTUNREACH(
                format!("{peer} is gone before it answered").into(),
            ));
        };
        loop {
            let reply = pipe.incoming().try_recv()?;
            if let Some(body) = unwrap_reply(reply, request_id) {
                self.state = ReqState::Ready;
                return Ok(body);
            }
        }
    }

    /// Receives under an explicit wall-clock bound — Lazy Pirate's poll,
    /// which is a timeout and then a decision.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        let exec = self.core.exec().clone();
        match exec.within(limit, self.recv()).await {
            Some(result) => result,
            None => Err(Error::EAGAIN(format!("no reply within {limit:?}").into())),
        }
    }

    /// Whether a reply is owed right now.
    pub fn awaiting_reply(&self) -> bool {
        matches!(self.state, ReqState::Awaiting { .. })
    }

    fn begin_request(&mut self) -> Result<Option<u32>> {
        match self.state {
            ReqState::Ready => {}
            ReqState::Awaiting { .. } if self.core.options().req_relaxed => {
                // ZMQ_REQ_RELAXED: the exchange is abandoned, and the late
                // reply will be discarded because its request id no longer
                // matches — which is why the pair is required.
                tracing::debug!("abandoning an outstanding request (ZMQ_REQ_RELAXED)");
            }
            ReqState::Awaiting { .. } => {
                return Err(Error::EFSM(
                    "a REQ socket must receive its reply before it sends again; \
                     ZMQ_REQ_RELAXED lifts that and ZMQ_REQ_CORRELATE makes it safe"
                        .into(),
                ));
            }
        }
        if !self.core.options().req_correlate {
            return Ok(None);
        }
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        Ok(Some(id))
    }

    async fn await_reply(&mut self, peer: PeerId, request_id: Option<u32>) -> Result<Multipart> {
        loop {
            let reply = self.core.recv_from(peer).await?;
            if let Some(body) = unwrap_reply(reply, request_id) {
                return Ok(body);
            }
        }
    }
}

socket_endpoints!(ReqSocket);

/// A REP socket: one request in, one reply out, in that order.
#[derive(Debug)]
pub struct RepSocket {
    core: SocketCore,
    /// The envelope and originator of the request being answered.
    pending: Option<Pending>,
}

#[derive(Clone, Debug)]
struct Pending {
    peer: PeerId,
    /// Everything up to and including the delimiter, kept verbatim: "SHALL
    /// remove and store the address envelope, including the delimiter".
    envelope: Vec<Message>,
}

impl RepSocket {
    /// A REP socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<RepSocket> {
        RepSocket::with_options(context, SocketOptions::default())
    }

    /// A REP socket with `options`.
    ///
    /// **The mute action is this socket type's own decision.**
    /// `zmq_socket(3)`'s table leaves REP's row blank (§4.1), so
    /// [`MuteAction::sending`] reports no answer for it — and 28/REQREP
    /// supplies one instead: REP "SHALL not block on sending" and "SHALL
    /// silently discard the reply… if the originating peer is no longer
    /// connected". Its outgoing queue therefore drops rather than blocks, and
    /// a drop is visible in what [`RepSocket::send`] returns.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<RepSocket> {
        options.pipe.outgoing.mute = MuteAction::Drop;
        options.pipe.incoming.mute = MuteAction::Block;
        Ok(RepSocket {
            core: SocketCore::new(context, SocketType::Rep, options)?,
            pending: None,
        })
    }

    /// Receives a request, fair-queued across peers, keeping its envelope.
    ///
    /// Bounded by `ZMQ_RCVTIMEO`. Fails with `EFSM` while a reply is still
    /// owed. A message with no envelope delimiter is not a request-reply
    /// message at all and is discarded.
    pub async fn recv(&mut self) -> Result<Multipart> {
        if self.pending.is_some() {
            return Err(Error::EFSM(
                "a REP socket must send its reply before it receives again".into(),
            ));
        }
        let limit = self.core.options().recv_timeout;
        let exec = self.core.exec().clone();
        match limit {
            None => self.await_request().await,
            Some(limit) => match exec.within(limit, self.await_request()).await {
                Some(result) => result,
                None => Err(Error::EAGAIN(
                    format!("no request within {limit:?} (ZMQ_RCVTIMEO)").into(),
                )),
            },
        }
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` when nothing is queued.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        if self.pending.is_some() {
            return Err(Error::EFSM(
                "a REP socket must send its reply before it receives again".into(),
            ));
        }
        loop {
            let (peer, request) = self.core.try_recv_fair()?;
            if let Some((envelope, body)) = split_envelope(request) {
                self.pending = Some(Pending { peer, envelope });
                return Ok(body);
            }
        }
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        let exec = self.core.exec().clone();
        match exec.within(limit, self.recv()).await {
            Some(result) => result,
            None => Err(Error::EAGAIN(format!("no request within {limit:?}").into())),
        }
    }

    /// Replies to the request just received, prepending its envelope.
    ///
    /// Never blocks. Returns [`Sent::Dropped`] when the originator is gone or
    /// its queue is full — libzmq's "the reply is silently discarded", made a
    /// return value so that it is not actually silent.
    ///
    /// Fails with `EFSM` when no request is outstanding.
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        let Some(pending) = self.pending.take() else {
            return Err(Error::EFSM(
                "a REP socket must receive a request before it sends a reply".into(),
            ));
        };
        let mut frames = pending.envelope;
        frames.extend(message.into().into_frames());
        let reply = Multipart::new(frames)?;
        match self.core.send_to(pending.peer, reply).await {
            Ok(sent) => Ok(sent),
            // "SHALL silently discard the reply… if the originating peer is
            // no longer connected." Discarded, counted, logged, not an error.
            Err(e) if e.errno() == "EHOSTUNREACH" => {
                tracing::debug!(
                    peer = %pending.peer,
                    "discarded a reply: the requester is gone"
                );
                Ok(Sent::Dropped)
            }
            Err(e) => Err(e),
        }
    }

    /// Whether a reply is owed right now.
    pub fn owes_reply(&self) -> bool {
        self.pending.is_some()
    }

    async fn await_request(&mut self) -> Result<Multipart> {
        loop {
            let (peer, request) = self.core.recv_fair().await?;
            match split_envelope(request) {
                Some((envelope, body)) => {
                    self.pending = Some(Pending { peer, envelope });
                    return Ok(body);
                }
                None => tracing::debug!(
                    peer = %peer,
                    "discarded a message with no envelope delimiter: not a request"
                ),
            }
        }
    }
}

/// Puts the request-reply envelope in front of a body.
fn wrap_request(body: Multipart, request_id: Option<u32>) -> Multipart {
    let mut frames = Vec::with_capacity(body.len() + 2);
    if let Some(id) = request_id {
        frames.push(Message::from(id.to_be_bytes().to_vec()));
    }
    frames.push(Message::empty());
    frames.extend(body.into_frames());
    Multipart::new(frames).expect("a request always has the delimiter")
}

/// Checks a reply's envelope and takes the body out of it.
///
/// `None` means "not for us": no delimiter where one is required, or a
/// request id that is not the outstanding one. Both are discarded silently,
/// which is what `ZMQ_REQ_CORRELATE` specifies for the second — "discards
/// incoming messages not starting with those two frames".
fn unwrap_reply(reply: Multipart, request_id: Option<u32>) -> Option<Multipart> {
    let mut frames = reply.into_frames().into_iter();
    if let Some(expected) = request_id {
        let id = frames.next()?;
        if id.len() != REQUEST_ID_LEN || id.as_slice() != expected.to_be_bytes() {
            return None;
        }
    }
    if !frames.next()?.is_empty() {
        return None;
    }
    Some(body_of(frames.collect()))
}

/// Splits a request at its first empty frame: the envelope, then the body.
fn split_envelope(request: Multipart) -> Option<(Vec<Message>, Multipart)> {
    let frames = request.into_frames();
    let delimiter = frames.iter().position(Message::is_empty)?;
    let mut envelope = frames;
    let body = envelope.split_off(delimiter + 1);
    Some((envelope, body_of(body)))
}

/// A message of no frames does not exist, so a body of none is one empty
/// frame — which is what a peer that replied with nothing but the delimiter
/// meant.
fn body_of(frames: Vec<Message>) -> Multipart {
    Multipart::new(frames).unwrap_or_else(|_| Multipart::single(Message::empty()))
}

socket_endpoints!(RepSocket);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;
    use crate::message::MessageLimits;
    use crate::session::{Incoming, Wire};
    use tokio::net::TcpListener;
    use weida_zmtp::{Command, Greeting, Metadata, greeting};

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    fn body(message: &Multipart) -> Vec<Vec<u8>> {
        message
            .frames()
            .iter()
            .map(|frame| frame.as_slice().to_vec())
            .collect()
    }

    /// Pushes a message into the unasked server's outgoing queue, bypassing
    /// its state machine: a REQ socket must not accept it.
    async fn other_speaks_unasked(other: &mut RepSocket) {
        let peers = other.core.peers();
        if let Some(peer) = peers.first() {
            let _ = peer
                .pipe
                .outgoing()
                .send(Multipart::new(vec![Message::empty(), Message::from("unasked")]).unwrap())
                .await;
        }
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: a request goes out, a reply comes back, and neither side ever
        /// sees the envelope — REP gets the body, REQ gets the reply body.
        #[tokio::test]
        async fn a_request_and_a_reply_cross_without_their_envelope() {
            let ctx = context();
            let mut server = RepSocket::new(&ctx).expect("rep");
            let endpoint = server.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut client = ReqSocket::new(&ctx).expect("req");
            client.connect(&endpoint.to_string()).expect("connect");

            client.send("ping").await.expect("send");
            assert!(client.awaiting_reply());

            let request = server.recv().await.expect("recv");
            assert_eq!(body(&request), vec![b"ping".to_vec()]);
            assert!(server.owes_reply());

            assert_eq!(server.send("pong").await.expect("reply"), Sent::Queued);
            let reply = client.recv().await.expect("reply");
            assert_eq!(body(&reply), vec![b"pong".to_vec()]);
            assert!(!client.awaiting_reply());
            assert!(!server.owes_reply());

            // And the pair alternates again, which is the whole pattern.
            client.send("two").await.expect("send");
            assert_eq!(
                body(&server.recv().await.expect("recv")),
                vec![b"two".to_vec()]
            );
            server.send("done").await.expect("reply");
            assert_eq!(
                body(&client.recv().await.expect("reply")),
                vec![b"done".to_vec()]
            );
        }
        /// Claim: REQ's alternation is strict and `EFSM` names the violation —
        /// and a socket discarded after `EFSM` leaves nothing behind, which is
        /// what Lazy Pirate's close-and-reopen needs.
        #[tokio::test]
        async fn req_alternation_is_efsm_and_recoverable_by_reopening() {
            let ctx = context();
            let mut server = RepSocket::new(&ctx).expect("rep");
            let endpoint = server.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut client = ReqSocket::new(&ctx).expect("req");
            client.connect(&endpoint.to_string()).expect("connect");

            // Receive before send.
            let err = client.try_recv().unwrap_err();
            assert_eq!(err.errno(), "EFSM", "{err}");

            client.send("one").await.expect("send");
            let err = client.send("two").await.unwrap_err();
            assert_eq!(err.errno(), "EFSM", "{err}");

            // The request itself is fine; it is the second one that was refused.
            let stale = server.recv().await.expect("the first request");
            assert_eq!(body(&stale), vec![b"one".to_vec()]);

            // Lazy Pirate: after EFSM, throw the socket away. The reply then has
            // nowhere to go and is discarded rather than blocking the server.
            drop(client);
            let gone = std::time::Instant::now();
            while server.peer_count() > 0 {
                assert!(
                    gone.elapsed() < Duration::from_secs(10),
                    "the closed requester never went away"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(server.send("late").await.expect("reply"), Sent::Dropped);

            // And a fresh socket completes its own exchange, which is what
            // close-and-reopen needs.
            let mut reopened = ReqSocket::new(&ctx).expect("a fresh req");
            reopened.connect(&endpoint.to_string()).expect("connect");
            reopened.send("after efsm").await.expect("send");
            assert_eq!(
                body(&server.recv().await.expect("recv")),
                vec![b"after efsm".to_vec()]
            );
            server.send("welcome back").await.expect("reply");
            assert_eq!(
                body(&reopened.recv().await.expect("reply")),
                vec![b"welcome back".to_vec()]
            );
        }
        /// Claim: REP's alternation is strict too, in both directions.
        #[tokio::test]
        async fn rep_alternation_is_efsm() {
            let ctx = context();
            let mut server = RepSocket::new(&ctx).expect("rep");
            let endpoint = server.bind("tcp://127.0.0.1:0").await.expect("bind");

            let err = server.send("unasked").await.unwrap_err();
            assert_eq!(err.errno(), "EFSM", "{err}");

            let mut client = ReqSocket::new(&ctx).expect("req");
            client.connect(&endpoint.to_string()).expect("connect");
            client.send("ask").await.expect("send");
            server.recv().await.expect("recv");

            let err = server.try_recv().unwrap_err();
            assert_eq!(err.errno(), "EFSM", "{err}");
        }
        /// Claim: REQ round-robins over its peers and reads only the one it sent
        /// to, discarding whatever another peer says unasked.
        #[tokio::test]
        async fn req_round_robins_and_hears_only_the_peer_it_asked() {
            let ctx = context();
            let mut first = RepSocket::new(&ctx).expect("rep one");
            let mut second = RepSocket::new(&ctx).expect("rep two");
            let one = first.bind("tcp://127.0.0.1:0").await.expect("bind");
            let two = second.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut client = ReqSocket::new(&ctx).expect("req");
            client.connect(&one.to_string()).expect("connect one");
            client.connect(&two.to_string()).expect("connect two");
            // Both peers must exist before the rotation means anything.
            while client.peer_count() < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            // Two requests, and each server sees exactly one: the rotation.
            client.send("a").await.expect("first");
            let served_first = tokio::time::timeout(Duration::from_secs(5), first.recv()).await;
            let (mut asked, mut other) = match served_first {
                Ok(Ok(request)) => {
                    assert_eq!(body(&request), vec![b"a".to_vec()]);
                    (first, second)
                }
                _ => {
                    let request = second.recv().await.expect("the other server");
                    assert_eq!(body(&request), vec![b"a".to_vec()]);
                    (second, first)
                }
            };

            // The server that was *not* asked speaks anyway. A REQ socket must
            // discard that silently, so the real reply is what arrives.
            other_speaks_unasked(&mut other).await;
            asked.send("the real reply").await.expect("reply");
            let reply = tokio::time::timeout(Duration::from_secs(5), client.recv())
                .await
                .expect("a reply arrived")
                .expect("reply");
            assert_eq!(body(&reply), vec![b"the real reply".to_vec()]);
        }
        /// Claim: `ZMQ_REQ_CORRELATE` puts a request-id frame in front of the
        /// delimiter on the wire, and a reply that does not echo it is discarded
        /// — checked against a peer driven by the codec itself, because that is
        /// where "on the wire" can be read.
        #[tokio::test]
        async fn correlate_prefixes_the_request_and_filters_the_reply() {
            let ctx = context();
            let mut client = ReqSocket::with_options(
                &ctx,
                SocketOptions {
                    req_correlate: true,
                    ..SocketOptions::default()
                },
            )
            .expect("req");

            let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
            let port = listener.local_addr().expect("addr").port();
            client
                .core
                .connect(&format!("tcp://127.0.0.1:{port}"))
                .expect("connect");

            let peer = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                let mut wire = Wire::new(stream, MessageLimits::default());
                // The handshake, from the peer's side.
                let mut greeting = [0u8; greeting::GREETING_LEN];
                wire.read_exactly_for_test(&mut greeting).await;
                wire.write_raw_for_test(&Greeting::null().encode()).await;
                let _ready = wire.read_next().await.expect("READY");
                wire.write_command(&Command::Ready(
                    Metadata::new().with_socket_type(SocketType::Rep),
                ))
                .await
                .expect("our READY");

                // The request: [request-id][empty][body].
                let Incoming::Message(request) = wire.read_next().await.expect("request") else {
                    panic!("expected a message");
                };
                assert_eq!(request.len(), 3, "id, delimiter, body");
                assert_eq!(request.frames()[0].len(), REQUEST_ID_LEN);
                assert!(request.frames()[1].is_empty());
                assert_eq!(request.frames()[2].as_slice(), b"hello");
                let id = request.frames()[0].as_slice().to_vec();

                // A reply with the wrong id first: it must be discarded.
                let wrong = Multipart::new(vec![
                    Message::from(vec![0xde, 0xad, 0xbe, 0xef]),
                    Message::empty(),
                    Message::from("stale"),
                ])
                .expect("frames");
                wire.write_message(&wrong).await.expect("wrong reply");
                // Then the right one.
                let right = Multipart::new(vec![
                    Message::from(id),
                    Message::empty(),
                    Message::from("fresh"),
                ])
                .expect("frames");
                wire.write_message(&right).await.expect("right reply");
                // Hold the connection while the client reads its reply.
                // Margin: 200 ms against a reply the client awaits rather than
                // samples for, so this only has to outlast the await.
                tokio::time::sleep(Duration::from_millis(200)).await;
            });

            client.send("hello").await.expect("send");
            let reply = tokio::time::timeout(Duration::from_secs(5), client.recv())
                .await
                .expect("a reply arrived")
                .expect("reply");
            assert_eq!(body(&reply), vec![b"fresh".to_vec()]);
            peer.await.expect("the peer");
        }
        /// Claim: a misbehaving peer cannot make a REQ socket return something
        /// that is not a reply. A message with no delimiter is discarded and the
        /// well-formed one behind it is delivered.
        #[tokio::test]
        async fn a_misbehaving_peer_is_discarded_not_delivered() {
            let ctx = context();
            let mut client = ReqSocket::new(&ctx).expect("req");

            let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
            let port = listener.local_addr().expect("addr").port();
            client
                .core
                .connect(&format!("tcp://127.0.0.1:{port}"))
                .expect("connect");

            let peer = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                let mut wire = Wire::new(stream, MessageLimits::default());
                let mut greeting = [0u8; greeting::GREETING_LEN];
                wire.read_exactly_for_test(&mut greeting).await;
                wire.write_raw_for_test(&Greeting::null().encode()).await;
                let _ready = wire.read_next().await.expect("READY");
                wire.write_command(&Command::Ready(
                    Metadata::new().with_socket_type(SocketType::Rep),
                ))
                .await
                .expect("our READY");
                let _request = wire.read_next().await.expect("request");

                // No delimiter: not a request-reply message at all.
                wire.write_message(&Multipart::single("naked"))
                    .await
                    .expect("malformed reply");
                wire.write_message(
                    &Multipart::new(vec![Message::empty(), Message::from("proper")])
                        .expect("frames"),
                )
                .await
                .expect("proper reply");
                tokio::time::sleep(Duration::from_millis(200)).await;
            });

            client.send("hello").await.expect("send");
            let reply = tokio::time::timeout(Duration::from_secs(5), client.recv())
                .await
                .expect("a reply arrived")
                .expect("reply");
            assert_eq!(body(&reply), vec![b"proper".to_vec()]);
            peer.await.expect("the peer");
        }
        /// Claim: `ZMQ_REQ_RELAXED` lets a second request go out with no `EFSM`,
        /// and the abandoned exchange's reply is discarded rather than returned
        /// as the answer to the new one — which is why the option requires
        /// correlation, and why the pair without it is refused.
        #[tokio::test]
        async fn relaxed_abandons_the_exchange_and_discards_its_late_reply() {
            let ctx = context();
            let err = ReqSocket::with_options(
                &ctx,
                SocketOptions {
                    req_relaxed: true,
                    ..SocketOptions::default()
                },
            )
            .unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{err}");

            let mut server = RepSocket::new(&ctx).expect("rep");
            let endpoint = server.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut client = ReqSocket::with_options(
                &ctx,
                SocketOptions {
                    req_relaxed: true,
                    req_correlate: true,
                    ..SocketOptions::default()
                },
            )
            .expect("req");
            client.connect(&endpoint.to_string()).expect("connect");

            client.send("first").await.expect("first");
            // No EFSM: the first exchange is abandoned.
            client.send("second").await.expect("second");

            // The server answers both, oldest first. The stale reply carries the
            // abandoned request id and must be discarded.
            let one = server.recv().await.expect("first request");
            assert_eq!(body(&one), vec![b"first".to_vec()]);
            server.send("answer to first").await.expect("reply");
            let two = server.recv().await.expect("second request");
            assert_eq!(body(&two), vec![b"second".to_vec()]);
            server.send("answer to second").await.expect("reply");

            let reply = tokio::time::timeout(Duration::from_secs(5), client.recv())
                .await
                .expect("a reply arrived")
                .expect("reply");
            assert_eq!(
                body(&reply),
                vec![b"answer to second".to_vec()],
                "the abandoned exchange's reply must not be returned"
            );
        }
        /// Claim: `ZMQ_RCVTIMEO` bounds a wait for a reply that is not coming,
        /// with `EAGAIN` — Lazy Pirate's poll, which is a timeout and then a
        /// decision.
        #[tokio::test]
        async fn a_reply_that_never_comes_times_out_with_eagain() {
            let ctx = context();
            let server = RepSocket::new(&ctx).expect("rep");
            let endpoint = server.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut client = ReqSocket::with_options(
                &ctx,
                SocketOptions {
                    recv_timeout: Some(Duration::from_millis(50)),
                    ..SocketOptions::default()
                },
            )
            .expect("req");
            client.connect(&endpoint.to_string()).expect("connect");

            client.send("unanswered").await.expect("send");
            let err = client.recv().await.unwrap_err();
            assert_eq!(err.errno(), "EAGAIN", "{err}");
            // The exchange is still outstanding: a timeout is not a reply.
            assert!(client.awaiting_reply());
        }
    }
}
