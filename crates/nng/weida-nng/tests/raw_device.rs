//! Raw sockets and the device, over real connections.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::task::JoinHandle;
use weida_nng::{
    BusSocket, Context, ContextConfig, Error, Message, RawSocket, RepSocket, ReqSocket,
    SocketOptions, raw,
};
use weida_sp::header::{HEADER_LEN, ProtocolHeader};
use weida_sp::{Backtrace, EndpointType, backtrace, message};

fn options() -> SocketOptions {
    SocketOptions {
        recv_timeout: Some(Duration::from_secs(5)),
        send_timeout: Some(Duration::from_secs(5)),
        handshake_timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(10),
        resend_time: Duration::from_secs(30),
        ..SocketOptions::default()
    }
}

/// Claim: a raw socket refuses a context, and the refusal names the reason
/// the state a context holds is absent (§4).
#[tokio::test]
async fn a_raw_socket_refuses_a_context() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    for protocol in [
        EndpointType::Req,
        EndpointType::Rep,
        EndpointType::Surveyor,
        EndpointType::Respondent,
        EndpointType::Bus,
    ] {
        let socket = RawSocket::open(&ctx, protocol).expect("raw socket");
        let err = socket.context().unwrap_err();
        assert!(matches!(err, Error::ENOTSUP(_)), "{protocol:?}: {err:?}");
        assert!(err.cause().contains("holds no protocol state"));
    }
}

/// Claim: a raw socket preserves the wire header. A raw REP receives the
/// tag stack rather than a stripped payload, and a raw send writes back
/// exactly what it is given — which is what makes a forwarder possible at
/// all (§4).
#[tokio::test]
async fn a_raw_socket_hands_over_the_wire_header_untouched() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let raw_rep = RawSocket::with_options(&ctx, EndpointType::Rep, options()).expect("raw rep");
    let url = raw_rep
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let client = ReqSocket::with_options(&ctx, options()).expect("req");
    client.dial(&url).await.expect("dial");
    client.send(b"ping".to_vec()).await.expect("send");

    let (pipe, request) = raw_rep.recv().await.expect("the raw request");
    assert!(
        request.header().is_empty(),
        "raw mode claims nothing: the whole payload is the body"
    );
    let (stack, payload) = backtrace::decode(request.body(), 8).expect("the stack is still there");
    assert_eq!(payload, b"ping");
    assert_eq!(stack.peers, Vec::<u32>::new());

    // Answer by hand: the application owns the header in raw mode.
    let reply = Message::from_body(stack.encode_message(b"pong"));
    raw_rep.send_to(pipe, reply).await.expect("reply");
    assert_eq!(client.recv().await.expect("reply").body(), b"pong");
}

/// Claim: a device forwards a request through and a reply back, and the
/// forwarder's **own peer id was pushed on the way in and popped on the
/// way out** — the mechanism that routes a reply with no global address
/// anywhere (§4).
#[tokio::test]
async fn a_request_through_a_device_is_pushed_and_popped() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // The device: a raw REP facing the clients, a raw REQ facing the
    // servers, which is the load-balancing intermediary of §9.
    let front = RawSocket::with_options(&ctx, EndpointType::Rep, options()).expect("raw rep");
    let back = RawSocket::with_options(&ctx, EndpointType::Req, options()).expect("raw req");
    let front_url = front
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let back_url = back
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let forwarding = {
        let front = front.clone();
        let back = back.clone();
        tokio::spawn(async move { raw::device(&front, &back).await })
    };

    let server = RepSocket::with_options(&ctx, options()).expect("rep");
    server.dial(&back_url).await.expect("dial the device");
    let client = ReqSocket::with_options(&ctx, options()).expect("req");
    client.dial(&front_url).await.expect("dial the device");
    while front.pipe_count() < 1 || back.pipe_count() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    client.send(b"work".to_vec()).await.expect("send");

    let request = server.recv().await.expect("the request arrived");
    assert_eq!(request.body(), b"work");
    let (stack, _) = backtrace::decode(request.header(), 8).expect("the stack");
    assert_eq!(
        stack.peers.len(),
        1,
        "the forwarder pushed exactly its own peer id: {stack:?}"
    );
    let pushed = stack.peers[0];

    server.send(b"done".to_vec()).await.expect("reply");
    let reply = server_reply(&client).await;
    let (reply_stack, _) = backtrace::decode(reply.header(), 8).expect("the stack");
    assert!(
        reply_stack.peers.is_empty(),
        "the forwarder popped its own id again; the originator sees only its request ID: \
         {reply_stack:?}"
    );
    assert_eq!(reply_stack.id, stack.id, "and the same request ID");
    assert_ne!(pushed, 0, "the pushed id was a real pipe id");
    assert_eq!(reply.body(), b"done");

    forwarding.abort();
}

async fn server_reply(client: &ReqSocket) -> Message {
    tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("the reply came back through the device")
        .expect("a reply")
}

/// Claim: a device between sockets that cannot pair is refused, because it
/// would forward messages nobody can interpret.
#[tokio::test]
async fn a_device_between_incompatible_sockets_is_refused() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RawSocket::open(&ctx, EndpointType::Rep).expect("raw rep");
    let pull = RawSocket::open(&ctx, EndpointType::Pull).expect("raw pull");
    let err = raw::device(&rep, &pull).await.unwrap_err();
    assert!(matches!(err, Error::ENOTSUP(_)), "{err:?}");
    assert!(err.cause().contains("pair"));
}

/// Claim: a device increments the PAIR v1 hop count and drops a message
/// past its own `NNG_OPT_MAXTTL`, with nothing sent back — the loop
/// control PAIR v1 exists for (§4, §8).
#[tokio::test]
async fn a_device_bounds_the_pair_v1_hop_count() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let ttl = SocketOptions {
        max_ttl: 3,
        ..options()
    };
    let left = RawSocket::with_options(&ctx, EndpointType::PairV1, ttl.clone()).expect("raw pair1");
    let right = RawSocket::with_options(&ctx, EndpointType::PairV1, ttl).expect("raw pair1");
    let left_url = left
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let right_url = right
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let forwarding = {
        let left = left.clone();
        let right = right.clone();
        tokio::spawn(async move { raw::device(&left, &right).await })
    };

    let sender = RawSocket::with_options(&ctx, EndpointType::PairV1, options()).expect("raw pair1");
    let receiver =
        RawSocket::with_options(&ctx, EndpointType::PairV1, options()).expect("raw pair1");
    sender.dial(&left_url).await.expect("dial");
    receiver.dial(&right_url).await.expect("dial");
    while left.pipe_count() < 1 || right.pipe_count() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // A message that has travelled once already: the device makes it two.
    sender
        .send(Message::from_body(
            [1u32.to_be_bytes().as_slice(), b"onward"].concat(),
        ))
        .await
        .expect("send");
    let (_, forwarded) = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("forwarded")
        .expect("a message");
    assert_eq!(
        &forwarded.body()[..4],
        2u32.to_be_bytes(),
        "the device incremented the hop count"
    );
    assert_eq!(&forwarded.body()[4..], b"onward");

    // And one that is already at the ceiling is dropped silently.
    sender
        .send(Message::from_body(
            [3u32.to_be_bytes().as_slice(), b"too far"].concat(),
        ))
        .await
        .expect("send");
    let nothing = tokio::time::timeout(Duration::from_millis(400), receiver.recv()).await;
    assert!(
        nothing.is_err(),
        "a message past the device's MAXTTL must be dropped and not forwarded"
    );
    assert_eq!(receiver.pipe_count(), 1, "and the pipe survives the drop");

    forwarding.abort();
}

/// Claim: a raw send writes the header it is given and nothing else, so a
/// tag stack an application built by hand arrives byte for byte.
#[tokio::test]
async fn a_raw_send_writes_exactly_what_it_is_given() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::with_options(&ctx, options()).expect("rep");
    let url = rep
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let raw_req = RawSocket::with_options(&ctx, EndpointType::Req, options()).expect("raw req");
    raw_req.dial(&url).await.expect("dial");

    // A stack with a forwarder id in it, invented by hand — raw mode means
    // nobody checks.
    let mut stack = Backtrace::direct(0x1234);
    stack.push_peer(0x0BAD);
    raw_req
        .send(Message::from_body(stack.encode_message(b"by hand")))
        .await
        .expect("send");

    let request = rep.recv().await.expect("the cooked replier");
    assert_eq!(request.body(), b"by hand");
    let (seen, _) = backtrace::decode(request.header(), 8).expect("the stack");
    assert_eq!(seen.peers, vec![0x0BAD]);
    assert_eq!(seen.id, 0x1234);
}

/// The payload limit of the boundary test, in octets.
const LIMIT: u64 = 1024;
/// The reply the stalled-client test asks for: big enough that a client which
/// never reads fills the kernel's buffers after a few of them.
const REPLY_BYTES: usize = 256 * 1024;
/// The request the backend answers late, and the one the honest client sends.
const HONEST: &[u8] = b"honest";
/// How long the request flood keeps writing.
const FLOOD: Duration = Duration::from_secs(3);

/// Bounds one await on a socket or a hand-driven peer, so that a stalled
/// operation fails the test instead of hanging it.
async fn bounded<T>(operation: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), operation)
        .await
        .expect("the operation finished within 10 s")
}

/// Polls `condition` every 5 ms until it holds, for at most 10 s.
async fn eventually(mut condition: impl FnMut() -> bool) {
    bounded(async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

/// The socket address a `tcp://` URL names.
fn address(url: &str) -> SocketAddr {
    url.trim_start_matches("tcp://")
        .parse()
        .expect("a tcp:// URL that names a socket address")
}

/// Completes the SP header exchange for a peer built by hand. Our eight octets
/// go first, as the mapping requires, and then the other side's, which has to
/// pair with the endpoint we claimed.
async fn handshake(mut stream: TcpStream, endpoint: EndpointType) -> TcpStream {
    bounded(stream.write_all(&ProtocolHeader::new(endpoint).encode()))
        .await
        .expect("our header");
    let mut theirs = [0u8; HEADER_LEN];
    bounded(stream.read_exact(&mut theirs))
        .await
        .expect("their header");
    let theirs = ProtocolHeader::decode(&theirs).expect("a well-formed header");
    assert!(
        theirs.accepts(endpoint),
        "a {:?} peer may not pair with {endpoint:?}",
        theirs.endpoint
    );
    stream
}

/// A hand-driven peer connected to `addr`, past its header exchange as
/// `endpoint`.
async fn peer(addr: SocketAddr, endpoint: EndpointType) -> TcpStream {
    let stream = bounded(TcpStream::connect(addr)).await.expect("connect");
    handshake(stream, endpoint).await
}

/// The two raw sockets of a device: the REP `front` that clients dial and the
/// REQ `back` that backends dial, each listening on an ephemeral loopback port.
/// Returns the sockets and the URL each one listens on.
async fn device_sockets(
    ctx: &Context,
    options: SocketOptions,
) -> (RawSocket, RawSocket, String, String) {
    let front = RawSocket::with_options(ctx, EndpointType::Rep, options.clone()).expect("raw rep");
    let back = RawSocket::with_options(ctx, EndpointType::Req, options).expect("raw req");
    let front_url = bounded(front.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let back_url = bounded(back.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    (front, back, front_url, back_url)
}

/// Runs the device between `front` and `back` on its own task, as an
/// application does.
fn spawn_device(
    front: &RawSocket,
    back: &RawSocket,
) -> JoinHandle<weida_nng::Result<raw::Forwarded>> {
    let (front, back) = (front.clone(), back.clone());
    tokio::spawn(async move { raw::device(&front, &back).await })
}

/// Writes requests over a hand-driven REQ connection for `for_how_long` and
/// never reads a reply. The deadline ends the writes even while the device has
/// stopped reading them, which is exactly when they would otherwise block.
async fn flood(addr: SocketAddr, for_how_long: Duration) {
    let (_replies_never_read, mut requests) = peer(addr, EndpointType::Req).await.into_split();
    let writing = async {
        for id in 1u32.. {
            let body = Backtrace::direct(id).encode_message(b"x");
            if requests.write_all(&message::encode(&body)).await.is_err() {
                break;
            }
        }
    };
    let _ = tokio::time::timeout(for_how_long, writing).await;
}

/// Claim: a client that stops reading its replies cannot stall the device. The
/// device queues each reply for the client it is for and discards what that
/// client's queue cannot hold, so the reply direction never waits on a TCP
/// window. A device that waited would park its one forwarding path behind the
/// first silent client, and every other client's request would stop with it.
#[tokio::test]
async fn a_client_that_stops_reading_cannot_stall_the_device() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (front, back, front_url, back_url) = device_sockets(&ctx, options()).await;
    let forwarding = spawn_device(&front, &back);

    // A backend that answers every request with a reply big enough to fill the
    // kernel buffers of a client that never reads.
    let backend = RepSocket::with_options(&ctx, options()).expect("rep");
    bounded(backend.dial(&back_url))
        .await
        .expect("dial the device");
    let serving = {
        let backend = backend.clone();
        tokio::spawn(async move {
            while backend.recv().await.is_ok() {
                if backend.send(vec![0u8; REPLY_BYTES]).await.is_err() {
                    break;
                }
            }
        })
    };
    eventually(|| back.pipe_count() == 1 && backend.pipe_count() == 1).await;

    // A client that stops reading. Its receive window is one kilobyte, so the
    // device's writes to it stall after a few replies, and each of its fifty
    // requests earns one of those replies.
    let socket = TcpSocket::new_v4().expect("a socket");
    socket
        .set_recv_buffer_size(1024)
        .expect("a small receive window");
    let stream = bounded(socket.connect(address(&front_url)))
        .await
        .expect("connect");
    let mut silent = handshake(stream, EndpointType::Req).await;
    for id in 1..=50u32 {
        let body = Backtrace::direct(id).encode_message(b"x");
        bounded(silent.write_all(&message::encode(&body)))
            .await
            .expect("a request");
    }

    // Long enough for the device to take all fifty before the honest client asks.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = ReqSocket::with_options(&ctx, options()).expect("req");
    bounded(client.dial(&front_url))
        .await
        .expect("dial the device");
    bounded(client.send(b"hello".to_vec())).await.expect("send");
    let reply = tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("the honest client gets its answer within 5 s")
        .expect("a reply");
    assert_eq!(reply.body().len(), REPLY_BYTES);

    // The silent client stays connected, unread, until the test is over.
    drop(silent);
    serving.abort();
    forwarding.abort();
}

/// Claim: a stream of requests from one client cannot starve the replies to
/// another. The device serves its two directions independently, so a request
/// queued in front of it does not hold back the backend's answers. A device
/// that favoured the request side would read no reply while any request is
/// queued, and a flood keeps requests queued for as long as it runs.
#[tokio::test]
async fn a_stream_of_requests_cannot_starve_the_replies() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (front, back, front_url, back_url) = device_sockets(&ctx, options()).await;
    let forwarding = spawn_device(&front, &back);

    // The honest request is answered half a second late, so its reply is still
    // on its way back while the flood runs.
    let backend = RepSocket::with_options(&ctx, options()).expect("rep");
    bounded(backend.dial(&back_url))
        .await
        .expect("dial the device");
    let serving = {
        let backend = backend.clone();
        tokio::spawn(async move {
            while let Ok(request) = backend.recv().await {
                if request.body() == HONEST {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                if backend.send(request.body().to_vec()).await.is_err() {
                    break;
                }
            }
        })
    };

    let client = ReqSocket::with_options(&ctx, options()).expect("req");
    bounded(client.dial(&front_url))
        .await
        .expect("dial the device");
    eventually(|| front.pipe_count() == 1 && back.pipe_count() == 1 && backend.pipe_count() == 1)
        .await;

    let flooding = tokio::spawn(flood(address(&front_url), FLOOD));
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The two seconds run from before the request is sent.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    bounded(client.send(HONEST.to_vec())).await.expect("send");
    let reply = tokio::time::timeout_at(deadline, client.recv())
        .await
        .expect("the honest reply came back within 2 s of the request")
        .expect("a reply");
    assert_eq!(reply.body(), HONEST);

    bounded(flooding).await.expect("the flood ran to its end");
    serving.abort();
    forwarding.abort();
}

/// Claim: a request whose tag stack is already as deep as the device's MAXTTL
/// costs that one request and nothing else. The stack has no room for the
/// device's own tag, so the device drops the request and keeps the sender's
/// pipe. The pipe from the device to the backend carries every client's
/// requests, so one client's over-deep stack must not close it.
#[tokio::test]
async fn a_stack_too_deep_for_the_backend_costs_the_message_and_not_the_backend_pipe() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (front, back, front_url, back_url) = device_sockets(&ctx, options()).await;
    let forwarding = spawn_device(&front, &back);

    let backend = RepSocket::new(&ctx).expect("rep with NNG's defaults");
    bounded(backend.dial(&back_url))
        .await
        .expect("dial the device");
    let serving = {
        let backend = backend.clone();
        tokio::spawn(async move {
            while let Ok(request) = backend.recv().await {
                if backend.send(request.body().to_vec()).await.is_err() {
                    break;
                }
            }
        })
    };
    eventually(|| back.pipe_count() == 1 && backend.pipe_count() == 1).await;

    // A REQ-type peer whose stack already holds MAXTTL forwarder tags, then the
    // request's own terminal tag, then a payload.
    let depth = u32::try_from(options().max_ttl).expect("a small MAXTTL");
    let mut stack = Backtrace::direct(7);
    for forwarder in 1..=depth {
        stack.push_peer(forwarder);
    }
    let mut hostile = peer(address(&front_url), EndpointType::Req).await;
    eventually(|| front.pipe_count() == 1).await;
    bounded(hostile.write_all(&message::encode(&stack.encode_message(b"too deep"))))
        .await
        .expect("the request");

    // Nothing comes back, and the sender's connection stays open.
    let mut reply = [0u8; 1];
    let answered = tokio::time::timeout(Duration::from_millis(300), hostile.read(&mut reply)).await;
    assert!(
        answered.is_err(),
        "a request the device dropped gets no reply"
    );
    assert_eq!(front.pipe_count(), 1, "the sender keeps its pipe");

    // The backend's pipe is untouched, and an ordinary request still gets through.
    assert_eq!(backend.pipe_count(), 1, "the pipe to the device survives");
    let client = ReqSocket::with_options(&ctx, options()).expect("req");
    bounded(client.dial(&front_url))
        .await
        .expect("dial the device");
    bounded(client.send(b"work".to_vec())).await.expect("send");
    let answer = tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("an ordinary request gets its answer")
        .expect("a reply");
    assert_eq!(answer.body(), b"work");

    serving.abort();
    forwarding.abort();
}

/// Claim: NNG_OPT_RECVMAXSZ bounds the payload once the tag stack is taken off.
/// A request whose payload is exactly the limit crosses the device and reaches
/// the backend, and one octet more is dropped at the device for its sender
/// alone. A declared length beyond what the transport admits closes only that
/// sender's pipe. Closing the backend's pipe for an oversized request would let
/// one sender cut every client off from that backend.
#[tokio::test]
async fn a_message_at_the_size_limit_survives_the_device_and_a_larger_one_costs_only_its_sender() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let limited = SocketOptions {
        recv_max_size: LIMIT,
        ..options()
    };
    let (front, back, front_url, back_url) = device_sockets(&ctx, limited.clone()).await;
    let forwarding = spawn_device(&front, &back);
    let backend = RepSocket::with_options(&ctx, limited).expect("rep");
    bounded(backend.dial(&back_url))
        .await
        .expect("dial the device");
    eventually(|| back.pipe_count() == 1 && backend.pipe_count() == 1).await;

    let client = ReqSocket::with_options(&ctx, options()).expect("req");
    bounded(client.dial(&front_url))
        .await
        .expect("dial the device");
    eventually(|| front.pipe_count() == 1).await;

    // Exactly the limit. The limit judges the payload, not the tag stack the
    // device adds in front of it.
    bounded(client.send(vec![7u8; LIMIT as usize]))
        .await
        .expect("send");
    let request = bounded(backend.recv()).await.expect("the request");
    assert_eq!(request.body().len(), LIMIT as usize);
    bounded(backend.send(b"got it".to_vec()))
        .await
        .expect("the reply");
    let reply = bounded(client.recv()).await.expect("a reply");
    assert_eq!(reply.body(), b"got it");
    assert_eq!(backend.pipe_count(), 1, "the backend keeps its pipe");

    // One octet over, sent by hand as a REQ-type peer.
    let mut oversize = peer(address(&front_url), EndpointType::Req).await;
    let body = Backtrace::direct(9).encode_message(&vec![8u8; LIMIT as usize + 1]);
    bounded(oversize.write_all(&message::encode(&body)))
        .await
        .expect("the request");
    eventually(|| front.pipe_count() == 2).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), backend.recv())
            .await
            .is_err(),
        "a payload over the limit does not reach the backend"
    );
    assert_eq!(backend.pipe_count(), 1, "the backend keeps its pipe");
    assert_eq!(front.pipe_count(), 2, "and the sender keeps its own");

    // An ordinary request still gets through afterwards.
    bounded(client.send(b"after".to_vec())).await.expect("send");
    let request = bounded(backend.recv()).await.expect("the request");
    assert_eq!(request.body(), b"after");
    bounded(backend.send(b"after".to_vec()))
        .await
        .expect("the reply");
    let reply = bounded(client.recv()).await.expect("a reply");
    assert_eq!(reply.body(), b"after");

    // A declared length far beyond the transport's allowance: only the sender's
    // own pipe goes.
    let mut hostile = peer(address(&front_url), EndpointType::Req).await;
    eventually(|| front.pipe_count() == 3).await;
    let mut declaration = Vec::new();
    message::encode_size(1 << 20, &mut declaration);
    bounded(hostile.write_all(&declaration))
        .await
        .expect("the declaration");
    eventually(|| front.pipe_count() == 2).await;
    assert_eq!(backend.pipe_count(), 1, "the backend keeps its pipe");

    forwarding.abort();
}

/// Claim: a device ends as soon as either of its sockets is closed, even while
/// both sides are still connected. A device that outlived one of its sockets
/// would keep forwarding into a socket nobody can reach, and its task would
/// never be joined.
#[tokio::test]
async fn a_device_ends_when_either_socket_closes() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    device_ends_after_closing(&ctx, true).await;
    device_ends_after_closing(&ctx, false).await;
}

/// One device with a client and a backend connected on both sides and one
/// exchange done through it, then its front socket or its back socket closed.
async fn device_ends_after_closing(ctx: &Context, close_front: bool) {
    let (front, back, front_url, back_url) = device_sockets(ctx, options()).await;
    let forwarding = spawn_device(&front, &back);
    let backend = RepSocket::with_options(ctx, options()).expect("rep");
    bounded(backend.dial(&back_url))
        .await
        .expect("dial the device");
    let client = ReqSocket::with_options(ctx, options()).expect("req");
    bounded(client.dial(&front_url))
        .await
        .expect("dial the device");
    eventually(|| front.pipe_count() == 1 && back.pipe_count() == 1 && backend.pipe_count() == 1)
        .await;

    bounded(client.send(b"ping".to_vec())).await.expect("send");
    let request = bounded(backend.recv()).await.expect("the request");
    bounded(backend.send(request.body().to_vec()))
        .await
        .expect("the reply");
    bounded(client.recv()).await.expect("a reply");

    if close_front {
        front.close();
    } else {
        back.close();
    }
    tokio::time::timeout(Duration::from_secs(2), forwarding)
        .await
        .expect("the device ends within 2 s of one of its sockets closing")
        .expect("the device task did not panic")
        .expect("the device ends without an error");
}

/// Claim: a device over **one** BUS socket does not reflect a message to the
/// pipe it arrived on, whichever handles of that socket it is given. The
/// exclusion is what BUS's re-broadcast owes the sender; a device that told
/// "one socket" from "two" by the address of the handle it was passed would
/// echo every message to its sender as soon as it was passed two clones.
#[tokio::test]
async fn a_device_over_one_bus_socket_does_not_echo_to_the_sender() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let hub = RawSocket::with_options(&ctx, EndpointType::Bus, options()).expect("raw bus");
    let url = bounded(hub.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let forwarding = spawn_device(&hub, &hub);

    let near = BusSocket::with_options(&ctx, options()).expect("bus");
    let far = BusSocket::with_options(&ctx, options()).expect("bus");
    bounded(near.dial(&url)).await.expect("dial");
    bounded(far.dial(&url)).await.expect("dial");
    eventually(|| hub.pipe_count() == 2).await;

    near.send(b"from near".to_vec()).expect("send");
    let heard = bounded(far.recv()).await.expect("far hears near");
    assert_eq!(heard.body(), b"from near");

    // What was reflected to `near` would be ahead of this in its pipe, which
    // carries the two in the order the device forwarded them.
    far.send(b"from far".to_vec()).expect("send");
    let heard = bounded(near.recv()).await.expect("near hears far");
    assert_eq!(
        heard.body(),
        b"from far",
        "near was handed its own message back"
    );
    forwarding.abort();
}
