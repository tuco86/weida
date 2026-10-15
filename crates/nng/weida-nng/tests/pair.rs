//! PAIR v0 and PAIR v1 against each other, and against raw octets.

use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weida_nng::{Context, ContextConfig, Error, Pair0Socket, Pair1Socket, SocketOptions, message};
use weida_sp::{EndpointType, HEADER_LEN, ProtocolHeader};

fn options() -> SocketOptions {
    SocketOptions {
        recv_timeout: Some(Duration::from_secs(5)),
        send_timeout: Some(Duration::from_secs(5)),
        handshake_timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

/// Claim: PAIR v0 carries no protocol header at all — a message on the
/// wire is the 64-bit length and the body, nothing between them (§3, §4).
#[tokio::test]
async fn pair_v0_puts_no_header_in_front_of_the_body() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let url = format!("tcp://127.0.0.1:{}", listener.local_addr().unwrap().port());

    let socket = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    let dialling = {
        let socket = socket.clone();
        let url = url.clone();
        tokio::spawn(async move { socket.dial(&url).await })
    };
    let (mut peer, _) = listener.accept().await.expect("accept");
    let mut theirs = [0u8; HEADER_LEN];
    peer.read_exact(&mut theirs).await.expect("their header");
    assert_eq!(theirs, [0x00, 0x53, 0x50, 0, 0x00, 0x10, 0, 0]);
    peer.write_all(&ProtocolHeader::new(EndpointType::PairV0).encode())
        .await
        .expect("write");
    dialling.await.expect("task").expect("dial");

    socket.send(b"hello".to_vec()).await.expect("send");
    let mut wire = [0u8; 13];
    peer.read_exact(&mut wire).await.expect("the message");
    assert_eq!(wire, *b"\x00\x00\x00\x00\x00\x00\x00\x05hello");
}

/// Claim: PAIR v1 prefixes the body with one 32-bit word, and the count
/// this library originates is **one** — the RFC's value, and the value a
/// running NNG 1.4.0-rc.0 was measured to send, against a reading of
/// NNG's source that suggested zero (§3, §31). This is the vector the doc
/// comment names.
#[tokio::test]
async fn pair_v1_originates_the_measured_hop_count() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let url = format!("tcp://127.0.0.1:{}", listener.local_addr().unwrap().port());

    let socket = Pair1Socket::with_options(&ctx, options()).expect("pair1");
    let dialling = {
        let socket = socket.clone();
        let url = url.clone();
        tokio::spawn(async move { socket.dial(&url).await })
    };
    let (mut peer, _) = listener.accept().await.expect("accept");
    let mut theirs = [0u8; HEADER_LEN];
    peer.read_exact(&mut theirs).await.expect("their header");
    assert_eq!(theirs, [0x00, 0x53, 0x50, 0, 0x00, 0x11, 0, 0]);
    peer.write_all(&ProtocolHeader::new(EndpointType::PairV1).encode())
        .await
        .expect("write");
    dialling.await.expect("task").expect("dial");

    socket.send(b"hi".to_vec()).await.expect("send");
    let mut wire = [0u8; 14];
    peer.read_exact(&mut wire).await.expect("the message");
    assert_eq!(
        wire, *b"\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x01hi",
        "six body octets: four of hop count, then the payload"
    );

    // And both readings of the initial count are accepted on the way in:
    // NNG's source reading and the RFC's one.
    for count in [0u32, 1] {
        let mut body = count.to_be_bytes().to_vec();
        body.extend_from_slice(b"back");
        peer.write_all(&message::Message::from_body(body).encode())
            .await
            .expect("write");
        let received = socket.recv().await.expect("delivered");
        assert_eq!(received.body(), b"back");
        assert_eq!(
            received.header(),
            count.to_be_bytes(),
            "the count travelled with the message"
        );
    }
    assert_eq!(socket.dropped_over_ttl(), 0);
}

/// Claim: a hop count past the local `NNG_OPT_MAXTTL` costs the message
/// and not the pipe, and nothing is sent back — the sender sees only the
/// absence of an answer (§8).
#[tokio::test]
async fn a_message_past_the_local_hop_ceiling_is_dropped_and_the_pipe_kept() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let url = format!("tcp://127.0.0.1:{}", listener.local_addr().unwrap().port());

    let socket = Pair1Socket::with_options(
        &ctx,
        SocketOptions {
            max_ttl: 4,
            ..options()
        },
    )
    .expect("pair1");
    assert_eq!(socket.max_ttl(), 4);
    let dialling = {
        let socket = socket.clone();
        let url = url.clone();
        tokio::spawn(async move { socket.dial(&url).await })
    };
    let (mut peer, _) = listener.accept().await.expect("accept");
    let mut theirs = [0u8; HEADER_LEN];
    peer.read_exact(&mut theirs).await.expect("their header");
    peer.write_all(&ProtocolHeader::new(EndpointType::PairV1).encode())
        .await
        .expect("write");
    dialling.await.expect("task").expect("dial");

    // One message past the ceiling, then one within it.
    for count in [9u32, 2] {
        let mut body = count.to_be_bytes().to_vec();
        body.extend_from_slice(b"payload");
        peer.write_all(&message::Message::from_body(body).encode())
            .await
            .expect("write");
    }

    let delivered = socket.recv().await.expect("the one within the ceiling");
    assert_eq!(delivered.body(), b"payload");
    assert_eq!(delivered.header(), 2u32.to_be_bytes());
    assert_eq!(socket.dropped_over_ttl(), 1, "the far one was dropped");
    assert_eq!(socket.pipe_count(), 1, "and the pipe survived it");

    // Nothing was sent back about it: the peer sees no traffic at all.
    let mut nothing = [0u8; 1];
    let quiet = tokio::time::timeout(Duration::from_millis(200), peer.read(&mut nothing)).await;
    assert!(quiet.is_err(), "a drop must be silent");
}

/// Claim: a paired socket refuses a second connection while one is live,
/// and takes one again once the first has gone (§4).
#[tokio::test]
async fn a_paired_socket_admits_one_peer_at_a_time() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    let url = server
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let first = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    first.dial(&url).await.expect("the first peer");
    while server.pipe_count() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let second = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    // The TCP connection is accepted and then dropped: the refusal is a
    // close and nothing else, so the intruder learns of it by losing the
    // connection rather than by any frame (§6).
    let refused = second.dial(&url).await.unwrap_err();
    assert!(
        matches!(
            refused,
            Error::ECONNRESET(_) | Error::ECONNABORTED(_) | Error::ETIMEDOUT(_)
        ),
        "{refused:?}"
    );
    assert_eq!(second.pipe_count(), 0, "the second pairing was refused");
    assert_eq!(server.pipe_count(), 1, "and the first one is untouched");

    // The pair still works, which is the point of refusing the intruder.
    first.send(b"still here".to_vec()).await.expect("send");
    assert_eq!(server.recv().await.expect("recv").body(), b"still here");

    // Once the first peer goes, the slot is free again.
    first.close();
    for _ in 0..200 {
        if server.pipe_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let third = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    third.dial(&url).await.expect("dial");
    for _ in 0..200 {
        if server.pipe_count() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(server.pipe_count(), 1, "a freed pair accepts a new peer");
}

/// Claim: PAIR blocks rather than discarding when no peer can receive, and
/// `NNG_OPT_SENDTIMEO` is what turns that into an error (§5).
#[tokio::test]
async fn a_pair_send_with_no_peer_waits_and_then_times_out() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let socket = Pair1Socket::with_options(
        &ctx,
        SocketOptions {
            send_timeout: Some(Duration::from_millis(100)),
            ..options()
        },
    )
    .expect("pair1");
    let err = socket.send(b"nobody".to_vec()).await.unwrap_err();
    assert!(matches!(err, Error::ETIMEDOUT(_)), "{err:?}");
}

/// Claim: two sockets of this library pair up and talk in both directions,
/// v0 and v1 alike.
#[tokio::test]
async fn a_pair_talks_both_ways() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    let server0 = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    let url0 = server0
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let client0 = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    client0.dial(&url0).await.expect("dial");
    client0.send(b"ping".to_vec()).await.expect("send");
    assert_eq!(server0.recv().await.expect("recv").body(), b"ping");
    server0.send(b"pong".to_vec()).await.expect("send");
    assert_eq!(client0.recv().await.expect("recv").body(), b"pong");

    let server1 = Pair1Socket::with_options(&ctx, options()).expect("pair1");
    let url1 = server1
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let client1 = Pair1Socket::with_options(&ctx, options()).expect("pair1");
    client1.dial(&url1).await.expect("dial");
    client1.send(b"ping".to_vec()).await.expect("send");
    let request = server1.recv().await.expect("recv");
    assert_eq!(request.body(), b"ping");
    assert_eq!(request.header(), [0, 0, 0, 1], "one hop so far: ours");
    server1.send(b"pong".to_vec()).await.expect("send");
    assert_eq!(client1.recv().await.expect("recv").body(), b"pong");
}

/// Bounds one await on the socket or on a peer, so that a stall fails the
/// test rather than hanging it.
async fn bounded<T>(step: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), step)
        .await
        .expect("the step finished within 5 s")
}

/// Claim: `NNG_OPT_RECVTIMEO` bounds the whole receive, not each message in
/// it. A peer that keeps sending messages past the hop ceiling, which the
/// socket drops without a word back, cannot extend the receive: it ends with
/// `NNG_ETIMEDOUT` at its own deadline. A clock restarted for every dropped
/// message would let a chatty peer hold the receive open for as long as it
/// keeps talking.
#[tokio::test]
async fn a_stream_of_over_ttl_messages_cannot_keep_a_receive_alive_past_its_timeout() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let pair = Pair1Socket::with_options(
        &ctx,
        SocketOptions {
            recv_timeout: Some(Duration::from_millis(300)),
            ..options()
        },
    )
    .expect("pair1");
    let url = bounded(pair.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let addr = url.trim_start_matches("tcp://").to_owned();

    let mut peer = bounded(tokio::net::TcpStream::connect(&addr))
        .await
        .expect("connect");
    bounded(peer.write_all(&ProtocolHeader::new(EndpointType::PairV1).encode()))
        .await
        .expect("our header");
    let mut theirs = [0u8; HEADER_LEN];
    bounded(peer.read_exact(&mut theirs))
        .await
        .expect("their header");

    // Twenty messages a hundred milliseconds apart: two seconds of traffic
    // against a 300 ms receive, every message past the eight-hop ceiling.
    let sender = tokio::spawn(async move {
        let frame = weida_sp::message::encode(&weida_sp::pair::encode(200, b"payload"));
        for _ in 0..20 {
            let written =
                tokio::time::timeout(Duration::from_secs(1), peer.write_all(&frame)).await;
            if !matches!(written, Ok(Ok(()))) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let started = Instant::now();
    let outcome = bounded(pair.recv()).await;
    let elapsed = started.elapsed();
    sender.abort();

    assert!(
        matches!(outcome, Err(Error::ETIMEDOUT(_))),
        "a stream of dropped messages must not hold the receive open: {outcome:?}"
    );
    assert!(
        (Duration::from_millis(250)..Duration::from_millis(1500)).contains(&elapsed),
        "the receive ends at its own 300 ms deadline, not when the stream stops: {elapsed:?}"
    );
    assert!(
        pair.dropped_over_ttl() >= 1,
        "the over-ceiling messages were dropped, not delivered"
    );
}
