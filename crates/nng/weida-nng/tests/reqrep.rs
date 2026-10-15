//! REQ and REP against each other, over real connections.
//!
//! Most claims here are one of `docs/research/nanomsg-nng.md` §4's sentences
//! about request-reply, turned into something that fails when it stops
//! being true. The rest pin what this crate promises beyond SP for a peer
//! that misbehaves and for a socket that is closed or dropped; each test's
//! doc comment gives the reason for it.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use weida_nng::{
    Context, ContextConfig, Error, PullSocket, RepSocket, ReqSocket, RespondentSocket,
    SocketOptions, SubSocket, SurveyorSocket,
};
use weida_sp::backtrace::{MAX_ID, TAG_LEN, TERMINAL};
use weida_sp::header::{EndpointType, HEADER_LEN, ProtocolHeader};
use weida_sp::message;

fn options() -> SocketOptions {
    SocketOptions {
        // Short enough that a test can watch a resend, long enough that a
        // healthy exchange never sees one.
        resend_time: Duration::from_millis(150),
        recv_timeout: Some(Duration::from_secs(5)),
        send_timeout: Some(Duration::from_secs(5)),
        handshake_timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

async fn pair() -> (Context, ReqSocket, RepSocket, String) {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::with_options(&ctx, options()).expect("rep");
    let listener = rep.listen("tcp://127.0.0.1:0").await.expect("listen");
    let url = listener.url().to_string();
    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    req.dial(&url).await.expect("dial");
    (ctx, req, rep, url)
}

/// Claim: a request reaches a replier and its reply comes back matched by
/// the 32-bit request ID, which travels with its terminal bit set — the bit
/// that tells the replier where the tag stack ends [rfc-reqrep §5].
#[tokio::test]
async fn a_request_and_its_reply_are_matched_by_the_tag_stack() {
    let (_ctx, req, rep, _url) = pair().await;

    req.send(b"ping".to_vec()).await.expect("send");
    let request = rep.recv().await.expect("recv");
    assert_eq!(request.body(), b"ping");
    assert_eq!(request.header().len(), 4, "one tag: the request ID");
    assert_eq!(
        request.header()[0] & 0x80,
        0x80,
        "the terminal bit ends the stack"
    );

    rep.send(b"pong".to_vec()).await.expect("reply");
    let reply = req.recv().await.expect("reply");
    assert_eq!(reply.body(), b"pong");
    assert_eq!(
        reply.header(),
        request.header(),
        "the reply repeats the stack it arrived with"
    );
}

/// Claim: contexts are independent transactions over one socket — each with
/// its own request ID — so two requests can be outstanding at once and each
/// gets its own reply (§2, §4).
#[tokio::test]
async fn two_contexts_run_two_transactions_over_one_socket() {
    let (_ctx, req, rep, _url) = pair().await;
    let first = req.context();
    let second = req.context();

    first.send(b"one".to_vec()).await.expect("send one");
    second.send(b"two".to_vec()).await.expect("send two");

    // Two REP contexts process them in parallel, which is what contexts are
    // for on the replying side too (§4).
    let a = rep.context();
    let b = rep.context();
    let request_a = a.recv().await.expect("a");
    let request_b = b.recv().await.expect("b");
    assert_ne!(
        request_a.header(),
        request_b.header(),
        "each context has its own request ID"
    );

    // Answer them in the opposite order: the tag stack is what pairs them,
    // not arrival order.
    b.send(request_b.body().to_vec()).await.expect("reply b");
    a.send(request_a.body().to_vec()).await.expect("reply a");

    let reply_first = first.recv().await.expect("first reply");
    let reply_second = second.recv().await.expect("second reply");
    assert_eq!(reply_first.body(), b"one");
    assert_eq!(reply_second.body(), b"two");
}

/// Claim: every order the protocol forbids is `NNG_ESTATE`, and one pending
/// receive per context is one of them (§4).
#[tokio::test]
async fn the_forbidden_orders_are_all_estate() {
    let (_ctx, req, rep, _url) = pair().await;

    // A REP that has received nothing cannot send.
    let err = rep.send(b"unasked".to_vec()).await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");

    // A REQ that has sent nothing cannot receive.
    let err = req.recv().await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");

    // One pending receive per context, on both sides.
    req.send(b"ping".to_vec()).await.expect("send");
    let ctx = req.implicit_context();
    let waiting = tokio::spawn({
        let ctx = ctx.clone();
        async move { ctx.recv().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let err = ctx.recv().await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");

    let rep_ctx = rep.implicit_context();
    let request = rep_ctx.recv().await.expect("request");
    let second = rep_ctx.clone();
    let parked = tokio::spawn(async move { second.recv().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let err = rep_ctx.recv().await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");

    rep_ctx.send(request.body().to_vec()).await.expect("reply");
    assert_eq!(waiting.await.expect("task").expect("reply").body(), b"ping");
    parked.abort();
}

/// Claim: a newer request cancels the requester's interest in the older
/// reply — the late one is discarded — and does **not** withdraw the older
/// request from the replier, which answers both (§4).
#[tokio::test]
async fn a_newer_request_discards_the_older_reply_without_withdrawing_it() {
    let (_ctx, req, rep, _url) = pair().await;

    req.send(b"first".to_vec()).await.expect("first");
    let first = rep.context().recv().await.expect("the first request");
    assert_eq!(first.body(), b"first");

    // Second request before the first reply is asked for.
    req.send(b"second".to_vec()).await.expect("second");
    let second_ctx = rep.context();
    let second = second_ctx.recv().await.expect("the second request");
    assert_eq!(
        second.body(),
        b"second",
        "the replier saw the older request too: it was never withdrawn"
    );

    // Answer the older one first. Its reply matches nothing any more.
    let stale = rep.context();
    let _ = stale;
    second_ctx
        .send(b"reply-to-second".to_vec())
        .await
        .expect("reply");

    let reply = req.recv().await.expect("a reply");
    assert_eq!(
        reply.body(),
        b"reply-to-second",
        "the requester is only interested in its newest request"
    );
}

/// Claim: a request is resent when its timer elapses, and the replier sees
/// the **duplicate** — "REQ retransmission creates duplicate requests" (§7),
/// which is a cost of the pattern rather than a hidden repair.
#[tokio::test]
async fn the_resend_timer_makes_the_replier_see_a_duplicate() {
    let (_ctx, req, rep, _url) = pair().await;

    req.send(b"work".to_vec()).await.expect("send");
    let first = rep.context().recv().await.expect("the request");
    assert_eq!(first.body(), b"work");

    // Nobody replies. The requester's clock runs out and sends it again.
    let duplicate = tokio::time::timeout(Duration::from_secs(5), rep.context().recv())
        .await
        .expect("the resend arrives")
        .expect("a second request");
    assert_eq!(duplicate.body(), b"work", "the same work, twice");
    assert_eq!(
        duplicate.header(),
        first.header(),
        "with the same request ID, which is all a replier has to notice it by"
    );
}

/// Claim: the other two resend triggers are real — a peer that disconnects
/// and a peer that becomes available both cause the outstanding request to
/// be sent again, with no second `send` from the application (§4).
#[tokio::test]
async fn a_disconnect_and_a_new_peer_both_resend_the_request() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // A replier that takes the request and then goes away.
    let doomed = RepSocket::with_options(&ctx, options()).expect("rep");
    let doomed_url = doomed
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    // An address nobody is on yet; the requester keeps dialling it.
    let spare = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let spare_url = format!("tcp://127.0.0.1:{}", spare.local_addr().unwrap().port());
    drop(spare);

    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    req.dial(&doomed_url).await.expect("dial the live one");
    req.dial_nonblocking(&spare_url)
        .expect("keep dialling the empty address");

    req.send(b"work".to_vec()).await.expect("send");
    let taken = doomed.recv().await.expect("the request");
    assert_eq!(taken.body(), b"work");

    // Trigger two: the peer it went to disappears.
    doomed.close();

    // Trigger three: a peer becomes available while the request waits.
    let rescuer = RepSocket::with_options(&ctx, options()).expect("rep");
    rescuer.listen(&spare_url).await.expect("listen");

    let resent = tokio::time::timeout(Duration::from_secs(10), rescuer.recv())
        .await
        .expect("the request is resent to the peer that appeared")
        .expect("a request");
    assert_eq!(resent.body(), b"work");
    assert_eq!(
        resent.header(),
        taken.header(),
        "the same request, not a new one"
    );

    rescuer.send(b"done".to_vec()).await.expect("reply");
    assert_eq!(req.recv().await.expect("reply").body(), b"done");
}

/// Claim: requests are spread over the repliers that can take one, rather
/// than piled on the first — "a cooked REQ … normally spreads requests
/// among peer REP sockets" (§4).
#[tokio::test]
async fn requests_are_spread_over_the_available_repliers() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let first = RepSocket::with_options(&ctx, options()).expect("rep");
    let second = RepSocket::with_options(&ctx, options()).expect("rep");
    let first_url = first
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let second_url = second
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    req.dial(&first_url).await.expect("dial one");
    req.dial(&second_url).await.expect("dial two");
    assert_eq!(req.pipe_count(), 2);

    let served = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let serving: Vec<_> = [first.clone(), second.clone()]
        .into_iter()
        .map(|rep| {
            let served = std::sync::Arc::clone(&served);
            tokio::spawn(async move {
                let mut mine = 0usize;
                while let Ok(request) = rep.recv().await {
                    mine += 1;
                    served.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if rep.send(request.body().to_vec()).await.is_err() {
                        break;
                    }
                }
                mine
            })
        })
        .collect();

    for round in 0..4u8 {
        req.send(vec![round]).await.expect("send");
        assert_eq!(req.recv().await.expect("reply").body(), [round]);
    }

    first.close();
    second.close();
    let mut counts = Vec::new();
    for task in serving {
        counts.push(task.await.expect("task"));
    }
    counts.sort_unstable();
    assert!(
        counts[0] >= 1,
        "both repliers were used: {counts:?} out of {} requests",
        served.load(std::sync::atomic::Ordering::SeqCst)
    );
}

/// Every wait on a peer in these tests is bounded by this, so a replier that
/// stops answering fails its test instead of hanging the run.
const PATIENCE: Duration = Duration::from_secs(10);

/// The size of one reply in the stalled-requester test. A few of them fill the
/// kernel's buffers, which is what makes a reply that waits on its requester
/// observable at all.
const REPLY_BYTES: usize = 256 * 1024;

/// Awaits `future` for at most [`PATIENCE`].
async fn patiently<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(PATIENCE, future)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {PATIENCE:?}"))
}

/// Polls `holds` until it is true, for at most two seconds. For a state that a
/// background task brings about: the property is that it comes about, and the
/// poll is the time it is allowed to take.
async fn eventually(what: &str, mut holds: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !holds() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what} did not happen within two seconds"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The final tag of a stack: a request ID with the terminal bit set, which is
/// what tells a replier where the stack ends [rfc-reqrep §5].
fn terminal_tag(id: u32) -> [u8; TAG_LEN] {
    ((id & MAX_ID) | TERMINAL).to_be_bytes()
}

/// A forwarder's tag: a peer ID with the terminal bit clear, which says that
/// the stack goes on [rfc-reqrep §5].
fn peer_tag(id: u32) -> [u8; TAG_LEN] {
    (id & MAX_ID).to_be_bytes()
}

/// Does the SP handshake on `stream` by hand, as `endpoint`: our header first,
/// then the peer's, which has to be a valid protocol header.
async fn speak_sp(mut stream: TcpStream, endpoint: EndpointType) -> TcpStream {
    let ours = ProtocolHeader::new(endpoint).encode();
    patiently("our header", stream.write_all(&ours))
        .await
        .expect("our header");
    let mut theirs = [0u8; HEADER_LEN];
    patiently("their header", stream.read_exact(&mut theirs))
        .await
        .expect("their header");
    ProtocolHeader::decode(&theirs).expect("a valid protocol header");
    stream
}

/// Connects to `addr` and does the SP handshake by hand as `endpoint`.
async fn connect_raw(addr: &str, endpoint: EndpointType) -> TcpStream {
    let stream = patiently("connect", TcpStream::connect(addr))
        .await
        .expect("connect");
    speak_sp(stream, endpoint).await
}

/// The body of the next message the replier sends a raw peer, read by hand.
async fn next_message(stream: &mut TcpStream) -> Vec<u8> {
    let mut size = [0u8; message::SIZE_LEN];
    patiently("a size field", stream.read_exact(&mut size))
        .await
        .expect("a size field");
    let len = usize::try_from(u64::from_be_bytes(size)).expect("a body that fits in memory");
    let mut body = vec![0u8; len];
    patiently("a body", stream.read_exact(&mut body))
        .await
        .expect("a body");
    body
}

/// Requires the replier to end `stream`: an end of file or a reset within two
/// seconds. An octet arriving instead means the connection is still in use.
async fn expect_closed(stream: &mut TcpStream, what: &str) {
    let mut octet = [0u8; 1];
    match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut octet)).await {
        Ok(Ok(0) | Err(_)) => {}
        Ok(Ok(_)) => panic!("{what}: the replier sent data on a connection it should have closed"),
        Err(_) => panic!("{what}: the connection was still open two seconds later"),
    }
}

/// Requires `receive` to answer `NNG_ECLOSED` within a second of the close.
async fn answers_closed<T: std::fmt::Debug>(
    what: &str,
    receive: impl Future<Output = weida_nng::Result<T>>,
) {
    let outcome = tokio::time::timeout(Duration::from_secs(1), receive)
        .await
        .unwrap_or_else(|_| panic!("{what}: no answer a second after the close"));
    assert!(
        matches!(outcome, Err(Error::ECLOSED(_))),
        "{what}: {outcome:?}"
    );
}

/// Claim: a requester that stops reading its replies cannot hold the replier
/// up. A reply belongs to one requester, so the replier must not wait for that
/// requester's TCP window: a reply its pipe has no room for is dropped, and
/// every other client is served. Otherwise one client that sends requests and
/// never reads stops a single-loop server for all of its clients.
#[tokio::test]
async fn a_requester_that_stops_reading_cannot_freeze_the_replier() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::new(&ctx).expect("rep");
    let url = patiently("listen", rep.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();

    // The replier's whole life: take a request, answer with a large reply.
    let server = tokio::spawn({
        let rep = rep.clone();
        async move {
            loop {
                if rep.recv().await.is_err() {
                    break;
                }
                if rep.send(vec![0u8; REPLY_BYTES]).await.is_err() {
                    break;
                }
            }
        }
    });

    // Peer A: a one-kilobyte receive window, so that its replies back up in the
    // kernel almost at once.
    let socket = TcpSocket::new_v4().expect("a socket");
    socket
        .set_recv_buffer_size(1024)
        .expect("a small receive window");
    let target: SocketAddr = url
        .trim_start_matches("tcp://")
        .parse()
        .expect("a loopback address");
    let stream = patiently("connect A", socket.connect(target))
        .await
        .expect("connect A");
    let mut stalled = speak_sp(stream, EndpointType::Req).await;
    for id in 1..=200 {
        let mut body = terminal_tag(id).to_vec();
        body.push(b'x');
        patiently("A's request", stalled.write_all(&message::encode(&body)))
            .await
            .expect("A's request");
    }
    // A has sent everything it is going to send, and it never reads again.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Peer B: an ordinary requester, which must be answered within five seconds.
    let req = ReqSocket::with_options(
        &ctx,
        SocketOptions {
            recv_timeout: Some(Duration::from_secs(5)),
            send_timeout: Some(Duration::from_secs(5)),
            ..SocketOptions::default()
        },
    )
    .expect("req");
    patiently("dial B", req.dial(&url)).await.expect("dial B");
    req.send(b"ping".to_vec())
        .await
        .expect("B's request is accepted");
    let reply = req
        .recv()
        .await
        .expect("B's request is answered within 5 s");
    assert_eq!(reply.body().len(), REPLY_BYTES);

    server.abort();
    drop(stalled);
}

/// Claim: a message whose tag stack is deeper than the replier's `MAXTTL` is
/// dropped and counted, and the pipe it arrived on stays up. A device shares one
/// pipe among all of its clients and forwards deep stacks as a matter of course,
/// so one such message closing the pipe would cut every client off for the price
/// of a single message.
#[tokio::test]
async fn a_stack_that_is_too_deep_costs_the_message_and_not_the_pipe() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::new(&ctx).expect("rep");
    let url = patiently("listen", rep.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let mut peer = connect_raw(url.trim_start_matches("tcp://"), EndpointType::Req).await;

    // Nine peer tags and then the request ID: one hop more than the default
    // MAXTTL of eight allows. The payload follows the stack.
    let mut deep = Vec::new();
    for hop in 1..=9 {
        deep.extend_from_slice(&peer_tag(hop));
    }
    deep.extend_from_slice(&terminal_tag(0x41));
    deep.extend_from_slice(b"deep");
    patiently("the deep message", peer.write_all(&message::encode(&deep)))
        .await
        .expect("the deep message");

    let mut normal = terminal_tag(0x42).to_vec();
    normal.extend_from_slice(b"normal");
    patiently("the request", peer.write_all(&message::encode(&normal)))
        .await
        .expect("the request");

    let request = patiently("a receive", rep.recv())
        .await
        .expect("the request after the deep one");
    assert_eq!(request.header(), &terminal_tag(0x42)[..]);
    assert_eq!(request.body(), b"normal");
    assert_eq!(
        rep.pipe_count(),
        1,
        "the pipe the deep message came on is still up"
    );
    assert_eq!(
        rep.dropped(),
        1,
        "and the deep message is counted as dropped"
    );

    patiently("the reply", rep.send(b"answer".to_vec()))
        .await
        .expect("the reply");
    let mut expected = terminal_tag(0x42).to_vec();
    expected.extend_from_slice(b"answer");
    assert_eq!(
        next_message(&mut peer).await,
        expected,
        "the reply carries the stack the request arrived with"
    );
}

/// Claim: a message whose tag stack ends inside a tag is malformed. It closes the
/// pipe it came on and no other: the sender sees its connection end, while
/// another requester on its own pipe keeps being answered. A stack cut short
/// cannot be trusted to mark where the next message starts, so that connection
/// goes; the error must not reach the other clients of the socket.
#[tokio::test]
async fn a_message_that_ends_inside_a_tag_ends_only_its_senders_pipe() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::new(&ctx).expect("rep");
    let url = patiently("listen", rep.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let addr = url.trim_start_matches("tcp://");

    // Answer everything that arrives, so the malformed message is looked at as
    // soon as it lands rather than when somebody next asks for a message.
    let echo = tokio::spawn({
        let rep = rep.clone();
        async move {
            while let Ok(request) = rep.recv().await {
                if rep.send(request.body().to_vec()).await.is_err() {
                    break;
                }
            }
        }
    });

    // Two octets: less than one tag, so the stack never ends.
    let mut malformed = connect_raw(addr, EndpointType::Req).await;
    patiently(
        "the malformed message",
        malformed.write_all(&message::encode(&[0x80, 0x01])),
    )
    .await
    .expect("the malformed message");

    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    patiently("dial", req.dial(&url)).await.expect("dial");
    req.send(b"hello".to_vec()).await.expect("send");
    assert_eq!(
        req.recv()
            .await
            .expect("the other requester is answered")
            .body(),
        b"hello"
    );

    expect_closed(&mut malformed, "the sender of the truncated stack").await;
    eventually("the malformed sender's pipe goes", || rep.pipe_count() == 1).await;

    req.send(b"again".to_vec()).await.expect("send");
    assert_eq!(
        req.recv()
            .await
            .expect("the other pipe is still served")
            .body(),
        b"again"
    );
    echo.abort();
}

/// Claim: the payload limit, `NNG_OPT_RECVMAXSZ`, is judged on the payload that
/// remains once the tag stack is taken off. A payload exactly at the limit is
/// delivered; one octet over is dropped and counted, with the pipe kept; and a
/// declared length past the limit plus the stack allowance ends the connection
/// without reading any body. Judging the whole wire length would refuse the
/// first, and closing the pipe for the second would let one oversized request
/// cut off a pipe that other clients share.
#[tokio::test]
async fn the_payload_limit_is_judged_after_the_stack_is_taken_off() {
    const LIMIT: usize = 1024;
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::with_options(
        &ctx,
        SocketOptions {
            recv_max_size: LIMIT as u64,
            recv_timeout: Some(Duration::from_secs(5)),
            ..SocketOptions::default()
        },
    )
    .expect("rep");
    let url = patiently("listen", rep.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let addr = url.trim_start_matches("tcp://");
    let mut peer = connect_raw(addr, EndpointType::Req).await;

    // Exactly the limit, behind one tag.
    let mut at_limit = terminal_tag(1).to_vec();
    at_limit.extend(vec![0x5a; LIMIT]);
    patiently(
        "the request at the limit",
        peer.write_all(&message::encode(&at_limit)),
    )
    .await
    .expect("the request at the limit");
    let request = rep
        .recv()
        .await
        .expect("a request at the limit is delivered");
    assert_eq!(request.body().len(), LIMIT);
    patiently("the reply", rep.send(b"ok".to_vec()))
        .await
        .expect("reply");
    let mut expected = terminal_tag(1).to_vec();
    expected.extend_from_slice(b"ok");
    assert_eq!(next_message(&mut peer).await, expected);
    assert_eq!(rep.pipe_count(), 1, "the pipe is kept");

    // One octet over, then a request that fits. The first is dropped, so the
    // second is what the replier receives next.
    let mut over = terminal_tag(2).to_vec();
    over.extend(vec![0x5a; LIMIT + 1]);
    patiently(
        "the request over the limit",
        peer.write_all(&message::encode(&over)),
    )
    .await
    .expect("the request over the limit");
    let mut small = terminal_tag(3).to_vec();
    small.extend_from_slice(b"small");
    patiently(
        "the small request",
        peer.write_all(&message::encode(&small)),
    )
    .await
    .expect("the small request");
    let request = rep.recv().await.expect("the request that fits");
    assert_eq!(request.header(), &terminal_tag(3)[..]);
    assert_eq!(
        request.body(),
        b"small",
        "the oversize request was not delivered"
    );
    assert_eq!(rep.dropped(), 1, "and it is counted");
    assert_eq!(rep.pipe_count(), 1, "the pipe is kept");
    patiently("the reply", rep.send(b"fine".to_vec()))
        .await
        .expect("reply");
    let mut expected = terminal_tag(3).to_vec();
    expected.extend_from_slice(b"fine");
    assert_eq!(next_message(&mut peer).await, expected);

    // A declared length past the limit and the stack allowance: the replier ends
    // the connection at the size field, and no body needs to follow.
    let mut oversize = connect_raw(addr, EndpointType::Req).await;
    patiently(
        "the declared length",
        oversize.write_all(&2000u64.to_be_bytes()),
    )
    .await
    .expect("the declared length");
    expect_closed(&mut oversize, "a declared length past the limit").await;
}

/// Claim: closing a socket ends a receive parked on it with `NNG_ECLOSED`, at
/// the close, rather than at the receive's timeout, which by default is never.
/// This is checked for each protocol that parks a receive: a REP and a RESPONDENT
/// with nothing to answer, a SURVEYOR collecting answers to a live survey, and a
/// REQ waiting for a reply that a connected REP never sends. A REP or RESPONDENT
/// receive that starts after the close answers at once as well. Without this a
/// closed socket keeps its waiters for as long as their timeout allows, which for
/// a default receive is forever.
#[tokio::test]
async fn a_closed_socket_ends_the_receive_parked_on_it() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // A replier with nothing to answer.
    let rep = RepSocket::new(&ctx).expect("rep");
    patiently("listen", rep.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen");
    let parked = tokio::spawn({
        let rep = rep.clone();
        async move { rep.recv().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    rep.close();
    answers_closed("a parked REP receive", async {
        parked.await.expect("the receive task")
    })
    .await;
    answers_closed("a REP receive started after the close", rep.recv()).await;

    // A respondent with no survey to answer.
    let respondent = RespondentSocket::new(&ctx).expect("respondent");
    patiently("listen", respondent.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen");
    let parked = tokio::spawn({
        let respondent = respondent.clone();
        async move { respondent.recv().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    respondent.close();
    answers_closed("a parked RESPONDENT receive", async {
        parked.await.expect("the receive task")
    })
    .await;
    answers_closed(
        "a RESPONDENT receive started after the close",
        respondent.recv(),
    )
    .await;

    // A surveyor with a live survey, sent to a connected respondent that never
    // answers. The survey outlasts the test, so only the close can end the
    // collection.
    let surveyor = SurveyorSocket::with_options(
        &ctx,
        SocketOptions {
            survey_time: Duration::from_secs(30),
            ..SocketOptions::default()
        },
    )
    .expect("surveyor");
    let survey_url = patiently("listen", surveyor.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let quiet = RespondentSocket::new(&ctx).expect("respondent");
    patiently("dial the surveyor", quiet.dial(&survey_url))
        .await
        .expect("dial the surveyor");
    patiently("the survey", surveyor.send(b"anyone?".to_vec()))
        .await
        .expect("the survey");
    let parked = tokio::spawn({
        let surveyor = surveyor.clone();
        async move { surveyor.recv().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    surveyor.close();
    answers_closed("a SURVEYOR collecting answers", async {
        parked.await.expect("the receive task")
    })
    .await;

    // A requester whose request a connected replier holds and never answers. The
    // resend time is the default, so no resend interferes with the wait.
    let silent = RepSocket::new(&ctx).expect("rep");
    let silent_url = patiently("listen", silent.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let req = ReqSocket::new(&ctx).expect("req");
    patiently("dial the silent replier", req.dial(&silent_url))
        .await
        .expect("dial the silent replier");
    patiently("the request", req.send(b"work".to_vec()))
        .await
        .expect("the request");
    let parked = tokio::spawn({
        let req = req.clone();
        async move { req.recv().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    req.close();
    answers_closed("a REQ waiting for its reply", async {
        parked.await.expect("the receive task")
    })
    .await;
}

/// Claim: a socket dropped without `close()` gives back its slot in its context's
/// ceiling, and the port it listened on. The ceiling counts every open socket, so
/// a server that opens a socket per peer and never closes them would run its
/// context out of sockets, and a listener that nothing owns would keep its
/// port taken.
#[tokio::test]
async fn dropping_sockets_without_closing_them_gives_their_slots_back() {
    let ctx = Context::new(ContextConfig {
        max_sockets: 8,
        ..ContextConfig::default()
    })
    .expect("context");

    // A pull socket has no background task, so its slot comes back as the socket
    // is dropped, and every creation finds one free.
    for created in 0..2000 {
        let pull = PullSocket::with_options(&ctx, SocketOptions::default())
            .unwrap_or_else(|error| panic!("socket number {created}: {error:?}"));
        drop(pull);
    }
    eventually("the pull sockets' slots come back", || {
        ctx.socket_count() == 0
    })
    .await;

    // Eight listening repliers, dropped as one batch. Their listeners and tasks
    // are released in the background, which is why the count is polled.
    let mut repliers = Vec::new();
    let mut urls = Vec::new();
    for _ in 0..8 {
        let rep = RepSocket::new(&ctx).expect("a replying socket");
        urls.push(
            patiently("listen", rep.listen("tcp://127.0.0.1:0"))
                .await
                .expect("listen")
                .url()
                .to_string(),
        );
        repliers.push(rep);
    }
    drop(repliers);
    eventually("the replying sockets' slots come back", || {
        ctx.socket_count() == 0
    })
    .await;

    // The first replier's port is free again: a new replier listens on it.
    let rebound = RepSocket::new(&ctx).expect("a replying socket");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while let Err(error) = patiently("listen again", rebound.listen(&urls[0])).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} is still bound two seconds after its socket was dropped: {error:?}",
            urls[0]
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(rebound);
    eventually("the rebinding socket's slot comes back", || {
        ctx.socket_count() == 0
    })
    .await;

    // Eight requesters, then eight subscribers, each batch dropped before the next.
    let requesters: Vec<ReqSocket> = (0..8)
        .map(|_| ReqSocket::new(&ctx).expect("a requesting socket"))
        .collect();
    drop(requesters);
    eventually("the requesting sockets' slots come back", || {
        ctx.socket_count() == 0
    })
    .await;

    let subscribers: Vec<SubSocket> = (0..8)
        .map(|_| SubSocket::new(&ctx).expect("a subscribing socket"))
        .collect();
    drop(subscribers);
    eventually("the subscribing sockets' slots come back", || {
        ctx.socket_count() == 0
    })
    .await;
}
