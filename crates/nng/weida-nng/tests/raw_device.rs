//! Raw sockets and the device, over real connections.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use weida_nng::{
    Context, ContextConfig, Error, Message, RawSocket, RepSocket, ReqSocket, SocketOptions, raw,
};
use weida_sp::header::{HEADER_LEN, ProtocolHeader};
use weida_sp::{Backtrace, EndpointType, message};

pub(crate) fn options() -> SocketOptions {
    SocketOptions {
        // No socket timeout: a slow run is reported by the test's own waits,
        // not failed by an option. The handshake bound is an hour, not the
        // library's ten seconds.
        recv_timeout: None,
        send_timeout: None,
        handshake_timeout: Duration::from_secs(3600),
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

pub(crate) async fn server_reply(client: &ReqSocket) -> Message {
    weida_testkit::within(
        "the reply coming back through the device",
        Duration::from_secs(5),
        client.recv(),
    )
    .await
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

/// The payload limit of the boundary test, in octets.
pub(crate) const LIMIT: u64 = 1024;
/// The reply the stalled-client test asks for: big enough that a client which
/// never reads fills the kernel's buffers after a few of them.
pub(crate) const REPLY_BYTES: usize = 256 * 1024;
/// The request the backend answers late, and the one the honest client sends.
pub(crate) const HONEST: &[u8] = b"honest";

/// Waits for one await on a socket or a hand-driven peer, reporting an
/// operation that takes longer than ten seconds instead of failing the test
/// for it.
pub(crate) async fn bounded<T>(operation: impl Future<Output = T>) -> T {
    weida_testkit::within(
        "an operation on a socket or a peer",
        Duration::from_secs(10),
        operation,
    )
    .await
}

/// Polls `condition` every 5 ms until it holds, reporting past 10 s.
pub(crate) async fn eventually(condition: impl FnMut() -> bool) {
    weida_testkit::until(
        "a condition on the sockets",
        Duration::from_secs(10),
        condition,
    )
    .await;
}

/// The socket address a `tcp://` URL names.
pub(crate) fn address(url: &str) -> SocketAddr {
    url.trim_start_matches("tcp://")
        .parse()
        .expect("a tcp:// URL that names a socket address")
}

/// Completes the SP header exchange for a peer built by hand. Our eight octets
/// go first, as the mapping requires, and then the other side's, which has to
/// pair with the endpoint we claimed.
pub(crate) async fn handshake(mut stream: TcpStream, endpoint: EndpointType) -> TcpStream {
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
pub(crate) async fn peer(addr: SocketAddr, endpoint: EndpointType) -> TcpStream {
    let stream = bounded(TcpStream::connect(addr)).await.expect("connect");
    handshake(stream, endpoint).await
}

/// The two raw sockets of a device: the REP `front` that clients dial and the
/// REQ `back` that backends dial, each listening on an ephemeral loopback port.
/// Returns the sockets and the URL each one listens on.
pub(crate) async fn device_sockets(
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
pub(crate) fn spawn_device(
    front: &RawSocket,
    back: &RawSocket,
) -> JoinHandle<weida_nng::Result<raw::Forwarded>> {
    let (front, back) = (front.clone(), back.clone());
    tokio::spawn(async move { raw::device(&front, &back).await })
}

/// Writes requests over a hand-driven REQ connection until the task is
/// aborted and never reads a reply. The test ends the flood, so the traffic
/// lasts exactly as long as the claim needs it, however slow the machine is.
pub(crate) async fn flood(addr: SocketAddr) {
    let (_replies_never_read, mut requests) = peer(addr, EndpointType::Req).await.into_split();
    for id in 1u32.. {
        let body = Backtrace::direct(id).encode_message(b"x");
        if requests.write_all(&message::encode(&body)).await.is_err() {
            break;
        }
    }
}

/// One device with a client and a backend connected on both sides and one
/// exchange done through it, then its front socket or its back socket closed.
pub(crate) async fn device_ends_after_closing(ctx: &Context, close_front: bool) {
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
    weida_testkit::within(
        "the device ending after one of its sockets closed",
        Duration::from_secs(2),
        forwarding,
    )
    .await
    .expect("the device task did not panic")
    .expect("the device ends without an error");
}
