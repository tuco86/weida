//! The tests of `tests/transports.rs` that need the kernel: the unix socket
//! and the named pipe themselves. The unix transport's protocol claims run on
//! a `SimNet` in `tests/sim_unix.rs`; what stays here is what only the OS can
//! answer, and one delivery of each pattern shape over a real socket.

use crate::common;
use common::{Harness, Transport};
#[cfg(windows)]
use weida::{Error, TransferMeta};

use crate::transports::*;

#[cfg(windows)]
#[tokio::test]
async fn pair_over_pipe() {
    pair_both_directions(&Harness::start(Transport::Pipe).await).await;
}
#[cfg(windows)]
#[tokio::test]
async fn flow_over_pipe() {
    flow_carries_datagrams(&Harness::start_with(Transport::Pipe, with_flows()).await).await;
}
#[cfg(windows)]
#[tokio::test]
async fn survey_over_pipe() {
    survey_one_respondent(&Harness::start(Transport::Pipe).await).await;
}
#[cfg(windows)]
#[tokio::test]
async fn bus_over_pipe() {
    let a = Harness::start(Transport::Pipe).await;
    let b = Harness::start(Transport::Pipe).await;
    bus_two_members(&a, &b).await;
    a.shutdown().await;
    b.shutdown().await;
}
#[cfg(unix)]
#[tokio::test]
async fn push_pull_over_unix() {
    push_pull_delivery(&Harness::start(Transport::Unix).await).await;
}
/// Claim: the peer a local connection presents is the one the **kernel**
/// attributed to it, and it is a principal rather than a key
/// ([decisions/0010](../../docs/decisions/0010-local-transport.md) §4.4).
///
/// The uid is this process's own, because both ends are this test. What the
/// assertion is really for is the *shape*: `IncomingMeta::peer` carries a
/// local principal on a socket transport, a key on QUIC and nothing in
/// process, and a PID is reported where the platform has one and never
/// authorized on.
#[cfg(unix)]
#[tokio::test]
async fn a_unix_peer_presents_the_principal_the_kernel_proved() {
    let h = Harness::start(Transport::Unix).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    within(pusher.send(b"work item")).await.expect("send");

    let transfer = within(puller.recv()).await.expect("recv");
    let peer = transfer
        .meta()
        .peer
        .clone()
        .expect("a local peer is proved");
    let principal = peer.local().expect("a principal, not a key");
    assert!(peer.key().is_none(), "a local peer presents no key");
    assert_eq!(
        principal.uid,
        owner_uid(h.socket_path().expect("a unix harness")),
        "the kernel names the process on the other end"
    );
    if cfg!(target_os = "linux") {
        assert!(
            principal.pid.is_some(),
            "Linux reports a PID; it is an observation, never authorized on"
        );
    }
    within(transfer.collect(64)).await.expect("collect");
    client.shutdown().await;
    h.shutdown().await;
}
/// Claim: a socket file left behind by a crash does not stop the next bind.
///
/// Unlink-then-bind is the only answer `AF_UNIX` has — closing a socket does
/// not remove its node — and the race it opens is closed by the directory's
/// ownership and permissions, which is why the doc comment on `bind_unix`
/// makes that the caller's obligation
/// (`docs/research/ipc.md` §1.2, [0010 §4.5]).
#[cfg(unix)]
#[tokio::test]
async fn a_stale_socket_file_does_not_stop_the_next_bind() {
    let h = Harness::start(Transport::Unix).await;
    let path = h.socket_path().expect("a unix harness").to_path_buf();
    assert!(path.exists(), "the socket file is there while bound");

    // A crash leaves the node behind: drop the *binding* without removing it,
    // by leaking the harness's runtime and recreating the file.
    let runtime = weida::Runtime::new(weida::RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    h.shutdown().await;
    std::os::unix::net::UnixListener::bind(&path).expect("leave a stale node");
    assert!(path.exists(), "a stale socket file is what a crash leaves");

    let second = listener.bind_unix(&path).expect("bind over the stale node");
    assert_eq!(second.path(), path);

    // And the mode is the one set explicitly, not whatever umask allowed.
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(&path)
        .expect("bound socket")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "the socket mode is set, not inherited");
    runtime.shutdown().await;
}
/// The named pipe runs the same three bodies.
#[cfg(windows)]
#[tokio::test]
async fn req_rep_over_pipe() {
    req_rep_echo(&Harness::start(Transport::Pipe).await).await;
}
#[cfg(windows)]
#[tokio::test]
async fn push_pull_over_pipe() {
    push_pull_delivery(&Harness::start(Transport::Pipe).await).await;
}
#[cfg(windows)]
#[tokio::test]
async fn pub_sub_over_pipe() {
    pub_sub_fan_out(&Harness::start(Transport::Pipe).await).await;
}
/// Claim: the peer a pipe connection presents is the account the **kernel**
/// attributed to it — the client's token SID — and it is a principal rather
/// than a key ([decisions/0010](../../docs/decisions/0010-local-transport.md)
/// §4.4).
///
/// The SID is this process's own, because both ends are this test; what the
/// assertion is for is the shape, and that the pid is reported and never
/// authorized on.
#[cfg(windows)]
#[tokio::test]
async fn a_pipe_peer_presents_the_principal_the_kernel_proved() {
    let h = Harness::start(Transport::Pipe).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    within(pusher.send(b"work item")).await.expect("send");

    let transfer = within(puller.recv()).await.expect("recv");
    let peer = transfer
        .meta()
        .peer
        .clone()
        .expect("a local peer is proved");
    let principal = peer.windows().expect("an account, not a key");
    assert!(peer.key().is_none() && peer.local().is_none());
    assert!(
        principal.sid.starts_with("S-1-"),
        "a SID in its string form: {}",
        principal.sid
    );
    assert_eq!(
        principal.pid,
        Some(std::process::id()),
        "the pipe reports the client's pid; an observation, never authorized on"
    );
    within(transfer.collect(64)).await.expect("collect");
    client.shutdown().await;
    h.shutdown().await;
}
/// Claim: a transfer connection that does not name a live peer is dispatched
/// nowhere — the same admission rule as on `AF_UNIX`, over a pipe
/// ([decisions/0012](../../docs/decisions/0012-local-connection-grouping.md)
/// §4.2).
#[cfg(windows)]
#[tokio::test]
async fn a_pipe_transfer_connection_with_an_unknown_token_is_refused() {
    use tokio::io::AsyncWriteExt;

    let h = Harness::start(Transport::Pipe).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let path = format!(r"\\.\pipe\{}", h.pipe_name().expect("a pipe harness"));

    // A transfer connection whose token names no peer: kind byte plus 16
    // zero bytes, then a DATA frame for a path that exists, as one payload
    // chunk.
    let mut raw = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(&path)
        .expect("open");
    let mut preamble = vec![0x02u8];
    preamble.extend_from_slice(&[0u8; 16]);
    raw.write_all(&preamble).await.expect("write preamble");
    let header = weida_protocol::DataHeader::addressed("/jobs").encode();
    let mut body = weida_protocol::encode_frame(weida_protocol::FrameKind::Data, &header);
    body.extend_from_slice(b"injected");
    let mut chunk = vec![0x00u8];
    chunk.extend_from_slice(&(body.len() as u32).to_le_bytes());
    chunk.extend_from_slice(&body);
    chunk.push(0x01);
    raw.write_all(&chunk).await.expect("write chunk");
    drop(raw);

    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    within(pusher.send(b"legitimate")).await.expect("send");

    let transfer = within(puller.recv()).await.expect("recv");
    assert_eq!(
        within(transfer.collect(64)).await.expect("collect"),
        b"legitimate".to_vec(),
        "the refused connection must not have been dispatched"
    );
    client.shutdown().await;
    h.shutdown().await;
}
/// Claim: a request abandoned mid-payload is reported to the replier as a
/// cancellation, by name.
///
/// A pipe has no half-close and no reset, so the end of a stream and its
/// abandonment are both chunks of the pipe transport's own framing; the
/// abandonment carries the code, which is more than the socket can say. A
/// framing that lost the reset marker would leave the replier reading until
/// the connection died.
#[cfg(windows)]
#[tokio::test]
async fn an_abandoned_request_over_a_pipe_is_a_named_cancellation() {
    let h = Harness::start(Transport::Pipe).await;
    let replier = h.listener.replier("/slow").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let mut body = request.take_body();
        body.read_capped(64 * 1024).await
    });

    let client = h.client();
    let requester = client.requester(h.trust());
    within(requester.connect(&h.url("/slow")))
        .await
        .expect("connect");
    let (mut request, _reply) = within(requester.open(TransferMeta::default()))
        .await
        .expect("open");
    within(request.write_all(b"partial")).await.expect("write");
    request.cancel();

    let seen = within(handler).await.expect("handler");
    assert!(
        matches!(seen, Err(Error::Canceled)),
        "the replier learns the request was abandoned: {seen:?}"
    );
    client.shutdown().await;
    h.shutdown().await;
}
/// Claim: an unknown-name pipe is a closed peer, and the address rules of
/// [0010 §4.8] hold for the pipe scheme too.
#[cfg(windows)]
#[tokio::test]
async fn a_pipe_address_is_checked_before_it_is_dialled() {
    let client = weida::Runtime::new(weida::RuntimeConfig::default()).expect("runtime");
    let pusher = client.pusher(weida::ClientTls::new(weida::Trust::by_address()));

    let err = within(pusher.connect("weida+pipe://weida-nobody-serves-this/jobs"))
        .await
        .expect_err("no such pipe");
    assert!(matches!(err, Error::ConnectionLost(_)), "{err:?}");

    let err = within(pusher.connect(r"weida+pipe://..\admin$\x/jobs"))
        .await
        .expect_err("a backslash would leave the pipe namespace");
    assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    client.shutdown().await;
}
#[cfg(unix)]
#[tokio::test]
async fn pub_sub_over_unix() {
    pub_sub_fan_out(&Harness::start(Transport::Unix).await).await;
}
#[cfg(windows)]
#[tokio::test]
async fn sequential_exchanges_reclaim_their_slots_over_pipe() {
    let h = Harness::start(Transport::Pipe).await;
    sequential_exchanges_reclaim_their_slots(&h).await;
    h.shutdown().await;
}
#[cfg(windows)]
#[tokio::test]
async fn push_pull_waits_for_a_slot_over_pipe() {
    let h = Harness::start(Transport::Pipe).await;
    push_pull_waits_for_a_slot(&h).await;
    h.shutdown().await;
}
