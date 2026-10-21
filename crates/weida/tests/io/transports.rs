//! The tests of `tests/transports.rs` that need the network, a process or real time.

use crate::common;
use common::{Harness, Transport};
use weida::{Error, TransferMeta};

use crate::transports::*;

#[cfg(unix)]
#[tokio::test]
async fn pair_over_unix() {
    pair_both_directions(&Harness::start(Transport::Unix).await).await;
}
#[cfg(windows)]
#[tokio::test]
async fn pair_over_pipe() {
    pair_both_directions(&Harness::start(Transport::Pipe).await).await;
}
#[cfg(unix)]
#[tokio::test]
async fn flow_over_unix() {
    flow_carries_datagrams(&Harness::start_with(Transport::Unix, with_flows()).await).await;
}
#[cfg(windows)]
#[tokio::test]
async fn flow_over_pipe() {
    flow_carries_datagrams(&Harness::start_with(Transport::Pipe, with_flows()).await).await;
}
#[cfg(unix)]
#[tokio::test]
async fn survey_over_unix() {
    survey_one_respondent(&Harness::start(Transport::Unix).await).await;
}
#[cfg(windows)]
#[tokio::test]
async fn survey_over_pipe() {
    survey_one_respondent(&Harness::start(Transport::Pipe).await).await;
}
#[cfg(unix)]
#[tokio::test]
async fn bus_over_unix() {
    let a = Harness::start(Transport::Unix).await;
    let b = Harness::start(Transport::Unix).await;
    bus_two_members(&a, &b).await;
    a.shutdown().await;
    b.shutdown().await;
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
/// `AF_UNIX` runs the same Req/Rep body. Ignored, with the reason, where the
/// suite cannot create a socket at all.
#[cfg(unix)]
#[tokio::test]
async fn req_rep_over_unix() {
    req_rep_echo(&Harness::start(Transport::Unix).await).await;
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
/// Claim: a transfer connection that does not name a live peer is dispatched
/// nowhere.
///
/// The group token is what binds a peer's connections together
/// ([decisions/0012](../../docs/decisions/0012-local-connection-grouping.md)
/// §4.2); a connection carrying an unknown one is refused before any frame
/// on it is read, which is what stops a second process on the same machine
/// from injecting transfers into somebody else's peer.
#[cfg(unix)]
#[tokio::test]
async fn a_transfer_connection_with_an_unknown_token_is_refused() {
    use tokio::io::AsyncWriteExt;

    let h = Harness::start(Transport::Unix).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let socket = h.socket_path().expect("a unix harness").to_path_buf();

    // A transfer connection whose token names no peer: kind byte plus 16
    // zero bytes, then a DATA frame for a path that exists.
    let mut raw = tokio::net::UnixStream::connect(&socket)
        .await
        .expect("connect");
    let mut preamble = vec![0x02u8];
    preamble.extend_from_slice(&[0u8; 16]);
    raw.write_all(&preamble).await.expect("write preamble");
    let header = weida_protocol::DataHeader::addressed("/jobs").encode();
    raw.write_all(&weida_protocol::encode_frame(
        weida_protocol::FrameKind::Data,
        &header,
    ))
    .await
    .expect("write frame");
    raw.write_all(b"injected").await.expect("write payload");
    drop(raw);

    // Nothing reaches the endpoint. A real transfer does, which is what
    // makes the negative observable rather than a race with the test's own
    // impatience.
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
/// Claim: a subscriber that parks nothing learns so at subscribe time.
///
/// The reverse pool is what makes fan-out possible over a socket transport
/// [0012 §4.4]; a runtime configured with none of it cannot receive a
/// published copy, and saying so at `connect` is the difference between a
/// refusal and a silence.
#[cfg(unix)]
#[tokio::test]
async fn a_subscriber_that_parks_nothing_is_refused_at_connect() {
    let h = Harness::start(Transport::Unix).await;
    let _publisher = h.listener.publisher("/md").expect("publisher");
    let mut config = weida::RuntimeConfig::default();
    config.limits.max_parked_reverse = 0;
    let client = h.client_with(config);
    let subscriber = client.subscriber(h.trust());

    let err = within(subscriber.connect(&h.url("/md")))
        .await
        .expect_err("no pool, no fan-out");
    assert!(matches!(err, weida::Error::Unsupported), "{err:?}");

    client.shutdown().await;
    h.shutdown().await;
}
/// Claim: when the pool runs out, the copy is dropped and counted — the
/// publisher is not stalled and the subscription is not torn down.
///
/// A pool of one, and a subscriber that never reads: the first copy takes
/// the parked connection and sits there unread, so the replacement the
/// subscriber parks is the only one available for the next copy. Publishing
/// faster than the pool can refill is exactly the overload [0012 §4.4]
/// answers with a drop, the same answer an exhausted subscriber budget
/// already gets.
#[cfg(unix)]
#[tokio::test]
async fn an_exhausted_reverse_pool_drops_the_copy_and_counts_it() {
    let h = Harness::start(Transport::Unix).await;
    let publisher = h.listener.publisher("/md").expect("publisher");
    let mut config = weida::RuntimeConfig::default();
    config.limits.max_parked_reverse = 1;
    let client = h.client_with(config);
    let subscriber = client.subscriber(h.trust());
    within(subscriber.connect(&h.url("/md")))
        .await
        .expect("connect");
    within(subscriber.subscribe("px.#"))
        .await
        .expect("subscribe");
    within(async {
        while publisher.filter_count() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;

    // Publish without ever reading, and count the attempts: the other two
    // ways `dropped` can rise need 1024 queued copies (the writer queue) or
    // 8 MiB of them (the subscriber budget), so a drop within a handful of
    // five-byte publishes can only be the empty pool.
    let mut published = 0usize;
    within(async {
        while publisher.dropped() == 0 && published < 64 {
            publisher.publish("px.eur", &b"price"[..]).expect("publish");
            published += 1;
            tokio::task::yield_now().await;
        }
    })
    .await;

    assert!(
        publisher.dropped() >= 1,
        "a pool of one, outrun by {published} publishes, must have dropped a copy"
    );
    // And the count says which failure it was: not a subscriber that could
    // not keep up, but a subscriber with nothing parked.
    let starved = publisher
        .dropped_on("px.eur")
        .expect("the dropped topic is counted");
    assert!(starved.no_parked_connection >= 1, "{starved:?}");
    assert_eq!(starved.subscriber_budget, 0, "{starved:?}");
    // The subscription survives its drops: a copy published after the pool
    // has recovered still arrives.
    let arrived = within(async {
        loop {
            publisher.publish("px.eur", &b"later"[..]).expect("publish");
            if let Ok(copy) = subscriber.recv().await {
                break copy;
            }
        }
    })
    .await;
    assert_eq!(arrived.meta().topic.as_deref(), Some("px.eur"));

    client.shutdown().await;
    h.shutdown().await;
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
/// Claim: over `AF_UNIX` a cancelled transfer is reported as a cancellation,
/// and **never** as a payload that looks complete.
///
/// This is the guarantee a plain socket cannot give. A unix socket has a
/// half-close and no abort — `SO_LINGER` with a zero timeout plus `close` is
/// byte-for-byte a plain close at the reader, which was measured rather than
/// assumed — so without the chunk framing `finish` and `reset` would be the
/// same call and a reader would take half a message for a whole one.
/// [PATTERNS.md](../../../docs/PATTERNS.md) §1.5,
/// [GUARANTEES.md](../../../docs/GUARANTEES.md) §6 and
/// [FAILURE_MODEL.md](../../../docs/FAILURE_MODEL.md) §4 state it for every
/// transport; this test holds the unix socket to it.
#[cfg(unix)]
#[tokio::test]
async fn an_abandoned_transfer_over_a_socket_is_a_named_cancellation() {
    let h = Harness::start(Transport::Unix).await;
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
        "a cancelled transfer must not read as a complete one: {seen:?}"
    );
    client.shutdown().await;
    h.shutdown().await;
}
/// Claim: over `AF_UNIX` a transfer **dropped** without `finish` is a
/// cancellation too, which is the case an application hits by accident rather
/// than on purpose.
///
/// The drop path is the one that matters in production: a task that panics or
/// a future that is cancelled mid-write leaves exactly this. `Drop` resets the
/// stream, and without the chunk framing that reset would be a clean close.
#[cfg(unix)]
#[tokio::test]
async fn a_dropped_transfer_over_a_socket_is_a_cancellation_and_not_a_short_payload() {
    let h = Harness::start(Transport::Unix).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let drained = tokio::spawn(async move {
        let transfer = puller.recv().await.expect("recv");
        transfer.collect(64 * 1024).await
    });

    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"half a message"))
        .await
        .expect("write");
    // No `finish()`: the transfer simply goes out of scope.
    drop(transfer);

    let seen = within(drained).await.expect("drain task");
    assert!(
        seen.is_err(),
        "a dropped transfer must not arrive as {seen:?}"
    );
    assert!(
        matches!(seen, Err(Error::Canceled)),
        "and it is a cancellation by name: {seen:?}"
    );
    client.shutdown().await;
    h.shutdown().await;
}
/// Claim: a finished transfer over `AF_UNIX` still arrives whole, byte for
/// byte, across more writes than one chunk.
///
/// The other half of the framing change, and the regression it could cause:
/// a reader that mis-parses a chunk boundary would truncate or concatenate.
/// Three writes of different sizes, one of them larger than the read buffer.
#[cfg(unix)]
#[tokio::test]
async fn a_socket_payload_written_in_several_chunks_arrives_whole() {
    let h = Harness::start(Transport::Unix).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let reading = tokio::spawn(async move {
        let transfer = puller.recv().await.expect("recv");
        transfer.collect(1024 * 1024).await
    });

    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    let big = vec![0x7au8; 96 * 1024];
    within(transfer.write_all(b"one")).await.expect("write one");
    within(transfer.write_all(&big)).await.expect("write big");
    within(transfer.write_all(b"three"))
        .await
        .expect("write three");
    transfer.finish().expect("finish");

    let body = within(reading)
        .await
        .expect("read task")
        .expect("a whole payload");
    let mut expected = b"one".to_vec();
    expected.extend_from_slice(&big);
    expected.extend_from_slice(b"three");
    assert_eq!(body.len(), expected.len());
    assert_eq!(
        body, expected,
        "the chunk boundaries are not in the payload"
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
#[cfg(unix)]
#[tokio::test]
async fn sequential_exchanges_reclaim_their_slots_over_unix() {
    let h = Harness::start(Transport::Unix).await;
    sequential_exchanges_reclaim_their_slots(&h).await;
    h.shutdown().await;
}
#[cfg(windows)]
#[tokio::test]
async fn sequential_exchanges_reclaim_their_slots_over_pipe() {
    let h = Harness::start(Transport::Pipe).await;
    sequential_exchanges_reclaim_their_slots(&h).await;
    h.shutdown().await;
}
#[cfg(unix)]
#[tokio::test]
async fn push_pull_waits_for_a_slot_over_unix() {
    let h = Harness::start(Transport::Unix).await;
    push_pull_waits_for_a_slot(&h).await;
    h.shutdown().await;
}
#[cfg(windows)]
#[tokio::test]
async fn push_pull_waits_for_a_slot_over_pipe() {
    let h = Harness::start(Transport::Pipe).await;
    push_pull_waits_for_a_slot(&h).await;
    h.shutdown().await;
}

/// A parked reverse connection waits for the publisher's next copy, however
/// long that takes: it is not a stream whose frame head is late. A
/// subscriber idle for longer than its header deadline still receives the
/// next copy.
#[cfg(unix)]
#[tokio::test]
async fn a_unix_subscriber_idle_past_the_header_deadline_still_receives() {
    let h = Harness::start(Transport::Unix).await;
    let publisher = h.listener.publisher("/md").expect("publisher");
    let mut config = weida::RuntimeConfig::default();
    config.limits.header_timeout_ms = 300;
    let client = h.client_with(config);
    let subscriber = client.subscriber(h.trust());
    within(subscriber.connect(&h.url("/md")))
        .await
        .expect("connect");
    within(subscriber.subscribe("px.#"))
        .await
        .expect("subscribe");
    within(async {
        while publisher.filter_count() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;

    // Three header deadlines with nothing to send.
    tokio::time::sleep(std::time::Duration::from_millis(900)).await;

    assert_eq!(
        publisher.publish("px.eur", &b"price"[..]).expect("publish"),
        1
    );
    let copy = tokio::time::timeout(std::time::Duration::from_secs(2), subscriber.recv())
        .await
        .expect("the copy arrives")
        .expect("recv");
    assert_eq!(copy.meta().topic.as_deref(), Some("px.eur"));
    assert_eq!(publisher.dropped(), 0, "nothing was lost");

    client.shutdown().await;
    h.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn unbounded_limits_are_held_at_the_semaphore_ceiling_over_unix() {
    let h = Harness::start_with(Transport::Unix, unbounded_limits()).await;
    limits_beyond_what_a_semaphore_counts_are_held_at_its_ceiling(&h).await;
    h.shutdown().await;
}
