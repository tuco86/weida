//! The `AF_UNIX` transport on a `SimNet` (`Transport::SimUnix`): the
//! production unix transport, connection grouping and chunk framing over
//! in-memory connections, on a paused clock, with no socket file. What only
//! the kernel can answer — `SO_PEERCRED`, a stale socket file, the OS
//! backlog — stays in `io::transports` and `io::local_hostile`.

use crate::common;
use common::{Harness, Transport};
use weida::{Error, TransferMeta};

use crate::transports::*;

#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn pair_over_unix() {
    pair_both_directions(&Harness::start(Transport::SimUnix).await).await;
}
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn flow_over_unix() {
    flow_carries_datagrams(&Harness::start_with(Transport::SimUnix, with_flows()).await).await;
}
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn survey_over_unix() {
    survey_one_respondent(&Harness::start(Transport::SimUnix).await).await;
}
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn bus_over_unix() {
    // Both members on one simulated network, as two processes on one host.
    let net = weida::SimNet::new();
    let config = weida::RuntimeConfig::default;
    let a = Harness::start_on(&net, Transport::SimUnix, config()).await;
    let b = Harness::start_on(&net, Transport::SimUnix, config()).await;
    bus_two_members(&a, &b).await;
    a.shutdown().await;
    b.shutdown().await;
}
/// `AF_UNIX` runs the same Req/Rep body. Ignored, with the reason, where the
/// suite cannot create a socket at all.
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn req_rep_over_unix() {
    req_rep_echo(&Harness::start(Transport::SimUnix).await).await;
}
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn push_pull_over_unix() {
    push_pull_delivery(&Harness::start(Transport::SimUnix).await).await;
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
#[tokio::test(start_paused = true)]
async fn a_transfer_connection_with_an_unknown_token_is_refused() {
    use tokio::io::AsyncWriteExt;

    let h = Harness::start(Transport::SimUnix).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let socket = h.socket_path().expect("a unix harness").to_path_buf();

    // A transfer connection whose token names no peer: kind byte plus 16
    // zero bytes, then a DATA frame for a path that exists.
    let mut raw = h
        .net()
        .expect("a simulated harness")
        .connect_unix(&socket)
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
#[tokio::test(start_paused = true)]
async fn a_subscriber_that_parks_nothing_is_refused_at_connect() {
    let h = Harness::start(Transport::SimUnix).await;
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
#[tokio::test(start_paused = true)]
async fn an_exhausted_reverse_pool_drops_the_copy_and_counts_it() {
    let h = Harness::start(Transport::SimUnix).await;
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

    // Publish without ever reading: the other two ways `dropped` can rise
    // need 1024 queued copies (the writer queue) or 8 MiB of them (the
    // subscriber budget), so a drop within a handful of five-byte publishes
    // can only be the empty pool. In one burst, because in memory the
    // subscriber parks its replacement as soon as it runs: a pool of one is
    // outrun only by copies that queue up before it can.
    const PUBLISHED: usize = 16;
    for _ in 0..PUBLISHED {
        publisher.publish("px.eur", &b"price"[..]).expect("publish");
    }
    within(async {
        while publisher.dropped() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await;
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
#[tokio::test(start_paused = true)]
async fn an_abandoned_transfer_over_a_socket_is_a_named_cancellation() {
    let h = Harness::start(Transport::SimUnix).await;
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
#[tokio::test(start_paused = true)]
async fn a_dropped_transfer_over_a_socket_is_a_cancellation_and_not_a_short_payload() {
    let h = Harness::start(Transport::SimUnix).await;
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
#[tokio::test(start_paused = true)]
async fn a_socket_payload_written_in_several_chunks_arrives_whole() {
    let h = Harness::start(Transport::SimUnix).await;
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
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn pub_sub_over_unix() {
    pub_sub_fan_out(&Harness::start(Transport::SimUnix).await).await;
}
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn sequential_exchanges_reclaim_their_slots_over_unix() {
    let h = Harness::start(Transport::SimUnix).await;
    sequential_exchanges_reclaim_their_slots(&h).await;
    h.shutdown().await;
}
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn push_pull_waits_for_a_slot_over_unix() {
    let h = Harness::start(Transport::SimUnix).await;
    push_pull_waits_for_a_slot(&h).await;
    h.shutdown().await;
}

/// A parked reverse connection waits for the publisher's next copy, however
/// long that takes: it is not a stream whose frame head is late. A
/// subscriber idle for longer than its header deadline still receives the
/// next copy.
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn a_unix_subscriber_idle_past_the_header_deadline_still_receives() {
    let h = Harness::start(Transport::SimUnix).await;
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
#[tokio::test(start_paused = true)]
async fn unbounded_limits_are_held_at_the_semaphore_ceiling_over_unix() {
    let h = Harness::start_with(Transport::SimUnix, unbounded_limits()).await;
    limits_beyond_what_a_semaphore_counts_are_held_at_its_ceiling(&h).await;
    h.shutdown().await;
}
