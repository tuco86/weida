//! What an interrupted stream means, per pattern.
//!
//! [PATTERNS.md](../../../docs/PATTERNS.md) §1.11 states a rule and then a
//! per-pattern table of answers — cancel, reschedule, reconnect — and until
//! now both were claims. This file is the table, one test per row, each
//! asserting what **both** sides observe. It adds no API: everything here is
//! the existing surface put under a failure.
//!
//! The rule under test, in the document's own words:
//!
//! > Within a connection, QUIC retransmits; when the connection ends, an
//! > unfinished stream is gone, and weida does not resend it.

use crate::common;

use std::time::Duration;

use common::Server;
use tokio::io::AsyncReadExt;
use weida::{
    Acknowledgement, CursorLevel, Error, Limits, RuntimeConfig, SimNet, TransferMeta,
    TransferMeta as Meta,
};

/// Generous ceiling: every assertion below should settle in milliseconds.
pub(crate) const DEADLINE: Duration = Duration::from_secs(15);

pub(crate) async fn within<F: Future>(f: F) -> F::Output {
    weida_testkit::within("an interruption step", DEADLINE, f).await
}

pub(crate) const STORED: CursorLevel = CursorLevel::Known(Acknowledgement::Stored);

#[tokio::test(start_paused = true)]
async fn a_transfer_interrupted_before_fin_is_connection_lost_for_the_sender() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let mut transfer = within(pusher.open(Meta::default())).await.expect("open");
    within(transfer.write_all(b"the first half"))
        .await
        .expect("write");
    // The receiver has the stream; now the connection goes away under it.
    let inbound = within(puller.recv()).await.expect("recv");
    server.runtime.shutdown().await;

    // Before FIN the outcome is **definite**: nothing was delivered, and the
    // sender is told so rather than being left with `Indeterminate`.
    let err = loop {
        match within(transfer.write_all(&[0u8; 64 * 1024])).await {
            Ok(()) => continue,
            Err(e) => break e,
        }
    };
    assert!(
        matches!(err, Error::ConnectionLost(_)),
        "a write before FIN reports a definite loss: {err:?}"
    );

    // And the receiver never saw it as complete.
    let err = within(inbound.collect(1024))
        .await
        .expect_err("an unfinished stream is never complete");
    assert!(
        !matches!(err, Error::Indeterminate),
        "the receiver's side of a break is definite too: {err:?}"
    );

    client.shutdown().await;
}

#[tokio::test]
async fn a_reader_never_sees_an_interrupted_stream_as_complete() {
    // The §1.5 rule, on both reading surfaces: `collect` fails with
    // `Canceled`, `AsyncRead` with `ConnectionReset`, and **never** `Ok(0)` —
    // which is the one answer that would make a half payload look whole.
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    // First: the whole-payload reader.
    let mut transfer = within(pusher.open(Meta::default())).await.expect("open");
    within(transfer.write_all(b"half a message"))
        .await
        .expect("write");
    let mut inbound = within(puller.recv()).await.expect("recv");
    transfer.cancel();
    let err = within(inbound.read_capped(1024))
        .await
        .expect_err("collect never reports a short payload as whole");
    assert!(matches!(err, Error::Canceled), "{err:?}");

    // Second: the streaming reader.
    let mut transfer = within(pusher.open(Meta::default())).await.expect("open");
    within(transfer.write_all(b"half again"))
        .await
        .expect("write");
    let mut inbound = within(puller.recv()).await.expect("recv");
    // Read the prefix that did arrive, so the failure is observed *after* a
    // successful read rather than instead of one.
    let mut head = [0u8; 4];
    within(inbound.read_exact(&mut head))
        .await
        .expect("the prefix arrived");
    assert_eq!(&head, b"half");
    transfer.cancel();

    let mut rest = [0u8; 64];
    loop {
        match within(inbound.read(&mut rest)).await {
            // Bytes still buffered from before the reset.
            Ok(n) if n > 0 => continue,
            Ok(0) => panic!("an interrupted stream must never read as EOF"),
            Ok(_) => unreachable!("n is either zero or positive"),
            Err(e) => {
                assert_eq!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset,
                    "AsyncRead reports a reset, not an end: {e:?}"
                );
                break;
            }
        }
    }

    client.shutdown().await;
}

#[tokio::test]
async fn a_reqrep_requester_can_reissue_after_an_indeterminate_outcome() {
    let net = SimNet::new();
    // Req/Rep's answer is **reschedule**, and it is available because the
    // requester learns the outcome: a reply half is what makes re-issuing a
    // decision rather than a guess.
    let server = Server::start_on(&net, RuntimeConfig::default()).await;
    let replier = server.listener.replier("/t").expect("replier");

    let (taken_tx, taken_rx) = tokio::sync::oneshot::channel();
    let taking = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        // The request is taken whole — so the replier may already have acted
        // on it — and then nothing ever answers.
        let body = request.body().read_capped(64).await.expect("body");
        assert_eq!(body, b"do the work");
        taken_tx.send(()).expect("the requester is waiting");
        // Leaked on purpose: dropping it would send ERROR `NO_REPLY`, which
        // is a *definite* answer. What is under test is the case where the
        // requester learns nothing.
        std::mem::forget(request);
        std::future::pending::<()>().await;
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/t")))
        .await
        .expect("connect");
    let pending = tokio::spawn(async move { requester.request(b"do the work").await });

    taken_rx.await.expect("the replier took the request");
    server.runtime.shutdown().await;
    let outcome = within(pending)
        .await
        .expect("the request task")
        .expect_err("a request whose FIN landed and whose reply never came is not a success");
    taking.abort();
    assert!(
        matches!(outcome, Error::Indeterminate),
        "the replier may have acted, so the outcome is indeterminate: {outcome:?}"
    );

    // Rescheduling is an ordinary new exchange on a fresh peer. weida
    // remembers nothing and re-opens nothing: the sender decides.
    let second = Server::start_on(&net, RuntimeConfig::default()).await;
    let replier = second.listener.replier("/t").expect("replier");
    let answering = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let body = request.body().read_capped(64).await.expect("body");
        assert_eq!(body, b"do the work");
        let mut reply = request.reply(Meta::default()).await.expect("reply");
        reply.write_all(b"done").await.expect("write");
        reply.finish().expect("finish");
    });

    let reissued = client.requester(second.trust());
    within(reissued.connect(&second.url("/t")))
        .await
        .expect("connect");
    let answer = within(reissued.request(b"do the work"))
        .await
        .expect("the re-issued request is answered");
    assert_eq!(within(answer.collect(64)).await.expect("collect"), b"done");

    answering.await.expect("replier");
    client.shutdown().await;
}

#[tokio::test]
async fn a_push_producer_is_the_only_side_that_can_reschedule() {
    // Push/Pull's answer is **reschedule, and only the sender can**: a
    // one-way transfer has no reply half, so the puller has nothing to ask
    // with. That is a type-level fact — an `IncomingTransfer` has no `reply`
    // — and what is observable here is its consequence: the puller sees a
    // failure and can do nothing about it, while the producer re-sends.
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let mut transfer = within(pusher.open(Meta::default())).await.expect("open");
    within(transfer.write_all(b"work")).await.expect("write");
    let inbound = within(puller.recv()).await.expect("recv");
    transfer.cancel();
    let err = within(inbound.collect(64))
        .await
        .expect_err("the puller sees a failure");
    assert!(matches!(err, Error::Canceled), "{err:?}");

    // The producer still holds the source, so it re-derives the work as a
    // **new** transfer. Nothing continues the old one: a reschedule is an
    // ordinary send.
    within(pusher.send(b"work")).await.expect("resend");
    let inbound = within(puller.recv()).await.expect("recv");
    assert_eq!(within(inbound.collect(64)).await.expect("collect"), b"work");

    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_pubsub_copy_lost_to_a_dead_subscriber_is_counted_not_retried() {
    // Pub/Sub's answer is **cancel**: a copy is per subscriber and best
    // effort, so a copy that cannot be taken is counted and the publisher
    // moves on. A tiny budget makes the boundary observable.
    let server = Server::start_with(Limits {
        subscriber_buffer_bytes: 4096,
        ..Limits::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let subscriber = client.subscriber(server.trust());
    within(subscriber.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(subscriber.subscribe("")).await.expect("subscribe");

    // Wait for the subscription to register.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while publisher.subscriber_count() == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(publisher.subscriber_count(), 1);

    // `publish` is synchronous and charges the subscriber's byte budget
    // before a writer task can run, so a burst that exceeds the budget is a
    // deterministic drop rather than a race: the budget is 4 KiB and this
    // burst wants 128 KiB of it.
    let payload = vec![0x33u8; 4000];
    let mut reached = 0usize;
    for _ in 0..32u32 {
        reached += publisher
            .publish("burst", payload.clone())
            .expect("publish never fails for a slow subscriber");
    }
    let drops: u64 = publisher.drops().iter().map(|d| d.total()).sum();
    assert!(
        drops > 0,
        "a copy the subscriber's budget cannot take is counted: {:?}",
        publisher.drops()
    );
    assert_eq!(
        reached as u64 + drops,
        32,
        "every copy is either sent or counted"
    );

    // And the dropped copies are **not** retried: exactly the ones that were
    // sent arrive, and no more — a later publish is what ends the count.
    publisher.publish("end", &b"end"[..]).expect("publish");
    let mut burst_seen = 0usize;
    loop {
        let arrived = within(subscriber.recv()).await.expect("recv");
        let topic = arrived.meta().topic.clone().unwrap_or_default();
        let body = within(arrived.collect(8192)).await.expect("collect");
        if topic == "end" {
            assert_eq!(body, b"end");
            break;
        }
        assert_eq!(topic, "burst");
        burst_seen += 1;
    }
    assert_eq!(
        burst_seen, reached,
        "a dropped copy is never re-sent: {burst_seen} arrived of {reached} sent"
    );

    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_cursor_reported_before_the_break_survives_the_break() {
    // The whole value of a cursor over a verdict: a whole-message verdict
    // tells an interrupted sender **nothing**, while a cursor tells it a
    // number, and the number is still there after the connection is gone
    // ([0023](../../../docs/decisions/0023-completion-is-a-cursor.md) §4.6).
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let mut transfer = within(pusher.open(TransferMeta::default().with_report([STORED])))
        .await
        .expect("open");
    let mut cursors = transfer.cursors().expect("cursors");
    within(transfer.write_all(&[0x5au8; 900]))
        .await
        .expect("write");

    // The receiver takes the prefix and reports how far it got.
    let mut inbound = within(puller.recv()).await.expect("recv");
    let mut reporter = inbound.reporter().expect("a reporter");
    let mut prefix = [0u8; 900];
    within(inbound.read_exact(&mut prefix))
        .await
        .expect("the prefix arrived");
    within(reporter.report(STORED, 900)).await.expect("report");
    let set = within(cursors.changed()).await.expect("a cursor arrived");
    assert_eq!(set.offset(STORED), Some(900));

    // Now the connection dies with the transfer unfinished.
    server.runtime.shutdown().await;
    let _ = transfer.write_all(&[0u8; 64 * 1024]).await;

    // The verdict is unknown — the transfer never finished — but the cursor
    // is a fact: 900 bytes reached the far end and the sender can act on it.
    assert_eq!(cursors.snapshot().offset(STORED), Some(900));
    assert_eq!(within(cursors.changed()).await, None);

    client.shutdown().await;
}
