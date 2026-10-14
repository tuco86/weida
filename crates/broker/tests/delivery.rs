//! What a consumer observes: nothing until it grants credit, then exactly as
//! much as it granted.
//!
//! Every assertion drives the public API over a real QUIC connection. A
//! consumer here is an ordinary [`weida::Subscriber`] — that is the point of
//! `0018` §4.5 — plus `grant`, which is the only thing an L2 consumer does
//! that an L1 subscriber does not.

use std::time::Duration;

use weida::{Requester, Runtime, RuntimeConfig, Subscriber};
use weida_broker::BrokerConfig;

mod common;

use common::{DEADLINE, Harness, SILENCE};

impl Harness {
    async fn producer(&self, path: &str) -> Requester {
        let requester = self.runtime.requester(self.client.clone());
        requester.connect(&self.url(path)).await.expect("connect");
        requester
    }

    /// A consumer subscribed with `filter` and no credit.
    ///
    /// Each consumer gets its own runtime, because two subscribers that share
    /// a pooled client connection collide on the path they claim in that
    /// connection's namespace (`Subscriber::connect`). Two consumers of one
    /// queue are two processes in every real deployment, and this is the
    /// cheapest honest way to be two of them.
    async fn consumer(&self, path: &str, filter: &str) -> (Runtime, Subscriber) {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("consumer runtime");
        let subscriber = runtime.subscriber(self.client.clone());
        subscriber.subscribe(filter).await.expect("subscribe");
        subscriber.connect(&self.url(path)).await.expect("connect");
        // The SUBSCRIBE is in flight; wait until the broker has it, so that a
        // test asserting `consumer_count` sees a settled registry. A grant
        // that arrives first is held and applied by the broker — that is the
        // subject of its own test, not a hazard every test has to dodge.
        let before = self.broker.consumer_count(path).expect("queue");
        let deadline = tokio::time::Instant::now() + DEADLINE;
        while self.broker.consumer_count(path) == Some(before) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the subscription never reached the broker"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        (runtime, subscriber)
    }

    /// Sends `count` messages and waits for each confirm.
    async fn fill(&self, producer: &Requester, count: usize) {
        for i in 0..count {
            let body = format!("job-{i}");
            producer
                .request(body.as_bytes())
                .await
                .expect("the queue accepts it");
        }
    }
}

/// Receives one message, or fails.
async fn next(consumer: &Subscriber) -> String {
    let transfer = tokio::time::timeout(DEADLINE, consumer.recv())
        .await
        .expect("a delivery inside the deadline")
        .expect("a delivery");
    let body = transfer.collect(4096).await.expect("collect");
    String::from_utf8(body).expect("utf-8 body")
}

/// Asserts that nothing arrives for [`SILENCE`].
async fn nothing(consumer: &Subscriber) {
    match tokio::time::timeout(SILENCE, consumer.recv()).await {
        Err(_) => {}
        Ok(Ok(transfer)) => {
            let body = transfer.collect(4096).await.unwrap_or_default();
            panic!(
                "expected no delivery, got {:?}",
                String::from_utf8_lossy(&body)
            );
        }
        Ok(Err(e)) => panic!("subscriber failed instead of waiting: {e:?}"),
    }
}

/// Initial credit is zero: subscribing is not consuming.
#[tokio::test]
async fn a_consumer_receives_nothing_until_it_grants_credit() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_consumer_rt, consumer) = harness.consumer("/jobs", "").await;

    harness.fill(&producer, 3).await;
    // Three messages queued, a consumer subscribed, and no credit: the queue
    // holds them.
    nothing(&consumer).await;
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 3);

    consumer.grant("", 1).await.expect("grant");
    assert_eq!(next(&consumer).await, "job-0");
    nothing(&consumer).await;
    assert_eq!(
        harness.broker.stats("/jobs").expect("queue").messages,
        2,
        "one delivery under a limit of one, and the rest stays queued"
    );
}

/// A duplicated grant delivers nothing extra, and a stale lower one changes
/// nothing: the limit is absolute and the broker keeps the highest it saw.
#[tokio::test]
async fn a_duplicated_or_reordered_grant_changes_nothing() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_consumer_rt, consumer) = harness.consumer("/jobs", "").await;
    harness.fill(&producer, 2).await;

    // A limit of 4 with two messages queued: both go out, and two units of
    // credit are left over.
    consumer.grant("", 4).await.expect("grant");
    assert_eq!(next(&consumer).await, "job-0");
    assert_eq!(next(&consumer).await, "job-1");
    nothing(&consumer).await;

    // The same limit again — a duplicate — and then a *lower* one, which is
    // what a reordered frame looks like when it arrives after a higher one.
    // Neither may deliver anything, because there is nothing queued...
    consumer.grant("", 4).await.expect("duplicate grant");
    consumer.grant("", 3).await.expect("stale grant");
    nothing(&consumer).await;

    // ...and neither may have *lowered* the accepted limit, which is the half
    // a "nothing arrived" assertion cannot see. Two more messages: under the
    // limit of 4 that was granted, both are deliverable. Had the stale grant
    // of 3 been taken as the new limit, the second one would still be sitting
    // in the queue.
    harness.fill(&producer, 2).await;
    assert_eq!(next(&consumer).await, "job-0");
    assert_eq!(
        next(&consumer).await,
        "job-1",
        "a grant below one already accepted must leave the limit alone"
    );
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 0);

    // And now the limit really is spent: four delivered under a limit of four.
    harness.fill(&producer, 1).await;
    nothing(&consumer).await;
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 1);
}

/// Two consumers, one queue: the one with credit gets the messages, the one at
/// its limit gets none, and each message goes to exactly one of them.
#[tokio::test]
async fn a_consumer_at_its_limit_yields_to_one_with_credit() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_first_rt, first) = harness.consumer("/jobs", "").await;
    let (_second_rt, second) = harness.consumer("/jobs", "").await;
    assert_eq!(harness.broker.consumer_count("/jobs"), Some(2));

    harness.fill(&producer, 3).await;

    // Only the first consumer has credit, and only for one message.
    first.grant("", 1).await.expect("grant");
    let got = next(&first).await;
    assert_eq!(got, "job-0");
    nothing(&first).await;
    nothing(&second).await;

    // Now the second one takes the rest. Each message went to exactly one
    // consumer, which is the difference from a publisher's fan-out: the first
    // consumer never sees job-1 or job-2.
    second.grant("", 2).await.expect("grant");
    let mut taken = vec![next(&second).await, next(&second).await];
    taken.sort();
    assert_eq!(taken, vec!["job-1".to_owned(), "job-2".to_owned()]);
    nothing(&first).await;
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 0);
}

/// A subscription that has spent its limit stops receiving, and the queue
/// keeps its messages, its connection and its streams.
///
/// Restating the delivered count is *not* how a consumer pauses — the limit is
/// monotone, so a grant that is not above the standing one changes nothing.
/// What stops delivery here is the limit of 1 being spent; the restatement is
/// in the test to prove it is the no-op it claims to be.
#[tokio::test]
async fn a_spent_limit_stops_delivery_without_a_reset() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_consumer_rt, consumer) = harness.consumer("/jobs", "").await;
    harness.fill(&producer, 3).await;

    consumer.grant("", 1).await.expect("grant");
    assert_eq!(next(&consumer).await, "job-0");

    // The limit equals what has been delivered, so nothing more may go out —
    // and restating it neither delivers anything nor lowers anything.
    consumer.grant("", 1).await.expect("restate");
    nothing(&consumer).await;

    // The connection is untouched by the pause: the producer still gets
    // confirms on it, and the consumer still receives when credit returns.
    harness.fill(&producer, 1).await;
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 3);
    nothing(&consumer).await;

    consumer.grant("", 9).await.expect("resume");
    assert_eq!(next(&consumer).await, "job-1");
    assert_eq!(next(&consumer).await, "job-2");
    assert_eq!(next(&consumer).await, "job-0");
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 0);
}

/// A grant that overtakes its own SUBSCRIBE is kept, and the message arrives.
///
/// The two orders ride two separate unidirectional streams and are handled by
/// two independent tasks, so the broker sees them in whichever order those
/// tasks reach its queue — and the credit path is the shorter one. Nothing
/// restates a grant: `Subscriber::grant` is a one-shot application call, so a
/// dropped one left its consumer receiving nothing, forever, with no error on
/// either side.
///
/// The order is forced rather than raced: the consumer connects with no
/// filters registered, so `connect` sends no SUBSCRIBE at all and the CREDIT
/// is the first thing the queue sees. Nothing here waits for
/// `consumer_count` to move, which is exactly what the other tests in this
/// file do to keep this race out of their way.
#[tokio::test]
async fn a_grant_that_overtakes_its_subscribe_is_kept_and_delivers() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    harness.fill(&producer, 1).await;

    let consumer_rt = Runtime::new(RuntimeConfig::default()).expect("consumer runtime");
    let consumer = consumer_rt.subscriber(harness.client.clone());
    let url = harness.url("/jobs");
    consumer.connect(&url).await.expect("connect");
    consumer.grant("", 5).await.expect("grant");

    // Long enough on loopback that the credit frame has been handled against a
    // queue with no subscription whatsoever, which is the case under test.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        harness.broker.consumer_count("/jobs"),
        Some(0),
        "the grant must have been handled before any subscription existed"
    );

    consumer.subscribe("").await.expect("subscribe");
    assert_eq!(
        next(&consumer).await,
        "job-0",
        "the held grant applies when the subscription lands, and the \
         subscription itself triggers the delivery scan"
    );
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 0);
}

/// An UNSUBSCRIBE removes exactly that subscription, and the queue carries on.
#[tokio::test]
async fn an_unsubscribe_removes_the_consumer() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_consumer_rt, consumer) = harness.consumer("/jobs", "").await;
    assert_eq!(harness.broker.consumer_count("/jobs"), Some(1));

    consumer.unsubscribe("").await.expect("unsubscribe");
    await_consumer_count(&harness, 0).await;

    // A producer is still confirmed, and the message waits for the next
    // consumer rather than being delivered to the one that left.
    harness.fill(&producer, 1).await;
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 1);
    nothing(&consumer).await;
}

/// A closed connection removes every subscription it held — including one it
/// never withdrew, which is the case a publisher never has to handle and a
/// queue does.
#[tokio::test]
async fn a_closed_connection_removes_every_consumer_it_held() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let (consumer_rt, consumer) = harness.consumer("/jobs", "").await;
    consumer.grant("", 5).await.expect("grant");
    assert_eq!(harness.broker.consumer_count("/jobs"), Some(1));

    // Leaked on purpose: dropping a `Subscriber` sends UNSUBSCRIBE, and this
    // test is about the *other* path — a consumer that goes away without
    // saying so. `forget` skips that destructor, so the only thing left to
    // tell the broker is the connection closing.
    std::mem::forget(consumer);
    consumer_rt.shutdown().await;

    await_consumer_count(&harness, 0).await;
}

/// Polls until a queue reports `want` subscriptions.
async fn await_consumer_count(harness: &Harness, want: usize) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while harness.broker.consumer_count("/jobs") != Some(want) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the broker reports {:?} subscriptions, wanted {want}",
            harness.broker.consumer_count("/jobs")
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A filter selects which messages a consumer may take, with the same grammar
/// a subscriber uses — so a queue can be shared by topic without a second
/// concept.
#[tokio::test]
async fn a_filter_selects_which_messages_a_consumer_takes() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_eur_rt, eur) = harness.consumer("/jobs", "px.eur").await;

    producer
        .request_with(
            weida::TransferMeta::default().with_topic("px.usd"),
            b"dollars",
        )
        .await
        .expect("accepted");
    producer
        .request_with(
            weida::TransferMeta::default().with_topic("px.eur"),
            b"euros",
        )
        .await
        .expect("accepted");

    eur.grant("px.eur", 5).await.expect("grant");
    assert_eq!(next(&eur).await, "euros");
    nothing(&eur).await;
    assert_eq!(
        harness.broker.stats("/jobs").expect("queue").messages,
        1,
        "the message no consumer's filter matches stays queued"
    );
}
