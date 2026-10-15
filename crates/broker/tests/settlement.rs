//! A delivery is not a deletion: what settles a message, and what gives it
//! back.
//!
//! The claim this file exists for is the one that makes the broker a broker
//! rather than a fan-out with a buffer: **a message a consumer never reports
//! on comes back**. Were a delivery a deletion, a consumer that died between
//! receiving the bytes and finishing its work would take the message with
//! it — RabbitMQ's `no-ack` mode, which its own documentation calls unsafe.
//!
//! Settlement is a **cursor**, not a reply: the delivery orders `Processed` on
//! its own DATA header and the consumer reports it on the cursor stream, so a
//! delivery keeps the one-way topology of a queue delivery
//! ([0029](../../../docs/decisions/0029-a-report-is-relayed-a-certificate-is-not.md)).
//! Every consumer here therefore reads `transfer.reporter()` — the same
//! accessor any `Subscriber` has — and nothing in this file uses an
//! application reply.

use std::time::Duration;

use weida::{
    Acknowledgement, CursorLevel, IncomingTransfer, Requester, Runtime, RuntimeConfig, Subscriber,
};
use weida_broker::BrokerConfig;

#[path = "common/mod.rs"]
mod common;

use common::{DEADLINE, Harness, SILENCE};

/// The level a delivery orders and a settlement is.
const PROCESSED: CursorLevel = CursorLevel::Known(Acknowledgement::Processed);

impl Harness {
    async fn producer(&self, path: &str) -> Requester {
        let requester = self.runtime.requester(self.client.clone());
        requester.connect(&self.url(path)).await.expect("connect");
        requester
    }

    /// A consumer with its own runtime, subscribed and holding `credit`.
    ///
    /// Its own runtime for `delivery.rs`'s reason: two subscribers sharing a
    /// pooled client connection collide on the path they claim, and two
    /// consumers of one queue are two processes in every real deployment.
    /// Dropping the returned runtime is how this file kills a consumer.
    async fn consumer(&self, path: &str, credit: u64) -> (Runtime, Subscriber) {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("consumer runtime");
        let subscriber = runtime.subscriber(self.client.clone());
        subscriber.subscribe("").await.expect("subscribe");
        subscriber.connect(&self.url(path)).await.expect("connect");
        let before = self.broker.consumer_count(path).expect("queue");
        self.until(DEADLINE, || {
            self.broker.consumer_count(path) != Some(before)
        })
        .await;
        subscriber.grant("", credit).await.expect("grant");
        (runtime, subscriber)
    }

    async fn fill(&self, producer: &Requester, count: usize) {
        for i in 0..count {
            let body = format!("job-{i}");
            producer
                .request(body.as_bytes())
                .await
                .expect("the queue accepts it");
        }
    }

    /// Polls `done` until it is true, or fails at `limit`.
    ///
    /// Polling rather than sleeping: a settlement travels a cursor stream and
    /// is applied by the queue's own loop, so the observable moment is when
    /// the queue says so and a fixed sleep would be a guess about the machine.
    async fn until(&self, limit: Duration, done: impl Fn() -> bool) {
        let deadline = tokio::time::Instant::now() + limit;
        while !done() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the broker never reached the expected state"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn held(&self, path: &str) -> usize {
        self.broker.stats(path).expect("queue").messages
    }

    fn unsettled(&self, path: &str) -> usize {
        self.broker.stats(path).expect("queue").unsettled
    }

    fn charged(&self, path: &str) -> usize {
        self.broker.stats(path).expect("queue").bytes
    }
}

/// Receives one delivery whole, leaving it **unsettled**.
///
/// The reporter is taken and dropped without a record, which is exactly what
/// a consumer that crashes mid-work does: the order was placed, nothing
/// answers it.
async fn take(consumer: &Subscriber) -> String {
    let transfer = tokio::time::timeout(DEADLINE, consumer.recv())
        .await
        .expect("a delivery inside the deadline")
        .expect("a delivery");
    body_of(transfer).await
}

/// Receives one delivery and keeps its reporter, to settle later.
///
/// The reporter outlives the transfer by design — it is bound to the report
/// the DATA header ordered, not to the stream the payload came on — which is
/// what lets a staged consumer report after it has finished working.
async fn take_keeping(consumer: &Subscriber) -> (String, weida::Reporter) {
    let transfer = tokio::time::timeout(DEADLINE, consumer.recv())
        .await
        .expect("a delivery inside the deadline")
        .expect("a delivery");
    let reporter = transfer.reporter().expect("a delivery orders a report");
    (body_of(transfer).await, reporter)
}

/// Receives one delivery and reports `Processed` for it.
async fn take_and_settle(consumer: &Subscriber) -> String {
    let transfer = tokio::time::timeout(DEADLINE, consumer.recv())
        .await
        .expect("a delivery inside the deadline")
        .expect("a delivery");
    let mut reporter = transfer
        .reporter()
        .expect("a delivery orders a report, so a consumer has one to answer with");
    assert_eq!(
        reporter.levels(),
        [PROCESSED],
        "a queue asks its consumer for exactly one level"
    );
    let body = body_of(transfer).await;
    reporter
        .report(PROCESSED, body.len() as u64)
        .await
        .expect("report");
    reporter.finish().await.expect("finish the report");
    body
}

async fn body_of(transfer: IncomingTransfer) -> String {
    let body = transfer.collect(4096).await.expect("collect");
    String::from_utf8(body).expect("utf-8 body")
}

/// Asserts that nothing arrives for [`SILENCE`].
async fn nothing(consumer: &Subscriber) {
    match tokio::time::timeout(SILENCE, consumer.recv()).await {
        Err(_) => {}
        Ok(Ok(transfer)) => panic!("expected no delivery, got {:?}", body_of(transfer).await),
        Ok(Err(e)) => panic!("subscriber failed instead of waiting: {e:?}"),
    }
}

/// A consumer that reports `Processed` ends the queue's responsibility: the
/// message is gone and its bytes are back.
#[tokio::test]
async fn a_reported_delivery_leaves_the_queue_and_frees_its_bytes() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_consumer_rt, consumer) = harness.consumer("/jobs", 1).await;

    harness.fill(&producer, 1).await;
    assert_eq!(take_and_settle(&consumer).await, "job-0");

    // The settlement travels its own stream and is applied by the queue's
    // loop, so this is where the assertion belongs rather than on the receive.
    harness
        .until(DEADLINE, || harness.unsettled("/jobs") == 0)
        .await;
    assert_eq!(harness.held("/jobs"), 0, "nothing left to deliver");
    assert_eq!(
        harness.charged("/jobs"),
        0,
        "a settled message is bytes the queue no longer owes"
    );
}

/// `Processed` is a byte cursor, "consumed up to byte N": progress through a
/// message is not the message being processed, and deleting it then would
/// lose it if the consumer failed before the end. Only an offset that covers
/// the whole body settles.
#[tokio::test]
async fn progress_through_a_body_does_not_settle_it() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_consumer_rt, consumer) = harness.consumer("/jobs", 1).await;

    harness.fill(&producer, 1).await;
    let (body, mut reporter) = take_keeping(&consumer).await;
    harness
        .until(DEADLINE, || harness.unsettled("/jobs") == 1)
        .await;

    reporter
        .report(PROCESSED, body.len() as u64 - 1)
        .await
        .expect("report progress");
    tokio::time::sleep(SILENCE).await;
    assert_eq!(
        harness.unsettled("/jobs"),
        1,
        "a consumer one byte short of the end has not processed the message"
    );
    assert!(harness.charged("/jobs") > 0, "the queue still owes it");

    reporter
        .report(PROCESSED, body.len() as u64)
        .await
        .expect("report the end");
    harness
        .until(DEADLINE, || harness.unsettled("/jobs") == 0)
        .await;
    assert_eq!(harness.charged("/jobs"), 0);
}

/// The claim settlement exists for: a consumer that dies without reporting
/// gives the message back, and another consumer gets it.
#[tokio::test]
async fn a_delivery_nobody_reports_on_comes_back_for_somebody_else() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (first_rt, first) = harness.consumer("/jobs", 1).await;
    let (_second_rt, second) = harness.consumer("/jobs", 0).await;

    harness.fill(&producer, 1).await;
    assert_eq!(take(&first).await, "job-0", "the first consumer takes it");
    harness
        .until(DEADLINE, || harness.unsettled("/jobs") == 1)
        .await;
    assert_eq!(
        harness.held("/jobs"),
        0,
        "out of the queue, and not yet out of its responsibility"
    );

    // The consumer dies with the delivery unreported. No timer is involved:
    // the report ends because the connection does, which is the third of the
    // three failure modes 0029 §4.6 names.
    drop(first);
    drop(first_rt);
    harness.until(DEADLINE, || harness.held("/jobs") == 1).await;
    assert_eq!(harness.unsettled("/jobs"), 0, "nobody holds it now");

    // And it is deliverable again, to a consumer that never saw it.
    second.grant("", 1).await.expect("grant");
    assert_eq!(take_and_settle(&second).await, "job-0");
    harness
        .until(DEADLINE, || harness.charged("/jobs") == 0)
        .await;
}

/// `max_unsettled` is a per-subscription bound and it is live: a consumer that
/// stops reporting stops being given messages, and one settlement lets the
/// next one through.
#[tokio::test]
async fn a_subscription_at_its_unsettled_bound_is_given_nothing_more() {
    let harness = Harness::start(BrokerConfig {
        max_unsettled: 2,
        ..BrokerConfig::with_queues(["/jobs"])
    })
    .await;
    let producer = harness.producer("/jobs").await;
    // Credit for everything: the bound under test is the outstanding one, so
    // credit must not be what stops the third delivery.
    let (_consumer_rt, consumer) = harness.consumer("/jobs", 10).await;

    harness.fill(&producer, 3).await;
    let (first, mut first_report) = take_keeping(&consumer).await;
    let (second, _second_report) = take_keeping(&consumer).await;
    assert_eq!((first.as_str(), second.as_str()), ("job-0", "job-1"));
    harness
        .until(DEADLINE, || harness.unsettled("/jobs") == 2)
        .await;

    // Two outstanding against a bound of two: the third message stays put,
    // although the subscription has eight units of credit left. That is the
    // difference between the two bounds — credit is cumulative and spent,
    // `max_unsettled` is outstanding and returnable.
    nothing(&consumer).await;
    assert_eq!(
        harness.held("/jobs"),
        1,
        "the queue keeps what the bound will not let out"
    );

    // One settlement frees exactly one slot, and the delivery that follows is
    // driven by the **settlement** rather than by a producer: nothing is
    // admitted after this point.
    first_report
        .report(PROCESSED, first.len() as u64)
        .await
        .expect("report");
    first_report.finish().await.expect("finish the report");
    assert_eq!(take(&consumer).await, "job-2");
    harness.until(DEADLINE, || harness.held("/jobs") == 0).await;
}

/// An unsettled delivery still costs the queue its budget, which is what makes
/// the bound above a memory bound rather than a counter.
#[tokio::test]
async fn an_unsettled_delivery_still_counts_against_the_budget() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_consumer_rt, consumer) = harness.consumer("/jobs", 1).await;

    harness.fill(&producer, 1).await;
    let charged_while_held = harness.charged("/jobs");
    assert!(charged_while_held > 0, "a queued message is charged");

    assert_eq!(take(&consumer).await, "job-0");
    harness
        .until(DEADLINE, || harness.unsettled("/jobs") == 1)
        .await;
    assert_eq!(
        harness.charged("/jobs"),
        charged_while_held,
        "handing a message out changes whose turn it is, not what it costs"
    );
}

/// The tasks alive in this runtime once their number has stopped moving.
///
/// The broker, its consumers and its producers share the test's runtime, so
/// the count is the sum of all of them; what a test learns from it is how it
/// *changes*.
async fn quiet_tasks() -> usize {
    let metrics = tokio::runtime::Handle::current().metrics();
    tokio::time::timeout(DEADLINE, async {
        let mut last = metrics.num_alive_tasks();
        let mut steady = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let now = metrics.num_alive_tasks();
            if now == last {
                steady += 1;
                if steady == 4 {
                    return now;
                }
            } else {
                steady = 0;
                last = now;
            }
        }
    })
    .await
    .expect("the number of tasks never settled")
}

/// A delivery watches its consumer's report in a task and in an entry of the
/// report table of that consumer's connection. When the queue takes the
/// delivery back, nothing is left to report to, so both go with it: a
/// consumer that unsubscribes with deliveries it never reported, and keeps
/// its connection, must not leave one task per delivery behind for as long
/// as the connection lives.
#[tokio::test]
async fn a_delivery_the_queue_takes_back_stops_being_watched() {
    const DELIVERIES: usize = 48;
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.producer("/jobs").await;
    let (_consumer_rt, consumer) = harness.consumer("/jobs", DELIVERIES as u64).await;

    harness.fill(&producer, DELIVERIES).await;
    for _ in 0..DELIVERIES {
        take(&consumer).await;
    }
    harness
        .until(DEADLINE, || harness.unsettled("/jobs") == DELIVERIES)
        .await;
    let watched = quiet_tasks().await;

    consumer.unsubscribe("").await.expect("unsubscribe");
    harness
        .until(DEADLINE, || harness.held("/jobs") == DELIVERIES)
        .await;
    assert_eq!(harness.unsettled("/jobs"), 0, "the queue has them back");
    let released = quiet_tasks().await;
    assert!(
        watched >= released + DELIVERIES / 2,
        "{watched} tasks while {DELIVERIES} deliveries were out, {released} after the queue \
         took them back: the watchers outlived their deliveries"
    );
}
