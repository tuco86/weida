//! A message a consumer's connection will not read.
//!
//! A consumer states in its HELLO how long a header it will read, and the
//! broker checks a delivery's header against it before opening a stream. A
//! message whose header is longer is **about the message, not about the
//! connection**: the consumer is healthy and the next message may be fine. So
//! the message goes back for other consumers, that connection is not offered
//! it again, and nothing is closed — closing would turn one such message into
//! a loop of redials, each offered the same message first. What every
//! connection refuses is dropped after a bounded number of refusals and
//! counted.

use std::time::Duration;

use tokio::time::timeout;
use weida::{
    IncomingTransfer, Limits, PeerEvent, Requester, Runtime, RuntimeConfig, Subscriber,
    TransferMeta,
};
use weida_broker::{BrokerConfig, MAX_REFUSALS, QueueStats};

#[path = "common/mod.rs"]
mod common;

use common::{DEADLINE, Harness, SILENCE};

/// What a healthy peer is promised while a message is refused.
const PROMPTLY: Duration = Duration::from_secs(2);

/// The longest header a consumer in these tests will read: more than a short
/// delivery's, less than one that carries a long topic.
const SMALL_HEADER: u64 = 160;

/// A topic that makes a delivery's header longer than [`SMALL_HEADER`], and
/// is still a legal topic.
fn long_topic() -> String {
    "t".repeat(240)
}

async fn within<F: Future>(limit: Duration, f: F) -> F::Output {
    timeout(limit, f).await.expect("timed out")
}

impl Harness {
    async fn requester(&self, path: &str) -> Requester {
        let requester = self.runtime.requester(self.client.clone());
        requester.connect(&self.url(path)).await.expect("connect");
        requester
    }

    /// A consumer of `""` on a runtime of its own, holding `credit`.
    async fn reader(
        &self,
        path: &str,
        config: RuntimeConfig,
        credit: u64,
    ) -> (Runtime, Subscriber) {
        let runtime = Runtime::new(config).expect("consumer runtime");
        let subscriber = runtime.subscriber(self.client.clone());
        subscriber.subscribe("").await.expect("subscribe");
        let before = self.broker.consumer_count(path).expect("queue");
        subscriber.connect(&self.url(path)).await.expect("connect");
        within(DEADLINE, async {
            while self.broker.consumer_count(path) != Some(before + 1) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        subscriber.grant("", credit).await.expect("grant");
        (runtime, subscriber)
    }

    /// Waits until `reached` holds of the queue's counters.
    async fn until(&self, path: &str, reached: impl Fn(&QueueStats) -> bool) {
        within(DEADLINE, async {
            while !reached(&self.broker.stats(path).expect("queue")) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
    }
}

/// A consumer that reads headers of at most [`SMALL_HEADER`] bytes.
fn small_headers() -> RuntimeConfig {
    RuntimeConfig {
        limits: Limits {
            max_header_bytes: SMALL_HEADER,
            ..Limits::default()
        },
        ..RuntimeConfig::default()
    }
}

async fn body_of(transfer: IncomingTransfer) -> String {
    String::from_utf8(transfer.collect(1 << 10).await.expect("collect")).expect("utf-8")
}

async fn promptly(consumer: &Subscriber) -> String {
    let transfer = within(PROMPTLY, consumer.recv()).await.expect("a delivery");
    body_of(transfer).await
}

/// The refusal is the message's: it goes to a consumer that can read it, the
/// consumer that could not stays connected and stays served, is not offered
/// the message again, and is not charged credit for a delivery that never
/// happened.
#[tokio::test]
async fn a_message_a_consumer_will_not_read_goes_to_another_and_the_consumer_stays() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    // One unit of credit: the refusal must give it back, or the next message
    // this consumer can read would never reach it.
    let (_small_rt, small) = harness.reader("/jobs", small_headers(), 1).await;
    let mut events = small.events();
    let producer = harness.requester("/jobs").await;

    // Only the small consumer has credit, so it is the one offered the
    // message with the long topic, and it cannot read its header.
    within(
        PROMPTLY,
        producer.request_with(TransferMeta::default().with_topic(long_topic()), b"long"),
    )
    .await
    .expect("confirmed");
    harness
        .until("/jobs", |s| {
            s.refused == 1 && s.messages == 1 && s.unsettled == 0
        })
        .await;

    // Nothing closed the connection, and nothing ever offers it that message
    // again: the count of refusals stays at one while the queue is quiet.
    loop {
        match timeout(SILENCE, events.recv()).await {
            Err(_) => break,
            Ok(Some(PeerEvent::Lost { cause, .. })) => {
                panic!(
                    "the consumer's connection was lost ({cause}) over a message it could not read"
                )
            }
            Ok(_) => continue,
        }
    }
    assert_eq!(harness.broker.consumer_count("/jobs"), Some(1));
    assert_eq!(harness.broker.stats("/jobs").expect("queue").refused, 1);

    // Another consumer reads it.
    let (_other_rt, other) = harness.reader("/jobs", RuntimeConfig::default(), 1).await;
    assert_eq!(promptly(&other).await, "long");

    // And the small consumer is served: a message it can read reaches it
    // with the credit the refusal gave back, the other consumer having spent
    // its own.
    within(PROMPTLY, producer.request(b"short"))
        .await
        .expect("confirmed");
    assert_eq!(promptly(&small).await, "short");
    assert_eq!(harness.broker.stats("/jobs").expect("queue").refused, 1);
}

/// A message every connection refuses is not kept for ever: after
/// [`MAX_REFUSALS`] refusals the queue drops it, counts it, and releases what
/// it was charged. The consumers that refused it are all still connected.
#[tokio::test]
async fn a_message_nobody_will_read_is_dropped_and_counted() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let mut readers = Vec::new();
    for _ in 0..MAX_REFUSALS {
        readers.push(harness.reader("/jobs", small_headers(), 1).await);
    }
    let producer = harness.requester("/jobs").await;

    within(
        PROMPTLY,
        producer.request_with(TransferMeta::default().with_topic(long_topic()), b"long"),
    )
    .await
    .expect("confirmed");

    harness.until("/jobs", |s| s.dropped == 1).await;
    let stats = harness.broker.stats("/jobs").expect("queue");
    assert_eq!(
        stats,
        QueueStats {
            messages: 0,
            unsettled: 0,
            bytes: 0,
            reserved: 0,
            refused: MAX_REFUSALS as u64,
            dropped: 1,
        },
        "every connection was offered it once, and it left nothing behind"
    );
    assert_eq!(
        harness.broker.consumer_count("/jobs"),
        Some(MAX_REFUSALS),
        "none of them was closed for it"
    );

    // The queue is as it was: a message they can read is delivered.
    within(PROMPTLY, producer.request(b"short"))
        .await
        .expect("confirmed");
    harness
        .until("/jobs", |s| s.messages == 0 && s.unsettled == 1)
        .await;
    let mut delivered = 0;
    for (_, reader) in &readers {
        if let Ok(Ok(transfer)) = timeout(Duration::from_millis(50), reader.recv()).await {
            assert_eq!(body_of(transfer).await, "short");
            delivered += 1;
        }
    }
    assert_eq!(delivered, 1, "exactly one consumer takes it");
}
