//! Peers that stop answering: what each of them costs, and whom.
//!
//! The claim of this file is the one the broker's loop is built around: **no
//! peer's silence is another peer's wait**. Each test sets one hostile or
//! stalled peer beside an ordinary pair and asks the ordinary pair for
//! something under a deadline that a parked queue could not meet. The stalled
//! consumer is an ordinary [`weida::Subscriber`] on a runtime that grants its
//! broker a single stream it never frees, which is the shape of a consumer that
//! stopped reading: `max_concurrent_uni_streams` and a one-deep endpoint queue
//! are what `weida`'s own backpressure tests use for it.

use std::time::{Duration, Instant};

use tokio::time::timeout;
use weida::{
    Acknowledgement, IncomingTransfer, Limits, LossCause, PeerEvent, Pusher, Requester, Runtime,
    RuntimeConfig, Subscriber, TransferMeta,
};
use weida_broker::BrokerConfig;

#[path = "common/mod.rs"]
mod common;

use common::{DEADLINE, Harness, SILENCE};

/// What a healthy peer is promised while another one is stalled.
const PROMPTLY: Duration = Duration::from_secs(2);

async fn within<F: Future>(limit: Duration, f: F) -> F::Output {
    timeout(limit, f).await.expect("timed out")
}

impl Harness {
    async fn requester(&self, path: &str) -> Requester {
        let requester = self.runtime.requester(self.client.clone());
        requester.connect(&self.url(path)).await.expect("connect");
        requester
    }

    async fn pusher(&self, path: &str) -> Pusher {
        let pusher = self.runtime.pusher(self.client.clone());
        pusher.connect(&self.url(path)).await.expect("connect");
        pusher
    }

    /// A consumer subscribed with `filters` on a runtime of its own, holding no
    /// credit yet.
    async fn consumer(
        &self,
        path: &str,
        filters: &[&str],
        config: RuntimeConfig,
    ) -> (Runtime, Subscriber) {
        let runtime = Runtime::new(config).expect("consumer runtime");
        let subscriber = runtime.subscriber(self.client.clone());
        for filter in filters {
            subscriber.subscribe(filter).await.expect("subscribe");
        }
        let before = self.broker.consumer_count(path).expect("queue");
        subscriber.connect(&self.url(path)).await.expect("connect");
        within(DEADLINE, async {
            while self.broker.consumer_count(path) != Some(before + filters.len()) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        (runtime, subscriber)
    }

    /// A consumer that stops reading after the broker has written it two
    /// messages: its runtime grants two streams and queues one transfer, and
    /// nothing here ever receives.
    async fn stalled_consumer(&self, path: &str) -> (Runtime, Subscriber) {
        let (runtime, subscriber) = self
            .consumer(
                path,
                &[""],
                RuntimeConfig {
                    limits: Limits {
                        max_concurrent_uni_streams: 2,
                        ..Limits::default()
                    },
                    endpoint_queue: 1,
                    ..RuntimeConfig::default()
                },
            )
            .await;
        subscriber.grant("", 1000).await.expect("grant");
        (runtime, subscriber)
    }

    /// Waits until `reached` holds of the queue's counters.
    async fn until(
        &self,
        path: &str,
        what: &str,
        reached: impl Fn(&weida_broker::QueueStats) -> bool,
    ) {
        within(DEADLINE, async {
            while !reached(&self.broker.stats(path).expect("queue")) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let _ = what;
    }
}

async fn body_of(transfer: IncomingTransfer) -> String {
    String::from_utf8(transfer.collect(1 << 20).await.expect("collect")).expect("utf-8")
}

/// One delivery, within [`PROMPTLY`].
async fn promptly(consumer: &Subscriber) -> String {
    let transfer = within(PROMPTLY, consumer.recv()).await.expect("a delivery");
    body_of(transfer).await
}

/// A producer that declares a megabyte, sends one byte and holds its stream
/// open is a read that never ends. An ordinary producer beside it is still
/// confirmed, and an ordinary consumer still receives.
#[tokio::test]
async fn a_producer_that_stops_mid_message_does_not_stall_the_queue() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let stalled = harness.pusher("/jobs").await;
    let mut held = within(
        DEADLINE,
        stalled.open(TransferMeta::default().with_content_len(1 << 20)),
    )
    .await
    .expect("open");
    within(DEADLINE, held.write_all(b"x"))
        .await
        .expect("the one byte");
    // The exchange form of the same producer: a request half that never ends.
    let requester = harness.requester("/jobs").await;
    let (mut half, _reply) = within(
        DEADLINE,
        requester.open(TransferMeta::default().with_content_len(1 << 20)),
    )
    .await
    .expect("open an exchange");
    within(DEADLINE, half.write_all(b"y"))
        .await
        .expect("the one byte");

    let ordinary = harness.requester("/jobs").await;
    let reply = within(PROMPTLY, ordinary.request(b"work item"))
        .await
        .expect("confirmed while two producers are stalled");
    assert_eq!(reply.meta().achieved, Some(Acknowledgement::Accepted));

    let (_runtime, consumer) = harness
        .consumer("/jobs", &[""], RuntimeConfig::default())
        .await;
    consumer.grant("", 1).await.expect("grant");
    assert_eq!(promptly(&consumer).await, "work item");
    drop((held, half));
}

/// A consumer that has credit and stops reading holds one write open. The
/// producer is still confirmed, and a second consumer that arrives afterwards
/// still receives.
#[tokio::test]
async fn a_consumer_that_stops_reading_does_not_stall_the_queue() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let (_stalled_rt, _stalled) = harness.stalled_consumer("/jobs").await;

    // Enough messages for the stalled consumer's runtime to fill its two
    // streams and then block the third write; every confirm is within a
    // deadline a parked queue could not meet.
    let producer = harness.requester("/jobs").await;
    for i in 0..6 {
        within(PROMPTLY, producer.request(format!("job-{i}").as_bytes()))
            .await
            .expect("confirmed while a consumer has stopped reading");
    }

    let (_healthy_rt, healthy) = harness
        .consumer("/jobs", &[""], RuntimeConfig::default())
        .await;
    healthy.grant("", 10).await.expect("grant");
    let got = promptly(&healthy).await;
    assert!(got.starts_with("job-"), "{got}");
}

/// What a stall costs is bounded by the deadline. With a short one, the stopped
/// producer's reservation is returned and a consumer that stopped reading is
/// dropped, with everything it held back in the queue.
#[tokio::test]
async fn a_stalled_peer_is_reclaimed_at_the_deadline() {
    let harness = Harness::start(BrokerConfig {
        io_timeout: Duration::from_millis(400),
        ..BrokerConfig::with_queues(["/jobs"])
    })
    .await;

    let stalled = harness.pusher("/jobs").await;
    let mut held = within(
        DEADLINE,
        stalled.open(TransferMeta::default().with_content_len(1 << 20)),
    )
    .await
    .expect("open");
    within(DEADLINE, held.write_all(b"x"))
        .await
        .expect("the one byte");
    harness
        .until("/jobs", "the stopped producer holds a reservation", |s| {
            s.reserved > 0
        })
        .await;
    harness
        .until("/jobs", "its reservation is returned", |s| s.reserved == 0)
        .await;
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 0);

    let (_rt, _consumer) = harness.stalled_consumer("/jobs").await;
    let producer = harness.requester("/jobs").await;
    for i in 0..6 {
        within(PROMPTLY, producer.request(format!("job-{i}").as_bytes()))
            .await
            .expect("confirmed");
    }
    within(DEADLINE, async {
        while harness.broker.consumer_count("/jobs") != Some(0) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    harness
        .until("/jobs", "everything is the queue's again", |s| {
            s.messages == 6 && s.unsettled == 0
        })
        .await;
    drop(held);
}

/// A consumer that stops reading is not only unsubscribed: its connection is
/// closed, so its client notices and its redial subscribes again. It is closed
/// within the deadline plus a margin, a healthy consumer beside it keeps
/// receiving throughout, and what the stalled one held is the healthy one's
/// to take.
#[tokio::test]
async fn a_consumer_that_stops_reading_has_its_connection_closed() {
    const IO_TIMEOUT: Duration = Duration::from_millis(400);
    /// What the close may take beyond the deadline: the broker's reaction and
    /// the loopback, not a second deadline.
    const MARGIN: Duration = Duration::from_millis(1500);

    let harness = Harness::start(BrokerConfig {
        io_timeout: IO_TIMEOUT,
        ..BrokerConfig::with_queues(["/jobs"])
    })
    .await;
    let (_stalled_rt, stalled) = harness.stalled_consumer("/jobs").await;
    let mut events = stalled.events();
    // Subscribed before anything is published and given credit only once the
    // stalled consumer has been handed its messages, so it joins before the
    // stall and cannot take the stalled consumer's share.
    let (_healthy_rt, healthy) = harness
        .consumer("/jobs", &[""], RuntimeConfig::default())
        .await;

    let producer = harness.requester("/jobs").await;
    for i in 0..6 {
        within(PROMPTLY, producer.request(format!("job-{i}").as_bytes()))
            .await
            .expect("confirmed");
    }
    let stalled_since = Instant::now();
    healthy.grant("", 100).await.expect("grant");

    let lost = within(IO_TIMEOUT + MARGIN, async {
        loop {
            match events.recv().await.expect("the endpoint lives") {
                PeerEvent::Lost { cause, .. } => break cause,
                _ => continue,
            }
        }
    })
    .await;
    assert_eq!(lost, LossCause::PeerClosed);
    assert!(
        stalled_since.elapsed() < IO_TIMEOUT + MARGIN,
        "closed after {:?}",
        stalled_since.elapsed()
    );

    // The healthy consumer ends up with every message, the ones the stalled
    // consumer was holding included.
    let mut seen = std::collections::BTreeSet::new();
    within(DEADLINE, async {
        while seen.len() < 6 {
            seen.insert(body_of(healthy.recv().await.expect("a delivery")).await);
        }
    })
    .await;

    // The redial subscribes again: the queue counts two consumers once more.
    within(DEADLINE, async {
        while harness.broker.consumer_count("/jobs") != Some(2) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// One connection's filters add up: eight messages and three matching
/// filters of one consumer, which may hold three unsettled in all, leave the
/// rest for the next consumer instead of pinning the queue.
#[tokio::test]
async fn one_connection_cannot_pin_a_queue_through_its_filters() {
    let harness = Harness::start(BrokerConfig {
        max_unsettled: 2,
        max_unsettled_per_consumer: 3,
        ..BrokerConfig::with_queues(["/jobs"])
    })
    .await;
    let (_rt, greedy) = harness
        .consumer("/jobs", &["", "j", "#"], RuntimeConfig::default())
        .await;
    for filter in ["", "j", "#"] {
        greedy.grant(filter, 100).await.expect("grant");
    }
    let producer = harness.requester("/jobs").await;
    for i in 0..8 {
        within(
            PROMPTLY,
            producer.request_with(
                TransferMeta::default().with_topic("j"),
                format!("job-{i}").as_bytes(),
            ),
        )
        .await
        .expect("confirmed");
    }

    let mut taken = 0;
    while let Ok(Ok(transfer)) = timeout(SILENCE, greedy.recv()).await {
        let _ = body_of(transfer).await;
        taken += 1;
    }
    assert_eq!(
        taken, 3,
        "three filters of two each would be six; the connection's own bound is three"
    );

    let (_other_rt, other) = harness
        .consumer("/jobs", &[""], RuntimeConfig::default())
        .await;
    other.grant("", 10).await.expect("grant");
    let got = promptly(&other).await;
    assert!(got.starts_with("job-"), "{got}");
}

/// A consumer with credit left and a full set of unsettled deliveries is
/// ineligible, and a queue behind it is not walked for every message that
/// arrives. Eight thousand empty messages are admitted in a time the quadratic
/// scan could not manage.
#[tokio::test]
async fn a_silent_consumer_does_not_make_admission_quadratic() {
    const MESSAGES: usize = 8_000;
    let harness = Harness::start(BrokerConfig {
        queue_bytes: 64 << 20,
        ..BrokerConfig::with_queues(["/jobs"])
    })
    .await;
    let (_rt, silent) = harness
        .consumer("/jobs", &[""], RuntimeConfig::default())
        .await;
    silent.grant("", 100_000).await.expect("grant");

    let producer = harness.requester("/jobs").await;
    within(Duration::from_secs(30), async {
        for _ in 0..MESSAGES {
            producer.request(b"").await.expect("admitted");
        }
    })
    .await;
    harness
        .until("/jobs", "everything is held or out", |s| {
            s.messages + s.unsettled == MESSAGES && s.unsettled == 256
        })
        .await;
}
