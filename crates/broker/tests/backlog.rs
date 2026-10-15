//! A backlog nobody ready wants, and what it costs every delivery behind it.
//!
//! A queue exists to hold messages nobody takes yet: a topic with no
//! subscriber, or subscribers that are all busy. The claim of this file is
//! that such a backlog is paid for **once**: what a delivery costs is what is
//! new to the consumers that could take it, never the length of the backlog
//! in front of it or the number of idle consumers beside it. A consumer's
//! write ending must not make the queue offer the whole backlog to everyone
//! again.

use std::time::Duration;

use tokio::time::timeout;
use weida::{Runtime, RuntimeConfig, Subscriber, TransferMeta};
use weida_broker::BrokerConfig;

mod common;

use common::{DEADLINE, Harness};

/// Messages nobody subscribes to, queued in front of everything that follows.
const COLD: usize = 8_000;
/// Consumers that are ready and want none of it.
const IDLE: usize = 200;
/// Messages somebody does want, behind the backlog.
const HOT: usize = 200;

async fn within<F: Future>(limit: Duration, f: F) -> F::Output {
    timeout(limit, f).await.expect("timed out")
}

impl Harness {
    /// A consumer on a runtime of its own, subscribed with `filters` and
    /// holding `credit` on each.
    async fn consumer_of(&self, filters: &[String], credit: u64) -> (Runtime, Subscriber) {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("consumer runtime");
        let subscriber = runtime.subscriber(self.client.clone());
        for filter in filters {
            subscriber.subscribe(filter).await.expect("subscribe");
        }
        let before = self.broker.consumer_count("/jobs").expect("queue");
        subscriber
            .connect(&self.url("/jobs"))
            .await
            .expect("connect");
        within(DEADLINE, async {
            while self.broker.consumer_count("/jobs") != Some(before + filters.len()) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        for filter in filters {
            subscriber.grant(filter, credit).await.expect("grant");
        }
        (runtime, subscriber)
    }
}

/// Eight thousand messages nobody wants and two hundred consumers that are
/// ready for something else: two hundred deliveries behind them still take a
/// moment, not the quarter of a million filter matches per delivery that
/// offering the backlog to every ready consumer after each write costs.
#[tokio::test]
async fn a_backlog_nobody_wants_does_not_slow_the_deliveries_behind_it() {
    let harness = Harness::start(BrokerConfig {
        queue_bytes: 64 << 20,
        ..BrokerConfig::with_queues(["/jobs"])
    })
    .await;
    let idle_filters: Vec<String> = (0..IDLE).map(|i| format!("idle.{i}")).collect();
    let (_idle_rt, _idle) = harness.consumer_of(&idle_filters, 1_000).await;
    let (_hot_rt, hot) = harness.consumer_of(&["hot".to_owned()], 100_000).await;

    let producer = harness.runtime.requester(harness.client.clone());
    producer
        .connect(&harness.url("/jobs"))
        .await
        .expect("connect");
    within(Duration::from_secs(30), async {
        for _ in 0..COLD {
            producer
                .request_with(TransferMeta::default().with_topic("cold"), b"")
                .await
                .expect("admitted");
        }
    })
    .await;
    assert_eq!(
        harness.broker.stats("/jobs").expect("queue").messages,
        COLD,
        "nobody ready wants the backlog, so it all waits"
    );

    within(Duration::from_secs(8), async {
        let publish = async {
            for i in 0..HOT {
                producer
                    .request_with(
                        TransferMeta::default().with_topic("hot"),
                        format!("hot-{i}").as_bytes(),
                    )
                    .await
                    .expect("admitted");
            }
        };
        let receive = async {
            for _ in 0..HOT {
                let transfer = hot.recv().await.expect("a delivery");
                transfer.collect(1 << 10).await.expect("collect");
            }
        };
        tokio::join!(publish, receive);
    })
    .await;
    assert_eq!(
        harness.broker.stats("/jobs").expect("queue").messages,
        COLD,
        "only the hot messages left the queue"
    );
}
