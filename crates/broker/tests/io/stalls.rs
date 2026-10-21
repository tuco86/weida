//! The tests of `tests/stalls.rs` that need the network, a process or real time.

use crate::stalls::common::Harness;
use std::time::Duration;
use weida::RuntimeConfig;
use weida_broker::BrokerConfig;

use crate::stalls::*;

/// A consumer with credit left and a full set of unsettled deliveries is
/// ineligible, and a queue behind it is not walked for every message that
/// arrives. Eight thousand empty messages are admitted in a time the quadratic
/// scan could not manage.
#[tokio::test(start_paused = true)]
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
