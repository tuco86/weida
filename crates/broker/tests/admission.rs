//! What a producer observes when it sends to a queue.
//!
//! Every assertion here is from the producer's side of a real QUIC connection:
//! the broker has no test hooks, and the confirm, the refusal and the silence
//! are the whole observable surface of admission.

use std::time::Duration;

use weida::{Acknowledgement, Error, Pusher, Requester, Runtime, RuntimeConfig};
use weida_broker::{Broker, BrokerConfig, PER_MESSAGE_OVERHEAD};

mod common;

use common::{DEADLINE, Harness};

impl Harness {
    /// A producer that waits for a certificate.
    async fn requester(&self, path: &str) -> Requester {
        let requester = self.runtime.requester(self.client.clone());
        requester.connect(&self.url(path)).await.expect("connect");
        requester
    }

    /// A producer that does not.
    async fn pusher(&self, path: &str) -> Pusher {
        let pusher = self.runtime.pusher(self.client.clone());
        pusher.connect(&self.url(path)).await.expect("connect");
        pusher
    }
}

/// The temporal decoupling a queue exists for: the confirm arrives although
/// nothing is consuming, and nothing ever will.
#[tokio::test]
async fn a_confirm_arrives_with_no_consumer_registered() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.requester("/jobs").await;

    // Nine bytes, not four: the queue charges the body's *allocation*, and
    // `Vec<u8>` never allocates fewer than eight bytes — so a shorter payload
    // would make the charge below an allocator's minimum rather than the
    // message's own size, which is not what this assertion is about.
    let body = b"work item";
    let reply = tokio::time::timeout(DEADLINE, producer.request(body))
        .await
        .expect("the confirm arrives well inside the deadline")
        .expect("the queue accepts the message");

    assert_eq!(
        reply.meta().achieved,
        Some(Acknowledgement::Accepted),
        "the reply half of the producer's exchange is the publisher confirm, \
         and it names the level the broker achieved (0018 §4.6)"
    );
    let stats = harness
        .broker
        .stats("/jobs")
        .expect("the queue is registered");
    assert_eq!(stats.messages, 1);
    assert_eq!(stats.bytes, PER_MESSAGE_OVERHEAD + body.len());
}

/// A confirm says `Accepted` and nothing stronger: the reply carries no
/// durability claim, because there is no store behind it.
#[tokio::test]
async fn the_confirm_never_claims_more_than_memory() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.requester("/jobs").await;
    let reply = producer.request(b"work").await.expect("accepted");
    let achieved = reply.meta().achieved.expect("a level");

    assert_eq!(achieved, Acknowledgement::Accepted);
    assert!(
        achieved < Acknowledgement::Stored,
        "`Stored` is a claim about surviving the process and this queue does \
         not (`docs/GUARANTEES.md` §1)"
    );
}

/// At the byte bound the producer is refused — and the refusal is a message on
/// a connection that stays usable, not a connection error.
#[tokio::test]
async fn a_full_queue_refuses_and_the_connection_survives() {
    // Room for exactly one 64-byte message.
    let config = BrokerConfig {
        queue_bytes: PER_MESSAGE_OVERHEAD + 64,
        ..BrokerConfig::with_queues(["/jobs"])
    };
    let harness = Harness::start(config).await;
    let producer = harness.requester("/jobs").await;

    let first = producer.request(&[b'a'; 64]).await.expect("the first fits");
    assert_eq!(first.meta().achieved, Some(Acknowledgement::Accepted));

    let refused = producer
        .request(b"one byte too many")
        .await
        .expect_err("the queue is at its bound");
    assert!(
        matches!(refused, Error::Rejected),
        "a cap refused before buffering is `{{REJECTED}}` on the reply half, \
         not a closed connection: {refused:?}"
    );

    // The same requester, the same connection: still alive, still refusing.
    let again = producer
        .request(b"and again")
        .await
        .expect_err("still at its bound");
    assert!(matches!(again, Error::Rejected), "{again:?}");
    assert_eq!(
        harness.broker.stats("/jobs").expect("registered").messages,
        1,
        "a refused message is not queued"
    );
}

/// A payload larger than the whole queue is refused without being buffered
/// first: the cap is what the read is bounded by.
#[tokio::test]
async fn a_payload_larger_than_the_queue_is_refused() {
    let config = BrokerConfig {
        queue_bytes: PER_MESSAGE_OVERHEAD + 16,
        ..BrokerConfig::with_queues(["/jobs"])
    };
    let harness = Harness::start(config).await;
    let producer = harness.requester("/jobs").await;

    let refused = producer
        .request(&[b'x'; 4096])
        .await
        .expect_err("four kibibytes into a sixteen byte queue");
    assert!(matches!(refused, Error::Rejected), "{refused:?}");
    assert_eq!(
        harness.broker.stats("/jobs").expect("registered").messages,
        0
    );
}

/// No queue on the path: the runtime's own dispatch answers, because the
/// broker never registered it. No declare frame exists, so this is the whole
/// story for an unknown queue (0018 §4.5).
#[tokio::test]
async fn an_unregistered_path_is_an_unknown_endpoint() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.requester("/nowhere").await;

    let refused = producer
        .request(b"work")
        .await
        .expect_err("no queue is registered there");
    assert!(
        matches!(refused, Error::UnknownEndpoint),
        "an endpoint path is an exact key into a flat map, and a miss is \
         `{{UNKNOWN_ENDPOINT}}` (PROTOCOL §9.4): {refused:?}"
    );
}

/// A one-way transfer is admitted and confirmed by nothing at all.
#[tokio::test]
async fn a_one_way_transfer_is_admitted_without_a_confirm() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.pusher("/jobs").await;

    producer.send(b"fire and forget").await.expect("queued");

    // The push returns when the FIN is queued, so the broker's admission is
    // concurrent with it: poll the queue rather than assume it has landed.
    let deadline = tokio::time::Instant::now() + DEADLINE;
    loop {
        let stats = harness.broker.stats("/jobs").expect("registered");
        if stats.messages == 1 {
            assert_eq!(stats.bytes, PER_MESSAGE_OVERHEAD + b"fire and forget".len());
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the one-way transfer never reached the queue"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The bounds are refused at configuration time, not at admission: a broker
/// asked for more queues than it may hold does not start.
#[tokio::test]
async fn more_queues_than_the_bound_is_a_configuration_error() {
    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let config = BrokerConfig {
        max_queues: 2,
        ..BrokerConfig::with_queues(["/a", "/b", "/c"])
    };

    let refused = Broker::new(&listener, config).expect_err("three queues, a bound of two");
    assert!(matches!(refused, Error::LimitExceeded), "{refused:?}");
}

/// A path another pattern already serves is a configuration error too, and the
/// broker leaves nothing half-registered.
#[tokio::test]
async fn a_path_another_pattern_holds_is_refused() {
    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let _replier = listener.replier("/taken").expect("replier");

    let refused = Broker::new(&listener, BrokerConfig::with_queues(["/free", "/taken"]))
        .expect_err("the second path is claimed");
    assert!(matches!(refused, Error::AlreadyRegistered), "{refused:?}");

    // `/free` was registered before the failure, so a second broker cannot
    // claim it either — the failure is a configuration error to fix, not a
    // state to retry blindly.
    let again = Broker::new(&listener, BrokerConfig::with_queues(["/free"]))
        .expect_err("the first path is still claimed by the failed attempt");
    assert!(matches!(again, Error::AlreadyRegistered), "{again:?}");
}
