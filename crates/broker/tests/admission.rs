//! What a producer observes when it sends to a queue.
//!
//! Every assertion here is from the producer's side of a real QUIC connection:
//! the broker has no test hooks, and the confirm, the refusal and the silence
//! are the whole observable surface of admission.

use std::time::Duration;

use tokio::task::JoinSet;
use weida::{
    Acknowledgement, ClientTls, ClientTrust, Error, Identity, Pusher, Requester, Runtime,
    RuntimeConfig, TransferMeta,
};
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

/// What a producer is promised while another peer holds its streams open.
const PROMPTLY: Duration = Duration::from_secs(2);

/// A broker with room for four admissions at once, two of them per
/// connection, and the default deadline: what is held below stays held for
/// the whole test. It asks every peer for a key, so that peers can be told
/// apart.
async fn narrow() -> Harness {
    Harness::start_with(
        BrokerConfig {
            max_inflight_admissions: 4,
            max_inflight_admissions_per_connection: 2,
            ..BrokerConfig::with_queues(["/jobs"])
        },
        |tls| tls.require_client(ClientTrust::AnyKey),
    )
    .await
}

impl Harness {
    /// The credentials of a peer that proved a key of its own: a different
    /// peer from every other one.
    fn peer(&self) -> ClientTls {
        self.client
            .clone()
            .with_identity(Identity::generate().expect("identity"))
    }
}

/// One connection can open as many streams as its limits allow and hold each
/// for the deadline. Past its share of the admissions a peer's streams are
/// refused at once, and the permits it was refused for are still there for
/// everybody else.
#[tokio::test]
async fn one_peer_cannot_hold_every_admission_and_keeps_the_rest_waiting() {
    let harness = narrow().await;
    let hostile = harness.runtime.requester(harness.peer());
    hostile
        .connect(&harness.url("/jobs"))
        .await
        .expect("connect");

    // Four exchanges, each with its header and one byte of a megabyte it
    // never finishes.
    let mut held = Vec::new();
    let mut replies = JoinSet::new();
    for _ in 0..4 {
        let (mut transfer, reply) = tokio::time::timeout(
            DEADLINE,
            hostile.open(TransferMeta::default().with_content_len(1 << 20)),
        )
        .await
        .expect("opened in time")
        .expect("open");
        tokio::time::timeout(DEADLINE, transfer.write_all(b"x"))
            .await
            .expect("written in time")
            .expect("the byte");
        held.push(transfer);
        replies.spawn(async move { reply.recv().await.map(|_| ()) });
    }

    // Two of them hold the peer's whole share; the other two are refused,
    // promptly, instead of waiting for permits that would not come.
    for _ in 0..2 {
        let outcome = tokio::time::timeout(PROMPTLY, replies.join_next())
            .await
            .expect("a refusal inside the deadline")
            .expect("a reply")
            .expect("the task");
        assert!(
            matches!(outcome, Err(Error::Rejected)),
            "a stream past the peer's share is `Rejected`: {outcome:?}"
        );
    }

    // And a producer that is another peer is read and confirmed while the
    // first still holds its streams.
    let other = harness.runtime.requester(harness.peer());
    other.connect(&harness.url("/jobs")).await.expect("connect");
    tokio::time::timeout(PROMPTLY, other.request(b"through"))
        .await
        .expect("the other peer is not made to wait for the stalled one")
        .expect("confirmed");
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 1);

    replies.abort_all();
    drop(held);
}

/// The same bound holds for one-way transfers, which have no reply half to
/// refuse on: the stalled peer's streams hold its share and no more, and
/// another peer's transfer is admitted.
#[tokio::test]
async fn one_peer_holding_one_way_streams_open_does_not_stop_another_peer() {
    let harness = narrow().await;
    let hostile = harness.runtime.pusher(harness.peer());
    hostile
        .connect(&harness.url("/jobs"))
        .await
        .expect("connect");
    let mut held = Vec::new();
    for _ in 0..4 {
        let mut transfer = tokio::time::timeout(
            DEADLINE,
            hostile.open(TransferMeta::default().with_content_len(1 << 20)),
        )
        .await
        .expect("opened in time")
        .expect("open");
        tokio::time::timeout(DEADLINE, transfer.write_all(b"x"))
            .await
            .expect("written in time")
            .expect("the byte");
        held.push(transfer);
    }

    let other = harness.runtime.pusher(harness.peer());
    other.connect(&harness.url("/jobs")).await.expect("connect");
    tokio::time::timeout(PROMPTLY, other.send(b"through"))
        .await
        .expect("sent in time")
        .expect("queued");
    let deadline = tokio::time::Instant::now() + PROMPTLY;
    while harness.broker.stats("/jobs").expect("queue").messages != 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the other peer's transfer was never admitted while the first held its streams"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(held);
}

/// Peers that proved nothing look alike, so they are told apart by their
/// connections: one anonymous connection that holds its whole share is
/// refused for more, and another anonymous connection is admitted. One bucket
/// for every anonymous peer would let any one of them lock the rest out.
#[tokio::test]
async fn anonymous_connections_do_not_share_one_share() {
    let harness = Harness::start(BrokerConfig {
        max_inflight_admissions: 4,
        max_inflight_admissions_per_connection: 2,
        ..BrokerConfig::with_queues(["/jobs"])
    })
    .await;
    let hostile = harness.requester("/jobs").await;

    // Two exchanges, each with its header and one byte of a megabyte it
    // never finishes: the connection's whole share.
    let mut held = Vec::new();
    let mut replies = JoinSet::new();
    for _ in 0..2 {
        let (mut transfer, reply) = tokio::time::timeout(
            DEADLINE,
            hostile.open(TransferMeta::default().with_content_len(1 << 20)),
        )
        .await
        .expect("opened in time")
        .expect("open");
        tokio::time::timeout(DEADLINE, transfer.write_all(b"x"))
            .await
            .expect("written in time")
            .expect("the byte");
        held.push(transfer);
        replies.spawn(async move { reply.recv().await.map(|_| ()) });
    }
    // Both are admitted once each holds a reservation.
    let deadline = tokio::time::Instant::now() + PROMPTLY;
    while harness.broker.stats("/jobs").expect("queue").reserved < 2 * PER_MESSAGE_OVERHEAD {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the two exchanges were never admitted"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // A third from the same connection is past its share and refused.
    let (mut third, reply) = tokio::time::timeout(
        DEADLINE,
        hostile.open(TransferMeta::default().with_content_len(1 << 20)),
    )
    .await
    .expect("opened in time")
    .expect("open");
    tokio::time::timeout(DEADLINE, third.write_all(b"x"))
        .await
        .expect("written in time")
        .expect("the byte");
    let outcome = tokio::time::timeout(PROMPTLY, reply.recv())
        .await
        .expect("a refusal inside the deadline");
    assert!(
        matches!(outcome, Err(Error::Rejected)),
        "a stream past the connection's share is `Rejected`"
    );

    // Another anonymous connection — a runtime of its own, so a connection of
    // its own — is read and confirmed while the first holds its share.
    let other_runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let other = other_runtime.requester(harness.client.clone());
    other.connect(&harness.url("/jobs")).await.expect("connect");
    tokio::time::timeout(PROMPTLY, other.request(b"through"))
        .await
        .expect("another anonymous connection is not made to wait for the stalled one")
        .expect("confirmed");
    assert_eq!(harness.broker.stats("/jobs").expect("queue").messages, 1);

    replies.abort_all();
    drop((held, third));
}
