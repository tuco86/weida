//! The driver against a peer that stops reading, floods, or announces what it
//! should not.
//!
//! Everything here is a hostile or merely broken *server*, because that is the
//! side a client does not control: one that stops draining its socket, one
//! that sends nothing but `PING`, one that tells the client it may be sent four
//! gigabytes. The assertions are about what the client does about it — returns
//! an error, keeps reading, fails at its own bound — and every wait is bounded,
//! so a wrong turn fails the test rather than hanging the suite.
//!
//! The scripted server stops reading by **not reading**. The listener it
//! accepts on has a small receive buffer ([`Server::listen_small_window`]), so
//! the client's socket fills after a few kilobytes and a test does not have to
//! push megabytes to find out what a full socket does.

use crate::support;

use std::time::Duration;

use support::{DEADLINE, PLAIN_INFO, Server, connected, options};
use tokio::io::AsyncWriteExt;
use weida_nats::{Error, State};

/// A server that stops draining its socket, and a client that keeps
/// publishing: the publisher is told, in bounded time, rather than blocked for
/// ever behind a write that never completes.
///
/// The command queue is small on purpose. With the default of 256 slots a
/// publisher would be handed 256 messages of room before it felt anything.
#[tokio::test]
async fn a_peer_that_stops_reading_fails_the_connection_instead_of_hanging_it() {
    let mut options = options();
    options.write_timeout = Duration::from_millis(300);
    options.outgoing_queue = 4;
    let (nats, _server) = connected(Server::listen_small_window().await, PLAIN_INFO, options).await;

    let chunk = vec![b'x'; 1 << 20];
    let publishing = async {
        for _ in 0..64 {
            nats.publish("flood", &chunk).await?;
        }
        Ok::<(), Error>(())
    };
    let outcome = tokio::time::timeout(DEADLINE, publishing)
        .await
        .expect("a publish returned an answer rather than waiting behind a stuck write");
    let error = outcome.expect_err("64 MiB cannot go into a socket nobody reads");
    assert!(matches!(error, Error::ConnectionGone), "{error}");

    match tokio::time::timeout(DEADLINE, nats.closed())
        .await
        .expect("the connection reports why it ended")
    {
        State::Failed(why) => assert!(why.contains("accepted no octets"), "{why}"),
        other => panic!("expected a failed connection, got {other:?}"),
    }
}

/// A server that stops reading and sends `PING` in a loop keeps the client's
/// read path busy for as long as it likes, and that must not keep a blocked
/// write alive: the stall clock is moved by the socket taking octets and by
/// nothing else. Reads, answers to the `PING`s and every other loop pass go on
/// around a write that is not completing, and the connection still ends.
#[tokio::test]
async fn a_ping_flood_does_not_keep_a_blocked_write_alive() {
    let mut options = options();
    options.write_timeout = Duration::from_millis(300);
    options.outgoing_queue = 4;
    let (nats, mut server) =
        connected(Server::listen_small_window().await, PLAIN_INFO, options).await;

    let flood = tokio::spawn(async move {
        let burst = "PING\r\n".repeat(1024);
        // Writes only, for as long as there is anyone to write to.
        while server.stream.write_all(burst.as_bytes()).await.is_ok() {}
    });
    let publisher = {
        let nats = nats.clone();
        tokio::spawn(async move {
            let chunk = vec![0u8; 1 << 20];
            while nats.publish("flood", &chunk).await.is_ok() {}
        })
    };
    let state = tokio::time::timeout(DEADLINE, nats.closed())
        .await
        .expect("the connection failed instead of being kept busy behind a stuck write");
    flood.abort();
    publisher.abort();
    match state {
        State::Failed(why) => assert!(why.contains("accepted no octets"), "{why}"),
        other => panic!("expected a failed connection, got {other:?}"),
    }
}

/// A write that is blocked does not stop the driver reading: a message the
/// server sends in the meantime reaches its subscription.
///
/// The write timeout is long here, so what is being tested is that the read
/// happens *during* the stall and not that the stall ends.
#[tokio::test]
async fn reads_keep_flowing_while_a_write_is_blocked() {
    let mut options = options();
    options.write_timeout = Duration::from_secs(30);
    options.outgoing_queue = 4;
    let (nats, mut server) =
        connected(Server::listen_small_window().await, PLAIN_INFO, options).await;
    let mut news = nats.subscribe("news").await.expect("subscribed");
    assert_eq!(server.read_op().await, "SUB news - 1");

    // From here the server reads nothing, and the client publishes until the
    // socket is full.
    let publisher = {
        let nats = nats.clone();
        tokio::spawn(async move {
            let chunk = vec![0u8; 1 << 20];
            while nats.publish("flood", &chunk).await.is_ok() {}
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;

    server.send_msg("news", "1", None, b"still reading").await;
    let message = tokio::time::timeout(DEADLINE, news.next())
        .await
        .expect("a message arrived although the client's write was blocked")
        .expect("the subscription is live");
    assert_eq!(message.payload, &b"still reading"[..]);
    publisher.abort();
}

/// The ping clock is not restarted by traffic: a publisher that never pauses
/// still tests its peer every interval, which is the only way it would ever
/// learn that the peer had gone.
#[tokio::test]
async fn a_publisher_that_never_pauses_still_pings() {
    let mut options = options();
    options.ping_interval = Duration::from_millis(150);
    options.max_pings_out = 100;
    let (nats, mut server) = connected(Server::listen().await, PLAIN_INFO, options).await;

    // Something goes out every ten milliseconds, for far longer than the
    // interval.
    let publisher = {
        let nats = nats.clone();
        tokio::spawn(async move {
            while nats.publish("busy", b"x").await.is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    let saw_a_ping = async { while server.read_op().await != "PING" {} };
    tokio::time::timeout(Duration::from_secs(3), saw_a_ping)
        .await
        .expect("a connection that never goes quiet still pings its peer");
    publisher.abort();
}

/// `close` on a connection whose peer stopped reading ends it, in bounded
/// time, instead of waiting behind the stuck write for ever.
#[tokio::test]
async fn close_ends_a_connection_whose_peer_stopped_reading() {
    let mut options = options();
    options.write_timeout = Duration::from_millis(300);
    options.outgoing_queue = 4;
    let (nats, _server) = connected(Server::listen_small_window().await, PLAIN_INFO, options).await;

    let publisher = {
        let nats = nats.clone();
        tokio::spawn(async move {
            let chunk = vec![0u8; 1 << 20];
            while nats.publish("flood", &chunk).await.is_ok() {}
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .expect("close returned")
        .expect("closing never fails");
    let state = tokio::time::timeout(DEADLINE, nats.closed())
        .await
        .expect("the connection is over");
    assert!(!state.is_usable(), "{state:?}");
    publisher.abort();
}

/// Everything queued between two writes goes out together, and goes out whole
/// and in the order it was queued.
#[tokio::test]
async fn a_burst_of_publishes_arrives_whole_and_in_order() {
    let (nats, mut server) = connected(Server::listen().await, PLAIN_INFO, options()).await;
    for n in 0..2000 {
        nats.publish("burst", n.to_string()).await.expect("queued");
    }
    let flushing = {
        let nats = nats.clone();
        tokio::spawn(async move { nats.flush().await })
    };
    for n in 0..2000 {
        assert_eq!(server.read_op().await, format!("PUB burst - {n}"));
    }
    assert_eq!(server.read_op().await, "PING");
    server.write(b"PONG\r\n").await;
    tokio::time::timeout(DEADLINE, flushing)
        .await
        .expect("the flush finished")
        .expect("the task ran")
        .expect("a PONG answered it");
}

/// `close` writes what was queued before it and only then ends the transport.
#[tokio::test]
async fn close_writes_what_was_queued_before_it() {
    let (nats, mut server) = connected(Server::listen().await, PLAIN_INFO, options()).await;
    for n in 0..100 {
        nats.publish("last.words", n.to_string())
            .await
            .expect("queued");
    }
    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .expect("close returned")
        .expect("closing never fails");
    for n in 0..100 {
        assert_eq!(server.read_op().await, format!("PUB last.words - {n}"));
    }
    server.expect_end_of_stream().await;
}

/// What the peer announces in `INFO.max_payload` is the bound on what the
/// client *publishes*. It is not the bound on what the client accepts: the
/// number comes from the peer, unauthenticated, so the peer would be choosing
/// how much memory to commit. A `MSG` declaring three gigabytes ends the
/// connection at the client's own cap, from the control line, with the body
/// never sent and never reserved.
#[tokio::test]
async fn a_server_cannot_raise_the_inbound_cap_by_announcing_a_huge_max_payload() {
    const HUGE_INFO: &str =
        "{\"server_id\":\"S1\",\"proto\":1,\"max_payload\":4294967296,\"headers\":true}";
    let mut options = options();
    options.max_inbound_payload = 64 * 1024;
    let (nats, mut server) = connected(Server::listen().await, HUGE_INFO, options).await;
    assert_eq!(
        nats.max_payload(),
        4_294_967_296,
        "the announcement still bounds what is published"
    );

    let _big = nats.subscribe("big").await.expect("subscribed");
    assert_eq!(server.read_op().await, "SUB big - 1");
    server.write(b"MSG big 1 3221225472\r\n").await;

    match tokio::time::timeout(DEADLINE, nats.closed())
        .await
        .expect("the connection failed at the cap instead of waiting for the body")
    {
        State::Failed(why) => {
            assert!(why.contains("3221225472"), "{why}");
            assert!(why.contains("65536"), "the cap that refused it: {why}");
        }
        other => panic!("expected a failed connection, got {other:?}"),
    }
}

/// A queue of a thousand messages is not a bound: the connection holds a byte
/// budget, and a subscription nobody reads fills it and then loses copies, in
/// a count the application can read, rather than growing.
#[tokio::test]
async fn queued_bytes_stay_under_the_budget_and_the_overflow_is_counted() {
    let budget = 256 * 1024;
    let mut options = options();
    options.max_queued_bytes = budget;
    options.subscription_queue = 100_000;
    let (nats, mut server) = connected(Server::listen().await, PLAIN_INFO, options).await;
    let mut bulk = nats.subscribe("bulk").await.expect("subscribed");
    assert_eq!(server.read_op().await, "SUB bulk - 1");

    let payload = [b'x'; 4096];
    for _ in 0..600 {
        server.send_msg("bulk", "1", None, &payload).await;
    }
    // A PONG is behind every message before it.
    let flushing = {
        let nats = nats.clone();
        tokio::spawn(async move { nats.flush().await })
    };
    assert_eq!(server.read_op().await, "PING");
    server.write(b"PONG\r\n").await;
    tokio::time::timeout(DEADLINE, flushing)
        .await
        .expect("the flush finished")
        .expect("the task ran")
        .expect("a PONG answered it");

    let mut received = 0u64;
    let mut octets = 0usize;
    while let Some(message) = bulk.try_next() {
        received += 1;
        octets += message.payload.len();
    }
    assert!(received > 0, "the budget holds some");
    assert!(
        octets <= budget,
        "{octets} octets were queued against a budget of {budget}"
    );
    assert_eq!(
        received + bulk.dropped(),
        600,
        "every message was either queued or counted as dropped"
    );
}

/// A full queue drops the copy and says how many it has dropped.
#[tokio::test]
async fn a_full_queue_counts_what_it_drops() {
    let mut options = options();
    options.subscription_queue = 1;
    let (nats, mut server) = connected(Server::listen().await, PLAIN_INFO, options).await;
    let mut slow = nats.subscribe("slow").await.expect("subscribed");
    assert_eq!(server.read_op().await, "SUB slow - 1");

    for _ in 0..100 {
        server.send_msg("slow", "1", None, b"m").await;
    }
    let flushing = {
        let nats = nats.clone();
        tokio::spawn(async move { nats.flush().await })
    };
    assert_eq!(server.read_op().await, "PING");
    server.write(b"PONG\r\n").await;
    tokio::time::timeout(DEADLINE, flushing)
        .await
        .expect("the flush finished")
        .expect("the task ran")
        .expect("a PONG answered it");

    assert_eq!(slow.dropped(), 99, "one fit, and the rest were counted");
    assert!(slow.try_next().is_some());
    assert!(slow.try_next().is_none());
}

/// Dropping every handle ends the connection: what was queued is written, and
/// then the transport is closed, which is the whole of a NATS close. A driver
/// that kept a sender to its own command queue would never see the queue
/// close, and the socket and the task would live for as long as the server let
/// them.
#[tokio::test]
async fn dropping_every_handle_ends_the_connection() {
    let (nats, mut server) = connected(Server::listen().await, PLAIN_INFO, options()).await;
    nats.publish("gone.dark", b"x").await.expect("queued");
    drop(nats);

    assert_eq!(server.read_op().await, "PUB gone.dark - x");
    server.expect_end_of_stream().await;
}

/// A subscription is a handle too: it keeps the connection open after the
/// `Connection` is dropped, so that messages go on arriving, and ending it is
/// what ends the connection.
#[tokio::test]
async fn a_subscription_keeps_the_connection_open_after_the_connection_is_dropped() {
    let (nats, mut server) = connected(Server::listen().await, PLAIN_INFO, options()).await;
    let mut news = nats.subscribe("news").await.expect("subscribed");
    assert_eq!(server.read_op().await, "SUB news - 1");
    drop(nats);

    // Not closed, and nothing written: the subscription is still wanted.
    server.expect_silence().await;
    server.send_msg("news", "1", None, b"still here").await;
    let message = tokio::time::timeout(DEADLINE, news.next())
        .await
        .expect("a message arrived")
        .expect("the subscription is live");
    assert_eq!(message.payload, &b"still here"[..]);

    drop(news);
    assert_eq!(server.read_op().await, "UNSUB 1 -");
    server.expect_end_of_stream().await;
}

/// A flush is a barrier for what was written before it, and the only evidence
/// is the `PONG` to *its* `PING`. A `PONG` that answers an earlier `PING` —
/// one the server read before the writes this flush follows — says nothing
/// about them, so it must not complete it.
#[tokio::test]
async fn a_flush_is_answered_by_its_own_pong_and_not_an_earlier_ones() {
    let (nats, mut server) = connected(Server::listen().await, PLAIN_INFO, options()).await;

    let first = {
        let nats = nats.clone();
        tokio::spawn(async move { nats.flush().await })
    };
    assert_eq!(server.read_op().await, "PING");

    nats.publish("barrier", b"x").await.expect("queued");
    let mut second = {
        let nats = nats.clone();
        tokio::spawn(async move { nats.flush().await })
    };
    assert_eq!(server.read_op().await, "PUB barrier - x");
    assert_eq!(server.read_op().await, "PING");

    // The server answers the first `PING` and has not looked at the publish.
    server.write(b"PONG\r\n").await;
    tokio::time::timeout(DEADLINE, first)
        .await
        .expect("the first flush finished")
        .expect("the task ran")
        .expect("its own PONG answered it");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut second)
            .await
            .is_err(),
        "the second flush finished on a PONG that answered the first PING"
    );

    server.write(b"PONG\r\n").await;
    tokio::time::timeout(DEADLINE, second)
        .await
        .expect("the second flush finished")
        .expect("the task ran")
        .expect("its own PONG answered it");
}

/// A deadline meant as "never" — `Duration::MAX` — is a clock that cannot be
/// represented as an instant, and it must not be added to one: the driver
/// would panic the first time it armed the stall clock, and the connection
/// would die with the caller none the wiser. Here every deadline goes through
/// the runtime's timer, which saturates, so the connection works and the
/// publish reaches the server.
#[tokio::test]
async fn a_timeout_too_large_to_add_to_an_instant_does_not_panic_the_driver() {
    let mut options = options();
    options.handshake_timeout = Duration::MAX;
    options.write_timeout = Duration::MAX;
    options.ping_interval = Duration::MAX;
    let (nats, mut server) = connected(Server::listen().await, PLAIN_INFO, options).await;

    nats.publish("never", b"x").await.expect("queued");
    let seen = tokio::time::timeout(DEADLINE, server.read_op())
        .await
        .expect("the driver wrote the publish rather than dying");
    assert_eq!(seen, "PUB never - x");
    assert!(nats.state().is_usable());
    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .expect("the close finished")
        .expect("closed");
}
