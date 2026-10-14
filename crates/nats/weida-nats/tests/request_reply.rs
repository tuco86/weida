//! Request-reply over an inbox, and the two ways a request ends without an
//! answer.
//!
//! The test that matters most here is the pair at the bottom. "With
//! `no_responders` and headers negotiated, a request with no responder
//! returns that status **immediately instead of waiting out the timeout**" —
//! so one test asserts the `NATS/1.0 503` outcome *and the elapsed time*, and
//! its twin asserts that a request nobody answers at all ends in the timeout.
//! Only the timing assertion catches a client that quietly fell back to the
//! timeout path, which is why it is there.
//!
//! Every await is bounded, so a wrong turn fails the test rather than hanging
//! the suite.

mod support;

use std::time::{Duration, Instant};

use support::{DEADLINE, Server, options};
use weida_nats::{Connection, Error};
use weida_runtime::Exec;

/// The timeout every "nobody answered" test uses.
///
/// Long enough that a fast 503 is unambiguously faster than it, short enough
/// that the timeout test does not slow the suite down.
const WINDOW: Duration = Duration::from_millis(800);

/// A request subscribes to a unique inbox, publishes with that inbox as its
/// reply subject, and the answer comes back on it by ordinary subject
/// interest.
///
/// There is no correlation field anywhere: the reply subject *is* the
/// correlation. The scripted server proves it by reading the reply subject
/// off the `PUB` line and publishing the answer to exactly that subject, with
/// nothing else to go on.
#[tokio::test]
async fn a_request_carries_an_inbox_reply_subject_and_the_answer_returns_on_it() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;

        // One `SUB` for the whole inbox, on the first request — not one per
        // request.
        let sub = server.read_op().await;
        let inbox_pattern = sub
            .strip_prefix("SUB ")
            .and_then(|rest| rest.split(' ').next())
            .expect("a SUB line")
            .to_owned();
        assert!(
            inbox_pattern.starts_with("_INBOX.") && inbox_pattern.ends_with(".>"),
            "the inbox subscription takes everything under a unique stem: {sub}"
        );
        assert!(
            sub.ends_with(" - 1"),
            "no queue group, and a client sid: {sub}"
        );

        // The request, with its reply subject as the middle argument.
        let request = server.read_op().await;
        let mut parts = request.split(' ');
        assert_eq!(parts.next(), Some("PUB"));
        assert_eq!(parts.next(), Some("service.echo"));
        let reply_to = parts.next().expect("a reply subject").to_owned();
        assert_eq!(parts.next(), Some("ping"));
        assert!(
            reply_to.starts_with("_INBOX."),
            "the reply subject sits under the inbox prefix: {reply_to}"
        );

        // The responder's answer: an ordinary publication to the subject the
        // request named, routed to the requester by subject interest and by
        // nothing else. The scripted server has no correlation id to use,
        // because there is none.
        server.send_msg(&reply_to, "1", None, b"pong").await;
        (inbox_pattern, reply_to)
    });

    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");

    let reply = tokio::time::timeout(
        DEADLINE,
        nats.request("service.echo", b"ping", Duration::from_secs(2)),
    )
    .await
    .expect("the request finished")
    .expect("a reply");
    assert_eq!(reply.payload, b"pong");

    let (pattern, reply_to) = tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reply.subject_str(),
        Some(reply_to.as_str()),
        "the reply arrived on the subject the request named"
    );
    assert!(
        weida_nats::subject::matches(pattern.as_bytes(), reply_to.as_bytes()),
        "{reply_to} is under {pattern}"
    );

    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .unwrap()
        .unwrap();
}

/// Two requests on one connection get two reply subjects and share the one
/// inbox subscription: the `SUB` is written once and the second request
/// reuses it.
#[tokio::test]
async fn requests_share_one_inbox_subscription_and_never_share_a_reply_subject() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;

        let sub = server.read_op().await;
        assert!(sub.starts_with("SUB _INBOX."), "{sub}");

        let first = server.read_op().await;
        let first_reply = reply_subject_of(&first);
        server.send_msg(&first_reply, "1", None, b"one").await;

        // No second SUB: the next operation is the second request.
        let second = server.read_op().await;
        let second_reply = reply_subject_of(&second);
        server.send_msg(&second_reply, "1", None, b"two").await;
        (first_reply, second_reply)
    });

    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");

    let first = tokio::time::timeout(
        DEADLINE,
        nats.request("service.echo", b"a", Duration::from_secs(2)),
    )
    .await
    .expect("finished")
    .expect("a reply");
    let second = tokio::time::timeout(
        DEADLINE,
        nats.request("service.echo", b"b", Duration::from_secs(2)),
    )
    .await
    .expect("finished")
    .expect("a reply");

    assert_eq!(first.payload, b"one");
    assert_eq!(second.payload, b"two");

    let (first_reply, second_reply) = tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        first_reply, second_reply,
        "each request gets its own reply subject, because that subject is the \
         whole of the correlation"
    );

    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .unwrap()
        .unwrap();
}

/// The scatter-gather form collects several responses inside one window and
/// returns what arrived when the window closed.
#[tokio::test]
async fn scatter_gather_collects_several_responses_inside_one_window() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert!(server.read_op().await.starts_with("SUB _INBOX."));
        let request = server.read_op().await;
        let reply_to = reply_subject_of(&request);
        // Three independent responders answering the same request, which is
        // what NATS documents as scatter-gather.
        for answer in [&b"alpha"[..], b"beta", b"gamma"] {
            server.send_msg(&reply_to, "1", None, answer).await;
        }
    });

    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");

    let replies = tokio::time::timeout(DEADLINE, nats.request_many("service.who", b"?", WINDOW, 8))
        .await
        .expect("the window closed")
        .expect("responses");

    let payloads: Vec<Vec<u8>> = replies.into_iter().map(|reply| reply.payload).collect();
    assert_eq!(
        payloads,
        vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
    );

    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .unwrap()
        .unwrap();
}

/// The collection is bounded: `max_responses` is required and the window
/// closes as soon as that many have arrived, so a responder that keeps
/// answering cannot fill memory inside one window.
#[tokio::test]
async fn scatter_gather_stops_at_the_bound_the_caller_named() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert!(server.read_op().await.starts_with("SUB _INBOX."));
        let request = server.read_op().await;
        let reply_to = reply_subject_of(&request);
        for n in 0..6u8 {
            server.send_msg(&reply_to, "1", None, &[b'0' + n]).await;
        }
    });

    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");

    let replies = tokio::time::timeout(DEADLINE, nats.request_many("service.who", b"?", WINDOW, 2))
        .await
        .expect("finished")
        .expect("responses");
    assert_eq!(replies.len(), 2, "the caller asked for two");

    assert!(
        matches!(
            nats.request_many("service.who", b"?", WINDOW, 0).await,
            Err(Error::Configuration(_))
        ),
        "a scatter-gather that collects nothing is a publish"
    );

    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .unwrap()
        .unwrap();
}

/// An empty window is a result, not an error: the caller asked for a window
/// rather than for an answer.
#[tokio::test]
async fn scatter_gather_returns_an_empty_collection_when_nothing_arrives() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert!(server.read_op().await.starts_with("SUB _INBOX."));
        assert!(server.read_op().await.starts_with("PUB service.who"));
        // And then nothing at all.
        server
    });

    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");

    let replies = tokio::time::timeout(
        DEADLINE,
        nats.request_many("service.who", b"?", Duration::from_millis(200), 4),
    )
    .await
    .expect("the window closed")
    .expect("an empty collection is a result");
    assert!(replies.is_empty());

    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .unwrap()
        .unwrap();
}

/// **The no-responder path.** With `headers` and `no_responders` negotiated,
/// a request to a subject nobody is listening on comes back as a
/// `NATS/1.0 503` on the inbox, and the client turns it into a distinct error
/// **immediately** rather than waiting out the caller's window.
///
/// The elapsed time is asserted, because "immediately instead of waiting out
/// the timeout" is the whole point of the feature and a client that silently
/// fell back to the timeout path would pass every other assertion here.
#[tokio::test]
async fn a_request_with_no_responder_fails_fast_with_the_503_status() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        let connect = server.handshake_full().await;
        // The two capabilities this outcome depends on, both claimed.
        assert!(connect.contains("\"headers\":true"), "{connect}");
        assert!(connect.contains("\"no_responders\":true"), "{connect}");

        assert!(server.read_op().await.starts_with("SUB _INBOX."));
        let request = server.read_op().await;
        let reply_to = reply_subject_of(&request);
        // What a server sends the instant it finds no interest in the
        // request's subject: a status message, no payload.
        server.send_status(&reply_to, "1", 503).await;
    });

    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");
    assert!(nats.headers_supported());

    let started = Instant::now();
    let error = tokio::time::timeout(DEADLINE, nats.request("nobody.here", b"?", WINDOW))
        .await
        .expect("the request finished")
        .expect_err("no responder");
    let elapsed = started.elapsed();

    assert!(
        matches!(error, Error::NoResponders),
        "the 503 is its own outcome, not a timeout: {error}"
    );
    assert!(
        elapsed < WINDOW / 2,
        "the 503 must arrive immediately, not at the end of the window: \
         {elapsed:?} of {WINDOW:?}"
    );

    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .unwrap()
        .unwrap();
}

/// **The timeout path**, which is the other half of the same assertion: a
/// request that nobody answers at all ends in the caller's window elapsing,
/// and it is reported as the timeout it is.
///
/// Together with the test above this is the difference the pair exists to
/// show: same request API, two different facts, two different errors, and two
/// very different elapsed times.
#[tokio::test]
async fn a_request_nobody_answers_ends_in_the_callers_timeout() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert!(server.read_op().await.starts_with("SUB _INBOX."));
        assert!(server.read_op().await.starts_with("PUB slow.service"));
        // A responder exists — so no 503 — and it simply never answers.
        server
    });

    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");

    let started = Instant::now();
    let error = tokio::time::timeout(DEADLINE, nats.request("slow.service", b"?", WINDOW))
        .await
        .expect("the request finished")
        .expect_err("nobody answered");
    let elapsed = started.elapsed();

    match error {
        Error::RequestTimeout { after } => assert_eq!(after, WINDOW),
        other => panic!("expected the caller's timeout, got {other}"),
    }
    assert!(
        elapsed >= WINDOW,
        "the window is the caller's and it is waited out: {elapsed:?}"
    );

    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .unwrap()
        .unwrap();
}

/// A 503 ends a scatter-gather too, and as the same distinct error: it is a
/// definite answer rather than an empty window.
#[tokio::test]
async fn a_503_ends_a_scatter_gather_as_the_no_responder_error() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert!(server.read_op().await.starts_with("SUB _INBOX."));
        let request = server.read_op().await;
        let reply_to = reply_subject_of(&request);
        server.send_status(&reply_to, "1", 503).await;
    });

    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");

    let started = Instant::now();
    let error = tokio::time::timeout(DEADLINE, nats.request_many("nobody.here", b"?", WINDOW, 4))
        .await
        .expect("finished")
        .expect_err("no responder");
    assert!(matches!(error, Error::NoResponders), "{error}");
    assert!(
        started.elapsed() < WINDOW,
        "a definite answer does not wait out the window"
    );

    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .unwrap()
        .unwrap();
}

/// A request on a connection that is gone fails rather than hanging, which is
/// the same promise the window makes by a different route.
#[tokio::test]
async fn a_request_on_a_lost_connection_fails_rather_than_hanging() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert!(server.read_op().await.starts_with("SUB _INBOX."));
        assert!(server.read_op().await.starts_with("PUB service.echo"));
        // The server drops the transport mid-request.
        drop(server);
    });

    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");

    let error = tokio::time::timeout(
        DEADLINE,
        nats.request("service.echo", b"?", Duration::from_secs(30)),
    )
    .await
    .expect("the request finished well inside its own 30 s window")
    .expect_err("the connection went away");
    assert!(matches!(error, Error::ConnectionGone), "{error}");

    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

/// The inbox map is bounded, because every entry is a request that may never
/// be answered.
#[tokio::test]
async fn the_pending_request_map_is_bounded() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert!(server.read_op().await.starts_with("SUB _INBOX."));
        assert!(server.read_op().await.starts_with("PUB slow.service"));
        assert!(server.read_op().await.starts_with("PUB slow.service"));
        server
    });

    let mut options = options();
    options.max_pending_requests = 2;
    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("finished")
    .expect("opened");

    // Two requests in flight, neither answered, and then a third.
    let window = Duration::from_millis(700);
    let first = nats.request("slow.service", b"1", window);
    let second = nats.request("slow.service", b"2", window);
    let outcomes = tokio::time::timeout(DEADLINE, async {
        tokio::join!(first, second, async {
            // The two above have to have registered before the third asks for
            // room, and the only ordering tool available is the driver's own
            // FIFO: the third request's Register command is queued behind
            // theirs, so by the time it is handled the map is full.
            tokio::time::sleep(Duration::from_millis(150)).await;
            nats.request("slow.service", b"3", window).await
        })
    })
    .await
    .expect("all three finished");

    assert!(matches!(outcomes.0, Err(Error::RequestTimeout { .. })));
    assert!(matches!(outcomes.1, Err(Error::RequestTimeout { .. })));
    assert!(
        matches!(outcomes.2, Err(Error::TooManyPendingRequests { max: 2 })),
        "{:?}",
        outcomes.2.as_ref().map(|_| ())
    );

    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, nats.close())
        .await
        .unwrap()
        .unwrap();
}

/// The reply subject off a rendered `PUB <subject> <reply-to> <payload>`
/// line.
///
/// The middle argument is the reply subject, and the *only* thing that says
/// so is that there are three arguments rather than two — which is the
/// protocol's argument-count rule, enforced by the codec both ways.
fn reply_subject_of(rendered: &str) -> String {
    let mut parts = rendered.split(' ');
    assert_eq!(parts.next(), Some("PUB"), "{rendered}");
    let _subject = parts.next().expect("a subject");
    let reply_to = parts.next().expect("a reply subject").to_owned();
    assert_ne!(reply_to, "-", "a request always carries a reply subject");
    reply_to
}
