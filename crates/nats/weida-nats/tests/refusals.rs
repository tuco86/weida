//! `-ERR` the server sends without ending the connection, and the ones that do
//! end it.
//!
//! The reference's table of `-ERR` reasons has a `Recoverable` column, and a
//! publish or a subscription outside the user's permissions, and a malformed
//! subject, are the rows that say yes. The server answers them and carries on,
//! so the connection, its subscriptions and its pending requests are all still
//! good, and the application is told through `Connection::refusals` — and,
//! for a refused subscription, on the subscription itself.
//!
//! Every await is bounded, so a wrong turn fails the test rather than hanging
//! the suite.

use crate::support;

use support::{DEADLINE, Server, options};
use tokio::time::timeout;
use weida_nats::{Connection, RefusalKind, State};
use weida_runtime::Exec;

fn err(reason: &str) -> Vec<u8> {
    format!("-ERR '{reason}'\r\n").into_bytes()
}

/// A publish the server will not take is reported, and the connection goes on
/// with everything it had: what one publish outside a user's permissions used
/// to cost was the whole connection.
#[tokio::test]
async fn a_refused_publish_is_reported_and_the_connection_goes_on() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "PUB orders.created - hello");
        server
            .write(&err(
                r#"Permissions Violation for Publish to "orders.created""#,
            ))
            .await;
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
        assert_eq!(
            server.read_op().await,
            "PUB orders.shipped - again",
            "the connection still carries publishes"
        );
        server
    });

    let nats = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");
    let mut refusals = nats.refusals();
    nats.publish("orders.created", b"hello")
        .await
        .expect("queued");

    let refusal = timeout(DEADLINE, refusals.next())
        .await
        .expect("the refusal arrived")
        .expect("the connection is open");
    assert_eq!(refusal.kind, RefusalKind::PermissionViolation);
    assert_eq!(refusal.subject.as_deref(), Some("orders.created"));
    assert_eq!(
        refusal.reason, r#"Permissions Violation for Publish to "orders.created""#,
        "the server's own sentence, without its quotes"
    );

    timeout(DEADLINE, nats.flush())
        .await
        .expect("the flush finished")
        .expect("a refusal is not a failed connection");
    assert_eq!(nats.state(), State::Connected);
    nats.publish("orders.shipped", b"again")
        .await
        .expect("queued");

    let _server = timeout(DEADLINE, server).await.unwrap().unwrap();
    timeout(DEADLINE, nats.close()).await.unwrap().unwrap();
}

/// A subscription the server refuses ends, and says why. The server holds no
/// such subscription, so no message will ever arrive, and a reader that went
/// on waiting for one would be a silence where an answer was given. Another
/// subscription on the same connection is untouched.
#[tokio::test]
async fn a_refused_subscription_ends_with_its_reason_and_the_connection_goes_on() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB secret.> - 1");
        server
            .write(&err(
                r#"Permissions Violation for Subscription to "secret.>""#,
            ))
            .await;
        assert_eq!(server.read_op().await, "SUB public.> - 2");
        server.send_msg("public.news", "2", None, b"headline").await;
        server
    });

    let nats = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");
    let mut refusals = nats.refusals();
    let mut secret = nats.subscribe("secret.>").await.expect("subscribed");

    assert!(
        timeout(DEADLINE, secret.next())
            .await
            .expect("the subscription ended rather than waiting for ever")
            .is_none()
    );
    let reason = secret.refusal().expect("it says why it ended");
    assert_eq!(reason.kind, RefusalKind::PermissionViolation);
    assert_eq!(reason.subject.as_deref(), Some("secret.>"));
    let told = timeout(DEADLINE, refusals.next()).await.unwrap().unwrap();
    assert_eq!(&told, reason, "the connection is told as well");

    let mut public = nats.subscribe("public.>").await.expect("still subscribing");
    let message = timeout(DEADLINE, public.next())
        .await
        .expect("a message arrived")
        .expect("the subscription is live");
    assert_eq!(message.payload, &b"headline"[..]);
    assert!(public.refusal().is_none());

    let _server = timeout(DEADLINE, server).await.unwrap().unwrap();
    timeout(DEADLINE, nats.close()).await.unwrap().unwrap();
}

/// A refusal that names no subscription — the server does not say which `SUB`
/// a malformed subject or the subscription limit refers to — is told to the
/// connection and ends nothing: guessing which entry to close would be
/// inventing the server's answer.
#[tokio::test]
async fn a_refusal_that_names_no_subscription_ends_none() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB events - 1");
        server.write(&err("Invalid Subject")).await;
        server.write(&err("Maximum Subscriptions Exceeded")).await;
        server.send_msg("events", "1", None, b"still here").await;
        server
    });

    let nats = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");
    let mut refusals = nats.refusals();
    let mut events = nats.subscribe("events").await.expect("subscribed");

    let first = timeout(DEADLINE, refusals.next()).await.unwrap().unwrap();
    assert_eq!(first.kind, RefusalKind::InvalidSubject);
    let second = timeout(DEADLINE, refusals.next()).await.unwrap().unwrap();
    assert_eq!(second.kind, RefusalKind::SubscriptionLimit);

    let message = timeout(DEADLINE, events.next())
        .await
        .expect("a message arrived")
        .expect("the subscription was not ended by a refusal that named none");
    assert_eq!(message.payload, &b"still here"[..]);
    assert!(events.refusal().is_none());
    assert_eq!(nats.state(), State::Connected);

    let _server = timeout(DEADLINE, server).await.unwrap().unwrap();
    timeout(DEADLINE, nats.close()).await.unwrap().unwrap();
}

/// Every other `-ERR` still ends the connection, in the server's own words,
/// and the readers of the refusals see the end.
#[tokio::test]
async fn any_other_error_still_ends_the_connection() {
    for reason in [
        "Authorization Violation",
        "Maximum Payload Violation",
        "Stale Connection",
        "Something Nobody Has Heard Of",
    ] {
        let exec = Exec::current().unwrap();
        let (listener, port) = Server::listen().await;
        let sentence = err(reason);
        let server = tokio::spawn(async move {
            let mut server = Server::accept(&listener).await;
            server.handshake_full().await;
            server.write(&sentence).await;
            server
        });

        let nats = timeout(
            DEADLINE,
            Connection::connect(&exec, "127.0.0.1", port, options()),
        )
        .await
        .expect("finished")
        .expect("opened");
        let mut refusals = nats.refusals();

        match timeout(DEADLINE, nats.closed()).await.expect("it ended") {
            State::Failed(text) => assert!(text.contains(reason), "{reason}: {text}"),
            other => panic!("{reason}: expected Failed, got {other:?}"),
        }
        assert!(
            timeout(DEADLINE, refusals.next())
                .await
                .expect("the reader was told the connection ended")
                .is_none(),
            "{reason} is not a refusal"
        );
        let _server = timeout(DEADLINE, server).await.unwrap().unwrap();
    }
}

/// A server that refuses without end costs the oldest refusals and not memory:
/// a reader that fell far behind finds the newest ones and a count of what it
/// missed.
#[tokio::test]
async fn a_flood_of_refusals_is_bounded_and_counted() {
    const SENT: usize = 100;
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        // The burst waits for the client's reader to exist: a reader starts at
        // the tail of what is sent after it.
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
        let mut burst = Vec::new();
        for n in 0..SENT {
            burst.extend(err(&format!(
                r#"Permissions Violation for Publish to "s{n}""#
            )));
        }
        server.write(&burst).await;
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
        server
    });

    let mut options = options();
    options.refusal_queue = 4;
    let nats = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("finished")
    .expect("opened");
    let mut refusals = nats.refusals();
    timeout(DEADLINE, nats.flush()).await.unwrap().unwrap();

    // The second PONG follows every refusal on the wire, so by the time this
    // flush returns the driver has seen all of them while nobody was reading.
    timeout(DEADLINE, nats.flush()).await.unwrap().unwrap();
    let _server = timeout(DEADLINE, server).await.unwrap().unwrap();

    let mut subjects = Vec::new();
    while let Some(refusal) = refusals.try_next() {
        subjects.push(refusal.subject.expect("a subject"));
    }
    assert_eq!(
        subjects,
        ["s96", "s97", "s98", "s99"],
        "the newest are kept"
    );
    assert_eq!(
        refusals.lost(),
        (SENT - 4) as u64,
        "and the rest are counted"
    );

    timeout(DEADLINE, nats.close()).await.unwrap().unwrap();
}
