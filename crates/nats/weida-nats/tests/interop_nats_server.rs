//! Interop against `nats-server`, with an `async-nats` peer on the far side.
//!
//! **Every test in this file is `#[ignore]`d**, because `nats-server` is a
//! separate binary and not a crate. Install it and run them deliberately:
//!
//! ```text
//! # Arch:            pacman -S nats-server
//! # Debian/Ubuntu:   the `nats-server` release tarball from
//! #                  https://github.com/nats-io/nats-server/releases
//! # Go toolchain:    go install github.com/nats-io/nats-server/v2@latest
//! # Docker:          docker run -p 4222:4222 nats:2
//! cargo test -p weida-nats --test interop_nats_server -- --ignored
//! ```
//!
//! # There is no pure-Rust substitute, so absence means no interop at all
//!
//! This is the difference from the AMQP half of this workstream, and it is
//! worth stating plainly rather than working around. `fe2o3-amqp` ships an
//! `acceptor` module — a foreign *server* — so AMQP interop runs on any
//! machine with a Rust toolchain. NATS has no such thing: `async-nats` is a
//! **client**, and two clients cannot talk to each other. Core NATS is a
//! subject-addressed system whose entire routing, queue-group and
//! request-reply behaviour lives in the server; there is nothing for two
//! clients to agree about without one.
//!
//! So where the binary is absent the honest state is *no interop*, not weaker
//! interop. `docs/libraries/nats.md` records this column as **not run**, and
//! the scripted-server tests in `tests/handshake.rs`, `tests/subscriptions.rs`
//! and `tests/request_reply.rs` are what they are: assertions about the bytes
//! this client writes and reads, checked against the protocol reference, and
//! *not* evidence that a real server accepts them.
//!
//! # What each test would establish
//!
//! The peer is an independent client, so every case below is this client
//! against `async-nats` **through** the server rather than against itself:
//! `async-nats` subscribes and this client publishes, and the reverse; a queue
//! group holds one subscriber of each kind and the server picks; and
//! request-reply crosses in both directions, including the one case Core NATS
//! answers itself — the 503 when nobody is listening.
//!
//! The server is started by the harness with a readiness condition and stopped
//! on success and on failure alike, which is what [`Server`]'s `Drop` is for:
//! a test that panics must not leave a `nats-server` behind.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

// `async_nats::Subscriber` is a `Stream`, so its `next` comes from the trait
// rather than from an inherent method. Ours is an inherent `next` on purpose:
// a subscription that is only usable through a trait is a subscription whose
// documentation has to explain the trait first.
use futures_util::StreamExt;
use weida_nats::{Connection, ConnectionOptions};
use weida_runtime::Exec;

/// What every `#[ignore]` says, so the install command is one grep away from a
/// skipped test.
macro_rules! needs_nats_server {
    () => {
        "needs nats-server: `pacman -S nats-server`, a release tarball from \
         github.com/nats-io/nats-server/releases, `go install \
         github.com/nats-io/nats-server/v2@latest` or `docker run -p 4222:4222 nats:2`, \
         then `cargo test -p weida-nats --test interop_nats_server -- --ignored`"
    };
}

const DEADLINE: Duration = Duration::from_secs(10);

/// The version every measured claim would be measured against, printed by the
/// harness so that `docs/libraries/nats.md` can name it rather than guess.
fn server_version() -> String {
    Command::new("nats-server")
        .arg("--version")
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_else(|error| format!("unknown ({error})"))
}

/// A supervised `nats-server`, stopped when the guard drops.
///
/// Readiness is **observed**: the server prints `Server is ready` on its
/// standard error, and the harness waits for that line rather than for a
/// sleep, because process creation is not a listening socket.
struct Server {
    child: Child,
    port: u16,
}

impl Server {
    fn start(port: u16) -> Self {
        let mut child = Command::new("nats-server")
            .arg("--port")
            .arg(port.to_string())
            // No cluster, no JetStream, no monitoring: the measurement is Core
            // NATS and nothing else.
            .arg("--no_sys_acc")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("nats-server starts");

        let stderr = child.stderr.take().expect("piped");
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            line.clear();
            let read = reader.read_line(&mut line).expect("the server logs");
            assert_ne!(read, 0, "nats-server exited before it was ready");
            if line.contains("Server is ready") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "nats-server was not ready within 30 s; last line: {line}"
            );
        }
        println!("MEASURED against {} on port {port}", server_version());
        Self { child, port }
    }
}

impl Drop for Server {
    /// Stopped on success and on failure alike. A panicking test must not
    /// leave a server behind for the next one to connect to by accident.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn options() -> ConnectionOptions {
    ConnectionOptions::new()
}

async fn ours(exec: &Exec, port: u16) -> Connection {
    tokio::time::timeout(
        DEADLINE,
        Connection::connect(exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("the server answered")
    .expect("INFO, CONNECT and the PING/PONG confirmation succeeded")
}

async fn theirs(port: u16) -> async_nats::Client {
    tokio::time::timeout(DEADLINE, async_nats::connect(format!("127.0.0.1:{port}")))
        .await
        .expect("the peer connected")
        .expect("async-nats connected")
}

#[tokio::test]
#[ignore = needs_nats_server!()]
async fn our_publish_reaches_an_async_nats_subscriber() {
    let exec = Exec::current().unwrap();
    let server = Server::start(14222);
    let peer = theirs(server.port).await;
    let connection = ours(&exec, server.port).await;

    // Their subscription first, and flushed, because Core NATS delivers only
    // to interest that already exists: "publishing to nobody is not an error"
    // is the protocol's own behaviour and would make this test pass for the
    // wrong reason.
    let mut subscriber = peer.subscribe("interop.one").await.expect("subscribed");
    peer.flush().await.expect("the SUB reached the server");

    tokio::time::timeout(
        DEADLINE,
        connection.publish("interop.one", b"from weida-nats"),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(DEADLINE, connection.flush())
        .await
        .unwrap()
        .unwrap();

    let message = tokio::time::timeout(DEADLINE, subscriber.next())
        .await
        .expect("the server delivered")
        .expect("a message");
    assert_eq!(&message.payload[..], b"from weida-nats");
    assert_eq!(message.subject.as_str(), "interop.one");

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = needs_nats_server!()]
async fn an_async_nats_publish_reaches_our_subscriber() {
    let exec = Exec::current().unwrap();
    let server = Server::start(14223);
    let peer = theirs(server.port).await;
    let connection = ours(&exec, server.port).await;

    let mut subscription = tokio::time::timeout(DEADLINE, connection.subscribe("interop.two"))
        .await
        .unwrap()
        .expect("subscribed");
    tokio::time::timeout(DEADLINE, connection.flush())
        .await
        .unwrap()
        .unwrap();

    peer.publish("interop.two", "from async-nats".into())
        .await
        .expect("published");
    peer.flush().await.expect("flushed");

    let message = tokio::time::timeout(DEADLINE, subscription.next())
        .await
        .expect("the server delivered")
        .expect("a message");
    assert_eq!(message.payload, b"from async-nats");
    assert_eq!(message.subject_str(), Some("interop.two"));
    assert_eq!(message.reply_to, None);

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = needs_nats_server!()]
async fn the_two_wildcards_match_what_the_server_says_they_match() {
    let exec = Exec::current().unwrap();
    let server = Server::start(14224);
    let peer = theirs(server.port).await;
    let connection = ours(&exec, server.port).await;

    // `*` is one token, `>` is one or more and only at the tail. The
    // authority on which subject matches which pattern is the server, and
    // this client's `subject::matches` is a pure function asserted against the
    // protocol reference — so the point of the test is that the two agree.
    let mut token = tokio::time::timeout(DEADLINE, connection.subscribe("interop.*.leaf"))
        .await
        .unwrap()
        .unwrap();
    let mut tail = tokio::time::timeout(DEADLINE, connection.subscribe("interop.deep.>"))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, connection.flush())
        .await
        .unwrap()
        .unwrap();

    for subject in ["interop.a.leaf", "interop.deep.a.b.c", "interop.a.b.leaf"] {
        peer.publish(subject.to_owned(), subject.into())
            .await
            .expect("published");
    }
    peer.flush().await.expect("flushed");

    let first = tokio::time::timeout(DEADLINE, token.next())
        .await
        .unwrap()
        .expect("a message");
    assert_eq!(
        first.subject_str(),
        Some("interop.a.leaf"),
        "`*` matches exactly one token, so interop.a.b.leaf is not a match"
    );
    assert!(
        token.try_next().is_none(),
        "and only one of the three reached it"
    );

    let deep = tokio::time::timeout(DEADLINE, tail.next())
        .await
        .unwrap()
        .expect("a message");
    assert_eq!(
        deep.subject_str(),
        Some("interop.deep.a.b.c"),
        "`>` matches one or more tokens"
    );

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = needs_nats_server!()]
async fn a_queue_group_holding_both_clients_delivers_to_exactly_one() {
    let exec = Exec::current().unwrap();
    let server = Server::start(14225);
    let peer = theirs(server.port).await;
    let connection = ours(&exec, server.port).await;

    // One subscriber of each kind in the same group. The server picks, and
    // what is asserted is the *count*: a queue group is "one of the set
    // receives each message", not a load-balancing promise about which.
    let mut ours_sub = tokio::time::timeout(
        DEADLINE,
        connection.subscribe_with_queue_group("interop.work", "workers"),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(DEADLINE, connection.flush())
        .await
        .unwrap()
        .unwrap();
    let mut theirs_sub = peer
        .queue_subscribe("interop.work", "workers".into())
        .await
        .expect("subscribed");
    peer.flush().await.expect("flushed");

    let publisher = theirs(server.port).await;
    const JOBS: usize = 20;
    for n in 0..JOBS {
        publisher
            .publish("interop.work", format!("job-{n}").into())
            .await
            .expect("published");
    }
    publisher.flush().await.expect("flushed");

    let mut mine = 0usize;
    let mut hers = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mine + hers < JOBS && tokio::time::Instant::now() < deadline {
        tokio::select! {
            message = ours_sub.next() => {
                if message.is_some() {
                    mine += 1;
                }
            }
            message = theirs_sub.next() => {
                if message.is_some() {
                    hers += 1;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    assert_eq!(
        mine + hers,
        JOBS,
        "every message reached exactly one member of the group ({mine} here, {hers} there)"
    );

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = needs_nats_server!()]
async fn our_request_is_answered_by_an_async_nats_responder() {
    let exec = Exec::current().unwrap();
    let server = Server::start(14226);
    let peer = theirs(server.port).await;
    let connection = ours(&exec, server.port).await;

    let mut service = peer.subscribe("interop.echo").await.expect("subscribed");
    peer.flush().await.expect("flushed");
    let responder = peer.clone();
    let answering = tokio::spawn(async move {
        let request = service.next().await.expect("a request");
        let reply = request.reply.expect("a reply subject");
        responder
            .publish(reply, request.payload.clone())
            .await
            .expect("answered");
        responder.flush().await.expect("flushed");
        request.payload
    });

    let answer = tokio::time::timeout(
        DEADLINE,
        connection.request("interop.echo", b"ping", Duration::from_secs(5)),
    )
    .await
    .expect("within the window")
    .expect("an answer");
    assert_eq!(answer.payload, b"ping");
    // The inbox is ours and the reply subject the responder saw is the one we
    // generated, which is the whole of Core NATS request-reply: there is no
    // request verb, only a subject nobody else subscribes to.
    let seen = tokio::time::timeout(DEADLINE, answering)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&seen[..], b"ping");

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = needs_nats_server!()]
async fn an_async_nats_request_is_answered_by_us() {
    let exec = Exec::current().unwrap();
    let server = Server::start(14227);
    let peer = theirs(server.port).await;
    let connection = ours(&exec, server.port).await;

    let mut service = tokio::time::timeout(DEADLINE, connection.subscribe("interop.serve"))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, connection.flush())
        .await
        .unwrap()
        .unwrap();

    let serving = {
        let connection = connection.clone();
        tokio::spawn(async move {
            let request = service.next().await.expect("a request");
            let reply = request.reply_to.clone().expect("a reply subject");
            connection.publish(&reply, b"pong").await.expect("answered");
            connection.flush().await.expect("flushed");
        })
    };

    let answer = tokio::time::timeout(DEADLINE, peer.request("interop.serve", "ping".into()))
        .await
        .expect("within the window")
        .expect("an answer");
    assert_eq!(&answer.payload[..], b"pong");
    tokio::time::timeout(DEADLINE, serving)
        .await
        .unwrap()
        .unwrap();

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = needs_nats_server!()]
async fn a_request_with_no_responder_is_the_servers_503_and_not_a_timeout() {
    let exec = Exec::current().unwrap();
    let server = Server::start(14228);
    let connection = ours(&exec, server.port).await;

    // The one answer Core NATS produces itself. `no_responders` is negotiated
    // in `CONNECT` and the server then answers a request on a subject with no
    // interest with a headers-only message carrying status 503 — which is a
    // different fact from "nobody answered in time" and must not collapse into
    // it.
    let started = std::time::Instant::now();
    let error = tokio::time::timeout(
        DEADLINE,
        connection.request("interop.nobody", b"anyone?", Duration::from_secs(5)),
    )
    .await
    .expect("the server answered rather than leaving us to time out")
    .expect_err("no responders");
    assert!(
        matches!(error, weida_nats::Error::NoResponders),
        "expected NoResponders, got {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the 503 came back immediately rather than at the end of the window"
    );

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = needs_nats_server!()]
async fn headers_cross_to_an_async_nats_subscriber_and_back() {
    let exec = Exec::current().unwrap();
    let server = Server::start(14229);
    let peer = theirs(server.port).await;
    let connection = ours(&exec, server.port).await;

    assert!(
        connection.headers_supported(),
        "the server advertised `headers` in INFO, so HPUB and HMSG are in play"
    );

    let mut subscriber = peer.subscribe("interop.headers").await.expect("subscribed");
    peer.flush().await.expect("flushed");

    let mut headers = weida_nats::OwnedHeaders::default();
    headers.push("Nats-Msg-Id", "interop-1");
    headers.push("X-Weida", "yes");
    tokio::time::timeout(
        DEADLINE,
        connection.publish_with("interop.headers", None, Some(&headers), b"with headers"),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(DEADLINE, connection.flush())
        .await
        .unwrap()
        .unwrap();

    let message = tokio::time::timeout(DEADLINE, subscriber.next())
        .await
        .unwrap()
        .expect("a message");
    let seen = message.headers.expect("the headers survived the server");
    assert_eq!(
        seen.get("Nats-Msg-Id").map(|value| value.as_str()),
        Some("interop-1")
    );
    assert_eq!(seen.get("X-Weida").map(|value| value.as_str()), Some("yes"));
    assert_eq!(&message.payload[..], b"with headers");

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
}
