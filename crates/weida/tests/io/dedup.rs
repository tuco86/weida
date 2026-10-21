//! Bounded deduplication over a real connection.
//!
//! The library never sends the same `(producer, scope, sequence)` twice — its
//! sequencer is monotone — so a *replay* has to come from the wire. These
//! tests speak `weida-protocol` over bare `quinn`, exactly as the hostile
//! suite does, to send one transfer twice inside the window and once after it.

use crate::common;

use std::time::Duration;

use common::{Server, raw};
use weida::{Deduplication, GuaranteeSet, RuntimeConfig};
use weida_protocol::{DataHeader, FrameKind, Hello, encode_frame};

/// Generous ceiling for everything but the window itself.
const DEADLINE: Duration = Duration::from_secs(10);

/// The window: long enough that two frames sent back to back land inside it,
/// short enough that waiting it out costs a fraction of a second. This is the
/// smallest interval that demonstrates expiry reliably; the assertions
/// below never race it, because each one waits for an observable state
/// change rather than for the clock.
const WINDOW: Duration = Duration::from_millis(200);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

fn bounded() -> GuaranteeSet {
    GuaranteeSet {
        deduplication: Deduplication::Bounded,
        dedup_window_ms: Some(WINDOW.as_millis() as u64),
        ..GuaranteeSet::CORE
    }
}

/// Sends one numbered DATA transfer with a payload, as a whole stream.
async fn send_numbered(conn: &quinn::Connection, path: &str, sequence: u64, body: &[u8]) {
    let mut header = DataHeader::addressed(path);
    header.sequence = Some(sequence);
    let mut stream = conn.open_uni().await.expect("open uni");
    stream
        .write_all(&encode_frame(FrameKind::Data, &header.encode()))
        .await
        .expect("write header");
    stream.write_all(body).await.expect("write body");
    stream.finish().expect("finish");
}

#[tokio::test]
async fn a_replay_inside_the_window_is_suppressed_and_counted() {
    let server = Server::start_os_with_config(RuntimeConfig {
        guarantees: bounded(),
        ..RuntimeConfig::default()
    })
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    // The server *requires* the set, so a `core` HELLO would fail
    // negotiation: the declaration is what makes this connection legal.
    let hello = Hello {
        guarantees_offered: Some(bounded()),
        guarantees_required: Some(bounded()),
        ..Hello::v0(16 * 1024, 1024)
    };
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;

    // The same transfer, twice, back to back: well inside the window.
    send_numbered(&conn, "/jobs", 7, b"once").await;
    send_numbered(&conn, "/jobs", 7, b"once").await;
    // A different number is a different message and must arrive.
    send_numbered(&conn, "/jobs", 8, b"sentinel").await;

    // Exactly two transfers reach the application, and the second is the
    // sentinel: the replay was suppressed rather than merely delayed. The
    // sentinel makes that conclusive without a sleep.
    let first = within(puller.recv()).await.expect("first");
    assert_eq!(first.meta().sequence, Some(7));
    assert_eq!(
        within(first.collect(64)).await.expect("collect"),
        b"once".to_vec()
    );
    let second = within(puller.recv()).await.expect("sentinel");
    assert_eq!(second.meta().sequence, Some(8));
    assert_eq!(
        within(second.collect(64)).await.expect("collect"),
        b"sentinel".to_vec()
    );

    assert_eq!(
        server.runtime.suppressed_duplicates(),
        1,
        "the replay must be counted where drops are counted"
    );
}

#[tokio::test]
async fn an_identity_replayed_after_the_window_is_delivered() {
    let server = Server::start_os_with_config(RuntimeConfig {
        guarantees: bounded(),
        ..RuntimeConfig::default()
    })
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    let hello = Hello {
        guarantees_offered: Some(bounded()),
        guarantees_required: Some(bounded()),
        ..Hello::v0(16 * 1024, 1024)
    };
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;

    send_numbered(&conn, "/jobs", 1, b"first").await;
    let first = within(puller.recv()).await.expect("first");
    assert_eq!(
        within(first.collect(64)).await.expect("collect"),
        b"first".to_vec()
    );

    // Bounded means bounded: once the window has passed the identity is
    // forgotten, and the same numbers name a new message.
    tokio::time::sleep(WINDOW + Duration::from_millis(50)).await;
    send_numbered(&conn, "/jobs", 1, b"again").await;
    let again = within(puller.recv()).await.expect("again");
    assert_eq!(again.meta().sequence, Some(1));
    assert_eq!(
        within(again.collect(64)).await.expect("collect"),
        b"again".to_vec()
    );

    assert_eq!(
        server.runtime.suppressed_duplicates(),
        0,
        "nothing was inside the window"
    );
}

#[tokio::test]
async fn nothing_is_suppressed_without_the_negotiated_level() {
    // The default `core` set: the same replay is delivered twice, because no
    // deduplication was negotiated and none is applied.
    let server = Server::start_os().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    send_numbered(&conn, "/jobs", 3, b"twice").await;
    send_numbered(&conn, "/jobs", 3, b"twice").await;

    for _ in 0..2 {
        let transfer = within(puller.recv()).await.expect("both arrive");
        assert_eq!(transfer.meta().sequence, Some(3));
        within(transfer.collect(64)).await.expect("collect");
    }
    assert_eq!(server.runtime.suppressed_duplicates(), 0);
}
