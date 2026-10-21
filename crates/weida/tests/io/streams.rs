//! The tests of `tests/streams.rs` that need the network, a process or real time.

use crate::common;
use common::Server;
use std::time::Duration;
use weida::{Error, Limits, TransferMeta};

use crate::streams::*;

/// Claim: a transfer given a deadline is reset with `CANCELED` when its bytes
/// are not acknowledged by then (0034 §4.3). The sender's blocked write fails
/// with `Expired`, the reader sees `Canceled` rather than EOF, and the runtime
/// counts the expiry apart from a cancel.
///
/// The reader holds its transfer unread, so a 64 KiB stream window stalls a
/// 1 MiB write long before the deadline.
#[tokio::test]
async fn a_transfer_past_its_deadline_is_reset() {
    let server = Server::start_with(Limits {
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    })
    .await;
    let acceptor = server.listener.acceptor("/slow").expect("acceptor");
    let client = server.client_runtime();
    let peer = client.peer(server.trust());
    within(peer.connect(&server.url("/slow")))
        .await
        .expect("connect");

    let mut transfer = within(peer.open(TransferMeta::default()))
        .await
        .expect("open");
    let started = std::time::Instant::now();
    transfer.expire_at(started + Duration::from_millis(300));
    let held = match within(acceptor.accept()).await.expect("accept") {
        weida::Incoming::Stream(held) => held,
        other => panic!("expected a stream, got {other:?}"),
    };

    let failed = within(transfer.write_all(&vec![0x5a; 1 << 20]))
        .await
        .expect_err("the window stalls the write past its deadline");
    let took = started.elapsed();
    assert!(matches!(failed, Error::Expired), "{failed:?}");
    assert!(
        took >= Duration::from_millis(300) && took < Duration::from_secs(3),
        "expired after {took:?}"
    );
    assert!(!failed.is_definite_failure());

    let read = within(held.collect(2 << 20)).await;
    assert!(matches!(read, Err(Error::Canceled)), "{read:?}");
    assert_eq!(client.expired_transfers(), 1);
    client.shutdown().await;
}
