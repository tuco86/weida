//! The tests of `tests/reorder.rs` that need the network, a process or real time.

use crate::common;
use common::Server;

use crate::reorder::*;

#[tokio::test]
async fn scrambled_arrivals_are_delivered_in_sequence_order() {
    let server = Server::start_os_with_config(config(256)).await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let conn = declaring_peer(&server).await;

    // Deliberately out of order behind 0. The first number a connection sees
    // establishes the position — the peer's counter is older than this
    // connection's view of it — so the scramble starts at 0 and the holes
    // are the ones after it.
    for sequence in [0u64, 3, 2, 1, 4] {
        send_numbered(&conn, "/jobs", sequence).await;
    }

    let mut arrivals = Vec::new();
    for _ in 0..5 {
        let transfer = within(puller.recv()).await.expect("recv");
        let sequence = transfer.meta().sequence.expect("numbered");
        assert_eq!(
            transfer.meta().gap,
            None,
            "every hole was filled before release"
        );
        let body = within(transfer.collect(64)).await.expect("collect");
        assert_eq!(
            u64::from_le_bytes(body[..].try_into().expect("8 bytes")),
            sequence,
            "the held stream and its header must stay together"
        );
        arrivals.push(sequence);
    }
    assert_eq!(arrivals, vec![0, 1, 2, 3, 4]);
}
#[tokio::test]
async fn a_full_hold_releases_out_of_order_and_reports_the_gap() {
    let server = Server::start_os_with_config(config(2)).await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let conn = declaring_peer(&server).await;

    // 1 is never sent. 2 and 3 fill the hold; 4 does not fit.
    for sequence in [0u64, 2, 3, 4] {
        send_numbered(&conn, "/jobs", sequence).await;
    }

    let first = within(puller.recv()).await.expect("recv 0");
    assert_eq!(first.meta().sequence, Some(0));
    assert_eq!(first.meta().gap, None);

    // The bound is enforced by releasing, not by growing: 2 comes out ahead
    // of the transfer it was waiting for, and says so.
    let forced = within(puller.recv()).await.expect("recv 2");
    assert_eq!(forced.meta().sequence, Some(2));
    assert_eq!(forced.meta().gap.expect("a gap").missed(), 1);

    for expected in [3u64, 4] {
        let transfer = within(puller.recv()).await.expect("recv");
        assert_eq!(transfer.meta().sequence, Some(expected));
        assert_eq!(
            transfer.meta().gap,
            None,
            "only the forced release skipped anything"
        );
    }
}
