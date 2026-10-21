//! Reassembly over a real connection: `PerProducer(reassemble)`.
//!
//! Two kinds of proof. The raw-wire tests scramble the sequence numbers
//! deliberately, because QUIC's stream opens arrive in order often enough
//! that a test relying on the network to reorder them would pass for the
//! wrong reason. The transfer-level test is the reverse-FIN probe of
//! `reverse_order_completion_measures_the_reorder_buffer` (`streams.rs`),
//! rerun with the runtime doing the holding that the probe made the
//! application do.

use crate::common;

use std::time::Duration;

use common::{Server, raw};
use tokio::sync::mpsc;
use weida::{GuaranteeSet, Limits, OrderingMode, RuntimeConfig, TransferMeta};
use weida_protocol::{DataHeader, FrameKind, Hello, encode_frame};

pub(crate) const DEADLINE: Duration = Duration::from_secs(15);

pub(crate) async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

pub(crate) fn reassemble() -> GuaranteeSet {
    GuaranteeSet {
        ordering: OrderingMode::PerProducerReassemble,
        ..GuaranteeSet::CORE
    }
}

pub(crate) fn config(max_reorder_hold: usize) -> RuntimeConfig {
    RuntimeConfig {
        guarantees: reassemble(),
        limits: Limits {
            max_reorder_hold,
            ..Limits::default()
        },
        ..RuntimeConfig::default()
    }
}

/// Sends one numbered DATA transfer, header and body and FIN.
pub(crate) async fn send_numbered(conn: &quinn::Connection, path: &str, sequence: u64) {
    let mut header = DataHeader::addressed(path);
    header.sequence = Some(sequence);
    let mut stream = conn.open_uni().await.expect("open uni");
    stream
        .write_all(&encode_frame(FrameKind::Data, &header.encode()))
        .await
        .expect("write header");
    stream
        .write_all(&sequence.to_le_bytes())
        .await
        .expect("write body");
    stream.finish().expect("finish");
}

/// A raw peer that has declared reassemble ordering, so the handshake with a
/// server requiring it succeeds.
pub(crate) async fn declaring_peer(server: &Server) -> quinn::Connection {
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    let hello = Hello {
        guarantees_offered: Some(reassemble()),
        guarantees_required: Some(reassemble()),
        ..Hello::v0(16 * 1024, 1024)
    };
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;
    // The endpoint must outlive the connection, and the connection is what
    // the caller drives; leaking it here keeps the test bodies readable.
    std::mem::forget(endpoint);
    conn
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Finish {
    Batched,
    Sequenced,
}

/// The reverse-FIN probe, with the runtime holding instead of the
/// application: `n` transfers are opened in order and finished from the last
/// to the first, and the receiver must still see them in sequence order,
/// exactly once each.
pub(crate) async fn reverse_finish_case(n: usize, finish: Finish) {
    let server = Server::start_with_config(RuntimeConfig {
        endpoint_queue: n + 1,
        ..config(256)
    })
    .await;
    let puller = server.listener.puller("/reorder").expect("puller");
    let client = server.client_runtime_with_config(config(256));
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/reorder")))
        .await
        .expect("connect");

    // One task per accepted transfer, as in the probe: a transfer whose FIN
    // arrives early must not be reported behind an unfinished sibling. What
    // is asserted is the order the runtime *released* them in, which is the
    // order `recv` returns them.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let reader = tokio::spawn(async move {
        for position in 0..n {
            let transfer = puller.recv().await.expect("recv");
            let sequence = transfer.meta().sequence.expect("numbered");
            let tx = tx.clone();
            tokio::spawn(async move {
                let body = transfer.collect(64).await.expect("collect");
                let payload = u64::from_le_bytes(body[..].try_into().expect("8 bytes"));
                let _ = tx.send((position, sequence, payload));
            });
        }
        puller
    });

    let mut transfers = Vec::with_capacity(n);
    for seq in 0..n as u64 {
        let mut transfer = within(pusher.open(TransferMeta::default()))
            .await
            .expect("open");
        within(transfer.write_all(&seq.to_le_bytes()))
            .await
            .expect("write");
        transfers.push(transfer);
    }
    // The transfer dispatched first is the last to be finished.
    for transfer in transfers.into_iter().rev() {
        let delivery = transfer.finish().expect("finish");
        if finish == Finish::Sequenced {
            within(delivery.delivered()).await.expect("delivered");
        }
    }

    let mut seen = vec![false; n];
    for _ in 0..n {
        let (position, sequence, payload) = within(rx.recv()).await.expect("arrival");
        assert_eq!(
            sequence, position as u64,
            "reassemble mode must release in sequence order"
        );
        assert_eq!(payload, sequence, "each stream kept its own header");
        assert!(
            !std::mem::replace(&mut seen[sequence as usize], true),
            "each transfer arrives exactly once"
        );
    }
    let _puller = within(reader).await.expect("reader task");
    client.shutdown().await;
}

#[tokio::test]
async fn reverse_order_completion_is_released_in_sequence_order() {
    reverse_finish_case(16, Finish::Batched).await;
    reverse_finish_case(256, Finish::Batched).await;
    // `Sequenced` awaits a transport receipt per transfer, so it runs at the
    // small size only, exactly as the probe in `streams.rs` does.
    reverse_finish_case(16, Finish::Sequenced).await;
}
