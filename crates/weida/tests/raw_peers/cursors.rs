//! The tests of `tests/cursors.rs` that need a raw peer: bare `quinn`
//! endpoints on the simulated network, writing CURSOR records by hand.

use crate::common;
use common::{Server, raw};
use std::time::Duration;
use weida::{ReportMode, TransferMeta, codes};
use weida_protocol::{CursorHeader, FrameKind, encode_cursor_record, encode_preamble};

use crate::cursors::*;

/// A client runtime on `net`.
fn sim_client(net: &weida::SimNet) -> weida::Runtime {
    weida::Runtime::new(weida::RuntimeConfig {
        network: weida::Network::sim(net),
        ..weida::RuntimeConfig::default()
    })
    .expect("runtime")
}

#[tokio::test(start_paused = true)]
async fn a_push_transfer_that_orders_cursors_stays_one_unidirectional_stream() {
    // The end-to-end proof of 0024 §4.4a: a raw peer counts the streams, so
    // the assertion is about the wire rather than about our own bookkeeping.
    let net = weida::SimNet::new();
    let certs = common::Certs::generate();
    let (endpoint, addr) = raw::sim_server_endpoint(&net, &certs);

    let client = sim_client(&net);
    let pusher = client.pusher(certs.client_tls());
    let url = format!("weida://127.0.0.1:{}/jobs", addr.port());

    let peer = tokio::spawn(async move {
        let incoming = endpoint.accept().await.expect("inbound");
        let conn = incoming.await.expect("handshake");
        raw::send_hello(&conn).await;
        let (mut recv, header) = raw::accept_data(&conn).await;
        let report_id = header.report_id.expect("the transfer ordered a report");
        assert_eq!(header.report, vec![ACCEPTED]);
        // Progress is the default and is never written, so an ordered report
        // costs the header its id and its level array and nothing else.
        assert_eq!(header.report_mode, ReportMode::Progress);

        let mut body = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        while let Some(n) = recv.read(&mut chunk).await.expect("read payload") {
            body.extend_from_slice(&chunk[..n]);
        }

        // No bidirectional stream was opened for the report: a report must
        // not turn a push into an exchange.
        assert!(
            tokio::time::timeout(Duration::from_millis(300), conn.accept_bi())
                .await
                .is_err(),
            "a report must not turn a push into an exchange"
        );

        report_raw(&conn, report_id, &[(ACCEPTED, body.len() as u64)]).await;
        // Hold the connection open until the sender has seen the report.
        tokio::time::sleep(Duration::from_secs(2)).await;
        body.len()
    });

    within(pusher.connect(&url)).await.expect("connect");
    let payload = vec![0x5au8; 4096];
    let mut transfer = within(pusher.open(TransferMeta::default().with_report([ACCEPTED])))
        .await
        .expect("open");
    let mut cursors = transfer.cursors().expect("the transfer ordered a report");
    // The report is one handle per transfer, so a second ask gets nothing.
    assert!(transfer.cursors().is_none());
    within(transfer.write_all(&payload)).await.expect("write");
    let delivery = transfer.finish().expect("finish");
    within(delivery.delivered()).await.expect("delivered");

    // The verdict arrives after the FIN, which is exactly why the handle is
    // independent of the transfer.
    let set = within(cursors.changed()).await.expect("a cursor arrived");
    assert_eq!(set.offset(ACCEPTED), Some(payload.len() as u64));
    assert_eq!(set.len(), 1);

    assert_eq!(peer.await.expect("peer"), payload.len());
    client.shutdown().await;
}
#[tokio::test(start_paused = true)]
async fn a_reordered_or_duplicated_record_changes_nothing() {
    let net = weida::SimNet::new();
    let certs = common::Certs::generate();
    let (endpoint, addr) = raw::sim_server_endpoint(&net, &certs);

    let client = sim_client(&net);
    let pusher = client.pusher(certs.client_tls());
    let url = format!("weida://127.0.0.1:{}/jobs", addr.port());

    let peer = tokio::spawn(async move {
        let incoming = endpoint.accept().await.expect("inbound");
        let conn = incoming.await.expect("handshake");
        raw::send_hello(&conn).await;
        let (mut recv, header) = raw::accept_data(&conn).await;
        let report_id = header.report_id.expect("a report was ordered");
        let mut chunk = vec![0u8; 1024];
        while recv.read(&mut chunk).await.expect("read").is_some() {}
        // Forward, repeated, then **backward** — and backward last on
        // purpose: with last-write-wins the reader would end at 40, and the
        // whole argument for a cursor is that it cannot.
        report_raw(
            &conn,
            report_id,
            &[(STORED, 100), (STORED, 100), (STORED, 40)],
        )
        .await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    });

    within(pusher.connect(&url)).await.expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default().with_report([STORED])))
        .await
        .expect("open");
    let mut cursors = transfer.cursors().expect("cursors");
    within(transfer.write_all(b"payload")).await.expect("write");
    transfer.finish().expect("finish");

    let set = within(cursors.changed()).await.expect("a cursor arrived");
    assert_eq!(set.offset(STORED), Some(100));
    // The stream FINed after three records; the reader is told once, because
    // only the first record moved anything.
    assert_eq!(within(cursors.changed()).await, None);
    assert_eq!(cursors.snapshot().offset(STORED), Some(100));

    peer.await.expect("peer");
    client.shutdown().await;
}
#[tokio::test(start_paused = true)]
async fn a_cursor_stream_for_an_unknown_report_id_is_reset_and_the_connection_survives() {
    let server = Server::start().await;
    let replier = server.listener.replier("/t").expect("replier");

    let endpoint = server.raw_client();
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // No transfer of the server's ordered anything, so every id is unknown.
    let head = CursorHeader { report_id: 999 }.encode();
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Cursor, head.len() as u64, &mut bytes);
    bytes.extend_from_slice(&head);
    encode_cursor_record(ACCEPTED, 1, &mut bytes).expect("encode record");
    let mut stream = conn.open_uni().await.expect("open uni");
    stream.write_all(&bytes).await.expect("write");

    // Reset with CANCELED — "the transfer is no longer wanted" — rather than
    // a connection close: no state was allocated for an id we never handed
    // out, which is the hostile case this rule exists for.
    let stopped = within(stream.stopped()).await.expect("stopped");
    assert_eq!(stopped.map(|c| c.into_inner()), Some(codes::CANCELED));

    // The connection survives, and a real exchange on it still works.
    let handler = tokio::spawn(async move {
        let mut request = within(replier.accept()).await.expect("accept");
        let body = within(request.body().read_capped(64)).await.expect("body");
        assert_eq!(body, b"ping");
        let reply = within(request.reply(TransferMeta::default()))
            .await
            .expect("reply");
        reply.finish().expect("finish");
    });
    let (mut send, recv) =
        raw::open_exchange(&conn, &weida_protocol::DataHeader::addressed("/t")).await;
    send.write_all(b"ping").await.expect("write request");
    send.finish().expect("finish request");
    let mut recv = recv;
    let (preamble, _) = raw::read_frame(&mut recv).await;
    assert_eq!(preamble.kind, FrameKind::Data);
    handler.await.expect("handler");
}
#[tokio::test(start_paused = true)]
async fn coalescing_loses_nothing() {
    // The claim of 0023 §4.3b, measured: a reporter that drops every
    // intermediate record leaves the reader at exactly the same offset as one
    // that writes all hundred. The counts come from a raw peer, because the
    // reader-side API keeps only the latest value by design.
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    // Coarse: a megabyte or an hour, so nothing after the first record is
    // worth writing.
    let coarse =
        run_with_granularity(&server, &puller, 1024 * 1024, Duration::from_secs(3600), 1).await;
    // Fine: every call is worth writing.
    let fine = run_with_granularity(&server, &puller, 0, Duration::ZERO, 2).await;

    // Two records against a hundred — the first, which is always written, and
    // the terminal flush.
    assert_eq!(coarse.len(), 2, "{coarse:?}");
    assert_eq!(fine.len(), 100);
    // And the numbers that matter are identical.
    assert_eq!(coarse.last().expect("a record").1, 100);
    assert_eq!(fine.last().expect("a record").1, 100);
}
#[tokio::test(start_paused = true)]
async fn final_only_emits_exactly_one_record_per_level() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let endpoint = server.raw_client();
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    let header = weida_protocol::DataHeader {
        report_id: Some(1),
        report: vec![ACCEPTED, STORED],
        report_mode: ReportMode::FinalOnly,
        ..weida_protocol::DataHeader::addressed("/jobs")
    };
    push_raw(&conn, &header, b"body").await;

    let transfer = within(puller.recv()).await.expect("recv");
    let mut reporter = transfer.reporter().expect("a reporter");
    assert_eq!(reporter.mode(), ReportMode::FinalOnly);
    let _ = within(transfer.collect(1024)).await.expect("collect");
    for offset in 1..=10u64 {
        within(reporter.report(ACCEPTED, offset))
            .await
            .expect("report");
        within(reporter.report(STORED, offset * 2))
            .await
            .expect("report");
    }
    within(reporter.finish()).await.expect("finish");

    // Twenty calls, two records: one per level, each the last offset that
    // level reached. The granularity is irrelevant in this mode.
    let records = read_report(&conn, 1).await;
    assert_eq!(records, vec![(ACCEPTED, 10), (STORED, 20)]);
}
/// A report is one shared value however many CURSOR streams name it: a peer
/// that opens a second stream for the same `report_id` cannot move it
/// backwards by starting that stream lower. Keeping the maximum per level is
/// the property the whole carrier rests on
/// ([PROTOCOL.md](../../../docs/PROTOCOL.md) §6.7 rule 1).
#[tokio::test(start_paused = true)]
async fn a_second_cursor_stream_for_one_report_cannot_move_it_backwards() {
    let net = weida::SimNet::new();
    let certs = common::Certs::generate();
    let (endpoint, addr) = raw::sim_server_endpoint(&net, &certs);

    let client = sim_client(&net);
    let pusher = client.pusher(certs.client_tls());
    let url = format!("weida://127.0.0.1:{}/jobs", addr.port());

    let (first_seen, wait_for_first) = tokio::sync::oneshot::channel::<()>();
    let (done, wait_for_done) = tokio::sync::oneshot::channel::<()>();
    let peer = tokio::spawn(async move {
        let incoming = endpoint.accept().await.expect("inbound");
        let conn = incoming.await.expect("handshake");
        raw::send_hello(&conn).await;
        let (mut recv, header) = raw::accept_data(&conn).await;
        let report_id = header.report_id.expect("a report was ordered");
        let mut chunk = vec![0u8; 1024];
        while recv.read(&mut chunk).await.expect("read").is_some() {}

        // The first stream reports 1000 and stays open, so the report is
        // live while the second stream arrives.
        let head = CursorHeader { report_id }.encode();
        let mut bytes = Vec::new();
        encode_preamble(FrameKind::Cursor, head.len() as u64, &mut bytes);
        bytes.extend_from_slice(&head);
        encode_cursor_record(STORED, 1000, &mut bytes).expect("encode record");
        let mut first = conn.open_uni().await.expect("open uni");
        first
            .write_all(&bytes)
            .await
            .expect("write the first stream");
        wait_for_first.await.expect("the client saw 1000");

        // The second stream starts lower. Its second record, a level the
        // first never reported, is what tells the client that its first one
        // has been applied: records of a stream are applied in order.
        report_raw(&conn, report_id, &[(STORED, 5), (PROCESSED, 1)]).await;
        wait_for_done.await.ok();
        first.finish().expect("finish the first stream");
    });

    within(pusher.connect(&url)).await.expect("connect");
    let mut transfer =
        within(pusher.open(TransferMeta::default().with_report([STORED, PROCESSED])))
            .await
            .expect("open");
    let mut cursors = transfer.cursors().expect("cursors");
    within(transfer.write_all(b"payload")).await.expect("write");
    transfer.finish().expect("finish");

    within(async {
        while cursors.offset(STORED) != Some(1000) {
            cursors.changed().await.expect("the first stream is live");
        }
    })
    .await;
    first_seen.send(()).expect("the peer is waiting");
    within(async {
        while cursors.offset(PROCESSED) != Some(1) {
            cursors.changed().await.expect("the report is live");
        }
    })
    .await;
    assert_eq!(
        cursors.offset(STORED),
        Some(1000),
        "the second stream's lower record must change nothing"
    );

    done.send(()).ok();
    peer.await.expect("peer");
    client.shutdown().await;
}
