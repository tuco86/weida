//! Cursors: reports about a transfer, on a stream of their own.
//!
//! The claims under test are the load-bearing ones of
//! [0023](../../../docs/decisions/0023-completion-is-a-cursor.md) and
//! [0024](../../../docs/decisions/0024-three-families-one-back-channel.md)
//! §4.4a, in order of how surprising they are:
//!
//! * ordering a report changes **no pattern's payload topology** — a Push
//!   transfer that orders cursors is still one unidirectional stream, and it
//!   still gets a reliable verdict;
//! * a cursor is **absolute**, so a duplicated or reordered record is a no-op;
//! * **nothing waits on a cursor**, proved from both sides: a sender that never
//!   reads its cursors blocks no transfer, and a receiver that never reports
//!   fails none;
//! * an **application** level is carried and never interpreted.
//!
//! The stream-counting tests use a raw `quinn` peer, because "one
//! unidirectional stream and no bidirectional one" is a statement about the
//! wire that the library's own API cannot make.

mod common;

use std::time::Duration;

use common::{Server, raw};
use weida::{Acknowledgement, CursorLevel, Error, ReportMode, TransferMeta, codes};
use weida_protocol::header::limits::MAX_REPORT_LEVELS;
use weida_protocol::{
    CursorHeader, FrameKind, decode_cursor_record, encode_cursor_record, encode_preamble,
};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

const ACCEPTED: CursorLevel = CursorLevel::Known(Acknowledgement::Accepted);
const STORED: CursorLevel = CursorLevel::Known(Acknowledgement::Stored);
const PROCESSED: CursorLevel = CursorLevel::Known(Acknowledgement::Processed);

/// Writes a whole cursor report on one fresh unidirectional stream.
async fn report_raw(conn: &quinn::Connection, report_id: u64, records: &[(CursorLevel, u64)]) {
    let head = CursorHeader { report_id }.encode();
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Cursor, head.len() as u64, &mut bytes);
    bytes.extend_from_slice(&head);
    for (level, offset) in records {
        encode_cursor_record(*level, *offset, &mut bytes).expect("encode record");
    }
    let mut stream = conn.open_uni().await.expect("open uni");
    stream.write_all(&bytes).await.expect("write cursor stream");
    stream.finish().expect("finish cursor stream");
}

/// Pushes one whole transfer on a fresh unidirectional stream, headers and
/// all, so a test can order a report the library's own API would not.
async fn push_raw(conn: &quinn::Connection, header: &weida_protocol::DataHeader, body: &[u8]) {
    let encoded = header.encode();
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, encoded.len() as u64, &mut bytes);
    bytes.extend_from_slice(&encoded);
    bytes.extend_from_slice(body);
    let mut stream = conn.open_uni().await.expect("open uni");
    stream.write_all(&bytes).await.expect("write transfer");
    stream.finish().expect("finish transfer");
}

/// Reads one whole cursor stream and returns its records in wire order.
///
/// Counting **records** is the only way to measure a granularity: the
/// reader-side API deliberately keeps the latest value per level, so a
/// coalesced report and a verbose one are indistinguishable there — which is
/// the property under test, not a gap in it.
async fn read_report(conn: &quinn::Connection, report_id: u64) -> Vec<(CursorLevel, u64)> {
    let (mut stream, header) = raw::accept_frame(conn, FrameKind::Cursor).await;
    let head = CursorHeader::decode(&header).expect("decode cursor head");
    assert_eq!(head.report_id, report_id);

    let mut bytes = Vec::new();
    let mut chunk = vec![0u8; 4096];
    while let Some(n) = stream.read(&mut chunk).await.expect("read cursor stream") {
        bytes.extend_from_slice(&chunk[..n]);
    }
    let mut records = Vec::new();
    let mut at = 0;
    while let Some((level, offset, used)) =
        decode_cursor_record(&bytes[at..]).expect("a well-formed record")
    {
        at += used;
        records.push((level, offset));
    }
    assert_eq!(at, bytes.len(), "the stream ended on a record boundary");
    records
}

#[tokio::test]
async fn a_push_transfer_that_orders_cursors_stays_one_unidirectional_stream() {
    // The end-to-end proof of 0024 §4.4a: a raw peer counts the streams, so
    // the assertion is about the wire rather than about our own bookkeeping.
    let certs = common::Certs::generate();
    let (endpoint, addr) = raw::server_endpoint(&certs);

    let client = weida::Runtime::new(weida::RuntimeConfig::default()).expect("runtime");
    let pusher = client.pusher(weida::ClientTls::new(weida::Trust::anchor_file(
        &certs.cert_pem,
    )));
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

#[tokio::test]
async fn an_exchange_reports_both_directions_on_two_cursor_streams() {
    let server = Server::start().await;
    let replier = server.listener.replier("/t").expect("replier");

    let handler = tokio::spawn(async move {
        let mut request = within(replier.accept()).await.expect("accept");
        // The requester's order arrived with the request, and the id is the
        // requester's own number.
        assert_eq!(request.meta().report, vec![ACCEPTED]);
        assert_eq!(request.meta().report_id, Some(1));

        let mut reporter = request.body().reporter().expect("a reporter");
        assert_eq!(reporter.levels(), [ACCEPTED]);
        let body = within(request.body().read_capped(1024))
            .await
            .expect("body");
        within(reporter.report(ACCEPTED, body.len() as u64))
            .await
            .expect("report");
        within(reporter.finish()).await.expect("finish report");

        // The reply orders its own report, from this side's id space: that is
        // how the reply direction is reported without a second header field.
        let mut reply = within(request.reply(TransferMeta::default().with_report([PROCESSED])))
            .await
            .expect("reply");
        let mut cursors = reply.cursors().expect("the reply ordered a report");
        within(reply.write_all(b"pong")).await.expect("write reply");
        reply.finish().expect("finish reply");
        let set = within(cursors.changed()).await.expect("a reply cursor");
        assert_eq!(set.offset(PROCESSED), Some(4));
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/t")))
        .await
        .expect("connect");

    let (mut request, reply) =
        within(requester.open(TransferMeta::default().with_report([ACCEPTED])))
            .await
            .expect("open");
    let mut cursors = request.cursors().expect("the request ordered a report");
    within(request.write_all(b"ping")).await.expect("write");
    request.finish().expect("finish");

    let set = within(cursors.changed()).await.expect("a request cursor");
    assert_eq!(set.offset(ACCEPTED), Some(4));

    let answer = within(reply.recv()).await.expect("reply");
    // Each side allocated `1`: the two directions have their own id spaces,
    // so nothing on the wire disambiguates them and nothing has to.
    assert_eq!(answer.meta().report_id, Some(1));
    let mut reporter = answer.reporter().expect("a reporter for the reply");
    let body = within(answer.collect(1024)).await.expect("collect");
    assert_eq!(body, b"pong");
    within(reporter.report(PROCESSED, body.len() as u64))
        .await
        .expect("report");
    within(reporter.finish()).await.expect("finish report");

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn a_reordered_or_duplicated_record_changes_nothing() {
    let certs = common::Certs::generate();
    let (endpoint, addr) = raw::server_endpoint(&certs);

    let client = weida::Runtime::new(weida::RuntimeConfig::default()).expect("runtime");
    let pusher = client.pusher(weida::ClientTls::new(weida::Trust::anchor_file(
        &certs.cert_pem,
    )));
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

#[tokio::test]
async fn an_unread_cursor_stream_blocks_no_transfer() {
    // The sender orders a report and never asks for it. A 1 MiB transfer
    // still completes and its receipt still resolves: nothing waits on a
    // cursor, seen from the sending side.
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let receiver = tokio::spawn(async move {
        let transfer = within(puller.recv()).await.expect("recv");
        let mut reporter = transfer.reporter().expect("a reporter");
        let body = within(transfer.collect(2 * 1024 * 1024))
            .await
            .expect("collect");
        // The sender never took its handle, so this report has nowhere to go.
        // It must not fail the receiving application.
        within(reporter.report(ACCEPTED, body.len() as u64))
            .await
            .expect("report");
        within(reporter.finish()).await.expect("finish");
        body.len()
    });

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let payload = vec![0x11u8; 1024 * 1024];
    let mut transfer = within(pusher.open(TransferMeta::default().with_report([ACCEPTED])))
        .await
        .expect("open");
    within(transfer.write_all(&payload)).await.expect("write");
    let delivery = transfer.finish().expect("finish");
    within(delivery.delivered()).await.expect("delivered");

    assert_eq!(receiver.await.expect("receiver"), payload.len());
    client.shutdown().await;
}

#[tokio::test]
async fn a_peer_that_ignores_cursor_streams_completes_every_transfer() {
    // The same rule from the other side: the receiver never takes its
    // reporter. The transfer succeeds, and the sender learns "no more
    // cursors" when the connection ends rather than waiting forever.
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let receiver = tokio::spawn(async move {
        let transfer = within(puller.recv()).await.expect("recv");
        within(transfer.collect(64 * 1024)).await.expect("collect")
    });

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let mut transfer = within(pusher.open(TransferMeta::default().with_report([ACCEPTED])))
        .await
        .expect("open");
    let mut cursors = transfer.cursors().expect("cursors");
    within(transfer.write_all(b"work")).await.expect("write");
    let delivery = transfer.finish().expect("finish");
    within(delivery.delivered()).await.expect("delivered");
    assert_eq!(receiver.await.expect("receiver"), b"work");

    // Nothing was reported, and nothing ever will be: the connection going
    // away is the only thing that can say so.
    client.shutdown().await;
    assert_eq!(within(cursors.changed()).await, None);
    assert!(cursors.snapshot().is_empty());
}

#[tokio::test]
async fn a_cursor_stream_for_an_unknown_report_id_is_reset_and_the_connection_survives() {
    let server = Server::start().await;
    let replier = server.listener.replier("/t").expect("replier");

    let endpoint = raw::client_endpoint(&server.certs);
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

#[tokio::test]
async fn an_application_level_is_carried_and_not_interpreted() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let stage = CursorLevel::application(17).expect("17 is above the floor");

    let receiver = tokio::spawn(async move {
        let transfer = within(puller.recv()).await.expect("recv");
        assert_eq!(transfer.meta().report, vec![stage]);
        let mut reporter = transfer.reporter().expect("a reporter");
        let body = within(transfer.collect(1024)).await.expect("collect");
        // A level the sender never ordered is ignored rather than refused:
        // the order says what the sender wants to hear.
        within(reporter.report(STORED, 7)).await.expect("report");
        within(reporter.report(stage, body.len() as u64))
            .await
            .expect("report");
        within(reporter.finish()).await.expect("finish");
    });

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let mut transfer = within(pusher.open(TransferMeta::default().with_report([stage])))
        .await
        .expect("open");
    let mut cursors = transfer.cursors().expect("cursors");
    within(transfer.write_all(b"batch")).await.expect("write");
    transfer.finish().expect("finish");

    let set = within(cursors.changed()).await.expect("a cursor arrived");
    assert_eq!(set.offset(stage), Some(5));
    // The unordered level never reached the wire, so it is absent here.
    assert_eq!(set.offset(STORED), None);
    assert_eq!(set.iter().collect::<Vec<_>>(), vec![(stage, 5)]);

    receiver.await.expect("receiver");
    client.shutdown().await;
}

#[tokio::test]
async fn more_levels_than_the_cap_is_a_limit_error() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    // One past the cap, all distinct, so nothing is deduplicated away.
    let too_many = (0..=MAX_REPORT_LEVELS as u64)
        .map(|i| CursorLevel::Application(CursorLevel::APPLICATION_FLOOR + i));
    let err = within(pusher.open(TransferMeta::default().with_report(too_many)))
        .await
        .expect_err("a report order past the cap");
    assert!(matches!(err, Error::LimitExceeded), "{err:?}");

    // Nothing was sent: the cap is checked before a stream is opened, so the
    // next transfer is the first the puller sees.
    within(pusher.send(b"after")).await.expect("send");
    let transfer = within(puller.recv()).await.expect("recv");
    assert!(transfer.meta().report.is_empty());
    assert_eq!(
        within(transfer.collect(64)).await.expect("collect"),
        b"after"
    );

    client.shutdown().await;
}

#[tokio::test]
async fn a_reporter_whose_peer_vanished_reports_without_failing() {
    // The swallow rule: a cursor is never load-bearing, so a write that
    // cannot land must not fail the application doing the reporting. The
    // sender is gone by the time the reporter opens its stream.
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");
    within(pusher.send_with(TransferMeta::default().with_report([ACCEPTED]), b"orphaned"))
        .await
        .expect("send");

    let transfer = within(puller.recv()).await.expect("recv");
    let mut reporter = transfer.reporter().expect("a reporter");
    let body = within(transfer.collect(1024)).await.expect("collect");
    assert_eq!(body, b"orphaned");

    // The whole client runtime goes away, connection and all.
    client.shutdown().await;
    drop(pusher);

    within(reporter.report(ACCEPTED, body.len() as u64))
        .await
        .expect("a report on a dead connection is not an error");
    within(reporter.finish())
        .await
        .expect("finishing a report that never opened is not an error");
}

#[tokio::test]
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

/// Pushes one raw transfer ordering `Stored`, has the puller report a hundred
/// advances at `bytes`/`interval`, and returns the records that reached the
/// wire.
async fn run_with_granularity(
    server: &Server,
    puller: &weida::Puller,
    bytes: u64,
    interval: Duration,
    report_id: u64,
) -> Vec<(CursorLevel, u64)> {
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    let header = weida_protocol::DataHeader {
        report_id: Some(report_id),
        report: vec![STORED],
        ..weida_protocol::DataHeader::addressed("/jobs")
    };
    push_raw(&conn, &header, b"body").await;

    let transfer = within(puller.recv()).await.expect("recv");
    let mut reporter = transfer
        .reporter()
        .expect("a reporter")
        .with_granularity(bytes, interval);
    let _ = within(transfer.collect(1024)).await.expect("collect");
    for offset in 1..=100u64 {
        within(reporter.report(STORED, offset))
            .await
            .expect("report");
    }
    within(reporter.finish()).await.expect("finish");

    read_report(&conn, report_id).await
}

#[tokio::test]
async fn final_only_emits_exactly_one_record_per_level() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let endpoint = raw::client_endpoint(&server.certs);
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

#[tokio::test]
async fn a_level_the_reporter_cannot_honour_is_absent_rather_than_fatal() {
    // The order is a request for reporting, not a guarantee: a level this
    // side cannot reach is simply missing from the report, and the transfer
    // succeeds. A level a peer *must* reach is the negotiated
    // `acknowledgement` dimension of HELLO instead.
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let receiver = tokio::spawn(async move {
        let transfer = within(puller.recv()).await.expect("recv");
        let reporter = transfer.reporter().expect("a reporter");
        assert_eq!(reporter.levels(), [STORED]);
        let body = within(transfer.collect(1024)).await.expect("collect");
        // This side has no store, so it reports nothing at all — not a zero,
        // and not an error.
        within(reporter.finish()).await.expect("finish");
        body
    });

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let mut transfer = within(pusher.open(TransferMeta::default().with_report([STORED])))
        .await
        .expect("open");
    let mut cursors = transfer.cursors().expect("cursors");
    within(transfer.write_all(b"durable?"))
        .await
        .expect("write");
    let delivery = transfer.finish().expect("finish");
    within(delivery.delivered()).await.expect("delivered");
    assert_eq!(receiver.await.expect("receiver"), b"durable?");

    // No cursor stream was ever opened, so the reader learns only that no
    // more are coming, and `Stored` is absent from the final snapshot.
    client.shutdown().await;
    assert_eq!(within(cursors.changed()).await, None);
    assert_eq!(cursors.snapshot().offset(STORED), None);
    assert!(cursors.snapshot().is_empty());
}
