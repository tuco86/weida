//! Hostile-peer tests: raw `quinn` endpoints speaking hand-encoded frames.
//!
//! Master doc §81 rule 18: all network input is hostile. These tests send byte
//! sequences the library would never produce, and assert on the QUIC
//! application error codes and on the outcomes the local API reports. The
//! library has no test hooks; everything below is written against the wire.

mod common;

use std::time::Duration;

use common::{Certs, Server, raw};
use weida::{Error, Runtime, RuntimeConfig, TransferMeta, codes};
use weida_protocol::header::{GuaranteeSet, OrderingMode};
use weida_protocol::{
    CreditHeader, DataHeader, ErrorHeader, FlowHeader, FrameKind, Hello, MAGIC, SubscriptionHeader,
    encode_frame, encode_preamble,
};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

// --- hostile client against our server ----------------------------------

#[tokio::test]
async fn garbage_first_bytes_close_the_connection() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/t").expect("replier");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    // Not our magic byte: the stream cannot be interpreted at all.
    raw::send_raw(&conn, &[0xff, 0xff, 0xff, 0xff]).await;

    let code = within(raw::closed_code(&conn)).await;
    assert_eq!(
        code,
        codes::PROTOCOL_VIOLATION,
        "expected a violation close"
    );

    // The binding survives: one hostile connection must not take out the
    // listener. A fresh well-behaved client still gets served.
    let replier = server.listener.replier("/ok").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let body = request.body().read_capped(64).await.expect("body");
        assert_eq!(body, b"ping");
        let reply = request.reply(TransferMeta::default()).await.expect("reply");
        reply.finish().expect("finish");
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/ok")))
        .await
        .expect("connect");
    let reply = within(requester.request(b"ping")).await.expect("request");
    assert!(within(reply.collect(64)).await.expect("collect").is_empty());
    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn an_unknown_frame_kind_closes_the_connection() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    // Correct magic, undefined kind 9. Kinds 5..=255 are reserved and are a
    // violation, not a forward-compatibility hook.
    raw::send_raw(&conn, &[MAGIC, 9, 0x00]).await;
    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_flow_from_a_peer_without_the_datagram_capability_closes_the_connection() {
    // Kind 7 is FLOW, legal only when both HELLOs listed capability code 1.
    // The server enables flows; this peer's HELLO lists nothing
    // (`docs/PROTOCOL.md` §6.1, §3.2).
    let server = Server::start_with(weida::Limits {
        datagram_receive_bytes: weida::DEFAULT_DATAGRAM_RECEIVE_BYTES,
        ..weida::Limits::default()
    })
    .await;
    let _acceptor = server.listener.acceptor("/v").expect("acceptor");
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;
    let mut stream = conn.open_uni().await.expect("open uni");
    stream
        .write_all(&encode_frame(
            FrameKind::Flow,
            &FlowHeader::new("/v", 1).encode(),
        ))
        .await
        .expect("write flow");
    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_credit_frame_for_a_path_with_no_queue_is_ignored() {
    // Kind 5 is CREDIT. A peer may send one for any path, and a
    // path with no queue behind it has nothing to honour — so the frame is
    // dropped and the connection survives. Closing on it would let any peer
    // kill a connection with a legal frame, and there is no reply half to
    // refuse on ([PROTOCOL.md](../../docs/PROTOCOL.md) §4).
    let server = Server::start().await;
    let _replier = server.listener.replier("/rpc").expect("replier");
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    let credit = CreditHeader::new("/rpc", "", 7);
    raw::send_raw(&conn, &encode_frame(FrameKind::Credit, &credit.encode())).await;

    // Still usable: an exchange on the same connection is answered, which no
    // closed connection could do.
    let (mut send, mut recv) = raw::open_exchange(&conn, &DataHeader::addressed("/rpc")).await;
    send.write_all(b"ping").await.expect("write request");
    send.finish().expect("finish request");
    let served = tokio::spawn(async move {
        let request = _replier.accept().await.expect("accept");
        let mut reply = request
            .reply(weida::TransferMeta::default())
            .await
            .expect("reply");
        reply.write_all(b"pong").await.expect("write reply");
        reply.finish().expect("finish reply");
    });
    let mut answer = Vec::new();
    within(async {
        let mut scratch = [0u8; 256];
        while let Ok(Some(n)) = recv.read(&mut scratch).await {
            answer.extend_from_slice(&scratch[..n]);
        }
    })
    .await;
    within(served).await.expect("served");
    assert!(
        answer.ends_with(b"pong"),
        "the connection did not survive the credit frame: {answer:?}"
    );
}

#[tokio::test]
async fn a_malformed_credit_header_closes_the_connection() {
    // A legal *kind* with an illegal header is still a framing violation: the
    // three keys of §6.6 are required, and a filter that breaks §6.4's grammar
    // is refused at the codec boundary.
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // `A1 00 62 2F 71`: endpoint only, no filter and no limit.
    raw::send_raw(
        &conn,
        &encode_frame(FrameKind::Credit, &[0xA1, 0x00, 0x62, 0x2F, 0x71]),
    )
    .await;
    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn hello_with_an_unsupported_version_fails_negotiation() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    let hello = Hello {
        versions: vec![99],
        ..Hello::v0(16 * 1024, 8)
    };
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::NEGOTIATION_FAILED
    );
}

#[tokio::test]
async fn a_required_capability_we_lack_fails_negotiation() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    let hello = Hello {
        capabilities: vec![7],
        required_capabilities: vec![7],
        ..Hello::v0(16 * 1024, 8)
    };
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::NEGOTIATION_FAILED
    );
}

/// A peer that requires a guarantee level this build does not offer gets a
/// failed handshake, never a quieter success
/// (`docs/PROTOCOL.md` §2.3 step 6).
#[tokio::test]
async fn a_required_guarantee_level_we_do_not_offer_fails_negotiation() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    let wants_ordering = GuaranteeSet {
        ordering: OrderingMode::PerProducerDetect,
        ..GuaranteeSet::CORE
    };
    let hello = Hello {
        guarantees_offered: Some(wants_ordering),
        guarantees_required: Some(wants_ordering),
        ..Hello::v0(16 * 1024, 8)
    };
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::NEGOTIATION_FAILED
    );
}

/// A guarantee set the grammar of §6.5 forbids is a framing violation, not a
/// negotiation failure: the header never becomes a declaration at all.
#[tokio::test]
async fn a_malformed_guarantee_set_is_a_framing_violation() {
    for header in [
        // An unknown ordering level.
        raw::hello_with_guarantee_map(&[(4, 99)]),
        // `durability` without `Stored`/`Replicated`.
        raw::hello_with_guarantee_map(&[(2, 1)]),
        // `replicas` of 1, and without `Replicated`.
        raw::hello_with_guarantee_map(&[(3, 1)]),
        // `Bounded` deduplication with no window.
        raw::hello_with_guarantee_map(&[(5, 1)]),
        // A window with no `Bounded`.
        raw::hello_with_guarantee_map(&[(6, 1_000)]),
        // `control_isolated` is a flag, not a number.
        raw::hello_with_guarantee_map(&[(9, 2)]),
    ] {
        let server = Server::start().await;
        let endpoint = raw::client_endpoint(&server.certs);
        let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
            .await
            .expect("handshake");
        raw::send_frame(&conn, FrameKind::Hello, &header).await;
        assert_eq!(
            within(raw::closed_code(&conn)).await,
            codes::PROTOCOL_VIOLATION
        );
    }
}

#[tokio::test]
async fn an_oversized_header_length_closes_the_connection_before_allocating() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    // Claim a 1 MiB header against a 16 KiB cap, and send none of it.
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, 1024 * 1024, &mut bytes);
    raw::send_raw(&conn, &bytes).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_malformed_cbor_header_closes_the_connection() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // A DATA preamble announcing three bytes of header, followed by bytes that
    // are not a CBOR map.
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, 3, &mut bytes);
    bytes.extend_from_slice(&[0xff, 0xff, 0xff]);
    raw::send_raw(&conn, &bytes).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn uni_data_without_an_endpoint_is_a_violation() {
    // The decoder accepts an empty DATA map — a reply half legitimately sends
    // one — so the endpoint requirement is enforced where the stream context
    // is known: dispatch.
    let server = Server::start().await;
    let _puller = server.listener.puller("/jobs").expect("puller");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    let header = DataHeader::reply().encode();
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, header.len() as u64, &mut bytes);
    bytes.extend_from_slice(&header);
    raw::send_raw(&conn, &bytes).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn bidi_data_without_an_endpoint_is_a_violation() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/t").expect("replier");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    let (mut send, _recv) = conn.open_bi().await.expect("open bi");
    let header = DataHeader::reply().encode();
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, header.len() as u64, &mut bytes);
    bytes.extend_from_slice(&header);
    send.write_all(&bytes).await.expect("write header");

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn error_frame_on_a_uni_stream_is_a_violation() {
    // ERROR answers a request, and a request always arrives on a bidirectional
    // stream. On a unidirectional one it refers to nothing.
    let server = Server::start().await;
    let _replier = server.listener.replier("/t").expect("replier");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    let header = ErrorHeader::new(weida::ErrorCode::Internal).encode();
    raw::send_frame(&conn, FrameKind::Error, &header).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_non_data_frame_may_not_open_a_bidirectional_stream() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/t").expect("replier");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    let (mut send, _recv) = conn.open_bi().await.expect("open bi");
    send.write_all(&weida_protocol::encode_frame(
        FrameKind::Subscribe,
        &SubscriptionHeader::new("/t", "").encode(),
    ))
    .await
    .expect("write frame");

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn bidi_request_to_a_pull_path_is_refused_with_unsupported() {
    let server = Server::start().await;
    let _puller = server.listener.puller("/jobs").expect("puller");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // A well-formed exchange aimed at a path that serves one-way transfers.
    let (mut send, mut recv) = conn.open_bi().await.expect("open bi");
    let header = DataHeader::addressed("/jobs").encode();
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, header.len() as u64, &mut bytes);
    bytes.extend_from_slice(&header);
    send.write_all(&bytes).await.expect("write header");

    // The refusal is a real ERROR frame on this exchange's own reply half —
    // not a stream of its own, and with nothing to correlate.
    let (preamble, error_header) = within(raw::read_frame(&mut recv)).await;
    assert_eq!(preamble.kind, FrameKind::Error);
    let error = ErrorHeader::decode(&error_header).expect("decode ERROR");
    assert_eq!(error.code, weida::ErrorCode::Unsupported.to_wire());

    // And the request half is stopped, so a large payload is not accepted.
    let stopped = within(async {
        let payload = vec![0u8; 1024 * 1024];
        loop {
            if let Err(e) = send.write_all(&payload).await {
                return e;
            }
        }
    })
    .await;
    match stopped {
        quinn::WriteError::Stopped(code) => assert_eq!(code.into_inner(), codes::UNSUPPORTED),
        other => panic!("expected STOP_SENDING(UNSUPPORTED), got {other}"),
    }

    // The connection itself stays healthy: a refused exchange is not a
    // protocol violation.
    assert!(conn.close_reason().is_none());
}

#[tokio::test]
async fn bidi_request_to_an_unknown_path_is_refused_with_unknown_endpoint() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/known").expect("replier");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    let (mut send, mut recv) = conn.open_bi().await.expect("open bi");
    let header = DataHeader::addressed("/nowhere").encode();
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, header.len() as u64, &mut bytes);
    bytes.extend_from_slice(&header);
    send.write_all(&bytes).await.expect("write header");

    let (preamble, error_header) = within(raw::read_frame(&mut recv)).await;
    assert_eq!(preamble.kind, FrameKind::Error);
    let error = ErrorHeader::decode(&error_header).expect("decode ERROR");
    assert_eq!(error.code, weida::ErrorCode::UnknownEndpoint.to_wire());
    assert!(conn.close_reason().is_none());
}

// --- path reports (`docs/PROTOCOL.md` §6.10) ------------------------------

/// A raw peer whose HELLO lists capability code `2`, against a server whose
/// profile does (`reporting`) or does not ask for path reports.
async fn reporting_peer(reporting: bool) -> (Server, quinn::Connection) {
    let server = Server::start_with(weida::Limits {
        path_report: reporting,
        ..weida::Limits::default()
    })
    .await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    let hello = Hello {
        capabilities: vec![weida_protocol::CAPABILITY_PATH_REPORT],
        ..Hello::v0(16 * 1024, 1024)
    };
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;
    (server, conn)
}

/// Opens a REPORT stream and writes its head frame and then `records`.
async fn report_stream(conn: &quinn::Connection, records: &[u8]) -> quinn::SendStream {
    let mut stream = conn.open_uni().await.expect("open uni");
    let mut bytes = encode_frame(FrameKind::Report, &weida_protocol::ReportHead.encode());
    bytes.extend_from_slice(records);
    stream.write_all(&bytes).await.expect("write report");
    stream
}

#[tokio::test]
async fn a_report_without_the_agreed_capability_closes_the_connection() {
    // The peer lists code 2, the server does not: nothing was agreed.
    let (_server, conn) = reporting_peer(false).await;
    let _stream = report_stream(&conn, &[]).await;
    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_second_report_stream_closes_the_connection() {
    let (_server, conn) = reporting_peer(true).await;
    let _first = report_stream(&conn, &[]).await;
    let _second = report_stream(&conn, &[]).await;
    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_report_record_above_256_bytes_closes_the_connection() {
    let (_server, conn) = reporting_peer(true).await;
    // `41 01` is a length of 257.
    let _stream = report_stream(&conn, &[0x41, 0x01]).await;
    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_report_record_cut_at_fin_closes_the_connection() {
    let (_server, conn) = reporting_peer(true).await;
    let mut stream = report_stream(&conn, &[0x0D, 0xA3, 0x00, 0x19]).await;
    stream.finish().expect("finish");
    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_whole_report_record_keeps_the_connection() {
    // The control case: the same stream with a whole record and FIN is
    // legal, so the violations above are the record rules and nothing else.
    let (_server, conn) = reporting_peer(true).await;
    let record = [
        0x0D, 0xA3, 0x00, 0x19, 0x61, 0xA8, 0x02, 0x19, 0x2E, 0xE0, 0x07, 0x19, 0x04, 0xB0,
    ];
    let mut stream = report_stream(&conn, &record).await;
    stream.finish().expect("finish");
    within(stream.stopped()).await.expect("acknowledged");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(conn.close_reason().is_none(), "the connection must survive");
}

// --- hostile server against our client ----------------------------------

/// Spawns a raw server that runs `behaviour` for its first connection.
fn raw_server<F, Fut>(certs: &Certs, behaviour: F) -> std::net::SocketAddr
where
    F: FnOnce(quinn::Connection) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let (endpoint, addr) = raw::server_endpoint(certs);
    tokio::spawn(async move {
        let incoming = endpoint.accept().await.expect("a connection");
        let conn = incoming.await.expect("handshake");
        behaviour(conn).await;
        // Keep the endpoint alive for the duration of the behaviour.
        drop(endpoint);
    });
    addr
}

fn client_for() -> Runtime {
    Runtime::new(RuntimeConfig::default()).expect("client runtime")
}

#[tokio::test]
async fn a_server_that_never_answers_yields_indeterminate() {
    let certs = Certs::generate();
    let addr = raw_server(&certs, |conn| async move {
        raw::send_hello(&conn).await;
        // Read the request to FIN, then vanish without a reply.
        let (_send, mut recv, header) = raw::accept_exchange(&conn).await;
        assert_eq!(header.endpoint.as_deref(), Some("/t"));
        let mut sink = vec![0u8; 64 * 1024];
        while let Ok(Some(n)) = recv.read(&mut sink).await {
            if n == 0 {
                break;
            }
        }
        conn.close(quinn::VarInt::from_u32(0), b"bye");
    });

    let client = client_for();
    let requester = client.requester(certs.client_tls());
    within(requester.connect(&format!("weida://127.0.0.1:{}/t", addr.port())))
        .await
        .expect("connect");

    let (mut transfer, reply) = within(requester.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"payload")).await.expect("write");
    let delivery = transfer.finish().expect("finish");

    // The receipt reports the transport truth and nothing else. The peer's
    // stack really did take every byte before it closed, so `Ok` here is
    // correct — and is precisely why a transport receipt is not an
    // application acknowledgement. A close that races the acknowledgement
    // yields `Indeterminate` instead; both are honest.
    match within(delivery.delivered()).await {
        Ok(()) | Err(Error::Indeterminate) => {}
        Err(e) => panic!("unexpected receipt outcome: {e:?}"),
    }

    // What is missing is the answer, and whether the peer ever produced one is
    // genuinely unknown (master doc §22).
    let err = within(reply.recv()).await.expect_err("must not succeed");
    assert!(
        matches!(err, Error::Indeterminate),
        "expected Indeterminate, got {err:?}"
    );
}

#[tokio::test]
async fn a_server_that_disappears_mid_stream_yields_connection_lost() {
    let certs = Certs::generate();
    let addr = raw_server(&certs, |conn| async move {
        raw::send_hello(&conn).await;
        let (_send, mut recv, _header) = raw::accept_exchange(&conn).await;
        // Read only a little, then drop the connection before the FIN.
        let mut sink = vec![0u8; 4096];
        let _ = recv.read(&mut sink).await;
        conn.close(quinn::VarInt::from_u32(0), b"gone");
    });

    let client = client_for();
    let requester = client.requester(certs.client_tls());
    within(requester.connect(&format!("weida://127.0.0.1:{}/t", addr.port())))
        .await
        .expect("connect");

    let (mut transfer, _reply) = within(requester.open(TransferMeta::default()))
        .await
        .expect("open");

    // The connection dies before our FIN, so the transfer definitely did not
    // arrive: this is a definite failure, not an unknown outcome.
    let payload = vec![0x5au8; 4 * 1024 * 1024];
    let err = within(async {
        loop {
            if let Err(e) = transfer.write_all(&payload).await {
                return e;
            }
        }
    })
    .await;
    assert!(
        matches!(err, Error::ConnectionLost(_)),
        "expected ConnectionLost, got {err:?}"
    );
    assert!(err.is_definite_failure());
}

#[tokio::test]
async fn a_server_that_never_sends_hello_is_dropped_after_the_timeout() {
    let certs = Certs::generate();
    let addr = raw_server(&certs, |conn| async move {
        // Stay silent: no HELLO, ever.
        conn.closed().await;
    });

    let limits = weida::Limits {
        hello_timeout_ms: 500,
        ..weida::Limits::default()
    };
    let client = Runtime::new(RuntimeConfig {
        limits,
        ..RuntimeConfig::default()
    })
    .expect("client runtime");

    let requester = client.requester(certs.client_tls());
    let err = within(requester.connect(&format!("weida://127.0.0.1:{}/t", addr.port())))
        .await
        .expect_err("connect must not succeed without negotiation");
    assert!(
        matches!(err, Error::Negotiation(_) | Error::ConnectionLost(_)),
        "expected a negotiation failure, got {err:?}"
    );
}

// --- subscriptions and one-way transfers --------------------------------

#[tokio::test]
async fn subscribe_with_an_oversized_filter_closes_the_connection() {
    let server = Server::start().await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // One byte over the 256-byte filter cap. The header is well-formed CBOR,
    // so only the decoder's per-field cap can catch it.
    let filter = "a".repeat(weida_protocol::header_limits::MAX_FILTER_BYTES + 1);
    let header = SubscriptionHeader::new("/md", filter).encode();
    raw::send_frame(&conn, FrameKind::Subscribe, &header).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_subscribe_flood_closes_the_connection_with_limit_exceeded() {
    let limits = weida::Limits {
        max_subscriptions: 8,
        ..weida::Limits::default()
    };
    let server = Server::start_with(limits).await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // Distinct filters, so none is deduplicated: one past the cap.
    for i in 0..=limits.max_subscriptions {
        let header = SubscriptionHeader::new("/md", format!("f{i}.")).encode();
        // The connection dies partway through, so a failed send is expected.
        let Ok(mut stream) = conn.open_uni().await else {
            break;
        };
        if stream
            .write_all(&weida_protocol::encode_frame(FrameKind::Subscribe, &header))
            .await
            .is_err()
        {
            break;
        }
        let _ = stream.finish();
    }

    // SUBSCRIBE arrives on a unidirectional stream, so there is no reply half
    // to answer with an ERROR frame: the connection is the only granularity
    // available.
    assert_eq!(within(raw::closed_code(&conn)).await, codes::LIMIT_EXCEEDED);
}

#[tokio::test]
async fn unsubscribing_an_unknown_filter_is_ignored() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // Neither the filter nor the connection is known to the registry.
    let header = SubscriptionHeader::new("/md", "never.").encode();
    raw::send_frame(&conn, FrameKind::Unsubscribe, &header).await;
    // An unknown path is equally harmless.
    let header = SubscriptionHeader::new("/nope", "x").encode();
    raw::send_frame(&conn, FrameKind::Unsubscribe, &header).await;

    // The connection survives and still accepts a real subscription.
    let header = SubscriptionHeader::new("/md", "px.").encode();
    raw::send_frame(&conn, FrameKind::Subscribe, &header).await;
    within(async {
        while publisher.filter_count() != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(conn.close_reason().is_none(), "the connection must survive");
}

#[tokio::test]
async fn data_before_hello_is_parked_not_rejected() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    // DATA first, HELLO second. Streams are unordered relative to each other,
    // so a transfer that overtakes our HELLO must be parked until negotiation
    // completes, not refused.
    let header = DataHeader::addressed("/jobs").encode();
    let mut stream = conn.open_uni().await.expect("open uni");
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, header.len() as u64, &mut bytes);
    bytes.extend_from_slice(&header);
    stream.write_all(&bytes).await.expect("write header");

    // Give the server a chance to (wrongly) reject before our HELLO arrives.
    tokio::time::sleep(Duration::from_millis(50)).await;
    raw::send_hello(&conn).await;

    stream
        .write_all(b"parked payload")
        .await
        .expect("write body");
    stream.finish().expect("finish");

    let transfer = within(puller.recv()).await.expect("recv");
    assert_eq!(transfer.meta().endpoint.as_deref(), Some("/jobs"));
    let body = within(transfer.collect(1024)).await.expect("collect");
    assert_eq!(body, b"parked payload");
    assert!(conn.close_reason().is_none(), "the connection must survive");
}

#[tokio::test]
async fn an_exchange_before_hello_is_parked_not_rejected() {
    let server = Server::start().await;
    let replier = server.listener.replier("/t").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let body = request.body().read_capped(1024).await.expect("body");
        assert_eq!(body, b"parked");
        let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
        reply.write_all(b"ok").await.expect("write reply");
        reply.finish().expect("finish");
    });

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    let (mut send, mut recv) = conn.open_bi().await.expect("open bi");
    let header = DataHeader::addressed("/t").encode();
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, header.len() as u64, &mut bytes);
    bytes.extend_from_slice(&header);
    send.write_all(&bytes).await.expect("write header");

    tokio::time::sleep(Duration::from_millis(50)).await;
    raw::send_hello(&conn).await;
    send.write_all(b"parked").await.expect("write body");
    send.finish().expect("finish");

    let (preamble, _header) = within(raw::read_frame(&mut recv)).await;
    assert_eq!(preamble.kind, FrameKind::Data);
    handler.await.expect("handler");
    assert!(conn.close_reason().is_none(), "the connection must survive");
}
