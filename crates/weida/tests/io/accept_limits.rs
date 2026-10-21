//! What a QUIC binding does with an Initial before it builds anything for it.
//!
//! Accepting an Initial builds the connection's state and runs the server half
//! of the TLS handshake for a source that has not proved the address it wrote.
//! These tests drive a binding with raw `quinn` dialers and a UDP relay that
//! can drop what the server sends back, which is what a spoofed source looks
//! like from the server's side: it never answers.

use crate::common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Server, raw};
use tokio::net::UdpSocket;
use weida::{Incoming, RuntimeConfig};
use weida_protocol::{DataHeader, FrameKind, SubscriptionHeader, encode_frame};

const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// A UDP relay in front of the server for exactly one dialler.
struct Relay {
    addr: SocketAddr,
    /// The first byte of every datagram the server sent toward the dialler,
    /// in order, whether or not the relay passed it on.
    from_server: Arc<Mutex<Vec<u8>>>,
    /// How many datagrams the dialler sent.
    from_dialler: Arc<AtomicUsize>,
}

/// Starts a relay to `server`. With `black_hole` it forwards what the dialler
/// sends and drops everything the server answers.
async fn relay(server: SocketAddr, black_hole: bool) -> Relay {
    let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await.expect("bind front"));
    let back = Arc::new(UdpSocket::bind("127.0.0.1:0").await.expect("bind back"));
    back.connect(server).await.expect("connect back");
    let addr = front.local_addr().expect("front address");

    let dialler: Arc<Mutex<Option<SocketAddr>>> = Arc::default();
    let from_server: Arc<Mutex<Vec<u8>>> = Arc::default();
    let from_dialler = Arc::new(AtomicUsize::new(0));

    {
        let (front, back) = (Arc::clone(&front), Arc::clone(&back));
        let (dialler, from_dialler) = (Arc::clone(&dialler), Arc::clone(&from_dialler));
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65_536];
            while let Ok((n, from)) = front.recv_from(&mut buf).await {
                *dialler.lock().expect("relay state") = Some(from);
                from_dialler.fetch_add(1, Ordering::Relaxed);
                let _ = back.send(&buf[..n]).await;
            }
        });
    }
    {
        let from_server = Arc::clone(&from_server);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65_536];
            while let Ok(n) = back.recv(&mut buf).await {
                if n == 0 {
                    continue;
                }
                from_server.lock().expect("relay state").push(buf[0]);
                if black_hole {
                    continue;
                }
                let target = *dialler.lock().expect("relay state");
                if let Some(target) = target {
                    let _ = front.send_to(&buf[..n], target).await;
                }
            }
        });
    }
    Relay {
        addr,
        from_server,
        from_dialler,
    }
}

fn is_refused(error: &quinn::ConnectionError) -> bool {
    matches!(
        error,
        quinn::ConnectionError::ConnectionClosed(close)
            if close.error_code == quinn::TransportErrorCode::CONNECTION_REFUSED
    )
}

/// A binding at `max_connections` refuses the next Initial outright: the
/// dialler sees `CONNECTION_REFUSED` instead of a completed handshake that is
/// then closed, and the binding spent no handshake on it.
#[tokio::test]
async fn a_binding_at_its_connection_limit_refuses_before_the_handshake() {
    let server = Server::start_os_with_config(RuntimeConfig {
        max_connections: 1,
        ..RuntimeConfig::default()
    })
    .await;
    let endpoint = raw::client_endpoint(&server.certs);

    let first = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("the first connection fits");
    raw::send_hello(&first).await;

    let refused = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect_err("the second connection is over the limit");
    assert!(
        is_refused(&refused),
        "expected CONNECTION_REFUSED: {refused:?}"
    );

    // The first one was not disturbed, and a place that frees up is taken.
    assert!(first.close_reason().is_none());
    first.close(quinn::VarInt::from_u32(0), b"done");
    let mut freed = false;
    for _ in 0..50 {
        match endpoint
            .connect(server.addr, "127.0.0.1")
            .expect("connect")
            .await
        {
            Ok(_) => {
                freed = true;
                break;
            }
            Err(e) => assert!(is_refused(&e), "{e:?}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(freed, "the freed place was never given out again");
}

/// Handshakes in progress are counted and capped: with two of them stuck
/// (their dialler never answers, as a spoofed source never does) the next
/// Initial is refused before any TLS state is built for it.
#[tokio::test]
async fn handshakes_in_progress_are_capped() {
    let server = Server::start_os_with_config(RuntimeConfig {
        max_handshakes: 2,
        // Retry off, so that the two Initials below are accepted and held.
        retry_above_handshakes: usize::MAX,
        ..RuntimeConfig::default()
    })
    .await;

    let mut held = Vec::new();
    for _ in 0..2 {
        let relay = relay(server.addr, true).await;
        let endpoint = raw::client_endpoint(&server.certs);
        let connecting = endpoint
            .connect(relay.addr, "127.0.0.1")
            .expect("start a handshake");
        held.push((relay, endpoint, tokio::spawn(connecting)));
    }
    within(async {
        while held
            .iter()
            .any(|(relay, _, _)| relay.from_dialler.load(Ordering::Relaxed) == 0)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // The server accepts an Initial as soon as it reads it.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let endpoint = raw::client_endpoint(&server.certs);
    let refused = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect_err("a third handshake is over the cap");
    assert!(
        is_refused(&refused),
        "expected CONNECTION_REFUSED: {refused:?}"
    );
}

/// Above `retry_above_handshakes` an Initial from an address the binding has
/// not validated is answered with a stateless Retry, and a dialler that can
/// receive at the address it wrote completes the handshake all the same.
#[tokio::test]
async fn above_the_threshold_an_unvalidated_initial_gets_a_retry() {
    let server = Server::start_os_with_config(RuntimeConfig {
        retry_above_handshakes: 0,
        ..RuntimeConfig::default()
    })
    .await;
    let relay = relay(server.addr, false).await;
    let endpoint = raw::client_endpoint(&server.certs);

    let conn = within(endpoint.connect(relay.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("the dialler answers the Retry and the handshake completes");
    raw::send_hello(&conn).await;

    let first = relay.from_server.lock().expect("relay state")[0];
    assert_eq!(
        first & 0xF0,
        0xF0,
        "the first packet the server sent was a Retry (long header, type 3), got {first:#04x}"
    );
}

/// The control for the test above: a binding that is not busy accepts an
/// Initial at once, so a quiet binding adds no round trip.
#[tokio::test]
async fn a_quiet_binding_accepts_an_initial_without_a_retry() {
    let server = Server::start_os().await;
    let relay = relay(server.addr, false).await;
    let endpoint = raw::client_endpoint(&server.certs);

    let _conn = within(endpoint.connect(relay.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("connect");
    let first = relay.from_server.lock().expect("relay state")[0];
    assert_ne!(
        first & 0xF0,
        0xF0,
        "a quiet binding sends no Retry: the first packet was {first:#04x}"
    );
}

/// A connection that consumed on a queue nobody is reading owes that queue a
/// notice when it closes, and the notice waits for room. The wait must not
/// keep the dead connection counted against `max_connections`: a binding
/// whose queue is stuck would otherwise refuse every new peer once as many
/// closed ones as it admits are waiting.
#[tokio::test]
async fn a_closed_consumer_waiting_to_be_announced_does_not_hold_a_place() {
    let server = Server::start_os_with_config(RuntimeConfig {
        max_connections: 2,
        endpoint_queue: 1,
        ..RuntimeConfig::default()
    })
    .await;
    let acceptor = server.listener.acceptor("/q").expect("acceptor");
    let endpoint = raw::client_endpoint(&server.certs);

    // A consumer subscribes. Reading its event once proves it was recorded,
    // and leaves the queue's single slot free.
    let consumer = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("the first connection fits");
    raw::send_hello(&consumer).await;
    raw::send_frame(
        &consumer,
        FrameKind::Subscribe,
        &SubscriptionHeader::new("/q", "").encode(),
    )
    .await;
    let id = match within(acceptor.accept()).await.expect("accept") {
        Incoming::Subscribed(consumer) => consumer.id(),
        other => panic!("expected the subscription, got {other:?}"),
    };

    // Another peer fills that slot with a transfer nobody reads.
    let filler = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("the second connection fits");
    raw::send_hello(&filler).await;
    let mut transfer = encode_frame(FrameKind::Data, &DataHeader::addressed("/q").encode());
    transfer.extend_from_slice(b"payload");
    raw::send_raw(&filler, &transfer).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The consumer goes away. Its notice cannot be delivered, so it waits.
    consumer.close(quinn::VarInt::from_u32(0), b"gone");

    // The place it held is given out again, though the notice still waits.
    let mut freed = false;
    for _ in 0..100 {
        match endpoint
            .connect(server.addr, "127.0.0.1")
            .expect("connect")
            .await
        {
            Ok(_) => {
                freed = true;
                break;
            }
            Err(e) => assert!(is_refused(&e), "{e:?}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        freed,
        "a closed connection kept its place while its notice waited for the queue"
    );

    // The notice itself is not lost: it follows the transfer once the queue
    // is read.
    assert!(matches!(
        within(acceptor.accept()).await.expect("accept"),
        Incoming::Stream(_)
    ));
    match within(acceptor.accept()).await.expect("accept") {
        Incoming::Unsubscribed {
            id: gone,
            filter: None,
        } => assert_eq!(gone, id),
        other => panic!("expected the consumer's departure, got {other:?}"),
    }
}
