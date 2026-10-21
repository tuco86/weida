//! The tests of `tests/reconnect.rs` that need the network, a process or real time.

use crate::common;
use common::Certs;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;
use weida::{
    Error, GiveUp, LossCause, Network, PeerEvent, ReconnectPolicy, Runtime, RuntimeConfig,
};

use crate::reconnect::*;

/// Claim: an `AF_UNIX` server that is shut down and bound again on the same
/// socket path is redialled under the same policy as QUIC, with the same
/// events. One difference is the transport's, not the redial's: a socket
/// peer has no idle timeout and nothing on the control connection after
/// the HELLOs, so the loss is learned at the **next open**, which is the
/// send during the outage here (`docs/PATTERNS.md` §1.10). The body is
/// still the runtime's from that call.
#[cfg(unix)]
#[tokio::test]
async fn a_unix_socket_that_is_rebound_is_redialled() {
    let dir = std::env::temp_dir().join(format!("weida-rc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("socket directory");
    let path = dir.join("s");
    let encoded: String = path
        .to_str()
        .expect("utf-8 socket path")
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect();
    let url = format!("weida+unix://{encoded}/jobs");

    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
    let listener = server.listener();
    let binding = listener.bind_unix(&path).expect("bind unix");
    let puller = listener.puller("/jobs").expect("puller");

    let client = Runtime::new(client_config()).expect("client runtime");
    let pusher = client.pusher(weida::Trust::by_address());
    let mut events = pusher.events();
    within(pusher.connect(&url)).await.expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    within(pusher.send(b"before")).await.expect("send");
    assert_eq!(pulled(&puller).await, b"before");

    drop(puller);
    drop(binding);
    drop(listener);
    server.shutdown().await;
    within(pusher.send(b"during"))
        .await
        .expect("send during outage");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));

    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
    let listener = server.listener();
    let binding = listener.bind_unix(&path).expect("rebind unix");
    let puller = listener.puller("/jobs").expect("puller");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    assert_eq!(pulled(&puller).await, b"during");
    drop(binding);

    client.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
/// Claim: a binding handed over tells its clients nothing until the
/// successor serves, and then each client reconnects exactly once and at
/// once. The dialler reports `Lost { PeerRestarting }`, a first `Retrying`
/// with no delay, even under `stop_on_peer_closed`, and the very next event
/// is `Connected`: the redial was not refused, so no backoff followed.
/// `hand_over` returns as soon as the closes are held, well inside its
/// bound.
///
/// The socket is handed over the way `SCM_RIGHTS` would hand it: the
/// duplicate descriptor in the `Handoff`. The old runtime stays alive until
/// the end, so the successor's delivery also shows that a held socket is no
/// longer read.
#[tokio::test]
async fn a_handed_over_binding_tells_its_clients_once_the_successor_serves() {
    const HANDOVER_TIMEOUT: Duration = Duration::from_secs(10);
    let certs = Certs::generate();
    let old = Runtime::new(RuntimeConfig {
        shutdown_timeout: HANDOVER_TIMEOUT,
        ..RuntimeConfig::default()
    })
    .expect("old runtime");
    let old_listener = old.listener();
    let old_binding = within(
        old_listener.bind_quic("127.0.0.1:0".parse().expect("loopback"), certs.server_tls()),
    )
    .await
    .expect("bind");
    let addr = old_binding.local_addr();
    let puller = old_listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        reconnect: ReconnectPolicy {
            initial: Duration::from_millis(200),
            max: Duration::from_millis(200),
            jitter: false,
            stop_on_peer_closed: true,
            ..ReconnectPolicy::default()
        },
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let mut events = pusher.events();
    within(pusher.connect(&format!("weida://127.0.0.1:{}/jobs", addr.port())))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    within(pusher.send(b"before")).await.expect("send before");
    assert_eq!(pulled(&puller).await, b"before");

    drop(puller);
    let started = std::time::Instant::now();
    let handoff = within(old_binding.hand_over()).await;
    assert!(
        started.elapsed() < HANDOVER_TIMEOUT / 2,
        "hand_over returns once the closes are held, not at its timeout"
    );
    assert!(
        handoff.held() >= 1,
        "the pusher's close is held, got {handoff:?}"
    );
    assert_eq!(handoff.local_addr(), addr);
    let (socket, held) = handoff.into_parts();
    assert!(
        timeout(Duration::from_millis(300), events.recv())
            .await
            .is_err(),
        "the client is told nothing before the successor serves"
    );

    let new = Runtime::new(RuntimeConfig::default()).expect("new runtime");
    let new_listener = new.listener();
    let puller = new_listener.puller("/jobs").expect("puller");
    let new_binding = within(new_listener.bind_quic_socket(socket, certs.server_tls()))
        .await
        .expect("serve the handed socket");
    assert_eq!(new_binding.local_addr(), addr);
    let sent = within(new_binding.take_over(&held))
        .await
        .expect("take over");
    assert!(sent >= 1, "the held close is sent, got {sent}");

    let lost = within(events.recv()).await.expect("event stream open");
    assert!(
        matches!(
            lost,
            PeerEvent::Lost {
                cause: LossCause::PeerRestarting,
                ..
            }
        ),
        "a handover is reported as a restart, got {lost:?}"
    );
    let retrying = within(events.recv()).await.expect("event stream open");
    let PeerEvent::Retrying { attempt, delay, .. } = retrying else {
        panic!("a restart is redialled, got {retrying:?}");
    };
    assert_eq!(attempt, 1);
    assert_eq!(delay, Duration::ZERO, "the first redial does not wait");
    let connected = within(events.recv()).await.expect("event stream open");
    assert!(
        matches!(connected, PeerEvent::Connected { .. }),
        "the first redial is accepted, so one reconnect and no backoff, got {connected:?}"
    );
    within(pusher.send(b"after")).await.expect("send after");
    assert_eq!(pulled(&puller).await, b"after");

    client.shutdown().await;
    within(old.shutdown()).await;
}
/// Claim: a handover reaches the operation, not only the event stream.
/// Under `ReconnectPolicy::never()` nothing redials a restart either, and
/// the next send fails with `ConnectionLost(PeerRestarting)`, which a caller
/// tells apart from a plain shutdown's `PeerClosed`.
#[tokio::test]
async fn a_send_after_a_handover_names_the_restart() {
    let certs = Certs::generate();
    let server = Server::start(
        Network::default(),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        reconnect: ReconnectPolicy::never(),
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let mut events = pusher.events();
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    within(pusher.send(b"before")).await.expect("send");
    assert_eq!(pulled(&puller).await, b"before");

    drop(puller);
    let Server {
        _runtime, binding, ..
    } = server;
    let (socket, held) = within(binding.hand_over()).await.into_parts();
    let successor = Runtime::new(RuntimeConfig::default()).expect("successor runtime");
    let successor_listener = successor.listener();
    let successor_binding = within(successor_listener.bind_quic_socket(socket, certs.server_tls()))
        .await
        .expect("serve the handed socket");
    within(successor_binding.take_over(&held))
        .await
        .expect("take over");

    let lost = next_transition(&mut events).await;
    assert!(
        matches!(
            lost,
            PeerEvent::Lost {
                cause: LossCause::PeerRestarting,
                ..
            }
        ),
        "a handover is reported as a restart, got {lost:?}"
    );
    let gave_up = next_transition(&mut events).await;
    assert!(
        matches!(
            gave_up,
            PeerEvent::GaveUp {
                why: GiveUp::Policy { attempts: 0 },
                ..
            }
        ),
        "no attempt is made, got {gave_up:?}"
    );
    let err = within(pusher.send(b"orphan"))
        .await
        .expect_err("a given-up peer fails the send");
    assert!(
        matches!(err, Error::ConnectionLost(LossCause::PeerRestarting)),
        "the restart survives as the cause, got {err:?}"
    );

    client.shutdown().await;
}
/// Claim: a handoff is taken over only by a binding on the address it was
/// made for, and bytes that are not a handoff are refused as such.
#[tokio::test]
async fn a_handoff_is_refused_where_it_does_not_belong() {
    let certs = Certs::generate();
    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let loopback: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let first = within(listener.bind_quic(loopback, certs.server_tls()))
        .await
        .expect("first binding");
    let second = within(listener.bind_quic(loopback, certs.server_tls()))
        .await
        .expect("second binding");

    let handoff = within(first.hand_over()).await;
    assert_eq!(handoff.held(), 0, "no connection, nothing held");
    let (_, held) = handoff.into_parts();

    match within(second.take_over(&held)).await {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput, "{e}"),
        other => panic!("another address's handoff is refused, got {other:?}"),
    }
    match within(second.take_over(b"garbage")).await {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "{e}"),
        other => panic!("garbage is refused, got {other:?}"),
    }
}
/// Claim: a successor sends only what a handover can have held. The bytes
/// come from outside this process, so a destination no client has and more
/// datagrams than a handover holds are refused before anything is sent.
#[tokio::test]
async fn a_handoff_is_bounded_by_what_a_handover_holds() {
    fn put_addr(out: &mut Vec<u8>, addr: SocketAddr) {
        let std::net::IpAddr::V4(ip) = addr.ip() else {
            panic!("the test names IPv4 addresses");
        };
        out.push(4);
        out.extend_from_slice(&ip.octets());
        out.extend_from_slice(&addr.port().to_be_bytes());
    }

    let certs = Certs::generate();
    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let loopback: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let binding = within(listener.bind_quic(loopback, certs.server_tls()))
        .await
        .expect("binding");
    let local = binding.local_addr();

    // The documented byte format: magic, version, the binding's address, a
    // count, and per datagram its destination, a tag for its source address
    // and its length-prefixed bytes.
    let blob = |entries: &[(SocketAddr, Vec<u8>)]| {
        let mut out = b"WDHO".to_vec();
        out.push(1);
        put_addr(&mut out, local);
        out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
        for (destination, contents) in entries {
            put_addr(&mut out, *destination);
            out.push(0);
            out.extend_from_slice(&(contents.len() as u16).to_be_bytes());
            out.extend_from_slice(contents);
        }
        out
    };
    let refused = |outcome: Result<usize, Error>, what: &str| match outcome {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "{what}: {e}"),
        other => panic!("{what} is refused as invalid data, got {other:?}"),
    };

    // The discard port of the loopback: a client address nobody answers on.
    let client: SocketAddr = "127.0.0.1:9".parse().expect("client");
    let packet = vec![0x40, 1, 2, 3];
    let sent = within(binding.take_over(&blob(&[(client, packet.clone())])))
        .await
        .expect("what a handover holds is sent");
    assert_eq!(sent, 1);

    let multicast: SocketAddr = "224.0.0.251:5353".parse().expect("multicast");
    refused(
        within(binding.take_over(&blob(&[(multicast, packet.clone())]))).await,
        "a multicast destination",
    );
    refused(
        within(binding.take_over(&blob(&[(client, vec![0x40; 65_000])]))).await,
        "a datagram larger than any packet this process sends",
    );
    let flood: Vec<(SocketAddr, Vec<u8>)> = (0..100_000).map(|_| (client, vec![0x40])).collect();
    refused(
        within(binding.take_over(&blob(&flood))).await,
        "far more datagrams than a handover holds",
    );
}
