//! A dialling endpoint reads the health of its own connections without
//! holding a flow
//! ([0036](../../../docs/decisions/0036-connection-statistics.md)).

use crate::common;

use std::time::Duration;

use common::{Certs, Restartable, Server};
use weida::{
    Limits, Network, PeerEvent, PeerEvents, ReconnectPolicy, Runtime, RuntimeConfig, SimNet,
};

pub(crate) const DEADLINE: Duration = Duration::from_secs(10);

pub(crate) async fn within<F: Future>(f: F) -> F::Output {
    weida_testkit::within("a stats step", DEADLINE, f).await
}

/// The next event that is not `Retrying`.
pub(crate) async fn next_transition(events: &mut PeerEvents) -> PeerEvent {
    within(async {
        loop {
            match events.recv().await.expect("event stream open") {
                PeerEvent::Retrying { .. } => continue,
                event => return event,
            }
        }
    })
    .await
}

/// Claim: a local connection is reported with its age and without
/// transport numbers, because it has no path to measure.
#[tokio::test(start_paused = true)]
async fn a_local_connection_has_an_age_and_no_transport() {
    let bus = format!("weida-stats-{}", std::process::id());
    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
    let listener = server.listener();
    let _binding = listener.bind_inproc(&bus).expect("bind inproc");
    let _puller = listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let pusher = client.pusher(weida::Trust::by_address());
    let url = format!("weida+inproc://{bus}/jobs");
    within(pusher.connect(&url)).await.expect("connect");

    let first = pusher.connection_stats();
    assert_eq!(first.len(), 1);
    assert_eq!(&*first[0].url, url);
    assert_eq!(first[0].transport, None, "in process there is no path");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let later = pusher.connection_stats();
    assert!(
        later[0].age >= first[0].age + Duration::from_millis(20),
        "age grows with the connection: {:?} -> {:?}",
        first[0].age,
        later[0].age
    );

    client.shutdown().await;
    server.shutdown().await;
}

/// Claim: a transparent redial is counted on the address, and the age
/// belongs to the connection, so it starts again on the new one. While the
/// address is down it has no live connection to report.
#[tokio::test(start_paused = true)]
async fn a_redial_is_counted_and_restarts_the_age() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let first = Restartable::start(&net, &certs, "127.0.0.1:0".parse().expect("loopback")).await;
    let addr = first.addr;
    let puller = first.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        reconnect: ReconnectPolicy {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(50),
            ..ReconnectPolicy::default()
        },
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let mut events = pusher.events();
    let url = format!("weida://127.0.0.1:{}/jobs", addr.port());
    within(pusher.connect(&url)).await.expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));

    tokio::time::sleep(Duration::from_millis(300)).await;
    let old = pusher.connection_stats();
    assert_eq!(old[0].redials, 0);
    let old_age = old[0].age;
    assert!(old_age >= Duration::from_millis(300));

    drop(puller);
    first.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));
    assert!(
        pusher.connection_stats().is_empty(),
        "an address being redialled has no live connection"
    );

    let second = Restartable::start(&net, &certs, addr).await;
    let _puller = second.listener.puller("/jobs").expect("puller");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    let new = pusher.connection_stats();
    assert_eq!(new.len(), 1);
    assert_eq!(&*new[0].url, url);
    assert_eq!(new[0].redials, 1, "one successful redial");
    assert!(
        new[0].age < old_age,
        "the new connection's age started again: {:?} after {old_age:?}",
        new[0].age
    );

    client.shutdown().await;
}

pub(crate) fn reporting() -> Limits {
    Limits {
        path_report: true,
        ..Limits::default()
    }
}

/// Claim: reports flow only when both sides ask, and never on a local
/// transport, which has no path.
#[tokio::test(start_paused = true)]
async fn the_remote_view_needs_both_sides_and_quic() {
    // The server asks, the client does not: nothing is agreed.
    let server = Server::start_with(reporting()).await;
    let client = server.client_runtime();
    let peer = client.peer(server.trust());
    within(peer.connect(&server.url("/any")))
        .await
        .expect("connect");
    // A record would arrive at once after the HELLOs; give it the time.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(peer.connection_stats()[0].remote, None);
    client.shutdown().await;

    // Both ask, in process: a local transport never lists the code.
    let bus = format!("weida-report-{}", std::process::id());
    let local = Runtime::new(RuntimeConfig {
        limits: reporting(),
        ..RuntimeConfig::default()
    })
    .expect("server runtime");
    let listener = local.listener();
    let _binding = listener.bind_inproc(&bus).expect("bind inproc");
    let _puller = listener.puller("/jobs").expect("puller");
    let dialler = Runtime::new(RuntimeConfig {
        limits: reporting(),
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = dialler.pusher(weida::Trust::by_address());
    within(pusher.connect(&format!("weida+inproc://{bus}/jobs")))
        .await
        .expect("connect");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(pusher.connection_stats()[0].remote, None);
    dialler.shutdown().await;
    local.shutdown().await;
}
