//! A closed QUIC binding frees its port at once, whatever still holds its
//! connections, so a server restarted on the same port binds it again
//! (proposal #5 from griasdi's `a_call_survives_a_server_restart`).

use std::net::SocketAddr;
use std::time::Duration;

use weida::{
    DEFAULT_DATAGRAM_RECEIVE_BYTES, FlowMeta, Incoming, JoinTerms, Limits, Received, Runtime,
    RuntimeConfig, TransferMeta,
};

use crate::common;

/// Rounds, alternating the two closers; every round after the first binds
/// the port of round 0 again.
const ROUNDS: usize = 200;
const STEP: Duration = Duration::from_secs(10);

async fn within<F: Future>(what: &str, f: F) -> F::Output {
    weida_testkit::within(what, STEP, f).await
}

/// Flows on, on both sides.
fn config() -> RuntimeConfig {
    RuntimeConfig {
        limits: Limits {
            datagram_receive_bytes: DEFAULT_DATAGRAM_RECEIVE_BYTES,
            ..Limits::default()
        },
        ..RuntimeConfig::default()
    }
}

/// Claim: when [`weida::Binding::close`] or [`Runtime::shutdown`] returns,
/// the binding's port can be bound again, even though the application still
/// holds a dish's downlink, an accepted flow and an exchange in flight on
/// it. The port of round 0 is bound again in every later round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_binding_frees_its_port_while_its_connections_are_held() {
    let certs = common::Certs::generate();
    let mut addr: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    for round in 0..ROUNDS {
        let server = Runtime::new(config()).expect("server runtime");
        let listener = server.listener();
        let radio = listener.radio("/r").expect("radio");
        let acceptor = listener.acceptor("/up").expect("acceptor");
        let replier = listener.replier("/rpc").expect("replier");
        let binding = within("bind", listener.bind_quic(addr, certs.server_tls()))
            .await
            .unwrap_or_else(|e| panic!("round {round}: binding {addr} again: {e}"));
        addr = binding.local_addr();
        let url = |path: &str| format!("weida://127.0.0.1:{}{path}", addr.port());

        // The client redials the closed port, and at teardown those dials are
        // still in their handshake, whose close waits out three probe timeouts
        // of the initial RTT estimate: longer than the default budget, which
        // every round would then spend. The client's close is not under test.
        let client = Runtime::new(RuntimeConfig {
            shutdown_timeout: Duration::from_millis(100),
            ..config()
        })
        .expect("client runtime");

        // A radio downlink: the radio's flow toward the dish stays open.
        let dish = client.dish(certs.client_tls());
        within("join", dish.join("voice", JoinTerms::default()))
            .await
            .expect("join");
        within("the dish's connect", dish.connect(&url("/r")))
            .await
            .expect("dish connect");
        within("a datagram at the dish", async {
            loop {
                radio
                    .datagram("voice", vec![0x33; 150])
                    .expect("radio datagram");
                if let Ok(Ok(Received::Datagram { .. })) =
                    tokio::time::timeout(Duration::from_millis(20), dish.recv()).await
                {
                    break;
                }
            }
        })
        .await;

        // An uplink flow, held open at the server.
        let peer = client.peer(certs.client_tls());
        within("the peer's connect", peer.connect(&url("/up")))
            .await
            .expect("peer connect");
        let flow = within(
            "open flow",
            peer.open_flow(FlowMeta::default().with_topic("mic")),
        )
        .await
        .expect("open flow");
        flow.send(vec![0x11; 100]).expect("flow send");
        let Incoming::Flow(uplink) = within("the flow at the acceptor", acceptor.accept())
            .await
            .expect("accept flow")
        else {
            panic!("round {round}: expected a flow");
        };

        // An exchange in flight: the request is never finished, never answered.
        let requester = client.requester(certs.client_tls());
        within("the requester's connect", requester.connect(&url("/rpc")))
            .await
            .expect("requester connect");
        let (mut request, reply) = within("open request", requester.open(TransferMeta::default()))
            .await
            .expect("open request");
        within("write request", request.write_all(b"in flight"))
            .await
            .expect("write request");
        let pending = within("the request at the replier", replier.accept())
            .await
            .expect("accept request");

        // The close under test, with every handle above still alive.
        if round % 2 == 0 {
            within("the binding's close", binding.close()).await;
        } else {
            within("the runtime's shutdown", server.clone().shutdown()).await;
        }

        let probe = std::net::UdpSocket::bind(addr).unwrap_or_else(|e| {
            panic!("round {round}: {addr} is still taken after the close returned: {e}")
        });
        drop(probe);

        drop((
            pending, request, reply, requester, uplink, flow, peer, dish, radio, acceptor, replier,
            binding, listener,
        ));
        within("the server's shutdown", server.shutdown()).await;
        within("the client's shutdown", client.shutdown()).await;
    }
}
