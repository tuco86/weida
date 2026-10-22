//! The tests of `tests/identity.rs` that need a raw server: a bare `quinn`
//! endpoint on the simulated network whose identity a test can swap.

use crate::common;
use common::Certs;
use weida::{ClientTls, Error, Network, Runtime, RuntimeConfig, SimNet, Trust};

use crate::identity::*;

/// Claim: the connections of one peer are bound together by the proved
/// fingerprint, and by nothing else
/// ([0008](../../../docs/decisions/0008-session-identity.md) §4.2). One
/// connection per dialled path means a second path is a second connection to
/// the *same* peer, so a second server answering on that authority is refused,
/// carrying the fingerprint that answered rather than a bare failure.
///
/// The setup is the load-balancer case, and it needs a raw server because two
/// `quinn` endpoints cannot share a port: one socket, whose identity is
/// swapped between the two handshakes. The client trusts both keys, so nothing
/// but the binding rule can refuse the second dial.
#[tokio::test(start_paused = true)]
async fn a_second_path_that_answers_with_another_identity_is_refused() {
    let first = Certs::generate();
    let second = Certs::generate();
    let first_fp = first.fingerprint();
    let second_fp = second.fingerprint();

    let net = SimNet::new();
    let (endpoint, addr) = common::raw::sim_server_endpoint(&net, &first);
    let swap = common::raw::server_config(&second);
    tokio::spawn(async move {
        // First handshake: identity one. The identity is swapped *before* the
        // HELLO that lets the client's `connect` return, so the second dial
        // cannot race ahead of the swap.
        let incoming = endpoint.accept().await.expect("first connection");
        let held = incoming.await.expect("first handshake");
        endpoint.set_server_config(Some(swap));
        common::raw::send_hello(&held).await;

        // Second handshake: identity two. It never needs a HELLO — the pool
        // refuses it on identity, before negotiation begins.
        if let Some(incoming) = endpoint.accept().await {
            let _ = incoming.await;
        }
        // Hold the first connection open: the binding rule compares against
        // this peer's *live* connections.
        held.closed().await;
    });

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("client");
    let pusher = client.pusher(ClientTls::new(Trust::pin(first_fp).and_pin(second_fp)));

    let base = format!("weida://127.0.0.1:{}", addr.port());
    within(pusher.connect(&format!("{base}/a")))
        .await
        .expect("the first path connects");

    let err = within(pusher.connect(&format!("{base}/b")))
        .await
        .expect_err("a different peer on the same authority must be refused");
    match err {
        Error::Untrusted(fp) => assert_eq!(
            fp, second_fp,
            "the refusal names who answered, so an operator can pin it"
        ),
        other => panic!("expected Untrusted, got {other:?}"),
    }
    // The path that was already connected is untouched by its neighbour's
    // refusal: one connection per path means one failure per path.
    assert_eq!(pusher.peer_count(), 1);

    client.shutdown().await;
}
