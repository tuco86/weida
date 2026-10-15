//! A binding offers exactly one ALPN token, and a dial under the other one
//! never reaches it: the mismatch fails the TLS handshake
//! (`docs/PROTOCOL.md` §2.1).

use crate::common;

use std::time::Duration;

use common::Certs;
use tokio::io::AsyncReadExt;
use weida::{Alpn, Binding, ClientTls, Listener, Runtime, RuntimeConfig, TransferMeta, Trust};

const DEADLINE: Duration = Duration::from_secs(10);

async fn bind(certs: &Certs, alpn: Alpn) -> (Runtime, Listener, Binding) {
    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback address"),
            certs.server_tls().with_alpn(alpn),
        )
        .await
        .expect("bind");
    (runtime, listener, binding)
}

fn pinned_url(certs: &Certs, binding: &Binding) -> String {
    format!(
        "weida://{}@127.0.0.1:{}/x",
        certs.fingerprint(),
        binding.local_addr().port()
    )
}

/// Answers every request on `/x` with its own body.
fn spawn_echo(listener: &Listener) {
    let replier = listener.replier("/x").expect("replier");
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let mut body = Vec::new();
            request
                .body()
                .read_to_end(&mut body)
                .await
                .expect("read body");
            let mut out = request
                .reply(TransferMeta::default())
                .await
                .expect("open reply");
            out.write_all(&body).await.expect("write reply");
            out.finish().expect("finish reply");
        }
    });
}

#[tokio::test]
async fn a_raft_binding_refuses_an_application_dial() {
    let certs = Certs::generate();
    let (_runtime, listener, binding) = bind(&certs, Alpn::Raft).await;
    spawn_echo(&listener);
    let url = pinned_url(&certs, &binding);

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let plain = client.requester(ClientTls::new(Trust::by_address()));
    let refused = tokio::time::timeout(DEADLINE, plain.connect(&url))
        .await
        .expect("the handshake ends within the deadline");
    assert!(
        refused.is_err(),
        "a weida/0 dial reached a weida-raft/0 binding"
    );

    let raft = client.requester(ClientTls::new(Trust::by_address()).with_alpn(Alpn::Raft));
    tokio::time::timeout(DEADLINE, raft.connect(&url))
        .await
        .expect("connect within the deadline")
        .expect("a weida-raft/0 dial reaches a weida-raft/0 binding");
    let reply = raft.request(b"ping").await.expect("request");
    assert_eq!(reply.collect(16).await.expect("collect"), b"ping");
    client.shutdown().await;
}

#[tokio::test]
async fn an_application_binding_refuses_a_raft_dial() {
    let certs = Certs::generate();
    let (_runtime, listener, binding) = bind(&certs, Alpn::Weida).await;
    spawn_echo(&listener);
    let url = pinned_url(&certs, &binding);

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let raft = client.requester(ClientTls::new(Trust::by_address()).with_alpn(Alpn::Raft));
    let refused = tokio::time::timeout(DEADLINE, raft.connect(&url))
        .await
        .expect("the handshake ends within the deadline");
    assert!(
        refused.is_err(),
        "a weida-raft/0 dial reached a weida/0 binding"
    );
    client.shutdown().await;
}
