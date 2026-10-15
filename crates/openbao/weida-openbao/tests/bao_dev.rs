//! The same claims as `tests/scripted.rs`, against a real OpenBao: the
//! local `bao` binary in dev mode, started and stopped by the test.
//!
//! Ignored by default — the gate has no OpenBao — and run by hand:
//!
//! ```text
//! cargo test -p weida-openbao --test bao_dev -- --ignored
//! ```
//!
//! What only this file can prove: that a wrapping token minted by
//! `auth/token/create` with `X-Vault-Wrap-TTL` is redeemed **once** and
//! refused the second time in the exact words the client keys on; and that
//! a certificate a real PKI role signed over weida's CSR verifies, under the
//! role's own CA, for a peer dialling a weida binding — and again after the
//! renewal, with the same fingerprint.

use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::json;
use tokio::time::timeout;
use weida::{
    ClientTls, FilesOptions, IdentityEvent, IdentitySource, Runtime, RuntimeConfig, ServerTls,
};
use weida_openbao::{Auth, Config, Error, HandoffSource, OpenBao, PkiAnchor, PkiSign};

const ROOT: &str = "root-for-tests";

/// The dev server speaks `http://` on this host, which the client only does
/// when told to.
fn insecure(address: &str) -> Config {
    Config {
        allow_insecure: true,
        ..Config::new(address)
    }
}

async fn within<F: Future>(f: F) -> F::Output {
    timeout(Duration::from_secs(30), f)
        .await
        .expect("timed out")
}

/// A dev server on an ephemeral port, killed when dropped.
struct DevServer {
    child: Child,
    address: String,
}

impl DevServer {
    async fn start() -> DevServer {
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("probe port")
            .local_addr()
            .expect("addr")
            .port();
        let listen = format!("127.0.0.1:{port}");
        let child = Command::new("bao")
            .args([
                "server",
                "-dev",
                &format!("-dev-root-token-id={ROOT}"),
                &format!("-dev-listen-address={listen}"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("bao is on PATH");
        let address = format!("http://{listen}");
        let server = DevServer { child, address };
        let ready = OpenBao::new(insecure(&server.address)).expect("client");
        within(async {
            loop {
                if ready.login(Auth::Token(ROOT.into())).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        server
    }

    fn root(&self) -> OpenBao {
        OpenBao::new(insecure(&self.address)).expect("client")
    }
}

impl Drop for DevServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn private_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("weida-bao-dev-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Claim: the hand-off, end to end. The controller (root here) mints a
/// child token wrapped for five seconds; the service redeems it and holds a
/// renewable token with the role's policies; a second redemption fails as
/// `HandoffStolen`; the controller revokes by accessor and the service's
/// token is dead.
#[tokio::test]
#[ignore = "needs the `bao` binary; see the module doc"]
async fn the_handoff_is_redeemed_once_against_a_real_server() {
    let dev = DevServer::start().await;
    let controller = dev.root();
    within(controller.login(Auth::Token(ROOT.into())))
        .await
        .expect("root login");

    // The service's policy and role, as a controller would have provisioned.
    within(controller.post(
        "sys/policies/acl/svc",
        json!({ "policy": "path \"secret/data/svc/*\" { capabilities = [\"read\"] }" }),
    ))
    .await
    .expect("policy");
    within(controller.post(
        "auth/token/roles/svc",
        json!({ "allowed_policies": "svc", "orphan": false, "renewable": true, "token_period": "60s" }),
    ))
    .await
    .expect("role");
    let wrapping = within(controller.post_wrapped(
        "auth/token/create/svc",
        json!({ "policies": ["svc"], "meta": { "unit": "svc.service" } }),
        Duration::from_secs(5),
    ))
    .await
    .expect("wrapped child token");

    let service = OpenBao::new(insecure(&dev.address)).expect("client");
    let info = within(service.login(Auth::Handoff(HandoffSource::Token(wrapping.clone()))))
        .await
        .expect("redeem");
    assert!(info.renewable);
    assert!(
        info.policies.iter().any(|p| p == "svc"),
        "{:?}",
        info.policies
    );
    let accessor = service.accessor().expect("accessor");

    let thief = OpenBao::new(insecure(&dev.address)).expect("client");
    let err = within(thief.login(Auth::Handoff(HandoffSource::Token(wrapping))))
        .await
        .expect_err("second redemption");
    assert!(matches!(err, Error::HandoffStolen), "{err:?}");

    within(controller.revoke_accessor(&accessor))
        .await
        .expect("revoke");
    let err = within(service.renew_self()).await.expect_err("revoked");
    assert!(matches!(err, Error::Api { status: 403, .. }), "{err:?}");
}

/// Claim: a certificate a real role signed over weida's CSR serves a weida
/// binding, verifies under the mount's CA for a dialling peer, and is
/// renewed under the same key — same fingerprint, new certificate, next
/// handshake.
#[tokio::test]
#[ignore = "needs the `bao` binary; see the module doc"]
async fn a_pki_role_signs_weida_and_the_renewal_keeps_the_key() {
    let dev = DevServer::start().await;
    let root = dev.root();
    within(root.login(Auth::Token(ROOT.into())))
        .await
        .expect("root login");
    within(root.post("sys/mounts/pki", json!({ "type": "pki" })))
        .await
        .expect("mount");
    within(root.post(
        "pki/config/urls",
        json!({ "issuing_certificates": format!("{}/v1/pki/ca", dev.address) }),
    ))
    .await
    .expect("urls");
    within(root.post(
        "pki/root/generate/internal",
        json!({ "common_name": "weida-test-ca", "ttl": "1h", "key_type": "ec", "key_bits": 256 }),
    ))
    .await
    .expect("root ca");
    // The role settings of 0032 §2, all of them.
    within(root.post(
        "pki/roles/svc",
        json!({
            "allowed_domains": "localhost",
            "allow_bare_domains": true,
            "allow_ip_sans": true,
            "server_flag": true,
            "client_flag": true,
            "key_type": "ec",
            "key_bits": 256,
            "max_ttl": "1h",
            "require_cn": false,
            "use_csr_sans": true,
        }),
    ))
    .await
    .expect("role");

    let dir = private_dir("pki");
    let key_source = IdentitySource::files(
        &dir,
        FilesOptions {
            names: vec!["localhost".into()],
            ..FilesOptions::default()
        },
    )
    .expect("key");
    let fingerprint = key_source.fingerprint().expect("fingerprint");
    let signing = PkiSign {
        ips: vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
        ttl: Some(Duration::from_secs(10)),
        renew_at: 0.3,
        ..PkiSign::new("svc", ["localhost"])
    };
    let served = within(signing.start(root.clone(), &key_source))
        .await
        .expect("signed");
    assert_eq!(served.fingerprint().expect("fingerprint"), fingerprint);
    let mut events = served.events();

    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
    let listener = server.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(served.clone()),
        )
        .await
        .expect("bind");
    let url = format!("weida://127.0.0.1:{}/jobs", binding.local_addr().port());
    let puller = listener.puller("/jobs").expect("puller");

    let anchor = within(PkiAnchor::new().start(root.clone()))
        .await
        .expect("anchor");
    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let pusher = client.pusher(ClientTls::new(anchor.clone()));
    within(pusher.connect(&url))
        .await
        .expect("connect under the CA");
    within(pusher.send(b"signed")).await.expect("send");
    let got = within(puller.recv()).await.expect("recv");
    assert_eq!(within(got.collect(64)).await.expect("collect"), b"signed");

    // The renewal, three seconds in: same key, new certificate, and the next
    // connection verifies against it.
    assert_eq!(
        within(events.recv()).await,
        Some(IdentityEvent::Renewed { fingerprint })
    );
    let again = client.pusher(ClientTls::new(anchor));
    within(again.connect(&url))
        .await
        .expect("connect after the renewal");
    within(again.send(b"renewed")).await.expect("send");
    let got = within(puller.recv()).await.expect("recv");
    assert_eq!(within(got.collect(64)).await.expect("collect"), b"renewed");

    client.shutdown().await;
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
