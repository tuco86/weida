//! An identity is a source, not a value
//! ([0032](../../../docs/decisions/0032-identity-sources-and-the-handoff.md) §4.1, §4.2).
//!
//! The claims are the ones an operator relies on: a certificate replaced
//! under a live binding is served to the next handshake and nothing is
//! re-bound; a key replaced is a new peer and the pinning client's redial
//! says so; a bootstrapped directory yields the same key on the second start
//! and is readable by nobody else.

use std::path::Path;
use std::time::Duration;

use tokio::time::timeout;
use weida::{
    ClientTls, FilesOptions, GiveUp, Identity, IdentityEvent, IdentitySource, PeerEvent,
    ReconnectPolicy, Runtime, RuntimeConfig, ServerTls, Trust,
};

const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    weida_testkit::within("an identity-source step", DEADLINE, f).await
}

/// A private directory for one test.
fn private_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "weida-id-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A new self-signed certificate over the **same** key the directory holds:
/// what an issuer that reuses the key produces at renewal.
fn resign(dir: &Path, names: &[&str]) -> String {
    let key_pem = std::fs::read_to_string(dir.join("key.pem")).expect("key.pem");
    let key = rcgen::KeyPair::from_pem(&key_pem).expect("key pair");
    let names: Vec<String> = names.iter().map(|n| (*n).to_owned()).collect();
    let params = rcgen::CertificateParams::new(names).expect("params");
    params.self_signed(&key).expect("self-sign").pem()
}

/// Claim: the bootstrap writes an owner-only directory and a second source
/// on the same directory loads the same key.
#[tokio::test]
async fn a_bootstrapped_directory_yields_the_same_key_twice() {
    let dir = private_dir("bootstrap");
    let first = IdentitySource::files(
        &dir,
        FilesOptions {
            names: vec!["localhost".into()],
            ..FilesOptions::default()
        },
    )
    .expect("bootstrap");
    let fingerprint = first.fingerprint().expect("fingerprint");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700, "the directory is owner-only");
        assert_eq!(mode(&dir.join("key.pem")), 0o600, "the key is owner-only");
        assert_eq!(mode(&dir.join("cert.pem")), 0o600);
    }
    drop(first);

    let second = IdentitySource::files(&dir, FilesOptions::default()).expect("load");
    assert_eq!(second.fingerprint().expect("fingerprint"), fingerprint);
    let mut events = second.events();
    // The event stream starts at subscription; the load itself was before
    // it. A forced reload with nothing changed is silent.
    assert!(!second.reload().expect("reload"), "nothing changed");
    assert!(
        timeout(Duration::from_millis(50), events.recv())
            .await
            .is_err(),
        "no event for an unchanged directory"
    );

    // Half an identity is refused rather than bootstrapped over.
    std::fs::remove_file(dir.join("cert.pem")).expect("remove cert");
    let err = IdentitySource::files(&dir, FilesOptions::default())
        .expect_err("a key without its certificate");
    assert!(matches!(err, weida::Error::Tls(_)), "{err:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Claim: a certificate replaced under a live binding is served to the next
/// handshake with no re-bind; the connection made before the replacement is
/// unaffected; the key — and so the fingerprint — is unchanged, which the
/// source reports as `Renewed`.
#[tokio::test]
async fn a_renewed_certificate_is_served_without_rebinding() {
    let dir = private_dir("renew");
    let source = IdentitySource::files(
        &dir,
        FilesOptions {
            names: vec!["localhost".into(), "127.0.0.1".into()],
            poll: Duration::from_millis(1),
        },
    )
    .expect("bootstrap");
    let fingerprint = source.fingerprint().expect("fingerprint");
    let old_cert = std::fs::read_to_string(dir.join("cert.pem")).expect("cert.pem");
    let mut events = source.events();

    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
    let listener = server.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(source.clone()),
        )
        .await
        .expect("bind");
    let url = format!("weida://127.0.0.1:{}/jobs", binding.local_addr().port());
    let puller = listener.puller("/jobs").expect("puller");

    // A client that trusts the *old* certificate as an anchor.
    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let before = client.pusher(Trust::anchor(old_cert.clone()));
    within(before.connect(&url))
        .await
        .expect("connect under the old certificate");
    within(before.send(b"old")).await.expect("send");
    let got = within(puller.recv()).await.expect("recv");
    assert_eq!(within(got.collect(64)).await.expect("collect"), b"old");

    // Renewal: a new certificate over the same key, written by "the agent".
    // It names `localhost` only, where the old one also named `127.0.0.1`:
    // a same-key certificate verifies under either as an anchor (the anchor
    // is a name and a key, and both are unchanged), so the *names* are what
    // shows which certificate a handshake was served.
    let new_cert = resign(&dir, &["localhost"]);
    assert_ne!(new_cert, old_cert);
    std::fs::write(dir.join("cert.pem"), &new_cert).expect("write cert");
    assert!(source.reload().expect("reload"), "the change was picked up");
    let event = within(events.recv()).await;
    assert_eq!(
        event,
        Some(IdentityEvent::Renewed { fingerprint }),
        "same key, new certificate"
    );
    assert_eq!(source.fingerprint().expect("fingerprint"), fingerprint);

    // The next handshake sees the new certificate: dialled by the name it
    // carries it verifies, dialled by the address it no longer carries it is
    // refused — and that refusal is the proof that nothing served the old one.
    let stale = Runtime::new(RuntimeConfig::default()).expect("second client runtime");
    let by_ip = stale.pusher(Trust::anchor(old_cert.clone()));
    let err = within(by_ip.connect(&url))
        .await
        .expect_err("the renewed certificate does not name 127.0.0.1");
    assert!(
        matches!(err, weida::Error::Untrusted(fp) if fp == fingerprint),
        "{err:?}"
    );
    let by_name = stale.pusher(Trust::anchor(new_cert));
    let named = format!("weida://localhost:{}/jobs", binding.local_addr().port());
    within(by_name.connect(&named))
        .await
        .expect("connect by the renewed name");
    within(by_name.send(b"new")).await.expect("send");
    let got = within(puller.recv()).await.expect("recv");
    assert_eq!(within(got.collect(64)).await.expect("collect"), b"new");

    // The connection from before the renewal still carries transfers.
    within(before.send(b"still"))
        .await
        .expect("send on the old connection");
    let got = within(puller.recv()).await.expect("recv");
    assert_eq!(within(got.collect(64)).await.expect("collect"), b"still");

    client.shutdown().await;
    stale.shutdown().await;
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Claim: a replaced key is a new peer. The source reports `KeyChanged`, and
/// a client that pinned the old fingerprint finds its redial refused as
/// `PeerChanged` rather than quietly talking to the new key.
#[tokio::test]
async fn a_replaced_key_is_a_new_peer_for_a_pinning_client() {
    let dir = private_dir("rekey");
    let source = IdentitySource::files(
        &dir,
        FilesOptions {
            names: vec!["localhost".into(), "127.0.0.1".into()],
            poll: Duration::from_millis(1),
        },
    )
    .expect("bootstrap");
    let old = source.fingerprint().expect("fingerprint");
    let mut identity_events = source.events();

    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
    let listener = server.listener();
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let binding = listener
        .bind_quic(addr, ServerTls::new(source.clone()))
        .await
        .expect("bind");
    let addr = binding.local_addr();
    let url = format!("weida://{old}@127.0.0.1:{}/jobs", addr.port());
    let puller = listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        reconnect: ReconnectPolicy {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(50),
            ..ReconnectPolicy::default()
        },
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = client.pusher(ClientTls::new(Trust::by_address()));
    let mut peer_events = pusher.events();
    within(pusher.connect(&url)).await.expect("connect pinned");
    assert!(matches!(
        within(peer_events.recv()).await,
        Some(PeerEvent::Connected { .. })
    ));
    within(pusher.send(b"before")).await.expect("send");
    let got = within(puller.recv()).await.expect("recv");
    assert_eq!(within(got.collect(64)).await.expect("collect"), b"before");

    // A whole new identity lands in the directory.
    let fresh = Identity::generate_for(["localhost", "127.0.0.1"]).expect("generate");
    std::fs::write(dir.join("cert.pem"), fresh.certificate_pem().expect("pem")).expect("cert");
    std::fs::write(dir.join("key.pem"), fresh.to_pem().expect("pem")).expect("key");
    assert!(source.reload().expect("reload"));
    let new = source.fingerprint().expect("fingerprint");
    assert_ne!(new, old);
    assert_eq!(
        within(identity_events.recv()).await,
        Some(IdentityEvent::KeyChanged { from: old, to: new })
    );

    // The old connection is intact; the peer that dialled it is still the
    // peer it proved. What changes is the *next* connection: the binding is
    // closed and re-bound on the same port so the client redials, and the
    // redial pins the old key.
    drop(puller);
    binding.close().await;
    drop(binding);
    assert!(matches!(
        within(peer_events.recv()).await,
        Some(PeerEvent::Lost { .. })
    ));
    // The runtime keeps a closed endpoint's socket until it shuts down, so
    // the re-bind is a new runtime on the same port with the *same* source:
    // the process restarted, the identity directory did not.
    drop(listener);
    server.shutdown().await;
    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime again");
    let listener = server.listener();
    let rebound = within(async {
        loop {
            match listener
                .bind_quic(addr, ServerTls::new(source.clone()))
                .await
            {
                Ok(binding) => break binding,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await;
    let _puller = listener.puller("/jobs").expect("puller");
    let gave_up = within(async {
        loop {
            match peer_events.recv().await.expect("events") {
                PeerEvent::Retrying { .. } => continue,
                event => break event,
            }
        }
    })
    .await;
    assert!(
        matches!(
            gave_up,
            PeerEvent::GaveUp {
                why: GiveUp::PeerChanged { presented: Some(fp) },
                ..
            } if fp == new
        ),
        "the redial refuses the new key, got {gave_up:?}"
    );
    assert_eq!(pusher.peer_count(), 0);

    drop(rebound);
    client.shutdown().await;
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Claim: two endpoints configured with equal static values share a pooled
/// connection, exactly as before sources existed; two file sources on
/// different directories do not.
#[tokio::test]
async fn static_sources_configured_alike_share_a_connection() {
    let a = ClientTls::new(Trust::anchor("-ca-"));
    let b = ClientTls::new(Trust::anchor("-ca-"));
    assert_eq!(a, b, "equal by content");
    let dir_a = private_dir("pool-a");
    let dir_b = private_dir("pool-b");
    let ida = IdentitySource::files(&dir_a, FilesOptions::default()).expect("a");
    let idb = IdentitySource::files(&dir_b, FilesOptions::default()).expect("b");
    assert_ne!(
        a.clone().with_identity(ida.clone()),
        a.clone().with_identity(idb),
        "different sources"
    );
    assert_eq!(
        a.clone().with_identity(ida.clone()),
        a.with_identity(ida),
        "the same source"
    );
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}
